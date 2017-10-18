// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 XSKY <haomai@xsky.com>
 *
 * Author: Haomai Wang <haomaiwang@gmail.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "RDMAStack.h"

#define dout_subsys ceph_subsys_ms
#undef dout_prefix
#define dout_prefix *_dout << " RDMAConnectedSocketImpl "


static const size_t RDMA_CONTROL_MSG = sizeof("00000000:00000000:00000000:00000000:00000000");


RDMAConnectedSocketImpl::RDMAConnectedSocketImpl(CephContext *cct, Infiniband* ib, RDMADispatcher* s,
						 RDMAWorker *w)
  : cct(cct), connected(0), error(0), infiniband(ib),
    dispatcher(s), worker(w), lock("RDMAConnectedSocketImpl::lock"),
    is_server(false), con_handler(new C_handle_connection(this)),
    active(false), pending(false),
    m_rdma_rec_buffers([](const uint32_t& seq1, const uint32_t& seq2) -> bool {return !rdma_is_seq_larger(seq1, seq2)}),
    m_rdma_send_seq(0),
    m_rdam_rec_seq(0)
{
  qp = infiniband->create_queue_pair(
				     cct, s->get_tx_cq(), s->get_rx_cq(), IBV_QPT_RC);
  my_msg.qpn = qp->get_local_qp_number();
  my_msg.psn = qp->get_initial_psn();
  my_msg.lid = infiniband->get_lid();
  my_msg.peer_qpn = 0;
  my_msg.gid = infiniband->get_gid();
  notify_fd = dispatcher->register_qp(qp, this);
  dispatcher->perf_logger->inc(l_msgr_rdma_created_queue_pair);
  dispatcher->perf_logger->inc(l_msgr_rdma_active_queue_pair);
}

RDMAConnectedSocketImpl::~RDMAConnectedSocketImpl()
{
  ldout(cct, 20) << __func__ << " destruct." << dendl;
  cleanup();
  worker->remove_pending_conn(this);
  dispatcher->erase_qpn(my_msg.qpn);

  for (unsigned i=0; i < wc.size(); ++i) {
    dispatcher->post_chunk_to_pool(reinterpret_cast<Chunk*>(wc[i].wr_id));
  }
  for (unsigned i=0; i < buffers.size(); ++i) {
    dispatcher->post_chunk_to_pool(buffers[i]);
  }

  Mutex::Locker l(lock);
  if (notify_fd >= 0)
    ::close(notify_fd);
  if (tcp_fd >= 0)
    ::close(tcp_fd);
  error = ECONNRESET;
}

void RDMAConnectedSocketImpl::pass_wc(std::vector<ibv_wc> &&v)
{
  Mutex::Locker l(lock);
  if (wc.empty())
    wc = std::move(v);
  else
    wc.insert(wc.end(), v.begin(), v.end());
  notify();
}

void RDMAConnectedSocketImpl::get_wc(std::vector<ibv_wc> &w)
{
  Mutex::Locker l(lock);
  if (wc.empty())
    return ;
  w.swap(wc);
}

int RDMAConnectedSocketImpl::activate()
{
  ibv_qp_attr qpa;
  int r;

  // now connect up the qps and switch to RTR
  memset(&qpa, 0, sizeof(qpa));
  qpa.qp_state = IBV_QPS_RTR;
  qpa.path_mtu = IBV_MTU_1024;
  qpa.dest_qp_num = peer_msg.qpn;
  qpa.rq_psn = peer_msg.psn;
  qpa.max_dest_rd_atomic = 1;
  qpa.min_rnr_timer = 12;
  //qpa.ah_attr.is_global = 0;
  qpa.ah_attr.is_global = 1;
  qpa.ah_attr.grh.hop_limit = 6;
  qpa.ah_attr.grh.dgid = peer_msg.gid;

  qpa.ah_attr.grh.sgid_index = infiniband->get_device()->get_gid_idx();

  qpa.ah_attr.dlid = peer_msg.lid;
  qpa.ah_attr.sl = cct->_conf->ms_async_rdma_sl;
  qpa.ah_attr.grh.traffic_class = cct->_conf->ms_async_rdma_dscp;
  qpa.ah_attr.src_path_bits = 0;
  qpa.ah_attr.port_num = (uint8_t)(infiniband->get_ib_physical_port());

  ldout(cct, 20) << __func__ << " Choosing gid_index " << (int)qpa.ah_attr.grh.sgid_index << ", sl " << (int)qpa.ah_attr.sl << dendl;

  r = ibv_modify_qp(qp->get_qp(), &qpa, IBV_QP_STATE |
      IBV_QP_AV |
      IBV_QP_PATH_MTU |
      IBV_QP_DEST_QPN |
      IBV_QP_RQ_PSN |
      IBV_QP_MIN_RNR_TIMER |
      IBV_QP_MAX_DEST_RD_ATOMIC);
  if (r) {
    lderr(cct) << __func__ << " failed to transition to RTR state: "
               << cpp_strerror(errno) << dendl;
    return -1;
  }

  ldout(cct, 20) << __func__ << " transition to RTR state successfully." << dendl;

  // now move to RTS
  qpa.qp_state = IBV_QPS_RTS;

  // How long to wait before retrying if packet lost or server dead.
  // Supposedly the timeout is 4.096us*2^timeout.  However, the actual
  // timeout appears to be 4.096us*2^(timeout+1), so the setting
  // below creates a 135ms timeout.
  qpa.timeout = 14;

  // How many times to retry after timeouts before giving up.
  qpa.retry_cnt = 7;

  // How many times to retry after RNR (receiver not ready) condition
  // before giving up. Occurs when the remote side has not yet posted
  // a receive request.
  qpa.rnr_retry = 7; // 7 is infinite retry.
  qpa.sq_psn = my_msg.psn;
  qpa.max_rd_atomic = 1;

  r = ibv_modify_qp(qp->get_qp(), &qpa, IBV_QP_STATE |
      IBV_QP_TIMEOUT |
      IBV_QP_RETRY_CNT |
      IBV_QP_RNR_RETRY |
      IBV_QP_SQ_PSN |
      IBV_QP_MAX_QP_RD_ATOMIC);
  if (r) {
    lderr(cct) << __func__ << " failed to transition to RTS state: "
               << cpp_strerror(errno) << dendl;
    return -1;
  }

  // the queue pair should be ready to use once the client has finished
  // setting up their end.
  ldout(cct, 20) << __func__ << " transition to RTS state successfully." << dendl;
  ldout(cct, 20) << __func__ << " QueuePair: " << qp << " with qp:" << qp->get_qp() << dendl;

  if (!is_server) {
    connected = 1; //indicate successfully
    ldout(cct, 20) << __func__ << " handle fake send, wake it up. QP: " << my_msg.qpn << dendl;
    submit(false);
  }
  active = true;

  return 0;
}

int RDMAConnectedSocketImpl::try_connect(const entity_addr_t& peer_addr, const SocketOptions &opts) {
  ldout(cct, 20) << __func__ << " nonblock:" << opts.nonblock << ", nodelay:"
                 << opts.nodelay << ", rbuf_size: " << opts.rcbuf_size << dendl;
  NetHandler net(cct);
  tcp_fd = net.connect(peer_addr, opts.connect_bind_addr);

  if (tcp_fd < 0) {
    return -errno;
  }
  net.set_close_on_exec(tcp_fd);

  int r = net.set_socket_options(tcp_fd, opts.nodelay, opts.rcbuf_size);
  if (r < 0) {
    ::close(tcp_fd);
    tcp_fd = -1;
    return -errno;
  }

  ldout(cct, 20) << __func__ << " tcp_fd: " << tcp_fd << dendl;
  net.set_priority(tcp_fd, opts.priority, peer_addr.get_family());
  my_msg.peer_qpn = 0;
  r = infiniband->send_msg(cct, tcp_fd, my_msg);
  if (r < 0)
    return r;

  worker->center.create_file_event(tcp_fd, EVENT_READABLE, con_handler);
  return 0;
}

void RDMAConnectedSocketImpl::handle_connection() {
  
  if(connected == 1) {
    handle_rdma_control();
    return;
  }  

  
  ldout(cct, 20) << __func__ << " QP: " << my_msg.qpn << " tcp_fd: " << tcp_fd << " notify_fd: " << notify_fd << dendl;
  int r = infiniband->recv_msg(cct, tcp_fd, peer_msg);
  if (r <= 0) {
    if (r != -EAGAIN) {
      dispatcher->perf_logger->inc(l_msgr_rdma_handshake_errors);
      ldout(cct, 1) << __func__ << " recv handshake msg failed." << dendl;
      fault();
    }
    return;
  }

  if (1 == connected) {
    ldout(cct, 1) << __func__ << " warnning: logic failed: read len: " << r << dendl;
    fault();
    return;
  }

  if (!is_server) {// syn + ack from server
    my_msg.peer_qpn = peer_msg.qpn;
    ldout(cct, 20) << __func__ << " peer msg :  < " << peer_msg.qpn << ", " << peer_msg.psn
                   <<  ", " << peer_msg.lid << ", " << peer_msg.peer_qpn << "> " << dendl;
    if (!connected) {
      r = activate();
      assert(!r);
    }
    notify();
    r = infiniband->send_msg(cct, tcp_fd, my_msg);
    if (r < 0) {
      ldout(cct, 1) << __func__ << " send client ack failed." << dendl;
      dispatcher->perf_logger->inc(l_msgr_rdma_handshake_errors);
      fault();
    }
  } else {
    if (peer_msg.peer_qpn == 0) {// syn from client
      if (active) {
        ldout(cct, 10) << __func__ << " server is already active." << dendl;
        return ;
      }
      r = activate();
      assert(!r);
      r = infiniband->send_msg(cct, tcp_fd, my_msg);
      if (r < 0) {
        ldout(cct, 1) << __func__ << " server ack failed." << dendl;
        dispatcher->perf_logger->inc(l_msgr_rdma_handshake_errors);
        fault();
        return ;
      }
    } else { // ack from client
      connected = 1;
      ldout(cct, 10) << __func__ << " handshake of rdma is done. server connected: " << connected << dendl;
      //cleanup();
      submit(false);
      notify();
    }
  }
}



void handle_rdma_structures(const std::vector<uint32_t>& compl_rdma,
     std::vector<std::pair<uint32_t, Chunk*> >& inter_rec)

  ldout(cct, 20) << __func__ " need to append RDMA structures." << dendl;
  Mutex::Locker l(lock); // need a lock

  if(!compl_rdma.empty()) {
    // release RDMA memory
    
    for(const auto& reg_id : compl_rdma) {
    
      auto reg_itr = m_rdma_send_buf.find(reg_id);
      
      if(reg_itr != m_rdma_send_buf.end()) {
        ldout(cct, 10) << __func__ << " cannot find RDMA SEND region." << dendl;

        // iterate though the vector and release memory
        for(const auto& chunk : reg_itr->second) {
          dispatcher->post_chunk_to_pool(chunk);
        }
        
        // delete allocation of RDMA memory
        m_rdma_send_buf.erase(reg_itr);         
 
      }
 
    }
 
  }

  // handle out_of_order work requests to preserve data order
  if(!inter_rec.empty()) {
    // need to move to the preserved map
    
    for(auto&& out_order : inter_rec) {
      m_rdma_rec_buffers.insert(std::move(out_order));
    }
  }


ssize_t RDMAConnectedSocketImpl::read(char* buf, size_t len)
{
  uint64_t i = 0;
  int r = ::read(notify_fd, &i, sizeof(i));
  ldout(cct, 20) << __func__ << " notify_fd : " << i << " in " << my_msg.qpn << " r = " << r << dendl;
  
  if (!active) {
    ldout(cct, 1) << __func__ << " when ib not active. len: " << len << dendl;
    return -EAGAIN;
  }
  
  if (0 == connected) {
    ldout(cct, 1) << __func__ << " when ib not connected. len: " << len <<dendl;
    return -EAGAIN;
  }
  ssize_t read = 0;
  if (!buffers.empty())
    read = read_buffers(buf,len);

  std::vector<ibv_wc> cqe;
  get_wc(cqe);
  if (cqe.empty()) {
    if (!buffers.empty()) {
      notify();
    }
    if (read > 0) {
      return read;
    }
    if (error) {
      return -error;
    } else {
      return -EAGAIN;
    }
  }

  // for RDMA Read notifications
  std::vector<uint32_t> rdma_cmpls(cqe.size());

  // for buffering out of order works
  std::vector< std::pair<uint32_t, Chunk*> > rdma_out_of_order(cqe.size()); 

  bool out_of_order = false; // when to start buffering


  ldout(cct, 20) << __func__ << " poll queue got " << cqe.size() << " responses. QP: " << my_msg.qpn << dendl;
  for (size_t i = 0; i < cqe.size(); ++i) {
    ibv_wc* response = &cqe[i];
    assert(response->status == IBV_WC_SUCCESS);

    // need to check if IBV_WR_RDMA_WRITE_WITH_IMM
    Chunk* chunk = reinterpret_cast<Chunk *>(response->wr_id);
    if(response->opcode == IBV_WC_RECV_RDMA_WITH_IMM) {
      
      assert(!response->byte_len); // no data is sent       

      const uint32_t rdma_reg_id = ntohl(reponse->imm_data);    // need to retrieve host-based value
      ldout(cct, 25) << __func__ << "WRITE_IMM ID: " << static_cast<long>(rdma_reg_id) << dendl;

      rdma_cmpls.push_back(rdma_reg_id);
      // done with this chunk
      dispathcer->post_chunk_to_pool(chunk);
      continue;
    }

    ldout(cct, 25) << __func__ << " chunk length: " << response->byte_len << " bytes." << chunk << dendl;
    chunk->prepare_read(response->byte_len);
    worker->perf_logger->inc(l_msgr_rdma_rx_bytes, response->byte_len);
    if (response->byte_len == 0) {
      dispatcher->perf_logger->inc(l_msgr_rdma_rx_fin);
      if (connected) {
        error = ECONNRESET;
        ldout(cct, 20) << __func__ << " got remote close msg..." << dendl;
      }
      dispatcher->post_chunk_to_pool(chunk);
    } else {
      
      // means a proper work request
      // need to check the sequence number
      
 
     const uint32_t rec_sec = ntohl(response->imm_data);

     if(out_of_order || !rdma_is_seq_larger(rec_sec, m_rdma_recv_seq.load())) {
       out_of_order = true;
       // buffer data
       rdma_out_of_order.push_back(std::make_pair<uint32_t, Chunk*>(rec_sec, chunk));
       
       continue
     }

      // need to update recv_seq number
      m_rdma_recv_seq.fetch_add(response->byte_len);


      if (read == (ssize_t)len) {
        buffers.push_back(chunk);
        ldout(cct, 25) << __func__ << " buffers add a chunk: " << response->byte_len << dendl;
      } else if (read + response->byte_len > (ssize_t)len) {
        read += chunk->read(buf+read, (ssize_t)len-read);
        buffers.push_back(chunk);
        ldout(cct, 25) << __func__ << " buffers add a chunk: " << chunk->get_offset() << ":" << chunk->get_bound() << dendl;
      } else {
        read += chunk->read(buf+read, response->byte_len);
        dispatcher->post_chunk_to_pool(chunk);
      }
    }
  }

  worker->perf_logger->inc(l_msgr_rdma_rx_chunks, cqe.size());
  if (is_server && connected == 0) {
    ldout(cct, 20) << __func__ << " we do not need last handshake, QP: " << my_msg.qpn << " peer QP: " << peer_msg.qpn << dendl;
    connected = 1; //if so, we don't need the last handshake
    cleanup();
    submit(false);
  }

  // need to pass the data structures for the internal use
  if(!rdma_cmpls.empty() || !rdma_out_of_order.empty()) {
    // need to update internal data structures
    handle_rdma_structures(rdma_compls, rdma_out_of_order);    
  }

  if (!buffers.empty()) {
    notify();
  }

  if (read == 0 && error)
    return -error;
  return read == 0 ? -EAGAIN : read;
}

ssize_t RDMAConnectedSocketImpl::read_buffers(char* buf, size_t len)
{
  size_t read = 0, tmp = 0;
  auto c = buffers.begin();
  for (; c != buffers.end() ; ++c) {
    tmp = (*c)->read(buf+read, len-read);
    read += tmp;
    ldout(cct, 25) << __func__ << " this iter read: " << tmp << " bytes." << " offset: " << (*c)->get_offset() << " ,bound: " << (*c)->get_bound()  << ". Chunk:" << *c  << dendl;
    if ((*c)->over()) {
      dispatcher->post_chunk_to_pool(*c);
      ldout(cct, 25) << __func__ << " one chunk over." << dendl;
    }
    if (read == len) {
      break;
    }
  }

  if (c != buffers.end() && (*c)->over())
    ++c;
  buffers.erase(buffers.begin(), c);
  ldout(cct, 25) << __func__ << " got " << read  << " bytes, buffers size: " << buffers.size() << dendl;
  return read;
}

ssize_t RDMAConnectedSocketImpl::zero_copy_read(bufferptr &data)
{
  if (error)
    return -error;
  static const int MAX_COMPLETIONS = 16;
  ibv_wc wc[MAX_COMPLETIONS];
  ssize_t size = 0;

  ibv_wc*  response;
  Chunk* chunk;
  bool loaded = false;
  auto iter = buffers.begin();
  if (iter != buffers.end()) {
    chunk = *iter;
    // FIXME need to handle release
    // auto del = std::bind(&Chunk::post_srq, std::move(chunk), infiniband);
    buffers.erase(iter);
    loaded = true;
    size = chunk->bound;
  }

  std::vector<ibv_wc> cqe;
  get_wc(cqe);
  if (cqe.empty())
    return size == 0 ? -EAGAIN : size;

  ldout(cct, 20) << __func__ << " pool completion queue got " << cqe.size() << " responses."<< dendl;

  for (size_t i = 0; i < cqe.size(); ++i) {
    response = &wc[i];
    chunk = reinterpret_cast<Chunk*>(response->wr_id);
    chunk->prepare_read(response->byte_len);
    if (!loaded && i == 0) {
      // FIXME need to handle release
      // auto del = std::bind(&Chunk::post_srq, std::move(chunk), infiniband);
      size = chunk->bound;
      continue;
    }
    buffers.push_back(chunk);
    iter++;
  }

  if (size == 0)
    return -EAGAIN;
  return size;
}

ssize_t RDMAConnectedSocketImpl::send(bufferlist &bl, bool more)
{
  if (error) {
    if (!active)
      return -EPIPE;
    return -error;
  }
  size_t bytes = bl.length();
  if (!bytes)
    return 0;


  // check if object is greater then a threshold

  bool direct_RDMA = false; // if need direct RDMA operation 

  if(bytes >= RDMAConnectedSocketImpl::RDMA_DIRECT_THRESH) {
    direct_RDMA = true;
  } 


  {
    Mutex::Locker l(lock);
     
    
    // need to check if use RDMA READ
    if(direct_RDMA) {
      // Direct memory access (RDMA READ)
      bufferlist data_buff;
      data_buff.claim_append(bl);
      m_rdma_data.push_back(std::make_pair<char*, bufferlist>(pending_bl.buffers().back.raw_c_str(), std::move(data_buff)));
    }
    else {  
      pending_bl.claim_append(bl);
    }

    if (!connected) {

      ldout(cct, 20) << __func__ << " fake send to upper, QP: " << my_msg.qpn << dendl;
      return bytes;
    }
  }
  ldout(cct, 20) << __func__ << " QP: " << my_msg.qpn << dendl;


  
  ssize_t r = submit(more);
 
  if (r < 0 && r != -EAGAIN)
    return r;
  return bytes;
}

ssize_t RDMAConnectedSocketImpl::submit(bool more)
{
  if (error)
    return -error;
  Mutex::Locker l(lock);
  size_t bytes = pending_bl.length();
  ldout(cct, 20) << __func__ << " we need " << bytes << " bytes. iov size: "
                 << pending_bl.buffers().size() << dendl;
  if (!bytes)
    return 0;

  auto fill_tx_via_copy = [this](std::vector<Chunk*> &tx_buffers, unsigned bytes,
                                 std::list<bufferptr>::const_iterator &start,
                                 std::list<bufferptr>::const_iterator &end) -> unsigned {
    assert(start != end);
    auto chunk_idx = tx_buffers.size();
    int ret = worker->get_reged_mem(this, tx_buffers, bytes);
    if (ret == 0) {
      ldout(cct, 1) << __func__ << " no enough buffers in worker " << worker << dendl;
      worker->perf_logger->inc(l_msgr_rdma_tx_no_mem);
      return 0;
    }

    unsigned total_copied = 0;
    Chunk *current_chunk = tx_buffers[chunk_idx];
    while (start != end) {
      const uintptr_t addr = reinterpret_cast<const uintptr_t>(start->c_str());
      unsigned copied = 0;
      while (copied < start->length()) {
        uint32_t r = current_chunk->write((char*)addr+copied, start->length() - copied);
        copied += r;
        total_copied += r;
        bytes -= r;
        if (current_chunk->full()){
          if (++chunk_idx == tx_buffers.size())
            return total_copied;
          current_chunk = tx_buffers[chunk_idx];
        }
      }
      ++start;
    }
    assert(bytes == 0);
    return total_copied;
  };

  std::vector<Chunk*> tx_buffers;
  std::list<bufferptr>::const_iterator it = pending_bl.buffers().begin();
  std::list<bufferptr>::const_iterator copy_it = it;
  unsigned total = 0;
  unsigned need_reserve_bytes = 0;

  uint32_t rdma_seq_offset = 0
  bool     apply_rdma = false;
 

  while (it != pending_bl.buffers().end()) {
    if(!m_rdma_data.empty() && 
        m_rdma_data.front.first == itr->raw_c_str()) {
    
      // after this operation, RDMA Read waits
      apply_rdma = true; 
    }
  
    if (infiniband->is_tx_buffer(it->raw_c_str())) {
      if (need_reserve_bytes) {
        unsigned copied = fill_tx_via_copy(tx_buffers, need_reserve_bytes, copy_it, it);
        total += copied;
        if (copied < need_reserve_bytes) {
          if(apply_rdma) {
            // need to update key
            
          }
          goto sending;
        }
        need_reserve_bytes = 0;
      }
      assert(copy_it == it);
      tx_buffers.push_back(infiniband->get_tx_chunk_by_buffer(it->raw_c_str()));
      ++rdma_seq_offset;
      total += it->length();
      ++copy_it;
    } else {
      need_reserve_bytes += it->length();
    }
    
    if(apply_rdma)
    {
      apply_rdma = false; 
      // set sequence number for this read
      
       
      perform_rdma_read(rdma_seq_offset); 
    }

    ++it;
  }
  if (need_reserve_bytes)
    total += fill_tx_via_copy(tx_buffers, need_reserve_bytes, copy_it, it);

 sending:
  if (total == 0)
    return -EAGAIN;
  assert(total <= pending_bl.length());
  bufferlist swapped;
  if (total < pending_bl.length()) {
    worker->perf_logger->inc(l_msgr_rdma_tx_parital_mem);
    pending_bl.splice(total, pending_bl.length()-total, &swapped);
    pending_bl.swap(swapped);
  } else {
    pending_bl.clear();
  }

  ldout(cct, 20) << __func__ << " left bytes: " << pending_bl.length() << " in buffers "
                 << pending_bl.buffers().size() << " tx chunks " << tx_buffers.size() << dendl;

  int r = post_work_request(tx_buffers);
  if (r < 0)
    return r;

  ldout(cct, 20) << __func__ << " finished sending " << bytes << " bytes." << dendl;
  return pending_bl.length() ? -EAGAIN : 0;
}


ssize_t RDMAConnectedSocketImpl::perform_rdma_read() {
  


  rdma_remote_region_d read_request;
  read_request.rdma_tag = RDMA_REQUEST_TAG;
  read_request.uq_key  = 0;
  read_request.rlength = 0;
  read_request.rkey    = 0; // Chunk key since they all share one  
                            // region

  read_request.raddr   = 0;  // Chunk address

  const bool result = send_rdma_control(&read_request);

  return ((result) ? 0 : -1);

}

bool RDMAConectedSocketImpl::send_rdma_control(const struct rdma_msg_d* msg){

  assert(tcp_fd >= 0);

  switch(msg->rdma_tag) {
    case RDMAConnectedSocketImpl::RDMA_REQUEST_TAG: {
    
      // cast message
      const struct rdma_remote_region_d* ptr = static_cast<const struct rdma_remote_region_d*>(msg);

      if(!ptr) { // casting error
        return false;
      }

      // need to send message
      char control_msg[RDMA_CONTROL_MSG];     


      const uint32_t net_uq_key   =  htonl(ptr->uq_key);
      const uint32_t net_rlength  =  htonl(ptr->rlength);
      const uint32_t net_rkey     =  htonl(ptr->rkey);
      
      const uint32_t upper_bits   = (uint32_t)((ptr->raddr & 0xFFFFFFFF00000000) >> 32);
 
      const uint32_t lower_bits   = (uint32_t)(ptr->raddr & 0x00000000FFFFFFFF);

      const uint32_t net_upper    = htonl(upper_bits);
      const uint32_t net_lower    = htonl(lower_bits);


      sprintf(control_msg, "%08x:%08x:%08x:%08x:%08x", 
              net_uq_key, net_rlength, net_key, net_addr,
              net_upper, net_lower); 

      
       ssize_t r;
       size_t cur_ptr = 0;
   

       size_t max_write =  sizeof(control_msg);
   
       retry:
       r = ::write(tcp_fd, (control_msg + cur_ptr), (max_write - cur_ptr));
       
       if ((size_t)r != (max_write - cur_ptr)) {
         if(r <= 0) {
           return false;
         }
         
         cur_ptr += (size_t) r;  
         goto retry;
       }

       return true
      
    }

    default: {return false;} 

  }

  return false;
  
}

bool RDMAConnectedSocketImpl::handle_rdma_control() {
  // read

  assert(tcp_fd >= 0);

  char rdma_control_msg[RDMA_CONTROL_MSG];
  
  size_t cur_ptr = 0;
  const size_t max_msg_read = sizeof(rdma_control_msg);
  
  ssize_t r;

  while(cur_ptr != max_msg_read) {
    r = ::read(tcp_fd, &rdma_control_msg, (max_msg_read - cur_ptr));
    
    if(r <= 0) {
      return false;
    }  

    cur_ptr += (size_t) r;
  }

  // read the received message
  uint32_t net_uq_key, net_rlength, net_rkey, net_upper, net_lower;

  sscanf(rdma_control_message, "%x:%x:%x:%x:%x", 
        &net_uq_key, &net_rlength, &net_rkey, 
        &net_upper, &net_upper);

  rdma_remote_region_d temp_remote;
  temp_remote.uq_key   =  ntohl(net_uq_key);
  temp_remote.rlength  =  ntohl(net_rlength);
  temp_remote.rkey     =  ntohl(net_rkey);
  
  const uint32_t host_upper = ntohl(net_upper);
  const uint32_t host_lower = ntohl(net_lower);
  
  temp_remote.raddr  = ((uint64_t) host_upper << 32) & 0xFFFFFFFF00000000;
  temp_remote.raddr != ((uint64_t host_lower) & 0x00000000FFFFFFFF);

  // enqueue request or something


  
  return true; // read message

}


int RDMAConnectedSocketImpl::post_work_request(std::vector<Chunk*> &tx_buffers)
{
  ldout(cct, 20) << __func__ << " QP: " << my_msg.qpn << " " << tx_buffers[0] << dendl;
  vector<Chunk*>::iterator current_buffer = tx_buffers.begin();
  ibv_sge isge[tx_buffers.size()];
  uint32_t current_sge = 0;
  ibv_send_wr iswr[tx_buffers.size()];
  uint32_t current_swr = 0;
  ibv_send_wr* pre_wr = NULL;
  uint32_t num = 0; 

  memset(iswr, 0, sizeof(iswr));
  memset(isge, 0, sizeof(isge));
 
  while (current_buffer != tx_buffers.end()) {
    isge[current_sge].addr = reinterpret_cast<uint64_t>((*current_buffer)->buffer);
    isge[current_sge].length = (*current_buffer)->get_offset();
    isge[current_sge].lkey = (*current_buffer)->mr->lkey;
    ldout(cct, 25) << __func__ << " sending buffer: " << *current_buffer << " length: " << isge[current_sge].length  << dendl;

    iswr[current_swr].wr_id = reinterpret_cast<uint64_t>(*current_buffer);
    iswr[current_swr].next = NULL;
    iswr[current_swr].sg_list = &isge[current_sge];
    iswr[current_swr].num_sge = 1;
    iswr[current_swr].opcode = IBV_WR_SEND;
    iswr[current_swr].send_flags = IBV_SEND_SIGNALED;
    
    // add send sequence to that the receiver could deliver
    // data to the client in order.
    // makes easier to use direct RDMA operations
    iswr[current_swr].imm_data = htonl(m_rdma_send_seq); 
    // update seqeuence number
    if(m_rdma_send_seq == 0xFFFFFFFF) {
      // reset sequene number
      m_rdma_send_seq = 0;
    }
    else { 
      ++m_rdma_send_seq;
    }
 
    /*if (isge[current_sge].length < infiniband->max_inline_data) {
      iswr[current_swr].send_flags = IBV_SEND_INLINE;
      ldout(cct, 20) << __func__ << " send_inline." << dendl;
      }*/

    num++;
    worker->perf_logger->inc(l_msgr_rdma_tx_bytes, isge[current_sge].length);
    if (pre_wr)
      pre_wr->next = &iswr[current_swr];
    pre_wr = &iswr[current_swr];
    ++current_sge;
    ++current_swr;
    ++current_buffer;
  }

  ibv_send_wr *bad_tx_work_request;
  if (ibv_post_send(qp->get_qp(), iswr, &bad_tx_work_request)) {
    ldout(cct, 1) << __func__ << " failed to send data"
                  << " (most probably should be peer not ready): "
                  << cpp_strerror(errno) << dendl;
    worker->perf_logger->inc(l_msgr_rdma_tx_failed);
    return -errno;
  }
  qp->add_tx_wr(num);
  worker->perf_logger->inc(l_msgr_rdma_tx_chunks, tx_buffers.size());
  ldout(cct, 20) << __func__ << " qp state is " << Infiniband::qp_state_string(qp->get_state()) << dendl;
  return 0;
}

void RDMAConnectedSocketImpl::fin() {
  ibv_send_wr wr;
  memset(&wr, 0, sizeof(wr));
  wr.wr_id = reinterpret_cast<uint64_t>(qp);
  wr.num_sge = 0;
  wr.opcode = IBV_WR_SEND;
  wr.send_flags = IBV_SEND_SIGNALED;
  ibv_send_wr* bad_tx_work_request;
  if (ibv_post_send(qp->get_qp(), &wr, &bad_tx_work_request)) {
    ldout(cct, 1) << __func__ << " failed to send message="
                  << " ibv_post_send failed(most probably should be peer not ready): "
                  << cpp_strerror(errno) << dendl;
    worker->perf_logger->inc(l_msgr_rdma_tx_failed);
    return ;
  }
  qp->add_tx_wr(1);
}

void RDMAConnectedSocketImpl::cleanup() {
  if (con_handler && tcp_fd >= 0) {
    (static_cast<C_handle_connection*>(con_handler))->close();
    worker->center.submit_to(worker->center.get_id(), [this]() {
      worker->center.delete_file_event(tcp_fd, EVENT_READABLE);
    }, false);
    delete con_handler;
    con_handler = nullptr;
  }
}

void RDMAConnectedSocketImpl::notify()
{
  // note: notify_fd is an event fd (man eventfd)
  // write argument must be a 64bit integer
  uint64_t i = 1;

  assert(sizeof(i) == write(notify_fd, &i, sizeof(i)));
}

void RDMAConnectedSocketImpl::shutdown()
{
  if (!error)
    fin();
  error = ECONNRESET;
  active = false;
}

void RDMAConnectedSocketImpl::close()
{
  if (!error)
    fin();
  error = ECONNRESET;
  active = false;
}

void RDMAConnectedSocketImpl::fault(const struct ibv_wc* const cperr)
{

  // check if it's a non-null
  if(cperr && cperr->opcode == IBV_WC_RDMA_READ) {
  
    ldout(cct, 1) << __func__ " RDMA READ failed." << dendl;
 
    // RDMA read failed
    // need to release memory for reading
    Mutex::Locker l(m_rdma_read_lock);
    
    // get the address to release
    const uint64_t laddr = cperr->wr_id;
    auto itr_del = m_rdma_read_buf.find(laddr);
    
    if(itr_del != m_rdma_rdma_read_buf.end()) {
      // move this region to the delete vectors
      m_free_rdma_bufs.push_back(
          std::move(itr_del->second));

      // delete the memory region from the registered map
      m_rdma_read_buf.erase(itr_del);
    }
    else {
      ldout(cct, 10) << __func__ << 
          " RDMA READ buffer has not been preallocated" << dendl;
    }//else
      
  }//if


  ldout(cct, 1) << __func__ << " tcp fd " << tcp_fd << dendl;
  /*if (qp) {
    qp->to_dead();
    qp = NULL;
    }*/
  error = ECONNRESET;
  connected = 1;
  notify();
}

void RDMAConnectedSocketImpl::set_accept_fd(int sd)
{
  tcp_fd = sd;
  is_server = true;
  worker->center.submit_to(worker->center.get_id(), [this]() {
			   worker->center.create_file_event(tcp_fd, EVENT_READABLE, con_handler);
			   }, true);
}
