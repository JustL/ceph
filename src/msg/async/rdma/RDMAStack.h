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

#ifndef CEPH_MSG_RDMASTACK_H
#define CEPH_MSG_RDMASTACK_H

#include <sys/eventfd.h>

#include <list>
#include <vector>
#include <map>
#include <utility>
#include <thread>

#include "common/ceph_context.h"
#include "common/debug.h"
#include "common/errno.h"
#include "msg/async/Stack.h"
#include "Infiniband.h"

class RDMAConnectedSocketImpl;
class RDMAServerSocketImpl;
class RDMAStack;
class RDMAWorker;

class RDMADispatcher {
  typedef Infiniband::MemoryManager::Chunk Chunk;
  typedef Infiniband::QueuePair QueuePair;

  std::thread t;
  CephContext *cct;
  Infiniband::CompletionQueue* tx_cq = nullptr;
  Infiniband::CompletionQueue* rx_cq = nullptr;
  Infiniband::CompletionChannel *tx_cc = nullptr, *rx_cc = nullptr;
  EventCallbackRef async_handler;
  bool done = false;
  std::atomic<uint64_t> num_dead_queue_pair = {0};
  std::atomic<uint64_t> num_qp_conn = {0};
  int post_backlog = 0;
  Mutex lock; // protect `qp_conns`, `dead_queue_pairs`
  // qp_num -> InfRcConnection
  // The main usage of `qp_conns` is looking up connection by qp_num,
  // so the lifecycle of element in `qp_conns` is the lifecycle of qp.
  //// make qp queue into dead state
  /**
   * 1. Connection call mark_down
   * 2. Move the Queue Pair into the Error state(QueuePair::to_dead)
   * 3. Wait for the affiliated event IBV_EVENT_QP_LAST_WQE_REACHED(handle_async_event)
   * 4. Wait for CQ to be empty(handle_tx_event)
   * 5. Destroy the QP by calling ibv_destroy_qp()(handle_tx_event)
   *
   * @param qp The qp needed to dead
   */
  ceph::unordered_map<uint32_t, std::pair<QueuePair*, RDMAConnectedSocketImpl*> > qp_conns;

  /// if a queue pair is closed when transmit buffers are active
  /// on it, the transmit buffers never get returned via tx_cq.  To
  /// work around this problem, don't delete queue pairs immediately. Instead,
  /// save them in this vector and delete them at a safe time, when there are
  /// no outstanding transmit buffers to be lost.
  std::vector<QueuePair*> dead_queue_pairs;

  std::atomic<uint64_t> num_pending_workers = {0};
  Mutex w_lock; // protect pending workers
  // fixme: lockfree
  std::list<RDMAWorker*> pending_workers;
  RDMAStack* stack;

  class C_handle_cq_async : public EventCallback {
    RDMADispatcher *dispatcher;
   public:
    C_handle_cq_async(RDMADispatcher *w): dispatcher(w) {}
    void do_request(uint64_t fd) {
      // worker->handle_tx_event();
      dispatcher->handle_async_event();
    }
  };

 public:
  PerfCounters *perf_logger;

  explicit RDMADispatcher(CephContext* c, RDMAStack* s);
  virtual ~RDMADispatcher();
  void handle_async_event();

  void polling_start();
  void polling_stop();
  void polling();
  int register_qp(QueuePair *qp, RDMAConnectedSocketImpl* csi);
  void make_pending_worker(RDMAWorker* w) {
    Mutex::Locker l(w_lock);
    auto it = std::find(pending_workers.begin(), pending_workers.end(), w);
    if (it != pending_workers.end())
      return;
    pending_workers.push_back(w);
    ++num_pending_workers;
  }
  RDMAStack* get_stack() { return stack; }
  RDMAConnectedSocketImpl* get_conn_lockless(uint32_t qp);
  QueuePair* get_qp(uint32_t qp);
  void erase_qpn_lockless(uint32_t qpn);
  void erase_qpn(uint32_t qpn);
  Infiniband::CompletionQueue* get_tx_cq() const { return tx_cq; }
  Infiniband::CompletionQueue* get_rx_cq() const { return rx_cq; }
  void notify_pending_workers();
  void handle_tx_event(ibv_wc *cqe, int n);
  void post_tx_buffer(std::vector<Chunk*> &chunks);

  std::atomic<uint64_t> inflight = {0};

  void post_chunk_to_pool(Chunk* chunk); 

};

class RDMAWorker : public Worker {
  typedef Infiniband::CompletionQueue CompletionQueue;
  typedef Infiniband::CompletionChannel CompletionChannel;
  typedef Infiniband::MemoryManager::Chunk Chunk;
  typedef Infiniband::MemoryManager MemoryManager;
  typedef std::vector<Chunk*>::iterator ChunkIter;
  RDMAStack *stack;
  EventCallbackRef tx_handler;
  std::list<RDMAConnectedSocketImpl*> pending_sent_conns;
  RDMADispatcher* dispatcher = nullptr;
  Mutex lock;

  class C_handle_cq_tx : public EventCallback {
    RDMAWorker *worker;
    public:
    C_handle_cq_tx(RDMAWorker *w): worker(w) {}
    void do_request(uint64_t fd) {
      worker->handle_pending_message();
    }
  };

 public:
  PerfCounters *perf_logger;
  explicit RDMAWorker(CephContext *c, unsigned i);
  virtual ~RDMAWorker();
  virtual int listen(entity_addr_t &addr, const SocketOptions &opts, ServerSocket *) override;
  virtual int connect(const entity_addr_t &addr, const SocketOptions &opts, ConnectedSocket *socket) override;
  virtual void initialize() override;
  RDMAStack *get_stack() { return stack; }
  int get_reged_mem(RDMAConnectedSocketImpl *o, std::vector<Chunk*> &c, size_t bytes);
  void remove_pending_conn(RDMAConnectedSocketImpl *o) {
    assert(center.in_thread());
    pending_sent_conns.remove(o);
  }
  void handle_pending_message();
  void set_stack(RDMAStack *s) { stack = s; }
  void notify_worker() {
    center.dispatch_event_external(tx_handler);
  }
};

class RDMAConnectedSocketImpl : public ConnectedSocketImpl {
 public:
  typedef Infiniband::MemoryManager::Chunk Chunk;
  typedef Infiniband::CompletionChannel CompletionChannel;
  typedef Infiniband::CompletionQueue CompletionQueue;

 private:
  CephContext *cct;
  Infiniband::QueuePair *qp;
  IBSYNMsg peer_msg;
  IBSYNMsg my_msg;
  int connected;
  int error;
  Infiniband* infiniband;
  RDMADispatcher* dispatcher;
  RDMAWorker* worker;
  std::vector<Chunk*> buffers;
  int notify_fd = -1;
  bufferlist pending_bl;

  Mutex lock;
  std::vector<ibv_wc> wc;
  bool is_server;
  EventCallbackRef con_handler;
  int tcp_fd = -1;
  bool active;// qp is active ?
  bool pending;


  // RDMA stuff

  // default RDMA switching threshold. If the configuration
  // does not contain a threshold value, sockets use the below
  // threshold for applying RDMA direct operations.
  
  /* 
   * RDMA Direct operations are motivated by the fact
   * that having multiple readers reading the same RDMA region
   * (replication case) or a few simultaneous readers (erasure coding)
   * is better than one entity handling all data transmission.
   * Of course, adding such  a functionality to higher layers
   * of the NetworkStack may result in better performance, but
   * now let's just focus on the RDMA part (socket). 
   **/
 
  static const size_t RDMA_DIRECT_THRESH = 32 * 1024; // 32KB 
  
  static const uint8_t RDMA_REQUEST_TAG = 0x01;
  static const uint8_t RDMA_ACK_TAG     = 0x02;

  /**
   * Generic RDMA interface.
   * All RDMA messages inherit this interface
   */
  struct rdma_msg_d {
    uint8_t rdma_tag;
  };

  /*
   * Structure used for RDMA direct memory
   * verbs (WRITE, WRITE_IMM, READ). 
   **/

  struct rdma_remote_region_d : public rdma_msg_d {
    uint32_t uq_key;  // unique rdma key 
    uint32_t rlength; 
    uint32_t rkey;  
    uint64_t raddr;
  };
  
  
  /**
   * RDNA Ack structure to acknoweledge RDMA
   * completed RDMA operations
   */
  struct rdma_ack_d : public rdma_msg_d {
    uint8_t  res_code
    uint32_t rkey;
    uint64_t raddr; 
  };
 
 
  // allocation map to know which rdma regions are allocated
  // for remote reading
  
    
  // lock for modyfing local memory used for RDMA read buffers
  Mutex m_rdma_read_lock; 
  
  
  // for ensuring that data to the user is delivered in order
  uint32_t  m_rdma_send_seq; // for sending sequence unit - work request 
                             // number(no need for atomic)
 
  // recv must be atomic
  std::atomic<uint32_t>  m_rdma_recv_seq; // for receiving sequence number
                                          // unit -- work request


  // need to ensure that data always delivered in order
  std::map<uint32_t, Chunk*, std::function<bool(const uint32_t&, const uint64_t&)> > m_rdma_rec_buffers;
    
  // for placing error/complete buffers
  std::vector<std::pair<uint32_t, std::vector<Chunk*> > m_free_rdma_bufs;
 
  // for storing state of currently happening RDMA operations
  ceph::unordered_map<uint32_t, std::vector<Chunk*> > m_rdma_send_buf;
  std::vector<std::pair<char*, bufferlist> > m_rdma_data;  
 

  ceph::unordered_map<uint64_t, std::pair<uint32_t, std::vector<Chunk*> > > m_rdma_read_buf;  
 

  /**
   * Function takes an RDMA control message and 
   * sends it to the remote host.
   */ 
  bool send_rdma_control(const struct rdma_msg_d* msg);
 

  void perform_rdma_read();
  bool handle_rdma_control();  


  /**
   * Function determines if seq1 is larger than seq2.
   * 
   * Function is provided by HKUST-SING laboratory:
   * can be found at: hithub.com/HKUST-SING/PIAS-Software/
   */
  bool rdma_is_seq_larger(const uint32_t seq1, const uint32_t seq2) {
   
    static const uint32_t SEQ_OVERFLOW_VALUE = 4294900000;

    if((seq1 > seq2) && (seq1 - seq2 <= SEQ_OVERFLOW_VALUE)) {
      return true;
    }
    
    if((seq1 < seq2) && (seq2 - seq1 > SEQ_OVERFLOW_VALUE)) {
      return true;
    }

    return false;    
  } 

  /**
   * Function returns lower bound of sequence numbers for
   * the passed value. Follows TCP logic that as long as
   * the window is (1/2) NUmber of seqeunce numbers it works.
   * 
   * @return : lower bound of a window (inclusive window) -->
    *          return value cannot be used -- current window value
   */
  uint32_t get_lower_seq_number(uint32_t seq_num) {
    // MAX_SEQ_NUMBERS = 2^32 = 4294967296
    // MAX_WINDOW_SIZE = 2^31 = 2147483648
    
    if(seq_num >= 2147483648) {
      return (seq_num - 2147483647);
    }
    
    else {
      return (seq_num + 2147483647);
    }

  }

  /** 
   * Receive signal to release memory from a remote host
   *
   */ 
  void handle_rdma_structures(const std::vector<uint32_t>& send_buff,
  std::vector<std::pair<uint32_t, Chunk*> >& inter_rec);  

  void notify();
  ssize_t read_buffers(char* buf, size_t len);
  int post_work_request(std::vector<Chunk*>&);

 public:
  RDMAConnectedSocketImpl(CephContext *cct, Infiniband* ib, RDMADispatcher* s,
                          RDMAWorker *w);
  virtual ~RDMAConnectedSocketImpl();

  void pass_wc(std::vector<ibv_wc> &&v);
  void get_wc(std::vector<ibv_wc> &w);
  virtual int is_connected() override { return connected; }

  virtual ssize_t read(char* buf, size_t len) override;
  virtual ssize_t zero_copy_read(bufferptr &data) override;
  virtual ssize_t send(bufferlist &bl, bool more) override;
  virtual void shutdown() override;
  virtual void close() override;
  virtual int fd() const override { return notify_fd; }
  void fault(const struct ibv_wc* const cperr = nullptr);
  const char* get_qp_state() { return Infiniband::qp_state_string(qp->get_state()); }
  ssize_t submit(bool more);
  int activate();
  void fin();
  void handle_connection();
  void cleanup();
  void set_accept_fd(int sd);
  int try_connect(const entity_addr_t&, const SocketOptions &opt);
  bool is_pending() {return pending;}
  void set_pending(bool val) {pending = val;}
  class C_handle_connection : public EventCallback {
    RDMAConnectedSocketImpl *csi;
    bool active;
   public:
    C_handle_connection(RDMAConnectedSocketImpl *w): csi(w), active(true) {}
    void do_request(uint64_t fd) {
      if (active)
        csi->handle_connection();
    }
    void close() {
      active = false;
    }
  };
  
};

class RDMAServerSocketImpl : public ServerSocketImpl {
  CephContext *cct;
  NetHandler net;
  int server_setup_socket;
  Infiniband* infiniband;
  RDMADispatcher *dispatcher;
  RDMAWorker *worker;
  entity_addr_t sa;

 public:
  RDMAServerSocketImpl(CephContext *cct, Infiniband* i, RDMADispatcher *s, RDMAWorker *w, entity_addr_t& a);

  int listen(entity_addr_t &sa, const SocketOptions &opt);
  virtual int accept(ConnectedSocket *s, const SocketOptions &opts, entity_addr_t *out, Worker *w) override;
  virtual void abort_accept() override;
  virtual int fd() const override { return server_setup_socket; }
  int get_fd() { return server_setup_socket; }
};

class RDMAStack : public NetworkStack {
  vector<std::thread> threads;
  PerfCounters *perf_counter;
  Infiniband ib;
  RDMADispatcher dispatcher;

  std::atomic<bool> fork_finished = {false};

 public:
  explicit RDMAStack(CephContext *cct, const string &t);
  virtual ~RDMAStack();
  virtual bool support_zero_copy_read() const override { return false; }
  virtual bool nonblock_connect_need_writable_event() const { return false; }

  virtual void spawn_worker(unsigned i, std::function<void ()> &&func) override;
  virtual void join_worker(unsigned i) override;
  RDMADispatcher &get_dispatcher() { return dispatcher; }
  Infiniband &get_infiniband() { return ib; }
  virtual bool is_ready() override { return fork_finished.load(); };
  virtual void ready() override { fork_finished = true; };
};


#endif
