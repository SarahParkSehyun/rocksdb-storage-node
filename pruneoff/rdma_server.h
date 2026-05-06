#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <infiniband/verbs.h>
#include <rdma/rdma_cma.h>

#include "pruneoff/bloom_evaluator.h"
#include "pruneoff/message_types.h"
#include "pruneoff/thread_pool.h"

namespace pruneoff {

class RDMAServer {
 public:
  struct Options {
    std::string bind_address = "0.0.0.0";
    uint16_t port = 7471;
    std::string db_path;
    size_t worker_threads = 8;
    size_t worker_threads_per_connection = 0;
    size_t recv_pool_depth = 256;
    size_t send_pool_depth = 256;
    std::vector<int> poller_cpus;
  };

  explicit RDMAServer(Options options);
  ~RDMAServer();

  RDMAServer(const RDMAServer&) = delete;
  RDMAServer& operator=(const RDMAServer&) = delete;

  bool Start();
  void Stop();

 private:
  struct BufferSlot {
    void* addr = nullptr;
    size_t index = 0;
  };

  struct Connection {
    ~Connection();

    rdma_cm_id* id = nullptr;
    ibv_pd* pd = nullptr;
    ibv_cq* cq = nullptr;
    ibv_qp* qp = nullptr;
    ibv_mr* recv_mr = nullptr;
    ibv_mr* send_mr = nullptr;
    void* recv_pool = nullptr;
    void* send_pool = nullptr;
    size_t recv_pool_depth = 0;
    size_t send_pool_depth = 0;
    std::mutex send_mutex;
    std::condition_variable send_cv;
    std::vector<size_t> free_send_slots;
    std::unique_ptr<ThreadPool> workers;
    std::thread poller;
    std::atomic<bool> running{false};
    int poller_cpu = -1;
    RDMAServer* server = nullptr;
  };

  struct WorkItem {
    std::shared_ptr<Connection> connection;
    PruneOffRequest request;
  };

  bool SetupListener();
  void EventLoop();
  bool HandleConnectRequest(rdma_cm_id* id);
  void HandleDisconnect(rdma_cm_id* id);
  bool InitConnection(const std::shared_ptr<Connection>& connection);
  void DestroyConnection(const std::shared_ptr<Connection>& connection);
  void PollLoop(std::shared_ptr<Connection> connection);
  void DispatchRecv(const std::shared_ptr<Connection>& connection,
                    size_t recv_slot);
  void EvaluateAndSend(WorkItem item);
  bool PostRecv(const std::shared_ptr<Connection>& connection, size_t slot);
  bool PostSend(const std::shared_ptr<Connection>& connection, size_t slot);
  bool AcquireSendSlot(const std::shared_ptr<Connection>& connection,
                       size_t* slot);
  void ReleaseSendSlot(const std::shared_ptr<Connection>& connection,
                       size_t slot);
  int NextPollerCpu();

  static uintptr_t RecvWrId(size_t slot);
  static uintptr_t SendWrId(size_t slot);
  static bool IsSendWrId(uintptr_t wr_id);
  static size_t SlotFromWrId(uintptr_t wr_id);
  static bool PinCurrentThreadToCpu(int cpu);

  Options options_;
  BloomEvaluator evaluator_;
  ThreadPool workers_;
  std::atomic<bool> running_{false};
  rdma_event_channel* event_channel_ = nullptr;
  rdma_cm_id* listener_ = nullptr;
  std::thread event_thread_;
  std::mutex connections_mutex_;
  std::vector<std::shared_ptr<Connection>> connections_;
  size_t next_poller_cpu_index_ = 0;
};

}  // namespace pruneoff
