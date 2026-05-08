#include "pruneoff/rdma_server.h"

#include <arpa/inet.h>
#include <netdb.h>
#include <pthread.h>
#include <poll.h>
#include <sys/socket.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstddef>
#include <cstring>
#include <iostream>

#include "rocksdb/slice.h"

namespace pruneoff {

namespace {

constexpr int kPollBatch = 32;
constexpr size_t kSendSignalInterval = 32;
constexpr size_t kMaxIdlePauseCount = 64;
constexpr uintptr_t kSendWrMask = uintptr_t{1} << (sizeof(uintptr_t) * 8 - 1);

void* SlotAddress(void* base, size_t slot, size_t slot_size) {
  return static_cast<char*>(base) + slot * slot_size;
}

size_t PruneOffResponseSerializedSize(uint32_t num_filter_blocks) {
  const uint32_t clamped =
      std::min(num_filter_blocks, kPruneOffMaxCandidates);
  return offsetof(PruneOffResponse, filter_blocks) +
         clamped * sizeof(FilterBlockEntry);
}

}  // namespace

RDMAServer::RDMAServer(Options options)
    : options_(std::move(options)),
      evaluator_(options_.db_path),
      workers_(options_.worker_threads_per_connection == 0
                   ? options_.worker_threads
                   : 0) {}

RDMAServer::~RDMAServer() { Stop(); }

RDMAServer::Connection::~Connection() {
  running = false;
  send_cv.notify_all();
  if (poller.joinable()) {
    if (poller.get_id() == std::this_thread::get_id()) {
      poller.detach();
    } else {
      poller.join();
    }
  }
  if (id != nullptr && qp != nullptr) {
    rdma_destroy_qp(id);
    qp = nullptr;
  }
  if (recv_mr != nullptr) {
    ibv_dereg_mr(recv_mr);
    recv_mr = nullptr;
  }
  if (send_mr != nullptr) {
    ibv_dereg_mr(send_mr);
    send_mr = nullptr;
  }
  if (send_cq != nullptr) {
    ibv_destroy_cq(send_cq);
    send_cq = nullptr;
  }
  if (recv_cq != nullptr) {
    ibv_destroy_cq(recv_cq);
    recv_cq = nullptr;
  }
  if (pd != nullptr) {
    ibv_dealloc_pd(pd);
    pd = nullptr;
  }
  if (id != nullptr) {
    rdma_destroy_id(id);
    id = nullptr;
  }
  free(recv_pool);
  recv_pool = nullptr;
  free(send_pool);
  send_pool = nullptr;
}

bool RDMAServer::Start() {
  if (running_.exchange(true)) {
    return true;
  }
  if (!SetupListener()) {
    running_ = false;
    return false;
  }
  event_thread_ = std::thread([this] { EventLoop(); });
  std::cerr << "PruneOff RDMA server listening on " << options_.bind_address
            << ":" << options_.port << ", db_path=" << options_.db_path
            << "\n";
  return true;
}

void RDMAServer::Stop() {
  if (!running_.exchange(false)) {
    return;
  }

  if (event_thread_.joinable()) {
    event_thread_.join();
  }
  if (listener_ != nullptr) {
    rdma_destroy_id(listener_);
    listener_ = nullptr;
  }
  if (event_channel_ != nullptr) {
    rdma_destroy_event_channel(event_channel_);
    event_channel_ = nullptr;
  }

  std::vector<std::shared_ptr<Connection>> connections;
  {
    std::lock_guard<std::mutex> lock(connections_mutex_);
    connections.swap(connections_);
  }
  for (const auto& connection : connections) {
    DestroyConnection(connection);
  }
  workers_.Stop();
}

bool RDMAServer::SetupListener() {
  event_channel_ = rdma_create_event_channel();
  if (event_channel_ == nullptr) {
    std::cerr << "rdma_create_event_channel failed: " << std::strerror(errno)
              << "\n";
    return false;
  }

  if (rdma_create_id(event_channel_, &listener_, nullptr, RDMA_PS_TCP) != 0) {
    std::cerr << "rdma_create_id failed: " << std::strerror(errno) << "\n";
    return false;
  }

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(options_.port);
  if (inet_pton(AF_INET, options_.bind_address.c_str(), &addr.sin_addr) != 1) {
    std::cerr << "invalid bind address: " << options_.bind_address << "\n";
    return false;
  }

  if (rdma_bind_addr(listener_, reinterpret_cast<sockaddr*>(&addr)) != 0) {
    std::cerr << "rdma_bind_addr failed: " << std::strerror(errno) << "\n";
    return false;
  }
  if (rdma_listen(listener_, 16) != 0) {
    std::cerr << "rdma_listen failed: " << std::strerror(errno) << "\n";
    return false;
  }
  return true;
}

void RDMAServer::EventLoop() {
  while (running_) {
    pollfd pfd{};
    pfd.fd = event_channel_->fd;
    pfd.events = POLLIN;

    const int poll_result = poll(&pfd, 1, 100);
    if (!running_) {
      break;
    }
    if (poll_result < 0) {
      if (errno == EINTR) {
        continue;
      }
      std::cerr << "poll RDMA CM event channel failed: "
                << std::strerror(errno) << "\n";
      break;
    }
    if (poll_result == 0) {
      continue;
    }
    if ((pfd.revents & (POLLERR | POLLHUP | POLLNVAL)) != 0) {
      if (running_) {
        std::cerr << "RDMA CM event channel closed unexpectedly\n";
      }
      break;
    }
    if ((pfd.revents & POLLIN) == 0) {
      continue;
    }

    rdma_cm_event* event = nullptr;
    if (rdma_get_cm_event(event_channel_, &event) != 0) {
      if (running_) {
        std::cerr << "rdma_get_cm_event failed: " << std::strerror(errno)
                  << "\n";
      }
      break;
    }

    rdma_cm_event event_copy = *event;
    rdma_ack_cm_event(event);

    switch (event_copy.event) {
      case RDMA_CM_EVENT_CONNECT_REQUEST:
        HandleConnectRequest(event_copy.id);
        break;
      case RDMA_CM_EVENT_DISCONNECTED:
        HandleDisconnect(event_copy.id);
        break;
      case RDMA_CM_EVENT_ESTABLISHED:
        std::cerr << "PruneOff RDMA connection established\n";
        break;
      default:
        std::cerr << "PruneOff RDMA CM event: "
                  << rdma_event_str(event_copy.event) << "\n";
        break;
    }
  }
}

bool RDMAServer::HandleConnectRequest(rdma_cm_id* id) {
  auto connection = std::make_shared<Connection>();
  connection->id = id;
  connection->server = this;
  connection->recv_pool_depth = options_.recv_pool_depth;
  connection->send_pool_depth = options_.send_pool_depth;
  id->context = connection.get();

  if (!InitConnection(connection)) {
    rdma_reject(id, nullptr, 0);
    DestroyConnection(connection);
    return false;
  }

  rdma_conn_param param{};
  param.initiator_depth = 1;
  param.responder_resources = 1;
  param.retry_count = 7;
  param.rnr_retry_count = 7;

  if (rdma_accept(id, &param) != 0) {
    std::cerr << "rdma_accept failed: " << std::strerror(errno) << "\n";
    DestroyConnection(connection);
    return false;
  }

  if (options_.worker_threads_per_connection > 0) {
    connection->workers =
        std::make_unique<ThreadPool>(options_.worker_threads_per_connection);
  }
  connection->poller_cpu = NextPollerCpu();
  connection->running = true;
  connection->poller = std::thread([this, connection] { PollLoop(connection); });
  {
    std::lock_guard<std::mutex> lock(connections_mutex_);
    connections_.push_back(connection);
  }
  return true;
}

void RDMAServer::HandleDisconnect(rdma_cm_id* id) {
  std::shared_ptr<Connection> found;
  {
    std::lock_guard<std::mutex> lock(connections_mutex_);
    for (auto it = connections_.begin(); it != connections_.end(); ++it) {
      if ((*it)->id == id) {
        found = *it;
        connections_.erase(it);
        break;
      }
    }
  }
  if (found) {
    DestroyConnection(found);
  }
}

bool RDMAServer::InitConnection(
    const std::shared_ptr<Connection>& connection) {
  connection->pd = ibv_alloc_pd(connection->id->verbs);
  if (connection->pd == nullptr) {
    std::cerr << "ibv_alloc_pd failed\n";
    return false;
  }

  connection->send_cq =
      ibv_create_cq(connection->id->verbs, 8192, nullptr, nullptr, 0);
  if (connection->send_cq == nullptr) {
    std::cerr << "ibv_create_cq for send CQ failed\n";
    return false;
  }
  connection->recv_cq =
      ibv_create_cq(connection->id->verbs, 8192, nullptr, nullptr, 0);
  if (connection->recv_cq == nullptr) {
    std::cerr << "ibv_create_cq for recv CQ failed\n";
    return false;
  }

  ibv_qp_init_attr qp_attr{};
  qp_attr.send_cq = connection->send_cq;
  qp_attr.recv_cq = connection->recv_cq;
  qp_attr.qp_type = IBV_QPT_RC;
  qp_attr.cap.max_send_wr = 4096;
  qp_attr.cap.max_recv_wr = 4096;
  qp_attr.cap.max_send_sge = 1;
  qp_attr.cap.max_recv_sge = 1;

  if (rdma_create_qp(connection->id, connection->pd, &qp_attr) != 0) {
    std::cerr << "rdma_create_qp failed: " << std::strerror(errno) << "\n";
    return false;
  }
  connection->qp = connection->id->qp;

  const size_t recv_bytes =
      connection->recv_pool_depth * sizeof(PruneOffRequest);
  const size_t send_bytes =
      connection->send_pool_depth * sizeof(PruneOffResponse);
  if (posix_memalign(&connection->recv_pool, 4096, recv_bytes) != 0 ||
      posix_memalign(&connection->send_pool, 4096, send_bytes) != 0) {
    std::cerr << "posix_memalign for RDMA buffers failed\n";
    return false;
  }
  connection->recv_mr =
      ibv_reg_mr(connection->pd, connection->recv_pool, recv_bytes,
                 IBV_ACCESS_LOCAL_WRITE);
  connection->send_mr =
      ibv_reg_mr(connection->pd, connection->send_pool, send_bytes, 0);
  if (connection->recv_mr == nullptr || connection->send_mr == nullptr) {
    std::cerr << "ibv_reg_mr failed\n";
    return false;
  }

  connection->free_send_slots.reserve(connection->send_pool_depth);
  for (size_t i = 0; i < connection->send_pool_depth; ++i) {
    connection->free_send_slots.push_back(connection->send_pool_depth - 1 - i);
  }
  std::array<size_t, kPollBatch> recv_slots;
  size_t recv_count = 0;
  for (size_t i = 0; i < connection->recv_pool_depth; ++i) {
    recv_slots[recv_count++] = i;
    if (recv_count == recv_slots.size()) {
      if (!PostRecvBatch(connection, recv_slots.data(), recv_count)) {
        return false;
      }
      recv_count = 0;
    }
  }
  if (recv_count > 0) {
    if (!PostRecvBatch(connection, recv_slots.data(), recv_count)) {
      return false;
    }
  }
  return true;
}

void RDMAServer::DestroyConnection(
    const std::shared_ptr<Connection>& connection) {
  connection->running = false;
  connection->send_cv.notify_all();
  if (connection->poller.joinable()) {
    connection->poller.join();
  }
  if (connection->workers) {
    connection->workers->Stop();
  }
}

void RDMAServer::PollLoop(std::shared_ptr<Connection> connection) {
  PinCurrentThreadToCpu(connection->poller_cpu);
  ibv_wc completions[kPollBatch];
  std::array<size_t, kPollBatch> recv_slots;
  size_t idle_poll_count = 0;
  while (running_ && connection->running) {
    const int recv_n =
        ibv_poll_cq(connection->recv_cq, kPollBatch, completions);
    if (recv_n < 0) {
      std::cerr << "ibv_poll_cq for recv CQ failed\n";
      break;
    }
    size_t recv_count = 0;
    for (int i = 0; i < recv_n; ++i) {
      const ibv_wc& wc = completions[i];
      if (wc.status != IBV_WC_SUCCESS) {
        std::cerr << "RDMA WC error: " << ibv_wc_status_str(wc.status)
                  << "\n";
        continue;
      }
      const uintptr_t wr_id = static_cast<uintptr_t>(wc.wr_id);
      if (wc.opcode == IBV_WC_RECV) {
        const size_t slot = SlotFromWrId(wr_id);
        DispatchRecv(connection, slot);
        recv_slots[recv_count++] = slot;
      }
    }
    if (recv_count > 0) {
      PostRecvBatch(connection, recv_slots.data(), recv_count);
    }

    const int send_n =
        ibv_poll_cq(connection->send_cq, kPollBatch, completions);
    if (send_n < 0) {
      std::cerr << "ibv_poll_cq for send CQ failed\n";
      break;
    }
    for (int i = 0; i < send_n; ++i) {
      const ibv_wc& wc = completions[i];
      if (wc.status != IBV_WC_SUCCESS) {
        std::cerr << "RDMA WC error: " << ibv_wc_status_str(wc.status)
                  << "\n";
        continue;
      }
      if (wc.opcode == IBV_WC_SEND) {
        ReleaseCompletedSendSlots(
            connection, SlotFromWrId(static_cast<uintptr_t>(wc.wr_id)));
      }
    }

    if (recv_n == 0 && send_n == 0) {
      const size_t pause_count =
          std::min(++idle_poll_count, kMaxIdlePauseCount);
      for (size_t i = 0; i < pause_count; ++i) {
        asm volatile("pause" ::: "memory");
      }
    } else {
      idle_poll_count = 0;
    }
  }
}

void RDMAServer::DispatchRecv(const std::shared_ptr<Connection>& connection,
                              size_t recv_slot) {
  WorkItem item;
  item.connection = connection;
  const auto* request = static_cast<const PruneOffRequest*>(
      SlotAddress(connection->recv_pool, recv_slot, sizeof(PruneOffRequest)));
  item.request = *request;
  auto task = [this, item = std::move(item)]() mutable {
    EvaluateAndSend(std::move(item));
  };
  if (connection->workers) {
    connection->workers->Enqueue(std::move(task));
  } else {
    workers_.Enqueue(std::move(task));
  }
}

void RDMAServer::EvaluateAndSend(WorkItem item) {
  if (!item.connection->running) {
    return;
  }

  PruneOffRequest& request = item.request;
  const bool invalid_request =
      request.num_candidates > kPruneOffMaxCandidates ||
      request.key_len > kPruneOffMaxKeyBytes;

  size_t send_slot = 0;
  if (!AcquireSendSlot(item.connection, &send_slot)) {
    return;
  }
  if (!item.connection->running) {
    ReleaseSendSlot(item.connection, send_slot);
    return;
  }

  auto* response = static_cast<PruneOffResponse*>(
      SlotAddress(item.connection->send_pool, send_slot,
                  sizeof(PruneOffResponse)));
  response->request_id = request.request_id;
  response->client_slot = request.client_slot;
  response->num_positives = 0;
  response->num_filter_blocks = 0;
  response->reserved = 0;

  if (invalid_request || request.noop == 1) {
    if (!PostSend(item.connection, send_slot,
                  PruneOffResponseSerializedSize(
                      response->num_filter_blocks))) {
      ReleaseSendSlot(item.connection, send_slot);
    }
    return;
  }

  ROCKSDB_NAMESPACE::Slice key(request.key, request.key_len);
  for (uint32_t i = 0; i < request.num_candidates; ++i) {
    const uint64_t file_number = request.sst_file_numbers[i];
    bool may_match = true;
    if (request.want_filter_blocks == 1 &&
        response->num_filter_blocks < kPruneOffMaxCandidates) {
      FilterBlockEntry& filter_block =
          response->filter_blocks[response->num_filter_blocks];
      filter_block.sst_file_number = file_number;
      filter_block.filter_block_size = 0;
      filter_block._pad = 0;
      bool filter_block_found = false;
      may_match = evaluator_.KeyMayMatchAndReadFilterBlock(
          file_number, key, filter_block.data, kPruneOffMaxFilterBlockSize,
          &filter_block.filter_block_size, &filter_block_found);
      if (filter_block_found) {
        ++response->num_filter_blocks;
      }
    } else {
      may_match = evaluator_.KeyMayMatch(file_number, key);
    }
    if (may_match && response->num_positives < kPruneOffMaxCandidates) {
      response->positive_sst_file_numbers[response->num_positives++] =
          file_number;
    }
  }

  if (!PostSend(item.connection, send_slot,
                PruneOffResponseSerializedSize(response->num_filter_blocks))) {
    ReleaseSendSlot(item.connection, send_slot);
  }
}

bool RDMAServer::PostRecv(const std::shared_ptr<Connection>& connection,
                          size_t slot) {
  return PostRecvBatch(connection, &slot, 1);
}

bool RDMAServer::PostRecvBatch(const std::shared_ptr<Connection>& connection,
                               const size_t* slots, size_t count) {
  while (count > 0) {
    const size_t chunk =
        count < static_cast<size_t>(kPollBatch) ? count : kPollBatch;
    std::array<ibv_sge, kPollBatch> sges{};
    std::array<ibv_recv_wr, kPollBatch> wrs{};

    for (size_t i = 0; i < chunk; ++i) {
      const size_t slot = slots[i];
      sges[i].addr = reinterpret_cast<uint64_t>(
          SlotAddress(connection->recv_pool, slot, sizeof(PruneOffRequest)));
      sges[i].length = sizeof(PruneOffRequest);
      sges[i].lkey = connection->recv_mr->lkey;

      wrs[i].wr_id = RecvWrId(slot);
      wrs[i].sg_list = &sges[i];
      wrs[i].num_sge = 1;
      wrs[i].next = i + 1 < chunk ? &wrs[i + 1] : nullptr;
    }

    ibv_recv_wr* bad_wr = nullptr;
    if (ibv_post_recv(connection->qp, &wrs[0], &bad_wr) != 0) {
      std::cerr << "ibv_post_recv batch failed: " << std::strerror(errno)
                << "\n";
      return false;
    }

    slots += chunk;
    count -= chunk;
  }
  return true;
}

bool RDMAServer::PostSend(const std::shared_ptr<Connection>& connection,
                          size_t slot, size_t length) {
  ibv_sge sge{};
  sge.addr = reinterpret_cast<uint64_t>(
      SlotAddress(connection->send_pool, slot, sizeof(PruneOffResponse)));
  sge.length =
      static_cast<uint32_t>(std::min(length, sizeof(PruneOffResponse)));
  sge.lkey = connection->send_mr->lkey;

  ibv_send_wr wr{};
  wr.wr_id = SendWrId(slot);
  wr.opcode = IBV_WR_SEND;
  wr.sg_list = &sge;
  wr.num_sge = 1;

  ibv_send_wr* bad_wr = nullptr;
  {
    std::lock_guard<std::mutex> lock(connection->send_post_mutex);
    connection->pending_send_slots.push_back(slot);
    ++connection->sends_since_signal;
    if (connection->sends_since_signal >= kSendSignalInterval) {
      wr.send_flags = IBV_SEND_SIGNALED;
      connection->sends_since_signal = 0;
    }

    if (ibv_post_send(connection->qp, &wr, &bad_wr) != 0) {
      connection->pending_send_slots.pop_back();
      if (wr.send_flags & IBV_SEND_SIGNALED) {
        connection->sends_since_signal = kSendSignalInterval - 1;
      } else if (connection->sends_since_signal > 0) {
        --connection->sends_since_signal;
      }
      std::cerr << "ibv_post_send failed: " << std::strerror(errno) << "\n";
      return false;
    }
  }
  return true;
}

bool RDMAServer::AcquireSendSlot(
    const std::shared_ptr<Connection>& connection, size_t* slot) {
  std::unique_lock<std::mutex> lock(connection->send_mutex);
  connection->send_cv.wait(lock, [&] {
    return !connection->running || !connection->free_send_slots.empty();
  });
  if (!connection->running) {
    return false;
  }
  *slot = connection->free_send_slots.back();
  connection->free_send_slots.pop_back();
  return true;
}

void RDMAServer::ReleaseSendSlot(
    const std::shared_ptr<Connection>& connection, size_t slot) {
  {
    std::lock_guard<std::mutex> lock(connection->send_mutex);
    connection->free_send_slots.push_back(slot);
  }
  connection->send_cv.notify_one();
}

void RDMAServer::ReleaseCompletedSendSlots(
    const std::shared_ptr<Connection>& connection, size_t signaled_slot) {
  std::vector<size_t> completed_slots;
  {
    std::lock_guard<std::mutex> lock(connection->send_post_mutex);
    auto end = connection->pending_send_slots.end();
    auto it = std::find(connection->pending_send_slots.begin(), end,
                        signaled_slot);
    if (it == end) {
      return;
    }
    while (!connection->pending_send_slots.empty()) {
      const size_t slot = connection->pending_send_slots.front();
      connection->pending_send_slots.pop_front();
      completed_slots.push_back(slot);
      if (slot == signaled_slot) {
        break;
      }
    }
  }

  {
    std::lock_guard<std::mutex> lock(connection->send_mutex);
    for (size_t slot : completed_slots) {
      connection->free_send_slots.push_back(slot);
    }
  }
  connection->send_cv.notify_all();
}

int RDMAServer::NextPollerCpu() {
  if (options_.poller_cpus.empty()) {
    return -1;
  }
  const int cpu =
      options_.poller_cpus[next_poller_cpu_index_ % options_.poller_cpus.size()];
  ++next_poller_cpu_index_;
  return cpu;
}

uintptr_t RDMAServer::RecvWrId(size_t slot) { return slot; }

uintptr_t RDMAServer::SendWrId(size_t slot) { return kSendWrMask | slot; }

bool RDMAServer::IsSendWrId(uintptr_t wr_id) {
  return (wr_id & kSendWrMask) != 0;
}

size_t RDMAServer::SlotFromWrId(uintptr_t wr_id) {
  return static_cast<size_t>(wr_id & ~kSendWrMask);
}

bool RDMAServer::PinCurrentThreadToCpu(int cpu) {
  if (cpu < 0) {
    return false;
  }
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(cpu, &cpuset);
  return pthread_setaffinity_np(pthread_self(), sizeof(cpuset), &cpuset) == 0;
}

}  // namespace pruneoff
