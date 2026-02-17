// Copyright (c) 2024-present, RocksDB Contributors
// This source code is licensed under both the GPLv2 and Apache 2.0 License
//
// eRPC-based bloomcheck server implementation
// This server runs alongside the existing gRPC server (bloomd.cc)

#include "tools/bloomcheck.pb.h"

#include <erpc/rpc.h>
#include <erpc/nexus.h>
#include <erpc/transport.h>

#include <csignal>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <chrono>
#include <iostream>
#include <memory>
#include <string>
#include <thread>

namespace bloomcheck {

// Request type constants
static constexpr uint8_t kReqTypeNoop = 1;
static constexpr uint8_t kReqTypeCheck = 2;

// Global flag for graceful shutdown
static std::atomic<bool> g_shutdown_requested{false};

// Global RPC pointer (set in main, used in handlers)
static erpc::Rpc<erpc::CTransport>* g_rpc = nullptr;

// Signal handler for SIGINT (Ctrl+C)
void SignalHandler(int sig) {
  if (sig == SIGINT) {
    g_shutdown_requested.store(true);
  }
}

// Noop RPC handler
void NoopHandler(erpc::ReqHandle* req_handle, void* context) {
  (void)context;  // Unused
  auto* rpc = g_rpc;
  if (!rpc) {
    std::cerr << "[ERROR] RPC not initialized" << std::endl;
    return;
  }
  
  // Get request buffer
  erpc::MsgBuffer* req_msgbuf = req_handle->get_req_msgbuf();
  const uint8_t* req_buf = req_msgbuf->buf;
  size_t req_size = req_msgbuf->get_data_size();
  
  // Parse NoopRequest (empty request is OK)
  NoopRequest req;
  if (req_size > 0) {
    if (!req.ParseFromArray(req_buf, static_cast<int>(req_size))) {
      std::cerr << "[ERROR] Failed to parse NoopRequest" << std::endl;
      // Send empty response on parse error
      req.Clear();
    }
  }
  
  // Create response
  NoopResponse resp;
  
  // Get current timestamp in nanoseconds (monotonic clock)
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  uint64_t now_ns = static_cast<uint64_t>(ts.tv_sec) * 1000000000ULL + 
                     static_cast<uint64_t>(ts.tv_nsec);
  resp.set_server_ts_ns(now_ns);
  
  // Serialize response
  size_t resp_size = resp.ByteSizeLong();
  
  // Resize response buffer
  req_handle->pre_resp_msgbuf_ = rpc->resize_msg_buffer(&req_handle->pre_resp_msgbuf_, resp_size);
  uint8_t* resp_buf = req_handle->pre_resp_msgbuf_.buf;
  
  // Serialize to buffer
  if (!resp.SerializeToArray(resp_buf, static_cast<int>(resp_size))) {
    std::cerr << "[ERROR] Failed to serialize NoopResponse" << std::endl;
    // Send empty response on serialize error
    resp.Clear();
    resp.set_server_ts_ns(now_ns);
    resp_size = resp.ByteSizeLong();
    req_handle->pre_resp_msgbuf_ = rpc->resize_msg_buffer(&req_handle->pre_resp_msgbuf_, resp_size);
    resp_buf = req_handle->pre_resp_msgbuf_.buf;
    resp.SerializeToArray(resp_buf, static_cast<int>(resp_size));
  }
  
  // Optional: sparse logging (1/100000 sample rate)
  static std::atomic<size_t> noop_count{0};
  size_t count = noop_count.fetch_add(1) + 1;
  
  static bool noop_log_enabled = []() {
    const char* env = std::getenv("BLOOMCHECK_ERPC_NOOP_LOG");
    return env && std::string(env) == "1";
  }();
  
  if (noop_log_enabled && count % 100000 == 0) {
    std::cerr << "[NOOP] Noop request #" << count 
              << " server_ts_ns=" << now_ns << std::endl;
  }
  
  // Enqueue response
  rpc->enqueue_response(req_handle, &req_handle->pre_resp_msgbuf_);
}

// Check RPC handler (stub implementation)
void CheckHandler(erpc::ReqHandle* req_handle, void* context) {
  (void)context;  // Unused
  auto* rpc = g_rpc;
  if (!rpc) {
    std::cerr << "[ERROR] RPC not initialized" << std::endl;
    return;
  }
  
  // Get request buffer
  erpc::MsgBuffer* req_msgbuf = req_handle->get_req_msgbuf();
  const uint8_t* req_buf = req_msgbuf->buf;
  size_t req_size = req_msgbuf->get_data_size();
  
  // Parse BatchRequest
  BatchRequest batch_req;
  BatchResponse batch_resp;
  
  if (req_size > 0) {
    if (!batch_req.ParseFromArray(req_buf, static_cast<int>(req_size))) {
      std::cerr << "[ERROR] Failed to parse BatchRequest" << std::endl;
      // Send empty response on parse error
      batch_resp.clear_resps();
    } else {
      // TODO: Implement actual bloom filter checking logic
      // For now, return safe default: all candidates as may_match
      batch_resp.clear_resps();
      for (const auto& key_req : batch_req.reqs()) {
        KeyResponse key_resp;
        key_resp.set_total_candidates(key_req.sst_file_numbers_size());
        key_resp.set_checked_count(0);  // Not checked yet
        key_resp.set_complete(false);
        // Add all candidates as may_match (safe default)
        for (uint64_t file_num : key_req.sst_file_numbers()) {
          key_resp.add_may_match_sst_file_numbers(file_num);
        }
        *batch_resp.add_resps() = key_resp;
      }
    }
  } else {
    // Empty request - return empty response
    batch_resp.clear_resps();
  }
  
  // Serialize response
  size_t resp_size = batch_resp.ByteSizeLong();
  
  // Resize response buffer
  req_handle->pre_resp_msgbuf_ = rpc->resize_msg_buffer(&req_handle->pre_resp_msgbuf_, resp_size);
  uint8_t* resp_buf = req_handle->pre_resp_msgbuf_.buf;
  
  // Serialize to buffer
  if (!batch_resp.SerializeToArray(resp_buf, static_cast<int>(resp_size))) {
    std::cerr << "[ERROR] Failed to serialize BatchResponse" << std::endl;
    // Send empty response on serialize error
    batch_resp.clear_resps();
    resp_size = batch_resp.ByteSizeLong();
    req_handle->pre_resp_msgbuf_ = rpc->resize_msg_buffer(&req_handle->pre_resp_msgbuf_, resp_size);
    resp_buf = req_handle->pre_resp_msgbuf_.buf;
    batch_resp.SerializeToArray(resp_buf, static_cast<int>(resp_size));
  }
  
  // Enqueue response
  rpc->enqueue_response(req_handle, &req_handle->pre_resp_msgbuf_);
}

// Request handler wrappers for each req_type
// eRPC calls the appropriate handler based on req_type registered with Nexus
void NoopReqHandler(erpc::ReqHandle* req_handle, void* context) {
  NoopHandler(req_handle, context);
}

void CheckReqHandler(erpc::ReqHandle* req_handle, void* context) {
  CheckHandler(req_handle, context);
}

}  // namespace bloomcheck

int main(int argc, char** argv) {
  // Parse command line arguments
  std::string local_uri = "10.0.0.1:31850";
  int numa_node = 0;
  uint8_t rpc_id = 0;
  
  for (int i = 1; i < argc; i++) {
    std::string arg = argv[i];
    if (arg.find("--local_uri=") == 0) {
      local_uri = arg.substr(12);
    } else if (arg.find("--numa_node=") == 0) {
      numa_node = std::stoi(arg.substr(12));
    } else if (arg.find("--rpc_id=") == 0) {
      rpc_id = static_cast<uint8_t>(std::stoi(arg.substr(9)));
    } else if (arg == "--help" || arg == "-h") {
      std::cout << "Usage: " << argv[0] << " [options]" << std::endl;
      std::cout << "Options:" << std::endl;
      std::cout << "  --local_uri=URI     eRPC SM UDP URI (default: 10.0.0.1:31850)" << std::endl;
      std::cout << "  --numa_node=N       NUMA node (default: 0)" << std::endl;
      std::cout << "  --rpc_id=ID         RPC ID (default: 0)" << std::endl;
      std::cout << "  --help, -h           Show this help message" << std::endl;
      return 0;
    }
  }
  
  // Setup signal handler for graceful shutdown
  std::signal(SIGINT, bloomcheck::SignalHandler);
  
  // Create Nexus
  erpc::Nexus nexus(local_uri, numa_node, 0);  // num_bg_threads = 0
  
  // Register request handlers (each req_type gets its own handler)
  nexus.register_req_func(bloomcheck::kReqTypeNoop, bloomcheck::NoopReqHandler);
  nexus.register_req_func(bloomcheck::kReqTypeCheck, bloomcheck::CheckReqHandler);
  
  // Create RPC
  erpc::Rpc<erpc::CTransport> rpc(&nexus, nullptr, rpc_id, nullptr);
  
  // Set global RPC pointer for handlers
  bloomcheck::g_rpc = &rpc;
  
  std::cout << "[INFO] eRPC bloomcheck server starting..." << std::endl;
  std::cout << "[INFO] local_uri=" << local_uri << std::endl;
  std::cout << "[INFO] numa_node=" << numa_node << std::endl;
  std::cout << "[INFO] rpc_id=" << static_cast<int>(rpc_id) << std::endl;
  std::cout << "[INFO] Press Ctrl+C to stop" << std::endl;
  
  // Run event loop until shutdown requested
  while (!bloomcheck::g_shutdown_requested.load()) {
    rpc.run_event_loop_once();
  }
  
  std::cout << "[INFO] Shutting down..." << std::endl;
  
  // Clear global RPC pointer before destruction
  bloomcheck::g_rpc = nullptr;
  
  // Rpc must be destroyed before Nexus (eRPC requirement)
  // Rpc will be destroyed when it goes out of scope
  // Nexus will be destroyed when it goes out of scope
  
  std::cout << "[INFO] Server stopped" << std::endl;
  return 0;
}

