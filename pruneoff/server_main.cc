#include "pruneoff/rdma_server.h"

#include <atomic>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <string>
#include <thread>

namespace {

std::atomic<bool> g_stop{false};

void SignalHandler(int) { g_stop = true; }

bool ParseArg(const std::string& arg, const std::string& prefix,
              std::string* value) {
  if (arg.rfind(prefix, 0) != 0) {
    return false;
  }
  *value = arg.substr(prefix.size());
  return true;
}

void Usage(const char* prog) {
  std::cerr << "usage: " << prog
            << " --db_path=/path/to/rocksdb [--bind=0.0.0.0]"
            << " [--port=7471] [--workers=8] [--poller_cpu=N]\n";
}

}  // namespace

int main(int argc, char** argv) {
  pruneoff::RDMAServer::Options options;

  for (int i = 1; i < argc; ++i) {
    std::string arg(argv[i]);
    std::string value;
    if (ParseArg(arg, "--db_path=", &value)) {
      options.db_path = value;
    } else if (ParseArg(arg, "--bind=", &value)) {
      options.bind_address = value;
    } else if (ParseArg(arg, "--port=", &value)) {
      options.port = static_cast<uint16_t>(std::stoul(value));
    } else if (ParseArg(arg, "--workers=", &value)) {
      options.worker_threads = std::stoul(value);
    } else if (ParseArg(arg, "--recv_depth=", &value)) {
      options.recv_pool_depth = std::stoul(value);
    } else if (ParseArg(arg, "--send_depth=", &value)) {
      options.send_pool_depth = std::stoul(value);
    } else if (ParseArg(arg, "--poller_cpu=", &value)) {
      options.poller_cpu = std::stoi(value);
    } else if (arg == "--help" || arg == "-h") {
      Usage(argv[0]);
      return 0;
    } else {
      std::cerr << "unknown argument: " << arg << "\n";
      Usage(argv[0]);
      return 2;
    }
  }

  if (options.db_path.empty()) {
    Usage(argv[0]);
    return 2;
  }

  std::signal(SIGINT, SignalHandler);
  std::signal(SIGTERM, SignalHandler);

  pruneoff::RDMAServer server(std::move(options));
  if (!server.Start()) {
    return 1;
  }

  while (!g_stop.load()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
  }
  server.Stop();
  return 0;
}
