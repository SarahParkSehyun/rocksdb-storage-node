#include "pruneoff/rdma_server.h"

#include <atomic>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

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

bool ParseCpuList(const std::string& value, std::vector<int>* cpus) {
  cpus->clear();
  size_t start = 0;
  while (start <= value.size()) {
    size_t comma = value.find(',', start);
    if (comma == std::string::npos) {
      comma = value.size();
    }
    if (comma == start) {
      return false;
    }
    const int cpu = std::stoi(value.substr(start, comma - start));
    if (cpu < 0) {
      return false;
    }
    cpus->push_back(cpu);
    if (comma == value.size()) {
      break;
    }
    start = comma + 1;
  }
  return !cpus->empty();
}

void Usage(const char* prog) {
  std::cerr << "usage: " << prog
            << " --db_path=/path/to/rocksdb [--bind=0.0.0.0]"
            << " [--port=7471] [--workers=8]"
            << " [--workers_per_connection=N]"
            << " [--recv_depth=256] [--send_depth=256]"
            << " [--poller_cpu=N] [--poller_cpus=N[,M...]]\n";
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
    } else if (ParseArg(arg, "--workers_per_connection=", &value)) {
      options.worker_threads_per_connection = std::stoul(value);
    } else if (ParseArg(arg, "--recv_depth=", &value) ||
               ParseArg(arg, "--recv_pool_depth=", &value)) {
      options.recv_pool_depth = std::stoul(value);
    } else if (ParseArg(arg, "--send_depth=", &value) ||
               ParseArg(arg, "--send_pool_depth=", &value)) {
      options.send_pool_depth = std::stoul(value);
    } else if (ParseArg(arg, "--poller_cpu=", &value)) {
      const int cpu = std::stoi(value);
      if (cpu < 0) {
        std::cerr << "--poller_cpu must be non-negative\n";
        return 2;
      }
      options.poller_cpus = {cpu};
    } else if (ParseArg(arg, "--poller_cpus=", &value)) {
      if (!ParseCpuList(value, &options.poller_cpus)) {
        std::cerr << "--poller_cpus must be a comma-separated CPU list\n";
        return 2;
      }
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
  if (options.worker_threads_per_connection == 0 &&
      options.worker_threads == 0) {
    std::cerr << "either --workers or --workers_per_connection must be > 0\n";
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
