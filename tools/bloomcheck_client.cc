// Copyright (c) 2024-present, RocksDB Contributors
// This source code is licensed under both the GPLv2 and Apache 2.0 License

#include <grpcpp/grpcpp.h>
#include <chrono>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include "tools/bloomcheck.grpc.pb.h"

using bloomcheck::BatchRequest;
using bloomcheck::BatchResponse;
using bloomcheck::BloomCheck;
using bloomcheck::KeyRequest;
using bloomcheck::KeyResponse;

class BloomCheckClient {
 public:
  explicit BloomCheckClient(std::shared_ptr<grpc::Channel> channel)
      : stub_(BloomCheck::NewStub(channel)) {}

  // Check a single key against a single SST file
  bool CheckKey(const std::string& key, const std::string& sst_path,
                std::vector<std::string>* may_match_paths) {
    BatchRequest request;
    KeyRequest* key_req = request.add_reqs();
    key_req->set_user_key(key);
    key_req->add_sst_paths(sst_path);

    BatchResponse response;
    grpc::ClientContext context;

    grpc::Status status = stub_->Check(&context, request, &response);

    if (!status.ok()) {
      std::cerr << "RPC failed: " << status.error_message() << std::endl;
      return false;
    }

    if (response.resps_size() != 1) {
      std::cerr << "Unexpected response size: " << response.resps_size()
                << std::endl;
      return false;
    }

    const KeyResponse& key_resp = response.resps(0);
    may_match_paths->clear();
    for (int i = 0; i < key_resp.may_match_sst_paths_size(); ++i) {
      may_match_paths->push_back(key_resp.may_match_sst_paths(i));
    }

    return true;
  }

  // Check multiple keys against multiple SST files
  bool CheckBatch(const BatchRequest& request, BatchResponse* response) {
    grpc::ClientContext context;
    grpc::Status status = stub_->Check(&context, request, response);
    return status.ok();
  }

 private:
  std::unique_ptr<BloomCheck::Stub> stub_;
};

std::string HexToBytes(const std::string& hex) {
  std::string bytes;
  for (size_t i = 0; i < hex.length(); i += 2) {
    std::string byte_str = hex.substr(i, 2);
    char byte = static_cast<char>(std::stoi(byte_str, nullptr, 16));
    bytes.push_back(byte);
  }
  return bytes;
}

std::string BytesToHex(const std::string& bytes) {
  std::stringstream ss;
  for (unsigned char c : bytes) {
    ss << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(c);
  }
  return ss.str();
}

void PrintUsage(const char* prog_name) {
  std::cout << "Usage: " << prog_name << " [options]\n"
            << "Options:\n"
            << "  -s <server>     Server address (default: localhost:50051)\n"
            << "  -k <key>        Key to check (required for single check)\n"
            << "  -x <hex_key>    Key in hex format (required for single check)\n"
            << "                  Note: -x takes user_key hex (as output by sst_dump --output_hex)\n"
            << "  -f <sst_path>   SST file path (required for single check)\n"
            << "  -t              Performance test (64-256 keys, 1-5 SSTs)\n"
            << "  -h              Show this help\n";
}

int main(int argc, char** argv) {
  std::string server_address = "localhost:50051";
  std::string key;
  std::string sst_path;
  bool perf_test = false;
  bool use_hex_key = false;

  // Parse arguments
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg == "-s" && i + 1 < argc) {
      server_address = argv[++i];
    } else if (arg == "-k" && i + 1 < argc) {
      key = argv[++i];
      use_hex_key = false;
    } else if (arg == "-x" && i + 1 < argc) {
      std::string hex_key = argv[++i];
      // Remove quotes if present
      if (!hex_key.empty() && hex_key[0] == '\'') {
        hex_key = hex_key.substr(1);
      }
      if (!hex_key.empty() && hex_key.back() == '\'') {
        hex_key = hex_key.substr(0, hex_key.length() - 1);
      }
      
      // Convert hex to bytes (this is user_key hex from sst_dump --output_hex)
      // sst_dump outputs user_key hex, not Internal Key hex
      key = HexToBytes(hex_key);
      use_hex_key = true;
    } else if (arg == "-f" && i + 1 < argc) {
      sst_path = argv[++i];
    } else if (arg == "-t") {
      perf_test = true;
    } else if (arg == "-h") {
      PrintUsage(argv[0]);
      return 0;
    }
  }

  // Create channel
  auto channel = grpc::CreateChannel(server_address,
                                      grpc::InsecureChannelCredentials());
  BloomCheckClient client(channel);

  if (perf_test) {
    // Performance test
    std::cout << "Running performance test...\n";

    // Generate test keys (64-256 keys)
    std::vector<std::string> keys;
    for (int i = 0; i < 128; ++i) {
      keys.push_back("key_" + std::to_string(i));
    }

    // Generate test SST paths (1-5 per key)
    std::vector<std::string> sst_paths;
    for (int i = 0; i < 3; ++i) {
      sst_paths.push_back("/tmp/test_sst_" + std::to_string(i) + ".sst");
    }

    BatchRequest request;
    for (const auto& k : keys) {
      KeyRequest* key_req = request.add_reqs();
      key_req->set_user_key(k);
      for (const auto& sst : sst_paths) {
        key_req->add_sst_paths(sst);
      }
    }

    auto start = std::chrono::high_resolution_clock::now();
    BatchResponse response;
    bool success = client.CheckBatch(request, &response);
    auto end = std::chrono::high_resolution_clock::now();

    if (success) {
      auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
          end - start);
      std::cout << "Processed " << keys.size() << " keys in "
                << duration.count() << " ms\n";
      std::cout << "Average: " << duration.count() / keys.size()
                << " ms per key\n\n";

      // Count results and show details
      int total_matches = 0;
      int keys_with_matches = 0;
      int keys_without_matches = 0;
      
      std::cout << "Results per key:\n";
      for (int i = 0; i < response.resps_size() && static_cast<size_t>(i) < keys.size(); ++i) {
        const KeyResponse& key_resp = response.resps(i);
        const std::string& test_key = keys[i];
        int match_count = key_resp.may_match_sst_paths_size();
        total_matches += match_count;
        
        if (match_count > 0) {
          keys_with_matches++;
          std::cout << "  Key[" << i << "] '" << test_key << "': MAY be in " 
                    << match_count << " SST(s): ";
          for (int j = 0; j < key_resp.may_match_sst_paths_size(); ++j) {
            std::cout << key_resp.may_match_sst_paths(j);
            if (j < key_resp.may_match_sst_paths_size() - 1) {
              std::cout << ", ";
            }
          }
          std::cout << "\n";
        } else {
          keys_without_matches++;
          std::cout << "  Key[" << i << "] '" << test_key 
                    << "': definitely NOT in any SST\n";
        }
      }
      
      std::cout << "\nSummary:\n";
      std::cout << "  Total keys: " << keys.size() << "\n";
      std::cout << "  Keys with matches: " << keys_with_matches << "\n";
      std::cout << "  Keys without matches: " << keys_without_matches << "\n";
      std::cout << "  Total may_match results: " << total_matches << "\n";
    } else {
      std::cerr << "Performance test failed\n";
      return 1;
    }
  } else if (!key.empty() && !sst_path.empty()) {
    // Single key check
    if (use_hex_key) {
      std::cout << "Checking key (hex): " << BytesToHex(key) 
                << " against SST: " << sst_path << "\n";
    } else {
      std::cout << "Checking key: " << key << " against SST: " << sst_path
                << "\n";
    }

    std::vector<std::string> may_match_paths;
    bool success = client.CheckKey(key, sst_path, &may_match_paths);

    if (success) {
      if (may_match_paths.empty()) {
        std::cout << "Result: Key definitely NOT in SST (filter said no)\n";
      } else {
        std::cout << "Result: Key MAY be in SST (filter said maybe)\n";
        for (const auto& path : may_match_paths) {
          std::cout << "  - " << path << "\n";
        }
      }
    } else {
      std::cerr << "Check failed\n";
      return 1;
    }
  } else {
    PrintUsage(argv[0]);
    return 1;
  }

  return 0;
}

