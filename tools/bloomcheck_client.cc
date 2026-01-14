// Copyright (c) 2024-present, RocksDB Contributors
// This source code is licensed under both the GPLv2 and Apache 2.0 License

#include <grpcpp/grpcpp.h>
#include <chrono>
#include <cctype>
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

  // Check a single key against a single SST file (by file_number)
  bool CheckKey(const std::string& key, uint64_t file_number,
                std::vector<uint64_t>* may_match_file_numbers) {
    BatchRequest request;
    KeyRequest* key_req = request.add_reqs();
    key_req->set_user_key(key);
    key_req->add_sst_file_numbers(file_number);

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
    may_match_file_numbers->clear();
    for (int i = 0; i < key_resp.may_match_sst_file_numbers_size(); ++i) {
      may_match_file_numbers->push_back(key_resp.may_match_sst_file_numbers(i));
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

static inline bool IsHexChar(char c) {
  return std::isxdigit(static_cast<unsigned char>(c)) != 0;
}

static std::string StripOptionalQuotes(std::string s) {
  if (!s.empty() && (s.front() == '\'' || s.front() == '"')) s.erase(0, 1);
  if (!s.empty() && (s.back() == '\'' || s.back() == '"')) s.pop_back();
  return s;
}

static std::string StripOptionalHexPrefix(std::string s) {
  if (s.size() >= 2 && s[0] == '0' && (s[1] == 'x' || s[1] == 'X')) {
    return s.substr(2);
  }
  return s;
}

static bool HexToBytesChecked(const std::string& hex_in, std::string* out_bytes,
                              std::string* err) {
  std::string hex = StripOptionalQuotes(hex_in);
  hex = StripOptionalHexPrefix(hex);

  if (hex.empty()) {
    if (err) *err = "empty hex string";
    return false;
  }
  if (hex.size() % 2 != 0) {
    if (err) *err = "hex string length must be even";
    return false;
  }
  for (char c : hex) {
    if (!IsHexChar(c)) {
      if (err) *err = std::string("invalid hex character: '") + c + "'";
      return false;
    }
  }

  std::string bytes;
  bytes.reserve(hex.size() / 2);

  for (size_t i = 0; i < hex.size(); i += 2) {
    const std::string byte_str = hex.substr(i, 2);
    char byte = static_cast<char>(std::stoi(byte_str, nullptr, 16));
    bytes.push_back(byte);
  }

  *out_bytes = std::move(bytes);
  return true;
}

static std::string BytesToHex(const std::string& bytes) {
  std::stringstream ss;
  ss << std::hex << std::setfill('0');
  for (unsigned char c : bytes) {
    ss << std::setw(2) << static_cast<int>(c);
  }
  return ss.str();
}

static void PrintUsage(const char* prog_name) {
  std::cout
      << "Usage: " << prog_name << " [options]\n"
      << "Options:\n"
      << "  -s <server>        Server address (default: localhost:50051)\n"
      << "  -k <key_str>       User key as a raw string (recommended; matches sst_dump output)\n"
      << "  -u <hex_user_key>  User key as hex bytes (will hex-decode and send as user_key)\n"
      << "  -x <hex_ikey>      InternalKey as hex bytes (will hex-decode, then strip last 8 bytes)\n"
      << "  --no-strip         With -x, do NOT strip last 8 bytes (send full decoded bytes)\n"
      << "  -f <file_number>   SST file number (required for single check, e.g., 16657 or 016657)\n"
      << "  -t                 Performance test (64-256 keys, 1-5 SSTs)\n"
      << "  -h                 Show this help\n"
      << "\nExamples:\n"
      << "  # 1) Best for your sst_dump scan output (raw string key)\n"
      << "  " << prog_name
      << " -s 10.1.1.1:50051 -k \"000000001B02E09C3030...3030\" -f 16657\n"
      << "\n"
      << "  # 2) If you have user_key bytes already in hex\n"
      << "  " << prog_name
      << " -s 10.1.1.1:50051 -u 30313233 -f 16657\n"
      << "\n"
      << "  # 3) If you have InternalKey hex (user_key + 7B seq + 1B type)\n"
      << "  " << prog_name
      << " -s 10.1.1.1:50051 -x <hex_internal_key> -f 16657\n";
}

int main(int argc, char** argv) {
  std::string server_address = "localhost:50051";
  std::string key;
  uint64_t file_number = 0;
  bool file_number_set = false;
  bool perf_test = false;

  enum class KeyMode {
    kNone,
    kRawString,   // -k
    kHexUserKey,  // -u
    kHexIKey      // -x
  };
  KeyMode key_mode = KeyMode::kNone;

  bool no_strip = false;

  // Parse arguments
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];

    if (arg == "-s" && i + 1 < argc) {
      server_address = argv[++i];

    } else if (arg == "-k" && i + 1 < argc) {
      key = argv[++i];
      key_mode = KeyMode::kRawString;

    } else if (arg == "-u" && i + 1 < argc) {
      std::string hex_user_key = argv[++i];
      std::string bytes, err;
      if (!HexToBytesChecked(hex_user_key, &bytes, &err)) {
        std::cerr << "Invalid -u hex_user_key: " << err << "\n";
        return 1;
      }
      key = std::move(bytes);
      key_mode = KeyMode::kHexUserKey;

    } else if (arg == "-x" && i + 1 < argc) {
      std::string hex_ikey = argv[++i];

      std::string internal_key_bytes, err;
      if (!HexToBytesChecked(hex_ikey, &internal_key_bytes, &err)) {
        std::cerr << "Invalid -x hex_ikey: " << err << "\n";
        return 1;
      }

      if (!no_strip && internal_key_bytes.size() >= 8) {
        // InternalKey = user_key + seq(7B) + type(1B)
        // But if the input is already a user_key (no seq+type suffix),
        // we should detect it. For now, always strip last 8 bytes.
        // Note: If the input is actually a user_key, use -u instead of -x
        key = internal_key_bytes.substr(0, internal_key_bytes.size() - 8);
      } else {
        key = std::move(internal_key_bytes);
      }
      key_mode = KeyMode::kHexIKey;

    } else if (arg == "--no-strip") {
      no_strip = true;

    } else if (arg == "-f" && i + 1 < argc) {
      std::string file_number_str = argv[++i];
      try {
        // Parse file_number (handles both "16657" and "016657")
        file_number = std::stoull(file_number_str, nullptr, 10);
        file_number_set = true;
      } catch (const std::exception& e) {
        std::cerr << "Invalid file_number: " << file_number_str << " (" << e.what() << ")\n";
        return 1;
      }

    } else if (arg == "-t") {
      perf_test = true;

    } else if (arg == "-h") {
      PrintUsage(argv[0]);
      return 0;

    } else {
      std::cerr << "Unknown or incomplete option: " << arg << "\n";
      PrintUsage(argv[0]);
      return 1;
    }
  }

  // Create channel
  auto channel =
      grpc::CreateChannel(server_address, grpc::InsecureChannelCredentials());
  BloomCheckClient client(channel);

  if (perf_test) {
    // Performance test
    std::cout << "Running performance test...\n";

    // Generate test keys (64-256 keys)
    std::vector<std::string> keys;
    for (int i = 0; i < 128; ++i) {
      keys.push_back("key_" + std::to_string(i));
    }

    BatchRequest request;
    for (const auto& k : keys) {
      KeyRequest* key_req = request.add_reqs();
      key_req->set_user_key(k);
      // Generate test file numbers (1-5 per key)
      for (int i = 0; i < 3; ++i) {
        key_req->add_sst_file_numbers(1000 + i);
      }
    }

    auto start = std::chrono::high_resolution_clock::now();
    BatchResponse response;
    bool success = client.CheckBatch(request, &response);
    auto end = std::chrono::high_resolution_clock::now();

    if (!success) {
      std::cerr << "Performance test failed (RPC)\n";
      return 1;
    }

    auto duration =
        std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    std::cout << "Processed " << keys.size() << " keys in " << duration.count()
              << " ms\n";
    std::cout << "Average: " << (keys.empty() ? 0 : duration.count() / keys.size())
              << " ms per key\n\n";

    // Count results and show details
    int total_matches = 0;
    int keys_with_matches = 0;
    int keys_without_matches = 0;

    std::cout << "Results per key:\n";
    for (int i = 0; i < response.resps_size() &&
                    static_cast<size_t>(i) < keys.size();
         ++i) {
      const KeyResponse& key_resp = response.resps(i);
      const std::string& test_key = keys[i];
      int match_count = key_resp.may_match_sst_file_numbers_size();
      total_matches += match_count;

      if (match_count > 0) {
        keys_with_matches++;
        std::cout << "  Key[" << i << "] '" << test_key << "': MAY be in "
                  << match_count << " SST(s): ";
        for (int j = 0; j < key_resp.may_match_sst_file_numbers_size(); ++j) {
          std::cout << key_resp.may_match_sst_file_numbers(j);
          if (j < key_resp.may_match_sst_file_numbers_size() - 1) {
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
    return 0;
  }

  // Single check mode
  if (key.empty() || !file_number_set) {
    PrintUsage(argv[0]);
    return 1;
  }

  // Print what we're actually sending
  if (key_mode == KeyMode::kRawString) {
    std::cout << "Checking key (raw string): " << key << " against SST file_number: " << file_number
              << "\n";
  } else if (key_mode == KeyMode::kHexUserKey) {
    std::cout << "Checking user_key (hex bytes): " << BytesToHex(key)
              << " against SST file_number: " << file_number << "\n";
  } else if (key_mode == KeyMode::kHexIKey) {
    std::cout << "Checking derived user_key from internal_key (hex bytes): "
              << BytesToHex(key) << " against SST file_number: " << file_number
              << (no_strip ? " (no-strip)\n" : " (strip last 8 bytes)\n");
  } else {
    std::cout << "Checking key: " << key << " against SST file_number: " << file_number << "\n";
  }

  std::vector<uint64_t> may_match_file_numbers;
  bool success = client.CheckKey(key, file_number, &may_match_file_numbers);

  if (!success) {
    std::cerr << "Check failed\n";
    return 1;
  }

  if (may_match_file_numbers.empty()) {
    std::cout << "Result: Key definitely NOT in SST (filter said no)\n";
  } else {
    std::cout << "Result: Key MAY be in SST (filter said maybe)\n";
    for (const auto& fn : may_match_file_numbers) {
      std::cout << "  - " << fn << "\n";
    }
  }

  return 0;
}

