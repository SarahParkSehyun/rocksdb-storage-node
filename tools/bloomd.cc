// Copyright (c) 2024-present, RocksDB Contributors
// This source code is licensed under both the GPLv2 and Apache 2.0 License

#include "tools/bloomcheck.grpc.pb.h"
#include "tools/bloomcheck.pb.h"

#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>

#include <atomic>
#include <cstdlib>
#include <cstring>
#include <chrono>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "db/dbformat.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/sst_file_reader.h"
#include "table/block_based/block_based_table_reader.h"
#include "table/get_context.h"
#include "table/table_reader.h"
#include "table/table_builder.h"
#include "util/cast_util.h"

namespace bloomcheck {

// LRU cache for TableReader
class TableReaderCache {
 public:
  struct CacheEntry {
    std::unique_ptr<ROCKSDB_NAMESPACE::TableReader> reader;
    std::chrono::steady_clock::time_point last_access;
    std::mutex mutex;  // Per-entry lock for thread safety
  };

  explicit TableReaderCache(size_t max_size) : max_size_(max_size) {}

  // Get or create TableReader for SST file
  // Returns nullptr on error (safe to treat as may_match=true)
  CacheEntry* GetOrOpen(const std::string& sst_path,
                        const ROCKSDB_NAMESPACE::Options& options) {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    
    // 경로 변환: Compute node 경로 → Storage node 경로
    // Compute node는 NVMe-oF 마운트 경로(/mnt/kvafa/db_bench/)를 전송하지만,
    // Storage node는 로컬 경로(/work/tmp/db_bench/)에서 파일을 읽어야 함
    std::string actual_path = sst_path;
    
    // 경로에서 파일 번호 추출
    // 예: /mnt/kvafa/db_bench/022694.sst -> 22694
    size_t last_slash = sst_path.find_last_of('/');
    std::string filename;
    if (last_slash != std::string::npos && last_slash + 1 < sst_path.length()) {
      filename = sst_path.substr(last_slash + 1);
    } else {
      filename = sst_path;
    }
    
    // 파일명에서 .sst 제거하고 숫자 추출
    // 예: 022694.sst -> 022694
    size_t dot_pos = filename.find(".sst");
    std::string file_number_str;
    if (dot_pos != std::string::npos) {
      file_number_str = filename.substr(0, dot_pos);
    } else {
      file_number_str = filename;
    }
    
    // Storage node의 실제 경로와 파일 번호를 조합
    // NVMe-oF로 연결된 경우, Storage node에서도 같은 경로를 사용할 수 있음
    // 또는 환경 변수로 설정 가능
    const char* storage_path_env = std::getenv("BLOOMCHECK_STORAGE_PATH");
    std::string storage_node_base_path;
    
    if (storage_path_env && strlen(storage_path_env) > 0) {
      // 환경 변수로 경로 지정 (예: /mnt/storage/db_bench/)
      storage_node_base_path = storage_path_env;
      if (storage_node_base_path.back() != '/') {
        storage_node_base_path += '/';
      }
    } else {
      // 기본값: Compute node와 같은 경로 (NVMe-oF로 연결된 경우)
      // /mnt/kvafa/db_bench/ -> /mnt/kvafa/db_bench/ (같은 경로 사용)
      // 또는 다른 기본 경로
      if (sst_path.find("/mnt/kvafa/db_bench/") == 0) {
        // Compute node 경로를 그대로 사용 (NVMe-oF로 연결된 경우)
        storage_node_base_path = "/mnt/kvafa/db_bench/";
      } else {
        // 기본 경로
        storage_node_base_path = "/mnt/kvafa/db_bench/";
      }
    }
    
    actual_path = storage_node_base_path + file_number_str + ".sst";

    // Check cache (original path로 캐시 확인)
    auto it = cache_.find(sst_path);
    if (it != cache_.end()) {
      it->second->last_access = std::chrono::steady_clock::now();
      return it->second.get();
    }
    
    // actual_path로도 캐시 확인 (이미 변환된 경로가 있을 수 있음)
    if (actual_path != sst_path) {
      it = cache_.find(actual_path);
      if (it != cache_.end()) {
        // 원본 경로로도 같은 CacheEntry를 가리키도록 (별도 저장은 안 함)
        // 대신 캐시 키만 추가 (shared_ptr처럼 동작)
        // 간단하게 actual_path의 entry를 반환
        it->second->last_access = std::chrono::steady_clock::now();
        // sst_path로도 캐시 키를 추가하여 나중에 찾을 수 있도록
        // 실제로는 actual_path의 entry를 재사용
        return it->second.get();
      }
    }

    // Cache miss - need to open file
    // If cache is full, evict LRU entry
    if (cache_.size() >= max_size_) {
      EvictLRU();
    }

    // Open SST file directly using BlockBasedTable
    // Create a new entry and open the file
    auto entry = std::make_unique<CacheEntry>();
    entry->last_access = std::chrono::steady_clock::now();

    // Open using BlockBasedTable directly
    ROCKSDB_NAMESPACE::ImmutableOptions ioptions(options);
    ROCKSDB_NAMESPACE::EnvOptions env_options;
    ROCKSDB_NAMESPACE::ReadOptions read_options;
    read_options.read_tier = ROCKSDB_NAMESPACE::kReadAllTier;  // Allow file reads

    uint64_t file_size = 0;
    std::unique_ptr<ROCKSDB_NAMESPACE::FSRandomAccessFile> file;
    const auto& fs = options.env->GetFileSystem();
    ROCKSDB_NAMESPACE::FileOptions fopts(env_options);
    
    // actual_path 사용 (변환된 경로)
    ROCKSDB_NAMESPACE::IOStatus s = fs->GetFileSize(actual_path, fopts.io_options, &file_size, nullptr);
    if (!s.ok()) {
      // 파일이 없을 때는 정상 동작 (may_match=true 반환)
      // 에러 로그는 처음 몇 번만 출력하여 로그 폭주 방지
      static std::atomic<size_t> file_not_found_count{0};
      size_t count = file_not_found_count.fetch_add(1) + 1;
      if (count <= 5 || count % 1000 == 0) {
        std::cerr << "[WARN] GetOrOpen: File not found: " << actual_path 
                  << " (requested: " << sst_path << ")" << std::flush << std::endl;
        if (count == 1000) {
          std::cerr << "[INFO] GetOrOpen: Suppressing further file not found warnings (total: " << count << ")" << std::flush << std::endl;
        }
      }
      return nullptr;
    }

    s = fs->NewRandomAccessFile(actual_path, fopts, &file, nullptr);
    if (!s.ok()) {
      // 파일 열기 실패 시에도 정상 동작 (may_match=true 반환)
      // 에러 로그는 처음 몇 번만 출력
      static std::atomic<size_t> file_open_error_count{0};
      size_t count = file_open_error_count.fetch_add(1) + 1;
      if (count <= 5 || count % 1000 == 0) {
        std::cerr << "[WARN] GetOrOpen: Failed to open file: " << actual_path 
                  << " (requested: " << sst_path << "): " << s.ToString() << std::flush << std::endl;
        if (count == 1000) {
          std::cerr << "[INFO] GetOrOpen: Suppressing further file open error warnings (total: " << count << ")" << std::flush << std::endl;
        }
      }
      return nullptr;
    }

    std::unique_ptr<ROCKSDB_NAMESPACE::RandomAccessFileReader> file_reader(
        new ROCKSDB_NAMESPACE::RandomAccessFileReader(std::move(file), actual_path));

    ROCKSDB_NAMESPACE::InternalKeyComparator internal_comparator(
        options.comparator);
    
    ROCKSDB_NAMESPACE::TableReaderOptions t_opt(
        ioptions, options.prefix_extractor,
        ROCKSDB_NAMESPACE::UnownedPtr<ROCKSDB_NAMESPACE::CompressionManager>(
            options.compression_manager.get()),
        env_options, internal_comparator,
        options.block_protection_bytes_per_key,
        /*skip_filters*/ false, /*immortal*/ false,
        /*force_direct_prefetch*/ false, /*level*/ -1,
        /*block_cache_tracer*/ nullptr,
        /*max_file_size_for_l0_meta_pin*/ 0, /*cur_db_session_id*/ "",
        /*cur_file_num*/ 0,
        /* unique_id */ {}, /* largest_seqno */ 0,
        /* tail_size */ 0, ioptions.persist_user_defined_timestamps);
    t_opt.largest_seqno = ROCKSDB_NAMESPACE::kMaxSequenceNumber;

    std::unique_ptr<ROCKSDB_NAMESPACE::TableReader> table_reader;
    ROCKSDB_NAMESPACE::Status table_status = options.table_factory->NewTableReader(
        read_options, t_opt, std::move(file_reader), file_size, &table_reader,
        false /* prefetch_index_and_filter_in_cache */);
    
    if (!table_status.ok() || !table_reader) {
      std::cerr << "[ERROR] GetOrOpen: NewTableReader failed for " << actual_path 
                << " (requested: " << sst_path << "): " << table_status.ToString() << std::flush << std::endl;
      return nullptr;
    }

    entry->reader = std::move(table_reader);
    CacheEntry* entry_ptr = entry.get();
    // 원본 경로와 실제 경로 모두 캐시에 저장
    cache_[sst_path] = std::move(entry);
    // actual_path가 다르면 별도로 캐시에 저장하지 않음 (메모리 절약)
    // 대신 원본 경로로만 캐시에 저장
    return entry_ptr;
  }

 private:
  void EvictLRU() {
    if (cache_.empty()) return;

    auto oldest_it = cache_.begin();
    auto oldest_time = oldest_it->second->last_access;

    for (auto it = cache_.begin(); it != cache_.end(); ++it) {
      if (it->second->last_access < oldest_time) {
        oldest_time = it->second->last_access;
        oldest_it = it;
      }
    }

    cache_.erase(oldest_it);
  }

  std::mutex cache_mutex_;
  std::unordered_map<std::string, std::unique_ptr<CacheEntry>> cache_;
  size_t max_size_;
};

// BloomCheckServiceImpl implements the gRPC service
class BloomCheckServiceImpl final : public BloomCheck::Service {
 public:
  explicit BloomCheckServiceImpl(size_t cache_size)
      : cache_(cache_size), rocksdb_options_() {
    // Configure RocksDB options for filter-only access
    rocksdb_options_.env = ROCKSDB_NAMESPACE::Env::Default();
    // Use default table factory (BlockBasedTable)
    // whole_key_filtering should be true (checked by caller)
    // partitioned_filters should be true (checked by caller)
  }

  grpc::Status Check(grpc::ServerContext* /*context*/,
                     const BatchRequest* request,
                     BatchResponse* response) override {
    static std::atomic<size_t> request_count{0};
    size_t req_id = request_count.fetch_add(1) + 1;
    
    if (req_id <= 10 || req_id % 100 == 0) {
      std::cerr << "[INFO] Check request #" << req_id 
                << " received: " << request->reqs_size() << " key request(s)" << std::flush << std::endl;
    }
    
    response->clear_resps();
    response->mutable_resps()->Reserve(request->reqs_size());

    // Process each key request
    for (const auto& key_req : request->reqs()) {
      KeyResponse key_resp;
      ProcessKeyRequest(key_req, &key_resp);
      *response->add_resps() = key_resp;
    }
    
    if (req_id <= 10 || req_id % 100 == 0) {
      std::cerr << "[INFO] Check request #" << req_id << " completed" << std::flush << std::endl;
    }

    return grpc::Status::OK;
  }

 private:
  void ProcessKeyRequest(const KeyRequest& req, KeyResponse* resp) {
    static std::atomic<size_t> key_request_count{0};
    size_t key_req_id = key_request_count.fetch_add(1) + 1;
    
    const ROCKSDB_NAMESPACE::Slice user_key(req.user_key());
    
    if (key_req_id <= 10 || key_req_id % 100 == 0) {
      std::cerr << "[INFO] ProcessKeyRequest #" << key_req_id 
                << ": key_len=" << user_key.size() 
                << ", sst_count=" << req.sst_paths_size() << std::flush << std::endl;
    }

    for (const std::string& sst_path : req.sst_paths()) {
      bool may_match = CheckKeyInSST(user_key, sst_path);
      if (may_match) {
        resp->add_may_match_sst_paths(sst_path);
      }
    }
    
    if (key_req_id <= 10 || key_req_id % 100 == 0) {
      std::cerr << "[INFO] ProcessKeyRequest #" << key_req_id 
                << " completed: " << resp->may_match_sst_paths_size() 
                << " may_match files" << std::flush << std::endl;
    }
  }

  bool CheckKeyInSST(const ROCKSDB_NAMESPACE::Slice& user_key,
                     const std::string& sst_path) {
    static std::atomic<size_t> call_count{0};
    size_t call_id = call_count.fetch_add(1) + 1;
    
    if (call_id <= 10) {
      std::cerr << "[DEBUG] CheckKeyInSST #" << call_id 
                << " called for key (hex) " << user_key.ToString(true) 
                << " in SST " << sst_path << std::flush << std::endl;
    }
    
    // Get or open TableReader
    TableReaderCache::CacheEntry* entry = cache_.GetOrOpen(sst_path, rocksdb_options_);
    if (!entry) {
      // Error opening file - safe to return true (may_match)
      if (call_id <= 10) {
        std::cerr << "[DEBUG] CheckKeyInSST #" << call_id 
                  << " failed to open SST file, returning true" << std::flush << std::endl;
      }
      return true;
    }

    // Lock entry for thread-safe access
    std::lock_guard<std::mutex> lock(entry->mutex);
    entry->last_access = std::chrono::steady_clock::now();

    ROCKSDB_NAMESPACE::TableReader* reader = entry->reader.get();
    if (!reader) {
      if (call_id <= 10) {
        std::cerr << "[DEBUG] CheckKeyInSST #" << call_id 
                  << " reader is null, returning true" << std::flush << std::endl;
      }
      return true;  // Safe default
    }

    // Cast to BlockBasedTable to access filter
    // Since we opened the table with BlockBasedTableFactory, it should be BlockBasedTable
    // Use static_cast instead of dynamic_cast to avoid RTTI dependency
    ROCKSDB_NAMESPACE::BlockBasedTable* bb_table =
        static_cast<ROCKSDB_NAMESPACE::BlockBasedTable*>(reader);
    if (!bb_table) {
      // Safety check - return true (may_match)
      if (call_id <= 10) {
        std::cerr << "[DEBUG] CheckKeyInSST #" << call_id 
                  << " bb_table is null, returning true" << std::flush << std::endl;
      }
      return true;
    }

    // Get filter from BlockBasedTable
    const ROCKSDB_NAMESPACE::BlockBasedTable::Rep* rep = bb_table->get_rep();
    if (!rep || !rep->filter) {
      // No filter - return true (may_match)
      if (call_id <= 10) {
        std::cerr << "[DEBUG] CheckKeyInSST #" << call_id 
                  << " no filter in SST, returning true" << std::flush << std::endl;
      }
      return true;
    }

    // Check whole_key_filtering
    if (!rep->whole_key_filtering) {
      // Not whole key filtering - return true (may_match)
      if (call_id <= 10) {
        std::cerr << "[DEBUG] CheckKeyInSST #" << call_id 
                  << " whole_key_filtering is false, returning true" << std::flush << std::endl;
      }
      return true;
    }

    // Create minimal GetContext for filter check
    ROCKSDB_NAMESPACE::ReadOptions read_options;
    read_options.read_tier = ROCKSDB_NAMESPACE::kReadAllTier;

    // Get comparator
    const ROCKSDB_NAMESPACE::Comparator* ucmp =
        rep->internal_comparator.user_comparator();

    // Create minimal GetContext
    // We don't need value, just filter check
    ROCKSDB_NAMESPACE::PinnableSlice dummy_value;
    bool dummy_value_found = false;
    ROCKSDB_NAMESPACE::MergeContext dummy_merge_context;
    ROCKSDB_NAMESPACE::SequenceNumber dummy_max_covering_tombstone_seq = 0;

    ROCKSDB_NAMESPACE::GetContext get_context(
        ucmp, nullptr /* merge_operator */, nullptr /* logger */,
        nullptr /* statistics */, ROCKSDB_NAMESPACE::GetContext::kNotFound,
        user_key, &dummy_value, nullptr /* columns */, &dummy_value_found,
        &dummy_merge_context, false /* do_merge */,
        &dummy_max_covering_tombstone_seq,
        ROCKSDB_NAMESPACE::SystemClock::Default().get());

    // Create BlockCacheLookupContext
    ROCKSDB_NAMESPACE::BlockCacheLookupContext lookup_context(
        ROCKSDB_NAMESPACE::TableReaderCaller::kUserGet, 0 /* tracing_get_id */,
        false /* get_from_user_specified_snapshot */);

    // Strip timestamp if needed
    size_t ts_sz = ucmp->timestamp_size();
    ROCKSDB_NAMESPACE::Slice user_key_without_ts =
        ROCKSDB_NAMESPACE::StripTimestampFromUserKey(user_key, ts_sz);

    if (call_id <= 10) {
      std::cerr << "[DEBUG] CheckKeyInSST #" << call_id 
                << " calling KeyMayMatch..." << std::flush << std::endl;
    }
    
    // Check filter
    // For whole_key_filtering, we use KeyMayMatch
    // const_ikey_ptr can be nullptr for whole key filtering
    bool may_match = rep->filter->KeyMayMatch(
        user_key_without_ts, nullptr /* const_ikey_ptr */, &get_context,
        &lookup_context, read_options);

    // If filter check fails (returns false), may_match is false
    // If filter check succeeds or errors, may_match is true
    // This is safe: false negative is prevented (errors return true)
    
    // Debug: Log filter result
    // 처음 10번, 그리고 이후 100번마다, 또는 false일 때 항상 로그
    if (call_id <= 10 || call_id % 100 == 0 || !may_match) {
      std::cerr << "[DEBUG] KeyMayMatch #" << call_id 
                << " for key (hex) " << user_key_without_ts.ToString(true) 
                << " in SST " << sst_path 
                << ": " << (may_match ? "MAY_MATCH" : "NOT_MATCH") << std::flush << std::endl;
    }
    
    // 첫 요청 시 서버 시작 로그 출력
    if (call_id == 1) {
      std::cerr << "[INFO] First BloomCheck request received and processed" << std::flush << std::endl;
    }
    
    return may_match;
  }

  TableReaderCache cache_;
  ROCKSDB_NAMESPACE::Options rocksdb_options_;
};

}  // namespace bloomcheck

// Server main function
void RunServer(const std::string& server_address, size_t cache_size) {
  bloomcheck::BloomCheckServiceImpl service(cache_size);

  grpc::EnableDefaultHealthCheckService(true);
  // grpc::reflection::InitProtoReflectionServerBuilderPlugin();  // Optional: remove if not needed

  grpc::ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  
  // 스레드 풀 설정: 동시 요청 처리를 위한 completion queue 스레드 수
  // 
  // gRPC 기본 동작:
  // - 비동기 completion queue 사용
  // - 여러 요청이 동시에 들어오면 자동으로 병렬 처리
  // - 기본 스레드 수: CPU 코어 수 (시스템 리소스에 따라 자동 조정)
  //
  // 명시적으로 스레드 개수를 제한하려면:
  // builder.SetResourceQuota(grpc::ResourceQuota().SetMaxThreads(num_threads));
  // 또는 completion queue를 직접 관리하여 스레드 수 제어 가능
  
  std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
  std::cout << "BloomCheck server listening on " << server_address << std::endl;
  std::cout << "Server will handle requests concurrently (default: multiple threads)" << std::endl;

  server->Wait();
}

int main(int argc, char** argv) {
  std::string server_address = "0.0.0.0:50051";
  size_t cache_size = 100;  // Max 100 SST files in cache

  if (argc > 1) {
    server_address = argv[1];
  }
  if (argc > 2) {
    cache_size = std::stoull(argv[2]);
  }

  RunServer(server_address, cache_size);
  return 0;
}

