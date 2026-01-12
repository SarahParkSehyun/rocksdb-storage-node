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
#include "rocksdb/filter_policy.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/sst_file_reader.h"
#include "rocksdb/table.h"
#include "table/block_based/block_based_table_reader.h"
#include "table/get_context.h"
#include "table/table_reader.h"
#include "table/table_builder.h"
#include "util/cast_util.h"

namespace bloomcheck {

// Reason codes for why CheckKeyInSST returned true/false
enum class CheckReason {
  OPEN_FAIL,              // GetOrOpen returned nullptr
  READER_NULL,            // entry->reader is null
  BB_TABLE_NULL,          // bb_table is null (shouldn't happen)
  REP_NULL,               // rep is null
  FILTER_NULL,            // rep->filter is null
  WHOLE_KEY_FILTERING_FALSE,  // rep->whole_key_filtering is false
  UCOMP_NULL,             // ucmp is null (shouldn't happen)
  KEYMAYMATCH_EXCEPTION,  // Exception during KeyMayMatch
  KEYMAYMATCH_TRUE,       // KeyMayMatch returned true
  KEYMAYMATCH_FALSE       // KeyMayMatch returned false
};

// Statistics counters
struct Stats {
  std::atomic<size_t> total_checks{0};
  std::atomic<size_t> open_fail_getfilesize{0};
  std::atomic<size_t> open_fail_newfile{0};
  std::atomic<size_t> open_fail_newreader{0};
  std::atomic<size_t> reason_open_fail{0};
  std::atomic<size_t> reason_reader_null{0};
  std::atomic<size_t> reason_bb_table_null{0};
  std::atomic<size_t> reason_rep_null{0};
  std::atomic<size_t> reason_filter_null{0};
  std::atomic<size_t> reason_whole_key_filtering_false{0};
  std::atomic<size_t> reason_ucomp_null{0};
  std::atomic<size_t> reason_keymaymatch_exception{0};
  std::atomic<size_t> reason_keymaymatch_true{0};
  std::atomic<size_t> reason_keymaymatch_false{0};
  std::atomic<size_t> keymaymatch_calls{0};
  
  void RecordReason(CheckReason reason) {
    total_checks++;
    switch (reason) {
      case CheckReason::OPEN_FAIL:
        reason_open_fail++;
        break;
      case CheckReason::READER_NULL:
        reason_reader_null++;
        break;
      case CheckReason::BB_TABLE_NULL:
        reason_bb_table_null++;
        break;
      case CheckReason::REP_NULL:
        reason_rep_null++;
        break;
      case CheckReason::FILTER_NULL:
        reason_filter_null++;
        break;
      case CheckReason::WHOLE_KEY_FILTERING_FALSE:
        reason_whole_key_filtering_false++;
        break;
      case CheckReason::UCOMP_NULL:
        reason_ucomp_null++;
        break;
      case CheckReason::KEYMAYMATCH_EXCEPTION:
        reason_keymaymatch_exception++;
        break;
      case CheckReason::KEYMAYMATCH_TRUE:
        reason_keymaymatch_true++;
        keymaymatch_calls++;
        break;
      case CheckReason::KEYMAYMATCH_FALSE:
        reason_keymaymatch_false++;
        keymaymatch_calls++;
        break;
    }
  }
  
  void PrintStats(std::mutex& log_mutex) {
    std::lock_guard<std::mutex> lock(log_mutex);
    size_t total = total_checks.load();
    size_t km_calls = keymaymatch_calls.load();
    size_t fallback_true = reason_open_fail.load() + reason_reader_null.load() +
                           reason_bb_table_null.load() + reason_rep_null.load() +
                           reason_filter_null.load() + reason_whole_key_filtering_false.load() +
                           reason_ucomp_null.load() + reason_keymaymatch_exception.load();
    
    double fallback_true_ratio = total > 0 ? (double)fallback_true / total : 0.0;
    double km_true_ratio = km_calls > 0 ? (double)reason_keymaymatch_true.load() / km_calls : 0.0;
    double km_false_ratio = km_calls > 0 ? (double)reason_keymaymatch_false.load() / km_calls : 0.0;
    
    std::cerr << "[STATS] ===== BloomCheck Statistics =====" << std::endl;
    std::cerr << "[STATS] total_checks: " << total << std::endl;
    std::cerr << "[STATS] open_fail_getfilesize: " << open_fail_getfilesize.load() << std::endl;
    std::cerr << "[STATS] open_fail_newfile: " << open_fail_newfile.load() << std::endl;
    std::cerr << "[STATS] open_fail_newreader: " << open_fail_newreader.load() << std::endl;
    std::cerr << "[STATS] reason_open_fail: " << reason_open_fail.load() << std::endl;
    std::cerr << "[STATS] reason_reader_null: " << reason_reader_null.load() << std::endl;
    std::cerr << "[STATS] reason_bb_table_null: " << reason_bb_table_null.load() << std::endl;
    std::cerr << "[STATS] reason_rep_null: " << reason_rep_null.load() << std::endl;
    std::cerr << "[STATS] reason_filter_null: " << reason_filter_null.load() << std::endl;
    std::cerr << "[STATS] reason_whole_key_filtering_false: " << reason_whole_key_filtering_false.load() << std::endl;
    std::cerr << "[STATS] reason_ucomp_null: " << reason_ucomp_null.load() << std::endl;
    std::cerr << "[STATS] reason_keymaymatch_exception: " << reason_keymaymatch_exception.load() << std::endl;
    std::cerr << "[STATS] reason_keymaymatch_true: " << reason_keymaymatch_true.load() << std::endl;
    std::cerr << "[STATS] reason_keymaymatch_false: " << reason_keymaymatch_false.load() << std::endl;
    std::cerr << "[STATS] keymaymatch_calls: " << km_calls << std::endl;
    std::cerr << "[STATS] fallback_true_ratio: " << fallback_true_ratio << " (" << fallback_true << "/" << total << ")" << std::endl;
    std::cerr << "[STATS] km_true_ratio: " << km_true_ratio << " (" << reason_keymaymatch_true.load() << "/" << km_calls << ")" << std::endl;
    std::cerr << "[STATS] km_false_ratio: " << km_false_ratio << " (" << reason_keymaymatch_false.load() << "/" << km_calls << ")" << std::endl;
    std::cerr << "[STATS] =================================" << std::endl;
  }
};

// LRU cache for TableReader
class TableReaderCache {
 public:
  struct CacheEntry {
    std::unique_ptr<ROCKSDB_NAMESPACE::TableReader> reader;
    ROCKSDB_NAMESPACE::InternalKeyComparator internal_comparator;
    ROCKSDB_NAMESPACE::ImmutableOptions ioptions;
    std::chrono::steady_clock::time_point last_access;
    std::mutex mutex;  // Per-entry lock for thread safety
    
    explicit CacheEntry(const ROCKSDB_NAMESPACE::Options& opt)
        : internal_comparator(opt.comparator),
          ioptions(opt),
          last_access(std::chrono::steady_clock::now()) {
      // ImmutableOptions constructor sets fs from env automatically
      // (ImmutableDBOptions constructor sets: fs = env->GetFileSystem())
    }
  };

  explicit TableReaderCache(size_t max_size) : max_size_(max_size) {}

  // Get or create TableReader for SST file
  // Returns nullptr on error (safe to treat as may_match=true)
  CacheEntry* GetOrOpen(const std::string& sst_path,
                        const ROCKSDB_NAMESPACE::Options& options,
                        Stats* stats, std::mutex* log_mutex, std::atomic<size_t>* open_count) {
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
    // Create a new entry with ImmutableOptions and InternalKeyComparator
    auto entry = std::make_unique<CacheEntry>(options);

    // Open using BlockBasedTable directly
    ROCKSDB_NAMESPACE::EnvOptions env_options;
    ROCKSDB_NAMESPACE::ReadOptions read_options;
    read_options.read_tier = ROCKSDB_NAMESPACE::kReadAllTier;  // Allow file reads

    uint64_t file_size = 0;
    std::unique_ptr<ROCKSDB_NAMESPACE::FSRandomAccessFile> file;
    const auto& fs = options.env->GetFileSystem();
    ROCKSDB_NAMESPACE::FileOptions fopts(env_options);
    
    // actual_path 사용 (변환된 경로)
    size_t open_id = (open_count ? (*open_count)++ : 0);
    bool cache_hit = false;
    
    ROCKSDB_NAMESPACE::IOStatus s = fs->GetFileSize(actual_path, fopts.io_options, &file_size, nullptr);
    if (!s.ok()) {
      if (stats) stats->open_fail_getfilesize++;
      // 샘플 로그 (초기 50회 + 매 1000회)
      if (open_id <= 50 || open_id % 1000 == 0) {
        std::lock_guard<std::mutex> log_lock(*log_mutex);
        std::cerr << "[SAMPLE] GetOrOpen #" << open_id 
                  << " FAIL: GetFileSize failed" << std::endl;
        std::cerr << "[SAMPLE]   sst_path: " << sst_path << std::endl;
        std::cerr << "[SAMPLE]   actual_path: " << actual_path << std::endl;
        std::cerr << "[SAMPLE]   cache_hit: " << (cache_hit ? "yes" : "no") << std::endl;
        std::cerr << "[SAMPLE]   error: " << s.ToString() << std::endl;
      }
      return nullptr;
    }

    s = fs->NewRandomAccessFile(actual_path, fopts, &file, nullptr);
    if (!s.ok()) {
      if (stats) stats->open_fail_newfile++;
      // 샘플 로그
      if (open_id <= 50 || open_id % 1000 == 0) {
        std::lock_guard<std::mutex> log_lock(*log_mutex);
        std::cerr << "[SAMPLE] GetOrOpen #" << open_id 
                  << " FAIL: NewRandomAccessFile failed" << std::endl;
        std::cerr << "[SAMPLE]   sst_path: " << sst_path << std::endl;
        std::cerr << "[SAMPLE]   actual_path: " << actual_path << std::endl;
        std::cerr << "[SAMPLE]   file_size: " << file_size << std::endl;
        std::cerr << "[SAMPLE]   cache_hit: " << (cache_hit ? "yes" : "no") << std::endl;
        std::cerr << "[SAMPLE]   error: " << s.ToString() << std::endl;
      }
      return nullptr;
    }

    std::unique_ptr<ROCKSDB_NAMESPACE::RandomAccessFileReader> file_reader(
        new ROCKSDB_NAMESPACE::RandomAccessFileReader(std::move(file), actual_path));

    // Use entry->ioptions and entry->internal_comparator to ensure lifetime
    const auto& ioptions = entry->ioptions;
    
    ROCKSDB_NAMESPACE::TableReaderOptions t_opt(
        ioptions, options.prefix_extractor,
        ROCKSDB_NAMESPACE::UnownedPtr<ROCKSDB_NAMESPACE::CompressionManager>(
            options.compression_manager.get()),
        env_options, entry->internal_comparator,
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
      if (stats) stats->open_fail_newreader++;
      // 샘플 로그
      if (open_id <= 50 || open_id % 1000 == 0) {
        std::lock_guard<std::mutex> log_lock(*log_mutex);
        std::cerr << "[SAMPLE] GetOrOpen #" << open_id 
                  << " FAIL: NewTableReader failed" << std::endl;
        std::cerr << "[SAMPLE]   sst_path: " << sst_path << std::endl;
        std::cerr << "[SAMPLE]   actual_path: " << actual_path << std::endl;
        std::cerr << "[SAMPLE]   file_size: " << file_size << std::endl;
        std::cerr << "[SAMPLE]   cache_hit: " << (cache_hit ? "yes" : "no") << std::endl;
        std::cerr << "[SAMPLE]   status: " << table_status.ToString() << std::endl;
      }
      return nullptr;
    }

    // 샘플 로그 (성공한 경우) 및 table properties 로그
    if (open_id <= 50 || open_id % 1000 == 0) {
      std::lock_guard<std::mutex> log_lock(*log_mutex);
      std::cerr << "[SAMPLE] GetOrOpen #" << open_id 
                << " SUCCESS" << std::endl;
      std::cerr << "[SAMPLE]   sst_path: " << sst_path << std::endl;
      std::cerr << "[SAMPLE]   actual_path: " << actual_path << std::endl;
      std::cerr << "[SAMPLE]   file_size: " << file_size << std::endl;
      std::cerr << "[SAMPLE]   cache_hit: " << (cache_hit ? "yes" : "no") << std::endl;
      std::cerr << "[SAMPLE]   status: OK" << std::endl;
      
      // Table properties 로그
      auto props = table_reader->GetTableProperties();
      if (props) {
        std::cerr << "[SAMPLE]   table_properties.filter_policy_name: " << props->filter_policy_name << std::endl;
      } else {
        std::cerr << "[SAMPLE]   table_properties: null" << std::endl;
      }
      
      // BlockBasedTable::Rep 정보 로그
      ROCKSDB_NAMESPACE::BlockBasedTable* bb_table =
          static_cast<ROCKSDB_NAMESPACE::BlockBasedTable*>(table_reader.get());
      if (bb_table) {
        const ROCKSDB_NAMESPACE::BlockBasedTable::Rep* rep = bb_table->get_rep();
        if (rep) {
          std::cerr << "[SAMPLE]   rep->filter_policy: " << (rep->table_options.filter_policy ? rep->table_options.filter_policy.get() : nullptr) << std::endl;
          if (rep->table_options.filter_policy) {
            std::cerr << "[SAMPLE]   rep->filter_policy->Name(): " << rep->table_options.filter_policy->Name() << std::endl;
          }
          std::cerr << "[SAMPLE]   rep->filter: " << (rep->filter ? rep->filter.get() : nullptr) << std::endl;
          std::cerr << "[SAMPLE]   rep->whole_key_filtering: " << (rep->whole_key_filtering ? "true" : "false") << std::endl;
          std::cerr << "[SAMPLE]   rep->index_type: " << static_cast<int>(rep->index_type) << std::endl;
        } else {
          std::cerr << "[SAMPLE]   rep: null" << std::endl;
        }
      }
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
      : cache_(cache_size), rocksdb_options_(),
        clock_(ROCKSDB_NAMESPACE::SystemClock::Default()),
        fs_(ROCKSDB_NAMESPACE::Env::Default()->GetFileSystem()) {
    // Configure RocksDB options for filter-only access
    rocksdb_options_.env = ROCKSDB_NAMESPACE::Env::Default();
    // FileSystem lifetime is maintained by fs_ member
    // ImmutableOptions constructor will set fs from env->GetFileSystem()
    
    // Set BlockBasedTableOptions with filter_policy
    ROCKSDB_NAMESPACE::BlockBasedTableOptions bbto;
    bbto.filter_policy.reset(ROCKSDB_NAMESPACE::NewBloomFilterPolicy(10, false));
    bbto.whole_key_filtering = true;
    bbto.partition_filters = true;
    rocksdb_options_.table_factory.reset(
        ROCKSDB_NAMESPACE::NewBlockBasedTableFactory(bbto));
    
    StartStatsThread();
  }
  
  ~BloomCheckServiceImpl() {
    StopStatsThread();
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
    
    // 샘플링 로그: key hex, 입력 sst_paths, 출력 may_match_paths
    // may_match_count > 0일 때만 출력 (확인을 수월하게 하기 위해)
    size_t may_match_count = resp->may_match_sst_paths_size();
    bool should_log = (may_match_count > 0);
    
    // 디버깅: CheckKeyInSST가 true를 반환했는데도 추가되지 않는 경우 확인
    if (key_req_id <= 50) {
      std::lock_guard<std::mutex> log_lock(log_mutex_);
      std::cerr << "[DEBUG] ProcessKeyRequest #" << key_req_id 
                << " after loop: may_match_count=" << may_match_count 
                << ", input_sst_count=" << req.sst_paths_size() << std::endl;
    }
    
    if (should_log) {
      std::lock_guard<std::mutex> log_lock(log_mutex_);
      
      // key 앞 8바이트 hex (key가 8바이트 이상인 경우)
      std::string key_hex_prefix;
      if (user_key.size() >= 8) {
        key_hex_prefix = ROCKSDB_NAMESPACE::Slice(user_key.data(), 8).ToString(true);
      } else {
        key_hex_prefix = ROCKSDB_NAMESPACE::Slice(user_key).ToString(true);
      }
      
      std::cerr << "[SAMPLE] ProcessKeyRequest #" << key_req_id << std::endl;
      std::cerr << "[SAMPLE]   key_hex_prefix: " << key_hex_prefix << std::endl;
      std::cerr << "[SAMPLE]   input_sst_paths_count: " << req.sst_paths_size() << std::endl;
      
      // 입력 sst_paths 목록 (최대 10개까지만)
      size_t input_count = std::min(static_cast<size_t>(req.sst_paths_size()), size_t(10));
      for (size_t i = 0; i < input_count; i++) {
        std::cerr << "[SAMPLE]   input_sst_paths[" << i << "]: " << req.sst_paths(i) << std::endl;
      }
      if (req.sst_paths_size() > 10) {
        std::cerr << "[SAMPLE]   ... (" << (req.sst_paths_size() - 10) << " more input paths)" << std::endl;
      }
      
      std::cerr << "[SAMPLE]   output_may_match_count: " << may_match_count << std::endl;
      
      // 출력 may_match_paths 목록
      if (may_match_count > 0) {
        for (size_t i = 0; i < may_match_count; i++) {
          std::cerr << "[SAMPLE]   may_match_paths[" << i << "]: " << resp->may_match_sst_paths(i) << std::endl;
        }
      } else {
        std::cerr << "[SAMPLE]   may_match_paths: (none)" << std::endl;
      }
    }
    
    if (key_req_id <= 10 || key_req_id % 100 == 0) {
      std::cerr << "[INFO] ProcessKeyRequest #" << key_req_id 
                << " completed: " << may_match_count 
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
    TableReaderCache::CacheEntry* entry = cache_.GetOrOpen(sst_path, rocksdb_options_, &stats_, &log_mutex_, &open_count_);
    if (!entry) {
      // Error opening file - safe to return true (may_match)
      stats_.RecordReason(CheckReason::OPEN_FAIL);
      if (call_id <= 10) {
        std::lock_guard<std::mutex> log_lock(log_mutex_);
        std::cerr << "[DEBUG] CheckKeyInSST #" << call_id 
                  << " failed to open SST file, returning true (reason=OPEN_FAIL)" << std::flush << std::endl;
      }
      return true;
    }

    // Lock entry for thread-safe access
    std::lock_guard<std::mutex> lock(entry->mutex);
    entry->last_access = std::chrono::steady_clock::now();

    ROCKSDB_NAMESPACE::TableReader* reader = entry->reader.get();
    if (!reader) {
      stats_.RecordReason(CheckReason::READER_NULL);
      return true;  // Safe default
    }

    // Cast to BlockBasedTable to access filter
    // Since we opened the table with BlockBasedTableFactory, it should be BlockBasedTable
    ROCKSDB_NAMESPACE::BlockBasedTable* bb_table =
        static_cast<ROCKSDB_NAMESPACE::BlockBasedTable*>(reader);
    if (!bb_table) {
      // Safety check - return true (may_match)
      stats_.RecordReason(CheckReason::BB_TABLE_NULL);
      return true;
    }

    // Get filter from BlockBasedTable
    const ROCKSDB_NAMESPACE::BlockBasedTable::Rep* rep = bb_table->get_rep();
    if (!rep) {
      stats_.RecordReason(CheckReason::REP_NULL);
      return true;
    }
    
    // FileSystem 포인터 로그 (call_id <= 20)
    if (call_id <= 20) {
      std::lock_guard<std::mutex> log_lock(log_mutex_);
      std::cerr << "[SAMPLE] CheckKeyInSST #" << call_id << " rep obtained" << std::endl;
      std::cerr << "[SAMPLE]   rocksdb_options_.env=" << (void*)rocksdb_options_.env << std::endl;
      if (rocksdb_options_.env) {
        auto env_fs = rocksdb_options_.env->GetFileSystem();
        std::cerr << "[SAMPLE]   rocksdb_options_.env->GetFileSystem().get()=" << (void*)env_fs.get() << std::endl;
      }
      std::cerr << "[SAMPLE]   rep->ioptions.fs.get()=" << (void*)rep->ioptions.fs.get() << std::endl;
    }
    
    // KeyMayMatch 호출 직전 fs null 체크
    if (!rep->ioptions.fs || !rep->ioptions.fs.get()) {
      stats_.RecordReason(CheckReason::FILTER_NULL);  // 임시로 FILTER_NULL 사용
      if (call_id <= 20) {
        std::lock_guard<std::mutex> log_lock(log_mutex_);
        std::cerr << "[ERROR] CheckKeyInSST #" << call_id << " rep->ioptions.fs is null -> fallback true" << std::endl;
      }
      return true;
    }
    
    if (!rep->filter) {
      stats_.RecordReason(CheckReason::FILTER_NULL);
      
      // Rate-limited logging (1초에 1번)
      auto now = std::chrono::steady_clock::now();
      auto last_log = last_filter_null_log_time_.load();
      if (now - last_log >= std::chrono::seconds(1)) {
        if (last_filter_null_log_time_.compare_exchange_strong(last_log, now)) {
          std::lock_guard<std::mutex> log_lock(log_mutex_);
          std::cerr << "[WARN] FilterNull: sst_path=" << sst_path << std::endl;
          
          // Table properties에서 filter_policy_name 가져오기
          auto props = bb_table->GetTableProperties();
          if (props) {
            std::cerr << "[WARN]   table_properties.filter_policy_name: " << props->filter_policy_name << std::endl;
          } else {
            std::cerr << "[WARN]   table_properties: null" << std::endl;
          }
          
          std::cerr << "[WARN]   rep->filter_policy_name: " << (rep->table_options.filter_policy ? rep->table_options.filter_policy->Name() : "null") << std::endl;
          std::cerr << "[WARN]   rep->filter_policy exists: " << (rep->table_options.filter_policy ? "yes" : "no") << std::endl;
        }
      }
      
      if (call_id <= 10) {
        std::lock_guard<std::mutex> log_lock(log_mutex_);
        std::cerr << "[DEBUG] CheckKeyInSST #" << call_id 
                  << " no filter in SST, returning true (reason=FILTER_NULL)" << std::flush << std::endl;
      }
      return true;
    }

    // Check whole_key_filtering
    if (!rep->whole_key_filtering) {
      stats_.RecordReason(CheckReason::WHOLE_KEY_FILTERING_FALSE);
      return true;
    }

    // Create minimal GetContext for filter check
    ROCKSDB_NAMESPACE::ReadOptions read_options;
    read_options.read_tier = ROCKSDB_NAMESPACE::kReadAllTier;

    // Get comparator
    const ROCKSDB_NAMESPACE::Comparator* ucmp =
        rep->internal_comparator.user_comparator();
    if (!ucmp) {
      stats_.RecordReason(CheckReason::UCOMP_NULL);
      return true;
    }

    // Create minimal GetContext
    // We don't need value, just filter check
    ROCKSDB_NAMESPACE::PinnableSlice dummy_value;
    bool dummy_value_found = false;
    ROCKSDB_NAMESPACE::MergeContext dummy_merge_context;
    ROCKSDB_NAMESPACE::SequenceNumber dummy_max_covering_tombstone_seq = 0;

    // Strip timestamp if needed
    size_t ts_sz = ucmp->timestamp_size();
    ROCKSDB_NAMESPACE::Slice user_key_without_ts =
        ROCKSDB_NAMESPACE::StripTimestampFromUserKey(user_key, ts_sz);

    ROCKSDB_NAMESPACE::GetContext get_context(
        ucmp, nullptr /* merge_operator */, nullptr /* logger */,
        nullptr /* statistics */, ROCKSDB_NAMESPACE::GetContext::kNotFound,
        user_key_without_ts, &dummy_value, nullptr /* columns */, &dummy_value_found,
        &dummy_merge_context, false /* do_merge */,
        &dummy_max_covering_tombstone_seq,
        clock_.get());

    // Create BlockCacheLookupContext
    ROCKSDB_NAMESPACE::BlockCacheLookupContext lookup_context(
        ROCKSDB_NAMESPACE::TableReaderCaller::kUserGet, 0 /* tracing_get_id */,
        false /* get_from_user_specified_snapshot */);

    // A/B 테스트: const_ikey_ptr=nullptr 모드 (환경변수로 토글)
    const char* use_null_ikey_env = std::getenv("BLOOMCHECK_USE_NULL_IKEY");
    bool use_null_ikey = (use_null_ikey_env && strlen(use_null_ikey_env) > 0 && use_null_ikey_env[0] == '1');
    
    const ROCKSDB_NAMESPACE::Slice* const_ikey_ptr = nullptr;
    ROCKSDB_NAMESPACE::InternalKey ikey_for_seek;
    ROCKSDB_NAMESPACE::Slice ikey_slice;
    
    if (!use_null_ikey) {
      // Generate internal key for partitioned filter lookup
      ikey_for_seek = ROCKSDB_NAMESPACE::InternalKey(
          user_key_without_ts, ROCKSDB_NAMESPACE::kMaxSequenceNumber,
          ROCKSDB_NAMESPACE::kValueTypeForSeek);
      ikey_slice = ikey_for_seek.Encode();
      const_ikey_ptr = &ikey_slice;
    }
    
    // 디버깅 로그 (call_id <= 20)
    if (call_id <= 20) {
      std::lock_guard<std::mutex> log_lock(log_mutex_);
      std::cerr << "[SAMPLE] CheckKeyInSST #" << call_id << std::endl;
      std::cerr << "[SAMPLE]   clock_ptr=" << (void*)clock_.get() << std::endl;
      if (rocksdb_options_.env) {
        std::cerr << "[SAMPLE]   fs_ptr=" << (void*)rocksdb_options_.env->GetFileSystem().get() << std::endl;
      }
      std::cerr << "[SAMPLE]   filter_ptr=" << (void*)rep->filter.get() << std::endl;
      std::cerr << "[SAMPLE]   whole_key_filtering=" << (rep->whole_key_filtering ? "true" : "false") << std::endl;
      std::cerr << "[SAMPLE]   index_key_includes_seq=" << (rep->index_key_includes_seq ? "true" : "false") << std::endl;
    }
    
    // Check filter
    bool may_match = false;
    try {
      may_match = rep->filter->KeyMayMatch(
          user_key_without_ts, const_ikey_ptr, &get_context,
          &lookup_context, read_options);
      
      if (call_id <= 20) {
        std::lock_guard<std::mutex> log_lock(log_mutex_);
        std::cerr << "[SAMPLE]   KeyMayMatch_result=" << (may_match ? "true" : "false") << std::endl;
      }
      
      if (may_match) {
        stats_.RecordReason(CheckReason::KEYMAYMATCH_TRUE);
      } else {
        stats_.RecordReason(CheckReason::KEYMAYMATCH_FALSE);
      }
    } catch (...) {
      stats_.RecordReason(CheckReason::KEYMAYMATCH_EXCEPTION);
      return true;  // Safe default on exception
    }
    
    return may_match;
  }

  TableReaderCache cache_;
  ROCKSDB_NAMESPACE::Options rocksdb_options_;
  Stats stats_;
  std::mutex log_mutex_;
  std::atomic<size_t> open_count_{0};
  std::shared_ptr<ROCKSDB_NAMESPACE::SystemClock> clock_;
  std::shared_ptr<ROCKSDB_NAMESPACE::FileSystem> fs_;
  
  // Stats printing thread
  std::thread stats_thread_;
  std::atomic<bool> stop_stats_thread_{false};
  
  // Rate-limited logging for filter null cases
  std::atomic<std::chrono::steady_clock::time_point> last_filter_null_log_time_{
      std::chrono::steady_clock::time_point::min()};
  
  void StartStatsThread() {
    stats_thread_ = std::thread([this]() {
      while (!stop_stats_thread_.load()) {
        std::this_thread::sleep_for(std::chrono::seconds(10));
        stats_.PrintStats(log_mutex_);
      }
    });
  }
  
  void StopStatsThread() {
    stop_stats_thread_ = true;
    if (stats_thread_.joinable()) {
      stats_thread_.join();
    }
  }
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

