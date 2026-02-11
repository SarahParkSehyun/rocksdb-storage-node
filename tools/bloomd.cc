// Copyright (c) 2024-present, RocksDB Contributors
// This source code is licensed under both the GPLv2 and Apache 2.0 License

#include "tools/bloomcheck.grpc.pb.h"
#include "tools/bloomcheck.pb.h"

#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>
#include <grpc/support/log.h>

#include <array>
#include <atomic>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <chrono>
#include <condition_variable>
#include <future>
#include <iomanip>
#include <iostream>
#include <list>
#include <memory>
#include <mutex>
#include <queue>
#include <shared_mutex>
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
  
  // Cache statistics
  std::atomic<size_t> cache_hits{0};
  std::atomic<size_t> cache_misses{0};
  std::atomic<size_t> evictions{0};
  
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
  
  void PrintStats(std::mutex& log_mutex, size_t total_opens, size_t active_rpcs) {
    std::lock_guard<std::mutex> lock(log_mutex);
    
    size_t hits = cache_hits.load();
    size_t misses = cache_misses.load();
    size_t total_accesses = hits + misses;
    double cache_hit_rate = total_accesses > 0 ? (double)hits / total_accesses : 0.0;
    size_t evictions_count = evictions.load();
    
    // std::cerr << "[STATS] cache_hit_rate: " << cache_hit_rate 
    //           << " (" << hits << "/" << total_accesses << ")" << std::endl;
    // std::cerr << "[STATS] evictions: " << evictions_count << std::endl;
    // std::cerr << "[STATS] NewTableReader(open): " << total_opens << std::endl;
    // std::cerr << "[STATS] active_rpcs: " << active_rpcs << std::endl;
  }
};

// Helper function to generate SST file path from file number
// Tries plain format first, then zero-padded format (6 digits) as fallback
static std::string MakeSstPath(uint64_t file_number) {
  const char* storage_path_env = std::getenv("BLOOMCHECK_STORAGE_PATH");
  std::string base_path;
  
  if (storage_path_env && strlen(storage_path_env) > 0) {
    base_path = storage_path_env;
    if (base_path.back() != '/') {
      base_path += '/';
    }
  } else {
    base_path = "/mnt/kvafa/db_bench/";
  }
  
  // First try: plain format (e.g., "16916.sst")
  return base_path + std::to_string(file_number) + ".sst";
}

// Try to open SST file with fallback to zero-padded format
static std::string TryOpenSstPath(uint64_t file_number, 
                                  ROCKSDB_NAMESPACE::FileSystem* fs,
                                  const ROCKSDB_NAMESPACE::FileOptions& fopts,
                                  uint64_t* file_size) {
  std::string base_path;
  const char* storage_path_env = std::getenv("BLOOMCHECK_STORAGE_PATH");
  
  if (storage_path_env && strlen(storage_path_env) > 0) {
    base_path = storage_path_env;
    if (base_path.back() != '/') {
      base_path += '/';
    }
  } else {
    base_path = "/mnt/kvafa/db_bench/";
  }
  
  // Try 1: plain format (e.g., "16916.sst")
  std::string path1 = base_path + std::to_string(file_number) + ".sst";
  ROCKSDB_NAMESPACE::IOStatus s1 = fs->GetFileSize(path1, fopts.io_options, file_size, nullptr);
  if (s1.ok()) {
    return path1;
  }
  
  // Try 2: 6-digit zero-padded format (e.g., "016916.sst")
  char padded[32];
  snprintf(padded, sizeof(padded), "%06llu", static_cast<unsigned long long>(file_number));
  std::string path2 = base_path + std::string(padded) + ".sst";
  ROCKSDB_NAMESPACE::IOStatus s2 = fs->GetFileSize(path2, fopts.io_options, file_size, nullptr);
  if (s2.ok()) {
    return path2;
  }
  
  // Try 3: 7-digit zero-padded format (e.g., "0016916.sst")
  snprintf(padded, sizeof(padded), "%07llu", static_cast<unsigned long long>(file_number));
  std::string path3 = base_path + std::string(padded) + ".sst";
  ROCKSDB_NAMESPACE::IOStatus s3 = fs->GetFileSize(path3, fopts.io_options, file_size, nullptr);
  if (s3.ok()) {
    return path3;
  }
  
  // All attempts failed, return first path (caller will handle error)
  return path1;
}

// LRU cache for TableReader (keyed by file_number)
// Uses sharding to reduce lock contention: file_number is hashed to select a shard
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

  // Inflight open tracking for deduplication
  struct InflightOpen {
    std::promise<std::shared_ptr<CacheEntry>> promise;
    std::shared_future<std::shared_ptr<CacheEntry>> future;
    
    InflightOpen() : promise(), future(promise.get_future().share()) {}
  };

  // Shard structure: each shard has its own lock, cache, and LRU
  struct Shard {
    mutable std::shared_mutex mutex;  // shared_mutex for read-heavy workload
    std::unordered_map<uint64_t, std::shared_ptr<CacheEntry>> cache;
    std::unordered_map<uint64_t, std::shared_ptr<InflightOpen>> inflight;
    std::list<uint64_t> lru_list;  // Front = MRU, Back = LRU
    std::unordered_map<uint64_t, std::list<uint64_t>::iterator> lru_map;
    size_t max_size;
    
    Shard(size_t max_size_per_shard) : max_size(max_size_per_shard) {}
  };

  static constexpr size_t N_SHARDS = 16;  // 2^4 shards for better distribution

  explicit TableReaderCache(size_t max_size) : max_size_(max_size) {
    // Initialize shards
    size_t max_size_per_shard = (max_size + N_SHARDS - 1) / N_SHARDS;
    for (size_t i = 0; i < N_SHARDS; ++i) {
      shards_[i] = std::make_unique<Shard>(max_size_per_shard);
    }
  }
  
  // Hash function to select shard from file_number
  static size_t GetShardIndex(uint64_t file_number) {
    // Simple hash: mix bits for better distribution
    return std::hash<uint64_t>{}(file_number) & (N_SHARDS - 1);
  }
  
  Shard& GetShard(uint64_t file_number) {
    return *shards_[GetShardIndex(file_number)];
  }

  // Get or create TableReader for SST file (by file_number)
  // Returns nullptr on error (safe to treat as may_match=true)
  std::shared_ptr<CacheEntry> GetOrOpen(uint64_t file_number,
                                        const ROCKSDB_NAMESPACE::Options& options,
                                        Stats* stats, std::mutex* log_mutex, std::atomic<size_t>* open_count) {
    Shard& shard = GetShard(file_number);
    
    // Phase A: Check cache hit with shared_lock (read-heavy optimization)
    std::shared_ptr<CacheEntry> cached_entry;
    {
      std::shared_lock<std::shared_mutex> lock(shard.mutex);
      
      // Check cache first (by file_number)
      auto it = shard.cache.find(file_number);
      if (it != shard.cache.end()) {
        // Cache hit - remember entry, update LRU outside shared_lock
        cached_entry = it->second;
        if (stats) stats->cache_hits++;
      }
    }
    
    // Update LRU outside shared_lock (requires unique_lock)
    if (cached_entry) {
      std::unique_lock<std::shared_mutex> lock(shard.mutex);
      // Double-check cache still exists (might have been evicted)
      if (shard.cache.find(file_number) != shard.cache.end()) {
        UpdateLRU(shard, file_number);
      }
      return cached_entry;
    }

    // Cache miss - need to open file
    if (stats) stats->cache_misses++;
    
    // Phase A (continued): Check inflight and register if needed
    std::shared_ptr<InflightOpen> inflight;
    bool is_waiting = false;
    {
      std::unique_lock<std::shared_mutex> lock(shard.mutex);
      
      // Double-check cache (might have been inserted by another thread)
      auto it = shard.cache.find(file_number);
      if (it != shard.cache.end()) {
        UpdateLRU(shard, file_number);
        if (stats) stats->cache_hits++;
        return it->second;
      }
      
      // Check if another thread is already opening this file
      auto inflight_it = shard.inflight.find(file_number);
      if (inflight_it != shard.inflight.end()) {
        // Another thread is opening - wait for result
        inflight = inflight_it->second;
        is_waiting = true;
      } else {
        // We are the first thread to open this file - register inflight
        inflight = std::make_shared<InflightOpen>();
        shard.inflight[file_number] = inflight;
        
        // If cache is full, evict LRU entry before opening
        if (shard.cache.size() >= shard.max_size) {
          EvictLRUOne(shard, stats);
        }
      }
      // Lock released here - proceed to Phase B
    }
    
    // If another thread is opening, wait for result
    if (is_waiting && inflight) {
      try {
        std::shared_ptr<CacheEntry> result = inflight->future.get();
        return result;
      } catch (...) {
        // Promise was broken (shouldn't happen, but handle gracefully)
        return nullptr;
      }
    }

    // Open SST file directly using BlockBasedTable
    // Create a new entry with ImmutableOptions and InternalKeyComparator
    auto entry = std::make_unique<CacheEntry>(options);

    // Open using BlockBasedTable directly
    ROCKSDB_NAMESPACE::EnvOptions env_options;
    ROCKSDB_NAMESPACE::ReadOptions read_options;
    read_options.read_tier = ROCKSDB_NAMESPACE::kReadAllTier;  // Allow file reads

    std::unique_ptr<ROCKSDB_NAMESPACE::FSRandomAccessFile> file;
    const auto& fs = options.env->GetFileSystem();
    ROCKSDB_NAMESPACE::FileOptions fopts(env_options);
    
    // Try to find actual path with fallback (plain -> 6-digit -> 7-digit)
    // TryOpenSstPath already calls GetFileSize, so file_size is set if successful
    uint64_t file_size = 0;
    std::string actual_path = TryOpenSstPath(file_number, fs.get(), fopts, &file_size);
    
    // Debug output for first 20 cache misses
    size_t open_id = (open_count ? (*open_count)++ : 0);
    if (open_id <= 20) {
      // std::lock_guard<std::mutex> log_lock(*log_mutex);
      // std::cerr << "[DBG] file_number=" << file_number << ", actual_path(len=" << actual_path.size() << "): \"" << actual_path << "\"\n";
    }
    
    // Check if TryOpenSstPath found a valid file (file_size > 0 means success)
    // Note: TryOpenSstPath already called GetFileSize, so we check file_size here
    if (actual_path.empty() || file_size == 0) {
      // TryOpenSstPath failed to find any matching file
      if (stats) stats->open_fail_getfilesize++;
      // 샘플 로그 (초기 50회 + 매 1000회) - 락 밖에서
      // if (open_id <= 50 || open_id % 1000 == 0) {
      //   std::lock_guard<std::mutex> log_lock(*log_mutex);
      //   std::cerr << "[SAMPLE] GetOrOpen #" << open_id 
      //             << " FAIL: TryOpenSstPath failed to find file" << std::endl;
      //   std::cerr << "[SAMPLE]   file_number: " << file_number << std::endl;
      //   std::cerr << "[SAMPLE]   actual_path: " << (actual_path.empty() ? "(empty)" : actual_path) << std::endl;
      // }
      // Clean up inflight entry on failure
      {
        std::lock_guard<std::shared_mutex> lock(shard.mutex);
        shard.inflight.erase(file_number);
        inflight->promise.set_value(nullptr);
      }
      return nullptr;
    }

    ROCKSDB_NAMESPACE::IOStatus s = fs->NewRandomAccessFile(actual_path, fopts, &file, nullptr);
    if (!s.ok()) {
      if (stats) stats->open_fail_newfile++;
      // 샘플 로그 - 락 밖에서
      // if (open_id <= 50 || open_id % 1000 == 0) {
      //   std::lock_guard<std::mutex> log_lock(*log_mutex);
      //   std::cerr << "[SAMPLE] GetOrOpen #" << open_id 
      //             << " FAIL: NewRandomAccessFile failed" << std::endl;
      //   std::cerr << "[SAMPLE]   file_number: " << file_number << std::endl;
      //   std::cerr << "[SAMPLE]   actual_path: " << actual_path << std::endl;
      //   std::cerr << "[SAMPLE]   file_size: " << file_size << std::endl;
      //   std::cerr << "[SAMPLE]   error: " << s.ToString() << std::endl;
      // }
      // Clean up inflight entry on failure
      {
        std::lock_guard<std::shared_mutex> lock(shard.mutex);
        shard.inflight.erase(file_number);
        inflight->promise.set_value(nullptr);
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
      // 샘플 로그 - 락 밖에서
      // if (open_id <= 50 || open_id % 1000 == 0) {
      //   std::lock_guard<std::mutex> log_lock(*log_mutex);
      //   std::cerr << "[SAMPLE] GetOrOpen #" << open_id 
      //             << " FAIL: NewTableReader failed" << std::endl;
      //   std::cerr << "[SAMPLE]   file_number: " << file_number << std::endl;
      //   std::cerr << "[SAMPLE]   actual_path: " << actual_path << std::endl;
      //   std::cerr << "[SAMPLE]   file_size: " << file_size << std::endl;
      //   std::cerr << "[SAMPLE]   status: " << table_status.ToString() << std::endl;
      // }
      // Clean up inflight entry on failure
      {
        std::lock_guard<std::shared_mutex> lock(shard.mutex);
        shard.inflight.erase(file_number);
        inflight->promise.set_value(nullptr);
      }
      return nullptr;
    }

    // 샘플 로그 (성공한 경우) 및 table properties 로그
    // if (open_id <= 5 || open_id % 1000 == 0) {
    //   std::lock_guard<std::mutex> log_lock(*log_mutex);
    //   std::cerr << "[SAMPLE] GetOrOpen #" << open_id 
    //             << " SUCCESS file_number=" << file_number 
    //             << " path=" << actual_path << std::endl;
    //   
    //   // Table properties 로그
    //   auto props = table_reader->GetTableProperties();
    //   if (props) {
    //     std::cerr << "[SAMPLE]   table_properties.filter_policy_name: " << props->filter_policy_name << std::endl;
    //   } else {
    //     std::cerr << "[SAMPLE]   table_properties: null" << std::endl;
    //   }
    //   
    //   // BlockBasedTable::Rep 정보 로그
    //   ROCKSDB_NAMESPACE::BlockBasedTable* bb_table =
    //       static_cast<ROCKSDB_NAMESPACE::BlockBasedTable*>(table_reader.get());
    //   if (bb_table) {
    //     const ROCKSDB_NAMESPACE::BlockBasedTable::Rep* rep = bb_table->get_rep();
    //     if (rep) {
    //       std::cerr << "[SAMPLE]   rep->filter_policy: " << (rep->table_options.filter_policy ? rep->table_options.filter_policy.get() : nullptr) << std::endl;
    //       if (rep->table_options.filter_policy) {
    //         std::cerr << "[SAMPLE]   rep->filter_policy->Name(): " << rep->table_options.filter_policy->Name() << std::endl;
    //       }
    //       std::cerr << "[SAMPLE]   rep->filter: " << (rep->filter ? rep->filter.get() : nullptr) << std::endl;
    //       std::cerr << "[SAMPLE]   rep->whole_key_filtering: " << (rep->whole_key_filtering ? "true" : "false") << std::endl;
    //       std::cerr << "[SAMPLE]   rep->index_type: " << static_cast<int>(rep->index_type) << std::endl;
    //       
    //       // 실제로 열린 파일의 첫 번째 키 확인 (초기 5회만)
    //       if (open_id <= 5) {
    //         ROCKSDB_NAMESPACE::ReadOptions ro;
    //         std::unique_ptr<ROCKSDB_NAMESPACE::InternalIterator> iter(
    //             table_reader->NewIterator(ro, nullptr, nullptr, false,
    //                                      ROCKSDB_NAMESPACE::TableReaderCaller::kSSTDumpTool));
    //         iter->SeekToFirst();
    //         if (iter->Valid()) {
    //           ROCKSDB_NAMESPACE::Slice first_key = iter->key();
    //           std::cerr << "[SAMPLE]   First key in file (hex): " << first_key.ToString(true) << std::endl;
    //           ROCKSDB_NAMESPACE::ParsedInternalKey parsed;
    //           if (ROCKSDB_NAMESPACE::ParseInternalKey(first_key, &parsed, false).ok()) {
    //             std::cerr << "[SAMPLE]   First user_key (hex): " << parsed.user_key.ToString(true) << std::endl;
    //           }
    //         }
    //       }
    //     } else {
    //       std::cerr << "[SAMPLE]   rep: null" << std::endl;
    //     }
    //   }
    // }

    entry->reader = std::move(table_reader);
    std::shared_ptr<CacheEntry> entry_ptr = std::move(entry);
    
    // Phase C: Insert into cache under lock (short duration)
    {
      std::unique_lock<std::shared_mutex> lock(shard.mutex);
      
      // Double-check cache (might have been inserted by another thread during Phase B)
      auto it = shard.cache.find(file_number);
      if (it != shard.cache.end()) {
        // Another thread already inserted - use existing entry
        UpdateLRU(shard, file_number);
        if (stats) stats->cache_hits++;
        // Clean up inflight entry
        shard.inflight.erase(file_number);
        inflight->promise.set_value(it->second);
        return it->second;
      }
      
      // Cache by file_number and update LRU
      shard.cache[file_number] = entry_ptr;
      UpdateLRU(shard, file_number);
      
      // Clean up inflight entry and notify waiters
      shard.inflight.erase(file_number);
      inflight->promise.set_value(entry_ptr);
    }
    
    return entry_ptr;
  }

 private:
  // Update LRU list: move file_number to front (MRU)
  // Requires: caller must hold unique_lock on shard.mutex
  void UpdateLRU(Shard& shard, uint64_t file_number) {
    auto lru_it = shard.lru_map.find(file_number);
    if (lru_it != shard.lru_map.end()) {
      // Already in LRU list - move to front
      shard.lru_list.splice(shard.lru_list.begin(), shard.lru_list, lru_it->second);
      // Iterator is still valid, no need to update lru_map
    } else {
      // Not in LRU list - add to front
      shard.lru_list.push_front(file_number);
      shard.lru_map[file_number] = shard.lru_list.begin();
    }
  }
  
  // Evict one LRU entry (O(1) operation)
  // Requires: caller must hold unique_lock on shard.mutex
  // Note: Removing from cache doesn't immediately delete CacheEntry because
  // shared_ptr ensures existing references remain valid until released
  void EvictLRUOne(Shard& shard, Stats* stats) {
    if (shard.lru_list.empty()) return;
    
    // Back of list is LRU
    uint64_t lru_file_number = shard.lru_list.back();
    shard.lru_list.pop_back();
    shard.lru_map.erase(lru_file_number);
    
    // Remove from cache
    // Note: shared_ptr ensures existing references remain valid until released
    auto cache_it = shard.cache.find(lru_file_number);
    if (cache_it != shard.cache.end()) {
      shard.cache.erase(cache_it);
      if (stats) stats->evictions++;
    }
  }

  std::array<std::unique_ptr<Shard>, N_SHARDS> shards_;
  size_t max_size_;
};

// Base class for all CallData (for polymorphic Proceed() call)
class CallDataBase {
 public:
  virtual ~CallDataBase() = default;
  virtual void Proceed() = 0;
};

// Forward declaration
class CheckCallData;

// Per-CQ worker queue (lock-free structure)
// Each CQ has its own worker queue and dedicated worker threads
class PerCQWorkerQueue {
 public:
  struct WorkItem {
    CheckCallData* call_data;
    BatchRequest request;
    std::chrono::steady_clock::time_point handler_start;
    size_t req_id;
    size_t active_count;
    
    WorkItem(CheckCallData* cd, const BatchRequest& req,
             std::chrono::steady_clock::time_point start,
             size_t id, size_t active)
        : call_data(cd), request(req), handler_start(start),
          req_id(id), active_count(active) {}
  };
  
  explicit PerCQWorkerQueue(size_t num_workers)
      : num_workers_(num_workers), shutdown_(false) {
    for (size_t i = 0; i < num_workers_; i++) {
      workers_.emplace_back([this]() { WorkerLoop(); });
    }
  }
  
  ~PerCQWorkerQueue() {
    Shutdown();
  }
  
  // Lock-free enqueue using atomic operations
  void Enqueue(WorkItem item) {
    // Use atomic operations for lock-free enqueue
    // For simplicity, we use a lock here but can be optimized with lock-free queue
    {
      std::lock_guard<std::mutex> lock(queue_mutex_);
      work_queue_.push(std::move(item));
    }
    queue_cv_.notify_one();
  }
  
  void Shutdown() {
    {
      std::lock_guard<std::mutex> lock(queue_mutex_);
      shutdown_ = true;
    }
    queue_cv_.notify_all();
    
    for (auto& worker : workers_) {
      if (worker.joinable()) {
        worker.join();
      }
    }
  }
  
 private:
  void WorkerLoop();
  
  size_t num_workers_;
  std::atomic<bool> shutdown_;
  std::queue<WorkItem> work_queue_;
  std::mutex queue_mutex_;
  std::condition_variable queue_cv_;
  std::vector<std::thread> workers_;
};

// Per-RPC state machine for Check RPC
class CheckCallData : public CallDataBase {
 public:
  CheckCallData(BloomCheck::AsyncService* service, grpc::ServerCompletionQueue* cq,
                TableReaderCache* cache, ROCKSDB_NAMESPACE::Options* rocksdb_options,
                Stats* stats, std::mutex* log_mutex, std::atomic<size_t>* open_count,
                std::atomic<size_t>* active_rpc_count, std::atomic<size_t>* total_rpc_count,
                std::shared_ptr<ROCKSDB_NAMESPACE::SystemClock> clock,
                std::shared_ptr<ROCKSDB_NAMESPACE::FileSystem> fs,
                PerCQWorkerQueue* worker_queue)
      : service_(service), cq_(cq), cache_(cache), rocksdb_options_(rocksdb_options),
        stats_(stats), log_mutex_(log_mutex), open_count_(open_count),
        active_rpc_count_(active_rpc_count), total_rpc_count_(total_rpc_count),
        clock_(clock), fs_(fs), worker_queue_(worker_queue),
        responder_(&ctx_), status_(CREATE) {
    Proceed();
  }

  void Proceed() override {
    if (status_ == CREATE) {
      // ACCEPT: 새로운 RPC 요청 수락
      status_ = PROCESS;
      service_->RequestCheck(&ctx_, &request_, &responder_, cq_, cq_, this);
      
      // Active RPC 카운터 증가
      active_count_ = active_rpc_count_->fetch_add(1) + 1;
      total_rpc_count_->fetch_add(1);
      
      handler_start_ = std::chrono::steady_clock::now();
      req_id_ = request_count_.fetch_add(1) + 1;
      
      if (req_id_ <= 10 || req_id_ % 100 == 0) {
        // std::cerr << "[INFO] Check request #" << req_id_ 
        //           << " received: " << request_.reqs_size() << " key request(s)" << std::flush << std::endl;
      }
    } else if (status_ == PROCESS) {
      // READ 완료, PROCESS 시작
      // 새로운 CallData를 생성하여 다음 요청을 받을 준비
      new CheckCallData(service_, cq_, cache_, rocksdb_options_, stats_, log_mutex_,
                       open_count_, active_rpc_count_, total_rpc_count_, clock_, fs_,
                       worker_queue_);
      
      // CQ poller thread: Enqueue work to per-CQ worker queue and return immediately
      // DO NOT perform bloom filter checks here - that happens in worker thread
      worker_queue_->Enqueue(PerCQWorkerQueue::WorkItem(
          this, request_, handler_start_, req_id_, active_count_));
      
      // Return immediately - worker thread will call Finish() directly
      // No need for WORK_DONE state or Alarm
    } else {
      // FINISH 완료
      GPR_ASSERT(status_ == FINISH);
      delete this;
    }
  }
  
  // Called from worker thread - performs actual bloom filter processing
  // Worker thread directly calls Finish() - no Alarm needed
  void ProcessInWorkerThread(PerCQWorkerQueue::WorkItem item) {
    // This runs in worker thread, NOT in CQ poller thread
    response_.clear_resps();
    response_.mutable_resps()->Reserve(item.request.reqs_size());

    // Process each key request (bloom filter checks happen here)
    for (const auto& key_req : item.request.reqs()) {
      KeyResponse key_resp;
      ProcessKeyRequest(key_req, &key_resp);
      *response_.add_resps() = key_resp;
    }
    
    // Handler 종료 시간 측정
    auto handler_end = std::chrono::steady_clock::now();
    auto handler_us = std::chrono::duration_cast<std::chrono::microseconds>(
        handler_end - item.handler_start).count();
    
    // if (item.req_id <= 10 || item.req_id % 100 == 0) {
    //   std::lock_guard<std::mutex> log_lock(*log_mutex_);
    //   std::cerr << "[INFO] Check request #" << item.req_id << " completed" << std::flush << std::endl;
    //   std::cerr << "[INFO] Check request #" << item.req_id 
    //             << " response size: " << response_.resps_size() << std::flush << std::endl;
    //   std::cerr << "[PERF] Check request #" << item.req_id 
    //             << " server_handler_us=" << handler_us 
    //             << " active_rpcs=" << item.active_count
    //             << " (handler processing time)" << std::flush << std::endl;
    // }
    
    // Worker thread directly calls Finish() - gRPC responder is thread-safe
    // No need for Alarm or CQ notification
    status_ = FINISH;
    responder_.Finish(response_, grpc::Status::OK, this);
    
    // Active RPC 카운터 감소
    active_rpc_count_->fetch_sub(1);
    
    // Note: CallData will be deleted when FINISH event is processed by CQ poller
  }

 private:
  void ProcessKeyRequest(const KeyRequest& req, KeyResponse* resp) {
    // ProcessKeyRequest 시작 시간 측정
    auto process_start = std::chrono::steady_clock::now();
    
    static std::atomic<size_t> key_request_count{0};
    size_t key_req_id = key_request_count.fetch_add(1) + 1;
    
    const ROCKSDB_NAMESPACE::Slice user_key(req.user_key());
    
    // total_candidates: 요청된 전체 candidate 파일 수
    uint64_t total_candidates = req.sst_file_numbers_size();
    resp->set_total_candidates(total_candidates);
    
    // if (key_req_id <= 10 || key_req_id % 100 == 0) {
    //   std::cerr << "[INFO] ProcessKeyRequest #" << key_req_id 
    //             << ": key_len=" << user_key.size() 
    //             << ", sst_count=" << total_candidates << std::flush << std::endl;
    // }

    // checked_count: 실제로 검사한 파일 수 (성공적으로 처리한 파일 수)
    uint64_t checked_count = 0;
    uint64_t total_check_us = 0;  // 모든 CheckKeyInSST 시간 합계
    
    for (uint64_t file_number : req.sst_file_numbers()) {
      auto sst_check_start = std::chrono::steady_clock::now();
      bool may_match = CheckKeyInSST(user_key, file_number);
      auto sst_check_end = std::chrono::steady_clock::now();
      auto sst_check_us = std::chrono::duration_cast<std::chrono::microseconds>(
          sst_check_end - sst_check_start).count();
      total_check_us += sst_check_us;
      
      checked_count++;  // CheckKeyInSST 호출 시도 (성공/실패 무관)
      
      if (may_match) {
        resp->add_may_match_sst_file_numbers(file_number);
      }
    }
    
    // checked_count 설정
    resp->set_checked_count(checked_count);
    
    // complete: 모든 candidate 파일을 검사했는지 여부
    bool complete = (checked_count == total_candidates);
    resp->set_complete(complete);
    
    // ProcessKeyRequest 종료 시간 측정
    auto process_end = std::chrono::steady_clock::now();
    auto process_us = std::chrono::duration_cast<std::chrono::microseconds>(
        process_end - process_start).count();
    
    // if (key_req_id <= 10 || key_req_id % 100 == 0) {
    //   std::lock_guard<std::mutex> log_lock(*log_mutex_);
    //   uint64_t overhead_us = process_us - total_check_us;
    //   std::cerr << "[PERF] ProcessKeyRequest #" << key_req_id 
    //             << " process_us=" << process_us 
    //             << " total_check_us=" << total_check_us
    //             << " overhead_us=" << overhead_us
    //             << " sst_count=" << total_candidates
    //             << " avg_check_us=" << (total_candidates > 0 ? total_check_us / total_candidates : 0)
    //             << " (per-key processing time)" << std::flush << std::endl;
    // }
  }

  bool CheckKeyInSST(const ROCKSDB_NAMESPACE::Slice& user_key, uint64_t file_number) {
    // CheckKeyInSST 시작 시간 측정
    auto check_start = std::chrono::steady_clock::now();
    
    static std::atomic<size_t> call_count{0};
    size_t call_id = call_count.fetch_add(1) + 1;
    
    // Get or open TableReader - returns shared_ptr to guarantee lifetime
    auto get_entry_start = std::chrono::steady_clock::now();
    std::shared_ptr<TableReaderCache::CacheEntry> entry = cache_->GetOrOpen(
        file_number, *rocksdb_options_, stats_, log_mutex_, open_count_);
    auto get_entry_end = std::chrono::steady_clock::now();
    auto get_entry_us = std::chrono::duration_cast<std::chrono::microseconds>(
        get_entry_end - get_entry_start).count();
    if (!entry) {
      stats_->RecordReason(CheckReason::OPEN_FAIL);
      return true;
    }

    // Lock entry for thread-safe access
    std::lock_guard<std::mutex> lock(entry->mutex);
    entry->last_access = std::chrono::steady_clock::now();

    ROCKSDB_NAMESPACE::TableReader* reader = entry->reader.get();
    if (!reader) {
      stats_->RecordReason(CheckReason::READER_NULL);
      return true;
    }

    // Cast to BlockBasedTable to access filter
    ROCKSDB_NAMESPACE::BlockBasedTable* bb_table =
        static_cast<ROCKSDB_NAMESPACE::BlockBasedTable*>(reader);
    if (!bb_table) {
      stats_->RecordReason(CheckReason::BB_TABLE_NULL);
      return true;
    }

    // Get filter from BlockBasedTable
    const ROCKSDB_NAMESPACE::BlockBasedTable::Rep* rep = bb_table->get_rep();
    if (!rep) {
      stats_->RecordReason(CheckReason::REP_NULL);
      return true;
    }
    
    if (!rep->ioptions.fs || !rep->ioptions.fs.get()) {
      stats_->RecordReason(CheckReason::FILTER_NULL);
      return true;
    }
    
    if (!rep->filter) {
      stats_->RecordReason(CheckReason::FILTER_NULL);
      return true;
    }

    // Check whole_key_filtering
    if (!rep->whole_key_filtering) {
      stats_->RecordReason(CheckReason::WHOLE_KEY_FILTERING_FALSE);
      return true;
    }

    // Create minimal GetContext for filter check
    ROCKSDB_NAMESPACE::ReadOptions read_options;
    read_options.read_tier = ROCKSDB_NAMESPACE::kReadAllTier;

    // Get comparator
    const ROCKSDB_NAMESPACE::Comparator* ucmp =
        rep->internal_comparator.user_comparator();
    if (!ucmp) {
      stats_->RecordReason(CheckReason::UCOMP_NULL);
      return true;
    }

    // Create minimal GetContext
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

    // Generate internal key for partitioned filter lookup
    ROCKSDB_NAMESPACE::InternalKey ikey_for_seek(
        user_key_without_ts, ROCKSDB_NAMESPACE::kMaxSequenceNumber,
        ROCKSDB_NAMESPACE::kValueTypeForSeek);
    ROCKSDB_NAMESPACE::Slice ikey_slice = ikey_for_seek.Encode();
    const ROCKSDB_NAMESPACE::Slice* const_ikey_ptr = &ikey_slice;
    
    // Check filter
    bool may_match = false;
    try {
      may_match = rep->filter->KeyMayMatch(
          user_key_without_ts, const_ikey_ptr, &get_context,
          &lookup_context, read_options);
      
      if (may_match) {
        stats_->RecordReason(CheckReason::KEYMAYMATCH_TRUE);
      } else {
        stats_->RecordReason(CheckReason::KEYMAYMATCH_FALSE);
      }
    } catch (...) {
      stats_->RecordReason(CheckReason::KEYMAYMATCH_EXCEPTION);
      return true;  // Safe default on exception
    }
    
    // CheckKeyInSST 종료 시간 측정
    auto check_end = std::chrono::steady_clock::now();
    auto check_us = std::chrono::duration_cast<std::chrono::microseconds>(
        check_end - check_start).count();
    
    // if (call_id <= 10 || call_id % 1000 == 0) {
    //   std::lock_guard<std::mutex> log_lock(*log_mutex_);
    //   std::cerr << "[PERF] CheckKeyInSST #" << call_id 
    //             << " check_us=" << check_us 
    //             << " get_entry_us=" << get_entry_us 
    //             << " (per-SST check time)" << std::flush << std::endl;
    // }
    
    return may_match;
  }

  enum CallStatus { CREATE, PROCESS, FINISH };
  CallStatus status_;
  
  BloomCheck::AsyncService* service_;
  grpc::ServerCompletionQueue* cq_;
  grpc::ServerContext ctx_;
  BatchRequest request_;
  BatchResponse response_;
  grpc::ServerAsyncResponseWriter<BatchResponse> responder_;
  
  TableReaderCache* cache_;
  ROCKSDB_NAMESPACE::Options* rocksdb_options_;
  Stats* stats_;
  std::mutex* log_mutex_;
  std::atomic<size_t>* open_count_;
  std::atomic<size_t>* active_rpc_count_;
  std::atomic<size_t>* total_rpc_count_;
  std::shared_ptr<ROCKSDB_NAMESPACE::SystemClock> clock_;
  std::shared_ptr<ROCKSDB_NAMESPACE::FileSystem> fs_;
  PerCQWorkerQueue* worker_queue_;
  
  std::chrono::steady_clock::time_point handler_start_;
  size_t req_id_;
  size_t active_count_;
  
  static std::atomic<size_t> request_count_;
};

std::atomic<size_t> CheckCallData::request_count_{0};

// PerCQWorkerQueue::WorkerLoop implementation (after CheckCallData definition)
void PerCQWorkerQueue::WorkerLoop() {
  while (true) {
    std::unique_lock<std::mutex> lock(queue_mutex_);
    queue_cv_.wait(lock, [this]() { return !work_queue_.empty() || shutdown_; });
    
    if (shutdown_ && work_queue_.empty()) {
      break;
    }
    
    if (!work_queue_.empty()) {
      WorkItem item = std::move(work_queue_.front());
      work_queue_.pop();
      lock.unlock();
      
      // Perform actual work in worker thread
      // This calls ProcessInWorkerThread which performs bloom filter checks
      // Worker thread directly calls Finish() - no Alarm needed
      item.call_data->ProcessInWorkerThread(std::move(item));
    }
  }
}

// Per-RPC state machine for Noop RPC
class NoopCallData : public CallDataBase {
 public:
  NoopCallData(BloomCheck::AsyncService* service, grpc::ServerCompletionQueue* cq)
      : service_(service), cq_(cq), responder_(&ctx_), status_(CREATE) {
    Proceed();
  }

  void Proceed() override {
    if (status_ == CREATE) {
      // ACCEPT: 새로운 RPC 요청 수락
      status_ = PROCESS;
      service_->RequestNoop(&ctx_, &request_, &responder_, cq_, cq_, this);
    } else if (status_ == PROCESS) {
      // READ 완료, PROCESS 시작
      // 새로운 CallData를 생성하여 다음 요청을 받을 준비
      new NoopCallData(service_, cq_);
      
      // 실제 요청 처리
      struct timespec ts;
      clock_gettime(CLOCK_MONOTONIC, &ts);
      uint64_t now_ns = static_cast<uint64_t>(ts.tv_sec) * 1000000000ULL + 
                        static_cast<uint64_t>(ts.tv_nsec);
      
      response_.set_server_ts_ns(now_ns);
      
      // Optional: very sparse logging (1/100000 sample rate)
      static std::atomic<size_t> noop_count{0};
      size_t count = noop_count.fetch_add(1) + 1;
      
      static bool noop_log_enabled = []() {
        const char* env = std::getenv("BLOOMD_NOOP_LOG");
        return env && std::string(env) == "1";
      }();
      
      // if (noop_log_enabled && count % 100000 == 0) {
      //   std::cerr << "[NOOP] Noop request #" << count 
      //             << " server_ts_ns=" << now_ns << std::flush << std::endl;
      // }
      
      // FINISH: 응답 전송
      status_ = FINISH;
      responder_.Finish(response_, grpc::Status::OK, this);
    } else {
      // FINISH 완료
      GPR_ASSERT(status_ == FINISH);
      delete this;
    }
  }

 private:
  enum CallStatus { CREATE, PROCESS, FINISH };
  CallStatus status_;
  
  BloomCheck::AsyncService* service_;
  grpc::ServerCompletionQueue* cq_;
  grpc::ServerContext ctx_;
  NoopRequest request_;
  NoopResponse response_;
  grpc::ServerAsyncResponseWriter<NoopResponse> responder_;
};

// BloomCheckServiceImpl implements the gRPC AsyncService
class BloomCheckServiceImpl {
 public:
  explicit BloomCheckServiceImpl(size_t cache_size)
      : cache_(cache_size), rocksdb_options_(),
        clock_(ROCKSDB_NAMESPACE::SystemClock::Default()),
        fs_(ROCKSDB_NAMESPACE::Env::Default()->GetFileSystem()) {
    // Configure RocksDB options for filter-only access
    rocksdb_options_.env = ROCKSDB_NAMESPACE::Env::Default();
    
    // Disable RocksDB verbose logging (ERROR level only)
    rocksdb_options_.info_log_level = ROCKSDB_NAMESPACE::InfoLogLevel::ERROR_LEVEL;
    
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
  
  BloomCheck::AsyncService* service() { return &service_; }
  
  TableReaderCache* cache() { return &cache_; }
  ROCKSDB_NAMESPACE::Options* rocksdb_options() { return &rocksdb_options_; }
  Stats* stats() { return &stats_; }
  std::mutex* log_mutex() { return &log_mutex_; }
  std::atomic<size_t>* open_count() { return &open_count_; }
  std::atomic<size_t>* active_rpc_count() { return &active_rpc_count_; }
  std::atomic<size_t>* total_rpc_count() { return &total_rpc_count_; }
  std::shared_ptr<ROCKSDB_NAMESPACE::SystemClock> clock() { return clock_; }
  std::shared_ptr<ROCKSDB_NAMESPACE::FileSystem> fs() { return fs_; }

 private:
  BloomCheck::AsyncService service_;
  
  TableReaderCache cache_;
  ROCKSDB_NAMESPACE::Options rocksdb_options_;
  Stats stats_;
  std::mutex log_mutex_;
  std::atomic<size_t> open_count_{0};
  std::atomic<size_t> active_rpc_count_{0};  // 동시 처리 중인 RPC 개수
  std::atomic<size_t> total_rpc_count_{0};   // 총 처리된 RPC 개수 (QPS 계산용)
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
      size_t last_total_rpc = 0;
      auto last_time = std::chrono::steady_clock::now();
      
      while (!stop_stats_thread_.load()) {
        // 1초마다 QPS 계산
        std::this_thread::sleep_for(std::chrono::seconds(1));
        
        auto now = std::chrono::steady_clock::now();
        size_t current_total_rpc = total_rpc_count_.load();
        size_t current_active_rpc = active_rpc_count_.load();
        
        // Delta 계산
        size_t delta_rpc = current_total_rpc - last_total_rpc;
        auto delta_time = std::chrono::duration_cast<std::chrono::milliseconds>(
            now - last_time).count();
        
        // QPS 계산 (delta_rpc / delta_time_seconds)
        double qps = 0.0;
        if (delta_time > 0) {
          qps = (delta_rpc * 1000.0) / delta_time;
        }
        
        // QPS 로그 출력 (매 1초)
        // {
        //   std::lock_guard<std::mutex> log_lock(log_mutex_);
        //   std::cerr << "[QPS] rpc_qps=" << static_cast<size_t>(qps)
        //             << " active_rpcs=" << current_active_rpc
        //             << " total_rpcs=" << current_total_rpc
        //             << " (qps=" << std::fixed << std::setprecision(1) << qps << ")"
        //             << std::endl;
        // }
        
        last_total_rpc = current_total_rpc;
        last_time = now;
        
        // 5초마다 상세 통계 출력
        // static int detail_counter = 0;
        // if (++detail_counter >= 5) {
        //   detail_counter = 0;
        //   stats_.PrintStats(log_mutex_, open_count_.load(), current_active_rpc);
        // }
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

// HandleRpcs: CQ 폴러 스레드에서 실행되는 함수
// IMPORTANT: This thread MUST NOT perform any application logic (bloom filter checks).
// It only handles CQ events and state transitions.
void HandleRpcs(BloomCheck::AsyncService* service, grpc::ServerCompletionQueue* cq,
                BloomCheckServiceImpl* impl, PerCQWorkerQueue* worker_queue) {
  // 초기 CallData 생성 (ACCEPT 단계)
  new CheckCallData(service, cq, impl->cache(), impl->rocksdb_options(), impl->stats(),
                   impl->log_mutex(), impl->open_count(), impl->active_rpc_count(),
                   impl->total_rpc_count(), impl->clock(), impl->fs(), worker_queue);
  new NoopCallData(service, cq);
  
  void* tag;
  bool ok;
  
  // CQ에서 이벤트 폴링
  // This loop runs in CQ poller thread - must be fast and non-blocking
  while (true) {
    GPR_ASSERT(cq->Next(&tag, &ok));
    GPR_ASSERT(ok);
    
    // CallData의 Proceed() 호출하여 state machine 진행
    // Proceed() should only do state transitions and enqueue work, NOT execute bloom filters
    // Worker thread will call Finish() directly - no WORK_DONE state needed
    static_cast<CallDataBase*>(tag)->Proceed();
  }
}

}  // namespace bloomcheck

// Server main function
void RunServer(const std::string& server_address, size_t cache_size) {
  // Disable gRPC verbose logging (ERROR level only)
  gpr_set_log_verbosity(GPR_LOG_SEVERITY_ERROR);
  
  bloomcheck::BloomCheckServiceImpl service(cache_size);

  grpc::EnableDefaultHealthCheckService(true);

  grpc::ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(service.service());
  
  // ============================================
  // gRPC AsyncServer 설정
  // ============================================
  
  // CompletionQueue 생성 (여러 CQ 사용 가능)
  const int NUM_CQS = 4;  // CQ 개수 (필요에 따라 조정)
  std::vector<std::unique_ptr<grpc::ServerCompletionQueue>> cqs;
  for (int i = 0; i < NUM_CQS; i++) {
    cqs.emplace_back(builder.AddCompletionQueue());
  }
  
  // 1. 스레드 풀 크기 증가
  grpc::ResourceQuota quota;
  quota.SetMaxThreads(128);
  builder.SetResourceQuota(quota);
  
  // 2. HTTP/2 stream concurrency 증가 (connection당)
  builder.AddChannelArgument(GRPC_ARG_MAX_CONCURRENT_STREAMS, 1000);
  
  // 3. 메시지 크기 제한
  builder.SetMaxReceiveMessageSize(4 * 1024 * 1024);  // 4MB
  builder.SetMaxSendMessageSize(4 * 1024 * 1024);    // 4MB
  
  // 4. Connection 관리
  builder.AddChannelArgument(GRPC_ARG_MAX_CONNECTION_IDLE_MS, 300000);  // 5분
  builder.AddChannelArgument(GRPC_ARG_MAX_CONNECTION_AGE_MS, 3600000);   // 1시간
  
  // ============================================
  // AsyncServer 모델
  // - 각 RPC는 per-RPC state machine으로 처리
  // - ServerCompletionQueue에서 이벤트 폴링
  // - 여러 CQ 폴러 스레드로 병렬 처리
  // ============================================
  
  std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
  // std::cout << "BloomCheck AsyncServer listening on " << server_address << std::endl;
  // std::cout << "Using " << NUM_CQS << " CompletionQueues" << std::endl;
  
  // Per-CQ worker queue 생성 (각 CQ마다 별도의 worker queue)
  // Lock contention을 줄이기 위해 CQ별로 분리
  const size_t NUM_WORKER_THREADS_PER_CQ = 8;  // CQ당 worker thread 개수
  std::vector<std::unique_ptr<bloomcheck::PerCQWorkerQueue>> worker_queues;
  for (int i = 0; i < NUM_CQS; i++) {
    worker_queues.emplace_back(
        std::make_unique<bloomcheck::PerCQWorkerQueue>(NUM_WORKER_THREADS_PER_CQ));
  }
  // std::cout << "Using " << NUM_WORKER_THREADS_PER_CQ 
  //           << " worker threads per CQ (total: " 
  //           << (NUM_CQS * NUM_WORKER_THREADS_PER_CQ) << ")" << std::endl;
  
  // CQ 폴러 스레드 생성 (각 CQ당 1개 스레드)
  // CQ poller threads: 빠른 이벤트 처리만 수행 (bloom filter 체크 안 함)
  std::vector<std::thread> cq_threads;
  for (int i = 0; i < NUM_CQS; i++) {
    cq_threads.emplace_back(std::thread(bloomcheck::HandleRpcs, 
                                        service.service(), cqs[i].get(), &service, 
                                        worker_queues[i].get()));
  }
  
  // 서버 종료 대기
  server->Wait();
  
  // 서버 종료 시 CQ 종료
  server->Shutdown();
  for (auto& cq : cqs) {
    cq->Shutdown();
  }
  
  // Worker queues 종료 (큐에 남은 작업 처리)
  for (auto& wq : worker_queues) {
    wq->Shutdown();
  }
  
  // CQ 폴러 스레드 종료 대기
  for (auto& t : cq_threads) {
    t.join();
  }
  
  // 남은 이벤트 처리
  for (auto& cq : cqs) {
    void* tag;
    bool ok;
    while (cq->Next(&tag, &ok)) {
      // 남은 이벤트 처리
    }
  }
}


int main(int argc, char** argv) {
  std::string server_address = "0.0.0.0:50051";
  size_t cache_size = 512;  // Max 512 SST files in cache

  if (argc > 1) {
    server_address = argv[1];
  }
  if (argc > 2) {
    cache_size = std::stoull(argv[2]);
  }

  RunServer(server_address, cache_size);
  return 0;
}

