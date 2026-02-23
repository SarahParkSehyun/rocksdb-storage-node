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
#include <deque>
#include <future>
#include <iomanip>
#include <iostream>
#include <list>
#include <memory>
#include <mutex>
#include <pthread.h>
#include <queue>
#include <sched.h>
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

bool PinCurrentThreadToCpu(int cpu_id) {
  if (cpu_id < 0) {
    return false;
  }
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(cpu_id, &cpuset);
  return pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset) == 0;
}

uint64_t SteadyNowMicros() {
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::microseconds>(
          std::chrono::steady_clock::now().time_since_epoch())
          .count());
}

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

  // Server stats (BLOOMCHECK_SERVER_STATS=1 only; callers must check before calling)
  std::atomic<uint64_t> recv_req_count{0};
  std::atomic<uint64_t> worker_deq_count{0};
  std::atomic<uint64_t> worker_queue_depth_sum{0};
  std::atomic<uint64_t> worker_queue_depth_count{0};
  std::atomic<uint64_t> worker_queue_depth_max{0};
  std::atomic<uint64_t> total_check_loop_us{0};
  std::atomic<uint64_t> check_loop_sample_count{0};
  std::atomic<uint64_t> writeq_sum{0};
  std::atomic<uint64_t> writeq_count{0};
  std::atomic<uint64_t> writeq_max{0};
  std::atomic<uint64_t> write_done_count{0};
  std::atomic<uint64_t> finish_or_close_count{0};
  std::atomic<uint64_t> read_req_event_count{0};
  std::atomic<uint64_t> write_resp_event_count{0};
  std::atomic<uint64_t> pending_work_items_max{0};

  static void UpdateMax(std::atomic<uint64_t>& m, uint64_t val) {
    uint64_t cur = m.load(std::memory_order_relaxed);
    while (val > cur && !m.compare_exchange_weak(cur, val, std::memory_order_relaxed)) {}
  }

  void AddRecvReq() { recv_req_count.fetch_add(1, std::memory_order_relaxed); }
  void AddWorkerDeq(uint64_t depth) {
    worker_deq_count.fetch_add(1, std::memory_order_relaxed);
    worker_queue_depth_sum.fetch_add(depth, std::memory_order_relaxed);
    worker_queue_depth_count.fetch_add(1, std::memory_order_relaxed);
    UpdateMax(worker_queue_depth_max, depth);
  }
  void AddCheckLoopUs(uint64_t us) {
    total_check_loop_us.fetch_add(us, std::memory_order_relaxed);
    check_loop_sample_count.fetch_add(1, std::memory_order_relaxed);
  }
  void AddWriteqSample(uint64_t len) {
    writeq_sum.fetch_add(len, std::memory_order_relaxed);
    writeq_count.fetch_add(1, std::memory_order_relaxed);
    UpdateMax(writeq_max, len);
  }
  void AddWriteDone() { write_done_count.fetch_add(1, std::memory_order_relaxed); }
  void AddFinishOrClose() { finish_or_close_count.fetch_add(1, std::memory_order_relaxed); }
  void AddReadReqEvent() { read_req_event_count.fetch_add(1, std::memory_order_relaxed); }
  void AddWriteRespEvent() {
    write_resp_event_count.fetch_add(1, std::memory_order_relaxed);
  }
  void AddPendingWorkItems(uint64_t depth) { UpdateMax(pending_work_items_max, depth); }
  
  void RecordReason(CheckReason reason) {
    // Statistics collection disabled for performance
    // total_checks++;
    // switch (reason) {
    //   case CheckReason::OPEN_FAIL:
    //     reason_open_fail++;
    //     break;
    //   case CheckReason::READER_NULL:
    //     reason_reader_null++;
    //     break;
    //   case CheckReason::BB_TABLE_NULL:
    //     reason_bb_table_null++;
    //     break;
    //   case CheckReason::REP_NULL:
    //     reason_rep_null++;
    //     break;
    //   case CheckReason::FILTER_NULL:
    //     reason_filter_null++;
    //     break;
    //   case CheckReason::WHOLE_KEY_FILTERING_FALSE:
    //     reason_whole_key_filtering_false++;
    //     break;
    //   case CheckReason::UCOMP_NULL:
    //     reason_ucomp_null++;
    //     break;
    //   case CheckReason::KEYMAYMATCH_EXCEPTION:
    //     reason_keymaymatch_exception++;
    //     break;
    //   case CheckReason::KEYMAYMATCH_TRUE:
    //     reason_keymaymatch_true++;
    //     keymaymatch_calls++;
    //     break;
    //   case CheckReason::KEYMAYMATCH_FALSE:
    //     reason_keymaymatch_false++;
    //     keymaymatch_calls++;
    //     break;
    // }
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
    std::atomic<uint64_t> last_access_us{0};
    mutable std::shared_mutex rw_mutex;
    
    explicit CacheEntry(const ROCKSDB_NAMESPACE::Options& opt)
        : internal_comparator(opt.comparator),
          ioptions(opt),
          last_access_us(SteadyNowMicros()) {
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
        // if (stats) stats->cache_hits++;
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
    // if (stats) stats->cache_misses++;
    
    // Phase A (continued): Check inflight and register if needed
    std::shared_ptr<InflightOpen> inflight;
    bool is_waiting = false;
    {
      std::unique_lock<std::shared_mutex> lock(shard.mutex);
      
      // Double-check cache (might have been inserted by another thread)
      auto it = shard.cache.find(file_number);
      if (it != shard.cache.end()) {
        UpdateLRU(shard, file_number);
        // if (stats) stats->cache_hits++;
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
      // if (stats) stats->open_fail_getfilesize++;
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
      // if (stats) stats->open_fail_newfile++;
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
      // if (stats) stats->open_fail_newreader++;
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
        // if (stats) stats->cache_hits++;
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
      // if (stats) stats->evictions++;
    }
  }

  std::array<std::unique_ptr<Shard>, N_SHARDS> shards_;
  size_t max_size_;
};

// Base class for all CallData (for polymorphic Proceed() call)
class CallDataBase {
 public:
  virtual ~CallDataBase() = default;
  virtual void Proceed(bool ok) = 0;
};

// Forward declaration
class CheckCallData;
class StreamCallData;

// Per-CQ worker queue (lock-free structure)
// Each CQ has its own worker queue and dedicated worker threads
class PerCQWorkerQueue {
 public:
  struct WorkItem {
    enum class Kind {
      kUnaryCheck,
      kStreamCheck,
    };

    Kind kind;
    CheckCallData* call_data;
    StreamCallData* stream_call_data;
    BatchRequest request;
    uint64_t stream_request_id;
    std::string stream_user_key;
    std::vector<uint64_t> stream_sst_file_numbers;
    size_t req_id;
    size_t active_count;
    uint64_t accept_wait_us;
    std::chrono::steady_clock::time_point enqueue_ts;
    
    WorkItem(CheckCallData* cd, const BatchRequest& req,
             size_t id, size_t active, uint64_t accept_wait,
             std::chrono::steady_clock::time_point enqueue_time)
        : kind(Kind::kUnaryCheck),
          call_data(cd),
          stream_call_data(nullptr),
          request(req),
          stream_request_id(0),
          req_id(id), active_count(active), accept_wait_us(accept_wait),
          enqueue_ts(enqueue_time) {}

    WorkItem(StreamCallData* stream_cd, uint64_t request_id,
             std::string user_key, std::vector<uint64_t> sst_numbers,
             std::chrono::steady_clock::time_point enqueue_time)
        : kind(Kind::kStreamCheck),
          call_data(nullptr),
          stream_call_data(stream_cd),
          stream_request_id(request_id),
          stream_user_key(std::move(user_key)),
          stream_sst_file_numbers(std::move(sst_numbers)),
          req_id(0),
          active_count(0),
          accept_wait_us(0),
          enqueue_ts(enqueue_time) {}
  };
  
  PerCQWorkerQueue(size_t num_workers, Stats* stats, bool server_stats_enabled)
      : num_workers_(num_workers), stats_(stats), server_stats_enabled_(server_stats_enabled),
        shutdown_(false) {
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
  Stats* stats_;
  bool server_stats_enabled_;
  std::atomic<bool> shutdown_;
  std::queue<WorkItem> work_queue_;
  std::mutex queue_mutex_;
  std::condition_variable queue_cv_;
  std::vector<std::thread> workers_;
};

// Per-RPC state machine for Check RPC
class CheckCallData : public CallDataBase {
 public:
  struct SstTraceSnapshot {
    bool valid = false;
    size_t call_id = 0;
    uint64_t file_number = 0;
    const char* stage = "na";
    uint64_t get_or_open_us = 0;
    uint64_t keymaymatch_us = 0;
    uint64_t total_check_us = 0;
  };

  struct CheckLogSnapshot {
    bool should_log = false;
    size_t req_id = 0;
    size_t keys = 0;
    uint64_t total_candidates = 0;
    uint64_t checked = 0;
    uint64_t may_match = 0;
    uint64_t handler_us = 0;
    uint64_t work_avg_us = 0;
    uint64_t sst_get_or_open_avg_us = 0;
    uint64_t sst_keymaymatch_avg_us = 0;
    size_t sst_call_id = 0;
    uint64_t sst_file_number = 0;
    const char* sst_stage = "na";
    uint64_t sst_get_or_open_us = 0;
    uint64_t sst_keymaymatch_us = 0;
    uint64_t sst_total_check_us = 0;
    uint64_t accept_wait_us = 0;
    uint64_t queue_wait_us = 0;
    uint64_t finish_call_us = 0;
    uint64_t finish_to_cq_pop_us = 0;
    uint64_t finish_cq_handle_us = 0;
    uint64_t rpc_e2e_us = 0;
  };

  CheckCallData(BloomCheck::AsyncService* service, grpc::ServerCompletionQueue* cq,
                TableReaderCache* cache, ROCKSDB_NAMESPACE::Options* rocksdb_options,
                Stats* stats, std::mutex* log_mutex, std::atomic<size_t>* open_count,
                std::atomic<size_t>* active_rpc_count, std::atomic<size_t>* total_rpc_count,
                std::shared_ptr<ROCKSDB_NAMESPACE::SystemClock> clock,
                std::shared_ptr<ROCKSDB_NAMESPACE::FileSystem> fs,
                PerCQWorkerQueue* worker_queue, bool server_stats_enabled)
      : service_(service), cq_(cq), cache_(cache), rocksdb_options_(rocksdb_options),
        stats_(stats), log_mutex_(log_mutex), open_count_(open_count),
        active_rpc_count_(active_rpc_count), total_rpc_count_(total_rpc_count),
        clock_(clock), fs_(fs), worker_queue_(worker_queue),
        server_stats_enabled_(server_stats_enabled),
        responder_(&ctx_), status_(CREATE) {
    Proceed(true);
  }

  void Proceed(bool ok) override {
    if (!ok) {
      if (status_ == FINISH) {
        delete this;
      }
      return;
    }
    if (status_ == CREATE) {
      // ACCEPT: 새로운 RPC 요청 수락
      status_ = PROCESS;
      service_->RequestCheck(&ctx_, &request_, &responder_, cq_, cq_, this);
      
      // Active RPC 카운터 증가
      active_count_ = active_rpc_count_->fetch_add(1) + 1;
      total_rpc_count_->fetch_add(1);
      req_id_ = request_count_.fetch_add(1) + 1;
      
      if (req_id_ <= 10 || req_id_ % 100 == 0) {
        // std::cerr << "[INFO] Check request #" << req_id_ 
        //           << " received: " << request_.reqs_size() << " key request(s)" << std::flush << std::endl;
      }
    } else if (status_ == PROCESS) {
      // 실제 요청이 CQ에 도착한 시점부터 서버 경로 타이밍 측정 시작
      accept_start_ = std::chrono::steady_clock::now();

      // READ 완료, PROCESS 시작
      // 새로운 CallData를 생성하여 다음 요청을 받을 준비
      new CheckCallData(service_, cq_, cache_, rocksdb_options_, stats_, log_mutex_,
                       open_count_, active_rpc_count_, total_rpc_count_, clock_, fs_,
                       worker_queue_, server_stats_enabled_);
      
      // CQ poller thread: Enqueue work to per-CQ worker queue and return immediately
      // DO NOT perform bloom filter checks here - that happens in worker thread
      if (server_stats_enabled_) stats_->AddRecvReq();
      auto enqueue_ts = std::chrono::steady_clock::now();
      uint64_t accept_wait_us = static_cast<uint64_t>(
          std::chrono::duration_cast<std::chrono::microseconds>(
              enqueue_ts - accept_start_).count());
      worker_queue_->Enqueue(PerCQWorkerQueue::WorkItem(
          this, request_, req_id_, active_count_, accept_wait_us, enqueue_ts));
      
      // Return immediately - worker thread will call Finish() directly
      // No need for WORK_DONE state or Alarm
    } else {
      // FINISH 완료
      GPR_ASSERT(status_ == FINISH);
      auto finish_handle_start = std::chrono::steady_clock::now();
      CheckLogSnapshot snapshot_copy;
      {
        std::lock_guard<std::mutex> snapshot_lock(log_snapshot_mutex_);
        snapshot_copy = log_snapshot_;
        if (snapshot_copy.should_log &&
            finish_call_done_ts_ > std::chrono::steady_clock::time_point::min()) {
          snapshot_copy.finish_to_cq_pop_us = static_cast<uint64_t>(
              std::chrono::duration_cast<std::chrono::microseconds>(
                  finish_handle_start - finish_call_done_ts_).count());
        }
      }

      auto finish_handle_end = std::chrono::steady_clock::now();
      if (snapshot_copy.should_log) {
        snapshot_copy.rpc_e2e_us = static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::microseconds>(
                finish_handle_start - accept_start_).count());
        snapshot_copy.finish_cq_handle_us = static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::microseconds>(
                finish_handle_end - finish_handle_start).count());
        std::lock_guard<std::mutex> log_lock(*log_mutex_);
        std::cerr << "[CHECK] req_id=" << snapshot_copy.req_id
                  << " keys=" << snapshot_copy.keys
                  << " total_candidates=" << snapshot_copy.total_candidates
                  << " checked=" << snapshot_copy.checked
                  << " may_match=" << snapshot_copy.may_match
                  << " handler_us=" << snapshot_copy.handler_us
                  << " work_avg_us=" << snapshot_copy.work_avg_us
                  << " sst_get_or_open_avg_us=" << snapshot_copy.sst_get_or_open_avg_us
                  << " sst_keymaymatch_avg_us=" << snapshot_copy.sst_keymaymatch_avg_us
                  << " sst_call_id=" << snapshot_copy.sst_call_id
                  << " sst_file_number=" << snapshot_copy.sst_file_number
                  << " sst_stage=" << snapshot_copy.sst_stage
                  << " sst_get_or_open_us=" << snapshot_copy.sst_get_or_open_us
                  << " sst_keymaymatch_us=" << snapshot_copy.sst_keymaymatch_us
                  << " sst_total_check_us=" << snapshot_copy.sst_total_check_us
                  << " accept_wait_us=" << snapshot_copy.accept_wait_us
                  << " queue_wait_us=" << snapshot_copy.queue_wait_us
                  << " finish_call_us=" << snapshot_copy.finish_call_us
                  << " finish_to_cq_pop_us=" << snapshot_copy.finish_to_cq_pop_us
                  << " finish_cq_handle_us=" << snapshot_copy.finish_cq_handle_us
                  << " rpc_e2e_us=" << snapshot_copy.rpc_e2e_us
                  << std::endl;
      }
      delete this;
    }
  }
  
  // Called from worker thread - performs actual bloom filter processing
  // Worker thread directly calls Finish() - no Alarm needed
  void ProcessInWorkerThread(PerCQWorkerQueue::WorkItem item) {
    auto& sst_trace = ThreadLocalSstTrace();
    sst_trace.valid = false;
    {
      std::lock_guard<std::mutex> snapshot_lock(log_snapshot_mutex_);
      log_snapshot_.should_log = false;
    }
    auto handler_start = std::chrono::steady_clock::now();
    uint64_t queue_wait_us = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::microseconds>(
            handler_start - item.enqueue_ts).count());
    // This runs in worker thread, NOT in CQ poller thread
    response_.clear_resps();
    response_.mutable_resps()->Reserve(item.request.reqs_size());

    // Process each key request (bloom filter checks happen here)
    auto work_start = std::chrono::steady_clock::now();
    uint64_t total_candidates = 0;
    uint64_t total_checked = 0;
    uint64_t total_may_match = 0;
    for (const auto& key_req : item.request.reqs()) {
      KeyResponse key_resp;
      ProcessKeyRequest(key_req, &key_resp);
      total_candidates += key_resp.total_candidates();
      total_checked += key_resp.checked_count();
      total_may_match += key_resp.may_match_sst_file_numbers_size();
      *response_.add_resps() = key_resp;
    }
    auto work_end = std::chrono::steady_clock::now();
    
    // Handler 종료 시간 측정
    auto handler_before_finish = std::chrono::steady_clock::now();
    auto work_us = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::microseconds>(
            work_end - work_start).count());
    auto handler_us = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::microseconds>(
            handler_before_finish - handler_start).count());
    
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
    auto finish_call_start = std::chrono::steady_clock::now();
    status_ = FINISH;
    responder_.Finish(response_, grpc::Status::OK, this);
    auto finish_call_end = std::chrono::steady_clock::now();
    uint64_t finish_call_us = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::microseconds>(
            finish_call_end - finish_call_start).count());
    {
      std::lock_guard<std::mutex> snapshot_lock(log_snapshot_mutex_);
      finish_call_done_ts_ = finish_call_end;
    }
    
    // Active RPC 카운터 감소
    active_rpc_count_->fetch_sub(1);

    // Statistics collection and logging disabled for performance
    // check_timing_req_count_.fetch_add(1, std::memory_order_relaxed);
    // check_timing_work_us_sum_.fetch_add(work_us, std::memory_order_relaxed);
    // check_timing_handler_us_sum_.fetch_add(handler_us, std::memory_order_relaxed);
    // bool check_log_enabled = []() {
    //   const char* env = std::getenv("BLOOMD_CHECK_TIMING_LOG");
    //   return env && std::string(env) == "1";
    // }();
    // bool should_log = check_log_enabled && (item.req_id <= 10 || item.req_id % 1000 == 0);
    // if (should_log) {
    //   const uint64_t req_cnt =
    //       check_timing_req_count_.load(std::memory_order_relaxed);
    //   const uint64_t sst_cnt =
    //       check_timing_sst_count_.load(std::memory_order_relaxed);
    //   const uint64_t work_avg_us =
    //       req_cnt == 0 ? 0
    //                    : check_timing_work_us_sum_.load(std::memory_order_relaxed) / req_cnt;
    //   const uint64_t handler_avg_us =
    //       req_cnt == 0 ? 0
    //                    : check_timing_handler_us_sum_.load(std::memory_order_relaxed) / req_cnt;
    //   const uint64_t sst_get_or_open_avg_us =
    //       sst_cnt == 0
    //           ? 0
    //           : check_timing_sst_get_or_open_us_sum_.load(std::memory_order_relaxed) / sst_cnt;
    //   const uint64_t sst_keymaymatch_avg_us =
    //       sst_cnt == 0
    //           ? 0
    //           : check_timing_sst_keymaymatch_us_sum_.load(std::memory_order_relaxed) / sst_cnt;

    //   std::lock_guard<std::mutex> snapshot_lock(log_snapshot_mutex_);
    //   log_snapshot_.should_log = true;
    //   log_snapshot_.req_id = item.req_id;
    //   log_snapshot_.keys = item.request.reqs_size();
    //   log_snapshot_.total_candidates = total_candidates;
    //   log_snapshot_.checked = total_checked;
    //   log_snapshot_.may_match = total_may_match;
    //   log_snapshot_.handler_us = handler_us;
    //   log_snapshot_.work_avg_us = work_avg_us;
    //   log_snapshot_.sst_get_or_open_avg_us = sst_get_or_open_avg_us;
    //   log_snapshot_.sst_keymaymatch_avg_us = sst_keymaymatch_avg_us;
    //   log_snapshot_.sst_call_id = sst_trace.call_id;
    //   log_snapshot_.sst_file_number = sst_trace.file_number;
    //   log_snapshot_.sst_stage = sst_trace.stage;
    //   log_snapshot_.sst_get_or_open_us = sst_trace.get_or_open_us;
    //   log_snapshot_.sst_keymaymatch_us = sst_trace.keymaymatch_us;
    //   log_snapshot_.sst_total_check_us = sst_trace.total_check_us;
    //   log_snapshot_.accept_wait_us = item.accept_wait_us;
    //   log_snapshot_.queue_wait_us = queue_wait_us;
    //   log_snapshot_.finish_call_us = finish_call_us;
    // }
    
    // Note: CallData will be deleted when FINISH event is processed by CQ poller
  }

 private:
  static SstTraceSnapshot& ThreadLocalSstTrace() {
    static thread_local SstTraceSnapshot trace;
    return trace;
  }

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
    if (server_stats_enabled_) stats_->AddCheckLoopUs(total_check_us);

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
    uint64_t keymaymatch_us = 0;
    
    // Statistics collection disabled for performance
    // auto maybe_log_timing = [&](const char* stage, uint64_t get_entry_us_val) {
    //   auto now = std::chrono::steady_clock::now();
    //   uint64_t total_us = static_cast<uint64_t>(
    //       std::chrono::duration_cast<std::chrono::microseconds>(now - check_start).count());
    //   auto& sst_trace = ThreadLocalSstTrace();
    //   sst_trace.valid = true;
    //   sst_trace.call_id = call_id;
    //   sst_trace.file_number = file_number;
    //   sst_trace.stage = stage;
    //   sst_trace.get_or_open_us = get_entry_us_val;
    //   sst_trace.keymaymatch_us = keymaymatch_us;
    //   sst_trace.total_check_us = total_us;
    // };
    
    // Get or open TableReader - returns shared_ptr to guarantee lifetime
    auto get_entry_start = std::chrono::steady_clock::now();
    std::shared_ptr<TableReaderCache::CacheEntry> entry = cache_->GetOrOpen(
        file_number, *rocksdb_options_, stats_, log_mutex_, open_count_);
    auto get_entry_end = std::chrono::steady_clock::now();
    auto get_entry_us = std::chrono::duration_cast<std::chrono::microseconds>(
        get_entry_end - get_entry_start).count();
    if (!entry) {
      // stats_->RecordReason(CheckReason::OPEN_FAIL);
      // maybe_log_timing("open_fail", static_cast<uint64_t>(get_entry_us));
      return true;
    }

    // Immutable SST reader path: allow concurrent readers.
    entry->last_access_us.store(SteadyNowMicros(), std::memory_order_relaxed);
    std::shared_lock<std::shared_mutex> lock(entry->rw_mutex);

    ROCKSDB_NAMESPACE::TableReader* reader = entry->reader.get();
    if (!reader) {
      // stats_->RecordReason(CheckReason::READER_NULL);
      // maybe_log_timing("reader_null", static_cast<uint64_t>(get_entry_us));
      return true;
    }

    // Cast to BlockBasedTable to access filter
    ROCKSDB_NAMESPACE::BlockBasedTable* bb_table =
        static_cast<ROCKSDB_NAMESPACE::BlockBasedTable*>(reader);
    if (!bb_table) {
      // stats_->RecordReason(CheckReason::BB_TABLE_NULL);
      // maybe_log_timing("bb_table_null", static_cast<uint64_t>(get_entry_us));
      return true;
    }

    // Get filter from BlockBasedTable
    const ROCKSDB_NAMESPACE::BlockBasedTable::Rep* rep = bb_table->get_rep();
    if (!rep) {
      // stats_->RecordReason(CheckReason::REP_NULL);
      // maybe_log_timing("rep_null", static_cast<uint64_t>(get_entry_us));
      return true;
    }
    
    if (!rep->ioptions.fs || !rep->ioptions.fs.get()) {
      // stats_->RecordReason(CheckReason::FILTER_NULL);
      // maybe_log_timing("fs_null", static_cast<uint64_t>(get_entry_us));
      return true;
    }
    
    if (!rep->filter) {
      // stats_->RecordReason(CheckReason::FILTER_NULL);
      // maybe_log_timing("filter_null", static_cast<uint64_t>(get_entry_us));
      return true;
    }

    // Check whole_key_filtering
    if (!rep->whole_key_filtering) {
      // stats_->RecordReason(CheckReason::WHOLE_KEY_FILTERING_FALSE);
      // maybe_log_timing("whole_key_filtering_false", static_cast<uint64_t>(get_entry_us));
      return true;
    }

    // Create minimal GetContext for filter check
    ROCKSDB_NAMESPACE::ReadOptions read_options;
    read_options.read_tier = ROCKSDB_NAMESPACE::kReadAllTier;

    // Get comparator
    const ROCKSDB_NAMESPACE::Comparator* ucmp =
        rep->internal_comparator.user_comparator();
    if (!ucmp) {
      // stats_->RecordReason(CheckReason::UCOMP_NULL);
      // maybe_log_timing("ucmp_null", static_cast<uint64_t>(get_entry_us));
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
      auto keymaymatch_start = std::chrono::steady_clock::now();
      may_match = rep->filter->KeyMayMatch(
          user_key_without_ts, const_ikey_ptr, &get_context,
          &lookup_context, read_options);
      auto keymaymatch_end = std::chrono::steady_clock::now();
      keymaymatch_us = static_cast<uint64_t>(
          std::chrono::duration_cast<std::chrono::microseconds>(
              keymaymatch_end - keymaymatch_start).count());
      
      // if (may_match) {
      //   stats_->RecordReason(CheckReason::KEYMAYMATCH_TRUE);
      // } else {
      //   stats_->RecordReason(CheckReason::KEYMAYMATCH_FALSE);
      // }
    } catch (...) {
      // stats_->RecordReason(CheckReason::KEYMAYMATCH_EXCEPTION);
      // maybe_log_timing("keymaymatch_exception", static_cast<uint64_t>(get_entry_us));
      return true;  // Safe default on exception
    }
    
    // CheckKeyInSST 종료 시간 측정
    auto check_end = std::chrono::steady_clock::now();
    auto check_us = std::chrono::duration_cast<std::chrono::microseconds>(
        check_end - check_start).count();
    // Statistics collection disabled for performance
    // check_timing_sst_count_.fetch_add(1, std::memory_order_relaxed);
    // check_timing_sst_get_or_open_us_sum_.fetch_add(
    //     static_cast<uint64_t>(get_entry_us), std::memory_order_relaxed);
    // check_timing_sst_keymaymatch_us_sum_.fetch_add(
    //     keymaymatch_us, std::memory_order_relaxed);
    // maybe_log_timing("ok", static_cast<uint64_t>(get_entry_us));
    
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
  bool server_stats_enabled_;

  std::chrono::steady_clock::time_point accept_start_;
  std::chrono::steady_clock::time_point finish_call_done_ts_ =
      std::chrono::steady_clock::time_point::min();
  size_t req_id_;
  size_t active_count_;
  std::mutex log_snapshot_mutex_;
  CheckLogSnapshot log_snapshot_;
  
  static std::atomic<size_t> request_count_;
  static std::atomic<uint64_t> check_timing_req_count_;
  static std::atomic<uint64_t> check_timing_work_us_sum_;
  static std::atomic<uint64_t> check_timing_handler_us_sum_;
  static std::atomic<uint64_t> check_timing_sst_count_;
  static std::atomic<uint64_t> check_timing_sst_get_or_open_us_sum_;
  static std::atomic<uint64_t> check_timing_sst_keymaymatch_us_sum_;
};

std::atomic<size_t> CheckCallData::request_count_{0};
std::atomic<uint64_t> CheckCallData::check_timing_req_count_{0};
std::atomic<uint64_t> CheckCallData::check_timing_work_us_sum_{0};
std::atomic<uint64_t> CheckCallData::check_timing_handler_us_sum_{0};
std::atomic<uint64_t> CheckCallData::check_timing_sst_count_{0};
std::atomic<uint64_t> CheckCallData::check_timing_sst_get_or_open_us_sum_{0};
std::atomic<uint64_t> CheckCallData::check_timing_sst_keymaymatch_us_sum_{0};

class StreamCallData {
 public:
  enum class OpType {
    kRequest,
    kRead,
    kWrite,
    kFinish,
  };

  StreamCallData(BloomCheck::AsyncService* service, grpc::ServerCompletionQueue* cq,
                 TableReaderCache* cache, ROCKSDB_NAMESPACE::Options* rocksdb_options,
                 Stats* stats, std::mutex* log_mutex, std::atomic<size_t>* open_count,
                 std::atomic<size_t>* active_rpc_count, std::atomic<size_t>* total_rpc_count,
                 std::shared_ptr<ROCKSDB_NAMESPACE::SystemClock> clock,
                 std::shared_ptr<ROCKSDB_NAMESPACE::FileSystem> fs,
                 PerCQWorkerQueue* worker_queue, bool server_stats_enabled)
      : service_(service),
        cq_(cq),
        stream_(&ctx_),
        cache_(cache),
        rocksdb_options_(rocksdb_options),
        stats_(stats),
        log_mutex_(log_mutex),
        open_count_(open_count),
        active_rpc_count_(active_rpc_count),
        total_rpc_count_(total_rpc_count),
        clock_(clock),
        fs_(fs),
        worker_queue_(worker_queue),
        server_stats_enabled_(server_stats_enabled),
        request_tag_(this, OpType::kRequest),
        read_tag_(this, OpType::kRead),
        write_tag_(this, OpType::kWrite),
        finish_tag_(this, OpType::kFinish) {
    service_->RequestCheckBatchStream(&ctx_, &stream_, cq_, cq_, &request_tag_);
  }

  void OnOpComplete(OpType op, bool ok) {
    if (op == OpType::kRequest) {
      HandleRequest(ok);
      return;
    }
    if (op == OpType::kRead) {
      HandleRead(ok);
      return;
    }
    if (op == OpType::kWrite) {
      HandleWrite(ok);
      return;
    }
    HandleFinish();
  }

  void ProcessInWorkerThread(PerCQWorkerQueue::WorkItem item) {
    StreamResponse response;
    response.set_request_id(item.stream_request_id);

    const ROCKSDB_NAMESPACE::Slice user_key(item.stream_user_key);
    auto check_loop_start = std::chrono::steady_clock::now();
    for (uint64_t file_number : item.stream_sst_file_numbers) {
      if (CheckKeyInSST(user_key, file_number)) {
        response.add_may_match_sst_file_numbers(file_number);
      }
    }
    auto check_loop_end = std::chrono::steady_clock::now();
    if (server_stats_enabled_) {
      uint64_t check_loop_us = static_cast<uint64_t>(
          std::chrono::duration_cast<std::chrono::microseconds>(
              check_loop_end - check_loop_start).count());
      stats_->AddCheckLoopUs(check_loop_us);
    }

    EnqueueResponse(std::move(response));
  }

 private:
  class StreamOpTag : public CallDataBase {
   public:
    StreamOpTag(StreamCallData* owner, OpType type) : owner_(owner), type_(type) {}
    void Proceed(bool ok) override { owner_->OnOpComplete(type_, ok); }

   private:
    StreamCallData* owner_;
    OpType type_;
  };

  void HandleRequest(bool ok) {
    if (!ok) {
      delete this;
      return;
    }

    new StreamCallData(service_, cq_, cache_, rocksdb_options_, stats_, log_mutex_,
                       open_count_, active_rpc_count_, total_rpc_count_, clock_, fs_,
                       worker_queue_, server_stats_enabled_);

    active_rpc_count_->fetch_add(1);
    total_rpc_count_->fetch_add(1);

    std::lock_guard<std::mutex> lock(mu_);
    StartReadLocked();
  }

  void StartReadLocked() {
    if (read_in_flight_ || read_closed_ || write_closed_) {
      return;
    }
    read_in_flight_ = true;
    stream_.Read(&read_request_, &read_tag_);
  }

  void HandleRead(bool ok) {
    uint64_t request_id = 0;
    std::string user_key;
    std::vector<uint64_t> sst_numbers;

    if (ok) {
      request_id = read_request_.request_id();
      user_key = std::move(*read_request_.mutable_user_key());
      sst_numbers.reserve(read_request_.sst_file_numbers_size());
      for (uint64_t file_number : read_request_.sst_file_numbers()) {
        sst_numbers.push_back(file_number);
      }
      read_request_.Clear();
    }

    auto enqueue_ts = std::chrono::steady_clock::now();
    {
      std::lock_guard<std::mutex> lock(mu_);
      read_in_flight_ = false;
      if (!ok) {
        read_closed_ = true;
        if (server_stats_enabled_) stats_->AddFinishOrClose();
        MaybeStartFinishLocked();
        return;
      }
      ++pending_work_items_;
      if (server_stats_enabled_) {
        stats_->AddPendingWorkItems(static_cast<uint64_t>(pending_work_items_));
      }
      StartReadLocked();
    }

    if (server_stats_enabled_) {
      stats_->AddRecvReq();
      stats_->AddReadReqEvent();
    }
    worker_queue_->Enqueue(PerCQWorkerQueue::WorkItem(
        this, request_id, std::move(user_key), std::move(sst_numbers), enqueue_ts));
  }

  void EnqueueResponse(StreamResponse response) {
    std::lock_guard<std::mutex> lock(mu_);
    GPR_ASSERT(pending_work_items_ > 0);
    --pending_work_items_;
    if (write_closed_) {
      MaybeStartFinishLocked();
      return;
    }
    write_queue_.push_back(std::move(response));
    if (server_stats_enabled_) {
      stats_->AddWriteqSample(static_cast<uint64_t>(write_queue_.size()));
    }
    if (!write_in_flight_) {
      StartNextWriteLocked();
    }
  }

  void StartNextWriteLocked() {
    if (write_queue_.empty() || write_in_flight_ || write_closed_) {
      MaybeStartFinishLocked();
      return;
    }
    write_in_flight_ = true;
    writing_response_ = std::move(write_queue_.front());
    write_queue_.pop_front();
    stream_.Write(writing_response_, &write_tag_);
  }

  void HandleWrite(bool ok) {
    std::lock_guard<std::mutex> lock(mu_);
    write_in_flight_ = false;
    if (!ok) {
      read_closed_ = true;
      write_closed_ = true;
      read_in_flight_ = false;
      MaybeStartFinishLocked();
      return;
    }
    if (server_stats_enabled_) {
      stats_->AddWriteDone();
      stats_->AddWriteRespEvent();
    }
    StartNextWriteLocked();
  }

  void MaybeStartFinishLocked() {
    if (finish_in_flight_) {
      return;
    }
    if (!read_closed_) {
      return;
    }
    if (pending_work_items_ != 0) {
      return;
    }
    if (write_in_flight_ || !write_queue_.empty()) {
      return;
    }
    finish_in_flight_ = true;
    stream_.Finish(grpc::Status::OK, &finish_tag_);
  }

  void HandleFinish() {
    if (server_stats_enabled_) stats_->AddFinishOrClose();
    active_rpc_count_->fetch_sub(1);
    delete this;
  }

  bool CheckKeyInSST(const ROCKSDB_NAMESPACE::Slice& user_key, uint64_t file_number) {
    std::shared_ptr<TableReaderCache::CacheEntry> entry = cache_->GetOrOpen(
        file_number, *rocksdb_options_, stats_, log_mutex_, open_count_);
    if (!entry) {
      // stats_->RecordReason(CheckReason::OPEN_FAIL);
      return true;
    }

    entry->last_access_us.store(SteadyNowMicros(), std::memory_order_relaxed);
    std::shared_lock<std::shared_mutex> lock(entry->rw_mutex);

    ROCKSDB_NAMESPACE::TableReader* reader = entry->reader.get();
    if (!reader) {
      // stats_->RecordReason(CheckReason::READER_NULL);
      return true;
    }

    ROCKSDB_NAMESPACE::BlockBasedTable* bb_table =
        static_cast<ROCKSDB_NAMESPACE::BlockBasedTable*>(reader);
    if (!bb_table) {
      // stats_->RecordReason(CheckReason::BB_TABLE_NULL);
      return true;
    }

    const ROCKSDB_NAMESPACE::BlockBasedTable::Rep* rep = bb_table->get_rep();
    if (!rep) {
      // stats_->RecordReason(CheckReason::REP_NULL);
      return true;
    }
    if (!rep->filter) {
      // stats_->RecordReason(CheckReason::FILTER_NULL);
      return true;
    }
    if (!rep->whole_key_filtering) {
      // stats_->RecordReason(CheckReason::WHOLE_KEY_FILTERING_FALSE);
      return true;
    }

    const ROCKSDB_NAMESPACE::Comparator* ucmp =
        rep->internal_comparator.user_comparator();
    if (!ucmp) {
      // stats_->RecordReason(CheckReason::UCOMP_NULL);
      return true;
    }

    ROCKSDB_NAMESPACE::ReadOptions read_options;
    read_options.read_tier = ROCKSDB_NAMESPACE::kReadAllTier;
    size_t ts_sz = ucmp->timestamp_size();
    ROCKSDB_NAMESPACE::Slice user_key_without_ts =
        ROCKSDB_NAMESPACE::StripTimestampFromUserKey(user_key, ts_sz);

    ROCKSDB_NAMESPACE::PinnableSlice dummy_value;
    bool dummy_value_found = false;
    ROCKSDB_NAMESPACE::MergeContext dummy_merge_context;
    ROCKSDB_NAMESPACE::SequenceNumber dummy_max_covering_tombstone_seq = 0;
    ROCKSDB_NAMESPACE::GetContext get_context(
        ucmp, nullptr /* merge_operator */, nullptr /* logger */,
        nullptr /* statistics */, ROCKSDB_NAMESPACE::GetContext::kNotFound,
        user_key_without_ts, &dummy_value, nullptr /* columns */,
        &dummy_value_found, &dummy_merge_context, false /* do_merge */,
        &dummy_max_covering_tombstone_seq, clock_.get());
    ROCKSDB_NAMESPACE::BlockCacheLookupContext lookup_context(
        ROCKSDB_NAMESPACE::TableReaderCaller::kUserGet, 0, false);
    ROCKSDB_NAMESPACE::InternalKey ikey_for_seek(
        user_key_without_ts, ROCKSDB_NAMESPACE::kMaxSequenceNumber,
        ROCKSDB_NAMESPACE::kValueTypeForSeek);
    ROCKSDB_NAMESPACE::Slice ikey_slice = ikey_for_seek.Encode();
    const ROCKSDB_NAMESPACE::Slice* const_ikey_ptr = &ikey_slice;

    bool may_match = false;
    try {
      may_match = rep->filter->KeyMayMatch(user_key_without_ts, const_ikey_ptr,
                                           &get_context, &lookup_context, read_options);
      // if (may_match) {
      //   stats_->RecordReason(CheckReason::KEYMAYMATCH_TRUE);
      // } else {
      //   stats_->RecordReason(CheckReason::KEYMAYMATCH_FALSE);
      // }
    } catch (...) {
      // stats_->RecordReason(CheckReason::KEYMAYMATCH_EXCEPTION);
      return true;
    }
    return may_match;
  }

  BloomCheck::AsyncService* service_;
  grpc::ServerCompletionQueue* cq_;
  grpc::ServerContext ctx_;
  grpc::ServerAsyncReaderWriter<StreamResponse, StreamRequest> stream_;
  StreamRequest read_request_;

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
  bool server_stats_enabled_;

  std::mutex mu_;
  std::deque<StreamResponse> write_queue_;
  StreamResponse writing_response_;
  size_t pending_work_items_ = 0;
  bool read_in_flight_ = false;
  bool read_closed_ = false;
  bool write_in_flight_ = false;
  bool write_closed_ = false;
  bool finish_in_flight_ = false;

  StreamOpTag request_tag_;
  StreamOpTag read_tag_;
  StreamOpTag write_tag_;
  StreamOpTag finish_tag_;
};

// PerCQWorkerQueue::WorkerLoop implementation (after CheckCallData definition)
void PerCQWorkerQueue::WorkerLoop() {
  while (true) {
    std::unique_lock<std::mutex> lock(queue_mutex_);
    queue_cv_.wait(lock, [this]() { return !work_queue_.empty() || shutdown_; });
    
    if (shutdown_ && work_queue_.empty()) {
      break;
    }
    
    if (!work_queue_.empty()) {
      uint64_t depth = work_queue_.size();
      WorkItem item = std::move(work_queue_.front());
      work_queue_.pop();
      lock.unlock();

      if (server_stats_enabled_ && stats_) {
        stats_->AddWorkerDeq(depth);
      }
      if (item.kind == WorkItem::Kind::kUnaryCheck) {
        item.call_data->ProcessInWorkerThread(std::move(item));
      } else {
        item.stream_call_data->ProcessInWorkerThread(std::move(item));
      }
    }
  }
}

// Per-RPC state machine for Noop RPC
class NoopCallData : public CallDataBase {
 public:
  NoopCallData(BloomCheck::AsyncService* service, grpc::ServerCompletionQueue* cq)
      : service_(service), cq_(cq), responder_(&ctx_), status_(CREATE) {
    Proceed(true);
  }

  void Proceed(bool ok) override {
    if (!ok) {
      if (status_ == FINISH) {
        delete this;
      }
      return;
    }
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
        fs_(ROCKSDB_NAMESPACE::Env::Default()->GetFileSystem()),
        server_stats_enabled_([]() {
          const char* v = std::getenv("BLOOMCHECK_SERVER_STATS");
          return v && std::strcmp(v, "1") == 0;
        }()) {
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
  bool server_stats_enabled() const { return server_stats_enabled_; }

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
  const bool server_stats_enabled_;

  // Stats printing thread (runs only when server_stats_enabled_)
  std::thread stats_thread_;
  std::atomic<bool> stop_stats_thread_{false};
  
  // Rate-limited logging for filter null cases
  std::atomic<std::chrono::steady_clock::time_point> last_filter_null_log_time_{
      std::chrono::steady_clock::time_point::min()};
  
  void StartStatsThread() {
    if (!server_stats_enabled_) return;
    stats_thread_ = std::thread([this]() {
      uint64_t last_recv_req = 0, last_worker_deq = 0, last_depth_sum = 0, last_depth_count = 0;
      uint64_t last_check_loop_us = 0, last_check_loop_count = 0;
      uint64_t last_writeq_sum = 0, last_writeq_count = 0;
      uint64_t last_write_done = 0, last_finish_or_close = 0;
      uint64_t last_read_req_event = 0, last_write_resp_event = 0;
      while (!stop_stats_thread_.load()) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        if (stop_stats_thread_.load()) break;

        uint64_t recv = stats_.recv_req_count.load(std::memory_order_relaxed);
        uint64_t deq = stats_.worker_deq_count.load(std::memory_order_relaxed);
        uint64_t depth_sum = stats_.worker_queue_depth_sum.load(std::memory_order_relaxed);
        uint64_t depth_count = stats_.worker_queue_depth_count.load(std::memory_order_relaxed);
        uint64_t depth_max = stats_.worker_queue_depth_max.exchange(0, std::memory_order_relaxed);
        uint64_t check_us = stats_.total_check_loop_us.load(std::memory_order_relaxed);
        uint64_t check_n = stats_.check_loop_sample_count.load(std::memory_order_relaxed);
        uint64_t wq_sum = stats_.writeq_sum.load(std::memory_order_relaxed);
        uint64_t wq_count = stats_.writeq_count.load(std::memory_order_relaxed);
        uint64_t wq_max = stats_.writeq_max.exchange(0, std::memory_order_relaxed);
        uint64_t write_done = stats_.write_done_count.load(std::memory_order_relaxed);
        uint64_t finish_close = stats_.finish_or_close_count.load(std::memory_order_relaxed);
        uint64_t read_req_event = stats_.read_req_event_count.load(std::memory_order_relaxed);
        uint64_t write_resp_event =
            stats_.write_resp_event_count.load(std::memory_order_relaxed);
        uint64_t pending_work_items_max =
            stats_.pending_work_items_max.exchange(0, std::memory_order_relaxed);

        uint64_t recv_req_per_s = recv - last_recv_req;
        uint64_t worker_deq_per_s = deq - last_worker_deq;
        uint64_t depth_count_delta = (depth_count >= last_depth_count) ? (depth_count - last_depth_count) : 0;
        uint64_t worker_queue_depth_avg = depth_count_delta ? (depth_sum - last_depth_sum) / depth_count_delta : 0;
        uint64_t check_loop_avg_us = (check_n > last_check_loop_count) ? (check_us - last_check_loop_us) / (check_n - last_check_loop_count) : 0;
        uint64_t wq_count_delta = (wq_count >= last_writeq_count) ? (wq_count - last_writeq_count) : 0;
        uint64_t writeq_avg = wq_count_delta ? (wq_sum - last_writeq_sum) / wq_count_delta : 0;
        uint64_t write_done_per_s = write_done - last_write_done;
        uint64_t finish_or_close_per_s = finish_close - last_finish_or_close;
        uint64_t read_req_per_s = read_req_event - last_read_req_event;
        uint64_t write_resp_per_s = write_resp_event - last_write_resp_event;

        last_recv_req = recv; last_worker_deq = deq; last_depth_sum = depth_sum; last_depth_count = depth_count;
        last_check_loop_us = check_us; last_check_loop_count = check_n;
        last_writeq_sum = wq_sum; last_writeq_count = wq_count;
        last_write_done = write_done; last_finish_or_close = finish_close;
        last_read_req_event = read_req_event; last_write_resp_event = write_resp_event;

        {
          std::lock_guard<std::mutex> log_lock(log_mutex_);
          std::cerr << "[STATS] recv_req_per_s=" << recv_req_per_s
                    << " worker_deq_per_s=" << worker_deq_per_s
                    << " worker_queue_depth_avg=" << worker_queue_depth_avg
                    << " worker_queue_depth_max=" << depth_max
                    << " check_loop_avg_us=" << check_loop_avg_us
                    << " writeq_avg=" << writeq_avg
                    << " writeq_max=" << wq_max
                    << " max_pending_work_items=" << pending_work_items_max
                    << " read_req_per_s=" << read_req_per_s
                    << " write_resp_per_s=" << write_resp_per_s
                    << " write_done_per_s=" << write_done_per_s
                    << " finish_or_close_per_s=" << finish_or_close_per_s
                    << std::endl;
        }
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
                   impl->total_rpc_count(), impl->clock(), impl->fs(), worker_queue,
                   impl->server_stats_enabled());
  new NoopCallData(service, cq);
  new StreamCallData(service, cq, impl->cache(), impl->rocksdb_options(), impl->stats(),
                     impl->log_mutex(), impl->open_count(), impl->active_rpc_count(),
                     impl->total_rpc_count(), impl->clock(), impl->fs(), worker_queue,
                     impl->server_stats_enabled());
  
  void* tag;
  bool ok;
  
  // CQ에서 이벤트 폴링
  // This loop runs in CQ poller thread - must be fast and non-blocking
  while (true) {
    GPR_ASSERT(cq->Next(&tag, &ok));

    static_cast<CallDataBase*>(tag)->Proceed(ok);
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
  builder.AddChannelArgument("grpc.tcp_nodelay", 1);
  builder.AddChannelArgument("grpc.http2.initial_stream_window_size", 1 << 20);
  builder.AddChannelArgument("grpc.http2.initial_connection_window_size", 16 << 20);
  
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
    worker_queues.emplace_back(std::make_unique<bloomcheck::PerCQWorkerQueue>(
        NUM_WORKER_THREADS_PER_CQ, service.stats(), service.server_stats_enabled()));
  }
  // std::cout << "Using " << NUM_WORKER_THREADS_PER_CQ 
  //           << " worker threads per CQ (total: " 
  //           << (NUM_CQS * NUM_WORKER_THREADS_PER_CQ) << ")" << std::endl;
  
  // CQ 폴러 스레드 생성 (각 CQ당 1개 스레드)
  // CQ poller threads: 빠른 이벤트 처리만 수행 (bloom filter 체크 안 함)
  const size_t NUM_POLLERS_PER_CQ = 1;
  const int POLLER_PIN_START_CPU = 15;  // Pin order: 15 -> 14 -> 13 -> ...
  std::atomic<int> poller_pin_index{0};
  std::vector<std::thread> cq_threads;
  for (int i = 0; i < NUM_CQS; i++) {
    for (size_t p = 0; p < NUM_POLLERS_PER_CQ; ++p) {
      cq_threads.emplace_back(std::thread(
          [&, i]() {
            const int pin_idx = poller_pin_index.fetch_add(1, std::memory_order_relaxed);
            const int cpu_id = POLLER_PIN_START_CPU - pin_idx;
            (void)bloomcheck::PinCurrentThreadToCpu(cpu_id);
            bloomcheck::HandleRpcs(service.service(), cqs[i].get(), &service,
                                   worker_queues[i].get());
          }));
    }
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

