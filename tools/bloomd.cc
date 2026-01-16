// Copyright (c) 2024-present, RocksDB Contributors
// This source code is licensed under both the GPLv2 and Apache 2.0 License

#include "tools/bloomcheck.grpc.pb.h"
#include "tools/bloomcheck.pb.h"

#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>

#include <array>
#include <atomic>
#include <cstdlib>
#include <cstring>
#include <chrono>
#include <future>
#include <iostream>
#include <list>
#include <memory>
#include <mutex>
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
  
  void PrintStats(std::mutex& log_mutex, size_t total_opens) {
    std::lock_guard<std::mutex> lock(log_mutex);
    
    size_t hits = cache_hits.load();
    size_t misses = cache_misses.load();
    size_t total_accesses = hits + misses;
    double cache_hit_rate = total_accesses > 0 ? (double)hits / total_accesses : 0.0;
    size_t evictions_count = evictions.load();
    
    std::cerr << "[STATS] cache_hit_rate: " << cache_hit_rate 
              << " (" << hits << "/" << total_accesses << ")" << std::endl;
    std::cerr << "[STATS] evictions: " << evictions_count << std::endl;
    std::cerr << "[STATS] NewTableReader(open): " << total_opens << std::endl;
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
      std::lock_guard<std::mutex> log_lock(*log_mutex);
      std::cerr << "[DBG] file_number=" << file_number << ", actual_path(len=" << actual_path.size() << "): \"" << actual_path << "\"\n";
    }
    
    // Check if TryOpenSstPath found a valid file (file_size > 0 means success)
    // Note: TryOpenSstPath already called GetFileSize, so we check file_size here
    if (actual_path.empty() || file_size == 0) {
      // TryOpenSstPath failed to find any matching file
      if (stats) stats->open_fail_getfilesize++;
      // 샘플 로그 (초기 50회 + 매 1000회) - 락 밖에서
      if (open_id <= 50 || open_id % 1000 == 0) {
        std::lock_guard<std::mutex> log_lock(*log_mutex);
        std::cerr << "[SAMPLE] GetOrOpen #" << open_id 
                  << " FAIL: TryOpenSstPath failed to find file" << std::endl;
        std::cerr << "[SAMPLE]   file_number: " << file_number << std::endl;
        std::cerr << "[SAMPLE]   actual_path: " << (actual_path.empty() ? "(empty)" : actual_path) << std::endl;
      }
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
      if (open_id <= 50 || open_id % 1000 == 0) {
        std::lock_guard<std::mutex> log_lock(*log_mutex);
        std::cerr << "[SAMPLE] GetOrOpen #" << open_id 
                  << " FAIL: NewRandomAccessFile failed" << std::endl;
        std::cerr << "[SAMPLE]   file_number: " << file_number << std::endl;
        std::cerr << "[SAMPLE]   actual_path: " << actual_path << std::endl;
        std::cerr << "[SAMPLE]   file_size: " << file_size << std::endl;
        std::cerr << "[SAMPLE]   error: " << s.ToString() << std::endl;
      }
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
      if (open_id <= 50 || open_id % 1000 == 0) {
        std::lock_guard<std::mutex> log_lock(*log_mutex);
        std::cerr << "[SAMPLE] GetOrOpen #" << open_id 
                  << " FAIL: NewTableReader failed" << std::endl;
        std::cerr << "[SAMPLE]   file_number: " << file_number << std::endl;
        std::cerr << "[SAMPLE]   actual_path: " << actual_path << std::endl;
        std::cerr << "[SAMPLE]   file_size: " << file_size << std::endl;
        std::cerr << "[SAMPLE]   status: " << table_status.ToString() << std::endl;
      }
      // Clean up inflight entry on failure
      {
        std::lock_guard<std::shared_mutex> lock(shard.mutex);
        shard.inflight.erase(file_number);
        inflight->promise.set_value(nullptr);
      }
      return nullptr;
    }

    // 샘플 로그 (성공한 경우) 및 table properties 로그
    if (open_id <= 5 || open_id % 1000 == 0) {
      std::lock_guard<std::mutex> log_lock(*log_mutex);
      std::cerr << "[SAMPLE] GetOrOpen #" << open_id 
                << " SUCCESS file_number=" << file_number 
                << " path=" << actual_path << std::endl;
      
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
          
          // 실제로 열린 파일의 첫 번째 키 확인 (초기 5회만)
          if (open_id <= 5) {
            ROCKSDB_NAMESPACE::ReadOptions ro;
            std::unique_ptr<ROCKSDB_NAMESPACE::InternalIterator> iter(
                table_reader->NewIterator(ro, nullptr, nullptr, false,
                                         ROCKSDB_NAMESPACE::TableReaderCaller::kSSTDumpTool));
            iter->SeekToFirst();
            if (iter->Valid()) {
              ROCKSDB_NAMESPACE::Slice first_key = iter->key();
              std::cerr << "[SAMPLE]   First key in file (hex): " << first_key.ToString(true) << std::endl;
              ROCKSDB_NAMESPACE::ParsedInternalKey parsed;
              if (ROCKSDB_NAMESPACE::ParseInternalKey(first_key, &parsed, false).ok()) {
                std::cerr << "[SAMPLE]   First user_key (hex): " << parsed.user_key.ToString(true) << std::endl;
              }
            }
          }
        } else {
          std::cerr << "[SAMPLE]   rep: null" << std::endl;
        }
      }
    }

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
      std::cerr << "[INFO] Check request #" << req_id 
                << " response size: " << response->resps_size() << std::flush << std::endl;
    }
    
    // 응답 전송 전 로그 (처음 10개)
    if (req_id <= 10) {
      std::cerr << "[INFO] Check request #" << req_id 
                << " about to return grpc::Status::OK" << std::flush << std::endl;
    }

    grpc::Status status = grpc::Status::OK;
    
    // 응답 반환 후 로그 (처음 10개)
    if (req_id <= 10) {
      std::cerr << "[INFO] Check request #" << req_id 
                << " returned grpc::Status::OK" << std::flush << std::endl;
    }
    
    return status;
  }

 private:
  void ProcessKeyRequest(const KeyRequest& req, KeyResponse* resp) {
    static std::atomic<size_t> key_request_count{0};
    size_t key_req_id = key_request_count.fetch_add(1) + 1;
    
    const ROCKSDB_NAMESPACE::Slice user_key(req.user_key());
    
    // total_candidates: 요청된 전체 candidate 파일 수
    uint64_t total_candidates = req.sst_file_numbers_size();
    resp->set_total_candidates(total_candidates);
    
    if (key_req_id <= 10 || key_req_id % 100 == 0) {
      std::cerr << "[INFO] ProcessKeyRequest #" << key_req_id 
                << ": key_len=" << user_key.size() 
                << ", sst_count=" << total_candidates << std::flush << std::endl;
    }

    // checked_count: 실제로 검사한 파일 수 (성공적으로 처리한 파일 수)
    uint64_t checked_count = 0;
    
    for (uint64_t file_number : req.sst_file_numbers()) {
      bool may_match = CheckKeyInSST(user_key, file_number);
      checked_count++;  // CheckKeyInSST 호출 시도 (성공/실패 무관)
      
      if (may_match) {
        resp->add_may_match_sst_file_numbers(file_number);
      }
    }
    
    // checked_count 설정
    resp->set_checked_count(checked_count);
    
    // complete: 모든 candidate 파일을 검사했는지 여부
    // 정상적으로 모든 파일을 검사했다면 true
    // (현재는 항상 true, 나중에 timeout/error 처리 시 false 가능)
    bool complete = (checked_count == total_candidates);
    resp->set_complete(complete);
    
    // 샘플링 로그: key hex, 입력 sst_file_numbers, 출력 may_match_file_numbers
    // may_match_count > 0일 때만 출력 (확인을 수월하게 하기 위해)
    size_t may_match_count = resp->may_match_sst_file_numbers_size();
    bool should_log = (may_match_count > 0);
    
    // 디버깅: CheckKeyInSST가 true를 반환했는데도 추가되지 않는 경우 확인
    if (key_req_id <= 50) {
      std::lock_guard<std::mutex> log_lock(log_mutex_);
      std::cerr << "[DEBUG] ProcessKeyRequest #" << key_req_id 
                << " after loop: may_match_count=" << may_match_count 
                << ", input_sst_count=" << req.sst_file_numbers_size() << std::endl;
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
      
      // std::cerr << "[SAMPLE] ProcessKeyRequest #" << key_req_id << std::endl;
      // std::cerr << "[SAMPLE]   key_hex_prefix: " << key_hex_prefix << std::endl;
      // std::cerr << "[SAMPLE]   input_sst_paths_count: " << req.sst_paths_size() << std::endl;
      // 
      // // 입력 sst_paths 목록 (최대 10개까지만)
      // size_t input_count = std::min(static_cast<size_t>(req.sst_paths_size()), size_t(10));
      // for (size_t i = 0; i < input_count; i++) {
      //   std::cerr << "[SAMPLE]   input_sst_paths[" << i << "]: " << req.sst_paths(i) << std::endl;
      // }
      // if (req.sst_paths_size() > 10) {
      //   std::cerr << "[SAMPLE]   ... (" << (req.sst_paths_size() - 10) << " more input paths)" << std::endl;
      // }
      // 
      // std::cerr << "[SAMPLE]   output_may_match_count: " << may_match_count << std::endl;
      // 
      // // 출력 may_match_paths 목록
      // if (may_match_count > 0) {
      //   for (size_t i = 0; i < may_match_count; i++) {
      //     std::cerr << "[SAMPLE]   may_match_paths[" << i << "]: " << resp->may_match_sst_paths(i) << std::endl;
      //   }
      // } else {
      //   std::cerr << "[SAMPLE]   may_match_paths: (none)" << std::endl;
      // }
    }
    
    if (key_req_id <= 10 || key_req_id % 100 == 0) {
      std::cerr << "[INFO] ProcessKeyRequest #" << key_req_id 
                << " completed: " << may_match_count 
                << " may_match files" << std::flush << std::endl;
    }
  }

  bool CheckKeyInSST(const ROCKSDB_NAMESPACE::Slice& user_key,
                     uint64_t file_number) {
    static std::atomic<size_t> call_count{0};
    size_t call_id = call_count.fetch_add(1) + 1;
    
    if (call_id <= 10) {
      std::cerr << "[DEBUG] CheckKeyInSST #" << call_id 
                << " called for key (hex) " << user_key.ToString(true) 
                << " in SST file_number=" << file_number << std::flush << std::endl;
    }
    
    // Get or open TableReader - returns shared_ptr to guarantee lifetime
    std::shared_ptr<TableReaderCache::CacheEntry> entry = cache_.GetOrOpen(file_number, rocksdb_options_, &stats_, &log_mutex_, &open_count_);
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
    // Note: entry lifetime is guaranteed by shared_ptr even if evicted from cache
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
          std::cerr << "[WARN] FilterNull: file_number=" << file_number << std::endl;
          
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

    // Generate internal key for partitioned filter lookup
    // RocksDB's BlockBasedTable::FullFilterKeyMayMatch creates internal_key from user_key
    // and passes it to filter->KeyMayMatch. We follow the same pattern.
    ROCKSDB_NAMESPACE::InternalKey ikey_for_seek(
        user_key_without_ts, ROCKSDB_NAMESPACE::kMaxSequenceNumber,
        ROCKSDB_NAMESPACE::kValueTypeForSeek);
    ROCKSDB_NAMESPACE::Slice ikey_slice = ikey_for_seek.Encode();
    const ROCKSDB_NAMESPACE::Slice* const_ikey_ptr = &ikey_slice;
    
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
      std::cerr << "[SAMPLE]   user_key_without_ts(hex): " << user_key_without_ts.ToString(true) << std::endl;
      std::cerr << "[SAMPLE]   internal_key(hex): " << const_ikey_ptr->ToString(true) << std::endl;
    }
    
    // Check filter
    // For whole_key_filtering, we use KeyMayMatch
    // Pass internal key (same pattern as BlockBasedTable::FullFilterKeyMayMatch)
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
        stats_.PrintStats(log_mutex_, open_count_.load());
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

