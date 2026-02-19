// Copyright (c) 2024-present, RocksDB Contributors
// This source code is licensed under both the GPLv2 and Apache 2.0 License

#include "tools/bloomcheck.pb.h"

#include "transport_impl/infiniband/ib_transport.h"
#include "nexus.h"
#include "rpc.h"

#include <array>
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <future>
#include <iostream>
#include <list>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>

#include "db/dbformat.h"
#include "rocksdb/env.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/sst_file_reader.h"
#include "rocksdb/table.h"
#include "table/block_based/block_based_table_reader.h"
#include "table/get_context.h"
#include "table/table_builder.h"
#include "table/table_reader.h"
#include "util/cast_util.h"

namespace bloomcheck {

static constexpr uint8_t kReqTypeNoop = 1;
static constexpr uint8_t kReqTypeCheck = 2;
static constexpr const char* kDefaultStoragePath = "/mnt/kvafa/db_bench/";
static volatile std::sig_atomic_t g_shutdown_requested = 0;

static bool IsCheckLogEnabled() {
  static bool enabled = []() {
    const char* env = std::getenv("BLOOMCHECK_ERPC_CHECK_LOG");
    return env != nullptr && std::string(env) == "1";
  }();
  return enabled;
}

static bool ShouldSample(size_t seq, size_t warmup, size_t every) {
  return (seq <= warmup) || (every != 0 && seq % every == 0);
}

enum class CheckReason {
  OPEN_FAIL,
  READER_NULL,
  BB_TABLE_NULL,
  REP_NULL,
  FILTER_NULL,
  WHOLE_KEY_FILTERING_FALSE,
  UCOMP_NULL,
  KEYMAYMATCH_EXCEPTION,
  KEYMAYMATCH_TRUE,
  KEYMAYMATCH_FALSE
};

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
};

static std::string NormalizeStoragePath(const std::string& storage_path) {
  if (storage_path.empty()) {
    return std::string(kDefaultStoragePath);
  }
  if (storage_path.back() == '/') {
    return storage_path;
  }
  return storage_path + "/";
}

static std::string TryOpenSstPath(uint64_t file_number,
                                  ROCKSDB_NAMESPACE::FileSystem* fs,
                                  const ROCKSDB_NAMESPACE::FileOptions& fopts,
                                  uint64_t* file_size,
                                  const std::string& storage_path) {
  const std::string base_path = NormalizeStoragePath(storage_path);

  std::string path1 = base_path + std::to_string(file_number) + ".sst";
  ROCKSDB_NAMESPACE::IOStatus s1 =
      fs->GetFileSize(path1, fopts.io_options, file_size, nullptr);
  if (s1.ok()) {
    return path1;
  }

  char padded[32];
  snprintf(padded, sizeof(padded), "%06llu",
           static_cast<unsigned long long>(file_number));
  std::string path2 = base_path + std::string(padded) + ".sst";
  ROCKSDB_NAMESPACE::IOStatus s2 =
      fs->GetFileSize(path2, fopts.io_options, file_size, nullptr);
  if (s2.ok()) {
    return path2;
  }

  snprintf(padded, sizeof(padded), "%07llu",
           static_cast<unsigned long long>(file_number));
  std::string path3 = base_path + std::string(padded) + ".sst";
  ROCKSDB_NAMESPACE::IOStatus s3 =
      fs->GetFileSize(path3, fopts.io_options, file_size, nullptr);
  if (s3.ok()) {
    return path3;
  }

  return path1;
}

class TableReaderCache {
 public:
  struct CacheEntry {
    std::unique_ptr<ROCKSDB_NAMESPACE::TableReader> reader;
    ROCKSDB_NAMESPACE::InternalKeyComparator internal_comparator;
    ROCKSDB_NAMESPACE::ImmutableOptions ioptions;
    std::chrono::steady_clock::time_point last_access;
    std::mutex mutex;

    explicit CacheEntry(const ROCKSDB_NAMESPACE::Options& opt)
        : internal_comparator(opt.comparator),
          ioptions(opt),
          last_access(std::chrono::steady_clock::now()) {}
  };

  struct InflightOpen {
    std::promise<std::shared_ptr<CacheEntry>> promise;
    std::shared_future<std::shared_ptr<CacheEntry>> future;
    InflightOpen() : promise(), future(promise.get_future().share()) {}
  };

  struct Shard {
    mutable std::shared_mutex mutex;
    std::unordered_map<uint64_t, std::shared_ptr<CacheEntry>> cache;
    std::unordered_map<uint64_t, std::shared_ptr<InflightOpen>> inflight;
    std::list<uint64_t> lru_list;
    std::unordered_map<uint64_t, std::list<uint64_t>::iterator> lru_map;
    size_t max_size;

    explicit Shard(size_t max_size_per_shard) : max_size(max_size_per_shard) {}
  };

  static constexpr size_t N_SHARDS = 16;

  explicit TableReaderCache(size_t max_size) {
    size_t max_size_per_shard = (max_size + N_SHARDS - 1) / N_SHARDS;
    for (size_t i = 0; i < N_SHARDS; ++i) {
      shards_[i] = std::make_unique<Shard>(max_size_per_shard);
    }
  }

  std::shared_ptr<CacheEntry> GetOrOpen(
      uint64_t file_number, const ROCKSDB_NAMESPACE::Options& options,
      Stats* stats, std::atomic<size_t>* open_count,
      const std::string& storage_path) {
    Shard& shard = *shards_[std::hash<uint64_t>{}(file_number) & (N_SHARDS - 1)];

    std::shared_ptr<CacheEntry> cached_entry;
    {
      std::shared_lock<std::shared_mutex> lock(shard.mutex);
      auto it = shard.cache.find(file_number);
      if (it != shard.cache.end()) {
        cached_entry = it->second;
        if (stats != nullptr) {
          stats->cache_hits++;
        }
      }
    }
    if (cached_entry != nullptr) {
      std::unique_lock<std::shared_mutex> lock(shard.mutex);
      if (shard.cache.find(file_number) != shard.cache.end()) {
        UpdateLRU(shard, file_number);
      }
      return cached_entry;
    }

    if (stats != nullptr) {
      stats->cache_misses++;
    }

    std::shared_ptr<InflightOpen> inflight;
    bool is_waiting = false;
    {
      std::unique_lock<std::shared_mutex> lock(shard.mutex);
      auto it = shard.cache.find(file_number);
      if (it != shard.cache.end()) {
        UpdateLRU(shard, file_number);
        if (stats != nullptr) {
          stats->cache_hits++;
        }
        return it->second;
      }

      auto inflight_it = shard.inflight.find(file_number);
      if (inflight_it != shard.inflight.end()) {
        inflight = inflight_it->second;
        is_waiting = true;
      } else {
        inflight = std::make_shared<InflightOpen>();
        shard.inflight[file_number] = inflight;
        if (shard.cache.size() >= shard.max_size) {
          EvictLRUOne(shard, stats);
        }
      }
    }

    if (is_waiting && inflight != nullptr) {
      try {
        return inflight->future.get();
      } catch (...) {
        return nullptr;
      }
    }

    auto entry = std::make_unique<CacheEntry>(options);
    ROCKSDB_NAMESPACE::EnvOptions env_options;
    ROCKSDB_NAMESPACE::ReadOptions read_options;
    read_options.read_tier = ROCKSDB_NAMESPACE::kReadAllTier;

    std::unique_ptr<ROCKSDB_NAMESPACE::FSRandomAccessFile> file;
    const auto& fs = options.env->GetFileSystem();
    ROCKSDB_NAMESPACE::FileOptions fopts(env_options);
    uint64_t file_size = 0;
    std::string actual_path =
        TryOpenSstPath(file_number, fs.get(), fopts, &file_size, storage_path);
    if (open_count != nullptr) {
      (*open_count)++;
    }

    if (actual_path.empty() || file_size == 0) {
      if (stats != nullptr) {
        stats->open_fail_getfilesize++;
      }
      std::lock_guard<std::shared_mutex> lock(shard.mutex);
      shard.inflight.erase(file_number);
      inflight->promise.set_value(nullptr);
      return nullptr;
    }

    ROCKSDB_NAMESPACE::IOStatus s =
        fs->NewRandomAccessFile(actual_path, fopts, &file, nullptr);
    if (!s.ok()) {
      if (stats != nullptr) {
        stats->open_fail_newfile++;
      }
      std::lock_guard<std::shared_mutex> lock(shard.mutex);
      shard.inflight.erase(file_number);
      inflight->promise.set_value(nullptr);
      return nullptr;
    }

    std::unique_ptr<ROCKSDB_NAMESPACE::RandomAccessFileReader> file_reader(
        new ROCKSDB_NAMESPACE::RandomAccessFileReader(std::move(file),
                                                      actual_path));

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
    if (!table_status.ok() || table_reader == nullptr) {
      if (stats != nullptr) {
        stats->open_fail_newreader++;
      }
      std::lock_guard<std::shared_mutex> lock(shard.mutex);
      shard.inflight.erase(file_number);
      inflight->promise.set_value(nullptr);
      return nullptr;
    }

    entry->reader = std::move(table_reader);
    std::shared_ptr<CacheEntry> entry_ptr = std::move(entry);

    {
      std::unique_lock<std::shared_mutex> lock(shard.mutex);
      auto it = shard.cache.find(file_number);
      if (it != shard.cache.end()) {
        UpdateLRU(shard, file_number);
        if (stats != nullptr) {
          stats->cache_hits++;
        }
        shard.inflight.erase(file_number);
        inflight->promise.set_value(it->second);
        return it->second;
      }
      shard.cache[file_number] = entry_ptr;
      UpdateLRU(shard, file_number);
      shard.inflight.erase(file_number);
      inflight->promise.set_value(entry_ptr);
    }

    return entry_ptr;
  }

 private:
  static void UpdateLRU(Shard& shard, uint64_t file_number) {
    auto it = shard.lru_map.find(file_number);
    if (it != shard.lru_map.end()) {
      shard.lru_list.splice(shard.lru_list.begin(), shard.lru_list, it->second);
    } else {
      shard.lru_list.push_front(file_number);
      shard.lru_map[file_number] = shard.lru_list.begin();
    }
  }

  static void EvictLRUOne(Shard& shard, Stats* stats) {
    if (shard.lru_list.empty()) {
      return;
    }
    uint64_t lru_file_number = shard.lru_list.back();
    shard.lru_list.pop_back();
    shard.lru_map.erase(lru_file_number);
    auto cache_it = shard.cache.find(lru_file_number);
    if (cache_it != shard.cache.end()) {
      shard.cache.erase(cache_it);
      if (stats != nullptr) {
        stats->evictions++;
      }
    }
  }

  std::array<std::unique_ptr<Shard>, N_SHARDS> shards_;
};

struct BloomcheckErpcServerCtx {
  struct TimingAgg {
    std::atomic<uint64_t> count{0};
    std::atomic<uint64_t> parse_us_sum{0};
    std::atomic<uint64_t> work_us_sum{0};
    std::atomic<uint64_t> serialize_us_sum{0};
    std::atomic<uint64_t> enqueue_us_sum{0};
    std::atomic<uint64_t> handler_us_sum{0};
  };

  erpc::Rpc<erpc::CTransport>* rpc;
  TableReaderCache cache;
  ROCKSDB_NAMESPACE::Options rocksdb_options;
  std::shared_ptr<ROCKSDB_NAMESPACE::SystemClock> clock;
  std::shared_ptr<ROCKSDB_NAMESPACE::FileSystem> fs;
  Stats stats;
  TimingAgg timing;
  std::mutex log_mutex;
  std::atomic<size_t> open_count{0};
  std::string storage_path;

  BloomcheckErpcServerCtx(size_t cache_size, std::string storage_path_arg)
      : rpc(nullptr),
        cache(cache_size),
        rocksdb_options(),
        clock(ROCKSDB_NAMESPACE::SystemClock::Default()),
        fs(ROCKSDB_NAMESPACE::Env::Default()->GetFileSystem()),
        storage_path(std::move(storage_path_arg)) {
    rocksdb_options.env = ROCKSDB_NAMESPACE::Env::Default();
    rocksdb_options.info_log_level =
        ROCKSDB_NAMESPACE::InfoLogLevel::ERROR_LEVEL;
    ROCKSDB_NAMESPACE::BlockBasedTableOptions bbto;
    bbto.filter_policy.reset(
        ROCKSDB_NAMESPACE::NewBloomFilterPolicy(10, false));
    bbto.whole_key_filtering = true;
    bbto.partition_filters = true;
    rocksdb_options.table_factory.reset(
        ROCKSDB_NAMESPACE::NewBlockBasedTableFactory(bbto));
  }
};

struct SstCheckTiming {
  uint64_t get_or_open_us = 0;
  uint64_t keymaymatch_us = 0;
  uint64_t total_us = 0;
};

static bool CheckKeyInSST(BloomcheckErpcServerCtx* ctx,
                          const ROCKSDB_NAMESPACE::Slice& user_key,
                          uint64_t file_number,
                          SstCheckTiming* timing) {
  const auto t0 = std::chrono::steady_clock::now();
  const auto open_start = t0;
  auto entry = ctx->cache.GetOrOpen(file_number, ctx->rocksdb_options, &ctx->stats,
                                    &ctx->open_count, ctx->storage_path);
  const auto open_end = std::chrono::steady_clock::now();
  if (timing != nullptr) {
    timing->get_or_open_us = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::microseconds>(open_end - open_start)
            .count());
  }
  if (entry == nullptr) {
    ctx->stats.RecordReason(CheckReason::OPEN_FAIL);
    if (timing != nullptr) {
      timing->total_us = static_cast<uint64_t>(
          std::chrono::duration_cast<std::chrono::microseconds>(open_end - t0)
              .count());
    }
    return true;
  }

  std::lock_guard<std::mutex> lock(entry->mutex);
  entry->last_access = std::chrono::steady_clock::now();

  ROCKSDB_NAMESPACE::TableReader* reader = entry->reader.get();
  if (reader == nullptr) {
    ctx->stats.RecordReason(CheckReason::READER_NULL);
    return true;
  }

  ROCKSDB_NAMESPACE::BlockBasedTable* bb_table =
      static_cast<ROCKSDB_NAMESPACE::BlockBasedTable*>(reader);
  if (bb_table == nullptr) {
    ctx->stats.RecordReason(CheckReason::BB_TABLE_NULL);
    return true;
  }

  const ROCKSDB_NAMESPACE::BlockBasedTable::Rep* rep = bb_table->get_rep();
  if (rep == nullptr) {
    ctx->stats.RecordReason(CheckReason::REP_NULL);
    return true;
  }
  if (!rep->ioptions.fs || !rep->ioptions.fs.get()) {
    ctx->stats.RecordReason(CheckReason::FILTER_NULL);
    return true;
  }
  if (rep->filter == nullptr) {
    ctx->stats.RecordReason(CheckReason::FILTER_NULL);
    return true;
  }
  if (!rep->whole_key_filtering) {
    ctx->stats.RecordReason(CheckReason::WHOLE_KEY_FILTERING_FALSE);
    return true;
  }

  ROCKSDB_NAMESPACE::ReadOptions read_options;
  read_options.read_tier = ROCKSDB_NAMESPACE::kReadAllTier;

  const ROCKSDB_NAMESPACE::Comparator* ucmp =
      rep->internal_comparator.user_comparator();
  if (ucmp == nullptr) {
    ctx->stats.RecordReason(CheckReason::UCOMP_NULL);
    return true;
  }

  ROCKSDB_NAMESPACE::PinnableSlice dummy_value;
  bool dummy_value_found = false;
  ROCKSDB_NAMESPACE::MergeContext dummy_merge_context;
  ROCKSDB_NAMESPACE::SequenceNumber dummy_max_covering_tombstone_seq = 0;

  size_t ts_sz = ucmp->timestamp_size();
  ROCKSDB_NAMESPACE::Slice user_key_without_ts =
      ROCKSDB_NAMESPACE::StripTimestampFromUserKey(user_key, ts_sz);

  ROCKSDB_NAMESPACE::GetContext get_context(
      ucmp, nullptr /* merge_operator */, nullptr /* logger */,
      nullptr /* statistics */, ROCKSDB_NAMESPACE::GetContext::kNotFound,
      user_key_without_ts, &dummy_value, nullptr /* columns */,
      &dummy_value_found, &dummy_merge_context, false /* do_merge */,
      &dummy_max_covering_tombstone_seq, ctx->clock.get());

  ROCKSDB_NAMESPACE::BlockCacheLookupContext lookup_context(
      ROCKSDB_NAMESPACE::TableReaderCaller::kUserGet, 0 /* tracing_get_id */,
      false /* get_from_user_specified_snapshot */);

  ROCKSDB_NAMESPACE::InternalKey ikey_for_seek(
      user_key_without_ts, ROCKSDB_NAMESPACE::kMaxSequenceNumber,
      ROCKSDB_NAMESPACE::kValueTypeForSeek);
  ROCKSDB_NAMESPACE::Slice ikey_slice = ikey_for_seek.Encode();
  const ROCKSDB_NAMESPACE::Slice* const_ikey_ptr = &ikey_slice;

  bool may_match = false;
  const auto keymaymatch_start = std::chrono::steady_clock::now();
  try {
    may_match = rep->filter->KeyMayMatch(user_key_without_ts, const_ikey_ptr,
                                         &get_context, &lookup_context,
                                         read_options);
    const auto keymaymatch_end = std::chrono::steady_clock::now();
    if (timing != nullptr) {
      timing->keymaymatch_us = static_cast<uint64_t>(
          std::chrono::duration_cast<std::chrono::microseconds>(
              keymaymatch_end - keymaymatch_start)
              .count());
      timing->total_us = static_cast<uint64_t>(
          std::chrono::duration_cast<std::chrono::microseconds>(keymaymatch_end - t0)
              .count());
    }
    if (may_match) {
      ctx->stats.RecordReason(CheckReason::KEYMAYMATCH_TRUE);
    } else {
      ctx->stats.RecordReason(CheckReason::KEYMAYMATCH_FALSE);
    }
  } catch (...) {
    ctx->stats.RecordReason(CheckReason::KEYMAYMATCH_EXCEPTION);
    const auto keymaymatch_end = std::chrono::steady_clock::now();
    if (timing != nullptr) {
      timing->keymaymatch_us = static_cast<uint64_t>(
          std::chrono::duration_cast<std::chrono::microseconds>(
              keymaymatch_end - keymaymatch_start)
              .count());
      timing->total_us = static_cast<uint64_t>(
          std::chrono::duration_cast<std::chrono::microseconds>(keymaymatch_end - t0)
              .count());
    }
    return true;
  }

  return may_match;
}

void SignalHandler(int sig) {
  if (sig == SIGINT) {
    g_shutdown_requested = 1;
  }
}

static uint64_t NowNs() {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return static_cast<uint64_t>(ts.tv_sec) * 1000000000ULL +
         static_cast<uint64_t>(ts.tv_nsec);
}

void NoopHandler(erpc::ReqHandle* req_handle, void* context) {
  auto* ctx = static_cast<BloomcheckErpcServerCtx*>(context);
  if (ctx == nullptr || ctx->rpc == nullptr) {
    return;
  }

  const erpc::MsgBuffer* req_msgbuf = req_handle->get_req_msgbuf();
  const uint8_t* req_buf = req_msgbuf->buf_;
  size_t req_size = req_msgbuf->get_data_size();

  NoopRequest req;
  if (req_size > 0) {
    (void)req.ParseFromArray(req_buf, static_cast<int>(req_size));
  }

  NoopResponse resp;
  resp.set_server_ts_ns(NowNs());

  size_t resp_size = resp.ByteSizeLong();
  ctx->rpc->resize_msg_buffer(&req_handle->pre_resp_msgbuf_, resp_size);
  uint8_t* resp_buf = req_handle->pre_resp_msgbuf_.buf_;
  if (!resp.SerializeToArray(resp_buf, static_cast<int>(resp_size))) {
    NoopResponse empty_resp;
    resp_size = empty_resp.ByteSizeLong();
    ctx->rpc->resize_msg_buffer(&req_handle->pre_resp_msgbuf_, resp_size);
    empty_resp.SerializeToArray(req_handle->pre_resp_msgbuf_.buf_,
                                static_cast<int>(resp_size));
  }
  ctx->rpc->enqueue_response(req_handle, &req_handle->pre_resp_msgbuf_);
}

void CheckHandler(erpc::ReqHandle* req_handle, void* context) {
  auto* ctx = static_cast<BloomcheckErpcServerCtx*>(context);
  if (ctx == nullptr || ctx->rpc == nullptr) {
    return;
  }

  static std::atomic<size_t> check_req_count{0};
  const size_t req_id = check_req_count.fetch_add(1) + 1;
  const auto t0 = std::chrono::steady_clock::now();

  const erpc::MsgBuffer* req_msgbuf = req_handle->get_req_msgbuf();
  const uint8_t* req_buf = req_msgbuf->buf_;
  size_t req_size = req_msgbuf->get_data_size();

  BatchRequest batch_req;
  BatchResponse batch_resp;
  uint64_t total_keys = 0;
  uint64_t aggregate_candidates = 0;
  uint64_t total_may_match = 0;
  uint64_t total_checked = 0;
  uint64_t sst_get_or_open_us_sum = 0;
  uint64_t sst_keymaymatch_us_sum = 0;
  uint64_t sst_total_us_sum = 0;
  bool parse_ok = false;

  const auto parse_start = std::chrono::steady_clock::now();
  if (req_size > 0) {
    parse_ok = batch_req.ParseFromArray(req_buf, static_cast<int>(req_size));
  } else {
    parse_ok = false;
  }
  const auto parse_end = std::chrono::steady_clock::now();

  const auto work_start = std::chrono::steady_clock::now();
  if (parse_ok) {
    batch_resp.clear_resps();
    total_keys = static_cast<uint64_t>(batch_req.reqs_size());
    for (const auto& key_req : batch_req.reqs()) {
      KeyResponse key_resp;
      const ROCKSDB_NAMESPACE::Slice user_key(key_req.user_key());
      const uint64_t key_total_candidates = key_req.sst_file_numbers_size();
      key_resp.set_total_candidates(key_total_candidates);

      uint64_t checked_count = 0;
      for (uint64_t file_number : key_req.sst_file_numbers()) {
        checked_count++;
        SstCheckTiming sst_timing;
        bool may_match = CheckKeyInSST(ctx, user_key, file_number, &sst_timing);
        sst_get_or_open_us_sum += sst_timing.get_or_open_us;
        sst_keymaymatch_us_sum += sst_timing.keymaymatch_us;
        sst_total_us_sum += sst_timing.total_us;
        if (may_match) {
          key_resp.add_may_match_sst_file_numbers(file_number);
        }
      }

      key_resp.set_checked_count(checked_count);
      key_resp.set_complete(checked_count == key_total_candidates);
      aggregate_candidates += key_resp.total_candidates();
      total_checked += key_resp.checked_count();
      total_may_match +=
          static_cast<uint64_t>(key_resp.may_match_sst_file_numbers_size());
      *batch_resp.add_resps() = key_resp;
    }
  } else {
    batch_resp.clear_resps();
  }
  const auto work_end = std::chrono::steady_clock::now();

  const auto ser_start = std::chrono::steady_clock::now();
  size_t resp_size = batch_resp.ByteSizeLong();
  ctx->rpc->resize_msg_buffer(&req_handle->pre_resp_msgbuf_, resp_size);
  uint8_t* resp_buf = req_handle->pre_resp_msgbuf_.buf_;
  if (!batch_resp.SerializeToArray(resp_buf, static_cast<int>(resp_size))) {
    batch_resp.clear_resps();
    resp_size = batch_resp.ByteSizeLong();
    ctx->rpc->resize_msg_buffer(&req_handle->pre_resp_msgbuf_, resp_size);
    batch_resp.SerializeToArray(req_handle->pre_resp_msgbuf_.buf_,
                                static_cast<int>(resp_size));
  }
  const auto ser_end = std::chrono::steady_clock::now();

  const auto enqueue_start = std::chrono::steady_clock::now();
  ctx->rpc->enqueue_response(req_handle, &req_handle->pre_resp_msgbuf_);
  const auto enqueue_end = std::chrono::steady_clock::now();

  const auto t1 = enqueue_end;

  const uint64_t parse_us = static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::microseconds>(parse_end - parse_start)
          .count());
  const uint64_t work_us = static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::microseconds>(work_end - work_start)
          .count());
  const uint64_t serialize_us = static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::microseconds>(ser_end - ser_start)
          .count());
  const uint64_t enqueue_us = static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::microseconds>(enqueue_end - enqueue_start)
          .count());
  const uint64_t handler_us = static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count());

  const uint64_t sample_count = ctx->timing.count.fetch_add(1) + 1;
  ctx->timing.parse_us_sum.fetch_add(parse_us);
  ctx->timing.work_us_sum.fetch_add(work_us);
  ctx->timing.serialize_us_sum.fetch_add(serialize_us);
  ctx->timing.enqueue_us_sum.fetch_add(enqueue_us);
  ctx->timing.handler_us_sum.fetch_add(handler_us);

  if (IsCheckLogEnabled() && ShouldSample(req_id, 10, 1000)) {
    const size_t etid = ctx->rpc->get_etid();
    const uint64_t parse_avg =
        ctx->timing.parse_us_sum.load() / (sample_count == 0 ? 1 : sample_count);
    const uint64_t work_avg =
        ctx->timing.work_us_sum.load() / (sample_count == 0 ? 1 : sample_count);
    const uint64_t serialize_avg =
        ctx->timing.serialize_us_sum.load() / (sample_count == 0 ? 1 : sample_count);
    const uint64_t enqueue_avg =
        ctx->timing.enqueue_us_sum.load() / (sample_count == 0 ? 1 : sample_count);
    const uint64_t handler_avg =
        ctx->timing.handler_us_sum.load() / (sample_count == 0 ? 1 : sample_count);
    std::lock_guard<std::mutex> lg(ctx->log_mutex);
    std::cerr << "[CHECK] req_id=" << req_id
              << " etid=" << etid
              << " parse_ok=" << (parse_ok ? "1" : "0")
              << " req_size=" << req_size
              << " keys=" << total_keys
              << " total_candidates=" << aggregate_candidates
              << " checked=" << total_checked
              << " may_match=" << total_may_match
              << " open_count=" << ctx->open_count.load()
              << " cache_hits=" << ctx->stats.cache_hits.load()
              << " cache_misses=" << ctx->stats.cache_misses.load()
              << " open_fail_getfilesize=" << ctx->stats.open_fail_getfilesize.load()
              << " open_fail_newfile=" << ctx->stats.open_fail_newfile.load()
              << " open_fail_newreader=" << ctx->stats.open_fail_newreader.load()
              << " parse_us=" << parse_us
              << " work_us=" << work_us
              << " serialize_us=" << serialize_us
              << " enqueue_us=" << enqueue_us
              << " handler_us=" << handler_us
              << " parse_avg_us=" << parse_avg
              << " work_avg_us=" << work_avg
              << " serialize_avg_us=" << serialize_avg
              << " enqueue_avg_us=" << enqueue_avg
              << " handler_avg_us=" << handler_avg
              << " sst_get_or_open_us_sum=" << sst_get_or_open_us_sum
              << " sst_keymaymatch_us_sum=" << sst_keymaymatch_us_sum
              << " sst_total_us_sum=" << sst_total_us_sum
              << " sst_get_or_open_avg_us="
              << (total_checked == 0 ? 0 : (sst_get_or_open_us_sum / total_checked))
              << " sst_keymaymatch_avg_us="
              << (total_checked == 0 ? 0 : (sst_keymaymatch_us_sum / total_checked))
              << " sst_total_avg_us="
              << (total_checked == 0 ? 0 : (sst_total_us_sum / total_checked))
              << std::endl;
  }
}

}  // namespace bloomcheck

int main(int argc, char** argv) {
  std::string local_uri = "10.0.0.1:31850";
  size_t numa_node = 0;
  uint8_t rpc_id = 0;
  size_t num_bg_threads = 8;
  size_t cache_size = 512;
  std::string cli_storage_path;

  for (int i = 1; i < argc; i++) {
    std::string arg = argv[i];
    if (arg.find("--local_uri=") == 0) {
      local_uri = arg.substr(12);
    } else if (arg.find("--numa_node=") == 0) {
      numa_node = static_cast<size_t>(std::stoull(arg.substr(12)));
    } else if (arg.find("--rpc_id=") == 0) {
      rpc_id = static_cast<uint8_t>(std::stoi(arg.substr(9)));
    } else if (arg.find("--num_bg_threads=") == 0) {
      num_bg_threads = static_cast<size_t>(std::stoull(arg.substr(17)));
    } else if (arg.find("--cache_size=") == 0) {
      cache_size = static_cast<size_t>(std::stoull(arg.substr(13)));
    } else if (arg.find("--storage_path=") == 0) {
      cli_storage_path = arg.substr(15);
    } else if (arg == "--help" || arg == "-h") {
      std::cout << "Usage: " << argv[0] << " [options]\n"
                << "Options:\n"
                << "  --local_uri=URI         eRPC SM UDP URI\n"
                << "  --numa_node=N           NUMA node (default: 0)\n"
                << "  --rpc_id=ID             RPC ID (default: 0)\n"
                << "  --num_bg_threads=N      Background threads (default: 8)\n"
                << "  --cache_size=N          SST cache size (default: 512)\n"
                << "  --storage_path=PATH     SST storage path\n";
      return 0;
    }
  }

  std::string storage_path;
  if (!cli_storage_path.empty()) {
    storage_path = cli_storage_path;
  } else {
    const char* env_storage = std::getenv("BLOOMCHECK_STORAGE_PATH");
    if (env_storage != nullptr && std::strlen(env_storage) > 0) {
      storage_path = env_storage;
    } else {
      storage_path = bloomcheck::kDefaultStoragePath;
    }
  }
  storage_path = bloomcheck::NormalizeStoragePath(storage_path);

  std::signal(SIGINT, bloomcheck::SignalHandler);

  auto ctx = std::make_unique<bloomcheck::BloomcheckErpcServerCtx>(
      cache_size, storage_path);
  erpc::Nexus nexus(local_uri, numa_node, num_bg_threads);

  int ret = nexus.register_req_func(bloomcheck::kReqTypeNoop,
                                    bloomcheck::NoopHandler,
                                    erpc::ReqFuncType::kForeground);
  if (ret != 0) {
    std::cerr << "[ERROR] Failed to register Noop req func: " << ret
              << std::endl;
    return 1;
  }

  erpc::ReqFuncType check_req_mode = erpc::ReqFuncType::kBackground;
  if (num_bg_threads == 0) {
    std::cerr << "[WARN] --num_bg_threads=0, Check handler uses foreground mode"
              << std::endl;
    check_req_mode = erpc::ReqFuncType::kForeground;
  }
  ret = nexus.register_req_func(bloomcheck::kReqTypeCheck, bloomcheck::CheckHandler,
                                check_req_mode);
  if (ret != 0) {
    std::cerr << "[ERROR] Failed to register Check req func: " << ret
              << std::endl;
    return 1;
  }

  {
    erpc::Rpc<erpc::CTransport> rpc(&nexus, ctx.get(), rpc_id, nullptr);
    ctx->rpc = &rpc;

    std::cout << "[INFO] eRPC bloomcheck server starting..." << std::endl;
    std::cout << "[INFO] local_uri=" << local_uri << std::endl;
    std::cout << "[INFO] numa_node=" << numa_node << std::endl;
    std::cout << "[INFO] rpc_id=" << static_cast<int>(rpc_id) << std::endl;
    std::cout << "[INFO] num_bg_threads=" << num_bg_threads << std::endl;
    std::cout << "[INFO] cache_size=" << cache_size << std::endl;
    std::cout << "[INFO] storage_path=" << storage_path << std::endl;
    std::cout << "[INFO] check_req_mode="
              << (check_req_mode == erpc::ReqFuncType::kBackground
                      ? "background"
                      : "foreground")
              << std::endl;
    std::cout << "[INFO] Press Ctrl+C to stop" << std::endl;

    while (!bloomcheck::g_shutdown_requested) {
      rpc.run_event_loop(50);
    }

    ctx->rpc = nullptr;
  }

  std::cout << "[INFO] Server stopped" << std::endl;
  return 0;
}

