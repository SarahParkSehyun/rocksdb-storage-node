#include "pruneoff/bloom_evaluator.h"

#include <algorithm>
#include <cstdio>
#include <iostream>
#include <utility>

#include "db/dbformat.h"
#include "file/filename.h"
#include "file/random_access_file_reader.h"
#include "rocksdb/cache.h"
#include "rocksdb/env.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/table.h"
#include "table/block_based/block_based_table_reader.h"
#include "table/get_context.h"
#include "table/table_builder.h"

namespace pruneoff {

namespace {

constexpr size_t kBlockCacheSize = 1;
constexpr int kCacheNumShardBits = -1;
constexpr int kBloomBitsPerKey = 10;
constexpr size_t kBlockSize = 4096;

std::string JoinPath(const std::string& dir, const std::string& file) {
  if (dir.empty() || dir.back() == '/') {
    return dir + file;
  }
  return dir + "/" + file;
}

}  // namespace

BloomEvaluator::CacheEntry::CacheEntry(
    const ROCKSDB_NAMESPACE::Options& options)
    : internal_comparator(options.comparator), ioptions(options) {}

BloomEvaluator::BloomEvaluator(std::string db_path, size_t cache_capacity)
    : db_path_(std::move(db_path)),
      cache_capacity_(cache_capacity),
      clock_(ROCKSDB_NAMESPACE::SystemClock::Default()) {
  options_.env = ROCKSDB_NAMESPACE::Env::Default();
  options_.use_direct_reads = true;
  options_.info_log_level = ROCKSDB_NAMESPACE::InfoLogLevel::ERROR_LEVEL;

  ROCKSDB_NAMESPACE::BlockBasedTableOptions table_options;
  table_options.filter_policy.reset(
      ROCKSDB_NAMESPACE::NewBloomFilterPolicy(kBloomBitsPerKey, false));
  table_options.whole_key_filtering = true;
  table_options.index_type =
      ROCKSDB_NAMESPACE::BlockBasedTableOptions::kTwoLevelIndexSearch;
  table_options.partition_filters = true;
  table_options.cache_index_and_filter_blocks = true;
  table_options.pin_top_level_index_and_filter = false;
  table_options.block_size = kBlockSize;
  table_options.block_cache =
      ROCKSDB_NAMESPACE::HyperClockCacheOptions(kBlockCacheSize, 0,
                                                kCacheNumShardBits)
          .MakeSharedCache();
  options_.table_factory.reset(
      ROCKSDB_NAMESPACE::NewBlockBasedTableFactory(table_options));

  env_options_.use_direct_reads = true;
}

bool BloomEvaluator::KeyMayMatch(uint64_t file_number,
                                 const ROCKSDB_NAMESPACE::Slice& key) {
  std::shared_ptr<CacheEntry> entry = GetOrOpen(file_number);
  if (!entry || !entry->reader) {
    return true;
  }

  auto* table = static_cast<ROCKSDB_NAMESPACE::BlockBasedTable*>(
      entry->reader.get());
  if (table == nullptr || table->get_rep() == nullptr) {
    return true;
  }

  const ROCKSDB_NAMESPACE::BlockBasedTable::Rep* rep = table->get_rep();
  if (rep->filter == nullptr || !rep->whole_key_filtering) {
    return true;
  }

  const ROCKSDB_NAMESPACE::Comparator* user_comparator =
      rep->internal_comparator.user_comparator();
  if (user_comparator == nullptr) {
    return true;
  }

  ROCKSDB_NAMESPACE::ReadOptions read_options;
  read_options.read_tier = ROCKSDB_NAMESPACE::kReadAllTier;
  read_options.fill_cache = false;

  const size_t timestamp_size = user_comparator->timestamp_size();
  ROCKSDB_NAMESPACE::Slice user_key_without_ts =
      ROCKSDB_NAMESPACE::StripTimestampFromUserKey(key, timestamp_size);

  ROCKSDB_NAMESPACE::PinnableSlice value;
  bool value_found = false;
  ROCKSDB_NAMESPACE::MergeContext merge_context;
  ROCKSDB_NAMESPACE::SequenceNumber max_covering_tombstone_seq = 0;
  ROCKSDB_NAMESPACE::GetContext get_context(
      user_comparator, nullptr, nullptr, nullptr,
      ROCKSDB_NAMESPACE::GetContext::kNotFound, user_key_without_ts, &value,
      nullptr, &value_found, &merge_context, false,
      &max_covering_tombstone_seq, clock_.get());

  ROCKSDB_NAMESPACE::BlockCacheLookupContext lookup_context(
      ROCKSDB_NAMESPACE::TableReaderCaller::kUserGet, 0, false);
  ROCKSDB_NAMESPACE::InternalKey internal_key(
      user_key_without_ts, ROCKSDB_NAMESPACE::kMaxSequenceNumber,
      ROCKSDB_NAMESPACE::kValueTypeForSeek);
  ROCKSDB_NAMESPACE::Slice internal_key_slice = internal_key.Encode();
  const ROCKSDB_NAMESPACE::Slice* internal_key_ptr = &internal_key_slice;

  return rep->filter->KeyMayMatch(user_key_without_ts, internal_key_ptr,
                                  &get_context, &lookup_context,
                                  read_options);
}

std::shared_ptr<BloomEvaluator::CacheEntry> BloomEvaluator::GetOrOpen(
    uint64_t file_number) {
  {
    CacheShard& shard = GetCacheShard(file_number);
    std::shared_lock<std::shared_mutex> lock(shard.mutex);
    auto it = shard.cache.find(file_number);
    if (it != shard.cache.end()) {
      return it->second;
    }
  }

  std::unique_lock<std::mutex> load_lock(GetLoaderMutex(file_number));
  {
    CacheShard& shard = GetCacheShard(file_number);
    std::shared_lock<std::shared_mutex> lock(shard.mutex);
    auto it = shard.cache.find(file_number);
    if (it != shard.cache.end()) {
      return it->second;
    }
  }

  auto entry = std::make_shared<CacheEntry>(options_);
  ROCKSDB_NAMESPACE::Status status = OpenTable(file_number, entry.get());
  if (!status.ok()) {
    std::cerr << "PruneOff: failed to open SST " << file_number << ": "
              << status.ToString() << "\n";
    return nullptr;
  }

  CacheShard& shard = GetCacheShard(file_number);
  std::unique_lock<std::shared_mutex> lock(shard.mutex);
  auto existing = shard.cache.find(file_number);
  if (existing != shard.cache.end()) {
    return existing->second;
  }
  if (shard.cache.size() >= PerShardCacheCapacity() && !shard.cache.empty()) {
    shard.cache.erase(shard.cache.begin());
  }
  auto [it, inserted] = shard.cache.emplace(file_number, entry);
  (void)inserted;
  return it->second;
}

BloomEvaluator::CacheShard& BloomEvaluator::GetCacheShard(
    uint64_t file_number) {
  return cache_shards_[std::hash<uint64_t>{}(file_number) %
                       cache_shards_.size()];
}

std::mutex& BloomEvaluator::GetLoaderMutex(uint64_t file_number) {
  return loader_mutexes_[std::hash<uint64_t>{}(file_number) %
                         loader_mutexes_.size()];
}

size_t BloomEvaluator::PerShardCacheCapacity() const {
  return std::max<size_t>(
      size_t{1}, (cache_capacity_ + kCacheShardCount - 1) / kCacheShardCount);
}

ROCKSDB_NAMESPACE::Status BloomEvaluator::OpenTable(uint64_t file_number,
                                                    CacheEntry* entry) {
  uint64_t file_size = 0;
  std::string path = ResolveSstPath(file_number, &file_size);
  if (path.empty() || file_size == 0) {
    return ROCKSDB_NAMESPACE::Status::PathNotFound(
        "cannot resolve SST file number " + std::to_string(file_number));
  }

  const auto& fs = options_.env->GetFileSystem();
  ROCKSDB_NAMESPACE::FileOptions file_options(env_options_);
  std::unique_ptr<ROCKSDB_NAMESPACE::FSRandomAccessFile> file;
  ROCKSDB_NAMESPACE::IOStatus io_status =
      fs->NewRandomAccessFile(path, file_options, &file, nullptr);
  if (!io_status.ok()) {
    return io_status;
  }

  auto file_reader =
      std::make_unique<ROCKSDB_NAMESPACE::RandomAccessFileReader>(
          std::move(file), path);

  ROCKSDB_NAMESPACE::ReadOptions read_options;
  read_options.read_tier = ROCKSDB_NAMESPACE::kReadAllTier;

  ROCKSDB_NAMESPACE::TableReaderOptions table_reader_options(
      entry->ioptions, options_.prefix_extractor,
      ROCKSDB_NAMESPACE::UnownedPtr<ROCKSDB_NAMESPACE::CompressionManager>(
          options_.compression_manager.get()),
      env_options_, entry->internal_comparator,
      options_.block_protection_bytes_per_key, false, false, false, -1,
      nullptr, 0, "", file_number, {}, ROCKSDB_NAMESPACE::kMaxSequenceNumber,
      0, entry->ioptions.persist_user_defined_timestamps);
  table_reader_options.largest_seqno = ROCKSDB_NAMESPACE::kMaxSequenceNumber;

  return options_.table_factory->NewTableReader(
      read_options, table_reader_options, std::move(file_reader), file_size,
      &entry->reader, false);
}

std::string BloomEvaluator::ResolveSstPath(uint64_t file_number,
                                           uint64_t* file_size) const {
  const auto& fs = options_.env->GetFileSystem();
  ROCKSDB_NAMESPACE::IOOptions io_options;
  std::string candidates[4];
  candidates[0] = JoinPath(db_path_, std::to_string(file_number) + ".sst");
  candidates[1] = ROCKSDB_NAMESPACE::MakeTableFileName(db_path_, file_number);
  candidates[2] = JoinPath(db_path_, [file_number] {
    char buf[32];
    std::snprintf(buf, sizeof(buf), "%06llu.sst",
                  static_cast<unsigned long long>(file_number));
    return std::string(buf);
  }());
  candidates[3] = JoinPath(db_path_, [file_number] {
    char buf[32];
    std::snprintf(buf, sizeof(buf), "%07llu.sst",
                  static_cast<unsigned long long>(file_number));
    return std::string(buf);
  }());

  for (const std::string& path : candidates) {
    uint64_t size = 0;
    ROCKSDB_NAMESPACE::IOStatus status =
        fs->GetFileSize(path, io_options, &size, nullptr);
    if (status.ok()) {
      *file_size = size;
      return path;
    }
  }

  *file_size = 0;
  return "";
}

}  // namespace pruneoff
