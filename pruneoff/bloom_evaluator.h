#pragma once

#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "table/table_reader.h"

namespace pruneoff {

class BloomEvaluator {
 public:
  explicit BloomEvaluator(std::string db_path, size_t cache_capacity = 4096);

  BloomEvaluator(const BloomEvaluator&) = delete;
  BloomEvaluator& operator=(const BloomEvaluator&) = delete;

  // Returns true on Bloom positive. On open/evaluation errors it returns true
  // as the conservative PruneOff fallback: do not prune a candidate SST.
  bool KeyMayMatch(uint64_t file_number, const ROCKSDB_NAMESPACE::Slice& key);

 private:
  struct CacheEntry {
    explicit CacheEntry(const ROCKSDB_NAMESPACE::Options& options);

    ROCKSDB_NAMESPACE::InternalKeyComparator internal_comparator;
    ROCKSDB_NAMESPACE::ImmutableOptions ioptions;
    std::unique_ptr<ROCKSDB_NAMESPACE::TableReader> reader;
  };

  std::shared_ptr<CacheEntry> GetOrOpen(uint64_t file_number);
  ROCKSDB_NAMESPACE::Status OpenTable(uint64_t file_number, CacheEntry* entry);
  std::string ResolveSstPath(uint64_t file_number, uint64_t* file_size) const;

  const std::string db_path_;
  const size_t cache_capacity_;
  ROCKSDB_NAMESPACE::Options options_;
  ROCKSDB_NAMESPACE::EnvOptions env_options_;
  std::shared_ptr<ROCKSDB_NAMESPACE::SystemClock> clock_;

  std::mutex cache_mutex_;
  std::unordered_map<uint64_t, std::shared_ptr<CacheEntry>> cache_;
};

}  // namespace pruneoff
