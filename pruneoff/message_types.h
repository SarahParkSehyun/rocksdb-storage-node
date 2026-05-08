#pragma once

#include <cstddef>
#include <cstdint>

namespace pruneoff {

constexpr uint32_t kPruneOffMaxCandidates = 8;
constexpr uint32_t kPruneOffMaxKeyBytes = 256;
constexpr uint32_t kPruneOffMaxFilterBlockSize = 16 * 1024;
constexpr uint32_t kMaxCandidates = kPruneOffMaxCandidates;
constexpr uint32_t kMaxKeyBytes = kPruneOffMaxKeyBytes;

struct PruneOffRequest {
  uint64_t request_id;  // offset 0
  uint32_t client_slot;  // offset 8
  uint32_t num_candidates;  // offset 12
  uint32_t key_len;  // offset 16
  uint8_t noop;  // offset 20
  uint8_t want_filter_blocks;  // offset 21
  uint8_t reserved[6];  // offset 22..27
  uint64_t sst_file_numbers[kPruneOffMaxCandidates];  // offset 32
  char key[kPruneOffMaxKeyBytes];  // offset 96
};

struct FilterBlockEntry {
  uint64_t sst_file_number;  // offset 0
  uint32_t filter_block_size;  // offset 8
  uint32_t _pad;  // offset 12
  uint8_t data[kPruneOffMaxFilterBlockSize];  // offset 16
};

struct PruneOffResponse {
  uint64_t request_id;  // offset 0
  uint32_t client_slot;  // offset 8
  uint32_t num_positives;  // offset 12
  uint32_t num_filter_blocks;  // offset 16
  uint32_t reserved;  // offset 20
  uint64_t positive_sst_file_numbers[kPruneOffMaxCandidates];  // offset 24
  FilterBlockEntry filter_blocks[kPruneOffMaxCandidates];  // offset 88
};

static_assert(offsetof(PruneOffRequest, request_id) == 0);
static_assert(offsetof(PruneOffRequest, client_slot) == 8);
static_assert(offsetof(PruneOffRequest, num_candidates) == 12);
static_assert(offsetof(PruneOffRequest, key_len) == 16);
static_assert(offsetof(PruneOffRequest, noop) == 20);
static_assert(offsetof(PruneOffRequest, want_filter_blocks) == 21);
static_assert(offsetof(PruneOffRequest, sst_file_numbers) == 32);
static_assert(offsetof(PruneOffRequest, key) == 96);
static_assert(sizeof(PruneOffRequest) == 352,
              "PruneOffRequest must stay wire-compatible");

static_assert(offsetof(FilterBlockEntry, sst_file_number) == 0);
static_assert(offsetof(FilterBlockEntry, filter_block_size) == 8);
static_assert(offsetof(FilterBlockEntry, data) == 16);
static_assert(sizeof(FilterBlockEntry) == 16400,
              "FilterBlockEntry must stay wire-compatible");

static_assert(offsetof(PruneOffResponse, request_id) == 0);
static_assert(offsetof(PruneOffResponse, client_slot) == 8);
static_assert(offsetof(PruneOffResponse, num_positives) == 12);
static_assert(offsetof(PruneOffResponse, num_filter_blocks) == 16);
static_assert(offsetof(PruneOffResponse, positive_sst_file_numbers) == 24);
static_assert(offsetof(PruneOffResponse, filter_blocks) == 88);
static_assert(sizeof(PruneOffResponse) == 131288,
              "PruneOffResponse must stay wire-compatible");

}  // namespace pruneoff
