#pragma once

#include <cstddef>
#include <cstdint>

namespace pruneoff {

constexpr uint32_t kPruneOffMaxCandidates = 64;
constexpr uint32_t kPruneOffMaxKeyBytes = 256;
constexpr uint32_t kMaxCandidates = kPruneOffMaxCandidates;
constexpr uint32_t kMaxKeyBytes = kPruneOffMaxKeyBytes;

struct PruneOffRequest {
  uint64_t request_id;  // offset 0
  uint32_t num_candidates;  // offset 8
  uint32_t key_len;  // offset 12
  uint8_t noop;  // offset 16
  uint8_t reserved[7];  // offset 17..23
  uint64_t sst_file_numbers[kPruneOffMaxCandidates];  // offset 24
  char key[kPruneOffMaxKeyBytes];  // offset 536
};

struct PruneOffResponse {
  uint64_t request_id;  // offset 0
  uint32_t num_positives;  // offset 8
  uint32_t reserved = 0;  // offset 12
  uint64_t positive_sst_file_numbers[kPruneOffMaxCandidates];  // offset 16
};

static_assert(offsetof(PruneOffRequest, request_id) == 0);
static_assert(offsetof(PruneOffRequest, num_candidates) == 8);
static_assert(offsetof(PruneOffRequest, key_len) == 12);
static_assert(offsetof(PruneOffRequest, noop) == 16);
static_assert(offsetof(PruneOffRequest, sst_file_numbers) == 24);
static_assert(offsetof(PruneOffRequest, key) == 536);
static_assert(sizeof(PruneOffRequest) == 792,
              "PruneOffRequest must stay wire-compatible");

static_assert(offsetof(PruneOffResponse, request_id) == 0);
static_assert(offsetof(PruneOffResponse, num_positives) == 8);
static_assert(offsetof(PruneOffResponse, positive_sst_file_numbers) == 16);
static_assert(sizeof(PruneOffResponse) == 528,
              "PruneOffResponse must stay wire-compatible");

}  // namespace pruneoff
