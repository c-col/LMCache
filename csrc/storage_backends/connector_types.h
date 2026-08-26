// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

// there are four "types" in this file:
// 1. Op: enum class for the three batched operations (GET, SET, EXISTS)
// 2. BatchState: shared communication state between threads executing a single
// batch operation
// 3. Request: the request data structure for a single operation (submitted to
// SQ)
// 4. Completion: the completion data structure for a single operation
// (collected from CQ)

namespace lmcache {
namespace connector {

// we only support batched operations
// benefits are fewer submissions and fewer completions
enum class Op : uint8_t {
  BATCH_TILE_GET,
  BATCH_TILE_SET,
  BATCH_TILE_EXISTS,
  BATCH_TILE_DELETE
};

/*
shared communication state between threads executing a single batch operation.
all threads need to complete before the completion is sent.

tiling refers to dividing work for batched operations between threads
beforehand.
*/
struct BatchState {
  std::atomic<uint32_t> remaining_tiles{0};
  std::atomic<bool> any_failed{false};

  std::mutex err_mu;
  std::string first_error;

  // Per-key success/failure results used by both EXISTS and GET.
  // For EXISTS: 1 = key found, 0 = not found.
  // For GET: 1 = read succeeded, 0 = read failed (e.g. file
  //   not found).  This enables per-key error tolerance on loads.
  // IMPORTANT: not vector<bool> due to concurrent write data race
  std::vector<uint8_t> per_key_results;

  Op batch_op;

  // -- stage timing (epoch seconds from connector_clock.h) --
  // t_submit / num_keys / total_bytes are written once on the submitting
  // thread before any tile is enqueued (the SQ mutex publishes them).
  // t_first_dequeue / t_first_byte are first-writer-wins CAS slots shared by
  // all tiles; 0.0 is a safe "unset" sentinel because a real anchored
  // timestamp is never 0.0. t_last_done is written only by the last tile.
  // Cross-tile visibility for the last tile's reads is provided by the
  // acq_rel fetch_sub on remaining_tiles in handle_tile_completion.
  double t_submit = 0.0;
  std::atomic<double> t_first_dequeue{0.0};
  std::atomic<double> t_first_byte{0.0};  // 0.0 = backend never reported it
  double t_last_done = 0.0;
  uint64_t num_keys = 0;
  uint64_t total_bytes = 0;  // sum of buf_lens (GET/SET); 0 for EXISTS/DELETE
};

// Per-batch stage-timing record, drained by Python via drain_batch_timings()
// alongside (and matched by future_id to) the batch's Completion. All
// timestamps come from connector_clock.h and are directly comparable to
// Python's time.time().
struct BatchTiming {
  uint64_t future_id = 0;
  Op op;
  uint64_t num_keys = 0;
  uint64_t total_bytes = 0;
  double t_submit = 0.0;
  double t_first_dequeue = 0.0;
  double t_first_byte = 0.0;  // 0.0 when the backend never reported it
  double t_last_done = 0.0;
};

/*
LIFETIME GUARANTEE:
we have a strict assumption that Python will NOT clean up any buffer memory
before all C++ operations finish. This is guaranteed by the Python-side design
where the caller holds references to all buffers until drain_completions()
returns the corresponding future_id. Therefore, we do NOT need to track
buf_owner references or acquire the GIL to prevent premature cleanup.
we can safely use raw pointers extracted under the GIL without additional
lifetime management on the C++ side.
*/

struct Request {
  uint64_t future_id = 0;
  Op op;

  // all operations use the batched structure (even single-item operations
  // are treated as batches of size 1)
  std::vector<std::string> keys;
  std::vector<void*> buf_ptrs;
  std::vector<size_t> buf_lens;

  // shared batch state between threads executing a single batch operation
  // so that they can coordinate when to send the completion
  std::shared_ptr<BatchState> batch;

  // for batch exists tiles, track which indices this tile is responsible for
  size_t start_idx = 0;

  // batch_chunk_num_bytes for get/set operations (passed per-operation, not
  // per-connection)
  size_t batch_chunk_num_bytes = 0;
};

struct Completion {
  uint64_t future_id = 0;

  bool ok = true;

  // for EXISTS operations, store boolean results as
  // bytes (0/1). Single EXISTS will have 1 element, batch EXISTS will have N
  // elements. No result in the completion for SET and GET.
  std::vector<uint8_t> result_bytes;

  std::string error;
};

}  // namespace connector
}  // namespace lmcache
