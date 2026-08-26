// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <chrono>

namespace lmcache {
namespace connector {

// Monotonic-anchored wall clock for connector stage timestamps (same pattern
// as csrc/cuda/event_recorder.cpp). std::chrono::system_clock can be slewed
// backward by NTP/chrony, which would let a later stage stamp land before an
// earlier one. Anchoring once to the (system_clock, steady_clock) pair keeps
// every timestamp monotonic while remaining expressed in Unix-epoch seconds,
// so stamps stay directly comparable to Python's time.time().
inline double wall_clock_time() {
  static const auto epoch_sys = std::chrono::system_clock::now();
  static const auto epoch_steady = std::chrono::steady_clock::now();
  auto now_steady = std::chrono::steady_clock::now();
  auto since_epoch = epoch_sys.time_since_epoch() + (now_steady - epoch_steady);
  return std::chrono::duration<double>(since_epoch).count();
}

}  // namespace connector
}  // namespace lmcache
