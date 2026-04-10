#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>

// LSNAllocator is responsible for assigning monotonically increasing
// log sequence numbers (LSNs) for WAL records based on their size.
//
// The numerical value it returns is interpreted as a byte offset from
// the beginning of the WAL stream. Given a record that occupies N bytes
// in the WAL (including header and body), callers pass N to allocate(),
// and receive the starting offset for that record.
//
// Concurrency:
// - allocate() is safe to call from multiple writer threads.
// - Internally it uses an atomic fetch_add to ensure unique,
//   strictly increasing LSNs without external locking.
class LSNAllocator {
 public:
  using value_type = std::uint64_t;

  explicit LSNAllocator(value_type start = 0) noexcept;
  value_type allocate(std::size_t record_size_bytes) noexcept;
  value_type current() const noexcept;

 private:
  std::atomic<value_type> next_;
};
