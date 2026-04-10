#include "lsn_allocator.h"

LSNAllocator::LSNAllocator(LSNAllocator::value_type start) noexcept
    : next_(start) {}

LSNAllocator::value_type LSNAllocator::allocate(
    std::size_t record_size_bytes) noexcept {
  const auto delta = static_cast<value_type>(record_size_bytes);
  return next_.fetch_add(delta, std::memory_order_relaxed);
}

LSNAllocator::value_type LSNAllocator::current() const noexcept {
  return next_.load(std::memory_order_relaxed);
}