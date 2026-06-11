#pragma once
#include <chrono>
#include <cstdint>
#include <map>
#include <set>
#include <string>
#include <utility>

#include "frame_directory.h"
#include "storage/disk/file.h"
#include "storage/page/page.h"

class WAL;

struct BufferPoolStats {
  std::uint64_t pin_page_calls = 0;
  std::uint64_t resident_hits = 0;
  std::uint64_t misses = 0;
  std::uint64_t evictions = 0;
  std::uint64_t dirty_evictions = 0;
  std::uint64_t read_page_into_buffer_calls = 0;
  std::uint64_t zero_out_frame_calls = 0;
};

class BufferPool {
 public:
  static constexpr size_t MAX_FRAME_COUNT = 16384;
  static constexpr uint16_t HAS_NO_CHILD = -1;
  explicit BufferPool(WAL& wal);
  Page* pinPage(int page_id, File& file);
  void unpinPage(Page* page, File& file);
  uint16_t createPage(PageKind kind, File& file,
                      uint16_t right_most_child_page_id = HAS_NO_CHILD);
  const BufferPoolStats& stats() const { return stats_; }
  ~BufferPool();

 private:
  static constexpr size_t BUFFER_SIZE_BYTE = 4096 * 2 * 16384;
  static constexpr size_t FRAME_SIZE_BYTE = 4096 * 2;
  static constexpr size_t MAX_PAGE_COUNT = 16384;
  void* buffer_;
  WAL& wal_;
  BufferPoolStats stats_;
  std::uint64_t buffer_pool_stats_log_interval_ms_;
  std::chrono::steady_clock::time_point last_buffer_pool_stats_log_at_;
  void evictOnePage();
  void zeroOutFrame(int frame_id);
  void logBufferPoolStatsIfDue();
  // Design Intent:
  // BufferPool and FrameDirectory are tightly coupled (1:1, same lifetime).
  // FrameDirectory is held by value (not pointer) because:
  //   - No polymorphism needed for FrameDirectory itself
  //   - They are inseparable (SRP: separate responsibilities, but coupled
  //   lifecycle)
  // Future: Eviction strategies (FIFO/LRU/Clock) will be injected into
  // FrameDirectory via Strategy pattern
  FrameDirectory frame_directory_;
  std::pair<int, char*> acquireFrame(bool zero_frame);
  bool isPageFlushable(const Page& page) const;
};
