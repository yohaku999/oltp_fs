#pragma once
#include <map>
#include <set>
#include <string>
#include <utility>

#include "file.h"
#include "frame_directory.h"
#include "storage/page/page.h"

class WAL;

class BufferPool {
 public:
  static constexpr size_t MAX_FRAME_COUNT = 10;
  static constexpr uint16_t HAS_NO_CHILD = -1;
  explicit BufferPool(WAL& wal);
  Page* pinPage(int page_id, File& file);
  void unpinPage(Page* page, File& file);
  uint16_t createPage(PageKind kind, File& file,
                      uint16_t right_most_child_page_id = HAS_NO_CHILD);
  ~BufferPool();

 private:
  static constexpr size_t BUFFER_SIZE_BYTE = 4096 * 10;
  static constexpr size_t FRAME_SIZE_BYTE = 4096;
  static constexpr size_t MAX_PAGE_COUNT = 10;
  void* buffer_;
  WAL& wal_;
  void evictOnePage();
  void zeroOutFrame(int frame_id);
  // Design Intent:
  // BufferPool and FrameDirectory are tightly coupled (1:1, same lifetime).
  // FrameDirectory is held by value (not pointer) because:
  //   - No polymorphism needed for FrameDirectory itself
  //   - They are inseparable (SRP: separate responsibilities, but coupled
  //   lifecycle)
  // Future: Eviction strategies (FIFO/LRU/Clock) will be injected into
  // FrameDirectory via Strategy pattern
  FrameDirectory frame_directory_;
  std::pair<int, char*> acquireFrame();
  bool isPageFlushable(const Page& page) const;
};