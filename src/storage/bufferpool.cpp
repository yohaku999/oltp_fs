#include "bufferpool.h"

#include <spdlog/spdlog.h>

#include <fstream>
#include <string>

#include "file.h"
#include "logging.h"
#include "page.h"
#include "wal/wal.h"

BufferPool::BufferPool(WAL& wal)
    : buffer_(operator new(BUFFER_SIZE_BYTE)), wal_(wal) {};

// Create new page with incremental pageID.
uint16_t BufferPool::createNewPage(bool is_leaf, File& file,
                                   uint16_t right_most_child_page_id) {
  uint16_t page_id = file.allocateNextPageId();
  auto [frame_id, frame_ptr] = allocateFrame();

  auto page =
      std::make_unique<Page>(frame_ptr, is_leaf, right_most_child_page_id,
                             page_id);
  frame_directory_.registerPage(frame_id, page_id, file.getFilePath(),
                                std::move(page));
  LOG_INFO("Created new page ID {} as {} page in frame ID {}", page_id,
           is_leaf ? "leaf" : "intermediate", frame_id);
  return page_id;
}

Page* BufferPool::getPage(int page_id, File& file) {
  LOG_DEBUG("Requesting page ID {} from file {}", page_id,
            file.getFilePath());
  auto it = frame_directory_.findFrameByPage(page_id, file.getFilePath());
  if (it.has_value()) {
    int frame_id = it.value();
    frame_directory_.pin(frame_id);
    return frame_directory_.getFrame(frame_id).page.get();
  } else {
    auto [frame_id, frame_p] = allocateFrame();

    // load page on frame
    if (!file.isPageIDUsed(page_id)) {
      throw std::logic_error(
          fmt::format("Should not expected to call getPage on uninitialized "
                      "page ID {} in file {}",
                      page_id, file.getFilePath()));
    }
    file.loadPageOnFrame(page_id, frame_p);
    auto page = std::make_unique<Page>(frame_p, page_id);
    Page* page_ptr = page.get();
    frame_directory_.registerPage(frame_id, page_id, file.getFilePath(),
                                  std::move(page));
    frame_directory_.pin(frame_id);
    LOG_DEBUG("Loaded page ID {} into frame ID {}", page_id, frame_id);
    return page_ptr;
  }
};

void BufferPool::unpin(Page* page, File& file) {
  if (!page) {
    throw std::invalid_argument("BufferPool::unpin called with null page");
  }
  auto it = frame_directory_.findFrameByPage(page->getPageID(),
                                             file.getFilePath());
  if (!it.has_value()) {
    throw std::logic_error(
        fmt::format("BufferPool::unpin: page ID {} in file {} is not "
                    "registered in FrameDirectory",
                    page->getPageID(), file.getFilePath()));
  }
  frame_directory_.unpin(it.value());
}

BufferPool::~BufferPool() { operator delete(buffer_); }

// private methods
bool BufferPool::isPageLSNFlushed(const Page& page) const {
  std::uint64_t page_lsn = page.getPageLSN();
  std::uint64_t flushed_lsn = wal_.getFlushedLSN();
  return page_lsn <= flushed_lsn;
}

void BufferPool::evictPage() {
  // decide which unused page to evict.
  auto victim_opt = frame_directory_.findVictimFrame();
  if (!victim_opt.has_value()) {
    // TODO: we should sleep or kill queries.
    throw std::runtime_error(
        "No victim frame found for eviction. All frames are pinned.");
  }
  // write the page back to the file if it's dirty.
  int victim_frame_id = victim_opt.value();
  auto& victim_frame = frame_directory_.getFrame(victim_frame_id);
  // REFACTOR: Have to cache these values before unregistering the page since
  // the frame will be cleared in unregisterPage().
  int evicted_page_id = victim_frame.page_id;
  if (victim_frame.page->isDirty()) {
    if (!isPageLSNFlushed(*victim_frame.page)) {
      wal_.flush();
      // Guard for concurrency mistake just in case.
      if (!isPageLSNFlushed(*victim_frame.page)) {
        throw std::runtime_error(
            "BufferPool::evictPage: WAL flush did not advance flushedLSN "
            "sufficiently for pageLSN");
      }
    }
    // this clearDirty() call is not necessary, since it will be cleared when
    // loading the page again with Page constructor. However, it can help to
    // avoid confusion and potential bugs in the future.
    victim_frame.page->clearDirty();
    File file(victim_frame.file_path);
    file.writePageOnFile(victim_frame.page_id, victim_frame.page->start_p_);
  }
  frame_directory_.unregisterPage(victim_frame_id);
  LOG_INFO("Evicted page ID {} from frame ID {}", evicted_page_id,
           victim_frame_id);
};

void BufferPool::zeroOutFrame(int frame_id) {
  LOG_DEBUG("Zeroing out frame ID: {}", frame_id);
  char* frame_p =
      static_cast<char*>(buffer_) + frame_id * BufferPool::FRAME_SIZE_BYTE;
  std::memset(frame_p, 0, BufferPool::FRAME_SIZE_BYTE);
};

std::pair<int, char*> BufferPool::allocateFrame() {
  auto free_frame = frame_directory_.claimFreeFrame();
  if (!free_frame.has_value()) {
    LOG_DEBUG("No free frame available, attempting eviction");
    evictPage();
    free_frame = frame_directory_.claimFreeFrame();
    if (!free_frame.has_value()) {
      LOG_WARN("Eviction completed but no free frame reclaimed");
    } else {
      LOG_DEBUG("Eviction reclaimed free frame {}", free_frame.value());
    }
  }
  int frame_id = free_frame.value();
  zeroOutFrame(frame_id);
  char* frame_p =
      static_cast<char*>(buffer_) + frame_id * BufferPool::FRAME_SIZE_BYTE;
  return {frame_id, frame_p};
}