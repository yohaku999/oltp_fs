#include "bufferpool.h"

#include <spdlog/spdlog.h>

#include <fstream>
#include <string>

#include "file.h"
#include "logging.h"
#include "storage/page/page.h"
#include "storage/wal/wal.h"

BufferPool::BufferPool(WAL& wal)
    : buffer_(operator new(BUFFER_SIZE_BYTE)), wal_(wal) {};

uint16_t BufferPool::createPage(PageKind kind, File& file,
                                uint16_t right_most_child_page_id) {
  uint16_t page_id = file.allocateNextPageId();
  auto [frame_id, frame_ptr] = acquireFrame();

  auto page = std::make_unique<Page>(Page::initializeNew(
      frame_ptr, kind, right_most_child_page_id, page_id));
  frame_directory_.registerResidentPage(frame_id, page_id, file.getFilePath(),
                                        std::move(page));
  const char* kind_label = "unknown";
  switch (kind) {
    case PageKind::Heap:
      kind_label = "heap";
      break;
    case PageKind::LeafIndex:
      kind_label = "leaf index";
      break;
    case PageKind::InternalIndex:
      kind_label = "internal index";
      break;
  }
  LOG_INFO("Created new page ID {} as {} page in frame ID {}", page_id,
           kind_label, frame_id);
  return page_id;
}

Page* BufferPool::pinPage(int page_id, File& file) {
  if (!file.isPageIDUsed(page_id)) {
    throw std::logic_error(
        fmt::format("BufferPool::pinPage called for uninitialized page ID {} "
                    "in file {}",
                    page_id, file.getFilePath()));
  }

  LOG_DEBUG("Requesting page ID {} from file {}", page_id, file.getFilePath());
  auto resident_frame_id =
      frame_directory_.findResidentFrame(page_id, file.getFilePath());
  if (resident_frame_id.has_value()) {
    int frame_id = resident_frame_id.value();
    frame_directory_.pin(frame_id);
    return frame_directory_.getFrame(frame_id).page.get();
  } else {
    auto [frame_id, frame_buffer] = acquireFrame();
    file.readPageIntoBuffer(page_id, frame_buffer);
    auto page =
        std::make_unique<Page>(Page::wrapExisting(frame_buffer, page_id));
    Page* loaded_page = page.get();
    frame_directory_.registerResidentPage(frame_id, page_id, file.getFilePath(),
                                          std::move(page));
    frame_directory_.pin(frame_id);
    LOG_DEBUG("Loaded page ID {} into frame ID {}", page_id, frame_id);
    return loaded_page;
  }
};

void BufferPool::unpinPage(Page* page, File& file) {
  if (!page) {
    throw std::invalid_argument("BufferPool::unpinPage called with null page");
  }
  auto resident_frame_id =
      frame_directory_.findResidentFrame(page->getPageID(), file.getFilePath());
  if (!resident_frame_id.has_value()) {
    throw std::logic_error(
        fmt::format("BufferPool::unpinPage: page ID {} in file {} is not "
                    "registered in FrameDirectory",
                    page->getPageID(), file.getFilePath()));
  }
  frame_directory_.unpin(resident_frame_id.value());
}

BufferPool::~BufferPool() { operator delete(buffer_); }

// private methods
bool BufferPool::isPageFlushable(const Page& page) const {
  std::uint64_t page_lsn = page.getPageLSN();
  std::uint64_t flushed_lsn = wal_.getFlushedLSN();
  return page_lsn <= flushed_lsn;
}

void BufferPool::evictOnePage() {
  auto victim_opt = frame_directory_.findVictimFrame();
  if (!victim_opt.has_value()) {
    // TODO: we should sleep or kill queries.
    throw std::runtime_error(
        "No victim frame found for eviction. All frames are pinned.");
  }
  int victim_frame_id = victim_opt.value();
  auto& victim_frame = frame_directory_.getFrame(victim_frame_id);
  int evict_page_id = victim_frame.page_id;
  if (victim_frame.page->isDirty()) {
    if (!isPageFlushable(*victim_frame.page)) {
      wal_.flush();
      if (!isPageFlushable(*victim_frame.page)) {
        // TODO: fix for multithreading later.
        throw std::runtime_error("Victim page ID " +
                                 std::to_string(evict_page_id) +
                                 " is dirty and not flushable. This should not "
                                 "happen in single-threaded execution.");
      }
    }
    File file(victim_frame.file_path);
    file.writePageFromBuffer(evict_page_id, victim_frame.page->data());
  }
  frame_directory_.unregisterResidentPage(victim_frame_id);
  LOG_INFO("Evicted page ID {} from frame ID {}", evict_page_id,
           victim_frame_id);
};

void BufferPool::zeroOutFrame(int frame_id) {
  LOG_DEBUG("Zeroing out frame ID: {}", frame_id);
  char* frame_buffer =
      static_cast<char*>(buffer_) + frame_id * BufferPool::FRAME_SIZE_BYTE;
  std::memset(frame_buffer, 0, BufferPool::FRAME_SIZE_BYTE);
};

std::pair<int, char*> BufferPool::acquireFrame() {
  auto free_frame = frame_directory_.reserveFreeFrame();
  if (!free_frame.has_value()) {
    LOG_DEBUG("No free frame available, attempting eviction");
    evictOnePage();
    free_frame = frame_directory_.reserveFreeFrame();
    if (!free_frame.has_value()) {
      // TODO: this should happen under multithreading when other thread theives
      // the frame right after eviction. Find a way around later.
      throw std::runtime_error(
          "Failed to acquire a free frame even after eviction. This should not "
          "happen in single-threaded execution.");
    } else {
      LOG_DEBUG("Eviction reclaimed free frame {}", free_frame.value());
    }
  }
  int frame_id = free_frame.value();
  zeroOutFrame(frame_id);
  char* frame_buffer =
      static_cast<char*>(buffer_) + frame_id * BufferPool::FRAME_SIZE_BYTE;
  return {frame_id, frame_buffer};
}