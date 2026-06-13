#include "bufferpool.h"

#include <spdlog/spdlog.h>

#include <chrono>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>

#include "logging.h"
#include "storage/disk/file.h"
#include "storage/page/page.h"
#include "storage/wal/wal.h"

namespace {

const char* pageKindLabel(PageKind kind) {
  switch (kind) {
    case PageKind::Heap:
      return "heap";
    case PageKind::LeafIndex:
      return "leaf_index";
    case PageKind::InternalIndex:
      return "internal_index";
  }
  return "unknown";
}

}  // namespace

BufferPool::BufferPool(WAL& wal)
    : buffer_(operator new(BUFFER_SIZE_BYTE)),
      wal_(wal),
      buffer_pool_stats_log_interval_ms_(0),
      buffer_pool_event_log_enabled_(false),
      buffer_pool_event_file_log_enabled_(false),
      buffer_pool_event_id_(0),
      last_buffer_pool_stats_log_at_() {
  const char* event_log_env = std::getenv("DBFS_BUFFER_POOL_EVENT_LOG");
  buffer_pool_event_log_enabled_ =
      event_log_env != nullptr && *event_log_env != '\0' &&
      std::strcmp(event_log_env, "0") != 0 &&
      std::strcmp(event_log_env, "false") != 0 &&
      std::strcmp(event_log_env, "FALSE") != 0;

  const char* event_log_file_env =
      std::getenv("DBFS_BUFFER_POOL_EVENT_LOG_FILE");
  if (event_log_file_env != nullptr && *event_log_file_env != '\0') {
    std::filesystem::path event_log_file_path(event_log_file_env);
    if (event_log_file_path.has_parent_path()) {
      std::filesystem::create_directories(event_log_file_path.parent_path());
    }
    buffer_pool_event_log_file_.open(event_log_file_env,
                                     std::ios::out | std::ios::trunc);
    if (!buffer_pool_event_log_file_.is_open()) {
      throw std::runtime_error(
          fmt::format("failed to open DBFS_BUFFER_POOL_EVENT_LOG_FILE: {}",
                      event_log_file_env));
    }
    buffer_pool_event_file_log_enabled_ = true;
  }

  const char* log_interval_env =
      std::getenv("DBFS_BUFFER_POOL_LOG_STATS_EVERY_MS");
  if (log_interval_env != nullptr && *log_interval_env != '\0') {
    try {
      buffer_pool_stats_log_interval_ms_ = std::stoull(log_interval_env);
    } catch (...) {
      buffer_pool_stats_log_interval_ms_ = 0;
    }
  }
};

uint16_t BufferPool::createPage(PageKind kind, File& file,
                                uint16_t right_most_child_page_id) {
  uint16_t page_id = file.allocateNextPageId();
  auto [frame_id, frame_ptr] = acquireFrame(true);

  auto page = std::make_unique<Page>(
      Page::initializeNew(frame_ptr, kind, right_most_child_page_id, page_id));
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
  dbfs_log::storage().debug("Created new page ID {} as {} page in frame ID {}",
                            page_id, kind_label, frame_id);
  logBufferPoolStatsIfDue();
  return page_id;
}

Page* BufferPool::pinPage(int page_id, File& file) {
  stats_.pin_page_calls++;
  if (!file.isPageIDUsed(page_id)) {
    throw std::logic_error(
        fmt::format("BufferPool::pinPage called for uninitialized page ID {} "
                    "in file {}",
                    page_id, file.getFilePath()));
  }

  dbfs_log::storage().debug("Requesting page ID {} from file {}", page_id,
                            file.getFilePath());
  auto resident_frame_id =
      frame_directory_.findResidentFrame(page_id, file.getFilePath());
  if (resident_frame_id.has_value()) {
    stats_.resident_hits++;
    int frame_id = resident_frame_id.value();
    frame_directory_.pin(frame_id);
    Page* resident_page = frame_directory_.getFrame(frame_id).page.get();
    logBufferPoolPinEvent(file, page_id, frame_id, *resident_page, true);
    logBufferPoolStatsIfDue();
    return resident_page;
  } else {
    stats_.misses++;
    auto [frame_id, frame_buffer] = acquireFrame(false);
    stats_.read_page_into_buffer_calls++;
    file.readPageIntoBuffer(page_id, frame_buffer);
    auto page =
        std::make_unique<Page>(Page::wrapExisting(frame_buffer, page_id));
    Page* loaded_page = page.get();
    frame_directory_.registerResidentPage(frame_id, page_id, file.getFilePath(),
                                          std::move(page));
    frame_directory_.pin(frame_id);
    logBufferPoolPinEvent(file, page_id, frame_id, *loaded_page, false);
    logBufferPoolReadEvent(file, page_id, frame_id, *loaded_page);
    dbfs_log::storage().debug("Loaded page ID {} into frame ID {}", page_id,
                              frame_id);
    logBufferPoolStatsIfDue();
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
  const bool dirty = victim_frame.page->isDirty();
  logBufferPoolEvictEvent(victim_frame.file_path, evict_page_id,
                          victim_frame_id, *victim_frame.page, dirty);
  if (dirty) {
    stats_.dirty_evictions++;
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
  stats_.evictions++;
  frame_directory_.unregisterResidentPage(victim_frame_id);
  dbfs_log::storage().debug("Evicted page ID {} from frame ID {}",
                            evict_page_id, victim_frame_id);
};

void BufferPool::logBufferPoolPinEvent(const File& file, int page_id,
                                       int frame_id, const Page& page,
                                       bool hit) {
  if (!buffer_pool_event_log_enabled_) {
    return;
  }

  writeBufferPoolEvent(fmt::format(
      "buffer_pool_event type=pin event_id={} file={} page_id={} "
      "frame_id={} page_kind={} dirty={} hit={}",
      ++buffer_pool_event_id_, file.getFilePath(), page_id, frame_id,
      pageKindLabel(page.kind()), page.isDirty() ? 1 : 0, hit ? 1 : 0));
}

void BufferPool::logBufferPoolReadEvent(const File& file, int page_id,
                                        int frame_id, const Page& page) {
  if (!buffer_pool_event_log_enabled_) {
    return;
  }

  writeBufferPoolEvent(fmt::format(
      "buffer_pool_event type=read event_id={} file={} page_id={} "
      "frame_id={} page_kind={} dirty={}",
      ++buffer_pool_event_id_, file.getFilePath(), page_id, frame_id,
      pageKindLabel(page.kind()), page.isDirty() ? 1 : 0));
}

void BufferPool::logBufferPoolEvictEvent(const std::string& file_path,
                                         int page_id, int frame_id,
                                         const Page& page, bool dirty) {
  if (!buffer_pool_event_log_enabled_) {
    return;
  }

  writeBufferPoolEvent(fmt::format(
      "buffer_pool_event type=evict event_id={} file={} page_id={} "
      "frame_id={} page_kind={} dirty={}",
      ++buffer_pool_event_id_, file_path, page_id, frame_id,
      pageKindLabel(page.kind()), dirty ? 1 : 0));
}

void BufferPool::writeBufferPoolEvent(const std::string& event) {
  const auto now = std::chrono::system_clock::now();
  const auto unix_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                           now.time_since_epoch())
                           .count();
  if (buffer_pool_event_file_log_enabled_) {
    buffer_pool_event_log_file_ << "logged_at_unix_ms=" << unix_ms << " "
                                << event << "\n";
    return;
  }

  dbfs_log::storage().info("logged_at_unix_ms={} {}", unix_ms, event);
}

void BufferPool::zeroOutFrame(int frame_id) {
  stats_.zero_out_frame_calls++;
  dbfs_log::storage().debug("Zeroing out frame ID: {}", frame_id);
  char* frame_buffer =
      static_cast<char*>(buffer_) + frame_id * BufferPool::FRAME_SIZE_BYTE;
  std::memset(frame_buffer, 0, BufferPool::FRAME_SIZE_BYTE);
};

std::pair<int, char*> BufferPool::acquireFrame(bool zero_frame) {
  auto free_frame = frame_directory_.reserveFreeFrame();
  if (!free_frame.has_value()) {
    dbfs_log::storage().debug("No free frame available, attempting eviction");
    evictOnePage();
    free_frame = frame_directory_.reserveFreeFrame();
    if (!free_frame.has_value()) {
      // TODO: this should happen under multithreading when other thread theives
      // the frame right after eviction. Find a way around later.
      throw std::runtime_error(
          "Failed to acquire a free frame even after eviction. This should not "
          "happen in single-threaded execution.");
    } else {
      dbfs_log::storage().debug("Eviction reclaimed free frame {}",
                                free_frame.value());
    }
  }
  int frame_id = free_frame.value();
  if (zero_frame) {
    zeroOutFrame(frame_id);
  }
  char* frame_buffer =
      static_cast<char*>(buffer_) + frame_id * BufferPool::FRAME_SIZE_BYTE;
  return {frame_id, frame_buffer};
}

void BufferPool::logBufferPoolStatsIfDue() {
  if (buffer_pool_stats_log_interval_ms_ == 0) {
    return;
  }

  auto now = std::chrono::steady_clock::now();
  if (last_buffer_pool_stats_log_at_ !=
          std::chrono::steady_clock::time_point() &&
      now - last_buffer_pool_stats_log_at_ <
          std::chrono::milliseconds(buffer_pool_stats_log_interval_ms_)) {
    return;
  }
  last_buffer_pool_stats_log_at_ = now;
  const auto frame_stats = frame_directory_.collectStats();

  dbfs_log::storage().info(
      "buffer_pool_stats pin_page_calls={} resident_hits={} misses={} "
      "evictions={} dirty_evictions={} reads={} zero_outs={} "
      "frames_free={} frames_resident={} frames_pinned={} "
      "frames_evictable={} resident_heap_pages={} "
      "resident_leaf_index_pages={} resident_internal_index_pages={} "
      "pinned_heap_pages={} pinned_leaf_index_pages={} "
      "pinned_internal_index_pages={} dirty_heap_pages={} "
      "dirty_leaf_index_pages={} dirty_internal_index_pages={}",
      stats_.pin_page_calls, stats_.resident_hits, stats_.misses,
      stats_.evictions, stats_.dirty_evictions,
      stats_.read_page_into_buffer_calls, stats_.zero_out_frame_calls,
      frame_stats.frames_free, frame_stats.frames_resident,
      frame_stats.frames_pinned, frame_stats.frames_evictable,
      frame_stats.resident_heap_pages, frame_stats.resident_leaf_index_pages,
      frame_stats.resident_internal_index_pages, frame_stats.pinned_heap_pages,
      frame_stats.pinned_leaf_index_pages,
      frame_stats.pinned_internal_index_pages, frame_stats.dirty_heap_pages,
      frame_stats.dirty_leaf_index_pages,
      frame_stats.dirty_internal_index_pages);
}
