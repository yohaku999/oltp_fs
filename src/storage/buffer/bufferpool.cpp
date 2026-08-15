#include "bufferpool.h"

#include <spdlog/spdlog.h>

#include <chrono>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <memory>
#include <sstream>
#include <string>
#include <unordered_map>

#include "logging.h"
#include "page_key.h"
#include "storage/disk/pagedfile.h"
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
      buffer_pool_reset_generation_(0),
      last_buffer_pool_stats_log_at_(),
      replacement_policy(MAX_FRAME_COUNT) {
  const char* event_log_env = std::getenv("DBFS_BUFFER_POOL_EVENT_LOG");
  buffer_pool_event_log_enabled_ = event_log_env != nullptr &&
                                   *event_log_env != '\0' &&
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

uint16_t BufferPool::createPage(PageKind kind, PagedFile& file,
                                uint16_t right_most_child_page_id) {
  uint16_t page_id = file.allocateNextPageId();
  const PageKey page_key{file.getFilePath(), page_id};
  auto [frame_id, frame_ptr] = acquireFrame(true, page_key);

  auto page = std::make_unique<Page>(frame_ptr, kind, right_most_child_page_id,
                                     page_id);
  frame_directory_.registerResidentPage(frame_id, page_id, file.getFilePath(),
                                        std::move(page));
  replacement_policy.onNoneResidentPageAccess(frame_id, page_key);
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

Page* BufferPool::pinPage(int page_id, PagedFile& file) {
  stats_.pin_page_calls++;
  if (!file.isPageIDUsed(page_id)) {
    throw std::logic_error(
        fmt::format("BufferPool::pinPage called for uninitialized page ID {} "
                    "in file {}",
                    page_id, file.getFilePath()));
  }

  dbfs_log::storage().debug("Requesting page ID {} from file {}", page_id,
                            file.getFilePath());
  PageKey page_key{file.getFilePath(), page_id};

  auto resident_frame_id =
      frame_directory_.findResidentFrame(page_id, file.getFilePath());
  if (resident_frame_id.has_value()) {
    replacement_policy.onResidentPageAccess(page_key);
    stats_.resident_hits++;
    int frame_id = resident_frame_id.value();
    frame_directory_.pin(frame_id);
    Page* resident_page = frame_directory_.getFrame(frame_id).page.get();
    logBufferPoolPinEvent(file, page_id, frame_id, *resident_page, true);
    logBufferPoolStatsIfDue();
    return resident_page;
  } else {
    stats_.misses++;
    auto [frame_id, frame_buffer] = acquireFrame(false, page_key);
    stats_.read_page_into_buffer_calls++;
    file.readPageIntoBuffer(page_id, frame_buffer);
    auto page = std::make_unique<Page>(frame_buffer, page_id);
    Page* loaded_page = page.get();
    frame_directory_.registerResidentPage(frame_id, page_id, file.getFilePath(),
                                          std::move(page));
    replacement_policy.onNoneResidentPageAccess(frame_id, page_key);
    frame_directory_.pin(frame_id);
    logBufferPoolPinEvent(file, page_id, frame_id, *loaded_page, false);
    logBufferPoolReadEvent(file, page_id, frame_id, *loaded_page);
    dbfs_log::storage().debug("Loaded page ID {} into frame ID {}", page_id,
                              frame_id);
    logBufferPoolStatsIfDue();
    return loaded_page;
  }
};

void BufferPool::unpinPage(Page* page, PagedFile& file) {
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

BufferPoolResetResult BufferPool::resetForBenchmark() {
  BufferPoolResetResult result;

  // Validate the entire pool before writing or clearing anything so a failed
  // reset cannot leave the directory and replacement policy out of sync.
  for (size_t frame_id = 0; frame_id < MAX_FRAME_COUNT; ++frame_id) {
    const auto& frame = frame_directory_.getFrame(static_cast<int>(frame_id));
    if (frame.page == nullptr) {
      continue;
    }
    if (frame.pin_count > 0) {
      throw std::runtime_error(fmt::format(
          "cannot reset buffer pool while frame {} is pinned", frame_id));
    }
    ++result.resident_frames_cleared;
    if (frame.page->isDirty()) {
      ++result.dirty_frames_flushed;
    }
  }

  if (result.dirty_frames_flushed > 0) {
    wal_.flush();
    std::unordered_map<std::string, std::unique_ptr<PagedFile>> files;
    for (size_t frame_id = 0; frame_id < MAX_FRAME_COUNT; ++frame_id) {
      auto& frame = frame_directory_.getFrame(static_cast<int>(frame_id));
      if (frame.page != nullptr && frame.page->isDirty()) {
        auto [file_it, inserted] = files.try_emplace(frame.file_path, nullptr);
        if (inserted) {
          file_it->second = std::make_unique<PagedFile>(frame.file_path);
        }
        persistPage(frame.page_id, *file_it->second, *frame.page);
      }
    }
  }

  frame_directory_.reset();
  replacement_policy.reset();
  stats_ = {};
  last_buffer_pool_stats_log_at_ = {};

  result.generation = ++buffer_pool_reset_generation_;
  writeBufferPoolEvent(
      fmt::format("buffer_pool_reset reason=benchmark_post_load generation={} "
                  "resident_frames_cleared={} dirty_frames_flushed={}",
                  result.generation, result.resident_frames_cleared,
                  result.dirty_frames_flushed));
  logBufferPoolStatsIfDue();
  return result;
}

// private methods
bool BufferPool::isPageLSNDurable(const Page& page) const {
  std::uint64_t page_lsn = page.getPageLSN();
  std::uint64_t flushed_lsn = wal_.getFlushedLSN();
  return page_lsn <= flushed_lsn;
}

void BufferPool::evictFrame(int victim_frame_id) {
  auto& victim_frame = frame_directory_.getFrame(victim_frame_id);
  int evict_page_id = victim_frame.page_id;
  const bool dirty = victim_frame.page->isDirty();
  logBufferPoolEvictEvent(victim_frame.file_path, evict_page_id,
                          victim_frame_id, *victim_frame.page, dirty);
  if (dirty) {
    stats_.dirty_evictions++;
    PagedFile file(victim_frame.file_path);
    persistPage(evict_page_id, file, *victim_frame.page);
  }
  stats_.evictions++;
  const PageKey key{victim_frame.file_path, evict_page_id};
  replacement_policy.onEviction(victim_frame_id, key);
  frame_directory_.unregisterResidentPage(victim_frame_id);
  dbfs_log::storage().debug("Evicted page ID {} from frame ID {}",
                            evict_page_id, victim_frame_id);
};

void BufferPool::persistPage(int page_id, PagedFile& file, Page& page) {
  if (!isPageLSNDurable(page)) {
    wal_.flush();
    if (!isPageLSNDurable(page)) {
      // TODO: fix for multithreading later.
      throw std::runtime_error("Victim page ID " + std::to_string(page_id) +
                               " is dirty and not flushable. This should not "
                               "happen in single-threaded execution.");
    }
  }
  file.writePageFromBuffer(page_id, page.data());
}

void BufferPool::logBufferPoolPinEvent(const PagedFile& file, int page_id,
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

void BufferPool::logBufferPoolReadEvent(const PagedFile& file, int page_id,
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

  writeBufferPoolEvent(
      fmt::format("buffer_pool_event type=evict event_id={} file={} page_id={} "
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

std::pair<int, char*> BufferPool::acquireFrame(bool zero_frame,
                                               const PageKey& page_key) {
  replacement_policy.onNonResidentPageRequest(page_key);

  auto free_frame = frame_directory_.reserveFreeFrame();
  int target_frame_id = -1;
  if (!free_frame.has_value()) {
    dbfs_log::storage().debug("No free frame available, attempting eviction");
    auto victim_frame_id =
        replacement_policy.selectVictimFrame(page_key, [this](int frame_id) {
          return frame_directory_.isEvictable(frame_id);
        });
    if (!victim_frame_id.has_value()) {
      throw std::runtime_error("All frame might be inevictable.");
    }
    target_frame_id = victim_frame_id.value();
    evictFrame(target_frame_id);
    auto reclaimed_frame = frame_directory_.reserveFreeFrame();
    if (!reclaimed_frame.has_value() ||
        reclaimed_frame.value() != target_frame_id) {
      throw std::runtime_error("Evicted frame was not available for reuse.");
    }
  } else {
    target_frame_id = free_frame.value();
  }
  // 書き込みを行う。
  if (zero_frame) {
    zeroOutFrame(target_frame_id);
  }
  char* frame_buffer = static_cast<char*>(buffer_) +
                       target_frame_id * BufferPool::FRAME_SIZE_BYTE;
  return {target_frame_id, frame_buffer};
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
      "buffer_pool_generation={} "
      "arc_t1={} arc_t2={} arc_b1={} arc_b2={} arc_t1_target={} "
      "arc_b1_hits={} arc_b2_hits={} arc_cold_misses={} "
      "frames_free={} frames_resident={} frames_pinned={} "
      "frames_evictable={} resident_heap_pages={} "
      "resident_leaf_index_pages={} resident_internal_index_pages={} "
      "pinned_heap_pages={} pinned_leaf_index_pages={} "
      "pinned_internal_index_pages={} dirty_heap_pages={} "
      "dirty_leaf_index_pages={} dirty_internal_index_pages={}",
      stats_.pin_page_calls, stats_.resident_hits, stats_.misses,
      stats_.evictions, stats_.dirty_evictions,
      stats_.read_page_into_buffer_calls, stats_.zero_out_frame_calls,
      buffer_pool_reset_generation_,
      replacement_policy.t1Size(), replacement_policy.t2Size(),
      replacement_policy.b1Size(), replacement_policy.b2Size(),
      replacement_policy.t1Target(), replacement_policy.b1Hits(),
      replacement_policy.b2Hits(), replacement_policy.coldMisses(),
      frame_stats.frames_free, frame_stats.frames_resident,
      frame_stats.frames_pinned, frame_stats.frames_evictable,
      frame_stats.resident_heap_pages, frame_stats.resident_leaf_index_pages,
      frame_stats.resident_internal_index_pages, frame_stats.pinned_heap_pages,
      frame_stats.pinned_leaf_index_pages,
      frame_stats.pinned_internal_index_pages, frame_stats.dirty_heap_pages,
      frame_stats.dirty_leaf_index_pages,
      frame_stats.dirty_internal_index_pages);
}
