#include "frame_directory.h"

#include <optional>
#include <utility>

#include "logging.h"
FrameDirectory::FrameDirectory() {
  for (size_t i = 0; i < MAX_FRAME_COUNT; ++i) {
    free_frames_.insert(static_cast<int>(i));
  }
}

std::optional<int> FrameDirectory::reserveFreeFrame() {
  if (free_frames_.empty()) {
    return std::nullopt;
  }

  int frame_id = *free_frames_.begin();
  free_frames_.erase(free_frames_.begin());
  dbfs_log::storage().debug("Found free frame {}", frame_id);
  return frame_id;
}

std::optional<int> FrameDirectory::findResidentFrame(
    int page_id, const std::string& file_path) {
  auto key = std::make_pair(page_id, file_path);
  auto resident_frame = page_to_frame_.find(key);
  if (resident_frame != page_to_frame_.end()) {
    return resident_frame->second;
  }
  return std::nullopt;
}

void FrameDirectory::registerResidentPage(int frame_id, int page_id,
                                          const std::string& file_path,
                                          std::unique_ptr<Page> page) {
  frames_[frame_id].page = std::move(page);
  frames_[frame_id].page_id = page_id;
  frames_[frame_id].file_path = file_path;
  frames_[frame_id].pin_count = 0;

  auto key = std::make_pair(page_id, file_path);
  page_to_frame_[key] = frame_id;

  dbfs_log::storage().debug("Registered page {} from {} in frame {}", page_id,
                            file_path, frame_id);
}

void FrameDirectory::unregisterResidentPage(int frame_id) {
  auto key =
      std::make_pair(frames_[frame_id].page_id, frames_[frame_id].file_path);
  page_to_frame_.erase(key);

  frames_[frame_id].clear();

  free_frames_.insert(frame_id);

  dbfs_log::storage().debug("Unregistered and deleted page from frame {}",
                            frame_id);
}

void FrameDirectory::reset() {
  page_to_frame_.clear();
  free_frames_.clear();
  for (size_t i = 0; i < frames_.size(); ++i) {
    frames_[i].clear();
    free_frames_.insert(static_cast<int>(i));
  }
  next_victim_frame_ = 0;
}

void FrameDirectory::pin(int frame_id) {
  frames_[frame_id].pin_count++;
  dbfs_log::storage().debug("Marked frame {} as pinned, count = {}", frame_id,
                            frames_[frame_id].pin_count);
}

void FrameDirectory::unpin(int frame_id) {
  if (frames_[frame_id].pin_count > 0) {
    frames_[frame_id].pin_count--;
    dbfs_log::storage().debug("Marked frame {} as unpinned, count = {}",
                              frame_id, frames_[frame_id].pin_count);
  }
}

bool FrameDirectory::isPinned(int frame_id) const {
  return frames_[frame_id].pin_count > 0;
}

bool FrameDirectory::isEvictable(int frame_id) const {
  return frames_[frame_id].pin_count == 0 && frames_[frame_id].page != nullptr;
}

FrameDirectoryStats FrameDirectory::collectStats() const {
  FrameDirectoryStats stats;

  for (const auto& frame : frames_) {
    if (frame.page == nullptr) {
      stats.frames_free++;
      continue;
    }

    stats.frames_resident++;
    if (frame.pin_count > 0) {
      stats.frames_pinned++;
    } else {
      stats.frames_evictable++;
    }

    switch (frame.page->kind()) {
      case PageKind::Heap:
        stats.resident_heap_pages++;
        if (frame.pin_count > 0) {
          stats.pinned_heap_pages++;
        }
        if (frame.page->isDirty()) {
          stats.dirty_heap_pages++;
        }
        break;
      case PageKind::LeafIndex:
        stats.resident_leaf_index_pages++;
        if (frame.pin_count > 0) {
          stats.pinned_leaf_index_pages++;
        }
        if (frame.page->isDirty()) {
          stats.dirty_leaf_index_pages++;
        }
        break;
      case PageKind::InternalIndex:
        stats.resident_internal_index_pages++;
        if (frame.pin_count > 0) {
          stats.pinned_internal_index_pages++;
        }
        if (frame.page->isDirty()) {
          stats.dirty_internal_index_pages++;
        }
        break;
    }
  }

  return stats;
}

const FrameDirectory::Frame& FrameDirectory::getFrame(int frame_id) const {
  return frames_[frame_id];
}

FrameDirectory::Frame& FrameDirectory::getFrame(int frame_id) {
  return frames_[frame_id];
}
