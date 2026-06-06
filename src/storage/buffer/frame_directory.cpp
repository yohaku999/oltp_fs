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

std::optional<int> FrameDirectory::findVictimFrame() {
  std::optional<int> first_dirty_victim;

  for (size_t scanned = 0; scanned < MAX_FRAME_COUNT; ++scanned) {
    const int frame_id =
        static_cast<int>((next_victim_frame_ + scanned) % MAX_FRAME_COUNT);
    if (!isEvictable(frame_id)) {
      continue;
    }

    if (frames_[frame_id].page->isDirty()) {
      if (!first_dirty_victim.has_value()) {
        first_dirty_victim = frame_id;
      }
      continue;
    }

    next_victim_frame_ = (static_cast<size_t>(frame_id) + 1) % MAX_FRAME_COUNT;
    dbfs_log::storage().debug("Found clean victim frame {}", frame_id);
    return frame_id;
  }

  if (first_dirty_victim.has_value()) {
    const int frame_id = first_dirty_victim.value();
    next_victim_frame_ = (static_cast<size_t>(frame_id) + 1) % MAX_FRAME_COUNT;
    dbfs_log::storage().debug("Found dirty victim frame {}", frame_id);
    return frame_id;
  }

  // Dump frame directory state to help debugging why no victim exists
  std::string dump = "FrameDirectory dump: ";
  for (size_t i = 0; i < MAX_FRAME_COUNT; ++i) {
    dump += fmt::format("[id={} pin={} has_page={}] ", i, frames_[i].pin_count,
                        frames_[i].page != nullptr);
  }
  dbfs_log::storage().warn(
      "No evictable frames found (all pinned or empty). {}", dump);
  return std::nullopt;
}

const FrameDirectory::Frame& FrameDirectory::getFrame(int frame_id) const {
  return frames_[frame_id];
}

FrameDirectory::Frame& FrameDirectory::getFrame(int frame_id) {
  return frames_[frame_id];
}
