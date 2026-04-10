#include "frame_directory.h"

#include <optional>
#include <random>
#include <utility>
#include <vector>

#include "logging.h"
FrameDirectory::FrameDirectory() {
  for (int i = 0; i < MAX_FRAME_COUNT; ++i) {
    free_frames_.insert(i);
  }
}

std::optional<int> FrameDirectory::reserveFreeFrame() {
  if (free_frames_.empty()) {
    return std::nullopt;
  }

  int frame_id = *free_frames_.begin();
  free_frames_.erase(free_frames_.begin());
  LOG_DEBUG("Found free frame {}", frame_id);
  return frame_id;
}

std::optional<int> FrameDirectory::findResidentFrame(
    int page_id, const std::string& file_path) {
  auto key = std::make_pair(page_id, file_path);
  auto it = page_to_frame_.find(key);
  if (it != page_to_frame_.end()) {
    return it->second;
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

  LOG_DEBUG("Registered page {} from {} in frame {}", page_id, file_path,
            frame_id);
}

void FrameDirectory::unregisterResidentPage(int frame_id) {
  auto key =
      std::make_pair(frames_[frame_id].page_id, frames_[frame_id].file_path);
  page_to_frame_.erase(key);

  frames_[frame_id].clear();

  free_frames_.insert(frame_id);

  LOG_DEBUG("Unregistered and deleted page from frame {}", frame_id);
}

void FrameDirectory::pin(int frame_id) {
  frames_[frame_id].pin_count++;
  LOG_DEBUG("Marked frame {} as pinned, count = {}", frame_id,
            frames_[frame_id].pin_count);
}

void FrameDirectory::unpin(int frame_id) {
  if (frames_[frame_id].pin_count > 0) {
    frames_[frame_id].pin_count--;
    LOG_DEBUG("Marked frame {} as unpinned, count = {}", frame_id,
              frames_[frame_id].pin_count);
  }
}

bool FrameDirectory::isPinned(int frame_id) const {
  return frames_[frame_id].pin_count > 0;
}

std::optional<int> FrameDirectory::findVictimFrame() {
  std::vector<int> candidates;
  for (int i = 0; i < MAX_FRAME_COUNT; ++i) {
    if (frames_[i].pin_count == 0 && frames_[i].page != nullptr) {
      candidates.push_back(i);
    }
  }

  if (candidates.empty()) {
    // Dump frame directory state to help debugging why no victim exists
    std::string dump = "FrameDirectory dump: ";
    for (int i = 0; i < MAX_FRAME_COUNT; ++i) {
      dump += fmt::format("[id={} pin={} has_page={}] ", i,
                          frames_[i].pin_count, frames_[i].page != nullptr);
    }
    LOG_WARN("No evictable frames found (all pinned or empty). {}", dump);
    return std::nullopt;
  }

  // Random selection from candidates
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> dis(0, candidates.size() - 1);
  int victim = candidates[dis(gen)];

  LOG_DEBUG("Found victim frame {} (randomly selected from {} candidates)",
            victim, candidates.size());
  return victim;
}

const FrameDirectory::Frame& FrameDirectory::getFrame(int frame_id) const {
  return frames_[frame_id];
}

FrameDirectory::Frame& FrameDirectory::getFrame(int frame_id) {
  return frames_[frame_id];
}