#include "frame_directory.h"
#include <spdlog/spdlog.h>

FrameDirectory::FrameDirectory()
{
    for (int i = 0; i < MAX_FRAME_COUNT; ++i) {
        free_frames_.insert(i);
    }
}

std::optional<int> FrameDirectory::findFreeFrame()
{
    if (free_frames_.empty()) {
        return std::nullopt;
    }
    
    int frame_id = *free_frames_.begin();
    free_frames_.erase(free_frames_.begin());
    spdlog::debug("Found free frame {}", frame_id);
    return frame_id;
}

std::optional<int> FrameDirectory::findFrameByPage(int pageID, const std::string& filePath)
{
    auto key = std::make_pair(pageID, filePath);
    auto it = page_to_frame_.find(key);
    if (it != page_to_frame_.end()) {
        return it->second;
    }
    return std::nullopt;
}

void FrameDirectory::registerPage(int frameID, int pageID, const std::string& filePath, Page* page)
{
    frames_[frameID].page = page;
    frames_[frameID].page_id = pageID;
    frames_[frameID].file_path = filePath;
    frames_[frameID].pin_count = 0;
    
    auto key = std::make_pair(pageID, filePath);
    page_to_frame_[key] = frameID;
    
    spdlog::debug("Registered page {} from {} in frame {}", pageID, filePath, frameID);
}

void FrameDirectory::unregisterPage(int frameID)
{
    auto key = std::make_pair(frames_[frameID].page_id, frames_[frameID].file_path);
    page_to_frame_.erase(key);
    
    frames_[frameID].clear();
    
    free_frames_.insert(frameID);
    
    spdlog::debug("Unregistered page from frame {}", frameID);
}

void FrameDirectory::pin(int frameID)
{
    frames_[frameID].pin_count++;
    spdlog::debug("Marked frame {} as pinned, count = {}", frameID, frames_[frameID].pin_count);
}

void FrameDirectory::unpin(int frameID)
{
    if (frames_[frameID].pin_count > 0) {
        frames_[frameID].pin_count--;
        spdlog::debug("Marked frame {} as unpinned, count = {}", frameID, frames_[frameID].pin_count);
    }
}

bool FrameDirectory::isPinned(int frameID) const
{
    return frames_[frameID].pin_count > 0;
}

std::optional<int> FrameDirectory::findVictimFrame()
{
    for (int i = 0; i < MAX_FRAME_COUNT; ++i) {
        if (frames_[i].isOccupied() && frames_[i].pin_count == 0) {
            spdlog::debug("Found victim frame {}", i);
            return i;
        }
    }
    
    spdlog::warn("No evictable frames found (all pinned or empty)");
    return std::nullopt;
}

const FrameDirectory::Frame& FrameDirectory::getFrame(int frameID) const
{
    return frames_[frameID];
}

FrameDirectory::Frame& FrameDirectory::getFrame(int frameID)
{
    return frames_[frameID];
}