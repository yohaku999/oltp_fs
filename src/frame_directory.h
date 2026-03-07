#pragma once
#include "page.h"
#include <array>
#include <unordered_set>
#include <map>
#include <string>
#include <optional>
#include <cstdint>
#include <memory>

class FrameDirectory {
public:
    static constexpr size_t MAX_FRAME_COUNT = 10;
    
private:
    struct Frame {
        std::unique_ptr<Page> page = nullptr;
        int page_id = -1;
        std::string file_path;
        int pin_count = 0;
        
        void clear() { *this = Frame{}; }
    };
    std::array<Frame, MAX_FRAME_COUNT> frames_;
    std::map<std::pair<int, std::string>, int> page_to_frame_;
    // optimization to find free frames in O(1) and avoid sequential search.
    std::unordered_set<int> free_frames_;
public:
    FrameDirectory();
        
    std::optional<int> claimFreeFrame();
    std::optional<int> findFrameByPage(int pageID, const std::string& filePath);
    
    void registerPage(int frameID, int pageID, const std::string& filePath, std::unique_ptr<Page> page);
    void unregisterPage(int frameID);
    /**
     * We use explicit pin/unpin.
     * I thought shared_ptr can be an option but, its lifetimes can accidentally grow (copies/captures/queues) and block eviction.
     * Intuitively, for a buffer pool, eviction depends on "safe to reuse this frame now?" and not "someone still holds a reference to this frame".
     */
    void pin(int frameID);
    void unpin(int frameID);
    bool isPinned(int frameID) const;
    
    std::optional<int> findVictimFrame();
    
    const Frame& getFrame(int frameID) const;
    Frame& getFrame(int frameID);
};
