#pragma once
#include <array>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <unordered_set>

#include "page.h"

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

  std::optional<int> reserveFreeFrame();
  std::optional<int> findResidentFrame(int page_id,
                                       const std::string& file_path);

  void registerResidentPage(int frame_id, int page_id,
                            const std::string& file_path,
                            std::unique_ptr<Page> page);
  void unregisterResidentPage(int frame_id);
  /**
   * We use explicit pin/unpin.
   * Although Intuitively, for a buffer pool, eviction depends on "safe to reuse
   * this frame now?" and not "someone still holds a reference to this frame"
   * and it is assumed that we need more control then shared_ptr provides
   * initially.
   */
  void pin(int frame_id);
  void unpin(int frame_id);
  bool isPinned(int frame_id) const;

  std::optional<int> findVictimFrame();

  const Frame& getFrame(int frame_id) const;
  Frame& getFrame(int frame_id);
};
