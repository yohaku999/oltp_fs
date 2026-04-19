#pragma once
#include <spdlog/spdlog.h>

#include <cstddef>
#include <cstdint>
#include <fstream>
#include <memory>
#include <string>
#include <unordered_map>
/**
 * The same file can be accessed by multiple buffer pool frames, so we can cache
 * the file stream in memory to avoid opening the same file multiple times. File
 * class should not own memory, it just provides utility functions to read/write
 * pages from/to the buffer pool.
 */
class File {
 private:
  struct SharedState {
    std::shared_ptr<std::fstream> stream;
    uint16_t max_page_id = 0;
    uint16_t root_page_id = 0;
    bool header_dirty = false;
  };

  static std::unordered_map<std::string, std::weak_ptr<SharedState>>
      state_cache_;

  std::shared_ptr<SharedState> state_;
  std::string file_path_;
  void writeHeader();

 public:
  static constexpr size_t HEADDER_SIZE_BYTE = 256;
  static constexpr size_t MAX_PAGE_ID_SIZE_BYTE = 2;
  static void invalidateCache(const std::string& file_path);
  uint16_t allocateNextPageId();
  bool isPageIDUsed(uint16_t page_id) const;
  File(const std::string& file_path);
  ~File();
  void initializeStreamIfClosed();
  void close();
  void readPageIntoBuffer(uint16_t const page_id, char* buffer);
  void writePageFromBuffer(uint16_t const page_id, char* buffer);
  std::string getFilePath() const { return file_path_; }
  uint16_t getMaxPageID() const { return state_->max_page_id; }
  uint16_t getRootPageID() const { return state_->root_page_id; }
  void setRootPageID(uint16_t root_page_id) {
    state_->root_page_id = root_page_id;
    state_->header_dirty = true;
  };
};