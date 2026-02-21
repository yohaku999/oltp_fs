#pragma once
#include <cstddef>
#include <cstdint>
#include <fstream>
#include <string>
#include <unordered_map>
#include <memory>
#include <spdlog/spdlog.h>
/**
 * The same file can be accessed by multiple buffer pool frames, so we can cache the file stream in memory to avoid opening the same file multiple times.
 * File class should not own memory, it just provides utility functions to read/write pages from/to the buffer pool.
 */
class File
{
private:
    static std::unordered_map<std::string, std::weak_ptr<std::fstream>> stream_cache_;
    std::shared_ptr<std::fstream> stream_;
    uint16_t max_page_id_;
    std::string filePath_;
public:
    static constexpr size_t HEADDER_SIZE_BYTE = 256;
    static constexpr size_t MAX_PAGE_ID_SIZE_BYTE = 2;
    uint16_t allocateNextPageId();
    bool isPageIDUsed(uint16_t page_id) const;
    File(const std::string &filePath, uint16_t max_page_id=0);
    ~File();
    void initializeStreamIfClosed();
    void close();
    void loadPageOnFrame(uint16_t const page_id, char *buffer);    
    void writePageOnFile(uint16_t const page_id, char *buffer);
    std::string getFilePath() const { return filePath_; }
    uint16_t getMaxPageID() const { return max_page_id_; }
};