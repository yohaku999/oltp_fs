#pragma once
#include <string>
#include <set>
#include <map>
#include <utility>
#include "page.h"
#include "file.h"

class BufferPool
{
    friend class BufferPoolTest;

private:
    static constexpr size_t BUFFER_SIZE_BYTE = 4096 * 10;
    static constexpr size_t FRAME_SIZE_BYTE = 4096;
    static constexpr size_t MAX_PAGE_COUNT = 10;
    std::string fileName;
    void *buffer;
    void evictPage(int frameID);
    void loadPage(int pageID);
    std::set<int> usedFrameIDs;
    std::map<std::pair<int, std::string>, Page *> loadedPageIDs;
    void zeroOutFrame(int frameID);
public:
    static constexpr size_t MAX_FRAME_COUNT = 10;
    BufferPool();
    Page *getPage(int pageID, File &file);
    ~BufferPool();
};