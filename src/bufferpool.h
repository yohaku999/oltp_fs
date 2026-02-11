#pragma once
#include <string>
#include <set>
#include <map>
#include "page.h"

class BufferPool
{
    friend class BufferPoolTest;

private:
    static constexpr size_t BUFFER_SIZE_BYTE = 4096 * 10;
    static constexpr size_t FRAME_SIZE_BYTE = 4096;
    static constexpr size_t MAX_FRAME_COUNT = 10;
    static constexpr size_t MAX_PAGE_COUNT = 10;
    std::string fileName;
    void *buffer;
    void evictPage(int frameID);
    void loadPage(int pageID);
    std::map<int, Page *> frameIDToPage;
    std::map<int, Page *> loadedPageIDs;
    void zeroOutFrame(int frameID);

public:
    BufferPool(const std::string &fileName);
    Page *getPage(int pageID);
    ~BufferPool();
};