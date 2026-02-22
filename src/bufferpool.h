#pragma once
#include <string>
#include <set>
#include <map>
#include <utility>
#include "page.h"
#include "file.h"
#include "frame_directory.h"

class BufferPool
{
private:
    static constexpr size_t BUFFER_SIZE_BYTE = 4096 * 10;
    static constexpr size_t FRAME_SIZE_BYTE = 4096;
    static constexpr size_t MAX_PAGE_COUNT = 10;
    std::string fileName;
    void *buffer;
    void evictPage();
    void loadPage(int pageID);
    void zeroOutFrame(int frameID);
    std::unique_ptr<FrameDirectory> frameDirectory_;
public:
    static constexpr size_t MAX_FRAME_COUNT = 10;
    BufferPool(std::unique_ptr<FrameDirectory> frameDirectory);
    Page *getPage(int pageID, File &file);
    ~BufferPool();
};