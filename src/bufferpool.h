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
    // Design Intent:
    // BufferPool and FrameDirectory are tightly coupled (1:1, same lifetime).
    // FrameDirectory is held by value (not pointer) because:
    //   - No polymorphism needed for FrameDirectory itself
    //   - They are inseparable (SRP: separate responsibilities, but coupled lifecycle)
    // Future: Eviction strategies (FIFO/LRU/Clock) will be injected into FrameDirectory via Strategy pattern
    FrameDirectory frameDirectory_;
    int obtainFreeFrame();
public:
    static constexpr size_t MAX_FRAME_COUNT = 10;
    BufferPool();
    Page *getPage(int pageID, File &file);
    u_int16_t createPage(bool is_leaf, File &file);
    ~BufferPool();
};