#include "bufferpool.h"
#include "page.h"
#include "file.h"
#include <spdlog/spdlog.h>
#include <string>
#include <fstream>

// public methods
// TODO: get rid of filePath parameter since the file path can be determined by the file name and a fixed directory path.
BufferPool::BufferPool()
{
    buffer = operator new(BUFFER_SIZE_BYTE);
};

Page *BufferPool::getPage(int pageID, File &file)
{
    spdlog::info("Requesting page ID {} from file {}", pageID, file.getFilePath());
    auto key = std::make_pair(pageID, file.getFilePath());
    auto it = loadedPageIDs.find(key);
    if (it != loadedPageIDs.end())
    {
        // page is already loaded in the buffer pool, return it directly.
        return it->second;
    }
    else
    {
        for (int i = 0; i < BufferPool::MAX_FRAME_COUNT; ++i)
        {
            // search for a free frame
            if (usedFrameIDs.find(i) == usedFrameIDs.end())
            {
                zeroOutFrame(i);
                char *frame_p = static_cast<char *>(buffer) + i * BufferPool::FRAME_SIZE_BYTE;
                if (!file.isPageIDUsed(pageID))
                {
                    file.loadPageOnFrame(pageID, frame_p);
                }else{
                    // Initialize a new page
                     // TODO: determine whether the new page is a leaf node or an intermediate node.
                    Page *new_page = Page::initializePage(frame_p, true, file.allocateNextPageId());
                }
                Page *page = Page::wrap(frame_p);
                loadedPageIDs[key] = page;
                usedFrameIDs.insert(i);
                spdlog::info("Loaded page ID {} into frame ID {}", pageID, i);
                return page;
            }
        }
        throw std::runtime_error("no free frame available");
    };
};

void BufferPool::evictPage(int frameID)
{
    char *frame_p = static_cast<char *>(buffer) + frameID * BufferPool::FRAME_SIZE_BYTE;
    // buffered i/o, random access
    std::fstream ofs(fileName, std::ios::in | std::ios::out | std::ios::binary);
    // create file if not exist
    if (!ofs)
    {
        ofs.open(fileName, std::ios::out | std::ios::binary);
        ofs.close();
        ofs.open(fileName, std::ios::in | std::ios::out | std::ios::binary);
    }
    ofs.seekp(frameID * BufferPool::FRAME_SIZE_BYTE, std::ios::beg);
    ofs.write(frame_p, BufferPool::FRAME_SIZE_BYTE);
};

// private methods
void BufferPool::zeroOutFrame(int frameID)
{
    spdlog::debug("Zeroing out frame ID: {}", frameID);
    char *frame_p = static_cast<char *>(buffer) + frameID * BufferPool::FRAME_SIZE_BYTE;
    std::memset(frame_p, 0, BufferPool::FRAME_SIZE_BYTE);
};

BufferPool::~BufferPool()
{
    operator delete(buffer);
}