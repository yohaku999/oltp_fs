#include "bufferpool.h"
#include "page.h"
#include <spdlog/spdlog.h>
#include <string>
#include <fstream>

// public methods
// TODO: get rid of filePath parameter since the file path can be determined by the file name and a fixed directory path.
BufferPool::BufferPool()
{
    buffer = operator new(BUFFER_SIZE_BYTE);
};

Page *BufferPool::getPage(int pageID, std::string filePath)
{
    spdlog::info("Requesting page ID {} from file {}", pageID, filePath);
    auto key = std::make_pair(pageID, filePath);
    auto it = loadedPageIDs.find(key);
    if (it != loadedPageIDs.end())
    {
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
                char *start_p = static_cast<char *>(buffer) + i * BufferPool::FRAME_SIZE_BYTE;
                bool is_new_page = false; // TODO: consider proper way.
                if (!is_new_page)
                {

                    // Load the data of the corresponding page from the file into the new frame
                    std::fstream ifs(filePath, std::ios::in | std::ios::binary);
                    if (!ifs)
                    {
                        throw std::runtime_error("error occured while trying to open file: " + filePath);
                    }
                    ifs.seekg(pageID * Page::PAGE_SIZE_BYTE, std::ios::beg);
                    ifs.read(start_p, Page::PAGE_SIZE_BYTE);
                }
                Page *page = new Page(start_p);
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
    char *start_p = static_cast<char *>(buffer) + frameID * BufferPool::FRAME_SIZE_BYTE;
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
    ofs.write(start_p, BufferPool::FRAME_SIZE_BYTE);
};

// private methods
void BufferPool::zeroOutFrame(int frameID)
{
    spdlog::debug("Zeroing out frame ID: {}", frameID);
    char *start_p = static_cast<char *>(buffer) + frameID * BufferPool::FRAME_SIZE_BYTE;
    std::memset(start_p, 0, BufferPool::FRAME_SIZE_BYTE);
};

BufferPool::~BufferPool()
{
    operator delete(buffer);
}