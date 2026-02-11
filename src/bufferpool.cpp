#include "bufferpool.h"
#include "page.h"
#include <spdlog/spdlog.h>
#include <string>
#include <fstream>

// public methods
BufferPool::BufferPool(const std::string &fileName) : fileName(fileName)
{
    buffer = operator new(BUFFER_SIZE_BYTE);
    std::ifstream ifs(fileName, std::ios::binary);
    if (!ifs)
    {
        spdlog::info("File not found, creating new file: {}", fileName);
        std::ofstream ofs(fileName, std::ios::binary);
        ofs.close();
    }
};

Page *BufferPool::getPage(int pageID)
{
    spdlog::info("Requesting page with ID: {}", pageID);
    auto it = loadedPageIDs.find(pageID);
    if (it != loadedPageIDs.end())
    {
        return it->second;
    }
    else
    {
        for (int i = 0; i < BufferPool::MAX_FRAME_COUNT; ++i)
        {
            auto it = frameIDToPage.find(i);
            if (it == frameIDToPage.end())
            {
                zeroOutFrame(i);
                char *start_p = static_cast<char *>(buffer) + i * BufferPool::FRAME_SIZE_BYTE;
                bool is_new_page = false; // TODO: consider proper way.
                if (!is_new_page)
                {

                    // Load the data of the corresponding page from the file into the new frame
                    std::fstream ifs(fileName, std::ios::in | std::ios::binary);
                    if (!ifs)
                    {
                        throw std::runtime_error("error occured while trying to open file: " + fileName);
                    }
                    ifs.seekg(pageID * Page::PAGE_SIZE_BYTE, std::ios::beg);
                    ifs.read(start_p, Page::PAGE_SIZE_BYTE);
                }
                Page *page = new Page(start_p);
                loadedPageIDs[pageID] = page;
                frameIDToPage[i] = page;
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