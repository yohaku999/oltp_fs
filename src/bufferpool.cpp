#include "bufferpool.h"
#include "page.h"
#include "file.h"
#include <spdlog/spdlog.h>
#include <string>
#include <fstream>

// public methods
// TODO: get rid of filePath parameter since the file path can be determined by the file name and a fixed directory path.
BufferPool::BufferPool()
    : buffer(operator new(BUFFER_SIZE_BYTE))
{
};

Page *BufferPool::getPage(int pageID, File &file)
{
    spdlog::info("Requesting page ID {} from file {}", pageID, file.getFilePath());
    auto key = std::make_pair(pageID, file.getFilePath());
    auto it = frameDirectory_.findFrameByPage(pageID, file.getFilePath());
    if (it.has_value()){
        int frame_id = it.value();
        return frameDirectory_.getFrame(frame_id).page;
    }else{
        // prepare frame
        auto free_frame = frameDirectory_.claimFreeFrame();
        if(!free_frame.has_value()){
            spdlog::warn("No free frames available for page ID {} from file {}", pageID, file.getFilePath());
            evictPage();
            free_frame = frameDirectory_.claimFreeFrame();
        }
        int frame_id = free_frame.value();
        zeroOutFrame(frame_id);
        char *frame_p = static_cast<char *>(buffer) + frame_id * BufferPool::FRAME_SIZE_BYTE;

        // load page on frame
        Page *page;
        if (file.isPageIDUsed(pageID))
        {
            file.loadPageOnFrame(pageID, frame_p);
            page = Page::wrap(frame_p);
        }
        else
        {
            // TODO: determine whether the new page is a leaf node or an intermediate node. all true for now.
            page = Page::initializePage(frame_p, true, file.allocateNextPageId());
        }
        frameDirectory_.registerPage(frame_id, pageID, file.getFilePath(), page);
        spdlog::info("Loaded page ID {} into frame ID {}", pageID, frame_id);
        return page;
    }
};

void BufferPool::evictPage()
{
    // decide which unused page to evict.
    auto victim_opt = frameDirectory_.findVictimFrame();
    if (!victim_opt.has_value())
    {
        // TODO: we should sleep or kill queries.
        throw std::runtime_error("No victim frame found for eviction. All frames are pinned.");
    }
    // write the page back to the file if it's dirty.
    int victim_frame_id = victim_opt.value();
    auto &victim_frame = frameDirectory_.getFrame(victim_frame_id);
    // REFACTOR: Have to cache these values before unregistering the page since the frame will be cleared in unregisterPage().
    int evicted_page_id = victim_frame.page_id;
    std::string evicted_file_path = victim_frame.file_path;
    if (victim_frame.page->isDirty()){
        // this clearDirty() call is not necessary, since it will be cleared when loading the page again with Page constructor.
        // However, it can help to avoid confusion and potential bugs in the future.
        victim_frame.page->clearDirty();
        File file(victim_frame.file_path);
        file.writePageOnFile(victim_frame.page_id, victim_frame.page->start_p_);
        spdlog::info("Evicted dirty page ID {} from file {} in frame ID {}", victim_frame.page_id, victim_frame.file_path, victim_frame_id);
    }
    frameDirectory_.unregisterPage(victim_frame_id);
    spdlog::info("Evicted page from frame ID {}, page ID {}", victim_frame_id, evicted_page_id);
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