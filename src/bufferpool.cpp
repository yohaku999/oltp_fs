#include "bufferpool.h"
#include "page.h"
#include "file.h"
#include <spdlog/spdlog.h>
#include "logging.h"
#include <string>
#include <fstream>

// public methods
// TODO: get rid of filePath parameter since the file path can be determined by the file name and a fixed directory path.
BufferPool::BufferPool()
    : buffer(operator new(BUFFER_SIZE_BYTE))
{
};

u_int16_t BufferPool::createNewPage(bool is_leaf, File &file, uint16_t rightMostChildPageId)
{
    u_int16_t pageID = file.allocateNextPageId();
    int frame_id = obtainFreeFrame();
    char *frame_p = static_cast<char *>(buffer) + frame_id * BufferPool::FRAME_SIZE_BYTE;

    auto page = std::make_unique<Page>(frame_p, is_leaf, rightMostChildPageId, pageID);
    Page* page_ptr = page.get();
    frameDirectory_.registerPage(frame_id, pageID, file.getFilePath(), std::move(page));
    LOG_INFO("Created new page ID {} as {} page in frame ID {}", 
                 pageID, is_leaf ? "leaf" : "intermediate", frame_id);
    return pageID;
}

int BufferPool::obtainFreeFrame(){
    auto free_frame = frameDirectory_.claimFreeFrame();
    if(!free_frame.has_value()){
        LOG_DEBUG("No free frame available, attempting eviction");
        evictPage();
        free_frame = frameDirectory_.claimFreeFrame();
        if (!free_frame.has_value()) {
            LOG_WARN("Eviction completed but no free frame reclaimed");
        } else {
            LOG_DEBUG("Eviction reclaimed free frame {}", free_frame.value());
        }
    }
    int frame_id = free_frame.value();
    zeroOutFrame(frame_id);
    return frame_id;
}

Page *BufferPool::getPage(int pageID, File &file)
{
    LOG_INFO("Requesting page ID {} from file {}", pageID, file.getFilePath());
    auto it = frameDirectory_.findFrameByPage(pageID, file.getFilePath());
    if (it.has_value()){
        int frame_id = it.value();
        frameDirectory_.pin(frame_id);
        return frameDirectory_.getFrame(frame_id).page.get();
    }else{
        int frame_id = obtainFreeFrame();
        char *frame_p = static_cast<char *>(buffer) + frame_id * BufferPool::FRAME_SIZE_BYTE;

        // load page on frame
        if (!file.isPageIDUsed(pageID))
        {   
            throw std::logic_error(fmt::format("Should not expected to call getPage on uninitialized page ID {} in file {}", pageID, file.getFilePath()));
        }
        file.loadPageOnFrame(pageID, frame_p);
        auto page = std::make_unique<Page>(frame_p, pageID);
        Page* page_ptr = page.get();
        frameDirectory_.registerPage(frame_id, pageID, file.getFilePath(), std::move(page));
        frameDirectory_.pin(frame_id);
        LOG_INFO("Loaded page ID {} into frame ID {}", pageID, frame_id);
        return page_ptr;
    }
};

void BufferPool::unpin(Page* page, File& file)
{
    if (!page) {
        throw std::invalid_argument("BufferPool::unpin called with null page");
    }
    auto it = frameDirectory_.findFrameByPage(page->getPageID(), file.getFilePath());
    if (!it.has_value()) {
        throw std::logic_error(fmt::format("BufferPool::unpin: page ID {} in file {} is not registered in FrameDirectory", page->getPageID(), file.getFilePath()));
    }
    frameDirectory_.unpin(it.value());
}

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
    }
    frameDirectory_.unregisterPage(victim_frame_id);
    LOG_INFO("Evicted page ID {} from frame ID {}", evicted_page_id, victim_frame_id);
};


// private methods
void BufferPool::zeroOutFrame(int frameID)
{
    LOG_DEBUG("Zeroing out frame ID: {}", frameID);
    char *frame_p = static_cast<char *>(buffer) + frameID * BufferPool::FRAME_SIZE_BYTE;
    std::memset(frame_p, 0, BufferPool::FRAME_SIZE_BYTE);
};

BufferPool::~BufferPool()
{
    operator delete(buffer);
}