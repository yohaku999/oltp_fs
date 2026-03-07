#include "file.h"
#include "page.h"
#include <cstring>
#include <filesystem>
#include <fstream>
#include <limits>
#include <memory>
#include <stdexcept>
#include <unordered_map>
#include <spdlog/spdlog.h>
#include "logging.h"

std::unordered_map<std::string, std::weak_ptr<std::fstream>> File::stream_cache_;

bool File::isPageIDUsed(uint16_t page_id) const
{
    return page_id <= max_page_id_;
}

uint16_t File::allocateNextPageId()
{
	if (max_page_id_ == std::numeric_limits<uint16_t>::max())
	{
		throw std::overflow_error("page ID overflow");
	}
	max_page_id_ += 1;
	header_dirty_ = true;
	return max_page_id_;
}

void File::writeHeader()
{
    initializeStreamIfClosed();

    char buffer[File::HEADDER_SIZE_BYTE];
    std::memset(buffer, 0, sizeof(buffer));

    std::memcpy(buffer, &max_page_id_, sizeof(uint16_t));
    std::memcpy(buffer + File::MAX_PAGE_ID_SIZE_BYTE, &root_page_id_, sizeof(uint16_t));

    stream_->seekp(0, std::ios::beg);
    if (!*stream_)
    {
        stream_->clear();
        throw std::runtime_error("failed to seek file when writing header: " + filePath_);
    }

    stream_->write(buffer, sizeof(buffer));
    if (!*stream_)
    {
        stream_->clear();
        throw std::runtime_error("failed to write header: " + filePath_);
    }

    stream_->clear();
    header_dirty_ = false;
}

/**
 * Ensure stream_ holds an open fstream for this file path,
 * reusing a cached shared instance or creating a new one if needed.
 * 
 * This way we keep at most one OS-level stream per file path while still
 * allowing multiple File objects to share it safely via shared_ptr/weak_ptr.
 * The last owning File closes the underlying fstream when its shared_ptr is released.
 */
void File::initializeStreamIfClosed()
{
    if (stream_ && stream_->is_open())
    {
        return;
    }

    stream_.reset();

    const auto cached = stream_cache_.find(filePath_);
    if (cached != stream_cache_.end())
    {
        auto existing = cached->second.lock();
        if (existing && existing->is_open())
        {
            stream_ = existing;
            return;
        }
        stream_cache_.erase(cached);
    }

    auto new_stream = std::make_shared<std::fstream>(filePath_, std::ios::in | std::ios::out | std::ios::binary);
    if (!*new_stream)
    {
        throw std::runtime_error("failed to open file: " + filePath_);
    }

    stream_ = new_stream;
    stream_cache_[filePath_] = stream_;
}

void File::close()
{
    if (header_dirty_)
    {
        try
        {
            writeHeader();
        }
        catch (const std::exception &ex)
        {
            LOG_ERROR("failed to write header for file {} during close: {}", filePath_, ex.what());
            // fall through and still attempt to close the stream
        }
        catch (...)
        {
            LOG_ERROR("failed to write header for file {} during close: unknown error", filePath_);
            // fall through and still attempt to close the stream
        }
    }

    // just reset the shared_ptr if there are other owners
    if (stream_.use_count() > 1)
    {
        stream_.reset();
        return;
    }

    // Close the stream and remove from cache otherwise.
    stream_->clear();
    stream_->flush();
    stream_->close();

    if (stream_->fail())
    {
        stream_->clear();
        throw std::runtime_error("failed to close file: " + filePath_);
    }

    auto cached = stream_cache_.find(filePath_);
    if (cached != stream_cache_.end())
    {
        auto existing = cached->second.lock();
        if (!existing || existing == stream_)
        {
            stream_cache_.erase(cached);
        }
    }

    stream_.reset();
}

File::~File()
{
    try
    {
        close();
    }
    catch (const std::exception &ex)
    {
        LOG_ERROR("failed to close file {} in destructor: {}", filePath_, ex.what());
    }
    catch (...)
    {
        LOG_ERROR("failed to close file {} in destructor: unknown error", filePath_);
    }
}

File::File(const std::string &filePath, uint16_t max_page_id) : filePath_(filePath), max_page_id_(max_page_id)
{
    const bool is_new_file = !std::filesystem::exists(filePath_) ||
                             std::filesystem::file_size(filePath_) == 0;
    LOG_INFO("initializing File object for path: {}, is_new_file: {}, provided max_page_id: {}", filePath_, is_new_file, max_page_id);

    if (!is_new_file)
    {
        initializeStreamIfClosed();

        // update max_page_id_ by reading the file header if the file already exists.
        stream_->seekg(0, std::ios::beg);
        std::unique_ptr<char[]> headder = std::make_unique<char[]>(File::HEADDER_SIZE_BYTE);
        stream_->read(headder.get(), File::HEADDER_SIZE_BYTE);
        max_page_id_ = readValue<uint16_t>(headder.get());
        root_page_id_ = readValue<uint16_t>(headder.get() + File::MAX_PAGE_ID_SIZE_BYTE);
        LOG_INFO("opened existing file: {}, max_page_id loaded from header: {}, root_page_id loaded from header: {}", filePath_, max_page_id_, root_page_id_);
    }
    else
    {
        std::ofstream creator(filePath_, std::ios::binary | std::ios::trunc);
        if (!creator)
        {
            throw std::runtime_error("failed to create file: " + filePath_);
        }
        LOG_INFO("created new file: {}", filePath_);
        creator.close();

        initializeStreamIfClosed();
        // For a new file, header has not been written yet.
        max_page_id_ = 0;
        root_page_id_ = 0;
        header_dirty_ = true;
    }
}

// this method should be called only from buffer pool in prod.
void File::writePageOnFile(uint16_t const page_id, char *buffer)
{
    initializeStreamIfClosed();

    const std::streamoff offset = static_cast<std::streamoff>(File::HEADDER_SIZE_BYTE) +
                                  static_cast<std::streamoff>(page_id) * Page::PAGE_SIZE_BYTE;
    stream_->seekp(offset, std::ios::beg);
    if (!stream_)
    {
        stream_->clear();
        throw std::runtime_error("failed to seek file: " + filePath_);
    }

    stream_->write(buffer, Page::PAGE_SIZE_BYTE);
    if (!stream_)
    {
        stream_->clear();
        throw std::runtime_error("failed to write page: " + filePath_);
    }

    stream_->clear();
}

void File::loadPageOnFrame(uint16_t const page_id, char *buffer)
{
	initializeStreamIfClosed();

	const std::streamoff offset = static_cast<std::streamoff>(File::HEADDER_SIZE_BYTE) +
								  static_cast<std::streamoff>(page_id) * Page::PAGE_SIZE_BYTE;
    stream_->seekg(offset, std::ios::beg);
    if (!stream_)
	{
        stream_->clear();
		throw std::runtime_error("failed to seek file: " + filePath_);
	}

    stream_->read(buffer, Page::PAGE_SIZE_BYTE);
    if (!stream_)
	{
        stream_->clear();
		throw std::runtime_error("failed to read page: " + filePath_);
	}

    stream_->clear();
}