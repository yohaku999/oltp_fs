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
	return max_page_id_;
}

void File::initializeStreamIfClosed()
{
    /**
     * Maintain a per-instance stream_ (RAII owner) while caching weak references in stream_cache_.
     * The strong owner guarantees proper close(); closing is deferred until the last owner releases stream_.
     */
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
    if (stream_.use_count() > 1)
    {
        stream_.reset();
        return;
    }

    stream_->clear();
    stream_->flush();
    stream_->fsync();
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
        spdlog::error("failed to close file {} in destructor: {}", filePath_, ex.what());
    }
    catch (...)
    {
        spdlog::error("failed to close file {} in destructor: unknown error", filePath_);
    }
}

File::File(const std::string &filePath, uint16_t max_page_id) : filePath_(filePath), max_page_id_(max_page_id)
{
    const bool is_new_file = !std::filesystem::exists(filePath_) ||
                             std::filesystem::file_size(filePath_) == 0;
    spdlog::info("initializing File object for path: {}, is_new_file: {}, provided max_page_id: {}", filePath_, is_new_file, max_page_id);

    if (!is_new_file)
    {
        initializeStreamIfClosed();

        // update max_page_id_ by reading the file header if the file already exists.
        stream_->seekg(0, std::ios::beg);
        std::unique_ptr<char[]> headder = std::make_unique<char[]>(File::HEADDER_SIZE_BYTE);
        stream_->read(headder.get(), File::HEADDER_SIZE_BYTE);
        max_page_id_ = readValue<uint16_t>(headder.get());
        spdlog::info("opened existing file: {}, max_page_id loaded from header: {}", filePath_, max_page_id_);
    }
    else
    {
        std::ofstream creator(filePath_, std::ios::binary | std::ios::trunc);
        if (!creator)
        {
            throw std::runtime_error("failed to create file: " + filePath_);
        }
        spdlog::info("created new file: {}", filePath_);
        creator.close();

        initializeStreamIfClosed();
    }
}

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