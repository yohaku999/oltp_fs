#include "file.h"

#include <spdlog/spdlog.h>

#include <cstring>
#include <filesystem>
#include <fstream>
#include <limits>
#include <memory>
#include <stdexcept>
#include <unordered_map>

#include "logging.h"
#include "storage/page/page.h"

std::unordered_map<std::string, std::weak_ptr<File::SharedState>>
    File::state_cache_;

void File::invalidateCache(const std::string& file_path) {
  auto cached = state_cache_.find(file_path);
  if (cached == state_cache_.end()) {
    return;
  }

  if (cached->second.lock()) {
    throw std::logic_error("cannot invalidate file cache while state is still in use: " +
                           file_path);
  }

  state_cache_.erase(cached);
}

bool File::isPageIDUsed(uint16_t page_id) const {
  return page_id <= state_->max_page_id;
}

uint16_t File::allocateNextPageId() {
  if (state_->max_page_id == std::numeric_limits<uint16_t>::max()) {
    throw std::overflow_error("page ID overflow");
  }
  state_->max_page_id += 1;
  state_->header_dirty = true;
  return state_->max_page_id;
}

void File::writeHeader() {
  initializeStreamIfClosed();

  char buffer[File::HEADDER_SIZE_BYTE];
  std::memset(buffer, 0, sizeof(buffer));

  std::memcpy(buffer, &state_->max_page_id, sizeof(uint16_t));
  std::memcpy(buffer + File::MAX_PAGE_ID_SIZE_BYTE, &state_->root_page_id,
              sizeof(uint16_t));

  state_->stream->seekp(0, std::ios::beg);
  if (!*state_->stream) {
    state_->stream->clear();
    throw std::runtime_error("failed to seek file when writing header: " +
                             file_path_);
  }

  state_->stream->write(buffer, sizeof(buffer));
  if (!*state_->stream) {
    state_->stream->clear();
    throw std::runtime_error("failed to write header: " + file_path_);
  }
  LOG_INFO("Wrote header for file {}: max_page_id {}, root_page_id {}",
           file_path_, state_->max_page_id, state_->root_page_id);

  state_->stream->clear();
  state_->header_dirty = false;
}

/**
 * Ensures `stream_` refers to an open stream for `file_path_`.
 */
void File::initializeStreamIfClosed() {
  if (!state_) {
    throw std::runtime_error("file state is not initialized: " + file_path_);
  }

  if (state_->stream && state_->stream->is_open()) {
    return;
  }

  state_->stream.reset();

  auto new_stream = std::make_shared<std::fstream>(
      file_path_, std::ios::in | std::ios::out | std::ios::binary);
  if (!*new_stream) {
    throw std::runtime_error("failed to open file: " + file_path_);
  }

  state_->stream = new_stream;
}

void File::close() {
  if (!state_) {
    return;
  }

  if (state_->header_dirty) {
    try {
      writeHeader();
    } catch (const std::exception& ex) {
      LOG_ERROR("failed to write header for file {} during close: {}",
                file_path_, ex.what());
      // fall through and still attempt to close the stream
    } catch (...) {
      LOG_ERROR(
          "failed to write header for file {} during close: unknown error",
          file_path_);
      // fall through and still attempt to close the stream
    }
  }

  if (state_.use_count() > 1) {
    return;
  }

  if (!state_->stream) {
    state_cache_.erase(file_path_);
    return;
  }

  state_->stream->clear();
  state_->stream->flush();
  state_->stream->close();

  if (state_->stream->fail()) {
    state_->stream->clear();
    throw std::runtime_error("failed to close file: " + file_path_);
  }

  auto cached = state_cache_.find(file_path_);
  if (cached != state_cache_.end()) {
    auto existing = cached->second.lock();
    if (!existing || existing == state_) {
      state_cache_.erase(cached);
    }
  }

  state_->stream.reset();
}

File::~File() {
  try {
    close();
  } catch (const std::exception& ex) {
    LOG_ERROR("failed to close file {} in destructor: {}", file_path_,
              ex.what());
  } catch (...) {
    LOG_ERROR("failed to close file {} in destructor: unknown error",
              file_path_);
  }
}

File::File(const std::string& file_path) : file_path_(file_path) {
  const auto cached = state_cache_.find(file_path_);
  if (cached != state_cache_.end()) {
    auto existing = cached->second.lock();
    if (existing) {
      state_ = std::move(existing);
      return;
    }
    state_cache_.erase(cached);
  }

  state_ = std::make_shared<SharedState>();
  const bool is_new_file = !std::filesystem::exists(file_path_) ||
                           std::filesystem::file_size(file_path_) == 0;
  LOG_INFO("initializing File object for path: {}, is_new_file: {}", file_path_,
           is_new_file);

  if (!is_new_file) {
    initializeStreamIfClosed();

    // update max_page_id_ by reading the file header if the file already
    // exists.
    state_->stream->seekg(0, std::ios::beg);
    std::unique_ptr<char[]> header_buffer =
        std::make_unique<char[]>(File::HEADDER_SIZE_BYTE);
    state_->stream->read(header_buffer.get(), File::HEADDER_SIZE_BYTE);
    state_->max_page_id = readValue<uint16_t>(header_buffer.get());
    state_->root_page_id =
        readValue<uint16_t>(header_buffer.get() + File::MAX_PAGE_ID_SIZE_BYTE);
    LOG_INFO(
        "opened existing file: {}, max_page_id loaded from header: {}, "
        "root_page_id loaded from header: {}",
      file_path_, state_->max_page_id, state_->root_page_id);
  } else {
    std::ofstream creator(file_path_, std::ios::binary | std::ios::trunc);
    if (!creator) {
      throw std::runtime_error("failed to create file: " + file_path_);
    }
    LOG_INFO("created new file: {}", file_path_);
    creator.close();

    initializeStreamIfClosed();
    // For a new file, header has not been written yet.
    state_->max_page_id = 0;
    state_->root_page_id = 0;
    state_->header_dirty = true;
  }

  state_cache_[file_path_] = state_;
}

// this method should be called only from buffer pool in prod.
void File::writePageFromBuffer(uint16_t const page_id, char* buffer) {
  initializeStreamIfClosed();

  const std::streamoff offset =
      static_cast<std::streamoff>(File::HEADDER_SIZE_BYTE) +
      static_cast<std::streamoff>(page_id) * Page::PAGE_SIZE_BYTE;
  state_->stream->seekp(offset, std::ios::beg);
  if (!state_->stream) {
    state_->stream->clear();
    throw std::runtime_error("failed to seek file: " + file_path_);
  }

  state_->stream->write(buffer, Page::PAGE_SIZE_BYTE);
  if (!state_->stream) {
    state_->stream->clear();
    throw std::runtime_error("failed to write page: " + file_path_);
  }

  state_->stream->clear();
}

void File::readPageIntoBuffer(uint16_t const page_id, char* buffer) {
  initializeStreamIfClosed();

  const std::streamoff offset =
      static_cast<std::streamoff>(File::HEADDER_SIZE_BYTE) +
      static_cast<std::streamoff>(page_id) * Page::PAGE_SIZE_BYTE;
  state_->stream->seekg(offset, std::ios::beg);
  if (!state_->stream) {
    state_->stream->clear();
    throw std::runtime_error("failed to seek file: " + file_path_);
  }

  state_->stream->read(buffer, Page::PAGE_SIZE_BYTE);
  if (!state_->stream) {
    state_->stream->clear();
    throw std::runtime_error("failed to read page: " + file_path_);
  }

  state_->stream->clear();
}