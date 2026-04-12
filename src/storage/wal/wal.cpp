#include "wal.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <fstream>
#include <system_error>

namespace {

std::pair<std::uint64_t, std::uint64_t> readWalBootstrapState(
    const std::string& wal_path) {
  std::ifstream wal_file(wal_path, std::ios::binary | std::ios::ate);
  if (!wal_file) {
    throw std::system_error(errno ? errno : ENOENT, std::generic_category(),
                            "Failed to open existing WAL file");
  }

  const auto file_size = wal_file.tellg();
  if (file_size < 0) {
    throw std::runtime_error("Failed to determine WAL size during bootstrap");
  }

  const auto next_lsn = static_cast<std::uint64_t>(file_size);
  if (next_lsn == 0) {
    return {0, 0};
  }

  wal_file.seekg(0, std::ios::beg);

  std::uint64_t offset = 0;
  std::uint64_t last_record_lsn = 0;

  while (offset < next_lsn) {
    if (next_lsn - offset < WALRecord::header_size_bytes()) {
      throw std::runtime_error("Incomplete WAL header during bootstrap");
    }

    wal_file.read(reinterpret_cast<char*>(&last_record_lsn),
                  sizeof(last_record_lsn));
    uint32_t body_size = 0;
    // We already consumed the leading LSN field, so skip only the remaining
    // header bytes between it and body_size (record type + page id).
    wal_file.seekg(WALRecord::body_size_offset_bytes() - sizeof(uint64_t),
                   std::ios::cur);
    wal_file.read(reinterpret_cast<char*>(&body_size), sizeof(body_size));

    offset += WALRecord::header_size_bytes();
    if (next_lsn - offset < body_size) {
      throw std::runtime_error("Incomplete WAL record body during bootstrap");
    }

    wal_file.seekg(body_size, std::ios::cur);
    offset += body_size;
  }

  return {next_lsn, last_record_lsn};
}

}  // namespace

std::unique_ptr<WAL> WAL::initializeNew(const std::string& wal_path) {
  return std::unique_ptr<WAL>(new WAL(wal_path, 0, 0,
                                      O_CREAT | O_EXCL | O_WRONLY));
}

std::unique_ptr<WAL> WAL::openExisting(const std::string& wal_path) {
  const auto [next_lsn, durable_lsn] = readWalBootstrapState(wal_path);
  return std::unique_ptr<WAL>(new WAL(wal_path, next_lsn, durable_lsn,
                                      O_WRONLY | O_APPEND));
}

WAL::WAL(const std::string& wal_path, std::uint64_t next_lsn,
         std::uint64_t durable_lsn, int open_flags)
    : wal_fd_(-1),
      allocator_(next_lsn),
      last_lsn_written_(durable_lsn),
      flushed_lsn_(durable_lsn) {
  int fd = ::open(wal_path.c_str(), open_flags, 0644);
  if (fd == -1) {
    throw std::system_error(errno, std::generic_category(),
                            "Failed to open WAL file");
  }
  wal_fd_ = fd;
}

void WAL::write(WALRecord::RecordType type, uint16_t page_id,
                const std::vector<std::byte>& body) {
  const auto size = WALRecord::size_bytes(body);
  const auto lsn = allocator_.allocate(size);
  WALRecord record(lsn, type, page_id, body);
  std::vector<std::byte> bytes = record.serialize();

  {
    std::lock_guard<std::mutex> lock(buffer_mutex_);
    buffer_.insert(buffer_.end(), bytes.begin(), bytes.end());
    last_lsn_written_ = record.get_lsn();

    if (buffer_.size() < flush_threshold_bytes_) {
      return;
    }
  }

  // TODO: file I/O should be async.
  this->flush();
}

void WAL::flush() {
  std::vector<std::byte> local_buffer;

  // batching / checkpointing lsn to flush up to allow last_lsn_written_ be
  // updated by writers concurrently.
  std::uint64_t flush_upto = 0;
  {
    std::lock_guard<std::mutex> lock(buffer_mutex_);
    if (buffer_.empty()) {
      return;
    }
    local_buffer.swap(buffer_);
    flush_upto = last_lsn_written_;
  }

  const std::byte* data = local_buffer.data();
  std::size_t remaining = local_buffer.size();

  while (remaining > 0) {
    ssize_t written = ::write(wal_fd_, data, remaining);
    if (written < 0) {
      if (errno == EINTR) {
        continue;
      }
      throw std::system_error(errno, std::generic_category(),
                              "Failed to write WAL data");
    }
    data += written;
    remaining -= static_cast<std::size_t>(written);
  }

  // make sure WAL bytes are durable before updating flushed_lsn_.
  if (::fsync(wal_fd_) != 0) {
    throw std::system_error(errno, std::generic_category(),
                            "Failed to fsync WAL file");
  }

  // update flushed LSN to tell pages are ready to be flushed up to this LSN.
  {
    std::lock_guard<std::mutex> lock(buffer_mutex_);
    if (flush_upto > flushed_lsn_) {
      flushed_lsn_ = flush_upto;
    }
  }
}

std::uint64_t WAL::getFlushedLSN() const {
  std::lock_guard<std::mutex> lock(buffer_mutex_);
  return flushed_lsn_;
}
