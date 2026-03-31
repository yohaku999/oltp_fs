#include "wal.h"

#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <system_error>

WAL::WAL(const std::string& wal_path)
    : wal_fd_(-1)
{
    int fd = ::open(wal_path.c_str(), O_CREAT | O_WRONLY, 0644);
    if (fd == -1) {
        throw std::system_error(errno, std::generic_category(), "Failed to open WAL file");
    }
    wal_fd_ = fd;
}

void WAL::write(const WALRecord& record)
{
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

void WAL::flush()
{
    std::vector<std::byte> local_buffer;

    // batching / checkpointing lsn to flush up to allow last_lsn_written_ be updated by writers concurrently.
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
            throw std::system_error(errno, std::generic_category(), "Failed to write WAL data");
        }
        data += written;
        remaining -= static_cast<std::size_t>(written);
    }

    // fsync
    if (::fsync(wal_fd_) != 0) {
        throw std::system_error(errno, std::generic_category(), "Failed to fsync WAL file");
    }

    // update flushed LSN to tell pages are ready to be flushed up to this LSN.
    {
        std::lock_guard<std::mutex> lock(buffer_mutex_);
        if (flush_upto > flushed_lsn_) {
            flushed_lsn_ = flush_upto;
        }
    }
}

std::uint64_t WAL::getFlushedLSN() const
{
    std::lock_guard<std::mutex> lock(buffer_mutex_);
    return flushed_lsn_;
}
