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
    // TODO: where should we assign LSNs?
    std::vector<std::byte> bytes = record.serialize();

    std::lock_guard<std::mutex> lock(buffer_mutex_);
    buffer_.insert(buffer_.end(), bytes.begin(), bytes.end());

    if (buffer_.size() >= flush_threshold_bytes_) {
        // TODO: file I/O should be async.
        this->flush();
    }
}

void WAL::flush()
{
    std::vector<std::byte> local_buffer;
    {
        if (buffer_.empty()) {
            return;
        }
        local_buffer.swap(buffer_);
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
}
