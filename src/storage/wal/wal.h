#pragma once

#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "lsn_allocator.h"
#include "wal_record.h"

/**
 * Invariants:
 * - buffer_ only contains records with LSN > flushed_lsn_.
 * - flushed_lsn_ never exceeds the LSN of the last record whose bytes have been
 *   written and fsync'ed to the WAL file.
 * - Multiple writer threads may call write() concurrently; flush() may run
 *   concurrently as well. All shared state updates are serialized by
 *   buffer_mutex_, so there are no data races on buffer_ / last_lsn_written_ /
 *   flushed_lsn_.
 */
class WAL {
 public:
  static std::unique_ptr<WAL> initializeNew(const std::string& wal_path);
  static std::unique_ptr<WAL> openExisting(const std::string& wal_path);

  // Makes buffered WAL bytes durable and advances flushed_lsn_.
  void flush();

  std::uint64_t getFlushedLSN() const;

  // Allocates an LSN, serializes the record, and may trigger a batch flush.
  void write(WALRecord::RecordType type, uint16_t page_id,
             const std::vector<std::byte>& body);

 private:
  WAL(const std::string& wal_path, std::uint64_t next_lsn,
      std::uint64_t durable_lsn, int open_flags);

  int wal_fd_;
  LSNAllocator allocator_;
  std::vector<std::byte> buffer_;
  // Protects buffer_, last_lsn_written_, and flushed_lsn_.
  mutable std::mutex buffer_mutex_;
  std::size_t flush_threshold_bytes_ = 1 * 1024 * 1024;
  std::uint64_t last_lsn_written_{0};
  // LSN of the last record known to be durable.
  std::uint64_t flushed_lsn_{0};
};