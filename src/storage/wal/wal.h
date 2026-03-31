#pragma once

#include <cstdint>
#include <vector>
#include <mutex>
#include <string>

#include "wal_record.h"

// Invariants:
// - buffer_ only contains records with LSN > flushed_lsn_.
// - flushed_lsn_ never exceeds the LSN of the last record whose bytes have been
//   written and fsync'ed to the WAL file.
// - Multiple writer threads may call write() concurrently; flush() may run
//   concurrently as well. All shared state updates are serialized by
//   buffer_mutex_, so there are no data races on buffer_ / last_lsn_written_ /
//   flushed_lsn_.
class WAL
{
public:
    explicit WAL(const std::string& wal_path);


    void flush();
    std::uint64_t getFlushedLSN() const;
    // We currently assign LSNs and serialize WALRecord objects on writer threads.
    //
    // Rationale:
    // - Per-record work is small (one atomic LSN increment and a short memcpy)
    //   and is done outside page latches, so it is unlikely to dominate throughput
    //   under our expected workloads (single-socket, <= 32 threads, moderate record sizes).
    // - For now, I/O costs (e.g., fsync and device latency) are more likely to be
    //   the primary bottleneck than LSN assignment or serialization.
    // - Since each record touches the global LSN counter only once, we do not
    //   currently treat cache-line contention on that counter as a primary concern.
    //
    // If profiling later shows that either the global LSN counter or serialization
    // becomes a measurable bottleneck, we can move both responsibilities to
    // a dedicated WAL thread without changing the on-disk format.
    void write(const WALRecord& record);

private:
    int wal_fd_;
    std::vector<std::byte> buffer_;
    // "mutable" allows getFlushedLSN() to acquire the mutex even though it's a const method. it is allowed because the mutex is only used to protect the internal state of WAL and does not affect the observable behavior of WAL from the caller's perspective.
    // protects all shared state in WAL: buffer_, last_lsn_written_, and flushed_lsn_.
    mutable std::mutex buffer_mutex_;
    std::size_t flush_threshold_bytes_ = 1 * 1024 * 1024; // 1MB for now
    // LSN of the last record that was handed to write().
    std::uint64_t last_lsn_written_{0};
    // LSN of the last record that is known to be durable　(its bytes have been written and fsync'ed).
    std::uint64_t flushed_lsn_{0};
};