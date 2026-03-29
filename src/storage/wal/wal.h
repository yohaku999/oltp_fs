#pragma once

#include <cstdint>
#include <vector>
#include <mutex>
#include <string>

#include "wal_record.h"

class WAL
{
public:
    explicit WAL(const std::string& wal_path);


    void flush();
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
    std::mutex buffer_mutex_;
    std::size_t flush_threshold_bytes_ = 1 * 1024 * 1024; // 1MB for now
};