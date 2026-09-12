# Caching Open Storage Files

## Summary

Before this change, each SQL execution created its own `LocalStorageManager`.
The `PagedFile` state cache held `weak_ptr` values, so the last strong reference
disappeared at the end of the SQL execution and the file was closed. The next
SQL statement reopened the same heap and index files. Header state was also
persisted as part of closing the file.

TPC-C repeatedly executes short SQL statements, so this fixed cost appeared in
the CPU profile. The server was changed to own the `LocalStorageManager` and
reuse open `PagedFile` objects across requests. Files used during the benchmark
therefore remained cached throughout the measurement interval.

Throughput increased by 27.2%, from 158.47 to 201.54 requests per second, while
average latency decreased by 21.4%, from 6.29 to 4.95 ms.

## Change

Storage files are resources used continuously by the server rather than by one
SQL execution, so their lifetime was aligned with the server lifetime. The
server owns one `LocalStorageManager`, which keeps a strong reference to each
`PagedFile` by file name. Only the first access opens the file, and later
accesses return the same cached state. Statement-local `HeapFile` objects share
that state, so their destruction no longer closes the file descriptor.

To support the longer file lifetime, header persistence was extracted into an
independently callable `flush()`. It writes dirty header state and flushes the
stream without closing the file. `close()` still calls `flush()` as a final
safeguard. Database-page residency remains managed by the buffer pool.

## Result

Both runs used the TPC-C throughput configuration with one terminal, scale
factor 1, a 60-second warmup, and a 300-second measurement interval.

| Metric | Before | After | Change |
| --- | ---: | ---: | ---: |
| Measured requests | 47,540 | 60,462 | +27.2% |
| Throughput | 158.47 req/s | 201.54 req/s | +27.2% |
| Average latency | 6.29 ms | 4.95 ms | -21.4% |
| Median latency | 4.82 ms | 4.03 ms | -16.3% |
| P95 latency | 18.30 ms | 11.95 ms | -34.7% |
| P99 latency | 26.02 ms | 19.10 ms | -26.6% |

File-operation stacks also nearly disappeared from the CPU profile, which is
consistent with removing file open and close operations from each SQL
execution.

## Limits

Opened files are currently retained until they are dropped or the server exits.
A workload with many files will require a file-descriptor limit and an eviction
policy.

`PagedFile::flush()` only flushes the C++ stream to the operating system. It
does not provide durability equivalent to `fsync`. WAL ordering and crash
recovery remain separate concerns.
