# Storage Notes

This document collects storage-layer design rationale that would be too noisy to
keep inline in the code.

## Storage responsibilities

```text
Table
  table-level aggregate
  - owns schema and the two backing files
  - persists schema metadata in data/<table>.meta.json
  - centralizes data/<table>.{index,db,meta.json} naming

Page
  owns physical slotted-page layout only
  - header
  - slot pointer array
  - variable-sized payload area
  - invalidation flag in each cell

LeafCell / IntermediateCell
  logical index-cell value objects
  - decode from bytes
  - serialize back to bytes

RecordSerializer
  write-side helper
  - input: Schema + TypedRow
  - output: serialized record bytes

RecordCellView
  read-side helper
  - input: raw bytes already stored in a page
  - output: TypedRow via Schema-aware parsing
```

## Heap record layout

```text
record bytes inside one slot

+----------------------+-------------------------------+
| field                | contents                      |
+----------------------+-------------------------------+
| byte[0]              | flags                         |
| byte[1..2]           | variable payload begin offset |
| byte[3..6]           | null bitmap                   |
| byte[7..y-1]         | fixed-length payload bytes    |
| byte[y..]            | variable area                 |
|                      | - end offset table            |
|                      | - variable payload bytes      |
+----------------------+-------------------------------+
```

## WAL
- This implementation adopts a Write-Ahead Logging (WAL) design inspired by ARIES.
- The current goal is to study the design and behavior of the buffer pool, in particular by supporting a steal / no-force page flushing policy that decouples data page flushes from update processing and commit operations.
- To achieve this, each update is assigned an LSN in its corresponding log record, and each page stores the latest reflected LSN as its pageLSN.
- Before a data page is flushed, the implementation ensures that the corresponding update logs have already been persisted, thereby satisfying the basic WAL rule.
- Full ARIES features such as restart recovery (analysis / redo / undo), checkpoints, and CLRs are currently out of scope.
- We use a page-oriented WAL format with page IDs and byte/field-level deltas because it keeps the design simple while making the interaction between the buffer pool, page updates, and WAL durability more explicit by glanular logging.
- For now, WAL records are only generated for heap (data) pages. B+tree index pages are treated as
  derived state that can be rebuilt from the heap and are intentionally left out of the logging
  surface to keep the prototype focused. Extending WAL coverage to index-structure changes
  (e.g., split/merge, pointer rewiring) and implementing restart recovery for indexes is left as
  future work.

### Coordination between WAL flush and page flush
Correctness requirement
 - A page must not be flushed until the WAL records describing the changes contained in that page have been made durable.
 - In practice, a page is flushable when its pageLSN is less than or equal to the WAL flushedLSN.

Consequence for hot pages
 - Since page updates continue asynchronously, there can be access patterns where WAL durability and page flushing do not keep up with ongoing writes.
 - As a result, the latest in-memory version of a hot page may remain unflushable for an extended period.
 - This is not necessarily a correctness problem, but it can increase restart recovery time.
 - It is also not always undesirable from a buffer pool perspective, since frequently accessed pages are often worth keeping resident in memory.

Design trade-off
 - One option is to flush only the latest in-memory page state.
 - Another option is to flush a stable snapshot taken at some earlier point in time.
 - The latter can improve concurrency by decoupling page write-back from ongoing updates, but introduces overhead such as extra memory usage, memcpy cost, and cache pollution.

Design implication
 - The main issue is therefore not correctness itself, but the trade-off among recovery time, concurrency, and memory/CPU overhead.
 - The system should clearly separate mandatory correctness rules from performance policy:
 - WAL durability before page flush is mandatory.
 - How aggressively dirty pages are written back is a design and tuning choice.

For now, page flushing is best-effort: correctness is enforced by the WAL rule, while aggressive coordination for hot pages and recovery-time optimization is left for future work.

### Current WAL write-path rationale
- We currently serialize WAL records on writer threads and append the bytes into a shared in-memory buffer.
- This keeps the prototype simple while preserving the on-disk format and the buffer-pool/WAL interaction we want to study.
- For the workloads we currently target, fsync and device latency are expected to dominate before per-record serialization work does.
- If profiling later shows contention or serialization overhead in this path, the same write API can move behind a dedicated WAL thread without changing the WAL format.

## Page model

- All page types share one slotted-page physical layout: header, slot pointer array, and variable-sized payload area.
- Heap pages store serialized heap records.
- Leaf index pages store `LeafCell` entries that map keys to heap RIDs.
- Internal index pages store `IntermediateCell` entries that map keys to child page IDs.
- The header physically reserves space for fields such as the node-type flag and right-most child pointer on every page.
- In practice, the right-most child pointer is only meaningful for internal index pages; heap pages and leaf index pages simply carry that header field unused.

## Current compromises

- `Page` is a shared physical container across heap and index storage, so some header fields are more index-oriented than heap-oriented.
- Heap-page creation still flows through APIs that take an `is_leaf` flag because page initialization is currently shared with index pages.
- This keeps the implementation compact for now, but the naming leak is a known design trade-off rather than a semantic claim that heap pages are B-tree leaves.

## File stream cache

`File` keeps a shared stream cache keyed by file path.

- Multiple `File` objects for the same path reuse one live OS-level stream when possible.
- This avoids repeatedly opening the same backing file while still letting callers keep lightweight `File` wrappers.
- When the last shared owner goes away, the underlying stream can be closed and removed from the cache.