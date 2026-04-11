# Storage responsibilities

- `Table`: table-level aggregate that owns schema metadata and the two backing files, and centralizes `data/<table>.{index,db,meta.json}` naming.
- `Page`: physical slotted-page container that owns the header, slot pointer array, payload area, and per-cell invalidation flag.
- `LeafCell` / `IntermediateCell`: logical index-cell value objects that decode from bytes and serialize back to bytes.
- `RecordSerializer`: write-side helper that converts `Schema + TypedRow` into serialized record bytes.
- `RecordCellView`: read-side helper that interprets raw record bytes already stored in a page.

# Page
## layout

All page types share one slotted-page physical layout.

```text
+----------------------+----------------------------------------------+
| region               | contents                                     |
+----------------------+----------------------------------------------+
| header               | node type flag                               |
|                      | slot count                                   |
|                      | slot directory offset                        |
|                      | right-most child pointer                     |
|                      | pageLSN                                      |
| slot pointer array   | uint16_t offsets to payload cells            |
| payload area         | variable-sized cell bytes                    |
+----------------------+----------------------------------------------+
```

### Shared header fields

| Field | Meaning | Notes |
| --- | --- | --- |
| node type flag | Distinguishes leaf vs internal index semantics | Heap pages currently reuse the leaf side of this shared flag |
| slot count | Number of slot pointers currently in use | Shared by all page types |
| slot directory offset | Start of free space / payload boundary | Shared by all page types |
| right-most child pointer | Final branch pointer for internal index pages | Physically present on all pages, semantically used only by internal index pages |
| pageLSN | Latest WAL record reflected in the page | Shared by all page types |

### Heap page payload

Heap pages store serialized records.

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

### Leaf index page payload

Leaf pages map an index key to a heap RID `(heap_page_id, slot_id)`.

```text
leaf cell bytes inside one slot

+----------------------+-------------------------------+
| field                | contents                      |
+----------------------+-------------------------------+
| byte[0]              | flags                         |
| byte[1..2]           | key size                      |
| byte[3..4]           | heap page ID                  |
| byte[5..6]           | slot ID                       |
| byte[7..10]          | key                           |
+----------------------+-------------------------------+
```

### Internal index page payload

Internal pages map an index key to a child page ID and use the shared header's
right-most child pointer for the final branch.

```text
intermediate cell bytes inside one slot

+----------------------+-------------------------------+
| field                | contents                      |
+----------------------+-------------------------------+
| byte[0]              | flags                         |
| byte[1..2]           | key size                      |
| byte[3..4]           | child page ID                 |
| byte[5..8]           | key                           |
+----------------------+-------------------------------+
```

# WAL

This implementation adopts a Write-Ahead Logging (WAL) design inspired by
ARIES. The current goal is to study the design and behavior of the buffer pool,
in particular by supporting a steal / no-force page flushing policy that
decouples data page flushes from update processing and commit operations.

## Scope and on-disk format

We use a page-oriented WAL format with page IDs and byte/field-level deltas.
That keeps the design simple while making the interaction between the buffer
pool, page updates, and WAL durability more explicit by glanular logging.

Full ARIES features such as restart recovery (analysis / redo / undo),
checkpoints, and CLRs are currently out of scope.

For now, WAL records are only generated for heap (data) pages. B+tree index
pages are treated as derived state that can be rebuilt from the heap and are
intentionally left out of the logging surface to keep the prototype focused.
Extending WAL coverage to index-structure changes (e.g., split/merge, pointer
rewiring) and implementing restart recovery for indexes is left as future work.

## Behavioral contract

Each update is assigned an LSN in its corresponding log record, and each page
stores the latest reflected LSN as its pageLSN.

Before a data page is flushed, the implementation ensures that the
corresponding update logs have already been persisted, thereby satisfying the
basic WAL rule. In practice, a page is flushable when its pageLSN is less than
or equal to the WAL flushedLSN.

## Issues and design options
### Page write-back coordination
#### concurrent updates can keep the newest page state unflushable
When multiple updates keep arriving on the same page, writer threads continue
to advance that page's pageLSN and produce newer in-memory page states. Even
if earlier WAL records are already durable, the latest in-memory page image may
still depend on newer log records whose LSNs are above flushedLSN.

In that situation, the buffer pool cannot flush the current page image yet, so
a hot page can remain dirty and unflushable for an extended period. This is
fundamentally a coordination issue among concurrent writers, WAL durability,
and page write-back, not a violation of the WAL rule itself.

One visible consequence is longer restart recovery, because more recent changes
may need to be replayed from WAL after a crash. From a buffer-pool
perspective, however, keeping a frequently updated page resident is not always
undesirable.

#### lock-based flush vs snapshot-based flush

One option is to flush only the latest in-memory page state and wait until its
pageLSN becomes flushable. Another option is to capture a stable snapshot of an
earlier page state while writers continue mutating the live page.

That snapshot is what lets page write-back proceed without writing a page image
that is being modified concurrently. The trade-off is extra buffering and copy
work.

Beyond the correctness rule itself, the main design question is how page
write-back should coordinate with concurrent page updates. When write
contention is low, simply locking the page during flush may be acceptable and
keeps the design simple. Snapshot-based flushing becomes attractive when the
system wants to let writers continue updating a hot page while write-back is
in progress.

The system should clearly separate mandatory correctness rules from
performance policy: WAL durability before page flush is mandatory, while how
aggressively dirty pages are written back and whether lock-based or snapshot-
based coordination is used are design and tuning choices.

For now, page flushing remains simple: the implementation only enforces the
WAL flushability check before writing a dirty page back. It does not yet take
stable page snapshots or add extra coordination for hot pages under concurrent
write traffic, so recovery-time optimization is left for future work.

### WAL write-path design
We currently serialize WAL records on writer threads and append them to a
shared in-memory buffer under a single mutex. This keeps the implementation
simple, but it means writer threads pay both record-construction cost and
shared-buffer coordination cost on the WAL write path.

The LSN allocator already assigns monotonically increasing byte offsets, but
the current implementation does not yet let writers publish record bytes into
independent reserved positions in the WAL buffer. Moving to that design could
reduce append-path contention, but it would still leave record serialization on
writer threads.

If profiling later shows that the WAL write path is slowing writers down, we
could either redesign the append path or move WAL construction behind a
dedicated WAL thread without changing the WAL format.

### File stream cache

`File` keeps a shared stream cache keyed by file path.

- Multiple `File` objects for the same path reuse one live OS-level stream when possible.
- This avoids repeatedly opening the same backing file while still letting callers keep lightweight `File` wrappers.
- When the last shared owner goes away, the underlying stream can be closed and removed from the cache.