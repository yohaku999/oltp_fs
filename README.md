# dbfs

Current design sketch for the storage, record, and execution layers.

## High-level Architecture

```text
+-------------------+
| executor          |
| - insert          |
| - read            |
| - update/remove   |
+---------+---------+
          |
          v
+-------------------+        +-------------------+
| BTreeCursor       |        | HeapFetch         |
| - index traversal |        | - fetch heap slot |
| - index insert    |        +---------+---------+
+---------+---------+                  |
          |                            v
          |                  +-------------------+
          |                  | Page              |
          |                  | slotted page      |
          |                  | - header          |
          |                  | - slot pointers   |
          |                  | - cell bytes      |
          |                  +---------+---------+
          |                            ^
          v                            |
+-------------------+                  |
| LeafCell          |                  |
| IntermediateCell  |                  |
| index cells       |                  |
+-------------------+                  |
                                       |
                     write path        |        read path
                     ----------        |        ---------
+-------------------+                  |   +-------------------+
| RecordSerializer  |------------------+   | RecordCellView    |
| Schema + TypedRow | serialized bytes     | bytes -> TypedRow |
+---------+---------+                      +---------+---------+
          |                                          |
          v                                          v
+-------------------+                      +-------------------+
| TypedRow          |                      | caller-specific   |
| FieldValue[]      |                      | conversion        |
+---------+---------+                      +-------------------+
          ^
          |
+-------------------+
| Schema / Column   |
| - type metadata   |
| - fixed/variable  |
+-------------------+
```

## Storage Responsibilities

```text
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

## Heap Record Layout

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

## Write Path

```text
TypedRow
  -> RecordSerializer(schema, row)
  -> serialized bytes
  -> Page::insertCell(bytes)
  -> slot id
  -> BTreeCursor::insertIntoIndex(key, heap_page_id, slot_id)
```

## Read Path

```text
key
  -> BTreeCursor / IndexLookup
  -> RID(heap_page_id, slot_id)
  -> HeapFetch
  -> Page::getXthSlotCellStart(slot_id)
  -> RecordCellView(cell_start).getTypedRow(schema)
  -> TypedRow
  -> caller-specific conversion
```

## Current Direction

```text
physical layer
  Page
    handles slot management and raw bytes

logical index layer
  LeafCell / IntermediateCell / LeafIndexPage / InternalIndexPage
    handle B+tree semantics

logical heap-record layer
  RecordSerializer / RecordCellView / Schema / TypedRow
    handle record encoding and decoding
```

## Current Constraints

```text
- executor::insert/read/update still expose a single-value API
- executor::read still returns char* for compatibility
- multi-column records are supported by RecordSerializer and RecordCellView
- E2E now exercises multi-column TypedRow parsing
- fixed/variable handling is schema-driven, not single-column-specific anymore
```
