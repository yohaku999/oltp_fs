# dbfs

Current design sketch for the storage, record, and execution layers.

## High-level Architecture

```text
         metadata path
         -------------
       +----------------------+
       | Table                |
       | - name               |
       | - schema             |
       | - index file         |
       | - heap file          |
       | - meta.json          |
       +----+------------+----+
         |            |
         |            +--------------------------+
         |                                       |
         v                                       v
      +-------------------+                  +-------------------+
      | executor          |                  | Schema / Column   |
      | - insert(key,row) |                  | - type metadata   |
      | - read(key)       |                  | - fixed/variable  |
      | - update(key,row) |                  +---------+---------+
      | - remove(key)     |                            |
      +---------+---------+                            v
          |                            +-------------------+
          |                            | TypedRow          |
          |                            | FieldValue[]      |
          |                            +---------+---------+
          |                                      |
          | write path                           | read path
          | ----------                           | ---------
    +-----------+-----------+                          |
    |                       |                          |
    v                       v                          v
 +-------------------+   +-------------------+    +-------------------+
 | BTreeCursor       |   | RecordSerializer  |    | RecordCellView    |
 | - index traversal |   | Schema + TypedRow |    | bytes -> TypedRow |
 | - index insert    |   | -> bytes          |    +---------+---------+
 +---------+---------+   +---------+---------+              |
     |                           |                    v
     |                           v          +-------------------+
     |                  +-------------------+ HeapFetch         |
     |                  | Page              | - fetch heap slot |
     |                  | slotted page      +---------+---------+
     |                  | - header                    |
     v                  | - slot pointers             |
 +-------------------+        | - cell bytes                |
 | LeafCell          |        +-----------------------------+
 | IntermediateCell  |
 | index cells       |
 +-------------------+
```

## Storage Responsibilities

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
Table::initialize(name, schema)
  -> create data/<table>.index
  -> create data/<table>.db
  -> write data/<table>.meta.json

executor::insert(pool, table, key, row)
  -> RecordSerializer(table.schema(), row)
  -> serialized bytes
  -> Page::insertCell(bytes) on table.heapFile()
  -> slot id
  -> BTreeCursor::insertIntoIndex(key, heap_page_id, slot_id)
```

## Read Path

```text
Table::getTable(name)
  -> read data/<table>.meta.json
  -> restore Schema

executor::read(pool, table, key)
  -> BTreeCursor / IndexLookup on table.indexFile()
  -> RID(heap_page_id, slot_id)
  -> HeapFetch on table.heapFile()
  -> Page::getXthSlotCellStart(slot_id)
  -> RecordCellView(cell_start).getTypedRow(table.schema())
  -> TypedRow
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

table layer
  Table
    binds schema metadata and backing files together
    provides the unit executor operates on
```

## Current Constraints

```text
- executor now operates on Table + TypedRow rather than raw File pairs
- schema metadata is persisted in data/<table>.meta.json
- multi-column records are supported by RecordSerializer and RecordCellView
- E2E uses Table-backed fixtures
- key is still passed separately from TypedRow in executor::insert/update
- range scan still drops below executor into IndexLookup + HeapFetch directly
```
