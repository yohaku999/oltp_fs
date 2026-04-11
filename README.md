# dbfs

Learning-oriented prototype of a disk-based OLTP RDBMS.
- https://www.youtube.com/@CMUDatabaseGroup
- https://www.oreilly.com/library/view/database-internals/9781492040330/

## Project

Goals:

- learn database internals and performance trade-offs
- support concurrent query execution

Focus:

- storage layout and page management
- WAL / page-flush coordination
- executor paths built on top of table-backed storage

Design assumptions:

- run in a multithreaded environment
- target a UMA architecture

## Module Notes

- storage internals and design rationale: [src/storage/README.md](src/storage/README.md)

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
