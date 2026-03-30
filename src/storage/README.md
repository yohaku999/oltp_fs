# Aim
- A disk-based OLTP RDBMS.
- A personal project to learn about database internals and performance.
- Design goals:
  - Run in a multithreaded environment.
  - Target a UMA architecture.
  - Support concurrent query execution.

# Component
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