#include "executor.h"

#include <optional>
#include <stdexcept>
#include <string>

#include "../logging.h"
#include "storage/runtime/bufferpool.h"
#include "storage/wal/lsn_allocator.h"
#include "storage/wal/wal.h"
#include "catalog/table.h"

/**
 * - executor now operates on Table + TypedRow rather than raw File pairs
- schema metadata is persisted in data/<table>.meta.json
- multi-column records are supported by RecordSerializer and RecordCellView
- E2E uses Table-backed fixtures
- key is still passed separately from TypedRow in executor::insert/update
- range scan still drops below executor into IndexLookup + HeapFetch directly
 * 
 * 
 */
TypedRow executor::read(BufferPool& pool, Table& table, int key) {
  std::optional<RID> rid = table.findRID(pool, key);
  if (!rid.has_value()) {
    throw std::runtime_error("Key " + std::to_string(key) +
                             " not found in index file.");
  }
  return table.readRow(pool, rid.value());
}

void executor::insert(BufferPool& pool, Table& table, int key,
                      const TypedRow& row) {
  RID rid = table.insertHeapRecord(pool, key, row);
  table.insertIndexEntry(pool, key, rid);
}

void executor::insert(BufferPool& pool, Table& table, int key,
                      const TypedRow& row, LSNAllocator& allocator, WAL& wal) {
  LOG_INFO("Inserting record with key {} into table {}.", key, table.name());
  std::optional<RID> existing_rid = table.findRID(pool, key);
  if (existing_rid.has_value()) {
    throw std::runtime_error(
        "Key " + std::to_string(key) +
        " already exists. Duplicate keys are not allowed.");
  }

  RID rid = table.insertHeapRecord(pool, key, row, &allocator, &wal);
  table.insertIndexEntry(pool, key, rid);
}

void executor::remove(BufferPool& pool, Table& table, int key) {
  std::optional<RID> rid = table.findRID(pool, key, true);
  if (!rid.has_value()) {
    throw std::runtime_error("Key " + std::to_string(key) +
                             " not found in leaf page.");
  }
  table.invalidateHeapRecord(pool, rid.value());
  LOG_INFO("Removed record with key {} successfully.", key);
}

void executor::remove(BufferPool& pool, Table& table, int key,
                      LSNAllocator& allocator, WAL& wal) {
  std::optional<RID> rid = table.findRID(pool, key, true);
  if (!rid.has_value()) {
    throw std::runtime_error("Key " + std::to_string(key) +
                             " not found in leaf page.");
  }
  table.invalidateHeapRecord(pool, rid.value(), allocator, wal);
  LOG_INFO("Removed record with key {} successfully.", key);
}

/**
 * We first design updates to be idempotent by modeling them as a remove
 * followed by an insert. This is not necessarily the most efficient strategy,
 * but it keeps the update path simple and robust and fasten the development.
 * Also, this unlocks follow-on benefits (e.g., easier recovery/retry and
 * fewer page-structure assumptions) without requiring in-place updates or
 * special-case split handling.
 */
void executor::update(BufferPool& pool, Table& table, int key,
                      const TypedRow& row) {
  executor::remove(pool, table, key);
  executor::insert(pool, table, key, row);
}

void executor::update(BufferPool& pool, Table& table, int key,
                      const TypedRow& row, LSNAllocator& allocator, WAL& wal) {
  executor::remove(pool, table, key, allocator, wal);
  executor::insert(pool, table, key, row, allocator, wal);
}
