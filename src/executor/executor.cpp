#include "executor.h"

#include <stdexcept>
#include <string>
#include <vector>

#include "../logging.h"
#include "../storage/btreecursor.h"
#include "../storage/bufferpool.h"
#include "../storage/file.h"
#include "../storage/lsn_allocator.h"
#include "../storage/page.h"
#include "../storage/record_cell.h"
#include "../storage/record_serializer.h"
#include "../storage/wal/wal.h"
#include "../storage/wal/wal_body.h"
#include "../storage/wal_record.h"
#include "../table/table.h"
#include "heap_fetch.h"
#include "index_scan.h"

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
namespace {
std::pair<uint16_t, uint16_t> insertIntoHeap(BufferPool& pool, File& heapFile,
                                             const Schema& schema,
                                             const TypedRow& row, int key,
                                             LSNAllocator* allocator = nullptr,
                                             WAL* wal = nullptr) {
  LOG_INFO("Inserting record with key {} into heap file {}.", key,
           heapFile.getFilePath());
  RecordSerializer cell(schema, row);
  const std::vector<std::byte>& serialized_cell = cell.serializedBytes();

  int target_page_id = heapFile.getMaxPageID();
  Page* heap_page = pool.pinPage(target_page_id, heapFile);
  auto inserted_slot_id = heap_page->insertCell(serialized_cell);
  if (!inserted_slot_id.has_value()) {
    pool.unpinPage(heap_page, heapFile);
    target_page_id = pool.createPage(PageKind::Heap, heapFile);
    heap_page = pool.pinPage(target_page_id, heapFile);
    inserted_slot_id = heap_page->insertCell(serialized_cell);
    if (!inserted_slot_id.has_value()) {
      throw std::runtime_error(
          "Failed to insert record cell into a new heap page due to "
          "insufficient space.");
    }
  }

  if (allocator != nullptr && wal != nullptr) {
    wal->write(make_wal_record(
        *allocator, WALRecord::RecordType::INSERT,
        static_cast<uint16_t>(target_page_id),
        InsertRedoBody(static_cast<uint16_t>(inserted_slot_id.value()),
                       serialized_cell)
            .encode()));
  }

  pool.unpinPage(heap_page, heapFile);
  LOG_INFO("Inserted record with key {} into heap page ID {} successfully.",
           key, target_page_id);

  return {static_cast<uint16_t>(target_page_id),
          static_cast<uint16_t>(inserted_slot_id.value())};
}

}  // namespace

TypedRow executor::read(BufferPool& pool, Table& table, int key) {
  auto lookup = IndexLookup::fromKey(pool, table.indexFile(), key);
  auto rid = lookup.next();
  if (!rid.has_value()) {
    throw std::runtime_error("Key " + std::to_string(key) +
                             " not found in index file.");
  }

  HeapFetch fetcher(pool, table.heapFile());
  char* cell_start = fetcher.fetch(rid->heap_page_id, rid->slot_id);
  return RecordCellView(cell_start).getTypedRow(table.schema());
}

void executor::insert(BufferPool& pool, Table& table, int key,
                      const TypedRow& row) {
  auto [heap_page_id, slot_id] =
      insertIntoHeap(pool, table.heapFile(), table.schema(), row, key);
  BTreeCursor::insertIntoIndex(pool, table.indexFile(), key, heap_page_id,
                               slot_id);
}

void executor::insert(BufferPool& pool, Table& table, int key,
                      const TypedRow& row, LSNAllocator& allocator, WAL& wal) {
  LOG_INFO("Inserting record with key {} into table {}.", key, table.name());
  auto location = BTreeCursor::findRecordLocation(pool, table.indexFile(), key);
  if (location.has_value()) {
    throw std::runtime_error(
        "Key " + std::to_string(key) +
        " already exists. Duplicate keys are not allowed.");
  }

  auto [heap_page_id, slot_id] = insertIntoHeap(
      pool, table.heapFile(), table.schema(), row, key, &allocator, &wal);
  BTreeCursor::insertIntoIndex(pool, table.indexFile(), key, heap_page_id,
                               slot_id);
}

void executor::remove(BufferPool& pool, Table& table, int key) {
  auto location =
      BTreeCursor::findRecordLocation(pool, table.indexFile(), key, true);
  if (!location.has_value()) {
    throw std::runtime_error("Key " + std::to_string(key) +
                             " not found in leaf page.");
  }
  auto [page_id, slot_id] = location.value();
  Page* page = pool.pinPage(page_id, table.heapFile());
  page->invalidateSlot(slot_id);
  pool.unpinPage(page, table.heapFile());
  LOG_INFO("Removed record with key {} successfully.", key);
}

void executor::remove(BufferPool& pool, Table& table, int key,
                      LSNAllocator& allocator, WAL& wal) {
  auto location =
      BTreeCursor::findRecordLocation(pool, table.indexFile(), key, true);
  if (!location.has_value()) {
    throw std::runtime_error("Key " + std::to_string(key) +
                             " not found in leaf page.");
  }
  auto [page_id, slot_id] = location.value();
  Page* page = pool.pinPage(page_id, table.heapFile());
  wal.write(make_wal_record(
      allocator, WALRecord::RecordType::DELETE, static_cast<uint16_t>(page_id),
      DeleteRedoBody(static_cast<uint16_t>(slot_id)).encode()));
  page->invalidateSlot(slot_id);
  pool.unpinPage(page, table.heapFile());
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
