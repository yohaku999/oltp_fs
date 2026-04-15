#include "executor.h"

#include <optional>
#include <stdexcept>
#include <string>

#include "../logging.h"
#include "storage/index/btreecursor.h"
#include "storage/page/page.h"
#include "storage/record/record_cell.h"
#include "storage/record/record_serializer.h"
#include "storage/runtime/bufferpool.h"
#include "storage/wal/wal.h"
#include "storage/wal/wal_body.h"
#include "storage/wal/wal_record.h"
#include "catalog/table.h"

namespace {

// only avaiable for single-column integer access indexes for now
int extractAccessKey(const Table& table, const TypedRow& row) {
  if (!table.indexedColumnName().has_value()) {
    throw std::runtime_error("Table has no configured indexed column: " +
                             table.name());
  }
  const std::string& column_name = table.indexedColumnName().value();
  const int column_index = table.schema().getColumnIndex(column_name);
  if (column_index < 0) {
    throw std::runtime_error("Access index column not found in schema: " +
                             column_name);
  }
  if (row.values.size() <= static_cast<std::size_t>(column_index)) {
    throw std::runtime_error("Row does not contain access-index column: " +
                             column_name);
  }
  if (!std::holds_alternative<Column::IntegerType>(row.values[column_index])) {
    throw std::runtime_error("Access-index column must be Integer: " +
                             column_name);
  }
  return std::get<Column::IntegerType>(row.values[column_index]);
}

}  // namespace

/**
 * - executor now operates on Table + TypedRow rather than raw File pairs
- schema metadata is persisted in data/<table>.meta.json
- multi-column records are supported by RecordSerializer and RecordCellView
- E2E uses Table-backed fixtures
- key is still passed separately from TypedRow in executor::insert/update
- range scan still drops below executor into IndexScan + HeapFetch directly
 * 
 * 
 */
TypedRow executor::read(BufferPool& pool, Table& table, int key) {
  std::optional<RID> rid = BTreeCursor::findRID(pool, table.indexFile(), key);
  if (!rid.has_value()) {
    throw std::runtime_error("Key " + std::to_string(key) +
                             " not found in index file.");
  }

  Page* page = pool.pinPage(rid->heap_page_id, table.heapFile());
  TypedRow row =
      RecordCellView(page->getSlotCellStart(rid->slot_id)).getTypedRow(
          table.schema());
  pool.unpinPage(page, table.heapFile());
  return row;
}

void executor::insert(BufferPool& pool, Table& table, const TypedRow& row,
                      WAL& wal) {
  const int key = extractAccessKey(table, row);
  LOG_INFO("Inserting record with key {} into table {}.", key, table.name());
  std::optional<RID> existing_rid =
      BTreeCursor::findRID(pool, table.indexFile(), key);
  if (existing_rid.has_value()) {
    throw std::runtime_error(
        "Key " + std::to_string(key) +
        " already exists. Duplicate keys are not allowed.");
  }

  RecordSerializer cell(table.schema(), row);
  const std::vector<std::byte>& serialized_cell = cell.serializedBytes();

  int target_page_id = table.heapFile().getMaxPageID();
  Page* heap_page = pool.pinPage(target_page_id, table.heapFile());
  auto inserted_slot_id = heap_page->insertCell(serialized_cell);
  if (!inserted_slot_id.has_value()) {
    pool.unpinPage(heap_page, table.heapFile());
    target_page_id = pool.createPage(PageKind::Heap, table.heapFile());
    heap_page = pool.pinPage(target_page_id, table.heapFile());
    inserted_slot_id = heap_page->insertCell(serialized_cell);
    if (!inserted_slot_id.has_value()) {
      throw std::runtime_error(
          "Failed to insert record cell into a new heap page due to "
          "insufficient space.");
    }
  }

  wal.write(WALRecord::RecordType::INSERT,
            static_cast<uint16_t>(target_page_id),
            InsertRedoBody(static_cast<uint16_t>(inserted_slot_id.value()),
                           serialized_cell)
                .encode());

  pool.unpinPage(heap_page, table.heapFile());

  BTreeCursor::insertIntoIndex(pool, table.indexFile(), key,
                               static_cast<uint16_t>(target_page_id),
                               static_cast<uint16_t>(inserted_slot_id.value()));
}

void executor::remove(BufferPool& pool, Table& table, int key, WAL& wal) {
  std::optional<RID> rid =
      BTreeCursor::findRID(pool, table.indexFile(), key, true);
  if (!rid.has_value()) {
    throw std::runtime_error("Key " + std::to_string(key) +
                             " not found in leaf page.");
  }

  Page* page = pool.pinPage(rid->heap_page_id, table.heapFile());
  wal.write(WALRecord::RecordType::DELETE, rid->heap_page_id,
            DeleteRedoBody(rid->slot_id).encode());
  page->invalidateSlot(rid->slot_id);
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
void executor::update(BufferPool& pool, Table& table, const TypedRow& row,
                      WAL& wal) {
  const int key = extractAccessKey(table, row);
  executor::remove(pool, table, key, wal);
  executor::insert(pool, table, row, wal);
}
