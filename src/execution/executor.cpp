#include "executor.h"

#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

#include "../logging.h"
#include "execution/operators/filter_operator.h"
#include "execution/operators/heap_fetch.h"
#include "execution/operators/index_scan.h"
#include "execution/operators/limit_operator.h"
#include "execution/operator.h"
#include "execution/operators/orderby_operator.h"
#include "execution/operators/projection_operator.h"
#include "execution/parsers/create_index_parser.h"
#include "execution/parsers/create_table_parser.h"
#include "execution/parsers/delete_parser.h"
#include "execution/parsers/drop_table_parser.h"
#include "execution/parsers/insert_parser.h"
#include "execution/parsers/select_parser.h"
#include "execution/parsers/update_parser.h"
#include "execution/operators/seq_scan_operator.h"
#include "storage/index/btreecursor.h"
#include "storage/page/cell.h"
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

std::vector<ComparisonPredicate> extractIndexedPredicates(
    const std::vector<ComparisonPredicate>& predicates,
    std::size_t indexed_column_index) {
  std::vector<ComparisonPredicate> index_predicates;
  for (const auto& predicate : predicates) {
    if (predicate.column_index == indexed_column_index) {
      index_predicates.push_back(predicate);
    }
  }
  return index_predicates;
}

bool matchesPredicates(const TypedRow& row,
                       const std::vector<ComparisonPredicate>& predicates) {
  for (const auto& predicate : predicates) {
    const auto& value = row.values[predicate.column_index];
    switch (predicate.op) {
      case ComparisonPredicate::Op::Eq:
        if (value != predicate.value) {
          return false;
        }
        break;
      case ComparisonPredicate::Op::Gt:
        if (value <= predicate.value) {
          return false;
        }
        break;
      case ComparisonPredicate::Op::Ge:
        if (value < predicate.value) {
          return false;
        }
        break;
      case ComparisonPredicate::Op::Lt:
        if (value >= predicate.value) {
          return false;
        }
        break;
      case ComparisonPredicate::Op::Le:
        if (value > predicate.value) {
          return false;
        }
        break;
    }
  }

  return true;
}

File& requireIndexFile(Table& table) {
  const auto index_file = table.indexFile();
  if (!index_file.has_value()) {
    throw std::runtime_error("Table has no index file: " + table.name());
  }
  return index_file->get();
}

std::vector<RID> collectHeapRids(BufferPool& pool, File& heap_file) {
  std::vector<RID> rids;
  for (uint16_t page_id = 0; page_id <= heap_file.getMaxPageID(); ++page_id) {
    Page* page = pool.pinPage(page_id, heap_file);
    for (uint16_t slot_id = 0; slot_id < page->slotCount(); ++slot_id) {
      char* cell_start = page->slotCellStartUnchecked(slot_id);
      if (!Cell::isValid(cell_start)) {
        continue;
      }
      rids.push_back(RID{page_id, slot_id});
    }
    pool.unpinPage(page, heap_file);
  }

  return rids;
}

std::vector<RID> collectCandidateRids(
    BufferPool& pool, Table& table,
    const std::vector<ComparisonPredicate>& predicates) {
  const std::optional<std::string>& indexed_column_name =
      table.indexedColumnName();
  if (!indexed_column_name.has_value()) {
    return collectHeapRids(pool, table.heapFile());
  }

  const int indexed_column_index =
      table.schema().getColumnIndex(indexed_column_name.value());
  if (indexed_column_index < 0) {
    return collectHeapRids(pool, table.heapFile());
  }

  std::vector<ComparisonPredicate> index_predicates = extractIndexedPredicates(
      predicates, static_cast<std::size_t>(indexed_column_index));
  if (index_predicates.empty()) {
    return collectHeapRids(pool, table.heapFile());
  }

  const auto index_file = table.indexFile();
  if (!index_file.has_value()) {
    return collectHeapRids(pool, table.heapFile());
  }

  std::vector<RID> rids;
  IndexScanOperator scan(pool, index_file->get(),
                         DiscreteIntegerIndexPredicates{std::move(index_predicates)});
  scan.open();
  while (std::optional<RID> rid = scan.next()) {
    rids.push_back(*rid);
  }
  scan.close();
  return rids;
}

std::size_t removeMatchingRows(BufferPool& pool, Table& table,
                               const std::vector<ComparisonPredicate>& predicates,
                               WAL& wal) {
  const std::vector<RID> candidate_rids =
      collectCandidateRids(pool, table, predicates);
  std::size_t removed_count = 0;

  for (const RID& rid : candidate_rids) {
    Page* page = pool.pinPage(rid.heap_page_id, table.heapFile());
    char* cell_start = page->slotCellStartUnchecked(rid.slot_id);
    if (!Cell::isValid(cell_start)) {
      pool.unpinPage(page, table.heapFile());
      continue;
    }

    TypedRow row = RecordCellView(cell_start).getTypedRow(table.schema());
    if (!matchesPredicates(row, predicates)) {
      pool.unpinPage(page, table.heapFile());
      continue;
    }

    wal.write(WALRecord::RecordType::DELETE, rid.heap_page_id,
              DeleteRedoBody(rid.slot_id).encode());
    page->invalidateSlot(rid.slot_id);
    pool.unpinPage(page, table.heapFile());
    ++removed_count;
  }

  return removed_count;
}

void removeByKey(BufferPool& pool, Table& table, int key, WAL& wal) {
  const std::size_t removed_count = removeMatchingRows(
      pool, table,
      std::vector<ComparisonPredicate>{ComparisonPredicate{
          static_cast<std::size_t>(table.schema().getColumnIndex(
              table.indexedColumnName().value())),
          ComparisonPredicate::Op::Eq, key}},
      wal);
  if (removed_count == 0) {
    throw std::runtime_error("Key " + std::to_string(key) +
                             " not found in leaf page.");
  }

  LOG_INFO("Removed record with key {} successfully.", key);
}

void insertRow(BufferPool& pool, Table& table, const TypedRow& row, WAL& wal) {
  const int key = extractAccessKey(table, row);
  File& index_file = requireIndexFile(table);
  LOG_INFO("Inserting record with key {} into table {}.", key, table.name());
  std::optional<RID> existing_rid =
      BTreeCursor::findRID(pool, index_file, key);
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

  BTreeCursor::insertIntoIndex(pool, index_file, key,
                               static_cast<uint16_t>(target_page_id),
                               static_cast<uint16_t>(inserted_slot_id.value()));
}

std::unique_ptr<Operator> buildReadSource(
    BufferPool& pool, Table& table,
    const std::vector<ComparisonPredicate>& predicates) {
  const std::optional<std::string>& indexed_column_name =
      table.indexedColumnName();
  if (!indexed_column_name.has_value()) {
    return std::make_unique<SeqScanOperator>(pool, table.heapFile(),
                                             table.schema());
  }

  const int indexed_column_index =
      table.schema().getColumnIndex(indexed_column_name.value());
  if (indexed_column_index < 0) {
    return std::make_unique<SeqScanOperator>(pool, table.heapFile(),
                                             table.schema());
  }

  std::vector<ComparisonPredicate> index_predicates = extractIndexedPredicates(
      predicates, static_cast<std::size_t>(indexed_column_index));
  if (index_predicates.empty()) {
    return std::make_unique<SeqScanOperator>(pool, table.heapFile(),
                                             table.schema());
  }

  const auto index_file = table.indexFile();
  if (!index_file.has_value()) {
    return std::make_unique<SeqScanOperator>(pool, table.heapFile(),
                                             table.schema());
  }

  auto scan = std::make_unique<IndexScanOperator>(
      pool, index_file->get(),
      DiscreteIntegerIndexPredicates{std::move(index_predicates)});
  return std::make_unique<HeapFetchOperator>(std::move(scan), pool,
                                             table.heapFile(), table.schema());
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
  File& index_file = requireIndexFile(table);
  std::optional<RID> rid = BTreeCursor::findRID(pool, index_file, key);
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

std::vector<TypedRow> executor::read(BufferPool& pool,
                                     const SelectParser& parser) {
  Table table = Table::getTable(parser.extractTableName());
  std::vector<std::size_t> projection_indices =
      parser.extractProjectionIndices(table.schema());
  std::vector<OrderBySpec> order_by_specs =
      parser.extractOrderBySpecs(table.schema());
  std::optional<std::size_t> limit_count = parser.extractLimitCount();
  std::vector<ComparisonPredicate> predicates =
      parser.extractComparisonPredicates(table.schema());

  std::unique_ptr<Operator> source = buildReadSource(pool, table, predicates);
  std::unique_ptr<Operator> pipeline =
      std::make_unique<FilterOperator>(std::move(source), predicates);
  if (!order_by_specs.empty()) {
    pipeline = std::make_unique<OrderByOperator>(std::move(pipeline),
                                                 std::move(order_by_specs));
  }
  if (limit_count.has_value()) {
    pipeline = std::make_unique<LimitOperator>(std::move(pipeline),
                                               limit_count.value());
  }
  ProjectionOperator projection(std::move(pipeline), projection_indices);
  projection.open();

  std::vector<TypedRow> rows;
  while (std::optional<TypedRow> row = projection.next()) {
    rows.push_back(*row);
  }
  projection.close();
  return rows;
}

void executor::create_table(const CreateTableParser& parser) {
  Table::initialize(parser.extractTableName(),
                    parser.extractSchema());
}

void executor::create_index(const CreateIndexParser& parser) {
  const std::vector<std::string> column_names = parser.extractColumnNames();
  if (column_names.empty()) {
    throw std::runtime_error("CREATE INDEX requires at least one index column.");
  }

  Table table = Table::getTable(parser.extractTableName());
  // TODO: currently only supports single-column indexes.
  table.createIndex(column_names.front());
}

void executor::drop_table(const DropTableParser& parser) {
  Table::removeBackingFilesFor(parser.extractTableName());
}

void executor::insert(BufferPool& pool, Table& table, const InsertParser& parser,
                      WAL& wal) {
  const TypedRow row = parser.extractRow();
  insertRow(pool, table, row, wal);
}

void executor::remove(BufferPool& pool, Table& table, const DeleteParser& parser,
                      WAL& wal) {
  const std::vector<ComparisonPredicate> predicates =
      parser.extractComparisonPredicates(table.schema());
  removeMatchingRows(pool, table, predicates, wal);
}

/**
 * We first design updates to be idempotent by modeling them as a remove
 * followed by an insert. This is not necessarily the most efficient strategy,
 * but it keeps the update path simple and robust and fasten the development.
 * Also, this unlocks follow-on benefits (e.g., easier recovery/retry and
 * fewer page-structure assumptions) without requiring in-place updates or
 * special-case split handling.
 */
void executor::update(BufferPool& pool, Table& table, const UpdateParser& parser, WAL& wal) {
  const int key = parser.extractTargetKey(table.schema());
  TypedRow original_row = executor::read(pool, table, key);
  TypedRow updated_row = parser.extractUpdatedRow(table.schema(), original_row);
  removeByKey(pool, table, key, wal);
  insertRow(pool, table, updated_row, wal);
}
