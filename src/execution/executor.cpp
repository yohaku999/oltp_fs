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
#include "execution/operators/loop_join_operator.h"
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
  const std::optional<std::size_t> indexed_column_index =
      table.indexedColumnIndex();
  if (!table.indexedColumnName().has_value()) {
    throw std::runtime_error("Table has no configured indexed column: " +
                             table.name());
  }
  const std::string& column_name = table.indexedColumnName().value();
  if (!indexed_column_index.has_value()) {
    throw std::runtime_error("Access index column not found in schema: " +
                             column_name);
  }
  if (row.values.size() <= indexed_column_index.value()) {
    throw std::runtime_error("Row does not contain access-index column: " +
                             column_name);
  }
  if (!std::holds_alternative<Column::IntegerType>(
          row.values[indexed_column_index.value()])) {
    throw std::runtime_error("Access-index column must be Integer: " +
                             column_name);
  }
  return std::get<Column::IntegerType>(row.values[indexed_column_index.value()]);
}

std::vector<UnboundComparisonPredicate> extractIndexedPredicates(
    const Table& table,
    const std::vector<UnboundComparisonPredicate>& predicates,
    std::size_t indexed_column_index) {
  std::vector<UnboundComparisonPredicate> index_predicates;
  const std::string& indexed_column_name =
      table.schema().columns().at(indexed_column_index).getName();
  for (const auto& predicate : predicates) {
    // index predicate must be in the form of "column op value" or "value op column",
    // where column is the indexed column and value is a constant value for now.
    // TODO: change this limit when implementing hash join.
    const auto* column_ref = std::get_if<ColumnRef>(&predicate.left);
    const auto* value = std::get_if<FieldValue>(&predicate.right);
    if (column_ref == nullptr || value == nullptr) {
      column_ref = std::get_if<ColumnRef>(&predicate.right);
      value = std::get_if<FieldValue>(&predicate.left);
    }
    if (column_ref == nullptr || value == nullptr) {
      continue;
    }
    if (column_ref->column_name != indexed_column_name) {
      continue;
    }
    index_predicates.push_back(predicate);
  }
  return index_predicates;
}

bool canUseIndexForDmlCandidateCollection(
    const std::vector<UnboundComparisonPredicate>& index_predicates) {
  if (index_predicates.empty()) {
    return false;
  }

  // TODO: currently we suppot only equality predicates for indexed column. We need to update range scan for index search.
  for (const auto& predicate : index_predicates) {
    if (predicate.op != Op::Eq) {
      return false;
    }
  }

  return true;
}

BoundOperand bindOperandForSingleTable(const UnboundOperand& operand,
                                       const Table& table) {
  if (const auto* column_ref = std::get_if<ColumnRef>(&operand)) {
    if (!column_ref->table_name.empty() &&
        column_ref->table_name != table.name()) {
      throw std::runtime_error("Predicate references another table: " +
                               column_ref->table_name);
    }

    const int column_index =
        table.schema().getColumnIndex(column_ref->column_name);
    if (column_index < 0) {
      throw std::runtime_error("Unknown predicate column: " +
                               column_ref->column_name);
    }

    return BoundColumnRef{0, static_cast<std::size_t>(column_index)};
  }

  if (const auto* value = std::get_if<FieldValue>(&operand)) {
    return *value;
  }

  return std::monostate{};
}

std::vector<BoundComparisonPredicate> bindPredicatesForSingleTable(
    const std::vector<UnboundComparisonPredicate>& predicates,
    const Table& table) {
  std::vector<BoundComparisonPredicate> bound_predicates;
  bound_predicates.reserve(predicates.size());

  for (const auto& predicate : predicates) {
    bound_predicates.push_back(BoundComparisonPredicate{
        predicate.op, bindOperandForSingleTable(predicate.left, table),
        bindOperandForSingleTable(predicate.right, table)});
  }

  return bound_predicates;
}

BoundOperand bindOperandForJoinedTables(const UnboundOperand& operand,
                                        const std::vector<Table>& tables) {
  if (const auto* column_ref = std::get_if<ColumnRef>(&operand)) {
    std::size_t joined_column_offset = 0;
    const Table* matched_table = nullptr;

    for (const auto& table : tables) {
      if (!column_ref->table_name.empty() && column_ref->table_name != table.name()) {
        joined_column_offset += table.schema().columns().size();
        continue;
      }

      const int column_index = table.schema().getColumnIndex(column_ref->column_name);
      if (column_index < 0) {
        joined_column_offset += table.schema().columns().size();
        continue;
      }

      if (matched_table != nullptr && column_ref->table_name.empty()) {
        throw std::runtime_error("Ambiguous joined predicate column: " +
                                 column_ref->column_name);
      }

      matched_table = &table;
      return BoundColumnRef{0, joined_column_offset +
                                   static_cast<std::size_t>(column_index)};
    }

    if (!column_ref->table_name.empty()) {
      throw std::runtime_error("Unknown predicate column on table " +
                               column_ref->table_name + ": " +
                               column_ref->column_name);
    }
    throw std::runtime_error("Unknown joined predicate column: " +
                             column_ref->column_name);
  }

  if (const auto* value = std::get_if<FieldValue>(&operand)) {
    return *value;
  }

  return std::monostate{};
}

bool matchesPredicates(const TypedRow& row,
                       const std::vector<BoundComparisonPredicate>& predicates) {
  for (const auto& predicate : predicates) {
    const FieldValue left = resolveBoundOperand(predicate.left, row);
    const FieldValue right = resolveBoundOperand(predicate.right, row);
    switch (predicate.op) {
      case Op::Eq:
        if (left != right) {
          return false;
        }
        break;
      case Op::Gt:
        if (left <= right) {
          return false;
        }
        break;
      case Op::Ge:
        if (left < right) {
          return false;
        }
        break;
      case Op::Lt:
        if (left >= right) {
          return false;
        }
        break;
      case Op::Le:
        if (left > right) {
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

void insertRow(BufferPool& pool, Table& table, const TypedRow& row, WAL& wal);

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
    const std::vector<UnboundComparisonPredicate>& predicates) {
  const std::optional<std::size_t> indexed_column_index =
      table.indexedColumnIndex();
  if (!indexed_column_index.has_value()) {
    return collectHeapRids(pool, table.heapFile());
  }

  std::vector<UnboundComparisonPredicate> index_predicates = extractIndexedPredicates(
      table, predicates, indexed_column_index.value());
  if (!canUseIndexForDmlCandidateCollection(index_predicates)) {
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
                               const std::vector<UnboundComparisonPredicate>& predicates,
                               WAL& wal) {
  const std::vector<RID> candidate_rids =
      collectCandidateRids(pool, table, predicates);
  const std::vector<BoundComparisonPredicate> bound_predicates =
    bindPredicatesForSingleTable(predicates, table);
  std::size_t removed_count = 0;

  for (const RID& rid : candidate_rids) {
    Page* page = pool.pinPage(rid.heap_page_id, table.heapFile());
    char* cell_start = page->slotCellStartUnchecked(rid.slot_id);
    if (!Cell::isValid(cell_start)) {
      pool.unpinPage(page, table.heapFile());
      continue;
    }

    TypedRow row = RecordCellView(cell_start).getTypedRow(table.schema());
    if (!matchesPredicates(row, bound_predicates)) {
      pool.unpinPage(page, table.heapFile());
      continue;
    }

    const auto index_file = table.indexFile();
    if (index_file.has_value()) {
      const int key = extractAccessKey(table, row);
      BTreeCursor::findRID(pool, index_file->get(), key, true);
    }

    wal.write(WALRecord::RecordType::DELETE, rid.heap_page_id,
              DeleteRedoBody(rid.slot_id).encode());
    page->invalidateSlot(rid.slot_id);
    pool.unpinPage(page, table.heapFile());
    ++removed_count;
  }

  return removed_count;
}

std::size_t updateMatchingRows(BufferPool& pool, Table& table,
                               const std::vector<UnboundComparisonPredicate>& predicates,
                               const UpdateParser& parser, WAL& wal) {
  const std::vector<RID> candidate_rids =
      collectCandidateRids(pool, table, predicates);
  std::vector<TypedRow> updated_rows;

  for (const RID& rid : candidate_rids) {
    Page* page = pool.pinPage(rid.heap_page_id, table.heapFile());
    char* cell_start = page->slotCellStartUnchecked(rid.slot_id);
    if (!Cell::isValid(cell_start)) {
      pool.unpinPage(page, table.heapFile());
      continue;
    }

    TypedRow original_row = RecordCellView(cell_start).getTypedRow(table.schema());
    pool.unpinPage(page, table.heapFile());
    if (!matchesPredicates(original_row, bindPredicatesForSingleTable(predicates, table))) {
      continue;
    }

    updated_rows.push_back(parser.extractUpdatedRow(table.schema(), original_row));
  }

  const std::size_t removed_count = removeMatchingRows(pool, table, predicates, wal);
  for (const TypedRow& updated_row : updated_rows) {
    insertRow(pool, table, updated_row, wal);
  }

  return removed_count;
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
    const std::vector<UnboundComparisonPredicate>& index_predicates) {

  // sequential scan
  if (index_predicates.empty()) {
    return std::make_unique<SeqScanOperator>(pool, table.heapFile(),
                                             table.schema());
  }

  // index scan
  const auto index_file = table.indexFile();
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
std::vector<TypedRow> executor::read(BufferPool& pool,
                                     const SelectParser& parser) {
  std::vector<std::string> table_names = parser.extractTableNames();
  std::vector<Table> tables;
  tables.reserve(table_names.size());
  for (const auto& table_name : table_names) {
    tables.push_back(Table::getTable(table_name));
  }

  // build joined schema
  std::vector<Column> columns;
  for (const auto& table : tables) {
    const auto& table_columns = table.schema().columns();
    columns.insert(columns.end(), table_columns.begin(), table_columns.end());
  }
  const Schema joined_schema(columns);

  const std::vector<UnboundComparisonPredicate> predicates =
      parser.extractComparisonPredicates(joined_schema);

  std::vector<std::unique_ptr<Operator>> sources;
  for (auto& table : tables) {
    std::unique_ptr<Operator> source;
    if (const std::optional<std::size_t> indexed_column_index =
            table.indexedColumnIndex();
        indexed_column_index.has_value()) {
      std::vector<UnboundComparisonPredicate> index_predicates;
      index_predicates = extractIndexedPredicates(
          table, predicates, indexed_column_index.value());
      source = buildReadSource(pool, table, index_predicates);
    } else {
      source = buildReadSource(pool, table, {});
    }
    sources.push_back(std::move(source));
  }
  
  // build bound predicates
  std::vector<BoundComparisonPredicate> bound_predicates;
  bound_predicates.reserve(predicates.size());

  for (const auto& predicate : predicates) {
    bound_predicates.push_back(BoundComparisonPredicate{
        predicate.op, bindOperandForJoinedTables(predicate.left, tables),
        bindOperandForJoinedTables(predicate.right, tables)});
  }

  // join
  std::unique_ptr<Operator> pipeline;
  if (sources.size() == 1) {
    pipeline = std::move(sources.front());
  } else {
    pipeline = std::make_unique<LoopJoinOperator>(std::move(sources));
  }

  // filter
  pipeline = std::make_unique<FilterOperator>(std::move(pipeline),
                                              bound_predicates);
  // order by
  std::vector<OrderBySpec> order_by_specs =
      parser.extractOrderBySpecs(joined_schema);
  if (!order_by_specs.empty()) {
    pipeline = std::make_unique<OrderByOperator>(std::move(pipeline),
                                                std::move(order_by_specs));
  }
  std::optional<std::size_t> limit_count = parser.extractLimitCount();
  // limit
  if (limit_count.has_value()) {
    pipeline = std::make_unique<LimitOperator>(std::move(pipeline),
                                              limit_count.value());
  }
  // projection
  std::vector<std::size_t> projection_indices =
      parser.extractProjectionIndices(joined_schema);
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
  const std::vector<UnboundComparisonPredicate> predicates =
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
  const std::vector<UnboundComparisonPredicate> predicates =
      parser.extractComparisonPredicates(table.schema());
  const std::size_t updated_count =
      updateMatchingRows(pool, table, predicates, parser, wal);
  if (updated_count == 0) {
    throw std::runtime_error("UPDATE matched no rows.");
  }
}
