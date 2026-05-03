#include "executor.h"

#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

#include "../logging.h"
#include "execution/operators/filter_operator.h"
#include "execution/operators/aggregate_operator.h"
#include "execution/binder.h"
#include "execution/index_lookup_planner.h"
#include "execution/operators/heap_fetch_operator.h"
#include "execution/operators/index_scan_operator.h"
#include "execution/operators/limit_operator.h"
#include "execution/operator.h"
#include "execution/operators/orderby_operator.h"
#include "execution/operators/projection_operator.h"
#include "execution/operators/rid_operator.h"
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
#include "storage/index/index_key.h"
#include "storage/page/cell.h"
#include "storage/page/page.h"
#include "storage/record/heap_file_access.h"
#include "storage/record/record_cell.h"
#include "storage/record/record_serializer.h"
#include "storage/runtime/bufferpool.h"
#include "storage/wal/wal.h"
#include "storage/wal/wal_body.h"
#include "storage/wal/wal_record.h"
#include "catalog/table.h"

namespace {

template <typename Item, typename Source>
std::vector<Item> collectItems(Source& source) {
  source.open();

  std::vector<Item> items;
  while (std::optional<Item> item = source.next()) {
    items.push_back(*item);
  }

  source.close();
  return items;
}

/**
 * Collect RIDs of records narrowed by the given predicates.
 */
std::vector<RID> collectRidsNarrowedByPredicates(
    BufferPool& pool, Table& table,
    const std::vector<UnboundComparisonPredicate>& predicates) {
  IndexLookupPlan plan = IndexLookupPlanner::plan(table, predicates);
  if (!plan.canUseIndex()) {
    return heap_file_access::collectRids(pool, table.heapFile());
  }

  IndexScanOperator scan(pool, table.requireIndexFile(),
                         std::move(plan.encoded_keys));
  return collectItems<RID>(scan);
}

std::size_t removeMatchingRows(BufferPool& pool, Table& table,
                               const std::vector<UnboundComparisonPredicate>& predicates,
                               WAL& wal) {
  const std::vector<RID> rids =
      collectRidsNarrowedByPredicates(pool, table, predicates);
  const std::vector<BoundComparisonPredicate> bound_predicates =
      binder::bindPredicates(predicates, {table});
  std::size_t removed_count = 0;

  for (const RID& rid : rids) {
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
      const std::string key = table.extractIndexKey(row);
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

void insertRow(BufferPool& pool, Table& table, const TypedRow& row, WAL& wal) {
  const auto index_file = table.indexFile();
  std::optional<std::string> key;
  if (index_file.has_value()) {
    key = table.extractIndexKey(row);
    LOG_INFO("Inserting record with key {} into table {}.",
             index_key::formatForDebug(key.value()),
             table.name());
    std::optional<RID> existing_rid =
        BTreeCursor::findRID(pool, index_file->get(), key.value());
    if (existing_rid.has_value()) {
      throw std::runtime_error(
          "Duplicate key is not allowed for indexed table: " + table.name());
    }
  } else {
    LOG_INFO("Inserting record into table {}.", table.name());
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

  if (index_file.has_value()) {
    BTreeCursor::insertIntoIndex(pool, index_file->get(), key.value(),
                                 static_cast<uint16_t>(target_page_id),
                                 static_cast<uint16_t>(inserted_slot_id.value()));
  }
}

std::size_t updateMatchingRows(BufferPool& pool, Table& table,
                               const std::vector<UnboundComparisonPredicate>& predicates,
                               const UpdateParser& parser, WAL& wal) {
  const std::vector<RID> rids =
      collectRidsNarrowedByPredicates(pool, table, predicates);
  const std::vector<BoundComparisonPredicate> bound_predicates =
      binder::bindPredicates(predicates, {table});
  std::vector<TypedRow> updated_rows;

  for (const RID& rid : rids) {
    Page* page = pool.pinPage(rid.heap_page_id, table.heapFile());
    char* cell_start = page->slotCellStartUnchecked(rid.slot_id);
    if (!Cell::isValid(cell_start)) {
      pool.unpinPage(page, table.heapFile());
      continue;
    }

    TypedRow original_row = RecordCellView(cell_start).getTypedRow(table.schema());
    pool.unpinPage(page, table.heapFile());
    if (!matchesPredicates(original_row, bound_predicates)) {
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

std::unique_ptr<Operator> buildReadSource(
    BufferPool& pool, Table& table,
    const std::vector<UnboundComparisonPredicate>& predicates) {
  IndexLookupPlan plan = IndexLookupPlanner::plan(table, predicates);
  if (!plan.canUseIndex()) {
    return std::make_unique<SeqScanOperator>(pool, table.heapFile(),
                                             table.schema());
  }

  auto scan = std::make_unique<IndexScanOperator>(
      pool, table.requireIndexFile(), std::move(plan.encoded_keys));
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
    const std::vector<UnboundSelectItem> select_items =
      parser.extractSelectItems();

  std::vector<std::unique_ptr<Operator>> sources;
  for (auto& table : tables) {
    std::unique_ptr<Operator> source = buildReadSource(pool, table, predicates);
    sources.push_back(std::move(source));
  }
  
  // build bound predicates
  const std::vector<BoundComparisonPredicate> bound_predicates =
      binder::bindPredicates(predicates, tables);

  const std::vector<BoundSelectItem> bound_select_items =
      binder::bindSelectItems(select_items, tables);
  bool has_aggregate = false;
  bool has_projection = false;
  for (const auto& item : bound_select_items) {
    has_aggregate = has_aggregate || std::holds_alternative<BoundAggregateCall>(item);
    has_projection = has_projection || std::holds_alternative<BoundColumnRef>(item);
  }
  if (has_aggregate && has_projection) {
    throw std::runtime_error(
        "Mixing aggregate and non-aggregate select items is not supported.");
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

  if (has_aggregate) {
    std::vector<OrderBySpec> order_by_specs =
        parser.extractOrderBySpecs(joined_schema);
    if (!order_by_specs.empty()) {
      throw std::runtime_error("ORDER BY is not supported for aggregate queries.");
    }

    pipeline = std::make_unique<AggregateOperator>(
      std::move(pipeline),
      select_item::extractAggregateCalls(bound_select_items));

    std::optional<std::size_t> limit_count = parser.extractLimitCount();
    if (limit_count.has_value()) {
      pipeline = std::make_unique<LimitOperator>(std::move(pipeline),
                                                limit_count.value());
    }

    return collectItems<TypedRow>(*pipeline);
  }

  // order by
  std::vector<OrderBySpec> order_by_specs =
      parser.extractOrderBySpecs(joined_schema);
  if (!order_by_specs.empty()) {
    pipeline = std::make_unique<OrderByOperator>(std::move(pipeline),
                                                std::move(order_by_specs));
  }

  // limit
  std::optional<std::size_t> limit_count = parser.extractLimitCount();
  if (limit_count.has_value()) {
    pipeline = std::make_unique<LimitOperator>(std::move(pipeline),
                                              limit_count.value());
  }
  
  // projection
  std::vector<std::size_t> projection_indices =
      select_item::extractProjectionIndices(bound_select_items);
  ProjectionOperator projection(std::move(pipeline), projection_indices);
  return collectItems<TypedRow>(projection);
}

void executor::create_table(const CreateTableParser& parser) {
  Table table = Table::initialize(parser.extractTableName(),
                                  parser.extractSchema());

  const std::vector<std::string> primary_key_columns =
      parser.extractPrimaryKeyColumnNames();
  if (!primary_key_columns.empty()) {
    table.createIndex(primary_key_columns);
  }
}

void executor::create_index(const CreateIndexParser& parser) {
  const std::vector<std::string> column_names = parser.extractColumnNames();
  if (column_names.empty()) {
    throw std::runtime_error("CREATE INDEX requires at least one index column.");
  }

  Table table = Table::getTable(parser.extractTableName());
  table.createIndex(column_names);
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
