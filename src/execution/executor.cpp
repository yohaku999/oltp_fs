#include "executor.h"

#include <algorithm>
#include <array>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "../logging.h"
#include "catalog/table.h"
#include "storage/heap/heapfile.h"
#include "execution/join_pipeline_builder.h"
#include "execution/operator.h"
#include "execution/operators/aggregate_operator.h"
#include "execution/operators/filter_operator.h"
#include "execution/operators/heap_fetch_operator.h"
#include "execution/operators/index_scan_operator.h"
#include "execution/operators/limit_operator.h"
#include "execution/operators/orderby_operator.h"
#include "execution/operators/projection_operator.h"
#include "execution/operators/seq_scan_operator.h"
#include "parser/create_index_parser.h"
#include "parser/create_table_parser.h"
#include "parser/delete_parser.h"
#include "parser/drop_table_parser.h"
#include "parser/insert_parser.h"
#include "parser/select_parser.h"
#include "parser/update_parser.h"
#include "planning/binder.h"
#include "planning/physical/join_planner.h"
#include "storage/buffer/bufferpool.h"
#include "storage/disk/pagedfile.h"
#include "storage/index/btreecursor.h"
#include "storage/index/index_key.h"
#include "storage/local_storage_manager.h"
#include "storage/page/cell.h"
#include "storage/record/record_cell.h"
#include "storage/record/record_serializer.h"
#include "storage/table_metadata_store.h"

namespace {

struct PredicateValueAndType {
  const FieldValue& value;
  Column::Type column_type;
};

PredicateValueAndType extractValueAndType(
    const BoundComparisonPredicate& predicate) {
  if (const auto* left_column =
          std::get_if<BoundColumnRef>(&predicate.left)) {
    return {std::get<FieldValue>(predicate.right), left_column->type};
  }
  const auto& right_column = std::get<BoundColumnRef>(predicate.right);
  return {std::get<FieldValue>(predicate.left), right_column.type};
}

const BoundComparisonPredicate* findPredicateByOp(
    const std::vector<BoundComparisonPredicate>& predicates, Op op) {
  const auto it = std::find_if(
      predicates.begin(), predicates.end(),
      [op](const BoundComparisonPredicate& predicate) {
        return predicate.op == op;
      });
  return it == predicates.end() ? nullptr : &*it;
}

void appendBoundaryValue(BTreeCursor::Boundary& boundary,
                         const BoundComparisonPredicate& predicate,
                         bool is_inclusive) {
  const auto [value, column_type] = extractValueAndType(predicate);
  boundary.composite_key +=
      index_key::encodeFieldValue(value, column_type);
  boundary.is_inclusive = is_inclusive;
}

std::pair<BTreeCursor::Boundary, BTreeCursor::Boundary>
buildTraversalBoundaries(
    const std::vector<std::vector<BoundComparisonPredicate>>&
        ordered_predicates) {
  BTreeCursor::Boundary left_boundary{"", true};
  BTreeCursor::Boundary right_boundary{"", true};

  for (size_t i = 0; i < ordered_predicates.size(); ++i) {
    const auto& predicates_for_key = ordered_predicates[i];
    if (predicates_for_key.empty()) {
      // If a non-leading key has no predicate, later keys cannot narrow the
      // traversal boundary.
      break;
    }

    const auto* eq_predicate = findPredicateByOp(predicates_for_key, Op::Eq);
    if (eq_predicate != nullptr) {
      appendBoundaryValue(left_boundary, *eq_predicate, true);
      appendBoundaryValue(right_boundary, *eq_predicate, true);
      continue;
    }

    for (const auto& [op, boundary, is_inclusive] :
         std::array<std::tuple<Op, BTreeCursor::Boundary*, bool>, 4>{
             {{Op::Gt, &left_boundary, false},
              {Op::Ge, &left_boundary, true},
              {Op::Lt, &right_boundary, false},
              {Op::Le, &right_boundary, true}}}) {
      const auto* predicate = findPredicateByOp(predicates_for_key, op);
      if (predicate != nullptr) {
        appendBoundaryValue(*boundary, *predicate, is_inclusive);
        break;
      }
    }
  }

  return {left_boundary, right_boundary};
}

FieldValue evaluateBoundUpdateValue(const BoundUpdateValue& value,
                                    const TypedRow& original_row,
                                    const Schema& schema,
                                    std::size_t target_column_index) {
  if (const auto* literal = std::get_if<FieldValue>(&value)) {
    return *literal;
  }

  const auto& arithmetic = std::get<BoundSelfArithmeticUpdate>(value);
  const FieldValue& original_value =
      original_row.values.at(arithmetic.target_column_index);
  const Column::Type target_type =
      schema.columns().at(target_column_index).getType();

  if (target_type == Column::Type::Integer) {
    const int lhs = std::get<Column::IntegerType>(original_value);
    const int rhs = std::get<Column::IntegerType>(arithmetic.literal);
    if (arithmetic.op == UpdateBinaryOperator::Add) {
      return lhs + rhs;
    }
    return lhs - rhs;
  }

  if (target_type == Column::Type::Double) {
    const double lhs =
        std::holds_alternative<Column::DoubleType>(original_value)
            ? std::get<Column::DoubleType>(original_value)
            : static_cast<double>(
                  std::get<Column::IntegerType>(original_value));
    const double rhs =
        std::holds_alternative<Column::DoubleType>(arithmetic.literal)
            ? std::get<Column::DoubleType>(arithmetic.literal)
            : static_cast<double>(
                  std::get<Column::IntegerType>(arithmetic.literal));
    if (arithmetic.op == UpdateBinaryOperator::Add) {
      return lhs + rhs;
    }
    return lhs - rhs;
  }

  throw std::runtime_error("Unsupported UPDATE arithmetic target type.");
}

TypedRow applyUpdateAssignments(
    const TypedRow& original_row,
    const std::vector<BoundUpdateAssignment>& assignments,
    const Schema& schema) {
  TypedRow updated_row = original_row;
  for (const auto& assignment : assignments) {
    updated_row.values[assignment.target_column_index] =
        evaluateBoundUpdateValue(assignment.value, original_row, schema,
                                 assignment.target_column_index);
  }
  return updated_row;
}

std::optional<TypedRow> readTypedRow(BufferPool& pool, const Table& table,
                                     HeapFile& heap, const RID& rid) {
  return heap.withRecord(
      pool, rid, [&](RecordCellView record) {
        return record.getTypedRow(table.schema());
      });
}

std::vector<PagedFile> openIndexFiles(const LocalStorageManager& storage,
                                      const Table& table) {
  std::vector<PagedFile> files;
  files.reserve(table.indexes().size());
  for (const IndexMetadata& index : table.indexes()) {
    files.push_back(storage.openIndex(index.file_name));
  }
  return files;
}

std::vector<std::string> encodeIndexKeys(const Table& table,
                                         const TypedRow& row) {
  std::vector<std::string> keys;
  keys.reserve(table.indexes().size());
  for (const IndexMetadata& index : table.indexes()) {
    keys.push_back(index_key::encodeRow(
        table.schema(), row, table.indexedColumnIndexes(index)));
  }
  return keys;
}

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
 * Prepare predicates for index scan.
 * Filter the given predicates to keep only those that can be used for index
 * scan and group them by their corresponding index key order.
 */
std::vector<std::vector<BoundComparisonPredicate>> prepareIndexKeyPredicates(
    const std::vector<BoundComparisonPredicate>& predicates,
    const std::vector<std::size_t>& key_order_indexes) {
  std::vector<std::vector<BoundComparisonPredicate>> ordered_predicates(
      key_order_indexes.size());

  for (const auto& predicate : predicates) {
    // only key value predicates can be used for index scan.
    const auto* left_column = std::get_if<BoundColumnRef>(&predicate.left);
    const auto* right_column = std::get_if<BoundColumnRef>(&predicate.right);
    const bool has_column_value_pair =
        (left_column != nullptr) != (right_column != nullptr);
    if (!has_column_value_pair) {
      continue;
    }

    const BoundColumnRef* column_ref =
        left_column != nullptr ? left_column : right_column;
    const auto it =
        std::find(key_order_indexes.begin(), key_order_indexes.end(),
                  column_ref->column_index);
    if (it == key_order_indexes.end()) {
      continue;
    }

    const std::size_t key_index =
        static_cast<std::size_t>(std::distance(key_order_indexes.begin(), it));
    BoundComparisonPredicate index_predicate = predicate;
    if (auto* index_column =
            std::get_if<BoundColumnRef>(&index_predicate.left)) {
      index_column->column_index = key_index;
    } else {
      std::get<BoundColumnRef>(index_predicate.right).column_index = key_index;
    }
    ordered_predicates[key_index].push_back(std::move(index_predicate));
  }

  return ordered_predicates;
}

struct IndexLookupPlan {
  std::size_t index_position;
  std::vector<std::vector<BoundComparisonPredicate>> ordered_predicates;
};

std::optional<IndexLookupPlan> planIndexLookup(
    const Table& table,
    const std::vector<BoundComparisonPredicate>& predicates) {
  std::optional<IndexLookupPlan> best_plan;
  std::size_t best_prefix_length = 0;
  for (std::size_t index_position = 0;
       index_position < table.indexes().size(); ++index_position) {
    const IndexMetadata& index = table.indexes()[index_position];
    std::vector<std::vector<BoundComparisonPredicate>> ordered_predicates =
        prepareIndexKeyPredicates(predicates,
                                  table.indexedColumnIndexes(index));
    std::size_t prefix_length = 0;
    while (prefix_length < ordered_predicates.size() &&
           !ordered_predicates[prefix_length].empty()) {
      ++prefix_length;
    }
    if (prefix_length > best_prefix_length) {
      best_prefix_length = prefix_length;
      best_plan =
          IndexLookupPlan{index_position, std::move(ordered_predicates)};
    }
  }
  return best_plan;
}

void appendInsertLog(BufferPool& pool, const RID& rid,
                     const RecordCell& cell) {
  pool.wal().write(
      WALRecord::RecordType::INSERT, rid.heap_page_id,
      InsertRedoBody(rid.slot_id, cell.serializedBytes()).encode());
}

void appendDeleteLog(BufferPool& pool, const RID& rid,
                     const RecordCell& before) {
  pool.wal().write(
      WALRecord::RecordType::DELETE, rid.heap_page_id,
      DeleteRedoBody(rid.slot_id, before.serializedBytes()).encode());
}

void appendUpdateLog(BufferPool& pool, const RID& rid,
                     const RecordCell& before,
                     const RecordCell& after) {
  UpdateRedoBody update_body{};
  update_body.offset = rid.slot_id;
  update_body.before = before.serializedBytes();
  update_body.after = after.serializedBytes();
  pool.wal().write(WALRecord::RecordType::UPDATE, rid.heap_page_id,
                   update_body.encode());
}

/**
 * Collect RIDs of records narrowed by the given predicates.
 */
std::vector<RID> collectRidsNarrowedByPredicates(
    BufferPool& pool, const LocalStorageManager& storage, const Table& table,
    HeapFile& heap,
    const std::vector<UnboundComparisonPredicate>& predicates) {
  const std::vector<BoundComparisonPredicate> bound_predicates =
      binder::bindPredicatesResolvableByTable(predicates, table);
  std::optional<IndexLookupPlan> index_plan =
      planIndexLookup(table, bound_predicates);
  if (!index_plan.has_value()) {
    dbfs_log::execution().debug(
        "Building sequential scan operator for table {} because index scan "
        "prerequisites are not met.",
        table.name());
    return heap.collectRids(pool);
  }

  const IndexMetadata& index =
      table.indexes().at(index_plan->index_position);
  IndexScanOperator scan(pool, storage.openIndex(index.file_name),
                         buildTraversalBoundaries(
                             index_plan->ordered_predicates),
                         std::move(index_plan->ordered_predicates));
  return collectItems<RID>(scan);
}

template <typename OnRecordRelocated>
void updateMatchingRows(
    BufferPool& pool, const Table& table, HeapFile& heap,
    const std::vector<RID>& rids,
    const std::vector<UnboundComparisonPredicate>& predicates,
    const UpdateParser& parser, OnRecordRelocated on_record_relocated) {
  const std::vector<BoundUpdateAssignment> bound_assignments =
      binder::bindUpdateAssignments(parser.extractAssignments(table.schema()),
                                    table);
  std::vector<std::size_t> indexed_column_indexes;
  for (const IndexMetadata& index : table.indexes()) {
    const std::vector<std::size_t> column_indexes =
        table.indexedColumnIndexes(index);
    indexed_column_indexes.insert(indexed_column_indexes.end(),
                                  column_indexes.begin(),
                                  column_indexes.end());
  }
  for (const BoundUpdateAssignment& assignment : bound_assignments) {
    if (std::find(indexed_column_indexes.begin(), indexed_column_indexes.end(),
                  assignment.target_column_index) !=
        indexed_column_indexes.end()) {
      throw std::runtime_error("Updating indexed columns is not supported: " +
                               table.name());
    }
  }

  const std::vector<BoundComparisonPredicate> bound_predicates =
      binder::bindPredicates(predicates, {table});
  for (const RID& rid : rids) {
    std::optional<TypedRow> original_row =
        readTypedRow(pool, table, heap, rid);
    if (!original_row.has_value()) {
      continue;
    }

    if (!passesPredicates(*original_row, bound_predicates)) {
      continue;
    }

    const TypedRow updated_row = applyUpdateAssignments(
        *original_row, bound_assignments, table.schema());
    const RecordCell original_cell =
        RecordSerializer::serialize(table.schema(), *original_row);
    const RecordCell updated_cell =
        RecordSerializer::serialize(table.schema(), updated_row);

    const bool updated_in_place = heap.tryUpdateRecord(rid, pool, updated_cell);
    if (updated_in_place) {
      appendUpdateLog(pool, rid, original_cell, updated_cell);
      continue;
    }
    // when page overflowed
    const RID new_rid = heap.insertRecord(pool, updated_cell);
    on_record_relocated(*original_row, rid, new_rid);
    heap.invalidateRecord(pool, rid);
    appendDeleteLog(pool, rid, original_cell);
  }
}

template <typename BeforeRecordRemoval>
int removeMatchingRows(
    BufferPool& pool, const Table& table, HeapFile& heap,
    const std::vector<RID>& rids,
    const std::vector<UnboundComparisonPredicate>& predicates,
    BeforeRecordRemoval before_record_removal) {
  const std::vector<BoundComparisonPredicate> bound_predicates =
      binder::bindPredicates(predicates, {table});
  int removed_count = 0;

  for (const RID& rid : rids) {
    std::optional<TypedRow> row = readTypedRow(pool, table, heap, rid);
    if (!row.has_value()) {
      continue;
    }

    if (!passesPredicates(*row, bound_predicates)) {
      continue;
    }

    before_record_removal(*row, rid);
    const RecordCell before =
        RecordSerializer::serialize(table.schema(), *row);
    heap.invalidateRecord(pool, rid);
    appendDeleteLog(pool, rid, before);
    ++removed_count;
  }
  return removed_count;
}

std::unique_ptr<TypedRowOperator> buildReadSource(
    BufferPool& pool, const LocalStorageManager& storage, const Table& table,
    std::vector<BoundComparisonPredicate> bound_predicates) {
  std::optional<IndexLookupPlan> index_plan =
      planIndexLookup(table, bound_predicates);
  if (!index_plan.has_value()) {
    dbfs_log::execution().debug(
        "Building sequential scan operator for table {} because index scan "
        "prerequisites are not met.",
        table.name());
    return std::make_unique<SeqScanOperator>(
        pool, storage.openHeap(table.heapFileName()), table.schema(),
        bound_predicates);
  }

  auto scan = std::make_unique<IndexScanOperator>(
      pool,
      storage.openIndex(
          table.indexes().at(index_plan->index_position).file_name),
      buildTraversalBoundaries(index_plan->ordered_predicates),
      std::move(index_plan->ordered_predicates));
  return std::make_unique<HeapFetchOperator>(std::move(scan), pool,
                                             storage.openHeap(
                                                 table.heapFileName()),
                                             table.schema(),
                                             bound_predicates);
}

void createIndexForTable(const LocalStorageManager& storage, Table& table,
                         const std::vector<std::string>& column_names,
                         bool is_unique) {
  const std::string index_file_name =
      storage.indexFileNameFor(table.name(), column_names);
  Table updated = table;
  updated.addIndex(IndexMetadata{index_file_name, column_names, is_unique});

  storage.createIndex(index_file_name);
  try {
    TableMetadataStore::write(updated);
  } catch (...) {
    storage.remove(index_file_name);
    throw;
  }
  table = std::move(updated);
}

}  // namespace

/**
 * - executor now operates on Table + TypedRow rather than raw PagedFile pairs
- schema metadata is persisted in data/<table>.meta.json
- multi-column records are supported by RecordSerializer and RecordCellView
- E2E uses Table-backed fixtures
- key is still passed separately from TypedRow in executor::insert/update
 *
 *
 */
std::vector<TypedRow> executor::read(BufferPool& pool,
                                     const SelectParser& parser) {
  const LocalStorageManager storage;
  binder::BoundSelectQuery query = binder::bindSelectQuery(parser);

  std::vector<std::unique_ptr<TypedRowOperator>> sources;
  for (std::size_t table_index = 0; table_index < query.tables.size();
       ++table_index) {
    std::unique_ptr<TypedRowOperator> source =
        buildReadSource(pool, storage, query.tables[table_index],
                        query.table_predicates[table_index]);
    sources.push_back(std::move(source));
  }

  physical::JoinPlan join_plan = physical::planJoin(query);
  std::unique_ptr<TypedRowOperator> pipeline = execution::buildJoinPipeline(
      std::move(join_plan), std::move(sources), pool, storage, query);

  // filter
  pipeline =
      std::make_unique<FilterOperator>(std::move(pipeline), query.predicates);
  std::vector<TypedRow> items;
  const bool has_aggregate =
      std::any_of(query.select_items.begin(), query.select_items.end(),
                  [](const BoundSelectItem& item) {
                    return std::holds_alternative<BoundAggregateCall>(item);
                  });
  if (has_aggregate) {
    pipeline = std::make_unique<AggregateOperator>(
        std::move(pipeline),
        select_item::extractAggregateCalls(query.select_items));

    if (query.limit_count.has_value()) {
      pipeline = std::make_unique<LimitOperator>(std::move(pipeline),
                                                 query.limit_count.value());
    }
    items = collectItems<TypedRow>(*pipeline);
  } else {
    // order by
    if (!query.order_by_specs.empty()) {
      pipeline = std::make_unique<OrderByOperator>(std::move(pipeline),
                                                   query.order_by_specs);
    }

    // limit
    if (query.limit_count.has_value()) {
      pipeline = std::make_unique<LimitOperator>(std::move(pipeline),
                                                 query.limit_count.value());
    }

    // projection
    std::vector<std::size_t> projection_indices =
        select_item::extractProjectionIndices(query.select_items);
    ProjectionOperator projection(std::move(pipeline), projection_indices);
    items = collectItems<TypedRow>(projection);
  }
  dbfs_log::execution().info("Query returned {} rows.", items.size());
  return items;
}

void executor::create_table(const CreateTableParser& parser) {
  const LocalStorageManager storage;
  const std::string table_name = parser.extractTableName();
  const std::string heap_file_name = storage.heapFileNameFor(table_name);

  Table table(table_name, parser.extractSchema(), heap_file_name);
  storage.createHeap(heap_file_name);
  try {
    TableMetadataStore::write(table);
  } catch (...) {
    storage.remove(heap_file_name);
    throw;
  }

  const std::vector<std::string> primary_key_columns =
      parser.extractPrimaryKeyColumnNames();
  if (!primary_key_columns.empty()) {
    createIndexForTable(storage, table, primary_key_columns, true);
  }
}

void executor::create_index(const CreateIndexParser& parser) {
  const std::vector<std::string> column_names = parser.extractColumnNames();
  const LocalStorageManager storage;
  Table table = TableMetadataStore::read(parser.extractTableName()).value();
  createIndexForTable(storage, table, column_names, parser.isUnique());
}

void executor::drop_table(const DropTableParser& parser) {
  const LocalStorageManager storage;
  const std::string table_name = parser.extractTableName();
  std::optional<Table> table = TableMetadataStore::read(table_name);
  if (!table.has_value()) {
    if (parser.hasIfExists()) {
      return;
    }
    throw std::runtime_error("Table is not in the catalog: " + table_name);
  }
  for (const IndexMetadata& index : table->indexes()) {
    storage.remove(index.file_name);
  }
  storage.remove(table->heapFileName());
  TableMetadataStore::remove(table_name);
}

void executor::insert(BufferPool& pool, Table& table,
                      const InsertParser& parser) {
  const TypedRow row = parser.extractRow(table.schema());
  const LocalStorageManager storage;
  HeapFile heap = storage.openHeap(table.heapFileName());
  const RecordCell cell = RecordSerializer::serialize(table.schema(), row);
  std::vector<PagedFile> index_files = openIndexFiles(storage, table);
  const std::vector<std::string> index_keys = encodeIndexKeys(table, row);
  for (std::size_t index = 0; index < index_files.size(); ++index) {
    if (!table.indexes()[index].is_unique) {
      continue;
    }
    if (BTreeCursor::containsKey(pool, index_files[index],
                                 index_keys[index])) {
      throw std::runtime_error(
          "Duplicate key is not allowed for indexed table: " + table.name());
    }
  }

  const RID inserted_rid = heap.insertRecord(pool, cell);
  appendInsertLog(pool, inserted_rid, cell);
  for (std::size_t index = 0; index < index_files.size(); ++index) {
    BTreeCursor::insertIntoIndex(pool, index_files[index], index_keys[index],
                                 inserted_rid.heap_page_id,
                                 inserted_rid.slot_id);
  }
}

int executor::remove(BufferPool& pool, Table& table,
                      const DeleteParser& parser) {
  const LocalStorageManager storage;
  HeapFile heap = storage.openHeap(table.heapFileName());
  const std::vector<UnboundComparisonPredicate> predicates =
      parser.extractComparisonPredicates(table.schema());
  const std::vector<RID> rids = collectRidsNarrowedByPredicates(
      pool, storage, table, heap, predicates);
  std::vector<PagedFile> index_files = openIndexFiles(storage, table);
  return removeMatchingRows(
      pool, table, heap, rids, predicates,
      [&](const TypedRow& row, const RID& rid) {
        for (std::size_t index = 0; index < index_files.size(); ++index) {
          const std::vector<std::size_t> indexed_column_indexes =
              table.indexedColumnIndexes(table.indexes()[index]);
          const std::string key = index_key::encodeRow(
              table.schema(), row, indexed_column_indexes);
          BTreeCursor::removeRID(pool, index_files[index], key, rid);
        }
      });
}

/**
 * We first design updates to be idempotent by modeling them as a remove
 * followed by an insert. This is not necessarily the most efficient strategy,
 * but it keeps the update path simple and robust and fasten the development.
 * Also, this unlocks follow-on benefits (e.g., easier recovery/retry and
 * fewer page-structure assumptions) without requiring in-place updates or
 * special-case split handling.
 */
void executor::update(BufferPool& pool, Table& table,
                      const UpdateParser& parser) {
  const LocalStorageManager storage;
  HeapFile heap = storage.openHeap(table.heapFileName());
  const std::vector<UnboundComparisonPredicate> predicates =
      parser.extractComparisonPredicates(table.schema());
  const std::vector<RID> rids = collectRidsNarrowedByPredicates(
      pool, storage, table, heap, predicates);
  std::vector<PagedFile> index_files = openIndexFiles(storage, table);
  updateMatchingRows(
      pool, table, heap, rids, predicates, parser,
      [&](const TypedRow& original_row, const RID& old_rid,
        const RID& new_rid) {
        const std::vector<std::string> index_keys =
            encodeIndexKeys(table, original_row);
        for (std::size_t index = 0; index < index_files.size(); ++index) {
          BTreeCursor::replaceRID(pool, index_files[index], index_keys[index],
                                  old_rid, new_rid);
        }
      });
}
