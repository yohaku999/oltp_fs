#include "executor.h"

#include <algorithm>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "../logging.h"
#include "execution/operators/filter_operator.h"
#include "execution/operators/aggregate_operator.h"
#include "execution/binder.h"
#include "execution/operators/heap_fetch_operator.h"
#include "execution/operators/index_scan_operator.h"
#include "execution/operators/index_lookup_join_operator.h"
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
#include "execution/operators/hash_join_operator.h"
#include "storage/index/btreecursor.h"
#include "storage/index/index_key.h"
#include "storage/page/cell.h"
#include "storage/page/page.h"
#include "storage/record/record_cell.h"
#include "storage/record/record_serializer.h"
#include "storage/buffer/bufferpool.h"
#include "storage/wal/wal.h"
#include "storage/wal/wal_body.h"
#include "storage/wal/wal_record.h"
#include "catalog/table.h"

namespace {

struct PredicateValueAndType {
  const FieldValue& value;
  Column::Type column_type;
};

PredicateValueAndType extractValueAndType(
    const BoundComparisonPredicate& predicate) {
  const BoundColumnRef* left_column = std::get_if<BoundColumnRef>(&predicate.left);
  const BoundColumnRef* right_column =
      std::get_if<BoundColumnRef>(&predicate.right);
  const FieldValue* left_value = std::get_if<FieldValue>(&predicate.left);
  const FieldValue* right_value = std::get_if<FieldValue>(&predicate.right);

  if (left_column != nullptr && right_value != nullptr) {
    return {*right_value, left_column->type};
  }
  if (right_column != nullptr && left_value != nullptr) {
    return {*left_value, right_column->type};
  }

  throw std::logic_error(
      "Index lookup predicates must compare a column with a value.");
}

std::vector<BoundComparisonPredicate>::const_iterator findPredicateByOp(
    const std::vector<BoundComparisonPredicate>& predicates, Op op) {
  return std::find_if(
      predicates.begin(), predicates.end(),
      [op](const BoundComparisonPredicate& predicate) {
        return predicate.op == op;
      });
}

std::pair<BTreeCursor::Boundary, BTreeCursor::Boundary> buildTraversalBoundaries(
    const std::vector<std::vector<BoundComparisonPredicate>>&
        ordered_predicates) {
  BTreeCursor::Boundary left_boundary{"", true};
  BTreeCursor::Boundary right_boundary{"", true};

  for (size_t i = 0; i < ordered_predicates.size(); ++i) {
    const auto& predicates_for_key = ordered_predicates[i];
    if (predicates_for_key.empty() && i == 0) {
      throw std::logic_error(
          "The leading key must have at least one predicate for index lookup.");
    }
    if (predicates_for_key.empty() && i != 0) {
      // If a non-leading key has no predicate, later keys cannot narrow the
      // traversal boundary.
      break;
    }

    const auto eq_pred_it = findPredicateByOp(predicates_for_key, Op::Eq);
    if (eq_pred_it != predicates_for_key.end()) {
      const auto [value, column_type] = extractValueAndType(*eq_pred_it);
      left_boundary.composite_key +=
          index_key::encodeFieldValue(value, column_type);
      right_boundary.composite_key +=
          index_key::encodeFieldValue(value, column_type);
      continue;
    }

    const auto gt_pred_it = findPredicateByOp(predicates_for_key, Op::Gt);
    if (gt_pred_it != predicates_for_key.end()) {
      const auto [value, column_type] = extractValueAndType(*gt_pred_it);
      left_boundary.composite_key +=
          index_key::encodeFieldValue(value, column_type);
      left_boundary.is_inclusive = false;
      continue;
    }

    const auto ge_pred_it = findPredicateByOp(predicates_for_key, Op::Ge);
    if (ge_pred_it != predicates_for_key.end()) {
      const auto [value, column_type] = extractValueAndType(*ge_pred_it);
      left_boundary.composite_key +=
          index_key::encodeFieldValue(value, column_type);
      continue;
    }

    const auto lt_pred_it = findPredicateByOp(predicates_for_key, Op::Lt);
    if (lt_pred_it != predicates_for_key.end()) {
      const auto [value, column_type] = extractValueAndType(*lt_pred_it);
      right_boundary.composite_key +=
          index_key::encodeFieldValue(value, column_type);
      right_boundary.is_inclusive = false;
      continue;
    }

    const auto le_pred_it = findPredicateByOp(predicates_for_key, Op::Le);
    if (le_pred_it != predicates_for_key.end()) {
      const auto [value, column_type] = extractValueAndType(*le_pred_it);
      right_boundary.composite_key +=
          index_key::encodeFieldValue(value, column_type);
      continue;
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
    const double lhs = std::holds_alternative<Column::DoubleType>(original_value)
                           ? std::get<Column::DoubleType>(original_value)
                           : static_cast<double>(
                                 std::get<Column::IntegerType>(original_value));
    const double rhs = std::holds_alternative<Column::DoubleType>(arithmetic.literal)
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

TypedRow applyUpdateAssignments(const TypedRow& original_row,
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
 * Filter the given predicates to keep only those that can be used for index scan and group them by their corresponding index key order.
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
    const bool has_column_value_pair = (left_column != nullptr) !=
                                       (right_column != nullptr);
    if (!has_column_value_pair) {
      continue;
    }

    const BoundColumnRef* column_ref =
        left_column != nullptr ? left_column : right_column;
    const auto it = std::find(key_order_indexes.begin(),
                              key_order_indexes.end(),
                              column_ref->column_index);
    if (it == key_order_indexes.end()) {
      continue;
    }

    const std::size_t key_index =
        static_cast<std::size_t>(std::distance(key_order_indexes.begin(), it));
    ordered_predicates[key_index].push_back(predicate);
  }

  return ordered_predicates;
}

struct IndexLookupPlan {
  bool can_use_index;
  std::vector<std::vector<BoundComparisonPredicate>> ordered_predicates;
};

struct IndexLookupJoinPlan {
  std::vector<IndexLookupJoinKey> join_keys;
  std::vector<IndexLookupJoinConstantKey> constant_keys;
};

IndexLookupPlan planIndexLookup(
    const Table& table,
    const std::vector<BoundComparisonPredicate>& predicates) {
  if (table.indexedColumnIndexes().empty()) {
    return {false, {}};
  }

  std::vector<std::vector<BoundComparisonPredicate>> ordered_predicates =
      prepareIndexKeyPredicates(predicates, table.indexedColumnIndexes());
  const bool can_use_index =
      !ordered_predicates.empty() && !ordered_predicates.front().empty();

  return {can_use_index, std::move(ordered_predicates)};
}

std::vector<BoundComparisonPredicate> buildIndexKeyEqualityPredicates(
    const Table& table, const TypedRow& row) {
  std::vector<BoundComparisonPredicate> predicates;
  predicates.reserve(table.indexedColumnIndexes().size());

  for (const std::size_t indexed_column_index : table.indexedColumnIndexes()) {
    const auto& column = table.schema().columns().at(indexed_column_index);
    predicates.push_back(BoundComparisonPredicate{
        Op::Eq,
        BoundColumnRef{0, indexed_column_index, column.getType()},
        row.values.at(indexed_column_index)});
  }

  return predicates;
}

/**
 * Collect RIDs of records narrowed by the given predicates.
 */
std::vector<RID> collectRidsNarrowedByPredicates(
    BufferPool& pool, Table& table,
    const std::vector<UnboundComparisonPredicate>& predicates) {
  const std::vector<BoundComparisonPredicate> bound_predicates =
      binder::bindPredicatesResolvableByTable(predicates, table);
  IndexLookupPlan index_plan = planIndexLookup(table, bound_predicates);
  if (!index_plan.can_use_index) {
    dbfs_log::execution().debug(
        "Building sequential scan operator for table {} because index scan "
        "prerequisites are not met.",
        table.name());
    return table.heapFile().collectRids(pool);
  }

  IndexScanOperator scan(pool, table.requireIndexFile(), buildTraversalBoundaries(index_plan.ordered_predicates),
                         std::move(index_plan.ordered_predicates));
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
    Page* page = pool.pinPage(rid.heap_page_id, table.heapFile().rawFile());
    char* cell_start = page->slotCellStartUnchecked(rid.slot_id);
    if (!Cell::isValid(cell_start)) {
      pool.unpinPage(page, table.heapFile().rawFile());
      continue;
    }

    TypedRow row = RecordCellView(cell_start).getTypedRow(table.schema());
    if (!passesPredicates(row, bound_predicates)) {
      pool.unpinPage(page, table.heapFile().rawFile());
      continue;
    }

    // delete corresponding key from index file.
    const auto index_file = table.indexFile();
    if (index_file.has_value()) {
      // create index key predicates from row, since index search result can have redundant RIDs by its nature.
      // OPTIMIZE : Even if we are doing an online delete, we can still use the original values to construct index key predicates and delete the index entry, instead of leaving the index entry dangling and cleaning it up later by a separate process.
      const std::vector<BoundComparisonPredicate> index_key_predicates =
          buildIndexKeyEqualityPredicates(table, row);
      const std::vector<std::vector<BoundComparisonPredicate>> ordered_predicates =
          prepareIndexKeyPredicates(index_key_predicates,
                                      table.indexedColumnIndexes());
      const auto& boundaries = buildTraversalBoundaries(ordered_predicates);
      BTreeCursor::findEntries(pool, index_file->get(), boundaries, true);
    }

    wal.write(WALRecord::RecordType::DELETE, rid.heap_page_id,
              DeleteRedoBody(rid.slot_id).encode());
    page->invalidateSlot(rid.slot_id);
    pool.unpinPage(page, table.heapFile().rawFile());
    ++removed_count;
  }

  return removed_count;
}

void insertRow(BufferPool& pool, Table& table, const TypedRow& row, WAL& wal) {
  const auto index_file = table.indexFile();
  std::optional<std::string> key;
  if (index_file.has_value()) {
    key = table.extractIndexKey(row);
    dbfs_log::execution().debug("Inserting record with key {} into table {}.",
         index_key::formatForDebug(key.value()),
         table.name());
    const std::vector<BoundComparisonPredicate> predicates =
        buildIndexKeyEqualityPredicates(table, row);
    const std::vector<std::vector<BoundComparisonPredicate>> ordered_predicates =
        prepareIndexKeyPredicates(predicates, table.indexedColumnIndexes());
    const auto& boundaries = buildTraversalBoundaries(ordered_predicates);
    std::vector<IndexEntry> existing_entries =
        BTreeCursor::findEntries(pool, index_file->get(), boundaries, false);
    if (!existing_entries.empty()) {
      throw std::runtime_error(
          "Duplicate key is not allowed for indexed table: " + table.name());
    }
  } else {
    dbfs_log::execution().debug("Inserting record into table {}.", table.name());
  }

  RecordSerializer cell(table.schema(), row);
  const std::vector<std::byte>& serialized_cell = cell.serializedBytes();

  File& heap_file = table.heapFile().rawFile();
  uint16_t target_page_id = heap_file.getMaxPageID();
  Page* heap_page = pool.pinPage(target_page_id, heap_file);
  auto inserted_slot_id = heap_page->insertCell(serialized_cell);
  if (!inserted_slot_id.has_value()) {
    pool.unpinPage(heap_page, heap_file);
    target_page_id = pool.createPage(PageKind::Heap, heap_file);
    heap_page = pool.pinPage(target_page_id, heap_file);
    inserted_slot_id = heap_page->insertCell(serialized_cell);
    if (!inserted_slot_id.has_value()) {
      pool.unpinPage(heap_page, heap_file);
      throw std::runtime_error(
          "Failed to insert record cell into a new heap page due to "
          "insufficient space.");
    }
  }
  pool.unpinPage(heap_page, heap_file);

  const RID inserted_rid{
      target_page_id, static_cast<uint16_t>(inserted_slot_id.value())};

  wal.write(WALRecord::RecordType::INSERT,
            inserted_rid.heap_page_id,
            InsertRedoBody(inserted_rid.slot_id, serialized_cell).encode());

  if (index_file.has_value()) {
    BTreeCursor::insertIntoIndex(pool, index_file->get(), key.value(),
                                 inserted_rid.heap_page_id,
                                 inserted_rid.slot_id);
  }
}

std::size_t updateMatchingRows(BufferPool& pool, Table& table,
                               const std::vector<UnboundComparisonPredicate>& predicates,
                               const UpdateParser& parser, WAL& wal) {
  const std::vector<BoundUpdateAssignment> bound_assignments =
    binder::bindUpdateAssignments(parser.extractAssignments(table.schema()),
                  table);
  const std::vector<RID> rids =
      collectRidsNarrowedByPredicates(pool, table, predicates);
  const std::vector<BoundComparisonPredicate> bound_predicates =
      binder::bindPredicates(predicates, {table});
  std::vector<TypedRow> updated_rows;

  for (const RID& rid : rids) {
    Page* page = pool.pinPage(rid.heap_page_id, table.heapFile().rawFile());
    char* cell_start = page->slotCellStartUnchecked(rid.slot_id);
    if (!Cell::isValid(cell_start)) {
      pool.unpinPage(page, table.heapFile().rawFile());
      continue;
    }

    TypedRow original_row = RecordCellView(cell_start).getTypedRow(table.schema());
    pool.unpinPage(page, table.heapFile().rawFile());
    if (!passesPredicates(original_row, bound_predicates)) {
      continue;
    }

    updated_rows.push_back(
        applyUpdateAssignments(original_row, bound_assignments, table.schema()));
  }

  const std::size_t removed_count = removeMatchingRows(pool, table, predicates, wal);
  for (const TypedRow& updated_row : updated_rows) {
    insertRow(pool, table, updated_row, wal);
  }

  return removed_count;
}

std::unique_ptr<TypedRowOperator> buildReadSource(
    BufferPool& pool, Table& table,
    const std::vector<UnboundComparisonPredicate>& predicates) {
  const std::vector<BoundComparisonPredicate> bound_predicates =
      binder::bindPredicatesResolvableByTable(predicates, table);
  IndexLookupPlan index_plan = planIndexLookup(table, bound_predicates);
  if (!index_plan.can_use_index) {
    dbfs_log::execution().debug(
        "Building sequential scan operator for table {} because index scan "
        "prerequisites are not met.",
        table.name());
    return std::make_unique<SeqScanOperator>(pool, table.heapFile().rawFile(),
                                             table.schema(), bound_predicates);
  }
  
  auto scan = std::make_unique<IndexScanOperator>(
      pool, table.requireIndexFile(), buildTraversalBoundaries(index_plan.ordered_predicates),std::move(index_plan.ordered_predicates));
  return std::make_unique<HeapFetchOperator>(std::move(scan), pool,
                                             table.heapFile(), table.schema(), bound_predicates);
}

std::optional<HashJoinKey> findHashJoinKeyForTwoTableJoin(
    const std::vector<BoundComparisonPredicate>& predicates,
    const std::vector<Table>& tables) {
  if (tables.size() != 2) {
    return std::nullopt;
  }

  const std::size_t outer_column_count =
      tables[0].schema().columns().size();
  const std::size_t inner_column_count =
      tables[1].schema().columns().size();
  const std::size_t joined_column_count =
      outer_column_count + inner_column_count;

  for (const auto& predicate : predicates) {
    if (predicate.op != Op::Eq) {
      continue;
    }

    const auto* left_column = std::get_if<BoundColumnRef>(&predicate.left);
    const auto* right_column = std::get_if<BoundColumnRef>(&predicate.right);
    if (left_column == nullptr || right_column == nullptr ||
        left_column->type != right_column->type) {
      continue;
    }

    const auto to_outer_index = [&](std::size_t column_index)
        -> std::optional<std::size_t> {
      if (column_index < outer_column_count) {
        return column_index;
      }
      return std::nullopt;
    };
    const auto to_inner_index = [&](std::size_t column_index)
        -> std::optional<std::size_t> {
      if (column_index >= outer_column_count &&
          column_index < joined_column_count) {
        return column_index - outer_column_count;
      }
      return std::nullopt;
    };

    const std::optional<std::size_t> left_outer =
        to_outer_index(left_column->column_index);
    const std::optional<std::size_t> left_inner =
        to_inner_index(left_column->column_index);
    const std::optional<std::size_t> right_outer =
        to_outer_index(right_column->column_index);
    const std::optional<std::size_t> right_inner =
        to_inner_index(right_column->column_index);

    if (left_outer.has_value() && right_inner.has_value()) {
      return HashJoinKey{left_outer.value(), right_inner.value()};
    }
    if (right_outer.has_value() && left_inner.has_value()) {
      return HashJoinKey{right_outer.value(), left_inner.value()};
    }
  }

  return std::nullopt;
}

std::optional<IndexLookupJoinPlan> findIndexLookupJoinPlanForTwoTableJoin(
    const std::vector<BoundComparisonPredicate>& predicates,
    const std::vector<Table>& tables) {
  if (tables.size() != 2 || tables[1].indexedColumnIndexes().empty()) {
    return std::nullopt;
  }

  const std::size_t outer_column_count =
      tables[0].schema().columns().size();
  const std::size_t inner_column_count =
      tables[1].schema().columns().size();
  const std::size_t joined_column_count =
      outer_column_count + inner_column_count;

  const auto to_outer_index = [&](std::size_t column_index)
      -> std::optional<std::size_t> {
    if (column_index < outer_column_count) {
      return column_index;
    }
    return std::nullopt;
  };
  const auto to_inner_index = [&](std::size_t column_index)
      -> std::optional<std::size_t> {
    if (column_index >= outer_column_count &&
        column_index < joined_column_count) {
      return column_index - outer_column_count;
    }
    return std::nullopt;
  };

  IndexLookupJoinPlan plan;
  std::vector<bool> covered_inner_columns(inner_column_count, false);
  const auto mark_covered = [&](std::size_t inner_column_index) {
    if (inner_column_index < covered_inner_columns.size()) {
      covered_inner_columns[inner_column_index] = true;
    }
  };

  for (const auto& predicate : predicates) {
    if (predicate.op != Op::Eq) {
      continue;
    }

    const auto* left_column = std::get_if<BoundColumnRef>(&predicate.left);
    const auto* right_column = std::get_if<BoundColumnRef>(&predicate.right);
    const auto* left_value = std::get_if<FieldValue>(&predicate.left);
    const auto* right_value = std::get_if<FieldValue>(&predicate.right);

    if (left_column != nullptr && right_column != nullptr &&
        left_column->type == right_column->type) {
      const std::optional<std::size_t> left_outer =
          to_outer_index(left_column->column_index);
      const std::optional<std::size_t> left_inner =
          to_inner_index(left_column->column_index);
      const std::optional<std::size_t> right_outer =
          to_outer_index(right_column->column_index);
      const std::optional<std::size_t> right_inner =
          to_inner_index(right_column->column_index);

      if (left_outer.has_value() && right_inner.has_value()) {
        plan.join_keys.push_back(
            IndexLookupJoinKey{left_outer.value(), right_inner.value()});
        mark_covered(right_inner.value());
        continue;
      }
      if (right_outer.has_value() && left_inner.has_value()) {
        plan.join_keys.push_back(
            IndexLookupJoinKey{right_outer.value(), left_inner.value()});
        mark_covered(left_inner.value());
        continue;
      }
    }

    const BoundColumnRef* column_ref =
        left_column != nullptr ? left_column : right_column;
    const FieldValue* value = left_value != nullptr ? left_value : right_value;
    if (column_ref == nullptr || value == nullptr) {
      continue;
    }

    const std::optional<std::size_t> inner_index =
        to_inner_index(column_ref->column_index);
    if (!inner_index.has_value()) {
      continue;
    }

    plan.constant_keys.push_back(
        IndexLookupJoinConstantKey{inner_index.value(), *value});
    mark_covered(inner_index.value());
  }

  for (const std::size_t indexed_column_index :
       tables[1].indexedColumnIndexes()) {
    if (indexed_column_index >= covered_inner_columns.size() ||
        !covered_inner_columns[indexed_column_index]) {
      return std::nullopt;
    }
  }

  if (plan.join_keys.empty()) {
    return std::nullopt;
  }
  return plan;
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

  std::vector<std::unique_ptr<TypedRowOperator>> sources;
  for (auto& table : tables) {
    std::unique_ptr<TypedRowOperator> source = buildReadSource(pool, table, predicates);
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
  std::unique_ptr<TypedRowOperator> pipeline;
  if (sources.size() == 1) {
    pipeline = std::move(sources.front());
  } else if (std::optional<IndexLookupJoinPlan> index_lookup_join_plan =
                 findIndexLookupJoinPlanForTwoTableJoin(bound_predicates,
                                                        tables);
             index_lookup_join_plan.has_value()) {
    std::vector<BoundComparisonPredicate> inner_predicates =
        binder::bindPredicatesResolvableByTable(predicates, tables[1]);
    pipeline = std::make_unique<IndexLookupJoinOperator>(
        std::move(sources[0]), pool, tables[1],
        std::move(index_lookup_join_plan->join_keys),
        std::move(index_lookup_join_plan->constant_keys),
        std::move(inner_predicates));
  } else if (std::optional<HashJoinKey> key =
                 findHashJoinKeyForTwoTableJoin(bound_predicates, tables);
             key.has_value()) {
    pipeline = std::make_unique<HashJoinOperator>(
        std::move(sources[0]), std::move(sources[1]), key.value());
  } else {
    pipeline = std::make_unique<LoopJoinOperator>(std::move(sources));
  }

  // filter
  pipeline = std::make_unique<FilterOperator>(std::move(pipeline),
                                              bound_predicates);
  std::vector<TypedRow> items;
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
    items = collectItems<TypedRow>(*pipeline);
  }else{
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
    items = collectItems<TypedRow>(projection);
  }
  dbfs_log::execution().info("Query returned {} rows.", items.size());  
  return items;
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
  const TypedRow row = parser.extractRow(table.schema());
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
