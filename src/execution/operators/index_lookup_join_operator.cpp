#include "execution/operators/index_lookup_join_operator.h"

#include <stdexcept>
#include <utility>

#include "catalog/table.h"
#include "execution/comparison_predicate.h"
#include "execution/heapfile.h"
#include "storage/index/btreecursor.h"
#include "storage/index/index_key.h"
#include "storage/index/rid.h"
#include "storage/record/record_cell.h"
#include "storage/runtime/bufferpool.h"

IndexLookupJoinOperator::IndexLookupJoinOperator(
    std::unique_ptr<TypedRowOperator> outer_child, BufferPool& pool,
    Table& inner_table, std::vector<IndexLookupJoinKey> join_keys,
    std::vector<IndexLookupJoinConstantKey> constant_keys,
    std::vector<BoundComparisonPredicate> inner_predicates)
    : outer_child_(std::move(outer_child)),
      pool_(pool),
      inner_table_(inner_table),
      join_keys_(std::move(join_keys)),
      constant_keys_(std::move(constant_keys)),
      inner_predicates_(std::move(inner_predicates)) {}

void IndexLookupJoinOperator::open() {
  logger_.open();
  outer_child_->open();
  current_outer_row_.reset();
  current_inner_rows_.clear();
  current_inner_pos_ = 0;
  index_lookups_ = 0;
}

std::optional<std::string> IndexLookupJoinOperator::buildLookupKey(
    const TypedRow& outer_row) const {
  const std::vector<std::size_t> indexed_column_indexes =
      inner_table_.indexedColumnIndexes();
  std::vector<std::optional<FieldValue>> key_values(
      indexed_column_indexes.size());

  const auto set_key_value = [&](std::size_t inner_column_index,
                                 FieldValue value) {
    for (std::size_t index = 0; index < indexed_column_indexes.size();
         ++index) {
      if (indexed_column_indexes[index] == inner_column_index) {
        key_values[index] = std::move(value);
        return;
      }
    }
  };

  for (const IndexLookupJoinConstantKey& constant_key : constant_keys_) {
    set_key_value(constant_key.inner_column_index, constant_key.value);
  }

  for (const IndexLookupJoinKey& join_key : join_keys_) {
    if (join_key.outer_column_index >= outer_row.values.size()) {
      throw std::runtime_error(
          "Index lookup join outer column index is out of range.");
    }
    set_key_value(join_key.inner_column_index,
                  outer_row.values[join_key.outer_column_index]);
  }

  std::string lookup_key;
  const auto& inner_columns = inner_table_.schema().columns();
  for (std::size_t index = 0; index < indexed_column_indexes.size(); ++index) {
    if (!key_values[index].has_value()) {
      return std::nullopt;
    }

    const std::size_t inner_column_index = indexed_column_indexes[index];
    lookup_key += index_key::encodeFieldValue(
        key_values[index].value(),
        inner_columns.at(inner_column_index).getType());
  }

  return lookup_key;
}

std::vector<TypedRow> IndexLookupJoinOperator::lookupInnerRows(
    const TypedRow& outer_row) {
  // rowからliookup keyを構築する。これもどこかで同じ実装をしていたような。
  const std::optional<std::string> lookup_key = buildLookupKey(outer_row);
  if (!lookup_key.has_value()) {
    return {};
  }

  ++index_lookups_;
  logger_.setMetric("index_lookups", index_lookups_);

  const BTreeCursor::Boundary boundary{lookup_key.value(), true};
  const std::vector<IndexEntry> entries = BTreeCursor::findEntries(
      pool_, inner_table_.requireIndexFile(), {boundary, boundary}, false);

  std::vector<TypedRow> rows;
  rows.reserve(entries.size());
  for (const IndexEntry& entry : entries) {
    std::optional<TypedRow> inner_row =
        inner_table_.heapFile().withCell(pool_, entry.rid,
                                         [&](RecordCellView cell) {
                                           return cell.getTypedRow(
                                               inner_table_.schema());
                                         });
    if (!inner_row.has_value()) {
      continue;
    }

    logger_.recordInput();
    if (passesPredicates(*inner_row, inner_predicates_)) {
      rows.push_back(std::move(*inner_row));
    }
  }

  return rows;
}

std::optional<TypedRow> IndexLookupJoinOperator::next() {
  while (true) {
    if (current_inner_pos_ < current_inner_rows_.size()) {
      TypedRow joined_row = *current_outer_row_;
      const TypedRow& inner_row = current_inner_rows_[current_inner_pos_++];
      joined_row.values.insert(joined_row.values.end(),
                               inner_row.values.begin(),
                               inner_row.values.end());
      logger_.recordOutput();
      return joined_row;
    }

    std::optional<TypedRow> outer_row = outer_child_->next();
    if (!outer_row.has_value()) {
      return std::nullopt;
    }

    logger_.recordInput();
    current_outer_row_ = std::move(outer_row);
    current_inner_rows_ = lookupInnerRows(*current_outer_row_);
    current_inner_pos_ = 0;
  }
}

void IndexLookupJoinOperator::close() {
  outer_child_->close();
  current_outer_row_.reset();
  current_inner_rows_.clear();
  current_inner_pos_ = 0;
  logger_.close();
}
