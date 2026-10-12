#include "execution/operators/index_lookup_join_operator.h"

#include <algorithm>
#include <stdexcept>
#include <utility>

#include "execution/comparison_predicate.h"
#include "storage/buffer/bufferpool.h"
#include "storage/heap/heapfile.h"
#include "storage/index/index_key.h"
#include "storage/index/rid.h"
#include "storage/record/record_cell.h"

IndexLookupJoinOperator::IndexLookupJoinOperator(
    std::unique_ptr<TypedRowOperator> outer_child, BufferPool& pool,
    HeapFile inner_heap_file, IndexAccessor& index_accessor,
    IndexMetadata inner_index, Schema inner_schema,
    std::vector<std::size_t> indexed_column_indexes,
    std::vector<physical::JoinColumnPair> join_keys,
    std::vector<physical::ColumnValuePair> constant_keys,
    std::vector<BoundComparisonPredicate> inner_predicates)
    : outer_child_(std::move(outer_child)),
      pool_(pool),
      inner_heap_file_(std::move(inner_heap_file)),
      index_accessor_(index_accessor),
      inner_index_(std::move(inner_index)),
      inner_schema_(std::move(inner_schema)),
      indexed_column_indexes_(std::move(indexed_column_indexes)),
      join_keys_(std::move(join_keys)),
      constant_keys_(std::move(constant_keys)),
      inner_predicates_(std::move(inner_predicates)) {}

void IndexLookupJoinOperator::open() {
  logger_.open();
  outer_child_->open();
  outer_row_batch_.clear();
  inner_row_group_index_by_outer_row_.clear();
  matched_inner_row_groups_.clear();
  current_outer_pos_ = 0;
  current_inner_pos_ = 0;
  index_lookups_ = 0;
}

std::optional<std::string> IndexLookupJoinOperator::buildLookupKey(
    const TypedRow& outer_row) const {
  std::vector<std::optional<FieldValue>> key_values(
      indexed_column_indexes_.size());

  const auto set_key_value = [&](std::size_t inner_column_index,
                                 FieldValue value) {
    for (std::size_t index = 0; index < indexed_column_indexes_.size();
         ++index) {
      if (indexed_column_indexes_[index] == inner_column_index) {
        key_values[index] = std::move(value);
        return;
      }
    }
  };

  for (const physical::ColumnValuePair& constant_key : constant_keys_) {
    set_key_value(constant_key.column_index, constant_key.value);
  }

  for (const physical::JoinColumnPair& join_key : join_keys_) {
    if (join_key.outer_column_index >= outer_row.values.size()) {
      throw std::runtime_error(
          "Index lookup join outer column index is out of range.");
    }
    set_key_value(join_key.inner_column_index,
                  outer_row.values[join_key.outer_column_index]);
  }

  std::string lookup_key;
  const auto& inner_columns = inner_schema_.columns();
  for (std::size_t index = 0; index < indexed_column_indexes_.size(); ++index) {
    if (!key_values[index].has_value()) {
      return std::nullopt;
    }

    const std::size_t inner_column_index = indexed_column_indexes_[index];
    lookup_key += index_key::encodeFieldValue(
        key_values[index].value(),
        inner_columns.at(inner_column_index).getType());
  }

  return lookup_key;
}

bool IndexLookupJoinOperator::prepareNextBatch() {
  outer_row_batch_.clear();
  inner_row_group_index_by_outer_row_.clear();
  matched_inner_row_groups_.clear();
  current_outer_pos_ = 0;
  current_inner_pos_ = 0;

  while (outer_row_batch_.size() < kLookupBatchSize) {
    std::optional<TypedRow> outer_row = outer_child_->next();
    if (!outer_row.has_value()) {
      break;
    }
    logger_.recordInput();
    outer_row_batch_.push_back(std::move(*outer_row));
  }
  if (outer_row_batch_.empty()) {
    return false;
  }

  std::vector<std::pair<std::string, std::size_t>> keyed_outer_rows;
  keyed_outer_rows.reserve(outer_row_batch_.size());
  for (std::size_t outer_index = 0;
       outer_index < outer_row_batch_.size(); ++outer_index) {
    std::optional<std::string> key =
        buildLookupKey(outer_row_batch_[outer_index]);
    if (key.has_value()) {
      keyed_outer_rows.emplace_back(std::move(*key), outer_index);
    }
  }
  std::sort(keyed_outer_rows.begin(), keyed_outer_rows.end(),
            [](const auto& left, const auto& right) {
              return index_key::compare(left.first, right.first) < 0;
            });

  inner_row_group_index_by_outer_row_.resize(outer_row_batch_.size());
  std::vector<std::string> lookup_keys;
  lookup_keys.reserve(keyed_outer_rows.size());
  for (const auto& [key, outer_index] : keyed_outer_rows) {
    if (lookup_keys.empty() ||
        index_key::compare(lookup_keys.back(), key) != 0) {
      lookup_keys.push_back(key);
    }
    inner_row_group_index_by_outer_row_[outer_index] =
        lookup_keys.size() - 1;
  }

  index_lookups_ += lookup_keys.size();
  logger_.setMetric("index_lookups", index_lookups_);
  matched_inner_row_groups_ = lookupInnerRows(lookup_keys);
  return true;
}

std::vector<std::vector<TypedRow>>
IndexLookupJoinOperator::lookupInnerRows(
    const std::vector<std::string>& lookup_keys) {

  if (lookup_keys.empty()) {
    return std::vector<std::vector<TypedRow>>(0);
  }

  std::vector<std::vector<TypedRow>> matched_inner_row_groups(
      lookup_keys.size());
  const std::vector<IndexEntry> entries =
      index_accessor_.findExact(inner_index_, lookup_keys);
  std::size_t group_index = 0;
  for (const IndexEntry& entry : entries) {
    while (group_index < lookup_keys.size() &&
           index_key::compare(lookup_keys[group_index], entry.key) < 0) {
      ++group_index;
    }
    if (group_index == lookup_keys.size() ||
        index_key::compare(lookup_keys[group_index], entry.key) != 0) {
      throw std::logic_error(
          "Batch index lookup returned an unrequested key.");
    }

    std::optional<TypedRow> inner_row = inner_heap_file_.withRecord(
        pool_, entry.rid, [&](RecordCellView record) {
          return record.getTypedRow(inner_schema_);
        });
    if (!inner_row.has_value()) {
      continue;
    }

    logger_.recordInput();
    if (passesPredicates(*inner_row, inner_predicates_)) {
      matched_inner_row_groups[group_index].push_back(std::move(*inner_row));
    }
  }

  return matched_inner_row_groups;
}

std::optional<TypedRow> IndexLookupJoinOperator::next() {
  while (true) {
    if (current_outer_pos_ >= outer_row_batch_.size() &&
        !prepareNextBatch()) {
      return std::nullopt;
    }

    const std::optional<std::size_t> inner_row_group_index =
        inner_row_group_index_by_outer_row_[current_outer_pos_];
    if (!inner_row_group_index.has_value()) {
      ++current_outer_pos_;
      current_inner_pos_ = 0;
      continue;
    }

    const std::vector<TypedRow>& matched_inner_rows =
        matched_inner_row_groups_[*inner_row_group_index];
    if (current_inner_pos_ >= matched_inner_rows.size()) {
      ++current_outer_pos_;
      current_inner_pos_ = 0;
      continue;
    }

    TypedRow joined_row = outer_row_batch_[current_outer_pos_];
    const TypedRow& inner_row = matched_inner_rows[current_inner_pos_++];
    joined_row.values.insert(joined_row.values.end(), inner_row.values.begin(),
                             inner_row.values.end());
    logger_.recordOutput();
    return joined_row;
  }
}

void IndexLookupJoinOperator::close() {
  outer_child_->close();
  outer_row_batch_.clear();
  inner_row_group_index_by_outer_row_.clear();
  matched_inner_row_groups_.clear();
  current_outer_pos_ = 0;
  current_inner_pos_ = 0;
  logger_.close();
}
