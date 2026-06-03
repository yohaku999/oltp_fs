#include "execution/operators/hash_join_operator.h"

#include <utility>

/**
 * HashJoinOperator implements a hash join algorithm for equi-join keys.
 * It is recommended to have child with more rows as inner child for better performance.
 */
HashJoinOperator::HashJoinOperator(
    std::unique_ptr<TypedRowOperator> outer_child,
    std::unique_ptr<TypedRowOperator> inner_child, HashJoinKey join_key)
    : outer_child_(std::move(outer_child)),
      inner_child_(std::move(inner_child)),
      join_key_(join_key) {}

void HashJoinOperator::open() {
  logger_.open();
  hash_table.clear();

  // create hash table for inner child
  inner_child_->open();
  for (std::optional<TypedRow> row = inner_child_->next(); row.has_value();
       row = inner_child_->next()) {
    logger_.recordInput();
    const TypedRow& typed_row = *row;
    FieldValue key = typed_row.values.at(join_key_.inner_column_index);
    hash_table.emplace(key, typed_row);
  }
  inner_child_->close();

  outer_child_->open();
  current_outer_row_.reset();
  current_inner_it_ = hash_table.end();
  current_inner_end_ = hash_table.end();
  logger_.setMetric("hash_table_keys", hash_table.size());
}

std::optional<TypedRow> HashJoinOperator::next() {
  while (true) {
    if (current_inner_it_ != current_inner_end_) {
      const TypedRow& inner_row = current_inner_it_->second;
      TypedRow joined_row = *current_outer_row_;
      joined_row.values.insert(joined_row.values.end(),
                               inner_row.values.begin(),
                               inner_row.values.end());
      ++current_inner_it_;
      logger_.recordOutput();
      return joined_row;
    }

    std::optional<TypedRow> row = outer_child_->next();
    if (!row.has_value()) {
      return std::nullopt;
    }

    logger_.recordInput();
    current_outer_row_ = *row;
    FieldValue key =
        current_outer_row_->values.at(join_key_.outer_column_index);
    auto range = hash_table.equal_range(key);
    current_inner_it_ = range.first;
    current_inner_end_ = range.second;
  }
}

void HashJoinOperator::close() {
  outer_child_->close();
  current_outer_row_.reset();
  current_inner_it_ = hash_table.end();
  current_inner_end_ = hash_table.end();
  logger_.close();
}
