#include "execution/operators/loop_join_operator.h"

#include <utility>

LoopJoinOperator::LoopJoinOperator(
    std::vector<std::unique_ptr<Operator>> children)
    : children_(std::move(children)) {}

void LoopJoinOperator::open() {
  materialized_source_rows_.clear();
  materialized_source_rows_.reserve(children_.size());
  source_row_cursors_.clear();

  if (children_.empty()) {
    exhausted_ = true;
    return;
  }

  for (const auto& child : children_) {
    materialized_source_rows_.push_back(materializeSourceRows(*child));
  }

  for (const auto& rows : materialized_source_rows_) {
    if (rows.empty()) {
      exhausted_ = true;
      return;
    }
  }

  source_row_cursors_.assign(children_.size(), 0);
  exhausted_ = false;
}

std::optional<TypedRow> LoopJoinOperator::next() {
  if (exhausted_) {
    return std::nullopt;
  }

  TypedRow row = buildJoinedRow();
  advanceSourceRowCursors();
  return row;
}

void LoopJoinOperator::close() {
  materialized_source_rows_.clear();
  source_row_cursors_.clear();
  exhausted_ = true;
}

std::vector<TypedRow> LoopJoinOperator::materializeSourceRows(
    Operator& child) const {
  std::vector<TypedRow> rows;
  child.open();
  while (std::optional<TypedRow> row = child.next()) {
    rows.push_back(*row);
  }
  child.close();
  return rows;
}

TypedRow LoopJoinOperator::buildJoinedRow() const {
  TypedRow row;
  for (std::size_t source_index = 0;
       source_index < materialized_source_rows_.size();
       ++source_index) {
    const TypedRow& source_row =
        materialized_source_rows_[source_index]
                                [source_row_cursors_[source_index]];
    row.values.insert(row.values.end(), source_row.values.begin(),
                      source_row.values.end());
  }

  return row;
}

void LoopJoinOperator::advanceSourceRowCursors() {
  for (std::size_t source_index = source_row_cursors_.size();
       source_index > 0;
       --source_index) {
    const std::size_t index = source_index - 1;
    source_row_cursors_[index] += 1;
    if (source_row_cursors_[index] <
        materialized_source_rows_[index].size()) {
      return;
    }
    source_row_cursors_[index] = 0;
  }

  exhausted_ = true;
}