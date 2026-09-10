#include "execution/operators/topN_operator.h"

#include <algorithm>

void TopNOperator::open() {
  logger_.open();
  child_->open();
  top_rows_.clear();
  next_index_ = 0;
  materialized_ = false;
}

std::optional<TypedRow> TopNOperator::next() {
  if (!materialized_) {
    materializeTopRows();
  }

  if (next_index_ >= top_rows_.size()) {
    return std::nullopt;
  }

  logger_.recordOutput();
  return top_rows_[next_index_++];
}

void TopNOperator::close() {
  child_->close();
  logger_.close();
  top_rows_.clear();
  next_index_ = 0;
  materialized_ = false;
}

bool TopNOperator::comesBefore(const TypedRow& left,
                               const TypedRow& right) const {
  for (const auto& order_by_spec : order_by_specs_) {
    const FieldValue& left_value = left.values[order_by_spec.column_index];
    const FieldValue& right_value = right.values[order_by_spec.column_index];
    const bool left_less = left_value < right_value;
    const bool right_less = right_value < left_value;
    if (!left_less && !right_less) {
      continue;
    }

    if (order_by_spec.direction == OrderByDirection::Asc) {
      return left_less;
    }
    return right_less;
  }

  return false;
}

void TopNOperator::materializeTopRows() {
  const auto heap_compare = [this](const TypedRow& left,
                                   const TypedRow& right) {
    return comesBefore(left, right);
  };

  while (std::optional<TypedRow> row = child_->next()) {
    logger_.recordInput();

    if (top_rows_.size() < limit_) {
      top_rows_.push_back(std::move(*row));
      std::push_heap(top_rows_.begin(), top_rows_.end(), heap_compare);
      continue;
    }

    if (comesBefore(*row, top_rows_.front())) {
      std::pop_heap(top_rows_.begin(), top_rows_.end(), heap_compare);
      top_rows_.back() = std::move(*row);
      std::push_heap(top_rows_.begin(), top_rows_.end(), heap_compare);
    }
  }

  // Result should be sorted, since topN queries always include ORDER BY clause.
  std::sort(top_rows_.begin(), top_rows_.end(), heap_compare);
  logger_.setMetric("materialized_rows", top_rows_.size());
  materialized_ = true;
}
