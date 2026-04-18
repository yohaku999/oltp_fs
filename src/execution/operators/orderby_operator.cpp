#include "execution/operators/orderby_operator.h"

#include <algorithm>
#include <stdexcept>

void OrderByOperator::open() {
  child_->open();
  sorted_rows_.clear();
  next_index_ = 0;
  materialized_ = false;
}

std::optional<TypedRow> OrderByOperator::next() {
  if (!materialized_) {
    materializeAndSort();
  }

  if (next_index_ >= sorted_rows_.size()) {
    return std::nullopt;
  }

  return sorted_rows_[next_index_++];
}

void OrderByOperator::close() {
  child_->close();
  sorted_rows_.clear();
  next_index_ = 0;
  materialized_ = false;
}

void OrderByOperator::materializeAndSort() {
  while (std::optional<TypedRow> row = child_->next()) {
    sorted_rows_.push_back(*row);
  }

  std::sort(sorted_rows_.begin(), sorted_rows_.end(),
            [this](const TypedRow& left, const TypedRow& right) {
              for (const auto& order_by_spec : order_by_specs_) {
                const std::size_t order_by_index = order_by_spec.column_index;
                if (order_by_index >= left.values.size() ||
                    order_by_index >= right.values.size()) {
                  throw std::runtime_error(
                      "ORDER BY column index out of range for row.");
                }

                const bool left_less =
                    left.values[order_by_index] < right.values[order_by_index];
                const bool right_less =
                    right.values[order_by_index] < left.values[order_by_index];
                if (!left_less && !right_less) {
                  continue;
                }

                if (order_by_spec.direction == OrderByDirection::Asc) {
                  return left_less;
                }

                return right_less;
              }
              // when all ORDER BY columns are equal
              return false;
            });

  materialized_ = true;
}