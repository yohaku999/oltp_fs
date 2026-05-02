#pragma once

#include <memory>
#include <optional>
#include <stdexcept>
#include <utility>
#include <vector>

#include "execution/operator.h"
#include "execution/select_item.h"

class AggregateOperator : public Operator {
 public:
  AggregateOperator(std::unique_ptr<Operator> child,
                    std::vector<BoundAggregateCall> aggregate_calls)
      : child_(std::move(child)),
        aggregate_calls_(std::move(aggregate_calls)) {}

  void open() override {
    child_->open();
    emitted_ = false;
    result_row_ = computeAggregate();
  }

  std::optional<TypedRow> next() override {
    if (emitted_) {
      return std::nullopt;
    }

    emitted_ = true;
    return result_row_;
  }

  void close() override { child_->close(); }

 private:
  TypedRow computeAggregate() {
    std::vector<long long> sums(aggregate_calls_.size(), 0);
    std::vector<bool> saw_value(aggregate_calls_.size(), false);

    while (std::optional<TypedRow> row = child_->next()) {
      for (std::size_t index = 0; index < aggregate_calls_.size(); ++index) {
        const BoundAggregateCall& aggregate_call = aggregate_calls_[index];
        const FieldValue& value = row->values[aggregate_call.argument.column_index];
        if (std::holds_alternative<std::monostate>(value)) {
          continue;
        }

        sums[index] += std::get<Column::IntegerType>(value);
        saw_value[index] = true;
      }
    }

    TypedRow result_row;
    result_row.values.reserve(aggregate_calls_.size());
    for (std::size_t index = 0; index < aggregate_calls_.size(); ++index) {
      if (!saw_value[index]) {
        result_row.values.push_back(std::monostate{});
        continue;
      }

      result_row.values.push_back(
          static_cast<Column::IntegerType>(sums[index]));
    }

    return result_row;
  }

  std::unique_ptr<Operator> child_;
  std::vector<BoundAggregateCall> aggregate_calls_;
  TypedRow result_row_;
  bool emitted_ = false;
};