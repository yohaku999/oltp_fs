#pragma once

#include <memory>
#include <optional>
#include <set>
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
    accumulators_ = buildAccumulators();
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
  class AggregateAccumulator {
   public:
    virtual ~AggregateAccumulator() = default;
    virtual void accumulate(const TypedRow& row) = 0;
    virtual FieldValue finalize() const = 0;
  };

  class CountAccumulator : public AggregateAccumulator {
   public:
    explicit CountAccumulator(BoundAggregateArgument argument)
        : argument_(std::move(argument)) {}

    void accumulate(const TypedRow& row) override {
      if (std::holds_alternative<AggregateAllColumnsArgument>(argument_)) {
        ++count_;
        return;
      }

      const auto& argument = std::get<BoundColumnRef>(argument_);
      const FieldValue& value = row.values[argument.column_index];
      if (!std::holds_alternative<std::monostate>(value)) {
        ++count_;
      }
    }

    FieldValue finalize() const override {
      return static_cast<Column::IntegerType>(count_);
    }

   private:
    BoundAggregateArgument argument_;
    long long count_ = 0;
  };

  class CountDistinctAccumulator : public AggregateAccumulator {
   public:
    explicit CountDistinctAccumulator(BoundColumnRef argument)
        : argument_(std::move(argument)) {}

    void accumulate(const TypedRow& row) override {
      const FieldValue& value = row.values[argument_.column_index];
      if (std::holds_alternative<std::monostate>(value)) {
        return;
      }

      distinct_values_.insert(value);
    }

    FieldValue finalize() const override {
      return static_cast<Column::IntegerType>(distinct_values_.size());
    }

   private:
    BoundColumnRef argument_;
    std::set<FieldValue> distinct_values_;
  };

  class SumAccumulator : public AggregateAccumulator {
   public:
    explicit SumAccumulator(BoundColumnRef argument)
        : argument_(std::move(argument)) {}

    void accumulate(const TypedRow& row) override {
      const FieldValue& value = row.values[argument_.column_index];
      if (std::holds_alternative<std::monostate>(value)) {
        return;
      }

      integer_sum_ += std::get<Column::IntegerType>(value);
      saw_value_ = true;
    }

    FieldValue finalize() const override {
      if (!saw_value_) {
        return std::monostate{};
      }
      return static_cast<Column::IntegerType>(integer_sum_);
    }

   private:
    BoundColumnRef argument_;
    long long integer_sum_ = 0;
    bool saw_value_ = false;
  };

  static std::unique_ptr<AggregateAccumulator> createAccumulator(
      const BoundAggregateCall& aggregate_call) {
    switch (aggregate_call.function) {
      case AggregateFunction::Count:
        if (aggregate_call.is_distinct) {
          return std::make_unique<CountDistinctAccumulator>(
              std::get<BoundColumnRef>(aggregate_call.argument));
        }
        return std::make_unique<CountAccumulator>(aggregate_call.argument);
      case AggregateFunction::Sum:
        return std::make_unique<SumAccumulator>(
            std::get<BoundColumnRef>(aggregate_call.argument));
    }

    throw std::runtime_error("Unsupported aggregate function.");
  }

  std::vector<std::unique_ptr<AggregateAccumulator>> buildAccumulators() const {
    std::vector<std::unique_ptr<AggregateAccumulator>> accumulators;
    accumulators.reserve(aggregate_calls_.size());
    for (const auto& aggregate_call : aggregate_calls_) {
      accumulators.push_back(createAccumulator(aggregate_call));
    }
    return accumulators;
  }

  TypedRow computeAggregate() {
    while (std::optional<TypedRow> row = child_->next()) {
      for (std::size_t index = 0; index < accumulators_.size(); ++index) {
        accumulators_[index]->accumulate(*row);
      }
    }

    TypedRow result_row;
    result_row.values.reserve(accumulators_.size());
    for (const auto& accumulator : accumulators_) {
      result_row.values.push_back(accumulator->finalize());
    }

    return result_row;
  }

  std::unique_ptr<Operator> child_;
  std::vector<BoundAggregateCall> aggregate_calls_;
  std::vector<std::unique_ptr<AggregateAccumulator>> accumulators_;
  TypedRow result_row_;
  bool emitted_ = false;
};