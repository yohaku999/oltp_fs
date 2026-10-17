#pragma once

#include <cstddef>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "execution/operator.h"
#include "execution/order_by_spec.h"

class TopNOperator : public TypedRowOperator {
 public:
  TopNOperator(std::unique_ptr<TypedRowOperator> child,
               std::vector<OrderBySpec> order_by_specs, std::size_t limit)
      : child_(std::move(child)), limit_(limit) {
    output_properties_.ordering = std::move(order_by_specs);
  }

  void open() override;
  std::optional<TypedRow> next() override;
  void close() override;
  const OutputProperties& outputProperties() const override {
    return output_properties_;
  }

 private:
  bool comesBefore(const TypedRow& left, const TypedRow& right) const;
  void materializeTopRows();

  std::unique_ptr<TypedRowOperator> child_;
  OutputProperties output_properties_;
  std::vector<TypedRow> top_rows_;
  OperatorExecutionLogger logger_{"TopNOperator"};
  std::size_t limit_;
  std::size_t next_index_ = 0;
  bool materialized_ = false;
};
