#pragma once

#include <cstddef>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "execution/operator.h"
#include "execution/order_by_spec.h"

class OrderByOperator : public TypedRowOperator {
 public:
  OrderByOperator(std::unique_ptr<TypedRowOperator> child,
                  std::vector<OrderBySpec> order_by_specs)
      : child_(std::move(child)),
        order_by_specs_(std::move(order_by_specs)) {}

  void open() override;
  std::optional<TypedRow> next() override;
  void close() override;

 private:
  void materializeAndSort();

  std::unique_ptr<TypedRowOperator> child_;
  std::vector<OrderBySpec> order_by_specs_;
  std::vector<TypedRow> sorted_rows_;
  OperatorExecutionLogger logger_{"OrderByOperator"};
  std::size_t next_index_ = 0;
  bool materialized_ = false;
};