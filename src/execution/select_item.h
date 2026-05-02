#pragma once

#include <cstddef>
#include <vector>
#include <variant>

#include "execution/comparison_predicate.h"

enum class AggregateFunction {
  Sum,
};

struct SelectAllItem {};

struct UnboundAggregateCall {
  AggregateFunction function;
  ColumnRef argument;
};

using UnboundSelectItem =
    std::variant<SelectAllItem, ColumnRef, UnboundAggregateCall>;

struct BoundAggregateCall {
  AggregateFunction function;
  BoundColumnRef argument;
};

using BoundSelectItem = std::variant<BoundColumnRef, BoundAggregateCall>;

namespace select_item {

std::vector<BoundAggregateCall> extractAggregateCalls(
    const std::vector<BoundSelectItem>& bound_select_items);

std::vector<std::size_t> extractProjectionIndices(
    const std::vector<BoundSelectItem>& bound_select_items);

}  // namespace select_item