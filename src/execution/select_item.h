#pragma once

#include <cstddef>
#include <vector>
#include <variant>

#include "execution/comparison_predicate.h"

enum class AggregateFunction {
  Sum,
  Count,
};

struct SelectAllItem {};

struct AggregateAllColumnsArgument {};

using UnboundAggregateArgument =
    std::variant<ColumnRef, AggregateAllColumnsArgument>;

using BoundAggregateArgument =
    std::variant<BoundColumnRef, AggregateAllColumnsArgument>;

struct UnboundAggregateCall {
  AggregateFunction function;
  UnboundAggregateArgument argument;
  bool is_distinct = false;
};

using UnboundSelectItem =
    std::variant<SelectAllItem, ColumnRef, UnboundAggregateCall>;

struct BoundAggregateCall {
  AggregateFunction function;
  BoundAggregateArgument argument;
  bool is_distinct = false;
};

using BoundSelectItem = std::variant<BoundColumnRef, BoundAggregateCall>;

namespace select_item {

std::vector<BoundAggregateCall> extractAggregateCalls(
    const std::vector<BoundSelectItem>& bound_select_items);

std::vector<std::size_t> extractProjectionIndices(
    const std::vector<BoundSelectItem>& bound_select_items);

}  // namespace select_item