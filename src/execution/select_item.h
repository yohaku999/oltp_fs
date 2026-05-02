#pragma once

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