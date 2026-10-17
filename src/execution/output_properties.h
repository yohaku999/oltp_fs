#pragma once

#include <algorithm>
#include <vector>

#include "execution/order_by_spec.h"

// Guarantees about the sequence emitted by an operator.
struct OutputProperties {
  // Empty means that the operator makes no guarantee.
  std::vector<OrderBySpec> ordering;

  bool satisfiesOrdering(
      const std::vector<OrderBySpec>& required_ordering) const {
    if (ordering.size() < required_ordering.size()) {
      return false;
    }

    return std::equal(
        required_ordering.begin(), required_ordering.end(), ordering.begin(),
        [](const OrderBySpec& required, const OrderBySpec& actual) {
          return required.column_index == actual.column_index &&
                 required.direction == actual.direction;
        });
  }
};
