#include "execution/select_item.h"

namespace select_item {

std::vector<BoundAggregateCall> extractAggregateCalls(
    const std::vector<BoundSelectItem>& bound_select_items) {
  std::vector<BoundAggregateCall> aggregate_calls;
  for (const auto& item : bound_select_items) {
    if (const auto* aggregate_call = std::get_if<BoundAggregateCall>(&item)) {
      aggregate_calls.push_back(*aggregate_call);
    }
  }

  return aggregate_calls;
}

std::vector<std::size_t> extractProjectionIndices(
    const std::vector<BoundSelectItem>& bound_select_items) {
  std::vector<std::size_t> projection_indices;
  projection_indices.reserve(bound_select_items.size());
  for (const auto& item : bound_select_items) {
    const BoundColumnRef& column_ref = std::get<BoundColumnRef>(item);
    projection_indices.push_back(column_ref.column_index);
  }

  return projection_indices;
}

}  // namespace select_item