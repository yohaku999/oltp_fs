#include "execution/comparison_predicate.h"

#include <stdexcept>

#include "tuple/typed_row.h"

FieldValue resolveBoundOperand(const BoundOperand& operand,
                              const TypedRow& row) {
  if (const auto* column_ref = std::get_if<BoundColumnRef>(&operand)) {
    if (column_ref->source_index != 0) {
      throw std::runtime_error(
          "Single-table predicate references a non-zero source index.");
    }
    if (column_ref->column_index >= row.values.size()) {
      throw std::runtime_error("Predicate column index is out of range.");
    }
    return row.values[column_ref->column_index];
  }

  if (const auto* value = std::get_if<FieldValue>(&operand)) {
    return *value;
  }

  throw std::logic_error("Unsupported bound predicate operand.");
}

bool matchesPredicates(const TypedRow& row,
                       const std::vector<BoundComparisonPredicate>& predicates) {
  for (const auto& predicate : predicates) {
    const FieldValue left = resolveBoundOperand(predicate.left, row);
    const FieldValue right = resolveBoundOperand(predicate.right, row);
    switch (predicate.op) {
      case Op::Eq:
        if (left != right) {
          return false;
        }
        break;
      case Op::Gt:
        if (left <= right) {
          return false;
        }
        break;
      case Op::Ge:
        if (left < right) {
          return false;
        }
        break;
      case Op::Lt:
        if (left >= right) {
          return false;
        }
        break;
      case Op::Le:
        if (left > right) {
          return false;
        }
        break;
    }
  }

  return true;
}



// reorder predicates in key order. if the predicate is not value and colum combination, skip them because we can not use them for bounary definition. if the predicate is value and column combination but the column is not indexed, skip them as well.
std::vector<std::vector<BoundComparisonPredicate>> align_predicates_with_key_order(const std::vector<BoundComparisonPredicate>& predicates, const std::vector<std::size_t>& key_order_indexes) {
  std::vector<std::vector<BoundComparisonPredicate>> ordered_predicates;
  ordered_predicates.resize(key_order_indexes.size());
  for (const auto& predicate : predicates) {
    bool cannot_use_for_boundary = (std::get_if<BoundColumnRef>(&predicate.left) && std::get_if<BoundColumnRef>(&predicate.right)) || (std::get_if<FieldValue>(&predicate.left) && std::get_if<FieldValue>(&predicate.right));
    if (cannot_use_for_boundary) {
      continue;
    }
    const BoundColumnRef* column_ref = std::get_if<BoundColumnRef>(&predicate.left) ? std::get_if<BoundColumnRef>(&predicate.left) : std::get_if<BoundColumnRef>(&predicate.right);
    auto it = std::find(key_order_indexes.begin(), key_order_indexes.end(), column_ref->column_index);
    if (it == key_order_indexes.end()) {
      continue;
    }
    size_t key_index = std::distance(key_order_indexes.begin(), it);
    ordered_predicates[key_index].push_back(predicate);
  }
  return ordered_predicates;
}