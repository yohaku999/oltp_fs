#include "execution/binder.h"

#include <stdexcept>

namespace binder {

BoundColumnRef bindColumnRef(const ColumnRef& column_ref,
                             const std::vector<Table>& tables) {
  std::size_t joined_column_offset = 0;
  const Table* matched_table = nullptr;

  for (const auto& table : tables) {
    if (!column_ref.table_name.empty() &&
        column_ref.table_name != table.name()) {
      joined_column_offset += table.schema().columns().size();
      continue;
    }

    const int column_index =
        table.schema().getColumnIndex(column_ref.column_name);
    if (column_index < 0) {
      joined_column_offset += table.schema().columns().size();
      continue;
    }

    if (matched_table != nullptr && column_ref.table_name.empty()) {
      throw std::runtime_error("Ambiguous column: " + column_ref.column_name);
    }

    matched_table = &table;
    return BoundColumnRef{0, joined_column_offset +
                                 static_cast<std::size_t>(column_index)};
  }

  if (!column_ref.table_name.empty()) {
    throw std::runtime_error("Unknown column on table " +
                             column_ref.table_name + ": " +
                             column_ref.column_name);
  }
  throw std::runtime_error("Unknown column: " + column_ref.column_name);
}

BoundOperand bindOperand(const UnboundOperand& operand,
                         const std::vector<Table>& tables) {
  if (const auto* column_ref = std::get_if<ColumnRef>(&operand)) {
    return bindColumnRef(*column_ref, tables);
  }

  if (const auto* value = std::get_if<FieldValue>(&operand)) {
    return *value;
  }

  return std::monostate{};
}

std::vector<BoundSelectItem> bindSelectItems(
    const std::vector<UnboundSelectItem>& select_items,
    const std::vector<Table>& tables) {
  std::vector<BoundSelectItem> bound_select_items;

  std::size_t joined_column_offset = 0;
  for (const auto& item : select_items) {
    if (std::holds_alternative<SelectAllItem>(item)) {
      joined_column_offset = 0;
      for (const auto& table : tables) {
        const std::size_t column_count = table.schema().columns().size();
        for (std::size_t column_index = 0; column_index < column_count;
             ++column_index) {
          bound_select_items.push_back(
              BoundColumnRef{0, joined_column_offset + column_index});
        }
        joined_column_offset += column_count;
      }
      continue;
    }

    if (const auto* column_ref = std::get_if<ColumnRef>(&item)) {
      bound_select_items.push_back(bindColumnRef(*column_ref, tables));
      continue;
    }

    const auto& aggregate_call = std::get<UnboundAggregateCall>(item);
    bound_select_items.push_back(BoundAggregateCall{
        aggregate_call.function,
        bindColumnRef(aggregate_call.argument, tables)});
  }

  return bound_select_items;
}

std::vector<BoundComparisonPredicate> bindPredicates(
    const std::vector<UnboundComparisonPredicate>& predicates,
    const std::vector<Table>& tables) {
  std::vector<BoundComparisonPredicate> bound_predicates;
  bound_predicates.reserve(predicates.size());

  for (const auto& predicate : predicates) {
    bound_predicates.push_back(BoundComparisonPredicate{
        predicate.op, bindOperand(predicate.left, tables),
        bindOperand(predicate.right, tables)});
  }

  return bound_predicates;
}

}  // namespace binder