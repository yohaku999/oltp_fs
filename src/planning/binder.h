#pragma once

#include <cstddef>
#include <optional>
#include <vector>

#include "catalog/table.h"
#include "execution/comparison_predicate.h"
#include "execution/order_by_spec.h"
#include "execution/select_item.h"
#include "execution/update_assignment.h"

class SelectParser;
class TableRepository;

namespace binder {

struct BoundSelectQuery {
  std::vector<Table> tables;
  std::vector<BoundSelectItem> select_items;
  std::vector<BoundComparisonPredicate> predicates;
  std::vector<std::vector<BoundComparisonPredicate>> table_predicates;
  std::vector<OrderBySpec> order_by_specs;
  std::optional<std::size_t> limit_count;
};

BoundSelectQuery bindSelectQuery(const SelectParser& parser,
                                 const TableRepository& catalog);

BoundColumnRef bindColumnRef(const ColumnRef& column_ref,
                             const std::vector<Table>& tables);

BoundOperand bindOperand(const UnboundOperand& operand,
                         const std::vector<Table>& tables);

std::vector<BoundSelectItem> bindSelectItems(
    const std::vector<UnboundSelectItem>& select_items,
    const std::vector<Table>& tables);

std::vector<BoundComparisonPredicate> bindPredicates(
    const std::vector<UnboundComparisonPredicate>& predicates,
    const std::vector<Table>& tables);

std::vector<BoundComparisonPredicate> bindPredicatesResolvableByTable(
    const std::vector<UnboundComparisonPredicate>& predicates,
    const Table& table);

std::vector<BoundUpdateAssignment> bindUpdateAssignments(
    const std::vector<UnboundUpdateAssignment>& assignments,
    const Table& table);

}  // namespace binder
