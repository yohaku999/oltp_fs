#pragma once

#include <vector>

#include "catalog/table.h"
#include "execution/comparison_predicate.h"
#include "execution/select_item.h"
#include "execution/update_assignment.h"

namespace binder {

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

std::vector<BoundUpdateAssignment> bindUpdateAssignments(
    const std::vector<UnboundUpdateAssignment>& assignments,
    const Table& table);

}  // namespace binder