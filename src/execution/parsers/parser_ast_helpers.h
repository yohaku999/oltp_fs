#pragma once

#include <vector>

#include <nlohmann/json.hpp>

#include "execution/comparison_predicate.h"
#include "execution/update_assignment.h"
#include "tuple/field_value.h"

class Schema;

ColumnRef parseColumnRef(const nlohmann::json& column_ref);

FieldValue parseConstFieldValue(const nlohmann::json& item,
                               Column::Type column_type);

UnboundUpdateValue parseUpdateAssignmentValue(
    const nlohmann::json& expression, const Schema& schema,
    const std::string& target_column_name, Column::Type target_type);

std::vector<UnboundComparisonPredicate> parseWhereClausePredicates(
    const nlohmann::json& statement, const Schema& schema);