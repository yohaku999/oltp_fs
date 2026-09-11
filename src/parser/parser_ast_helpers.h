#pragma once

#include <nlohmann/json.hpp>
#include <vector>

#include "execution/comparison_predicate.h"
#include "execution/update_assignment.h"
#include "parser/query_parameters.h"
#include "tuple/field_value.h"

class Schema;

ColumnRef parseColumnRef(const nlohmann::json& column_ref);

int parseConstIntegerValue(const nlohmann::json& item);

int parseIntegerValue(const nlohmann::json& item,
                      const QueryParameters& parameters);

FieldValue parseConstFieldValue(const nlohmann::json& item,
                                Column::Type column_type);

FieldValue parseFieldValue(const nlohmann::json& item, Column::Type column_type,
                           const QueryParameters& parameters);

UnboundUpdateValue parseUpdateAssignmentValue(
    const nlohmann::json& expression, const Schema& schema,
    const std::string& target_column_name, Column::Type target_type,
    const QueryParameters& parameters);

std::vector<UnboundComparisonPredicate> parseWhereClausePredicates(
    const nlohmann::json& statement, const Schema& schema,
    const QueryParameters& parameters);
