#include "update_parser.h"

#include <stdexcept>
#include <utility>

#include "execution/parsers/parser_ast_helpers.h"
#include "schema/schema.h"

UpdateParser::UpdateParser(std::string sql)
    : PgQueryJsonParser(std::move(sql)) {}

const nlohmann::json& UpdateParser::updateStatement() const {
    return statementNode().at("UpdateStmt");
}

std::string UpdateParser::extractTableName() const {
    return updateStatement()
      .at("relation")
      .at("relname")
      .get<std::string>();
}

std::vector<UnboundComparisonPredicate> UpdateParser::extractComparisonPredicates(
    const Schema& schema) const {
    return parseWhereClausePredicates(updateStatement(), schema);
}

std::vector<UnboundUpdateAssignment> UpdateParser::extractAssignments(
    const Schema& schema) const {
    const auto& target_list = updateStatement().at("targetList");
    std::vector<UnboundUpdateAssignment> assignments;
    assignments.reserve(target_list.size());

    for (const auto& target : target_list) {
        const auto& res_target = target.at("ResTarget");
        const std::string column_name = res_target.at("name").get<std::string>();
        const int column_index = schema.getColumnIndex(column_name);
        if (column_index < 0) {
            throw std::runtime_error("Unknown UPDATE column: " + column_name);
        }

        assignments.push_back(UnboundUpdateAssignment{
            column_name,
            parseUpdateAssignmentValue(res_target.at("val"), schema,
                                       column_name,
                                       schema.columns().at(static_cast<std::size_t>(column_index)).getType())});
    }

    return assignments;
}
