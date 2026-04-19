#include "update_parser.h"

#include <stdexcept>
#include <utility>

#include "execution/parsers/parser_ast_helpers.h"
#include "schema/schema.h"
#include "tuple/typed_row.h"

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

std::vector<ComparisonPredicate> UpdateParser::extractComparisonPredicates(
    const Schema& schema) const {
    return parseWhereClausePredicates(updateStatement(), schema);
}

TypedRow UpdateParser::extractUpdatedRow(const Schema& schema,
                                                                                const TypedRow& original_row) const {
    if (schema.columns().size() != original_row.values.size()) {
        throw std::runtime_error(
                "Schema column count and original row value count must match.");
    }

    TypedRow updated_row = original_row;
    const auto& target_list = updateStatement().at("targetList");
    for (const auto& target : target_list) {
        const auto& res_target = target.at("ResTarget");
        const std::string column_name = res_target.at("name").get<std::string>();
        const int column_index = schema.getColumnIndex(column_name);
        if (column_index < 0) {
            throw std::runtime_error("Unknown UPDATE column: " + column_name);
        }

        updated_row.values[static_cast<std::size_t>(column_index)] =
                parseConstFieldValue(res_target.at("val"),
                                                         schema.columns().at(column_index).getType());
    }

    return updated_row;
}
