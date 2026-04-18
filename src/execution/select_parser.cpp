#include "select_parser.h"

#include <stdexcept>
#include <utility>

#include "execution/parser_ast_helpers.h"
#include "schema/schema.h"

SelectParser::SelectParser(std::string sql)
    : PgQueryJsonParser(std::move(sql)) {}

std::string SelectParser::extractTableName() const {
  return statementNode()
      .at("SelectStmt")
      .at("fromClause")
      .at(0)
      .at("RangeVar")
      .at("relname")
      .get<std::string>();
}

std::vector<std::size_t> SelectParser::extractProjectionIndices(
    const Schema& schema) const {
  const auto& targets = statementNode().at("SelectStmt").at("targetList");

  std::vector<std::size_t> projection_indices;
  for (const auto& target : targets) {
    const auto& fields = target.at("ResTarget")
                             .at("val")
                             .at("ColumnRef")
                             .at("fields");

    if (fields.size() == 1 && fields.at(0).contains("A_Star")) {
      projection_indices.clear();
      projection_indices.reserve(schema.columns().size());
      for (std::size_t index = 0; index < schema.columns().size(); ++index) {
        projection_indices.push_back(index);
      }
      return projection_indices;
    }

    const std::string column_name =
        fields.at(0).at("String").at("sval").get<std::string>();
    const int column_index = schema.getColumnIndex(column_name);
    if (column_index < 0) {
      throw std::runtime_error("Unknown projection column: " + column_name);
    }
    projection_indices.push_back(static_cast<std::size_t>(column_index));
  }

  return projection_indices;
}

std::vector<ComparisonPredicate> SelectParser::extractComparisonPredicates(
    const Schema& schema) const {
  return parseWhereClausePredicates(statementNode().at("SelectStmt"), schema);
}