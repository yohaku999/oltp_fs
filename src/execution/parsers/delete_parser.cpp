#include "delete_parser.h"

#include <utility>

#include "execution/parsers/parser_ast_helpers.h"
#include "schema/schema.h"

DeleteParser::DeleteParser(std::string sql)
    : PgQueryJsonParser(std::move(sql)) {}

const nlohmann::json& DeleteParser::deleteStatement() const {
  return statementNode().at("DeleteStmt");
}

std::string DeleteParser::extractTableName() const {
  return deleteStatement().at("relation").at("relname").get<std::string>();
}

std::vector<ComparisonPredicate> DeleteParser::extractComparisonPredicates(
    const Schema& schema) const {
  return parseWhereClausePredicates(deleteStatement(), schema);
}