#include "select_parser.h"

#include <stdexcept>
#include <utility>

#include "execution/parsers/parser_ast_helpers.h"
#include "schema/schema.h"

SelectParser::SelectParser(std::string sql)
    : PgQueryJsonParser(std::move(sql)) {}

std::vector<std::string> SelectParser::extractTableNames() const {
  const auto& from_clause = statementNode().at("SelectStmt").at("fromClause");
  std::vector<std::string> table_names;
  for (const auto& entry : from_clause) {
    const std::string table_name = entry.at("RangeVar").at("relname").get<std::string>();
    table_names.push_back(table_name);
  }
  return table_names;
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

std::vector<OrderBySpec> SelectParser::extractOrderBySpecs(
    const Schema& schema) const {
  const auto& select_stmt = statementNode().at("SelectStmt");
  if (!select_stmt.contains("sortClause")) {
    return {};
  }

  const auto& sort_clause = select_stmt.at("sortClause");
  std::vector<OrderBySpec> order_by_specs;
  order_by_specs.reserve(sort_clause.size());
  for (const auto& entry : sort_clause) {
    const auto& sort_by = entry.at("SortBy");
    const auto& fields = sort_by.at("node")
                             .at("ColumnRef")
                             .at("fields");
    const std::string column_name =
        fields.at(0).at("String").at("sval").get<std::string>();
    const int column_index = schema.getColumnIndex(column_name);
    if (column_index < 0) {
      throw std::runtime_error("Unknown ORDER BY column: " + column_name);
    }

    const std::string direction =
        sort_by.value("sortby_dir", "SORTBY_DEFAULT");
    OrderByDirection order_by_direction;
    if (direction == "SORTBY_DEFAULT" || direction == "SORTBY_ASC") {
      order_by_direction = OrderByDirection::Asc;
    } else if (direction == "SORTBY_DESC") {
      order_by_direction = OrderByDirection::Desc;
    } else {
      throw std::runtime_error("Unsupported ORDER BY direction: " + direction);
    }

    order_by_specs.push_back(OrderBySpec{static_cast<std::size_t>(column_index),
                                         order_by_direction});
  }

  return order_by_specs;
}

std::optional<std::size_t> SelectParser::extractLimitCount() const {
  const auto& select_stmt = statementNode().at("SelectStmt");
  const auto limit_count_it = select_stmt.find("limitCount");
  if (limit_count_it == select_stmt.end()) {
    return std::nullopt;
  }
  const auto& limit_count = *limit_count_it;
  const int limit = limit_count.at("A_Const").at("ival").at("ival").get<int>();
  return static_cast<std::size_t>(limit);
}

std::vector<ComparisonPredicate> SelectParser::extractComparisonPredicates(
    const Schema& schema) const {
  return parseWhereClausePredicates(statementNode().at("SelectStmt"), schema);
}