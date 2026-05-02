#include "select_parser.h"

#include <algorithm>
#include <cctype>
#include <stdexcept>
#include <string>
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

std::vector<UnboundSelectItem> SelectParser::extractSelectItems() const {
  const auto& targets = statementNode().at("SelectStmt").at("targetList");

  std::vector<UnboundSelectItem> select_items;
  select_items.reserve(targets.size());
  for (const auto& target : targets) {
    const auto& value = target.at("ResTarget").at("val");
    if (value.contains("ColumnRef")) {
      const auto& column_ref = value.at("ColumnRef");
      const auto& fields = column_ref.at("fields");

      if (fields.size() == 1 && fields.at(0).contains("A_Star")) {
        select_items.push_back(SelectAllItem{});
        continue;
      }

      select_items.push_back(parseColumnRef(column_ref));
      continue;
    }

    if (value.contains("FuncCall")) {
      const auto& func_call = value.at("FuncCall");
      const auto& func_name_node = func_call.at("funcname");

      std::string function_name =
          func_name_node.at(0).at("String").at("sval").get<std::string>();
      std::transform(function_name.begin(), function_name.end(),
                     function_name.begin(),
                     [](unsigned char ch) {
                       return static_cast<char>(std::tolower(ch));
                     });
      if (function_name != "sum") {
        throw std::runtime_error("Unsupported aggregate function: " +
                                 function_name);
      }

      const auto& args = func_call.at("args");
      select_items.push_back(UnboundAggregateCall{
          AggregateFunction::Sum, parseColumnRef(args.at(0).at("ColumnRef"))});
      continue;
    }

    throw std::runtime_error("Unsupported select target.");
  }

  return select_items;
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
    const ColumnRef column_ref =
        parseColumnRef(sort_by.at("node").at("ColumnRef"));
    const int column_index = schema.getColumnIndex(column_ref.column_name);
    if (column_index < 0) {
      throw std::runtime_error("Unknown ORDER BY column: " +
                               column_ref.column_name);
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

std::vector<UnboundComparisonPredicate> SelectParser::extractComparisonPredicates(
    const Schema& schema) const {
  return parseWhereClausePredicates(statementNode().at("SelectStmt"), schema);
}