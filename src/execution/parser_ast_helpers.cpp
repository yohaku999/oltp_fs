#include "execution/parser_ast_helpers.h"

#include <stdexcept>
#include <string>

#include "schema/schema.h"

namespace {

ComparisonPredicate::Op parseComparisonOp(const nlohmann::json& expr) {
  const std::string op =
      expr.at("name").at(0).at("String").at("sval").get<std::string>();
  if (op == "=") {
    return ComparisonPredicate::Op::Eq;
  }
  if (op == ">") {
    return ComparisonPredicate::Op::Gt;
  }
  if (op == ">=") {
    return ComparisonPredicate::Op::Ge;
  }
  if (op == "<") {
    return ComparisonPredicate::Op::Lt;
  }
  if (op == "<=") {
    return ComparisonPredicate::Op::Le;
  }
  throw std::runtime_error("Unsupported comparison operator: " + op);
}

void extractPredicatesFromNode(const nlohmann::json& node, const Schema& schema,
                               std::vector<ComparisonPredicate>& out) {
  if (node.contains("BoolExpr")) {
    const auto& bool_expr = node.at("BoolExpr");
    const std::string op = bool_expr.at("boolop").get<std::string>();
    if (op != "AND_EXPR") {
      throw std::runtime_error("Only AND predicates are supported.");
    }
    for (const auto& child : bool_expr.at("args")) {
      extractPredicatesFromNode(child, schema, out);
    }
    return;
  }

  const auto& expr = node.at("A_Expr");
  const std::string column_name = expr.at("lexpr")
                                      .at("ColumnRef")
                                      .at("fields")
                                      .at(0)
                                      .at("String")
                                      .at("sval")
                                      .get<std::string>();
  const int column_index = schema.getColumnIndex(column_name);
  if (column_index < 0) {
    throw std::runtime_error("Unknown predicate column: " + column_name);
  }

  const auto& rhs = expr.at("rexpr");
  if (!rhs.contains("A_Const")) {
    throw std::runtime_error("Unsupported predicate rhs.");
  }

  out.push_back(ComparisonPredicate{
      static_cast<std::size_t>(column_index), parseComparisonOp(expr),
      parseConstFieldValue(rhs, schema.columns().at(column_index).getType())});
}

}  // namespace

FieldValue parseConstFieldValue(const nlohmann::json& item,
                               Column::Type column_type) {
  const auto& constant = item.at("A_Const");
  if (constant.value("isnull", false)) {
    return std::monostate{};
  }

  switch (column_type) {
    case Column::Type::Integer:
      if (constant.contains("ival")) {
        return static_cast<Column::IntegerType>(
            constant.at("ival").at("ival").get<int>());
      }
      if (constant.contains("fval")) {
        return static_cast<Column::IntegerType>(std::stod(
            constant.at("fval").at("fval").get<std::string>()));
      }
      throw std::runtime_error("Unsupported constant for Integer column");
    case Column::Type::Varchar:
      if (constant.contains("sval")) {
        return constant.at("sval").at("sval").get<std::string>();
      }
      if (constant.contains("ival")) {
        return std::to_string(constant.at("ival").at("ival").get<int>());
      }
      if (constant.contains("fval")) {
        return constant.at("fval").at("fval").get<std::string>();
      }
      throw std::runtime_error("Unsupported constant for Varchar column");
  }

  throw std::runtime_error("Unknown column type in parser AST helper");
}

std::vector<ComparisonPredicate> parseWhereClausePredicates(
    const nlohmann::json& statement, const Schema& schema) {
  if (!statement.contains("whereClause")) {
    return {};
  }

  std::vector<ComparisonPredicate> predicates;
  extractPredicatesFromNode(statement.at("whereClause"), schema, predicates);
  return predicates;
}