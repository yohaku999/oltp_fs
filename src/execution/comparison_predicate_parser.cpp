#include "execution/comparison_predicate_parser.h"

#include <stdexcept>
#include <string>

#include "schema/schema.h"

std::vector<ComparisonPredicate> ComparisonPredicateParser::parse(
    const nlohmann::json& node, const Schema& schema) {
  std::vector<ComparisonPredicate> predicates;
  extractFromNode(node, schema, predicates);
  return predicates;
}

void ComparisonPredicateParser::extractFromNode(
    const nlohmann::json& node, const Schema& schema,
    std::vector<ComparisonPredicate>& out) {
  if (node.contains("BoolExpr")) {
    const auto& bool_expr = node.at("BoolExpr");
    const std::string op = bool_expr.at("boolop").get<std::string>();
    if (op != "AND_EXPR") {
      throw std::runtime_error("Only AND predicates are supported.");
    }
    for (const auto& child : bool_expr.at("args")) {
      extractFromNode(child, schema, out);
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

  out.push_back(ComparisonPredicate{static_cast<std::size_t>(column_index),
                                    parseComparisonOp(expr),
                                    parsePredicateValue(expr)});
}

ComparisonPredicate::Op ComparisonPredicateParser::parseComparisonOp(
    const nlohmann::json& expr) {
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

FieldValue ComparisonPredicateParser::parsePredicateValue(
    const nlohmann::json& expr) {
  const auto& rhs = expr.at("rexpr");
  if (!rhs.contains("A_Const")) {
    throw std::runtime_error("Unsupported predicate rhs.");
  }

  const auto& constant = rhs.at("A_Const");
  if (constant.contains("ival")) {
    return Column::IntegerType(constant.at("ival").at("ival").get<int>());
  }
  if (constant.contains("sval")) {
    return Column::VarcharType(
        constant.at("sval").at("sval").get<std::string>());
  }
  throw std::runtime_error("Unsupported predicate constant type.");
}