#include "execution/parsers/parser_ast_helpers.h"
#include "execution/comparison_predicate.h"
#include <stdexcept>
#include <string>

#include "schema/schema.h"

namespace {

Column::Type requireColumnType(const nlohmann::json& expression,
                               const Schema& schema);
UnboundOperand parseOperand(const nlohmann::json& expression,
                            const Schema& schema,
                            Column::Type constant_type);

Op parseComparisonOp(const nlohmann::json& expr) {
  const std::string op =
      expr.at("name").at(0).at("String").at("sval").get<std::string>();
  if (op == "=") {
    return Op::Eq;
  }
  if (op == ">") {
    return Op::Gt;
  }
  if (op == ">=") {
    return Op::Ge;
  }
  if (op == "<") {
    return Op::Lt;
  }
  if (op == "<=") {
    return Op::Le;
  }
  throw std::runtime_error("Unsupported comparison operator: " + op);
}

void extractPredicatesFromNode(const nlohmann::json& node, const Schema& schema,
                               std::vector<UnboundComparisonPredicate>& out) {
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
  const auto& lhs = expr.at("lexpr");
  const auto& rhs = expr.at("rexpr");
  const bool lhs_is_column = lhs.contains("ColumnRef");
  const bool rhs_is_column = rhs.contains("ColumnRef");

  if (!lhs_is_column && !rhs_is_column) {
    throw std::runtime_error(
        "At least one side of a predicate must reference a column.");
  }

  const Column::Type constant_type = lhs_is_column
                                         ? requireColumnType(lhs, schema)
                                         : requireColumnType(rhs, schema);
  out.push_back(UnboundComparisonPredicate{
      parseComparisonOp(expr), parseOperand(lhs, schema, constant_type),
      parseOperand(rhs, schema, constant_type)});
}

ColumnRef parseColumnRef(const nlohmann::json& column_ref) {
  const auto& fields = column_ref.at("fields");
  if (fields.size() == 1) {
    return ColumnRef{"", fields.at(0).at("String").at("sval").get<std::string>()};
  }
  if (fields.size() == 2) {
    return ColumnRef{fields.at(0).at("String").at("sval").get<std::string>(),
                     fields.at(1).at("String").at("sval").get<std::string>()};
  }
  throw std::runtime_error("Unsupported column reference shape.");
}

Column::Type requireColumnType(const nlohmann::json& expression,
                               const Schema& schema) {
  if (!expression.contains("ColumnRef")) {
    throw std::runtime_error("Expected a column reference operand.");
  }

  const ColumnRef column_ref = parseColumnRef(expression.at("ColumnRef"));
  const int column_index = schema.getColumnIndex(column_ref.column_name);
  if (column_index < 0) {
    throw std::runtime_error("Unknown predicate column: " +
                             column_ref.column_name);
  }

  return schema.columns().at(column_index).getType();
}

UnboundOperand parseOperand(const nlohmann::json& expression,
                            const Schema& schema,
                            Column::Type constant_type) {
  if (expression.contains("ColumnRef")) {
    const ColumnRef column_ref = parseColumnRef(expression.at("ColumnRef"));
    const int column_index = schema.getColumnIndex(column_ref.column_name);
    if (column_index < 0) {
      throw std::runtime_error("Unknown predicate column: " +
                               column_ref.column_name);
    }
    return column_ref;
  }

  if (expression.contains("A_Const")) {
    return parseConstFieldValue(expression, constant_type);
  }

  throw std::runtime_error("Unsupported predicate operand.");
}

}

ColumnRef parseColumnRef(const nlohmann::json& column_ref) {
  const auto& fields = column_ref.at("fields");
  if (fields.size() == 1) {
    return ColumnRef{"", fields.at(0).at("String").at("sval").get<std::string>()};
  }
  if (fields.size() == 2) {
    return ColumnRef{fields.at(0).at("String").at("sval").get<std::string>(),
                     fields.at(1).at("String").at("sval").get<std::string>()};
  }

  throw std::runtime_error("Unsupported column reference shape.");
}

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

std::vector<UnboundComparisonPredicate> parseWhereClausePredicates(
    const nlohmann::json& statement, const Schema& schema) {
  if (!statement.contains("whereClause")) {
    return {};
  }

  std::vector<UnboundComparisonPredicate> predicates;
  extractPredicatesFromNode(statement.at("whereClause"), schema, predicates);
  return predicates;
}