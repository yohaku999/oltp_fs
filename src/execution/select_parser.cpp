#include "select_parser.h"

#include <stdexcept>
#include <utility>

#include "schema/schema.h"

SelectParser::SelectParser(std::string sql) {
  result_ = pg_query_parse(sql.c_str());
  if (result_.error != nullptr) {
    const std::string message = result_.error->message != nullptr
                                    ? result_.error->message
                                    : "unknown parse error";
    pg_query_free_parse_result(result_);
    throw std::runtime_error("parse error: " + message);
  }

  try {
    parse_tree_ = nlohmann::json::parse(result_.parse_tree);
  } catch (...) {
    pg_query_free_parse_result(result_);
    throw;
  }
}

SelectParser::~SelectParser() { pg_query_free_parse_result(result_); }

std::string SelectParser::extractTableName() const {
  return parse_tree_.at("stmts")
      .at(0)
      .at("stmt")
      .at("SelectStmt")
      .at("fromClause")
      .at(0)
      .at("RangeVar")
      .at("relname")
      .get<std::string>();
}

std::vector<std::size_t> SelectParser::extractProjectionIndices(
    const Schema& schema) const {
  const auto& targets = parse_tree_.at("stmts")
                            .at(0)
                            .at("stmt")
                            .at("SelectStmt")
                            .at("targetList");

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