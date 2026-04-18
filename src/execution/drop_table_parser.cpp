#include <stdexcept>
#include <utility>

#include "drop_table_parser.h"

DropTableParser::DropTableParser(std::string sql) {
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

DropTableParser::~DropTableParser() { pg_query_free_parse_result(result_); }

std::string DropTableParser::extractTableName() const {
  return parse_tree_.at("stmts")
      .at(0)
      .at("stmt")
      .at("DropStmt")
      .at("objects")
      .at(0)
  .at("List")
  .at("items")
  .at(0)
      .at("String")
  .at("sval")
  .get<std::string>();
}