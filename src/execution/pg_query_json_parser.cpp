#include "execution/pg_query_json_parser.h"

#include <stdexcept>
#include <utility>

PgQueryJsonParser::PgQueryJsonParser(std::string sql) {
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

PgQueryJsonParser::~PgQueryJsonParser() { pg_query_free_parse_result(result_); }

const nlohmann::json& PgQueryJsonParser::statementNode() const {
  return parse_tree_.at("stmts").at(0).at("stmt");
}