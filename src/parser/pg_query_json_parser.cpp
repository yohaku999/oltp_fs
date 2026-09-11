#include "parser/pg_query_json_parser.h"

#include <stdexcept>
#include <string>
#include <utility>

extern "C" {
#include <pg_query.h>
}

namespace {

std::string toPostgresParameterSyntax(const std::string& sql_template) {
  std::string sql;
  sql.reserve(sql_template.size());
  std::size_t parameter_number = 1;
  bool in_single_quoted_string = false;

  for (std::size_t index = 0; index < sql_template.size(); ++index) {
    const char current = sql_template[index];
    if (current == '\'') {
      sql.push_back(current);
      if (in_single_quoted_string && index + 1 < sql_template.size() &&
          sql_template[index + 1] == '\'') {
        sql.push_back(sql_template[++index]);
        continue;
      }
      in_single_quoted_string = !in_single_quoted_string;
      continue;
    }

    if (current == '?' && !in_single_quoted_string) {
      sql += "$" + std::to_string(parameter_number++);
      continue;
    }

    sql.push_back(current);
  }

  return sql;
}

}  // namespace

PgQueryJsonParser::PgQueryJsonParser(std::string sql_template) {
  const std::string sql = toPostgresParameterSyntax(sql_template);
  PgQueryParseResult result = pg_query_parse(sql.c_str());
  if (result.error != nullptr) {
    const std::string message = result.error->message != nullptr
                                    ? result.error->message
                                    : "unknown parse error";
    pg_query_free_parse_result(result);
    throw std::runtime_error("parse error: " + message);
  }

  try {
    parse_tree_ = nlohmann::json::parse(result.parse_tree);
  } catch (...) {
    pg_query_free_parse_result(result);
    throw;
  }
  pg_query_free_parse_result(result);
}

const nlohmann::json& PgQueryJsonParser::statementNode() const {
  return parse_tree_.at("stmts").at(0).at("stmt");
}
