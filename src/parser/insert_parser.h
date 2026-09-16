#pragma once

#include <string>
#include <vector>

#include "parser/pg_query_json_parser.h"
#include "parser/query_parameters.h"

class Schema;

class InsertParser : private PgQueryJsonParser {
 public:
  using PgQueryJsonParser::sqlTemplate;

  explicit InsertParser(std::string sql);
  ~InsertParser() = default;

  std::string extractTableName() const;
  std::vector<PreparedFieldValue> extractPreparedValues(
      const Schema& schema) const;
};
