#pragma once

#include <string>
#include <vector>

#include "execution/parsers/pg_query_json_parser.h"

class CreateIndexParser : private PgQueryJsonParser {
 public:
  explicit CreateIndexParser(std::string sql);
  ~CreateIndexParser() = default;

  std::string extractIndexName() const;
  std::string extractTableName() const;
  std::vector<std::string> extractColumnNames() const;
};