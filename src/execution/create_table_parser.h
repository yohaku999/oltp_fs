#pragma once

#include <string>
#include <vector>

#include "execution/pg_query_json_parser.h"

class Schema;

class CreateTableParser : private PgQueryJsonParser {
 public:
  explicit CreateTableParser(std::string sql);
  ~CreateTableParser() = default;

  std::string extractTableName() const;
  Schema extractSchema() const;
};