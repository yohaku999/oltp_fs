#pragma once

#include <string>
#include <vector>

#include "execution/pg_query_json_parser.h"

class Schema;

class DropTableParser : private PgQueryJsonParser {
 public:
  explicit DropTableParser(std::string sql);
  ~DropTableParser() = default;

  std::string extractTableName() const;
};