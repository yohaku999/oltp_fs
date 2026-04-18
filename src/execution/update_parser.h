#pragma once

#include <string>

#include "tuple/typed_row.h"
#include "execution/pg_query_json_parser.h"

class Schema;

class UpdateParser : private PgQueryJsonParser {
 public:
  explicit UpdateParser(std::string sql);
  ~UpdateParser() = default;

  std::string extractTableName() const;
};