#pragma once

#include <string>
#include <vector>

#include "tuple/typed_row.h"
#include "execution/parsers/pg_query_json_parser.h"

class Schema;

class InsertParser : private PgQueryJsonParser {
 public:
  explicit InsertParser(std::string sql);
  ~InsertParser() = default;

  std::string extractTableName() const;
  TypedRow extractRow(const Schema& schema) const;
};