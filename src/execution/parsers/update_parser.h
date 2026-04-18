#pragma once

#include <string>
#include <vector>

#include "tuple/typed_row.h"
#include "execution/parsers/pg_query_json_parser.h"

class Schema;

class UpdateParser : private PgQueryJsonParser {
 public:
  explicit UpdateParser(std::string sql);
  ~UpdateParser() = default;

  std::string extractTableName() const;

  int extractTargetKey(const Schema& schema) const;
  TypedRow extractUpdatedRow(const Schema& schema,
                             const TypedRow& original_row) const;

 private:
  const nlohmann::json& updateStatement() const;
};