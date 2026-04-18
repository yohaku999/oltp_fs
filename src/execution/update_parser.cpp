#include "update_parser.h"

#include <stdexcept>
#include <utility>

#include "tuple/typed_row.h"

UpdateParser::UpdateParser(std::string sql)
    : PgQueryJsonParser(std::move(sql)) {}

std::string UpdateParser::extractTableName() const {
  return statementNode()
      .at("UpdateStmt")
      .at("relation")
      .at("relname")
      .get<std::string>();
}
