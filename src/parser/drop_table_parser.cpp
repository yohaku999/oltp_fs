#include "drop_table_parser.h"

#include <stdexcept>
#include <utility>

DropTableParser::DropTableParser(std::string sql)
    : PgQueryJsonParser(std::move(sql)) {}

std::string DropTableParser::extractTableName() const {
  return statementNode()
      .at("DropStmt")
      .at("objects")
      .at(0)
      .at("List")
      .at("items")
      .at(0)
      .at("String")
      .at("sval")
      .get<std::string>();
}