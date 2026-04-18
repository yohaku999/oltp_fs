#include "insert_parser.h"

#include <stdexcept>
#include <utility>

#include "catalog/table.h"
#include "execution/parser_ast_helpers.h"
#include "tuple/typed_row.h"

InsertParser::InsertParser(std::string sql)
    : PgQueryJsonParser(std::move(sql)) {}

std::string InsertParser::extractTableName() const {
  return statementNode()
      .at("InsertStmt")
      .at("relation")
      .at("relname")
      .get<std::string>();
}

TypedRow InsertParser::extractRow() const {
  const auto& items = statementNode()
                          .at("InsertStmt")
                          .at("selectStmt")
                          .at("SelectStmt")
                          .at("valuesLists")
                          .at(0)
                          .at("List")
                          .at("items");

  Table table = Table::getTable(extractTableName());
  const auto& columns = table.schema().columns();

  TypedRow row;
  row.values.reserve(columns.size());
  for (std::size_t index = 0; index < columns.size(); ++index) {
    row.values.push_back(
        parseConstFieldValue(items.at(index), columns[index].getType()));
  }

  return row;
}