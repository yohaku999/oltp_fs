#include "insert_parser.h"

#include <unordered_set>

#include <stdexcept>
#include <utility>

#include "execution/parsers/parser_ast_helpers.h"
#include "schema/schema.h"
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

TypedRow InsertParser::extractRow(const Schema& schema) const {
  const auto& insert_stmt = statementNode().at("InsertStmt");
  const auto& items = insert_stmt.at("selectStmt")
                          .at("SelectStmt")
                          .at("valuesLists")
                          .at(0)
                          .at("List")
                          .at("items");

  std::vector<std::size_t> target_column_indexes;
  if (insert_stmt.contains("cols")) {
    // For column specific insert statements.
    const auto& cols = insert_stmt.at("cols");
    if (cols.size() != items.size()) {
      throw std::runtime_error(
          "INSERT column count must match VALUES item count.");
    }

    std::unordered_set<std::string> seen_column_names;
    target_column_indexes.reserve(cols.size());
    for (const auto& col : cols) {
      const std::string column_name =
          col.at("ResTarget").at("name").get<std::string>();
      if (!seen_column_names.insert(column_name).second) {
        throw std::runtime_error("Duplicate INSERT column: " + column_name);
      }

      const int column_index = schema.getColumnIndex(column_name);
      if (column_index < 0) {
        throw std::runtime_error("Unknown INSERT column: " + column_name);
      }
      target_column_indexes.push_back(static_cast<std::size_t>(column_index));
    }
  } else {
    // For non-column specific insert statements.
    if (schema.columns().size() != items.size()) {
      throw std::runtime_error(
          "INSERT VALUES item count must match schema column count.");
    }

    target_column_indexes.reserve(schema.columns().size());
    for (std::size_t index = 0; index < schema.columns().size(); ++index) {
      target_column_indexes.push_back(index);
    }
  }

  TypedRow row;
  row.values.assign(schema.columns().size(), std::monostate{});
  for (std::size_t item_index = 0; item_index < items.size(); ++item_index) {
    const std::size_t column_index = target_column_indexes.at(item_index);
    row.values[column_index] = parseConstFieldValue(
        items.at(item_index), schema.columns().at(column_index).getType());
  }

  return row;
}