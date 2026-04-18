#include "create_table_parser.h"

#include <stdexcept>
#include <utility>

#include "schema/schema.h"

CreateTableParser::CreateTableParser(std::string sql) {
  result_ = pg_query_parse(sql.c_str());
  if (result_.error != nullptr) {
    const std::string message = result_.error->message != nullptr
                                    ? result_.error->message
                                    : "unknown parse error";
    pg_query_free_parse_result(result_);
    throw std::runtime_error("parse error: " + message);
  }

  try {
    parse_tree_ = nlohmann::json::parse(result_.parse_tree);
  } catch (...) {
    pg_query_free_parse_result(result_);
    throw;
  }
}

CreateTableParser::~CreateTableParser() { pg_query_free_parse_result(result_); }

std::string CreateTableParser::extractTableName() const {
  return parse_tree_.at("stmts")
      .at(0)
      .at("stmt")
      .at("CreateStmt")
      .at("relation")
      .at("relname")
      .get<std::string>();
}

Schema CreateTableParser::extractSchema() const {
  const auto& column_definitions = parse_tree_.at("stmts")
                                      .at(0)
                                      .at("stmt")
                                      .at("CreateStmt")
                                      .at("tableElts");

  std::vector<Column> columns;
  for (const auto& column_definition : column_definitions) {
    const auto& column_name =
        column_definition.at("ColumnDef").at("colname").get<std::string>();
    const auto& type_name =
        column_definition.at("ColumnDef")
            .at("typeName")
            .at("names")
            .at(1)
            .at("String")
            .at("sval")
            .get<std::string>();
    Column::Type column_type;
    if (type_name == "numeric" || type_name == "int4") {
      column_type = Column::Type::Integer;
    } else if (type_name == "varchar") {
      column_type = Column::Type::Varchar;
    } else {
      throw std::runtime_error(
          "Unsupported type identifier for column " + column_name + ": " +
          type_name);
    }
    columns.emplace_back(column_name, column_type);
  }

  return Schema(columns);
}