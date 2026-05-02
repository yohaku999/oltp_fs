#include "create_table_parser.h"

#include <stdexcept>
#include <utility>

#include "schema/schema.h"

CreateTableParser::CreateTableParser(std::string sql)
    : PgQueryJsonParser(std::move(sql)) {}

std::string CreateTableParser::extractTableName() const {
  return statementNode()
      .at("CreateStmt")
      .at("relation")
      .at("relname")
      .get<std::string>();
}

Schema CreateTableParser::extractSchema() const {
  const auto& column_definitions =
      statementNode().at("CreateStmt").at("tableElts");

  std::vector<Column> columns;
  for (const auto& column_definition : column_definitions) {
    if (!column_definition.contains("ColumnDef")) {
      continue;
    }

    const auto& column_name =
        column_definition.at("ColumnDef").at("colname").get<std::string>();
    const auto& type_name =
        column_definition.at("ColumnDef")
            .at("typeName")
            .at("names")
            .back()
            .at("String")
            .at("sval")
            .get<std::string>();
    Column::Type column_type;
    if (type_name == "int4") {
      column_type = Column::Type::Integer;
    } else if (type_name == "numeric" || type_name == "float4" ||
               type_name == "float8") {
      column_type = Column::Type::Double;
    } else if (type_name == "varchar" || type_name == "bpchar" ||
               type_name == "timestamp") {
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