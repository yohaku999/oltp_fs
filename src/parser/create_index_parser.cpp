#include "create_index_parser.h"

#include <utility>

CreateIndexParser::CreateIndexParser(std::string sql)
    : PgQueryJsonParser(std::move(sql)) {}

std::string CreateIndexParser::extractIndexName() const {
  return statementNode().at("IndexStmt").at("idxname").get<std::string>();
}

std::string CreateIndexParser::extractTableName() const {
  return statementNode()
      .at("IndexStmt")
      .at("relation")
      .at("relname")
      .get<std::string>();
}

std::vector<std::string> CreateIndexParser::extractColumnNames() const {
  const auto& index_params = statementNode().at("IndexStmt").at("indexParams");

  std::vector<std::string> column_names;
  column_names.reserve(index_params.size());
  for (const auto& index_param : index_params) {
    column_names.push_back(
        index_param.at("IndexElem").at("name").get<std::string>());
  }
  return column_names;
}