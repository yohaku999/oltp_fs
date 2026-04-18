#pragma once

#include <string>
#include <vector>

#include <nlohmann/json.hpp>

extern "C" {
#include <pg_query.h>
}

class Schema;

class CreateTableParser {
 public:
  explicit CreateTableParser(std::string sql);
  ~CreateTableParser();

  // Delete copy and move constructors and assignment operators to prevent accidental copying or moving of the parser, which manages a C struct with manual memory management.
  CreateTableParser(const CreateTableParser&) = delete;
  CreateTableParser& operator=(const CreateTableParser&) = delete;
  CreateTableParser(CreateTableParser&&) = delete;
  CreateTableParser& operator=(CreateTableParser&&) = delete;

  std::string extractTableName() const;
  Schema extractSchema() const;

 private:
  PgQueryParseResult result_{};
  nlohmann::json parse_tree_;
};