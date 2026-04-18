#pragma once

#include <string>
#include <vector>

#include <nlohmann/json.hpp>

extern "C" {
#include <pg_query.h>
}

class Schema;

class DropTableParser {
 public:
  explicit DropTableParser(std::string sql);
  ~DropTableParser();

  // Delete copy and move constructors and assignment operators to prevent accidental copying or moving of the parser, which manages a C struct with manual memory management.
  DropTableParser(const DropTableParser&) = delete;
  DropTableParser& operator=(const DropTableParser&) = delete;
  DropTableParser(DropTableParser&&) = delete;
  DropTableParser& operator=(DropTableParser&&) = delete;

  std::string extractTableName() const;

 private:
  PgQueryParseResult result_{};
  nlohmann::json parse_tree_;
};