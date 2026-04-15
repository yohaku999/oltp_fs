#pragma once

#include <string>
#include <vector>

#include <nlohmann/json.hpp>

extern "C" {
#include <pg_query.h>
}

class Schema;

#include "execution/comparison_predicate.h"

class SelectParser {
 public:
  explicit SelectParser(std::string sql);
  ~SelectParser();

  // Delete copy and move constructors and assignment operators to prevent accidental copying or moving of the parser, which manages a C struct with manual memory management.
  SelectParser(const SelectParser&) = delete;
  SelectParser& operator=(const SelectParser&) = delete;
  SelectParser(SelectParser&&) = delete;
  SelectParser& operator=(SelectParser&&) = delete;

  std::string extractTableName() const;
  std::vector<std::size_t> extractProjectionIndices(const Schema& schema) const;
  std::vector<ComparisonPredicate> extractComparisonPredicates(
      const Schema& schema) const;

 private:
  PgQueryParseResult result_{};
  nlohmann::json parse_tree_;
};