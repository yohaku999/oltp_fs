#pragma once

#include <string>

#include <nlohmann/json.hpp>

extern "C" {
#include <pg_query.h>
}

class PgQueryJsonParser {
 public:
  explicit PgQueryJsonParser(std::string sql);
  ~PgQueryJsonParser();

  PgQueryJsonParser(const PgQueryJsonParser&) = delete;
  PgQueryJsonParser& operator=(const PgQueryJsonParser&) = delete;
  PgQueryJsonParser(PgQueryJsonParser&&) = delete;
  PgQueryJsonParser& operator=(PgQueryJsonParser&&) = delete;

 protected:
  const nlohmann::json& statementNode() const;

  nlohmann::json parse_tree_;

 private:
  PgQueryParseResult result_{};
};