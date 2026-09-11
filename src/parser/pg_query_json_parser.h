#pragma once

#include <nlohmann/json.hpp>
#include <string>

class PgQueryJsonParser {
 public:
  explicit PgQueryJsonParser(std::string sql);
  ~PgQueryJsonParser() = default;

  PgQueryJsonParser(const PgQueryJsonParser&) = delete;
  PgQueryJsonParser& operator=(const PgQueryJsonParser&) = delete;
  PgQueryJsonParser(PgQueryJsonParser&&) = delete;
  PgQueryJsonParser& operator=(PgQueryJsonParser&&) = delete;

 protected:
  const nlohmann::json& statementNode() const;

  nlohmann::json parse_tree_;
};
