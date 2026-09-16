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

  const std::string& sqlTemplate() const;

 protected:
  const nlohmann::json& statementNode() const;

 private:
  std::string sql_template_;
  nlohmann::json parse_tree_;
};
