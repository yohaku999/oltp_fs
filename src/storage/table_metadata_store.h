#pragma once

#include <optional>
#include <string>

#include "catalog/table.h"

class TableMetadataStore {
 public:
  static std::optional<Table> read(const std::string& table_name);
  static void write(const Table& table);
  static void remove(const std::string& table_name);

 private:
  static std::string pathFor(const std::string& table_name);
  static bool exists(const std::string& table_name);
  static Table readFromPath(const std::string& meta_path);
};
