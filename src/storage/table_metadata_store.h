#pragma once

#include <string>

class Table;

class TableMetadataStore {
 public:
  static std::string pathFor(const std::string& table_name);

  static bool exists(const std::string& table_name);

  static void write(const Table& table);

  static void remove(const std::string& table_name);

  static Table read(const std::string& table_name);

  static Table readFromPath(const std::string& meta_path);
};
