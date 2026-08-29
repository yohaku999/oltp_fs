#pragma once

#include <string>
#include <vector>

#include "schema/schema.h"

class Table;

struct PersistedIndexMetadata {
  std::string file_name;
  std::vector<std::string> indexed_column_names;
};

struct PersistedTableMetadata {
  Schema schema;
  std::string heap_file_name;
  std::vector<PersistedIndexMetadata> indexes;
};

class TableMetadataStore {
 public:
  static std::string pathFor(const std::string& table_name);

  static bool exists(const std::string& table_name);

  static void write(const Table& table);

  static void remove(const std::string& table_name);

  static PersistedTableMetadata read(const std::string& table_name);

  static PersistedTableMetadata readFromPath(const std::string& meta_path);
};
