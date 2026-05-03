#pragma once

#include <optional>
#include <string>
#include <vector>

#include "schema/schema.h"

struct PersistedIndexMetadata {
  std::string index_path;
  std::vector<std::string> indexed_column_names;
};

struct PersistedTableMetadata {
  Schema schema;
  std::vector<PersistedIndexMetadata> indexes;
};

class TableMetadataStore {
 public:
  static std::string pathFor(const std::string& table_name);

  static bool exists(const std::string& table_name);

  static void write(const std::string& table_name, const Schema& schema,
                    const std::vector<PersistedIndexMetadata>& indexes);

  static PersistedTableMetadata read(const std::string& table_name);

  static PersistedTableMetadata readFromPath(const std::string& meta_path);
};