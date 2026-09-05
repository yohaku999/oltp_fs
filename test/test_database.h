#pragma once

#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "catalog/table.h"
#include "storage/local_storage_manager.h"
#include "storage/table_metadata_store.h"

namespace test_db {

inline std::optional<Table> findTable(const std::string& table_name) {
  return TableMetadataStore::read(table_name);
}

inline Table getTable(const std::string& table_name) {
  std::optional<Table> table = TableMetadataStore::read(table_name);
  if (!table.has_value()) {
    throw std::runtime_error("Table is not in the catalog: " + table_name);
  }
  return std::move(*table);
}

inline bool isPersisted(const std::string& table_name) {
  const LocalStorageManager storage;
  const std::optional<Table> table = findTable(table_name);
  if (!table.has_value()) {
    return false;
  }
  if (!storage.exists(table->heapFileName())) {
    return false;
  }
  for (const IndexMetadata& index : table->indexes()) {
    if (!storage.exists(index.file_name)) {
      return false;
    }
  }
  return true;
}

inline void removeTable(const std::string& table_name) {
  const LocalStorageManager storage;
  const std::optional<Table> table = findTable(table_name);
  if (table.has_value()) {
    for (const IndexMetadata& index : table->indexes()) {
      if (storage.exists(index.file_name)) {
        storage.remove(index.file_name);
      }
    }
    if (storage.exists(table->heapFileName())) {
      storage.remove(table->heapFileName());
    }
    TableMetadataStore::remove(table_name);
    return;
  }

  const std::string heap_file_name = storage.heapFileNameFor(table_name);
  if (storage.exists(heap_file_name)) {
    storage.remove(heap_file_name);
  }
}

inline Table createTable(const std::string& table_name, const Schema& schema) {
  const LocalStorageManager storage;
  const std::string heap_file_name = storage.heapFileNameFor(table_name);
  Table table(table_name, schema, heap_file_name);
  storage.createHeap(heap_file_name);
  try {
    TableMetadataStore::write(table);
  } catch (...) {
    storage.remove(heap_file_name);
    throw;
  }
  return table;
}

inline void createIndex(Table& table,
                        const std::vector<std::string>& column_names,
                        bool is_unique) {
  const LocalStorageManager storage;
  const std::string index_file_name =
      storage.indexFileNameFor(table.name(), column_names);
  Table updated = table;
  updated.addIndex(IndexMetadata{index_file_name, column_names, is_unique});
  storage.createIndex(index_file_name);
  try {
    TableMetadataStore::write(updated);
  } catch (...) {
    storage.remove(index_file_name);
    throw;
  }
  table = std::move(updated);
}

}  // namespace test_db
