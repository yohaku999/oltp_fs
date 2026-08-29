#pragma once

#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "catalog/table.h"
#include "catalog/table_repository.h"
#include "storage/local_storage_manager.h"

namespace test_db {

inline std::optional<Table> findTable(const std::string& table_name) {
  try {
    return TableRepository().get(table_name);
  } catch (const std::runtime_error&) {
    return std::nullopt;
  }
}

inline Table getTable(const std::string& table_name) {
  return TableRepository().get(table_name);
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
  TableRepository catalog;
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
    catalog.erase(table_name);
    return;
  }

  const std::string heap_file_name = storage.heapFileNameFor(table_name);
  if (storage.exists(heap_file_name)) {
    storage.remove(heap_file_name);
  }
}

inline Table createTable(const std::string& table_name, const Schema& schema) {
  TableRepository catalog;
  const LocalStorageManager storage;
  const std::string heap_file_name = storage.heapFileNameFor(table_name);
  Table table(table_name, schema, heap_file_name);
  storage.createHeap(heap_file_name);
  try {
    catalog.insert(table);
  } catch (...) {
    storage.remove(heap_file_name);
    throw;
  }
  return table;
}

inline void createIndex(Table& table,
                        const std::vector<std::string>& column_names) {
  TableRepository catalog;
  const LocalStorageManager storage;
  const std::string index_file_name =
      storage.indexFileNameFor(table.name(), column_names);
  Table updated = table;
  updated.addIndex(IndexMetadata{index_file_name, column_names});
  storage.createIndex(index_file_name);
  try {
    catalog.update(updated);
  } catch (...) {
    storage.remove(index_file_name);
    throw;
  }
  table = std::move(updated);
}

}  // namespace test_db
