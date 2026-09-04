#include "catalog/table_repository.h"

#include <optional>
#include <stdexcept>
#include <utility>

#include "storage/table_metadata_store.h"

std::optional<Table> TableRepository::find(
    const std::string& table_name) const {
  if (!TableMetadataStore::exists(table_name)) {
    return std::nullopt;
  }

  return TableMetadataStore::read(table_name);
}

Table TableRepository::get(const std::string& table_name) const {
  std::optional<Table> table = find(table_name);
  if (!table.has_value()) {
    throw std::runtime_error("Table is not in the catalog: " + table_name);
  }
  return std::move(*table);
}

void TableRepository::insert(const Table& table) {
  if (TableMetadataStore::exists(table.name())) {
    throw std::runtime_error("Table already exists in catalog: " +
                             table.name());
  }
  TableMetadataStore::write(table);
}

void TableRepository::update(const Table& table) {
  if (!TableMetadataStore::exists(table.name())) {
    throw std::runtime_error("Table is not in the catalog: " + table.name());
  }
  TableMetadataStore::write(table);
}

void TableRepository::erase(const std::string& table_name) {
  TableMetadataStore::remove(table_name);
}
