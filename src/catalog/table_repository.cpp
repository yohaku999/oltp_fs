#include "catalog/table_repository.h"

#include <stdexcept>
#include <utility>
#include <vector>

#include "storage/table_metadata_store.h"

Table TableRepository::get(const std::string& table_name) const {
  if (!TableMetadataStore::exists(table_name)) {
    throw std::runtime_error("Table is not in the catalog: " + table_name);
  }

  PersistedTableMetadata persisted = TableMetadataStore::read(table_name);
  std::vector<IndexMetadata> indexes;
  indexes.reserve(persisted.indexes.size());
  for (PersistedIndexMetadata& index : persisted.indexes) {
    indexes.push_back(IndexMetadata{std::move(index.file_name),
                                    std::move(index.indexed_column_names)});
  }
  return Table(table_name, std::move(persisted.schema),
               std::move(persisted.heap_file_name), std::move(indexes));
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
