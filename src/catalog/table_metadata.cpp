#include "catalog/table_metadata.h"

#include <filesystem>
#include <fstream>
#include <nlohmann/json.hpp>
#include <stdexcept>
#include <unordered_map>

namespace {

/**
 * Benchmark-only process-local metadata cache.
 * 
 * TPC-C creates tables and indexes during setup, then runs the measured
 * workload without DDL. Under that assumption, table metadata is effectively
 * immutable during the measured phase, so caching avoids repeated metadata JSON
 * file reads/parses from Table::getTable. 
 * 
 * This is intentionally not a production catalog cache:
 * - no invalidation for external metadata file changes
 * - no timestamp/version checks
 * - no cross-process consistency
 * - not safe for workloads that run DDL concurrently with queries
 * 
 * If DBFS starts supporting runtime DDL seriously, replace this with an
 * explicit catalog cache and invalidation path.
 */
std::unordered_map<std::string, PersistedTableMetadata> metadata_cache;

std::string prepareMetadataPath(const std::string& path) {
  std::filesystem::path filesystem_path(path);
  if (filesystem_path.has_parent_path()) {
    std::filesystem::create_directories(filesystem_path.parent_path());
  }
  return filesystem_path.string();
}

}  // namespace

std::string TableMetadataStore::pathFor(const std::string& table_name) {
  return (std::filesystem::path("data") / (table_name + ".meta.json")).string();
}

bool TableMetadataStore::exists(const std::string& table_name) {
  return std::filesystem::exists(pathFor(table_name));
}

void TableMetadataStore::write(
    const std::string& table_name, const Schema& schema,
    const std::vector<PersistedIndexMetadata>& indexes) {
  nlohmann::json metadata;
  metadata["indexes"] = nlohmann::json::array();
  for (const auto& index : indexes) {
    metadata["indexes"].push_back(
        {{"indexFile", index.index_path},
         {"indexedColumns", index.indexed_column_names}});
  }
  metadata["columns"] = nlohmann::json::array();
  for (const auto& column : schema.columns()) {
    metadata["columns"].push_back(
        {{"name", column.getName()},
         {"type", Column::typeToString(column.getType())}});
  }

  const std::string meta_path = pathFor(table_name);
  std::ofstream output(prepareMetadataPath(meta_path));
  if (!output.is_open()) {
    throw std::runtime_error("failed to open metadata file for write: " +
                             meta_path);
  }
  output << metadata.dump(2);
  if (!output.good()) {
    throw std::runtime_error("failed to write metadata file: " + meta_path);
  }

  metadata_cache.insert_or_assign(table_name,
                                  PersistedTableMetadata{schema, indexes});
}

PersistedTableMetadata TableMetadataStore::read(const std::string& table_name) {
  const auto cached = metadata_cache.find(table_name);
  if (cached != metadata_cache.end()) {
    return cached->second;
  }

  PersistedTableMetadata metadata = readFromPath(pathFor(table_name));
  metadata_cache.insert_or_assign(table_name, metadata);
  return metadata;
}

PersistedTableMetadata TableMetadataStore::readFromPath(
    const std::string& meta_path) {
  std::ifstream input(meta_path);
  if (!input.is_open()) {
    throw std::runtime_error("failed to open metadata file for read: " +
                             meta_path);
  }

  nlohmann::json metadata;
  input >> metadata;

  if (!metadata.contains("columns") || !metadata["columns"].is_array()) {
    throw std::runtime_error("invalid table metadata: missing columns array");
  }

  std::vector<PersistedIndexMetadata> indexes;
  if (metadata.contains("indexes") && metadata["indexes"].is_array()) {
    for (const auto& index_json : metadata["indexes"]) {
      if (!index_json.contains("indexFile") ||
          !index_json["indexFile"].is_string()) {
        throw std::runtime_error(
            "invalid table metadata: index requires indexFile");
      }
      std::vector<std::string> indexed_column_names;
      if (index_json.contains("indexedColumns") &&
          index_json["indexedColumns"].is_array()) {
        for (const auto& indexed_column_json : index_json["indexedColumns"]) {
          if (!indexed_column_json.is_string()) {
            throw std::runtime_error(
                "invalid table metadata: indexedColumns must be strings");
          }
          indexed_column_names.push_back(
              indexed_column_json.get<std::string>());
        }
      } else if (index_json.contains("indexedColumn") &&
                 index_json["indexedColumn"].is_string()) {
        indexed_column_names.push_back(
            index_json["indexedColumn"].get<std::string>());
      }
      indexes.push_back(
          PersistedIndexMetadata{index_json["indexFile"].get<std::string>(),
                                 std::move(indexed_column_names)});
    }
  }

  std::vector<Column> columns;
  for (const auto& column_json : metadata["columns"]) {
    if (!column_json.contains("name") || !column_json["name"].is_string() ||
        !column_json.contains("type") || !column_json["type"].is_string()) {
      throw std::runtime_error(
          "invalid table metadata: column requires name and type");
    }

    columns.emplace_back(
        column_json["name"].get<std::string>(),
        Column::typeFromString(column_json["type"].get<std::string>()));
  }

  return PersistedTableMetadata{Schema(std::move(columns)), std::move(indexes)};
}
