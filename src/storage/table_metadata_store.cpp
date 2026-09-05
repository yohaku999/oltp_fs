#include "storage/table_metadata_store.h"

#include <filesystem>
#include <fstream>
#include <nlohmann/json.hpp>
#include <stdexcept>
#include <unordered_map>
#include <utility>

#include "catalog/table.h"

namespace {

/**
 * Benchmark-only process-local metadata cache.
 *
 * TPC-C creates tables and indexes during setup, then runs the measured
 * workload without DDL. Under that assumption, table metadata is effectively
 * immutable during the measured phase, so caching avoids repeated metadata JSON
 * file reads/parses from TableMetadataStore::read.
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
std::unordered_map<std::string, Table> metadata_cache;

std::string prepareMetadataPath(const std::string& path) {
  std::filesystem::path filesystem_path(path);
  if (filesystem_path.has_parent_path()) {
    std::filesystem::create_directories(filesystem_path.parent_path());
  }
  return filesystem_path.string();
}

}  // namespace

std::optional<Table> TableMetadataStore::read(const std::string& table_name) {
  if (!exists(table_name)) {
    return std::nullopt;
  }
  const auto cached = metadata_cache.find(table_name);
  if (cached != metadata_cache.end()) {
    return cached->second;
  }

  Table table = readFromPath(pathFor(table_name));
  metadata_cache.insert_or_assign(table_name, table);
  return table;
}

std::string TableMetadataStore::pathFor(const std::string& table_name) {
  return (std::filesystem::path("data") / (table_name + ".meta.json")).string();
}

bool TableMetadataStore::exists(const std::string& table_name) {
  return std::filesystem::exists(pathFor(table_name));
}

void TableMetadataStore::write(const Table& table) {
  nlohmann::json metadata;
  metadata["heapFileName"] = table.heapFileName();
  metadata["indexes"] = nlohmann::json::array();
  for (const IndexMetadata& index : table.indexes()) {
    metadata["indexes"].push_back(
        {{"fileName", index.file_name},
         {"indexedColumns", index.column_names},
         {"isUnique", index.is_unique}});
  }
  metadata["columns"] = nlohmann::json::array();
  for (const Column& column : table.schema().columns()) {
    metadata["columns"].push_back(
        {{"name", column.getName()},
         {"type", Column::typeToString(column.getType())}});
  }

  const std::string meta_path = pathFor(table.name());
  std::ofstream output(prepareMetadataPath(meta_path));
  if (!output.is_open()) {
    throw std::runtime_error("failed to open metadata file for write: " +
                             meta_path);
  }
  output << metadata.dump(2);
  if (!output.good()) {
    throw std::runtime_error("failed to write metadata file: " + meta_path);
  }

  metadata_cache.insert_or_assign(table.name(), table);
}

void TableMetadataStore::remove(const std::string& table_name) {
  metadata_cache.erase(table_name);
  std::error_code error;
  std::filesystem::remove(pathFor(table_name), error);
  if (error) {
    throw std::runtime_error("failed to remove table metadata: " +
                             error.message());
  }
}

Table TableMetadataStore::readFromPath(const std::string& meta_path) {
  std::ifstream input(meta_path);
  if (!input.is_open()) {
    throw std::runtime_error("failed to open metadata file for read: " +
                             meta_path);
  }

  nlohmann::json metadata;
  input >> metadata;

  const std::filesystem::path metadata_path(meta_path);
  std::string table_name = metadata_path.filename().string();
  constexpr const char* metadata_suffix = ".meta.json";
  if (table_name.size() >= std::char_traits<char>::length(metadata_suffix)) {
    table_name.erase(table_name.size() -
                     std::char_traits<char>::length(metadata_suffix));
  }
  const std::string heap_file_name =
      metadata.contains("heapFileName") &&
              metadata["heapFileName"].is_string()
          ? metadata["heapFileName"].get<std::string>()
          : metadata.contains("heapStorageId") &&
                    metadata["heapStorageId"].is_string()
                ? metadata["heapStorageId"].get<std::string>()
                : table_name + ".db";

  if (!metadata.contains("columns") || !metadata["columns"].is_array()) {
    throw std::runtime_error("invalid table metadata: missing columns array");
  }

  std::vector<IndexMetadata> indexes;
  if (metadata.contains("indexes") && metadata["indexes"].is_array()) {
    for (const auto& index_json : metadata["indexes"]) {
      const bool has_file_name =
          index_json.contains("fileName") &&
          index_json["fileName"].is_string();
      const bool has_legacy_storage_id =
          index_json.contains("storageId") &&
          index_json["storageId"].is_string();
      const bool has_legacy_path =
          index_json.contains("indexFile") &&
          index_json["indexFile"].is_string();
      if (!has_file_name && !has_legacy_storage_id && !has_legacy_path) {
        throw std::runtime_error(
            "invalid table metadata: index requires fileName");
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
      const std::string file_name =
          has_file_name
              ? index_json["fileName"].get<std::string>()
              : has_legacy_storage_id
                    ? index_json["storageId"].get<std::string>()
                    : std::filesystem::path(
                          index_json["indexFile"].get<std::string>())
                          .filename()
                          .string();
      const bool is_unique = index_json.at("isUnique").get<bool>();
      indexes.push_back(IndexMetadata{file_name,
                                      std::move(indexed_column_names),
                                      is_unique});
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

  return Table(table_name, Schema(std::move(columns)), heap_file_name,
               std::move(indexes));
}
