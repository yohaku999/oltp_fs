#include "table.h"

#include <algorithm>
#include <array>
#include <filesystem>
#include <fstream>
#include <nlohmann/json.hpp>

#include "execution/heap_fetch.h"
#include "logging.h"
#include "storage/index/btreecursor.h"
#include "storage/page/page.h"
#include "storage/record/record_cell.h"
#include "storage/record/record_serializer.h"
#include "storage/runtime/bufferpool.h"
#include "storage/wal/wal.h"
#include "storage/wal/wal_body.h"
#include "storage/wal/wal_record.h"

namespace {

std::string columnTypeToString(Column::Type type) {
  switch (type) {
    case Column::Type::Integer:
      return "Integer";
    case Column::Type::Varchar:
      return "Varchar";
  }
  throw std::runtime_error("Unknown column type.");
}

Column::Type columnTypeFromString(const std::string& type_name) {
  if (type_name == "Integer") {
    return Column::Type::Integer;
  }
  if (type_name == "Varchar") {
    return Column::Type::Varchar;
  }
  throw std::runtime_error("Unknown column type in metadata: " + type_name);
}

std::string indexFileName(
    const std::string& table_name,
    const std::optional<std::string>& indexed_column_name) {
  if (!indexed_column_name.has_value()) {
    return table_name + ".index";
  }
  return table_name + "." + indexed_column_name.value() + ".index";
}

}  // namespace

Table::Table(std::string name, Schema schema,
             std::optional<std::string> index_path,
             std::optional<std::string> indexed_column_name)
    : name_(std::move(name)),
      schema_(std::move(schema)),
      indexed_column_name_(std::move(indexed_column_name)),
      index_file_(index_path.has_value()
                      ? std::optional<File>(
                            std::in_place, preparePath(index_path.value()))
                      : std::nullopt),
      heap_file_(preparePath(defaultHeapPath(name_))) {}

Table Table::initialize(const std::string& table_name, const Schema& schema) {
  if (anyBackingFileExists(table_name)) {
    throw std::runtime_error("Table already exists: " + table_name);
  }

  try {
    Table table(table_name, schema, std::nullopt, std::nullopt);

    std::array<char, Page::PAGE_SIZE_BYTE> heap_page_buffer{};
    Page::initializeNew(heap_page_buffer.data(), PageKind::Heap, 0, 0);
    table.heap_file_.writePageFromBuffer(0, heap_page_buffer.data());

    writeSchemaMetadata(defaultMetaPath(table_name), schema, {});
    return table;
  } catch (...) {
    removeBackingFilesFor(table_name);
    throw;
  }
}

void Table::createIndex(const std::string& column_name) {
  if (schema_.getColumnIndex(column_name) < 0) {
    throw std::runtime_error("Index references unknown column: " + column_name);
  }
  if (indexed_column_name_.has_value()) {
    if (indexed_column_name_.value() == column_name) {
      return;
    }
    throw std::runtime_error("Table already has an index on column: " +
                             indexed_column_name_.value());
  }

  const std::string index_path = defaultIndexPath(name_, column_name);
  index_file_.emplace(preparePath(index_path));
  indexed_column_name_ = column_name;

  try {
    std::array<char, Page::PAGE_SIZE_BYTE> index_root_buffer{};
    Page::initializeNew(index_root_buffer.data(), PageKind::LeafIndex, 0, 0);
    index_file_->writePageFromBuffer(0, index_root_buffer.data());

    writeSchemaMetadata(defaultMetaPath(name_), schema_,
                        {PersistedIndex{index_file_->getFilePath(),
                                        indexed_column_name_}});
  } catch (...) {
    index_file_.reset();
    indexed_column_name_.reset();
    removeFileIfExists(index_path);
    writeSchemaMetadata(defaultMetaPath(name_), schema_, {});
    throw;
  }
}

Table Table::getTable(const std::string& table_name) {
  if (!isPersisted(table_name)) {
    throw std::runtime_error("Table does not exist: " + table_name);
  }
  PersistedMetadata metadata =
      readSchemaMetadata(defaultMetaPath(table_name));
  std::optional<std::string> index_path;
  std::optional<std::string> indexed_column_name;
  if (!metadata.indexes.empty()) {
    PersistedIndex index = std::move(metadata.indexes.front());
    index_path = std::move(index.index_path);
    indexed_column_name = std::move(index.indexed_column_name);
  }
  return Table(table_name, std::move(metadata.schema), std::move(index_path),
               std::move(indexed_column_name));
}

bool Table::isPersisted(const std::string& table_name) {
  const std::string meta_path = defaultMetaPath(table_name);
  if (!std::filesystem::exists(meta_path)) {
    return false;
  }

  const PersistedMetadata metadata = readSchemaMetadata(meta_path);
  return std::filesystem::exists(defaultHeapPath(table_name)) &&
         std::all_of(metadata.indexes.begin(), metadata.indexes.end(),
                     [](const PersistedIndex& index) {
                       return std::filesystem::exists(index.index_path);
                     });
}

void Table::removeBackingFilesFor(const std::string& table_name) {
  const std::string meta_path = defaultMetaPath(table_name);
  if (std::filesystem::exists(meta_path)) {
    for (const auto& index : readSchemaMetadata(meta_path).indexes) {
      removeFileIfExists(index.index_path);
    }
  }
  removeFileIfExists(defaultHeapPath(table_name));
  removeFileIfExists(meta_path);
}

std::string Table::defaultIndexPath(
    const std::string& table_name,
    const std::optional<std::string>& indexed_column_name) {
  return (std::filesystem::path("data") /
          indexFileName(table_name, indexed_column_name))
      .string();
}

std::string Table::defaultHeapPath(const std::string& table_name) {
  return (std::filesystem::path("data") / (table_name + ".db")).string();
}

std::string Table::defaultMetaPath(const std::string& table_name) {
  return (std::filesystem::path("data") / (table_name + ".meta.json")).string();
}

std::string Table::preparePath(const std::string& path) {
  std::filesystem::path filesystem_path(path);
  if (filesystem_path.has_parent_path()) {
    std::filesystem::create_directories(filesystem_path.parent_path());
  }
  return filesystem_path.string();
}

void Table::removeFileIfExists(const std::string& path) {
  std::error_code error;
  std::filesystem::remove(path, error);
  if (error) {
    throw std::runtime_error("failed to remove file: " + path + ": " +
                             error.message());
  }
}

void Table::writeSchemaMetadata(
    const std::string& meta_path, const Schema& schema,
    const std::vector<PersistedIndex>& indexes) {
  nlohmann::json metadata;
  metadata["indexes"] = nlohmann::json::array();
  for (const auto& index : indexes) {
    metadata["indexes"].push_back(
        {{"indexFile", index.index_path},
         {"indexedColumn", index.indexed_column_name.has_value()
                               ? nlohmann::json(index.indexed_column_name.value())
                               : nlohmann::json(nullptr)}});
  }
  metadata["columns"] = nlohmann::json::array();
  for (const auto& column : schema.columns()) {
    metadata["columns"].push_back(
        {{"name", column.getName()},
         {"type", columnTypeToString(column.getType())}});
  }

  std::ofstream output(preparePath(meta_path));
  if (!output.is_open()) {
    throw std::runtime_error("failed to open metadata file for write: " +
                             meta_path);
  }
  output << metadata.dump(2);
  if (!output.good()) {
    throw std::runtime_error("failed to write metadata file: " + meta_path);
  }
}

Table::PersistedMetadata Table::readSchemaMetadata(
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

  std::vector<PersistedIndex> indexes;
  if (metadata.contains("indexes") && metadata["indexes"].is_array()) {
    for (const auto& index_json : metadata["indexes"]) {
      if (!index_json.contains("indexFile") ||
          !index_json["indexFile"].is_string()) {
        throw std::runtime_error(
            "invalid table metadata: index requires indexFile");
      }
      std::optional<std::string> indexed_column_name;
      if (index_json.contains("indexedColumn") &&
          index_json["indexedColumn"].is_string()) {
        indexed_column_name = index_json["indexedColumn"].get<std::string>();
      }
      indexes.push_back(PersistedIndex{index_json["indexFile"].get<std::string>(),
                                       std::move(indexed_column_name)});
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
        columnTypeFromString(column_json["type"].get<std::string>()));
  }

  return PersistedMetadata{Schema(std::move(columns)), std::move(indexes)};
}

bool Table::hasIndexForColumn(const std::string& column_name) const {
  return indexed_column_name_.has_value() &&
         indexed_column_name_.value() == column_name;
}

std::optional<std::reference_wrapper<File>> Table::indexFile() {
  if (!index_file_.has_value()) {
    return std::nullopt;
  }
  return index_file_.value();
}

bool Table::anyBackingFileExists(const std::string& table_name) {
  const std::string meta_path = defaultMetaPath(table_name);
  return std::filesystem::exists(meta_path) ||
         std::filesystem::exists(defaultHeapPath(table_name));
}