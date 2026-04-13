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

Table::Table(std::string name, Schema schema, std::string index_path,
             std::optional<std::string> indexed_column_name)
    : name_(std::move(name)),
      schema_(std::move(schema)),
      indexed_column_name_(std::move(indexed_column_name)),
      index_file_(preparePath(index_path)),
      heap_file_(preparePath(defaultHeapPath(name_))) {}

Table Table::initialize(const std::string& table_name, const Schema& schema,
                        std::optional<std::string> indexed_column_name) {
  if (anyBackingFileExists(table_name)) {
    throw std::runtime_error("Table already exists: " + table_name);
  }

  if (indexed_column_name.has_value() &&
      schema.getColumnIndex(indexed_column_name.value()) < 0) {
    throw std::runtime_error("Index references unknown column: " +
                             indexed_column_name.value());
  }

  try {
    const std::string index_path =
        defaultIndexPath(table_name, indexed_column_name);
    Table table(table_name, schema, index_path, std::move(indexed_column_name));

    std::array<char, Page::PAGE_SIZE_BYTE> index_root_buffer{};
    Page::initializeNew(index_root_buffer.data(), PageKind::LeafIndex, 0, 0);
    table.index_file_.writePageFromBuffer(0, index_root_buffer.data());

    std::array<char, Page::PAGE_SIZE_BYTE> heap_page_buffer{};
    Page::initializeNew(heap_page_buffer.data(), PageKind::Heap, 0, 0);
    table.heap_file_.writePageFromBuffer(0, heap_page_buffer.data());

    writeSchemaMetadata(defaultMetaPath(table_name), schema,
                        {PersistedIndex{table.index_file_.getFilePath(),
                                        table.indexed_column_name_}});
    return table;
  } catch (...) {
    removeBackingFilesFor(table_name);
    throw;
  }
}

Table Table::getTable(const std::string& table_name) {
  if (!isPersisted(table_name)) {
    throw std::runtime_error("Table does not exist: " + table_name);
  }
  PersistedMetadata metadata =
      readSchemaMetadata(table_name, defaultMetaPath(table_name));
  // TODO: currently we support only one index per table.
  PersistedIndex index = metadata.indexes.front();
  return Table(table_name, std::move(metadata.schema), index.index_path,
               std::move(index.indexed_column_name));
}

bool Table::isPersisted(const std::string& table_name) {
  const std::string meta_path = defaultMetaPath(table_name);
  if (!std::filesystem::exists(meta_path)) {
    return false;
  }

  const PersistedMetadata metadata = readSchemaMetadata(table_name, meta_path);
  return std::filesystem::exists(defaultHeapPath(table_name)) &&
         std::all_of(metadata.indexes.begin(), metadata.indexes.end(),
                     [](const PersistedIndex& index) {
                       return std::filesystem::exists(index.index_path);
                     });
}

void Table::removeBackingFilesFor(const std::string& table_name) {
  const std::string meta_path = defaultMetaPath(table_name);
  if (std::filesystem::exists(meta_path)) {
    for (const auto& index : readSchemaMetadata(table_name, meta_path).indexes) {
      removeFileIfExists(index.index_path);
    }
  } else {
    removeFileIfExists(defaultIndexPath(table_name, std::nullopt));
    if (const auto indexed_column_name = findPersistedIndexedColumn(table_name);
        indexed_column_name.has_value()) {
      removeFileIfExists(defaultIndexPath(table_name, indexed_column_name));
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
    const std::string& table_name, const std::string& meta_path) {
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
  } else {
    std::optional<std::string> indexed_column_name;
    if (metadata.contains("indexedColumn") &&
        metadata["indexedColumn"].is_string()) {
      indexed_column_name = metadata["indexedColumn"].get<std::string>();
    }

    std::string index_path;
    if (metadata.contains("indexFile") && metadata["indexFile"].is_string()) {
      index_path = metadata["indexFile"].get<std::string>();
    } else {
      if (!indexed_column_name.has_value()) {
        indexed_column_name = findPersistedIndexedColumn(table_name);
      }
      index_path = defaultIndexPath(table_name, indexed_column_name);
    }
    indexes.push_back(
        PersistedIndex{std::move(index_path), std::move(indexed_column_name)});
  }

  if (indexes.empty()) {
    std::optional<std::string> indexed_column_name =
        findPersistedIndexedColumn(table_name);
    indexes.push_back(PersistedIndex{
        defaultIndexPath(table_name, indexed_column_name),
        std::move(indexed_column_name)});
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

std::optional<std::string> Table::findPersistedIndexedColumn(
    const std::string& table_name) {
  const std::filesystem::path data_dir("data");
  if (!std::filesystem::exists(data_dir)) {
    return std::nullopt;
  }

  const std::string plain_file_name = table_name + ".index";
  const std::string prefix = table_name + ".";
  const std::string suffix = ".index";
  std::optional<std::string> indexed_column_name;

  for (const auto& entry : std::filesystem::directory_iterator(data_dir)) {
    if (!entry.is_regular_file()) {
      continue;
    }

    const std::string file_name = entry.path().filename().string();
    if (file_name == plain_file_name) {
      return std::nullopt;
    }

    const size_t column_name_start = file_name.find('.');
    const size_t suffix_start = file_name.rfind(suffix);
    if (column_name_start == std::string::npos ||
        suffix_start == std::string::npos ||
        file_name.rfind(prefix, 0) != 0 || suffix_start <= prefix.size()) {
      continue;
    }

    const std::string column_name =
        file_name.substr(prefix.size(), suffix_start - prefix.size());
    if (indexed_column_name.has_value()) {
      continue;
    }
    indexed_column_name = column_name;
  }

  return indexed_column_name;
}

bool Table::hasIndexForColumn(const std::string& column_name) const {
  return indexed_column_name_.has_value() &&
         indexed_column_name_.value() == column_name;
}

bool Table::anyBackingFileExists(const std::string& table_name) {
  const std::string meta_path = defaultMetaPath(table_name);
  if (std::filesystem::exists(meta_path)) {
    const PersistedMetadata metadata = readSchemaMetadata(table_name, meta_path);
    if (!metadata.indexes.empty()) {
      return true;
    }
    return std::filesystem::exists(defaultHeapPath(table_name));
  }
  return std::filesystem::exists(defaultIndexPath(table_name, std::nullopt)) ||
         findPersistedIndexedColumn(table_name).has_value() ||
         std::filesystem::exists(defaultHeapPath(table_name));
}