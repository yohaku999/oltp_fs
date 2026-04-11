#include "table.h"

#include <array>
#include <fstream>
#include <nlohmann/json.hpp>

#include "../storage/page.h"

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

}  // namespace

Table::Table(std::string name, Schema schema)
    : name_(std::move(name)),
      schema_(std::move(schema)),
      index_file_(preparePath(defaultIndexPath(name_))),
      heap_file_(preparePath(defaultHeapPath(name_))) {}

Table Table::initialize(const std::string& table_name, const Schema& schema) {
  if (anyBackingFileExists(table_name)) {
    throw std::runtime_error("Table already exists: " + table_name);
  }

  try {
    Table table(table_name, schema);

    std::array<char, Page::PAGE_SIZE_BYTE> index_root_buffer{};
    Page::initializeNew(index_root_buffer.data(), PageKind::LeafIndex, 0, 0);
    table.index_file_.writePageFromBuffer(0, index_root_buffer.data());

    std::array<char, Page::PAGE_SIZE_BYTE> heap_page_buffer{};
    Page::initializeNew(heap_page_buffer.data(), PageKind::Heap, 0, 0);
    table.heap_file_.writePageFromBuffer(0, heap_page_buffer.data());

    writeSchemaMetadata(defaultMetaPath(table_name), schema);
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
  return Table(table_name,
               readSchemaMetadata(table_name, defaultMetaPath(table_name)));
}

bool Table::isPersisted(const std::string& table_name) {
  return std::filesystem::exists(defaultIndexPath(table_name)) &&
         std::filesystem::exists(defaultHeapPath(table_name)) &&
         std::filesystem::exists(defaultMetaPath(table_name));
}

void Table::removeBackingFilesFor(const std::string& table_name) {
  removeFileIfExists(defaultIndexPath(table_name));
  removeFileIfExists(defaultHeapPath(table_name));
  removeFileIfExists(defaultMetaPath(table_name));
}

std::string Table::defaultIndexPath(const std::string& table_name) {
  return (std::filesystem::path("data") / (table_name + ".index")).string();
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

void Table::writeSchemaMetadata(const std::string& meta_path,
                                const Schema& schema) {
  nlohmann::json metadata;
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

Schema Table::readSchemaMetadata(const std::string& table_name,
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

  return Schema(table_name, std::move(columns));
}
bool Table::anyBackingFileExists(const std::string& table_name) {
  return std::filesystem::exists(defaultIndexPath(table_name)) ||
         std::filesystem::exists(defaultHeapPath(table_name)) ||
         std::filesystem::exists(defaultMetaPath(table_name));
}