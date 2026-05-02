#include "table.h"

#include <algorithm>
#include <array>
#include <filesystem>

#include "catalog/table_metadata.h"
#include "execution/operators/heap_fetch_operator.h"
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

    TableMetadataStore::write(table_name, schema, {});
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

    TableMetadataStore::write(
        name_, schema_,
        {PersistedIndexMetadata{index_file_->getFilePath(), indexed_column_name_}});
  } catch (...) {
    index_file_.reset();
    indexed_column_name_.reset();
    removeFileIfExists(index_path);
    TableMetadataStore::write(name_, schema_, {});
    throw;
  }
}

Table Table::getTable(const std::string& table_name) {
  if (!isPersisted(table_name)) {
    throw std::runtime_error("Table does not exist: " + table_name);
  }
  PersistedTableMetadata metadata = TableMetadataStore::read(table_name);
  std::optional<std::string> index_path;
  std::optional<std::string> indexed_column_name;
  if (!metadata.indexes.empty()) {
    PersistedIndexMetadata index = std::move(metadata.indexes.front());
    index_path = std::move(index.index_path);
    indexed_column_name = std::move(index.indexed_column_name);
  }
  return Table(table_name, std::move(metadata.schema), std::move(index_path),
               std::move(indexed_column_name));
}

bool Table::isPersisted(const std::string& table_name) {
  if (!TableMetadataStore::exists(table_name)) {
    return false;
  }

  const PersistedTableMetadata metadata = TableMetadataStore::read(table_name);
  return std::filesystem::exists(defaultHeapPath(table_name)) &&
         std::all_of(metadata.indexes.begin(), metadata.indexes.end(),
                     [](const PersistedIndexMetadata& index) {
                       return std::filesystem::exists(index.index_path);
                     });
}

void Table::removeBackingFilesFor(const std::string& table_name) {
  const std::string meta_path = TableMetadataStore::pathFor(table_name);
  if (std::filesystem::exists(meta_path)) {
    for (const auto& index : TableMetadataStore::readFromPath(meta_path).indexes) {
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

std::string Table::preparePath(const std::string& path) {
  std::filesystem::path filesystem_path(path);
  if (filesystem_path.has_parent_path()) {
    std::filesystem::create_directories(filesystem_path.parent_path());
  }
  return filesystem_path.string();
}

void Table::removeFileIfExists(const std::string& path) {
  File::invalidateCache(path);
  std::error_code error;
  std::filesystem::remove(path, error);
  if (error) {
    throw std::runtime_error("failed to remove file: " + path + ": " +
                             error.message());
  }
}

bool Table::hasIndexForColumn(const std::string& column_name) const {
  return indexed_column_name_.has_value() &&
         indexed_column_name_.value() == column_name;
}

int Table::extractIndexKey(const TypedRow& row) const {
  const std::optional<std::size_t> indexed_column_index = indexedColumnIndex();
  // TODO: only available for single-column integer access indexes for now.
  return std::get<Column::IntegerType>(row.values[indexed_column_index.value()]);
}

std::optional<std::size_t> Table::indexedColumnIndex() const {
  if (!indexed_column_name_.has_value()) {
    return std::nullopt;
  }

  const int column_index = schema_.getColumnIndex(indexed_column_name_.value());
  if (column_index < 0) {
    return std::nullopt;
  }

  return static_cast<std::size_t>(column_index);
}

std::optional<std::reference_wrapper<File>> Table::indexFile() {
  if (!index_file_.has_value()) {
    return std::nullopt;
  }
  return index_file_.value();
}

File& Table::requireIndexFile() {
  const auto index_file = indexFile();
  if (!index_file.has_value()) {
    throw std::runtime_error("Table has no index file: " + name_);
  }
  return index_file->get();
}

bool Table::anyBackingFileExists(const std::string& table_name) {
  return TableMetadataStore::exists(table_name) ||
         std::filesystem::exists(defaultHeapPath(table_name));
}