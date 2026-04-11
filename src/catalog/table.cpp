#include "table.h"

#include <array>
#include <fstream>
#include <nlohmann/json.hpp>

#include "execution/heap_fetch.h"
#include "logging.h"
#include "storage/index/btreecursor.h"
#include "storage/page/page.h"
#include "storage/record/record_cell.h"
#include "storage/record/record_serializer.h"
#include "storage/runtime/bufferpool.h"
#include "storage/wal/lsn_allocator.h"
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

std::optional<RID> Table::findRID(BufferPool& pool, int key,
                                  bool do_invalidate) {
  return BTreeCursor::findRID(pool, index_file_, key, do_invalidate);
}

TypedRow Table::readRow(BufferPool& pool, RID rid) const {
  Page* page = pool.pinPage(rid.heap_page_id,
                            const_cast<Table*>(this)->heap_file_);
  TypedRow row = RecordCellView(page->getSlotCellStart(rid.slot_id))
                     .getTypedRow(schema_);
  pool.unpinPage(page, const_cast<Table*>(this)->heap_file_);
  return row;
}

RID Table::insertHeapRecord(BufferPool& pool, int key, const TypedRow& row,
                            LSNAllocator* allocator, WAL* wal) {
  LOG_INFO("Inserting record with key {} into heap file {}.", key,
           heap_file_.getFilePath());
  RecordSerializer cell(schema_, row);
  const std::vector<std::byte>& serialized_cell = cell.serializedBytes();

  int target_page_id = heap_file_.getMaxPageID();
  Page* heap_page = pool.pinPage(target_page_id, heap_file_);
  auto inserted_slot_id = heap_page->insertCell(serialized_cell);
  if (!inserted_slot_id.has_value()) {
    pool.unpinPage(heap_page, heap_file_);
    target_page_id = pool.createPage(PageKind::Heap, heap_file_);
    heap_page = pool.pinPage(target_page_id, heap_file_);
    inserted_slot_id = heap_page->insertCell(serialized_cell);
    if (!inserted_slot_id.has_value()) {
      throw std::runtime_error(
          "Failed to insert record cell into a new heap page due to "
          "insufficient space.");
    }
  }

  if (allocator != nullptr && wal != nullptr) {
    wal->write(make_wal_record(
        *allocator, WALRecord::RecordType::INSERT,
        static_cast<uint16_t>(target_page_id),
        InsertRedoBody(static_cast<uint16_t>(inserted_slot_id.value()),
                       serialized_cell)
            .encode()));
  }

  pool.unpinPage(heap_page, heap_file_);
  LOG_INFO("Inserted record with key {} into heap page ID {} successfully.",
           key, target_page_id);

  return RID{static_cast<uint16_t>(target_page_id),
             static_cast<uint16_t>(inserted_slot_id.value())};
}

void Table::insertIndexEntry(BufferPool& pool, int key, RID rid) {
  BTreeCursor::insertIntoIndex(pool, index_file_, key, rid.heap_page_id,
                               rid.slot_id);
}

void Table::invalidateHeapRecord(BufferPool& pool, RID rid) {
  Page* page = pool.pinPage(rid.heap_page_id, heap_file_);
  page->invalidateSlot(rid.slot_id);
  pool.unpinPage(page, heap_file_);
}

void Table::invalidateHeapRecord(BufferPool& pool, RID rid,
                                 LSNAllocator& allocator, WAL& wal) {
  Page* page = pool.pinPage(rid.heap_page_id, heap_file_);
  wal.write(make_wal_record(allocator, WALRecord::RecordType::DELETE,
                            rid.heap_page_id,
                            DeleteRedoBody(rid.slot_id).encode()));
  page->invalidateSlot(rid.slot_id);
  pool.unpinPage(page, heap_file_);
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