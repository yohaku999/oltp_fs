#pragma once

#include <optional>
#include <filesystem>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "schema/schema.h"
#include "storage/index/rid.h"
#include "storage/runtime/file.h"
#include "tuple/typed_row.h"

class BufferPool;
class WAL;

class Table {
 public:
  static Table initialize(const std::string& table_name, const Schema& schema,
                          std::optional<std::string> indexed_column_name =
                              std::nullopt);

  static Table getTable(const std::string& table_name);

  static bool isPersisted(const std::string& table_name);

  static void removeBackingFilesFor(const std::string& table_name);

  const std::string& name() const { return name_; }
  const Schema& schema() const { return schema_; }
  bool hasIndexForColumn(const std::string& column_name) const;
  const std::optional<std::string>& indexedColumnName() const {
    return indexed_column_name_;
  }
  File& indexFile() { return index_file_; }
  File& heapFile() { return heap_file_; }

 private:
    struct PersistedIndex {
      std::string index_path;
      std::optional<std::string> indexed_column_name;
    };

    struct PersistedMetadata {
      Schema schema;
      std::vector<PersistedIndex> indexes;
    };

    Table(std::string name, Schema schema, std::string index_path,
          std::optional<std::string> indexed_column_name);

  static std::string defaultIndexPath(
        const std::string& table_name,
        const std::optional<std::string>& indexed_column_name);

  static std::string defaultHeapPath(const std::string& table_name);

  static std::string defaultMetaPath(const std::string& table_name);

  static std::string preparePath(const std::string& path);

  static void removeFileIfExists(const std::string& path);

  static void writeSchemaMetadata(
      const std::string& meta_path, const Schema& schema,
      const std::vector<PersistedIndex>& indexes);

  static PersistedMetadata readSchemaMetadata(const std::string& table_name,
                                              const std::string& meta_path);

  static std::optional<std::string> findPersistedIndexedColumn(
      const std::string& table_name);

  static bool anyBackingFileExists(const std::string& table_name);

  std::string name_;
  Schema schema_;
  std::optional<std::string> indexed_column_name_;
  File index_file_;
  File heap_file_;
};