#pragma once

#include <optional>
#include <filesystem>
#include <functional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "catalog/table_metadata.h"
#include "schema/schema.h"
#include "storage/index/rid.h"
#include "storage/runtime/file.h"
#include "tuple/typed_row.h"

class BufferPool;
class WAL;

struct ExactMatchIndexColumnValue {
  std::string column_name;
  FieldValue value;
};

class Table {
 public:
  static Table initialize(const std::string& table_name, const Schema& schema);

  static Table getTable(const std::string& table_name);

  static bool isPersisted(const std::string& table_name);

  static void removeBackingFilesFor(const std::string& table_name);

  void createIndex(const std::vector<std::string>& column_names);

  const std::string& name() const { return name_; }
  const Schema& schema() const { return schema_; }
  bool hasIndexForColumn(const std::string& column_name) const;
  std::string extractIndexKey(const TypedRow& row) const;
  std::optional<std::string> tryBuildExactMatchIndexKey(
      const std::vector<ExactMatchIndexColumnValue>& exact_match_values) const;
  const std::vector<std::string>& indexedColumnNames() const {
    return indexed_column_names_;
  }
  std::vector<std::size_t> indexedColumnIndexes() const;
  std::optional<std::reference_wrapper<File>> indexFile();
  File& requireIndexFile();
  File& heapFile() { return heap_file_; }

 private:
        Table(std::string name, Schema schema,
          std::optional<std::string> index_path,
          std::vector<std::string> indexed_column_names);

  static std::string defaultIndexPath(
        const std::string& table_name,
        const std::vector<std::string>& indexed_column_names);

  static std::string defaultHeapPath(const std::string& table_name);

  static std::string preparePath(const std::string& path);

  static void removeFileIfExists(const std::string& path);

  static bool anyBackingFileExists(const std::string& table_name);

  std::string name_;
  Schema schema_;
  std::vector<std::string> indexed_column_names_;
  std::optional<File> index_file_;
  File heap_file_;
};