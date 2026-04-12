#pragma once

#include <optional>
#include <filesystem>
#include <stdexcept>
#include <string>
#include <utility>

#include "schema/schema.h"
#include "storage/index/rid.h"
#include "storage/runtime/file.h"
#include "tuple/typed_row.h"

class BufferPool;
class WAL;

class Table {
 public:
  static Table initialize(const std::string& table_name, const Schema& schema);

  static Table getTable(const std::string& table_name);

  static bool isPersisted(const std::string& table_name);

  static void removeBackingFilesFor(const std::string& table_name);

  const std::string& name() const { return name_; }
  const Schema& schema() const { return schema_; }
  std::optional<RID> findRID(BufferPool& pool, int key,
                             bool do_invalidate = false);
  TypedRow readRow(BufferPool& pool, RID rid) const;
  RID insertHeapRecord(BufferPool& pool, int key, const TypedRow& row,
                       WAL& wal);
  void insertIndexEntry(BufferPool& pool, int key, RID rid);
  void invalidateHeapRecord(BufferPool& pool, RID rid, WAL& wal);
  File& indexFile() { return index_file_; }
  File& heapFile() { return heap_file_; }

 private:
  Table(std::string name, Schema schema);

  static std::string defaultIndexPath(const std::string& table_name);

  static std::string defaultHeapPath(const std::string& table_name);

  static std::string defaultMetaPath(const std::string& table_name);

  static std::string preparePath(const std::string& path);

  static void removeFileIfExists(const std::string& path);

  static void writeSchemaMetadata(const std::string& meta_path,
                                  const Schema& schema);

  static Schema readSchemaMetadata(const std::string& table_name,
                                   const std::string& meta_path);

  static bool anyBackingFileExists(const std::string& table_name);

  std::string name_;
  Schema schema_;
  File index_file_;
  File heap_file_;
};