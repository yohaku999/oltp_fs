#pragma once

#include <filesystem>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "storage/disk/pagedfile.h"
#include "storage/heap/heapfile.h"

class LocalStorageManager {
 public:
  explicit LocalStorageManager(std::filesystem::path root = "data")
      : root_(std::move(root)) {}

  LocalStorageManager(const LocalStorageManager&) = delete;
  LocalStorageManager& operator=(const LocalStorageManager&) = delete;

  std::string heapFileNameFor(const std::string& table_name) const;
  std::string indexFileNameFor(
      const std::string& table_name,
      const std::vector<std::string>& indexed_column_names) const;

  void createHeap(const std::string& file_name);
  void createIndex(const std::string& file_name);
  void remove(const std::string& file_name);
  void flush();

  bool exists(const std::string& file_name) const;

  PagedFile& open(const std::string& file_name);
  HeapFile openHeap(const std::string& file_name);

 private:
  std::string pathFor(const std::string& file_name) const;

  std::filesystem::path root_;
  std::unordered_map<std::string, PagedFile> files_;
};
