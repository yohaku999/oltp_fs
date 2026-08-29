#pragma once

#include <filesystem>
#include <string>
#include <utility>
#include <vector>

#include "storage/disk/pagedfile.h"
#include "storage/heap/heapfile.h"

class LocalStorageManager {
 public:
  explicit LocalStorageManager(std::filesystem::path root = "data")
      : root_(std::move(root)) {}

  std::string heapFileNameFor(const std::string& table_name) const;
  std::string indexFileNameFor(
      const std::string& table_name,
      const std::vector<std::string>& indexed_column_names) const;

  void createHeap(const std::string& file_name) const;
  void createIndex(const std::string& file_name) const;
  void remove(const std::string& file_name) const;

  bool exists(const std::string& file_name) const;

  HeapFile openHeap(const std::string& file_name) const;
  PagedFile openIndex(const std::string& file_name) const;

 private:
  std::string pathFor(const std::string& file_name) const;

  std::filesystem::path root_;
};
