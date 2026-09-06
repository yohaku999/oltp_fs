#pragma once

#include <cstddef>
#include <string>
#include <vector>

#include "schema/schema.h"

struct IndexMetadata {
  std::string file_name;
  std::vector<std::string> column_names;
  bool is_unique;
};

class Table {
 public:
  Table(std::string name, Schema schema, std::string heap_file_name,
        std::vector<IndexMetadata> indexes = {});

  const std::string& name() const { return name_; }
  const Schema& schema() const { return schema_; }
  const std::string& heapFileName() const { return heap_file_name_; }
  const std::vector<IndexMetadata>& indexes() const { return indexes_; }

  bool hasIndex() const { return !indexes_.empty(); }
  bool hasIndexForColumn(const std::string& column_name) const;
  std::vector<std::size_t> indexedColumnIndexes(
      const IndexMetadata& index) const;

  void addIndex(IndexMetadata index);

 private:
  void validateIndex(const IndexMetadata& index) const;

  std::string name_;
  Schema schema_;
  std::string heap_file_name_;
  std::vector<IndexMetadata> indexes_;
};
