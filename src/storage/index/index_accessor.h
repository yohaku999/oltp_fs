#pragma once

#include <string>
#include <utility>
#include <vector>

#include "catalog/table.h"
#include "storage/index/rid.h"

class BufferPool;
class LocalStorageManager;

class IndexAccessor {
 public:
  struct Boundary {
    std::string composite_key;
    bool is_inclusive;
  };

  using Boundaries = std::pair<Boundary, Boundary>;

  IndexAccessor(BufferPool& pool, LocalStorageManager& storage);

  IndexAccessor(const IndexAccessor&) = delete;
  IndexAccessor& operator=(const IndexAccessor&) = delete;

  std::vector<IndexEntry> find(const IndexMetadata& index,
                               const Boundaries& boundaries);
  bool contains(const IndexMetadata& index, const std::string& key);
  void insert(const IndexMetadata& index, const std::string& key,
              const RID& rid);
  void remove(const IndexMetadata& index, const std::string& key,
              const RID& rid);
  void replaceRID(const IndexMetadata& index, const std::string& key,
                  const RID& old_rid, const RID& new_rid);

 private:
  BufferPool& pool_;
  LocalStorageManager& storage_;
};
