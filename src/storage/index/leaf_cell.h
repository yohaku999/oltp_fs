#pragma once
#include <cstdint>
#include <cstring>
#include <string>

#include "storage/page/cell.h"

class LeafCell : public Cell {
 private:
  uint16_t key_size_;
  uint16_t heap_page_id_;
  uint16_t slot_id_;
  std::string key_;

 public:
  static LeafCell decodeCell(char* data_p);
  static std::string getKey(const char* data_p);
  LeafCell(std::string key, uint16_t heap_page_id, uint16_t slot_id)
      : key_size_(static_cast<uint16_t>(key.size())),
        heap_page_id_(heap_page_id),
        slot_id_(slot_id),
        key_(std::move(key)) {}

  const std::string& key() const { return key_; }
  uint16_t heap_page_id() const { return heap_page_id_; }
  uint16_t slot_id() const { return slot_id_; }
  size_t payloadSize() const override {
    return Cell::FLAG_FIELD_SIZE + sizeof(uint16_t) + sizeof(uint16_t) +
           sizeof(uint16_t) + key_size_;
  }
  std::vector<std::byte> serialize() const override;
  CellKind kind() const override { return CellKind::Leaf; }
};
