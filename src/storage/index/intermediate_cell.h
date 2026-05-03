#pragma once
#include <cstdint>
#include <cstring>
#include <string>

#include "storage/page/cell.h"

class IntermediateCell : public Cell {
 private:
  uint16_t key_size_;
  uint16_t page_id_;
  std::string key_;

 public:
  static IntermediateCell decodeCell(char* data_p);
  static std::string getKey(const char* data_p);
  const std::string& key() const { return key_; }
  uint16_t page_id() const { return page_id_; }
  uint16_t key_size() const { return key_size_; }
  size_t payloadSize() const override {
    return Cell::FLAG_FIELD_SIZE + sizeof(uint16_t) + sizeof(uint16_t) +
           key_size_;
  }
  std::vector<std::byte> serialize() const override;
  CellKind kind() const override { return CellKind::Intermediate; }
  IntermediateCell(uint16_t page_id, std::string key)
      : key_size_(static_cast<uint16_t>(key.size())),
        page_id_(page_id),
        key_(std::move(key)) {}
};