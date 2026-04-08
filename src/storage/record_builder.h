#pragma once

#include <cstddef>
#include <vector>

#include "cell.h"
#include "record_cell.h"

class RecordBuilder : public Cell {
 public:
  RecordBuilder(char* value, size_t value_size);

  size_t payloadSize() const override { return serialized_bytes_.size(); }
  std::vector<std::byte> serialize() const override;
  CellKind kind() const override { return CellKind::Record; }

 private:
  std::vector<std::byte> serialized_bytes_;
};