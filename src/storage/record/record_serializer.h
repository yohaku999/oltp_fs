#pragma once

#include <vector>

#include "record_cell.h"
#include "schema/schema.h"
#include "tuple/typed_row.h"

class RecordSerializer {
 public:
  RecordSerializer(const Schema& schema, const TypedRow& row);

  const std::vector<std::byte>& serializedBytes() const {
    return serialized_bytes_;
  }

 private:
  std::vector<std::byte> serialized_bytes_;
};
