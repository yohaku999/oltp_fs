#include "record_builder.h"

#include <cstring>

// fix builfer later.
RecordBuilder::RecordBuilder(char* value, size_t value_size)
    : serialized_bytes_(RecordCellLayout::HEADER_SIZE_BYTE + sizeof(uint16_t) +
                        value_size) {
  char* dst = reinterpret_cast<char*>(serialized_bytes_.data());
  uint8_t flags = 0;
  std::memcpy(dst, &flags, Cell::FLAG_FIELD_SIZE);
  dst += Cell::FLAG_FIELD_SIZE;

  const uint16_t variable_length_payload_begin_offset =
      static_cast<uint16_t>(RecordCellLayout::HEADER_SIZE_BYTE);
  std::memcpy(dst, &variable_length_payload_begin_offset, sizeof(uint16_t));
  dst += sizeof(uint16_t);

  const uint32_t null_bitmap = 0;
  std::memcpy(dst, &null_bitmap, sizeof(uint32_t));
  dst += sizeof(uint32_t);

  const uint16_t first_column_end_offset = static_cast<uint16_t>(value_size);
  std::memcpy(dst, &first_column_end_offset, sizeof(uint16_t));
  dst += sizeof(uint16_t);

  std::memcpy(dst, value, value_size);
}

std::vector<std::byte> RecordBuilder::serialize() const {
  return serialized_bytes_;
}