#include "intermediate_cell.h"

#include <vector>

/**
 * The structure of intermediate cell is as follows:
 * | key size (2 bytes) | page ID (2 bytes) | key bytes |
 */
IntermediateCell IntermediateCell::decodeCell(char* data_p) {
  return IntermediateCell(readPageID(data_p), std::string(keyView(data_p)));
}

std::string_view IntermediateCell::keyView(const char* data_p) {
  // Skip: FLAG (1 byte) + key_size (2 bytes) + page_id (2 bytes)
  const uint16_t key_size = readValue<uint16_t>(data_p + Cell::FLAG_FIELD_SIZE);
  const char* key_p = data_p + Cell::FLAG_FIELD_SIZE + sizeof(uint16_t) * 2;
  return std::string_view(key_p, key_size);
}

uint16_t IntermediateCell::readPageID(const char* data_p) {
  return readValue<uint16_t>(data_p + Cell::FLAG_FIELD_SIZE + sizeof(uint16_t));
}

std::vector<std::byte> IntermediateCell::serialize() const {
  std::vector<std::byte> buffer(payloadSize());
  char* dst = reinterpret_cast<char*>(buffer.data());
  uint8_t flags = 0;
  std::memcpy(dst, &flags, Cell::FLAG_FIELD_SIZE);
  dst += Cell::FLAG_FIELD_SIZE;
  std::memcpy(dst, &key_size_, sizeof(uint16_t));
  dst += sizeof(uint16_t);
  std::memcpy(dst, &page_id_, sizeof(uint16_t));
  dst += sizeof(uint16_t);
  std::memcpy(dst, key_.data(), key_.size());

  return buffer;
}
