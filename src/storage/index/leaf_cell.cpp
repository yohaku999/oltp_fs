#include "leaf_cell.h"

#include <vector>

/**
 * The structure of leaf cell is as follows:
 * | key size (2 bytes) | heap page ID (2 bytes) | slot ID (2 bytes) | key
 * bytes |
 */
LeafCell LeafCell::decodeCell(char* data_p) {
  data_p += Cell::FLAG_FIELD_SIZE;
  uint16_t key_size = readValue<uint16_t>(data_p);
  data_p += sizeof(uint16_t);

  uint16_t heap_page_id = readValue<uint16_t>(data_p);
  data_p += sizeof(uint16_t);

  uint16_t slot_id = readValue<uint16_t>(data_p);
  data_p += sizeof(uint16_t);

  std::string key(data_p, data_p + key_size);
  return LeafCell(std::move(key), heap_page_id, slot_id);
}

std::string LeafCell::getKey(const char* data_p) {
  const uint16_t key_size =
      readValue<uint16_t>(data_p + Cell::FLAG_FIELD_SIZE);
  const char* key_p = data_p + Cell::FLAG_FIELD_SIZE + sizeof(uint16_t) * 3;
  return std::string(key_p, key_p + key_size);
}

std::vector<std::byte> LeafCell::serialize() const {
  std::vector<std::byte> buffer(payloadSize());
  char* dst = reinterpret_cast<char*>(buffer.data());
  uint8_t flags = 0;
  std::memcpy(dst, &flags, Cell::FLAG_FIELD_SIZE);
  dst += Cell::FLAG_FIELD_SIZE;

  std::memcpy(dst, &key_size_, sizeof(uint16_t));
  dst += sizeof(uint16_t);
  std::memcpy(dst, &heap_page_id_, sizeof(uint16_t));
  dst += sizeof(uint16_t);
  std::memcpy(dst, &slot_id_, sizeof(uint16_t));
  dst += sizeof(uint16_t);
  std::memcpy(dst, key_.data(), key_.size());

  return buffer;
}
