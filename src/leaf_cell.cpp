#include "leaf_cell.h"
#include <vector>

/**
 * The structure of leaf cell is as follows:
 * | key size (2 bytes) | heap page ID (2 bytes) | slot ID (2 bytes) | key (4 bytes) |
 * The value range of pageID, slotID, cell key is 0~4095 for now, so we can use uint16_t to store them.
 */
LeafCell LeafCell::decodeCell(char *data_p)
{

    uint16_t key_size = readValue<uint16_t>(data_p);
    data_p += sizeof(uint16_t);

    uint16_t heap_page_id = readValue<uint16_t>(data_p);
    data_p += sizeof(uint16_t);

    uint16_t slot_id = readValue<uint16_t>(data_p);
    data_p += sizeof(uint16_t);

    int key = readValue<int>(data_p);
    return LeafCell(key, heap_page_id, slot_id);
}

std::vector<std::byte> LeafCell::serialize() const
{
    std::vector<std::byte> buffer(payloadSize());
    char *dst = reinterpret_cast<char *>(buffer.data());

    std::memcpy(dst, &key_size_, sizeof(uint16_t));
    dst += sizeof(uint16_t);
    std::memcpy(dst, &heap_page_id_, sizeof(uint16_t));
    dst += sizeof(uint16_t);
    std::memcpy(dst, &slot_id_, sizeof(uint16_t));
    dst += sizeof(uint16_t);
    std::memcpy(dst, &key_, sizeof(int));

    return buffer;
}
