#include "intermediate_cell.h"
#include <vector>

/**
 * The structure of intermediate cell is as follows:
 * | key size (2 bytes) | page ID (2 bytes) | key (4 bytes) |
 * The value range of pageID, cell key is 0~4095 for now, so we can use uint16_t to store them.
 * key size is the size of key in bytes, which is 4 for now since we only support int as key type, but we can support variable length key in the future by using key size to indicate the size of key.
 */
IntermediateCell IntermediateCell::decodeCell(char *data_p)
{
    uint16_t key_size = readValue<uint16_t>(data_p);
    char *page_id_p = data_p + sizeof(uint16_t);
    uint16_t cell_pageID = readValue<uint16_t>(page_id_p);
    char *key_p = page_id_p + sizeof(uint16_t);
    int cell_key = readValue<int>(key_p);
    return IntermediateCell(cell_pageID, cell_key);
}

std::vector<std::byte> IntermediateCell::serialize() const
{
    std::vector<std::byte> buffer(payloadSize());
    char *dst = reinterpret_cast<char *>(buffer.data());
    std::memcpy(dst, &key_size_, sizeof(uint16_t));
    dst += sizeof(uint16_t);
    std::memcpy(dst, &page_id_, sizeof(uint16_t));
    dst += sizeof(uint16_t);
    std::memcpy(dst, &key_, sizeof(int));

    return buffer;
}