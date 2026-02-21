#include "record_cell.h"
#include <vector>

std::vector<std::byte> RecordCell::serialize() const
{
    std::vector<std::byte> buffer(payloadSize());
    char *dst = reinterpret_cast<char *>(buffer.data());
    uint8_t flags = 0;
    std::memcpy(dst, &flags, Cell::FLAG_FIELD_SIZE);
    dst += Cell::FLAG_FIELD_SIZE;

    std::memcpy(dst, &key_, sizeof(int));
    dst += sizeof(int);
    std::memcpy(dst, &value_size_, sizeof(size_t));
    dst += sizeof(size_t);
    std::memcpy(dst, value_, value_size_);

    return buffer;
}