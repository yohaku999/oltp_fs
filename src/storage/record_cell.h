#pragma once
#include <cstdint>
#include <cstring>
#include "cell.h"

class RecordCell : public Cell
{
private:
    int key_;
    char *value_;
    size_t value_size_;

public:
    RecordCell(int key, char *value, size_t value_size)
        : key_(key), value_(value), value_size_(value_size) {}

    static int getKey(const char *data_p);
    int key() const override { return key_; }
    size_t payloadSize() const override { return Cell::FLAG_FIELD_SIZE + sizeof(int) + sizeof(size_t) + value_size_; }
    std::vector<std::byte> serialize() const override;
    CellKind kind() const override { return CellKind::Record; }
};