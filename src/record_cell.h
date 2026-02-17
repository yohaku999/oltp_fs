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

    int key() const override { return key_; }
    size_t payloadSize() const override { return sizeof(int) + sizeof(size_t) + value_size_; }
    std::vector<std::byte> RecordCell::serialize() const;
    CellKind kind() const override { return CellKind::Record; }
};