#pragma once
#include <cstdint>
#include <cstring>
#include "cell.h"

class RecordCell : public Cell
{
private:
    char *value_;
    size_t value_size_;

public:
    RecordCell(char *value, size_t value_size)
        : value_(value), value_size_(value_size) {}

    static char* getValue(char *cell_start);
    size_t payloadSize() const override { return Cell::FLAG_FIELD_SIZE + sizeof(size_t) + value_size_; }
    std::vector<std::byte> serialize() const override;
    CellKind kind() const override { return CellKind::Record; }
};