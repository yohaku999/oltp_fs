#pragma once
#include <cstdint>
#include <cstring>
#include "cell.h"

class IntermediateCell : public Cell
{
private:
    // The value range of cell key, pageID is 0~4095 for now, so we can use uint16_t to store them.
    static constexpr size_t KEY_SIZE_BYTE = sizeof(int);
    static constexpr size_t PAYLOAD_SIZE = Cell::FLAG_FIELD_SIZE + sizeof(uint16_t) + sizeof(uint16_t) + KEY_SIZE_BYTE;
    uint16_t key_size_;
    uint16_t page_id_;
    int key_;

public:
    
    static IntermediateCell decodeCell(char *data_p);
    int key() const override { return key_; }
    uint16_t page_id() const { return page_id_; }
    uint16_t key_size() const { return key_size_; }
    size_t payloadSize() const override { return PAYLOAD_SIZE; }
    std::vector<std::byte> serialize() const override;
    CellKind kind() const override { return CellKind::Intermediate; }
    IntermediateCell(uint16_t page_id, int key) : key_size_(KEY_SIZE_BYTE), page_id_(page_id), key_(key) {}
};