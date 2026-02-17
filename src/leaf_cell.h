#pragma once
#include <cstdint>
#include <cstring>
#include "cell.h"

class LeafCell : public Cell
{
private:
    // The value range of pageID, slotID, cell key is 0~4095 for now, so we can use uint16_t to store them.
    static constexpr size_t KEY_SIZE_BYTE = sizeof(int);
    static constexpr size_t PAYLOAD_SIZE_BYTE = sizeof(uint16_t) + sizeof(uint16_t) + sizeof(uint16_t) + KEY_SIZE_BYTE;
    uint16_t key_size_;
    uint16_t heap_page_id_;
    uint16_t slot_id_;
    int key_;

public:
    static LeafCell decodeCell(char *data_p);
    LeafCell(int key, uint16_t heap_page_id, uint16_t slot_id)
        : key_size_(KEY_SIZE_BYTE), heap_page_id_(heap_page_id), slot_id_(slot_id), key_(key) {}

    int key() const override { return key_; }
    uint16_t heap_page_id() const { return heap_page_id_; }
    uint16_t slot_id() const { return slot_id_; }
    size_t payloadSize() const override { return PAYLOAD_SIZE_BYTE; }
    std::vector<std::byte> serialize() const override;
    CellKind kind() const override { return CellKind::Leaf; }
};
