#include "leaf_cell.h"

LeafCell LeafCell::decodeCell(char *data_p)
{
    LeafCell cell;

    cell.keySize = readValue<uint16_t>(data_p);
    data_p += CELL_KEY_SIZE;

    cell.heapPageID = readValue<uint16_t>(data_p);
    data_p += PAGE_ID_SIZE;

    cell.slotID = readValue<uint16_t>(data_p);
    data_p += SLOT_ID_SIZE;

    cell.key = readValue<int>(data_p);
    return cell;
}

void LeafCell::encodeCell(char *data_p) const
{
    std::memcpy(data_p, &keySize, sizeof(uint16_t));
    data_p += CELL_KEY_SIZE;
    std::memcpy(data_p, &heapPageID, sizeof(uint16_t));
    data_p += PAGE_ID_SIZE;
    std::memcpy(data_p, &slotID, sizeof(uint16_t));
    data_p += SLOT_ID_SIZE;
    std::memcpy(data_p, &key, sizeof(int));
}
