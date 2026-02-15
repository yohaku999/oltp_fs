#include "intermediate_cell.h"

IntermediateCell IntermediateCell::decodeCell(char *data_p)
{
    IntermediateCell cell;
    cell.keySize = readValue<uint16_t>(data_p);
    char *cell_key_p = data_p + CELL_KEY_SIZE;
    uint16_t cell_pageID;
    cell.pageID = readValue<uint16_t>(cell_key_p);
    cell_key_p += PAGE_ID_SIZE;
    cell.key = readValue<int>(cell_key_p);
    return cell;
}

void IntermediateCell::encodeCell(char *data_p) const
{
    std::memcpy(data_p, &keySize, sizeof(uint16_t));
    data_p += CELL_KEY_SIZE;
    std::memcpy(data_p, &pageID, sizeof(uint16_t));
    data_p += PAGE_ID_SIZE;
    std::memcpy(data_p, &key, sizeof(int));
}