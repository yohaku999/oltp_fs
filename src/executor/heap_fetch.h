#pragma once

#include <cstdint>

class BufferPool;
class File;

class HeapFetch {
public:
    HeapFetch(BufferPool &pool, File &heapFile);
    char *fetch(uint16_t heap_page_id, uint16_t slot_id);

private:
    BufferPool &pool_;
    File &heapFile_;
};
