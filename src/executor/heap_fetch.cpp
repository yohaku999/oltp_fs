#include "heap_fetch.h"

#include "../storage/bufferpool.h"
#include "../storage/file.h"
#include "../storage/page.h"

HeapFetch::HeapFetch(BufferPool &pool, File &heapFile)
    : pool_(pool), heapFile_(heapFile)
{
}

char *HeapFetch::fetch(uint16_t heap_page_id, uint16_t slot_id)
{
    Page *page = pool_.getPage(heap_page_id, heapFile_);
    char *p = page->getXthSlotCellStart(slot_id);
    pool_.unpin(page, heapFile_); // ここのunpin矛盾しないか？
    return p;
}