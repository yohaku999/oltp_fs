#include "btreecursor.h"
#include "bufferpool.h"
#include "page.h"
#include "cell.h"
#include <cstdint>
#include <string>
#include <utility>
#include "record_cell.h"
#include <optional>
#include <spdlog/spdlog.h>
#include <stdexcept>

int BTreeCursor::findLeafPageID(BufferPool& pool, File& indexFile, int key)
{
    int pageID = 0;
    while (true)
    {
        Page *page = pool.getPage(pageID, indexFile);
        if (page->isLeaf())
        {
            spdlog::info("Found leaf page ID {} for key {} in index {}", pageID, key, indexFile.getFilePath());
            break;
        }
        pageID = page->findChildPage(key);
    }
    return pageID;
}

std::pair<uint16_t, uint16_t> BTreeCursor::findRecordLocation(BufferPool& pool, File& indexFile, int key, bool do_invalidate)
{
    // TODO: do not invalidate index nodes during traversal for now. we will come back to this when we start to support concurrency.
    int pageID = findLeafPageID(pool, indexFile, key);
    Page *leafPage = pool.getPage(pageID, indexFile);
    auto ref = leafPage->findLeafRef(key);
    if (!ref.has_value())
    {
        throw std::runtime_error("Key " + std::to_string(key) + " not found in leaf page.");
    }
    return ref.value();
}

char* BTreeCursor::read(BufferPool& pool, File& indexFile, File& heapFile, int key)
{
    auto [pageID, slotID] = findRecordLocation(pool, indexFile, key);
    Page *page = pool.getPage(pageID, heapFile);
    return page->getXthSlotValue(slotID);
}

void BTreeCursor::remove(BufferPool& pool, File& indexFile, File& heapFile, int key)
{   auto [pageID, slotID] = findRecordLocation(pool, indexFile, key, true);
    Page *page = pool.getPage(pageID, heapFile);
    page->invalidateSlot(slotID);
    spdlog::info("Removed record with key {} successfully.", key);
}

void BTreeCursor::insert(BufferPool& pool, File& indexFile, File& heapFile, int key, char* value, size_t value_size)
{
    int leaf_page_id = findLeafPageID(pool, indexFile, key);
    Page *leafPage = pool.getPage(leaf_page_id, indexFile);
    if (leafPage->hasKey(key))
    {
        throw std::logic_error("The key " + std::to_string(key) + " already exists.");
    }

    RecordCell cell(key, value, value_size);
    int targetPageID = heapFile.getMaxPageID();
    Page *heapPage = pool.getPage(targetPageID, heapFile);
    auto insertedSlotID = heapPage->insertCell(cell);
    if (!insertedSlotID.has_value())
    {
        targetPageID = heapFile.allocateNextPageId();
        heapPage = pool.getPage(targetPageID, heapFile);
        insertedSlotID = heapPage->insertCell(cell);
        if (!insertedSlotID.has_value())
        {
            throw std::runtime_error("Failed to insert record cell into a new page due to insufficient space.");
        }
    }
    spdlog::info("Inserted record with key {} into heap page ID {} successfully.", key, targetPageID);

    auto leaf_slot_id = leafPage->insertCell(LeafCell(key, targetPageID, insertedSlotID.value()));
    if (!leaf_slot_id.has_value())
    {
        throw std::runtime_error("Failed to insert leaf cell for key " + std::to_string(key));
        // TODO: handle page splits when the leaf node is full.
    }
    spdlog::info("inserted record with key {} at heap page ID {}, slot ID {} successfully.", key, targetPageID, insertedSlotID.value());
}