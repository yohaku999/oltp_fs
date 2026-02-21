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

std::optional<std::pair<uint16_t, uint16_t>> BTreeCursor::findRecordLocation(BufferPool& pool, File& indexFile, int key, bool do_invalidate)
{
    // NOTE: we decided not to invalidate intermediate nodes during traversal for now.
    // we will come back to this when we start to support concurrency.
    int pageID = findLeafPageID(pool, indexFile, key);
    Page *leafPage = pool.getPage(pageID, indexFile);
    return leafPage->findLeafRef(key, do_invalidate);
}

void BTreeCursor::update(BufferPool& pool, File& indexFile, File& heapFile, int key, char* value, size_t value_size)
{
    /**
     * We first design updates to be idempotent by modeling them as a remove followed by an insert.
     * This is not necessarily the most efficient strategy, but it keeps the update path simple and robust and fasten the development.
     * Also, this unlocks follow-on benefits (e.g., easier recovery/retry and fewer page-structure assumptions) without requiring 
     * in-place updates or special-case split handling.
     */
    BTreeCursor::remove(pool, indexFile, heapFile, key);
    BTreeCursor::insert(pool, indexFile, heapFile, key, value, value_size);
}

char* BTreeCursor::read(BufferPool& pool, File& indexFile, File& heapFile, int key)
{
    auto location = findRecordLocation(pool, indexFile, key);
    if (!location.has_value())
    {
        throw std::runtime_error("Key " + std::to_string(key) + " not found in leaf page.");
    }
    auto [pageID, slotID] = location.value();
    Page *page = pool.getPage(pageID, heapFile);
    return page->getXthSlotValue(slotID);
}

void BTreeCursor::remove(BufferPool& pool, File& indexFile, File& heapFile, int key)
{
    auto location = findRecordLocation(pool, indexFile, key, true);
    if (!location.has_value())
    {
        throw std::runtime_error("Key " + std::to_string(key) + " not found in leaf page.");
    }
    auto [pageID, slotID] = location.value();
    Page *page = pool.getPage(pageID, heapFile);
    page->invalidateSlot(slotID);
    spdlog::info("Removed record with key {} successfully.", key);
}

void BTreeCursor::insert(BufferPool& pool, File& indexFile, File& heapFile, int key, char* value, size_t value_size)
{
    spdlog::info("Inserting record with key {} into index {}, heap {}", key, indexFile.getFilePath(), heapFile.getFilePath());
    // check if valid key already exists.
    auto location = findRecordLocation(pool, indexFile, key);
    if (location.has_value())
    {
        throw std::runtime_error("Key " + std::to_string(key) + " already exists. Duplicate keys are not allowed.");
    }

    // insert to the heap file
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

    // find the leaf page to insert the new record.
    int leaf_page_id = findLeafPageID(pool, indexFile, key);
    Page *leafPage = pool.getPage(leaf_page_id, indexFile);
    auto leaf_slot_id = leafPage->insertCell(LeafCell(key, targetPageID, insertedSlotID.value()));
    if (!leaf_slot_id.has_value())
    {
        throw std::runtime_error("Failed to insert leaf cell for key " + std::to_string(key));
        // TODO: handle page splits when the leaf node is full.
    }
    spdlog::info("inserted record with key {} at heap page ID {}, slot ID {} successfully.", key, targetPageID, insertedSlotID.value());
}