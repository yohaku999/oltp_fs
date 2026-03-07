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
#include "logging.h"

int BTreeCursor::findLeafPageID(BufferPool& pool, File& indexFile, int key)
{
    // traverse the btree from the root page (page ID 0) until we find the leaf page that may contain the key.
    int pageID = 0;
    int parentPageID = Page::HAS_NO_PARENT;
    while (true)
    {
        Page *page = pool.getPage(pageID, indexFile);
        // if the page has parent page id of -1, the page is the root page or just not memoed yet. 
        page->setParentPageID(parentPageID);
        if (page->isLeaf())
        {
            LOG_INFO("Found leaf page ID {} for key {} in index {}", pageID, key, indexFile.getFilePath());
            pool.unpin(page, indexFile);
            break;
        }
        int childPageID = page->findChildPage(key);
        pool.unpin(page, indexFile);
        LOG_INFO("The child page ID of page ID {} for key {} is {}", pageID, key, childPageID);
        parentPageID = pageID;
        pageID = childPageID;
    }
    return pageID;
}

std::optional<std::pair<uint16_t, uint16_t>> BTreeCursor::findRecordLocation(BufferPool& pool, File& indexFile, int key, bool do_invalidate)
{
    // NOTE: we decided not to invalidate intermediate nodes during traversal for now.
    // we will come back to this when we start to support concurrency.
    int pageID = findLeafPageID(pool, indexFile, key);
    Page* leafPage = pool.getPage(pageID, indexFile);
    auto result = leafPage->findLeafRef(key, do_invalidate);
    pool.unpin(leafPage, indexFile);
    return result;
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
    Page* page = pool.getPage(pageID, heapFile);
    // NOTE: we intentionally do not unpin here because the caller
    // receives a pointer into the page buffer.
    char* value = page->getXthSlotValue(slotID);
    pool.unpin(page, heapFile);
    return value;
}

void BTreeCursor::remove(BufferPool& pool, File& indexFile, File& heapFile, int key)
{
    auto location = findRecordLocation(pool, indexFile, key, true);
    if (!location.has_value())
    {
        throw std::runtime_error("Key " + std::to_string(key) + " not found in leaf page.");
    }
    auto [pageID, slotID] = location.value();
    Page* page = pool.getPage(pageID, heapFile);
    page->invalidateSlot(slotID);
    pool.unpin(page, heapFile);
    LOG_INFO("Removed record with key {} successfully.", key);
}

void BTreeCursor::insert(BufferPool& pool, File& indexFile, File& heapFile, int key, char* value, size_t value_size)
{
    LOG_INFO("Inserting record with key {} into index {}, heap {}", key, indexFile.getFilePath(), heapFile.getFilePath());
    // check if valid key already exists.
    auto location = findRecordLocation(pool, indexFile, key);
    if (location.has_value())
    {
        throw std::runtime_error("Key " + std::to_string(key) + " already exists. Duplicate keys are not allowed.");
    }

    // insert to the heap file
    RecordCell cell(key, value, value_size);
    int targetPageID = heapFile.getMaxPageID();
    Page* heapPage = pool.getPage(targetPageID, heapFile);
    auto insertedSlotID = heapPage->insertCell(cell);
    if (!insertedSlotID.has_value())
    {
        pool.unpin(heapPage, heapFile);
        targetPageID = heapFile.allocateNextPageId();
        heapPage = pool.getPage(targetPageID, heapFile);
        insertedSlotID = heapPage->insertCell(cell);
        if (!insertedSlotID.has_value())
        {
            throw std::runtime_error("Failed to insert record cell into a new page due to insufficient space.");
        }
    }
    pool.unpin(heapPage, heapFile);
    LOG_INFO("Inserted record with key {} into heap page ID {} successfully.", key, targetPageID);

    // find the leaf page to insert the new record.
    int leaf_page_id = findLeafPageID(pool, indexFile, key);
    Page* leafPage = pool.getPage(leaf_page_id, indexFile);
    auto leaf_slot_id = leafPage->insertCell(LeafCell(key, targetPageID, insertedSlotID.value()));
    if (!leaf_slot_id.has_value())
    {   
        splitPage(pool, indexFile, leafPage);
        // after split, try to insert again. we can guarantee the second insert will succeed because splitPage will create a new page and transfer half of the cells to the new page.
        leaf_slot_id = leafPage->insertCell(LeafCell(key, targetPageID, insertedSlotID.value()));
    }
    pool.unpin(leafPage, indexFile);
    LOG_INFO("inserted record with key {} at heap page ID {}, slot ID {} successfully.", key, targetPageID, insertedSlotID.value());
}

void BTreeCursor::splitPage(BufferPool& pool, File& index_file, Page* old_page)
{
    LOG_INFO("splitPage called on page ID {}.", old_page->getPageID());

    // Ensure the old page has a valid parent; create a new root if needed.
    int parent_page_id;
    if (old_page->getParentPageID() == Page::HAS_NO_PARENT)
    {
        LOG_INFO("Initialized Parent Page for page ID {} because it has no parent.", old_page->getPageID());
        // Create intermediate page as the new root and parent of old_page.
        int new_root_page_id = pool.createNewPage(false, index_file, old_page->getPageID());
        old_page->setParentPageID(new_root_page_id);
        parent_page_id = new_root_page_id;
    }
    else
    {
        parent_page_id = old_page->getParentPageID();
    }

    // Pin the parent page for the duration of the split.
    Page* parent_page = pool.getPage(parent_page_id, index_file);

    LOG_INFO("Split old page and rewire pointer.");
    char* separate_key = old_page->getSeparateKey();

    // Pointer from parent to its old child does not change on split.
    if (old_page->isLeaf())
    {
        int new_page_id = pool.createNewPage(true, index_file);
        Page* new_page = pool.getPage(new_page_id, index_file);
        old_page->transferCellsTo(new_page, separate_key);
        parent_page->insertCell(IntermediateCell(new_page_id, LeafCell::getKey(separate_key)));
        pool.unpin(new_page, index_file);
    }
    else
    {
        int new_page_id = pool.createNewPage(false, index_file, old_page->getPageID());
        Page* new_page = pool.getPage(new_page_id, index_file);
        old_page->transferCellsTo(new_page, separate_key);
        parent_page->insertCell(IntermediateCell(new_page_id, IntermediateCell::getKey(separate_key)));
        pool.unpin(new_page, index_file);
    }
    pool.unpin(parent_page, index_file);
}