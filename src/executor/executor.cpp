#include "executor.h"

#include "index_scan.h"
#include "heap_fetch.h"

#include "../storage/btreecursor.h"
#include "../storage/bufferpool.h"
#include "../storage/file.h"
#include "../storage/page.h"
#include "../storage/record_cell.h"
#include "../logging.h"

#include <stdexcept>
#include <string>

namespace
{

std::pair<uint16_t, uint16_t> insertIntoHeap(
	BufferPool &pool, File &heapFile, int key, char *value, std::size_t value_size)
{
	LOG_INFO("Inserting record with key {} into heap file {}.", key, heapFile.getFilePath());
	RecordCell cell(value, value_size);

	int targetPageID = heapFile.getMaxPageID();
	Page *heapPage = pool.getPage(targetPageID, heapFile);
	auto insertedSlotID = heapPage->insertCell(cell);
	if (!insertedSlotID.has_value())
	{
		pool.unpin(heapPage, heapFile);
		// WARNING : currently how we deal with leafpage and heappage is the same.
		// Thus it is correct to set is_leaf = true here but, it's confusing.
		targetPageID = pool.createNewPage(true, heapFile);
		heapPage = pool.getPage(targetPageID, heapFile);
		insertedSlotID = heapPage->insertCell(cell);
		if (!insertedSlotID.has_value())
		{
			throw std::runtime_error("Failed to insert record cell into a new heap page due to insufficient space.");
		}
	}
	pool.unpin(heapPage, heapFile);
	LOG_INFO("Inserted record with key {} into heap page ID {} successfully.", key, targetPageID);

	return {static_cast<uint16_t>(targetPageID), static_cast<uint16_t>(insertedSlotID.value())};
}

} // namespace

char *executor::read(BufferPool &pool, File &indexFile, File &heapFile, int key)
{
	// find the record location from the index file.
	auto lookup = IndexLookup::fromKey(pool, indexFile, key);
	auto rid = lookup.next();
	if (!rid.has_value())
	{
		throw std::runtime_error("Key " + std::to_string(key) + " not found in index file.");
	}

	HeapFetch fetcher(pool, heapFile);
	return fetcher.fetch(rid->heap_page_id, rid->slot_id);
}

void executor::remove(BufferPool &pool, File &indexFile, File &heapFile, int key)
{
	auto location = BTreeCursor::findRecordLocation(pool, indexFile, key, true);
	if (!location.has_value())
	{
		throw std::runtime_error("Key " + std::to_string(key) + " not found in leaf page.");
	}
	auto [pageID, slotID] = location.value();
	Page *page = pool.getPage(pageID, heapFile);
	page->invalidateSlot(slotID);
	pool.unpin(page, heapFile);
	LOG_INFO("Removed record with key {} successfully.", key);
}

void executor::insert(BufferPool &pool, File &indexFile, File &heapFile,
				  int key, char *value, std::size_t value_size)
{
	LOG_INFO("Inserting record with key {} into index {}, heap {}", key, indexFile.getFilePath(), heapFile.getFilePath());
	// check if valid key already exists.
	auto location = BTreeCursor::findRecordLocation(pool, indexFile, key);
	if (location.has_value())
	{
		throw std::runtime_error("Key " + std::to_string(key) + " already exists. Duplicate keys are not allowed.");
	}

	auto [heap_page_id, slot_id] = insertIntoHeap(pool, heapFile, key, value, value_size);
	BTreeCursor::insertIntoIndex(pool, indexFile, key, heap_page_id, slot_id);
}

void executor::update(BufferPool &pool, File &indexFile, File &heapFile,
				  int key, char *value, std::size_t value_size)
{
	/**
	 * We first design updates to be idempotent by modeling them as a remove followed by an insert.
	 * This is not necessarily the most efficient strategy, but it keeps the update path simple and robust and fasten the development.
	 * Also, this unlocks follow-on benefits (e.g., easier recovery/retry and fewer page-structure assumptions) without requiring 
	 * in-place updates or special-case split handling.
	 */
	executor::remove(pool, indexFile, heapFile, key);
	executor::insert(pool, indexFile, heapFile, key, value, value_size);
}

