#pragma once
#include <optional>
#include <string>
#include <utility>
#include <ostream>
#include "bufferpool.h"

/**
 * BTreeCursor is an arbitration layer that coordinates BufferPool and File to execute
 * B-Tree index traversal and insertion of the actual record into the heap file as a
 * single transactional sequence.
 * 
 * A path-generation helper centralizes file naming under ./data/<table>.{index,db}.
 * File is responsible for page-ID allocation and persistence (e.g., maintaining the
 * high-water mark), while BufferPool provides page caching.
 */
class BTreeCursor
{
private:
    static std::optional<std::pair<uint16_t, uint16_t>> findRecordLocation(
        BufferPool& pool, File& indexFile, int key, bool do_invalidate=false);
    static int findLeafPageID(BufferPool& pool, File& indexFile, int key);
    static void splitPage(BufferPool& pool, File& indexFile, Page* old_page);
    static std::pair<uint16_t, uint16_t> insertIntoHeap(
        BufferPool& pool, File& heapFile, int key, char* value, size_t value_size);
    static void insertIntoIndex(
        BufferPool& pool, File& indexFile, int key, uint16_t heap_page_id, uint16_t slot_id);
    static void splitLeafPage(BufferPool& pool, File& index_file, Page& old_page, Page& parent_page, char* separate_key);
    static void splitInternalPage(BufferPool& pool, File& index_file, Page& old_page, Page& parent_page, char* separate_key);
    static Page* ensureParentPage(BufferPool& pool, File& index_file, Page& old_page);
public:
    static char* read(BufferPool& pool, File& indexFile, File& heapFile, int key);
    static void insert(BufferPool& pool, File& indexFile, File& heapFile, int key, char* value, size_t value_size);
    static void remove(BufferPool& pool, File& indexFile, File& heapFile, int key);
    static void update(BufferPool& pool, File& indexFile, File& heapFile, int key, char* value, size_t value_size);

    static void dumpTree(BufferPool& pool, File& indexFile, std::ostream& os);
};