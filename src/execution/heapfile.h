#pragma once

#include <array>
#include <cstddef>
#include <optional>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "storage/buffer/bufferpool.h"
#include "storage/disk/pagedfile.h"
#include "storage/index/rid.h"
#include "storage/page/cell.h"
#include "storage/page/page.h"
#include "storage/record/record_cell.h"
#include "storage/wal/wal.h"
#include "storage/wal/wal_body.h"
#include "storage/wal/wal_record.h"
#include "execution/heapscan.h"

class HeapFile {
 public:
  explicit HeapFile(std::string path) : file_(std::move(path)) {}

  void initialize() {
    std::array<char, Page::PAGE_SIZE_BYTE> buffer{};
    Page(buffer.data(), PageKind::Heap, 0, 0);
    file_.writePageFromBuffer(0, buffer.data());
  }

  bool isPageIDUsed(uint16_t page_id) const {
    return file_.isPageIDUsed(page_id);
  }

  /**
   * Collects all RIDs from the heap file.
   */
  std::vector<RID> collectRids(BufferPool& pool) {
    std::vector<RID> rids;
    for (uint16_t page_id = 0; page_id <= file_.getMaxPageID(); ++page_id) {
      Page* page = pool.pinPage(page_id, file_);
      for (uint16_t slot_id = 0; slot_id < page->slotCount(); ++slot_id) {
        char* cell_start = page->slotCellStartUnchecked(slot_id);
        if (!Cell::isValid(cell_start)) {
          continue;
        }
        rids.push_back(RID{page_id, slot_id});
      }
      pool.unpinPage(page, file_);
    }

    return rids;
  }

  /**
   * Inserts a record cell into the heap file and returns the RID of the inserted record.
   */
  RID insertRecord(BufferPool& pool, const RecordCell& cell) {
    const uint16_t insert_page_id = file_.getMaxPageID();
    if (auto rid = tryInsertIntoPage(pool, insert_page_id, cell)) {
      return *rid;
    }
    const uint16_t new_page_id = pool.createPage(PageKind::Heap, file_);
    if (auto rid = tryInsertIntoPage(pool, new_page_id, cell)) {
      return *rid;
    }
    throw std::runtime_error(
        "Record cell does not fit in an empty heap page.");
  }

  void invalidateRecord(BufferPool& pool, const RID& rid,
                          const RecordCell& before) {
    Page* page = pool.pinPage(rid.heap_page_id, file_);
    page->invalidateSlot(rid.slot_id);
    pool.wal().write(
        WALRecord::RecordType::DELETE, rid.heap_page_id,
        DeleteRedoBody(rid.slot_id, before.serializedBytes()).encode());
    pool.unpinPage(page, file_);
  }

  HeapScan scan(BufferPool& pool) { return HeapScan(pool, file_); }

  template <typename Reader>
  auto withRecord(BufferPool& pool, const RID& rid, Reader&& reader) const
      -> std::optional<std::invoke_result_t<Reader, RecordCellView>> {
    Page* page = pool.pinPage(rid.heap_page_id, file_);
    char* cell_start = page->slotCellStartUnchecked(rid.slot_id);
    if (!Cell::isValid(cell_start)) {
      pool.unpinPage(page, file_);
      return std::nullopt;
    }

    auto result =
        std::forward<Reader>(reader)(RecordCellView(cell_start));
    pool.unpinPage(page, file_);
    return result;
  }

  bool tryUpdateRecord(const RID& rid, BufferPool& pool,
                    const RecordCell& before, const RecordCell& after) {
    Page* page = pool.pinPage(rid.heap_page_id, file_);
    const bool is_updated = page->updateRecordCell(rid.slot_id, after);
    if (is_updated) {
      UpdateRedoBody update_body{};
      update_body.offset = rid.slot_id;
      update_body.before = before.serializedBytes();
      update_body.after = after.serializedBytes();
      pool.wal().write(WALRecord::RecordType::UPDATE, rid.heap_page_id,
                       update_body.encode());
    }
    pool.unpinPage(page, file_);
    return is_updated;
  }

 private:
  mutable PagedFile file_;

  std::optional<RID> tryInsertIntoPage(BufferPool& pool, uint16_t page_id,
                                       const RecordCell& cell) {
    Page* page = pool.pinPage(page_id, file_);
    const auto slot_id = page->insertCell(cell);

    if (!slot_id.has_value()) {
      pool.unpinPage(page, file_);
      return std::nullopt;
    }

    const RID rid{page_id, static_cast<uint16_t>(*slot_id)};
    pool.wal().write(
        WALRecord::RecordType::INSERT, rid.heap_page_id,
        InsertRedoBody(rid.slot_id, cell.serializedBytes()).encode());
    pool.unpinPage(page, file_);
    return rid;
  }

};
