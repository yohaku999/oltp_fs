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
#include "storage/disk/file.h"
#include "storage/index/rid.h"
#include "storage/page/cell.h"
#include "storage/page/page.h"
#include "storage/record/record_cell.h"

class HeapFile {
 public:
  explicit HeapFile(std::string path) : file_(std::move(path)) {}

  void initialize() {
    std::array<char, Page::PAGE_SIZE_BYTE> buffer{};
    Page::initializeNew(buffer.data(), PageKind::Heap, 0, 0);
    file_.writePageFromBuffer(0, buffer.data());
  }

  File& rawFile() { return file_; }
  const File& rawFile() const { return file_; }

  bool isPageIDUsed(uint16_t page_id) const {
    return file_.isPageIDUsed(page_id);
  }

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

  template <typename Fn>
  auto withCell(BufferPool& pool, const RID& rid, Fn&& fn) const
      -> std::optional<std::invoke_result_t<Fn, RecordCellView>> {
    using Result = std::invoke_result_t<Fn, RecordCellView>;

    Page* page = pool.pinPage(rid.heap_page_id, file_);
    char* cell_start = page->slotCellStartUnchecked(rid.slot_id);
    if (!Cell::isValid(cell_start)) {
      pool.unpinPage(page, file_);
      return std::nullopt;
    }

    Result result = std::forward<Fn>(fn)(RecordCellView(cell_start));
    pool.unpinPage(page, file_);
    return result;
  }

 private:
  mutable File file_;
};
