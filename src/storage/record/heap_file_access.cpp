#include "storage/record/heap_file_access.h"

#include <stdexcept>

#include "storage/page/cell.h"
#include "storage/page/page.h"
#include "storage/runtime/bufferpool.h"
#include "storage/runtime/file.h"

namespace heap_file_access {

std::vector<RID> collectRids(BufferPool& pool, File& heap_file) {
  std::vector<RID> rids;
  for (uint16_t page_id = 0; page_id <= heap_file.getMaxPageID(); ++page_id) {
    Page* page = pool.pinPage(page_id, heap_file);
    for (uint16_t slot_id = 0; slot_id < page->slotCount(); ++slot_id) {
      char* cell_start = page->slotCellStartUnchecked(slot_id);
      if (!Cell::isValid(cell_start)) {
        continue;
      }
      rids.push_back(RID{page_id, slot_id});
    }
    pool.unpinPage(page, heap_file);
  }

  return rids;
}

RID appendCell(BufferPool& pool, File& heap_file,
               const std::vector<std::byte>& serialized_cell) {
  uint16_t target_page_id = heap_file.getMaxPageID();
  Page* heap_page = pool.pinPage(target_page_id, heap_file);
  auto inserted_slot_id = heap_page->insertCell(serialized_cell);
  if (!inserted_slot_id.has_value()) {
    pool.unpinPage(heap_page, heap_file);
    target_page_id = pool.createPage(PageKind::Heap, heap_file);
    heap_page = pool.pinPage(target_page_id, heap_file);
    inserted_slot_id = heap_page->insertCell(serialized_cell);
    if (!inserted_slot_id.has_value()) {
      throw std::runtime_error(
          "Failed to insert record cell into a new heap page due to "
          "insufficient space.");
    }
  }

  pool.unpinPage(heap_page, heap_file);
  return RID{target_page_id, static_cast<uint16_t>(inserted_slot_id.value())};
}

}  // namespace heap_file_access