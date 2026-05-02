#include "storage/record/heap_file_access.h"

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

}  // namespace heap_file_access