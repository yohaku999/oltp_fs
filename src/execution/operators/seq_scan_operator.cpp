#include "seq_scan_operator.h"

#include "schema/schema.h"
#include "storage/buffer/bufferpool.h"
#include "storage/disk/file.h"
#include "storage/page/cell.h"
#include "storage/page/page.h"
#include "storage/record/record_cell.h"
SeqScanOperator::SeqScanOperator(
    BufferPool& pool, File& heap_file, const Schema& schema,
    std::vector<BoundComparisonPredicate> predicates)
    : pool_(pool),
      heap_file_(heap_file),
      schema_(schema),
      predicates_(std::move(predicates)) {}

void SeqScanOperator::open() {
  current_page_id_ = 0;
  current_slot_id_ = 0;
  is_open_ = true;
  logger_.open();
}

std::optional<TypedRow> SeqScanOperator::next() {
  if (!is_open_) {
    return std::nullopt;
  }

  while (current_page_id_ <= heap_file_.getMaxPageID()) {
    Page* page = pool_.pinPage(current_page_id_, heap_file_);
    while (current_slot_id_ < page->slotCount()) {
      const uint16_t slot_id = current_slot_id_++;
      char* cell_start = page->slotCellStartUnchecked(slot_id);
      if (!Cell::isValid(cell_start)) {
        continue;
      }

      logger_.recordInput();
      TypedRow row = RecordCellView(cell_start).getTypedRow(schema_);
      pool_.unpinPage(page, heap_file_);
      // Apply filtering: skip row if predicates don't match
      if (!passesPredicates(row, predicates_)) {
        continue;
      }
      logger_.recordOutput();
      return row;
    }

    pool_.unpinPage(page, heap_file_);
    ++current_page_id_;
    current_slot_id_ = 0;
  }

  return std::nullopt;
}

void SeqScanOperator::close() {
  is_open_ = false;
  logger_.close();
}