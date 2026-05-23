#include "heap_fetch_operator.h"

#include "execution/comparison_predicate.h"
#include "schema/schema.h"
#include "storage/index/rid.h"
#include "storage/page/page.h"
#include "storage/record/record_cell.h"
#include "execution/comparison_predicate.h"
#include "storage/runtime/bufferpool.h"
#include "storage/runtime/file.h"

/**
 * HeapFetchOperator returns TypedRow given RID from its child operator.
 * It searches the heap file for the corresponding record cell reflecting predicate and decodes it into TypedRow.
 */
HeapFetchOperator::HeapFetchOperator(
    std::unique_ptr<RidOperator> child, BufferPool& pool, File& heap_file,
    const Schema& schema, std::vector<BoundComparisonPredicate> predicates)
    : child_(std::move(child)),
      pool_(pool),
      heap_file_(heap_file),
      schema_(schema),
      predicates_(std::move(predicates)) {}

void HeapFetchOperator::open() {
  logger_.open();
  child_->open();
}

std::optional<TypedRow> HeapFetchOperator::next() {
  while (true) {
    std::optional<RID> rid = child_->next();
    if (!rid.has_value()) {
      return std::nullopt;
    }

    logger_.recordInput();
    Page* page = pool_.pinPage(rid->heap_page_id, heap_file_);
    TypedRow row =
        RecordCellView(page->getSlotCellStart(rid->slot_id))
            .getTypedRow(schema_);
    pool_.unpinPage(page, heap_file_);

    // Apply filtering: skip row if predicates don't match
    if (!matchesPredicates(row, predicates_)) {
      continue;
    }

    logger_.recordOutput();
    return row;
  }
}

void HeapFetchOperator::close() {
  child_->close();
  logger_.close();
}