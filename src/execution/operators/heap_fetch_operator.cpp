#include "heap_fetch_operator.h"

#include "execution/comparison_predicate.h"
#include "execution/heapfile.h"
#include "schema/schema.h"
#include "storage/buffer/bufferpool.h"
#include "storage/index/rid.h"
#include "storage/record/record_cell.h"

/**
 * HeapFetchOperator returns TypedRow given RID from its child operator.
 * It searches the heap file for the corresponding record cell reflecting
 * predicate and decodes it into TypedRow.
 */
HeapFetchOperator::HeapFetchOperator(
    std::unique_ptr<RidOperator> child, BufferPool& pool, HeapFile& heap_file,
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
    std::optional<TypedRow> row = heap_file_.withCell(
        pool_, *rid,
        [&](RecordCellView cell) { return cell.getTypedRow(schema_); });
    if (!row.has_value()) {
      continue;
    }

    // Apply filtering: skip row if predicates don't match
    if (!passesPredicates(*row, predicates_)) {
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
