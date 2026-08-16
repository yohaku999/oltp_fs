#include "seq_scan_operator.h"

#include "schema/schema.h"
#include "storage/buffer/bufferpool.h"
#include "execution/heapfile.h"
#include "storage/page/cell.h"
#include "storage/page/page.h"
#include "storage/record/record_cell.h"
SeqScanOperator::SeqScanOperator(
    BufferPool& pool, HeapFile& heap_file, const Schema& schema,
    std::vector<BoundComparisonPredicate> predicates)
    : predicates_(std::move(predicates)),
      scan_(heap_file.scan(pool, schema)) {}

void SeqScanOperator::open() {
  is_open_ = true;
  logger_.open();
}

std::optional<TypedRow> SeqScanOperator::next() {
  if (!is_open_) {
    return std::nullopt;
  }

  while (auto item = scan_.next()) {
    logger_.recordInput();
    if (!passesPredicates(item->second, predicates_)) {
      continue;
    }

    logger_.recordOutput();
    return std::move(item->second);
  }

  return std::nullopt;
}

void SeqScanOperator::close() {
  is_open_ = false;
  scan_.close();
  logger_.close();
}
