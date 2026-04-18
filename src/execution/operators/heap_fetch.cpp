#include "heap_fetch.h"

#include "execution/operators/rid_operator.h"
#include "schema/schema.h"
#include "storage/record/record_cell.h"
#include "storage/index/rid.h"
#include "storage/runtime/bufferpool.h"
#include "storage/runtime/file.h"
#include "storage/page/page.h"

HeapFetchOperator::HeapFetchOperator(std::unique_ptr<RidOperator> child,
                                     BufferPool& pool, File& heap_file,
                                     const Schema& schema)
    : child_(std::move(child)),
      pool_(pool),
      heap_file_(heap_file),
      schema_(schema) {}

void HeapFetchOperator::open() { child_->open(); }

std::optional<TypedRow> HeapFetchOperator::next() {
  std::optional<RID> rid = child_->next();
  if (!rid.has_value()) {
    return std::nullopt;
  }

  Page* page = pool_.pinPage(rid->heap_page_id, heap_file_);
  TypedRow row =
      RecordCellView(page->getSlotCellStart(rid->slot_id)).getTypedRow(schema_);
  pool_.unpinPage(page, heap_file_);
  return row;
}

void HeapFetchOperator::close() { child_->close(); }