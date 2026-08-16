#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <exception>
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

class HeapScan {
 public:
  HeapScan(const HeapScan&) = delete;
  HeapScan& operator=(const HeapScan&) = delete;

  HeapScan(HeapScan&& other) noexcept
      : pool_(other.pool_),
        file_(other.file_),
        schema_(other.schema_),
        current_page_(std::exchange(other.current_page_, nullptr)),
        current_page_id_(other.current_page_id_),
        current_slot_id_(other.current_slot_id_) {}

  HeapScan& operator=(HeapScan&&) = delete;

  ~HeapScan() noexcept {
    try {
      close();
    } catch (...) {
      std::terminate();
    }
  }

  void close() {
    if (current_page_ == nullptr) {
      return;
    }
    pool_.unpinPage(current_page_, file_);
    current_page_ = nullptr;
  }

  std::optional<std::pair<RID, TypedRow>> next() {
    const uint16_t last_page_id = file_.getMaxPageID();
    while (current_page_id_ <= last_page_id) {
      if (current_page_ == nullptr) {
        current_page_ =
            pool_.pinPage(static_cast<int>(current_page_id_), file_);

        current_slot_id_ = 0;
      }

      while (current_slot_id_ < current_page_->slotCount()) {
        const uint16_t slot_id = current_slot_id_++;
        const char* cell_start = current_page_->slotCellStartUnchecked(slot_id);
        if (!Cell::isValid(cell_start)) {
          continue;
        }
        return std::pair{
            RID{
                static_cast<uint16_t>(current_page_id_),
                slot_id,
            },
            RecordCellView(cell_start).getTypedRow(schema_),
        };
      }

      close();
      ++current_page_id_;
    }

    return std::nullopt;
  };

 private:
  friend class HeapFile;

  HeapScan(BufferPool& pool, PagedFile& file, const Schema& schema) : pool_(pool),
      file_(file),
      schema_(schema),
      current_page_(nullptr) {}

  BufferPool& pool_;
  PagedFile& file_;
  const Schema& schema_;
  Page* current_page_;
  uint32_t current_page_id_ = 0;
  uint16_t current_slot_id_ = 0;

};
