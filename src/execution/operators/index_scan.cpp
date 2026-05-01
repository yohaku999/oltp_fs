#include "index_scan.h"

#include <limits>
#include <stdexcept>

#include "storage/index/btreecursor.h"
#include "logging.h"

namespace {

int exclusiveLowerToInclusive(int value) {
  if (value == std::numeric_limits<int>::max()) {
    return value;
  }
  return value + 1;
}

int exclusiveUpperToInclusive(int value) {
  if (value == std::numeric_limits<int>::min()) {
    return value;
  }
  return value - 1;
}

}  // namespace

IndexScanOperator::IndexScanOperator(BufferPool& pool, File& index_file,
                                     DiscreteIntegerIndexPredicates
                                         discrete_integer_predicates)
    : pool_(pool),
      indexFile_(index_file),
      pos_(0),
      mode_(Mode::Keys) {
  configureScanRangeFromPredicates(discrete_integer_predicates.predicates);
}

void IndexScanOperator::configureScanRangeFromPredicates(
    const std::vector<UnboundComparisonPredicate>& predicates) {
  if (predicates.empty()) {
    throw std::runtime_error("Index scan requires one or two predicates.");
  }

  int low_key = std::numeric_limits<int>::min();
  int high_key = std::numeric_limits<int>::max();

  if (predicates.size() == 1) {
    const auto& predicate = predicates[0];
    const auto& fieldvalue = std::holds_alternative<FieldValue>(predicate.left) ? std::get<FieldValue>(predicate.left) : std::get<FieldValue>(predicate.right);
    const int value = std::get<Column::IntegerType>(fieldvalue);
    switch (predicate.op) {
      case Op::Eq:
        keys_.push_back(value);
        mode_ = Mode::Keys;
        return;
      case Op::Gt:
        low_key = exclusiveLowerToInclusive(value);
        break;
      case Op::Ge:
        low_key = value;
        break;
      case Op::Lt:
        high_key = exclusiveUpperToInclusive(value);
        break;
      case Op::Le:
        high_key = value;
        break;
    }

    setDiscreteIntegerRange(low_key, high_key);
    return;
  }

  if (predicates.size() != 2) {
    throw std::logic_error(
        "Index scan requires one predicate or exactly two range predicates. Should be merged beforehand.");
  }

  for (const auto& predicate : predicates) {
    const auto& fieldvalue = std::holds_alternative<FieldValue>(predicate.left) ? std::get<FieldValue>(predicate.left) : std::get<FieldValue>(predicate.right);
    const int value = std::get<Column::IntegerType>(fieldvalue);
    switch (predicate.op) {
      case Op::Eq:
        throw std::logic_error("Equality predicates must be merged before index scan initialization.");
      case Op::Gt:
        low_key = exclusiveLowerToInclusive(value);
        break;
      case Op::Ge:
        low_key = value;
        break;
      case Op::Lt:
        high_key = exclusiveUpperToInclusive(value);
        break;
      case Op::Le:
        high_key = value;
        break;
    }
  }

  setDiscreteIntegerRange(low_key, high_key);
}

void IndexScanOperator::setDiscreteIntegerRange(int low_key, int high_key) {
  LOG_INFO("Starting discrete integer index scan for keys in range [{}, {}]",
           low_key, high_key);
  mode_ = Mode::Range;

  if (low_key <= high_key) {
    low_key_ = low_key;
    high_key_ = high_key;
    current_key_ = low_key;
  } else {
    low_key_ = low_key;
    high_key_ = high_key;
    current_key_ = high_key + 1;
  }
  LOG_INFO("Finished discrete integer index scan for keys in range [{}, {}]",
           low_key, high_key);
}

void IndexScanOperator::open() {
  pos_ = 0;
  if (mode_ == Mode::Range) {
    current_key_ = low_key_;
    if (low_key_ > high_key_) {
      current_key_ = high_key_ + 1;
    }
  }
}

std::optional<RID> IndexScanOperator::next() {
  if (mode_ == Mode::Keys) {
    while (pos_ < keys_.size()) {
      int key = keys_[pos_++];
      std::optional<RID> rid = BTreeCursor::findRID(pool_, indexFile_, key);
      if (!rid.has_value()) {
        continue;
      }

      return rid;
    }
  } else {
    while (current_key_ <= high_key_) {
      int key = current_key_++;
      std::optional<RID> rid = BTreeCursor::findRID(pool_, indexFile_, key);
      if (!rid.has_value()) {
        continue;
      }

      return rid;
    }
  }

  return std::nullopt;
}

void IndexScanOperator::close() {}