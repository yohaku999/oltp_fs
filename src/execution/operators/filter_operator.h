#pragma once

#include <cstddef>
#include <memory>
#include <optional>
#include <stdexcept>
#include <utility>
#include <vector>

#include "execution/comparison_predicate.h"
#include "execution/operator.h"

class FilterOperator : public Operator {
 public:
  FilterOperator(std::unique_ptr<Operator> child,
                 std::vector<BoundComparisonPredicate> predicates)
      : child_(std::move(child)),
        predicates_(std::move(predicates)) {}

  void open() override { child_->open(); }

  std::optional<TypedRow> next() override {
    while (std::optional<TypedRow> row = child_->next()) {
      if (matchesPredicates(row.value(), predicates_)) {
        return row;
      }
    }

    return std::nullopt;
  }

  void close() override { child_->close(); }

 private:
  std::unique_ptr<Operator> child_;
  std::vector<BoundComparisonPredicate> predicates_;
};