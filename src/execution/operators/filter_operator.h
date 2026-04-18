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
                 std::vector<ComparisonPredicate> predicates)
      : child_(std::move(child)),
        predicates_(std::move(predicates)) {}

  void open() override { child_->open(); }

  std::optional<TypedRow> next() override {
    while (std::optional<TypedRow> row = child_->next()) {
      bool matches = true;
      for (const auto& predicate : predicates_) {
        const auto& value = row->values[predicate.column_index];
        switch (predicate.op) {
          case ComparisonPredicate::Op::Eq:
            if (value != predicate.value) {
              matches = false;
            }
            break;
          case ComparisonPredicate::Op::Gt:
            if (value <= predicate.value) {
              matches = false;
            }
            break;
          case ComparisonPredicate::Op::Ge:
            if (value < predicate.value) {
              matches = false;
            }
            break;
          case ComparisonPredicate::Op::Lt:
            if (value >= predicate.value) {
              matches = false;
            }
            break;
          case ComparisonPredicate::Op::Le:
            if (value > predicate.value) {
              matches = false;
            }
            break;
        }

        if (!matches) {
          break;
        }
      }

      if (matches) {
        return row;
      }
    }

    return std::nullopt;
  }

  void close() override { child_->close(); }

 private:
  std::unique_ptr<Operator> child_;
  std::vector<ComparisonPredicate> predicates_;
};