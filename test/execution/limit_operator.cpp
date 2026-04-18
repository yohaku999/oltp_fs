#include "execution/operators/limit_operator.h"

#include <gtest/gtest.h>

#include <optional>
#include <vector>

#include "stub_row_operator.h"

TEST(LimitOperatorTest, StopsAfterConfiguredRowCount) {
  std::vector<TypedRow> rows;
  rows.push_back(makeStubRow(1, "alice", 10));
  rows.push_back(makeStubRow(2, "bob", 20));
  rows.push_back(makeStubRow(3, "carol", 30));

  auto child = std::make_unique<StubRowOperator>(std::move(rows));
  LimitOperator limit(std::move(child), 2);

  limit.open();

  std::optional<TypedRow> first = limit.next();
  ASSERT_TRUE(first.has_value());
  EXPECT_EQ(std::get<Column::IntegerType>(first->values[0]), 1);

  std::optional<TypedRow> second = limit.next();
  ASSERT_TRUE(second.has_value());
  EXPECT_EQ(std::get<Column::IntegerType>(second->values[0]), 2);

  EXPECT_FALSE(limit.next().has_value());
  limit.close();
}

TEST(LimitOperatorTest, ReturnsNoRowsForZeroLimit) {
  std::vector<TypedRow> rows;
  rows.push_back(makeStubRow(1, "alice", 10));

  auto child = std::make_unique<StubRowOperator>(std::move(rows));
  LimitOperator limit(std::move(child), 0);

  limit.open();
  EXPECT_FALSE(limit.next().has_value());
  limit.close();
}