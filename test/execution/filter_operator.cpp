#include "execution/filter_operator.h"

#include <gtest/gtest.h>

#include <optional>
#include <vector>

#include "stub_row_operator.h"

TEST(FilterOperatorTest, ReturnsOnlyRowsMatchingEqualityPredicate) {
  std::vector<TypedRow> rows;
  rows.push_back(makeStubRow(1, "alice", 10));
  rows.push_back(makeStubRow(2, "bob", 20));
  rows.push_back(makeStubRow(3, "alice", 30));

  auto child = std::make_unique<StubRowOperator>(std::move(rows));
  FilterOperator filter(
      std::move(child),
      std::vector<ComparisonPredicate>{{1, ComparisonPredicate::Op::Eq,
                                        Column::VarcharType("alice")}});

  filter.open();

  std::optional<TypedRow> first = filter.next();
  ASSERT_TRUE(first.has_value());
  EXPECT_EQ(std::get<Column::IntegerType>(first->values[0]), 1);

  std::optional<TypedRow> second = filter.next();
  ASSERT_TRUE(second.has_value());
  EXPECT_EQ(std::get<Column::IntegerType>(second->values[0]), 3);

  EXPECT_FALSE(filter.next().has_value());
  filter.close();
}

TEST(FilterOperatorTest, SkipsRowsUntilRangePredicateMatches) {
  std::vector<TypedRow> rows;
  rows.push_back(makeStubRow(1, "alice", 10));
  rows.push_back(makeStubRow(2, "bob", 20));
  rows.push_back(makeStubRow(3, "carol", 30));

  auto child = std::make_unique<StubRowOperator>(std::move(rows));
  FilterOperator filter(
      std::move(child),
      std::vector<ComparisonPredicate>{{2, ComparisonPredicate::Op::Ge,
                                        Column::IntegerType(20)}});

  filter.open();

  std::optional<TypedRow> first = filter.next();
  ASSERT_TRUE(first.has_value());
  EXPECT_EQ(std::get<Column::IntegerType>(first->values[0]), 2);

  std::optional<TypedRow> second = filter.next();
  ASSERT_TRUE(second.has_value());
  EXPECT_EQ(std::get<Column::IntegerType>(second->values[0]), 3);

  EXPECT_FALSE(filter.next().has_value());
  filter.close();
}

TEST(FilterOperatorTest, AppliesAllPredicatesAsConjunction) {
  std::vector<TypedRow> rows;
  rows.push_back(makeStubRow(1, "alice", 10));
  rows.push_back(makeStubRow(2, "alice", 20));
  rows.push_back(makeStubRow(3, "bob", 20));

  auto child = std::make_unique<StubRowOperator>(std::move(rows));
  FilterOperator filter(
      std::move(child),
      std::vector<ComparisonPredicate>{{1, ComparisonPredicate::Op::Eq,
                                        Column::VarcharType("alice")},
                                       {2, ComparisonPredicate::Op::Eq,
                                        Column::IntegerType(20)}});

  filter.open();

  std::optional<TypedRow> row = filter.next();
  ASSERT_TRUE(row.has_value());
  EXPECT_EQ(std::get<Column::IntegerType>(row->values[0]), 2);

  EXPECT_FALSE(filter.next().has_value());
  filter.close();
}