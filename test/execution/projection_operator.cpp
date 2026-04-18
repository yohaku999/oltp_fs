#include "execution/operators/projection_operator.h"
#include "stub_row_operator.h"

#include <gtest/gtest.h>

#include <optional>
#include <vector>

TEST(ProjectionOperatorTest, SelectsSpecificColumnsByIndex) {
  std::vector<TypedRow> rows;
  rows.push_back(makeStubRow(1, "alice", 10));
  rows.push_back(makeStubRow(2, "bob", 20));

  auto child = std::make_unique<StubRowOperator>(std::move(rows));
  ProjectionOperator projection(std::move(child), {1, 2});

  projection.open();

  std::optional<TypedRow> first = projection.next();
  ASSERT_TRUE(first.has_value());
  ASSERT_EQ(first->values.size(), 2u);
  EXPECT_EQ(std::get<Column::VarcharType>(first->values[0]), "alice");
  EXPECT_EQ(std::get<Column::IntegerType>(first->values[1]), 10);

  std::optional<TypedRow> second = projection.next();
  ASSERT_TRUE(second.has_value());
  ASSERT_EQ(second->values.size(), 2u);
  EXPECT_EQ(std::get<Column::VarcharType>(second->values[0]), "bob");
  EXPECT_EQ(std::get<Column::IntegerType>(second->values[1]), 20);

  EXPECT_FALSE(projection.next().has_value());
  projection.close();
}

TEST(ProjectionOperatorTest, StarCaseKeepsAllColumns) {
  std::vector<TypedRow> rows;
  rows.push_back(makeStubRow(7, "carol", 70));

  auto child = std::make_unique<StubRowOperator>(std::move(rows));
  ProjectionOperator projection(std::move(child), {0, 1, 2});

  projection.open();

  std::optional<TypedRow> row = projection.next();
  ASSERT_TRUE(row.has_value());
  ASSERT_EQ(row->values.size(), 3u);
  EXPECT_EQ(std::get<Column::IntegerType>(row->values[0]), 7);
  EXPECT_EQ(std::get<Column::VarcharType>(row->values[1]), "carol");
  EXPECT_EQ(std::get<Column::IntegerType>(row->values[2]), 70);

  EXPECT_FALSE(projection.next().has_value());
  projection.close();
}