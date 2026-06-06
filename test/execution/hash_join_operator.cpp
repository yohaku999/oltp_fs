#include "execution/operators/hash_join_operator.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "stub_row_operator.h"

namespace {

TypedRow makeRow(std::initializer_list<FieldValue> values) {
  return TypedRow{std::vector<FieldValue>(values)};
}

Column::IntegerType integerValue(const FieldValue& value) {
  return std::get<Column::IntegerType>(value);
}

std::string stringValue(const FieldValue& value) {
  return std::get<Column::VarcharType>(value);
}

}  // namespace

TEST(HashJoinOperatorTest, JoinsRowsUsingConfiguredColumnIndexes) {
  std::vector<TypedRow> outer_rows;
  outer_rows.push_back(
      makeRow({Column::IntegerType(1), Column::VarcharType("dept-a")}));
  outer_rows.push_back(
      makeRow({Column::IntegerType(2), Column::VarcharType("dept-b")}));

  std::vector<TypedRow> inner_rows;
  inner_rows.push_back(
      makeRow({Column::VarcharType("dept-b"), Column::VarcharType("bob")}));
  inner_rows.push_back(
      makeRow({Column::VarcharType("dept-a"), Column::VarcharType("alice")}));
  inner_rows.push_back(
      makeRow({Column::VarcharType("dept-a"), Column::VarcharType("carol")}));

  HashJoinOperator join(
      std::make_unique<StubRowOperator>(std::move(outer_rows)),
      std::make_unique<StubRowOperator>(std::move(inner_rows)),
      HashJoinKey{1, 0});

  join.open();

  std::vector<std::pair<Column::IntegerType, std::string>> joined_rows;
  for (std::optional<TypedRow> row = join.next(); row.has_value();
       row = join.next()) {
    ASSERT_TRUE(std::holds_alternative<Column::IntegerType>(row->values[0]));
    ASSERT_TRUE(std::holds_alternative<Column::VarcharType>(row->values[1]));
    ASSERT_TRUE(std::holds_alternative<Column::VarcharType>(row->values[2]));
    ASSERT_TRUE(std::holds_alternative<Column::VarcharType>(row->values[3]));
    EXPECT_EQ(stringValue(row->values[1]), stringValue(row->values[2]));
    joined_rows.emplace_back(integerValue(row->values[0]),
                             stringValue(row->values[3]));
  }

  std::sort(joined_rows.begin(), joined_rows.end());
  ASSERT_EQ(joined_rows.size(), 3U);
  EXPECT_EQ(joined_rows[0],
            std::make_pair(Column::IntegerType(1), std::string("alice")));
  EXPECT_EQ(joined_rows[1],
            std::make_pair(Column::IntegerType(1), std::string("carol")));
  EXPECT_EQ(joined_rows[2],
            std::make_pair(Column::IntegerType(2), std::string("bob")));

  EXPECT_FALSE(join.next().has_value());
  join.close();
}
