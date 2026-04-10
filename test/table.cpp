#include "../src/table/table.h"

#include <gtest/gtest.h>

#include <fstream>

class TableTest : public ::testing::Test {
 protected:
  static constexpr const char* kTableName = "table_test_table";

  void SetUp() override { Table::removeFilesFor(kTableName); }

  void TearDown() override { Table::removeFilesFor(kTableName); }
};

TEST_F(TableTest, InitializeAndGetTableRoundTripSchema) {
  EXPECT_FALSE(Table::exists(kTableName));

  Schema schema(kTableName,
                std::vector<Column>{Column("value", Column::Type::Varchar)});
  Table table = Table::initialize(kTableName, schema);

  EXPECT_TRUE(Table::exists(kTableName));

  Table reopened = Table::getTable(kTableName);
  EXPECT_EQ(reopened.name(), kTableName);
  ASSERT_EQ(reopened.schema().columns().size(), 1u);
  EXPECT_EQ(reopened.schema().columns()[0].getName(), "value");
  EXPECT_EQ(reopened.schema().columns()[0].getType(), Column::Type::Varchar);
}