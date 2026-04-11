#include "catalog/table.h"

#include <gtest/gtest.h>

#include <array>
#include <fstream>

#include "storage/page/page.h"

class TableTest : public ::testing::Test {
 protected:
  static constexpr const char* kTableName = "table_test_table";

  void SetUp() override { Table::removeBackingFilesFor(kTableName); }

  void TearDown() override { Table::removeBackingFilesFor(kTableName); }
};

TEST_F(TableTest, InitializeCreatesReadableTableBootstrap) {
  EXPECT_FALSE(Table::isPersisted(kTableName));

  Schema schema(kTableName,
                std::vector<Column>{Column("value", Column::Type::Varchar)});
  Table table = Table::initialize(kTableName, schema);

  EXPECT_TRUE(Table::isPersisted(kTableName));
  EXPECT_EQ(table.indexFile().getRootPageID(), 0u);
  EXPECT_TRUE(table.indexFile().isPageIDUsed(0));
  EXPECT_TRUE(table.heapFile().isPageIDUsed(0));

  Table reopened = Table::getTable(kTableName);
  EXPECT_EQ(reopened.name(), kTableName);
  ASSERT_EQ(reopened.schema().columns().size(), 1u);
  EXPECT_EQ(reopened.schema().columns()[0].getName(), "value");
  EXPECT_EQ(reopened.schema().columns()[0].getType(), Column::Type::Varchar);

  std::array<char, Page::PAGE_SIZE_BYTE> index_page_buffer{};
  std::array<char, Page::PAGE_SIZE_BYTE> heap_page_buffer{};

  EXPECT_NO_THROW(
      table.indexFile().readPageIntoBuffer(0, index_page_buffer.data()));
  EXPECT_NO_THROW(
      table.heapFile().readPageIntoBuffer(0, heap_page_buffer.data()));

  Page index_root = Page::wrapExisting(index_page_buffer.data(), 0);
  EXPECT_TRUE(index_root.isLeaf());
  EXPECT_EQ(index_root.getPageLSN(), 0u);
}