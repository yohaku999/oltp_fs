#include "catalog/table.h"

#include <gtest/gtest.h>

#include <array>
#include <cstdio>
#include <fstream>
#include <memory>

#include "storage/runtime/bufferpool.h"
#include "storage/page/page.h"
#include "storage/wal/wal.h"
#include "storage/wal/wal_record.h"

class TableTest : public ::testing::Test {
 protected:
  static constexpr const char* kTableName = "table_test_table";
  static constexpr const char* kWalPath = "table_test_table.wal";
  std::unique_ptr<BufferPool> pool_;
  std::unique_ptr<WAL> wal_;

  void SetUp() override {
    Table::removeBackingFilesFor(kTableName);
    std::remove(kWalPath);
    wal_ = WAL::initializeNew(kWalPath);
    pool_ = std::make_unique<BufferPool>(*wal_);
  }

  void TearDown() override {
    pool_.reset();
    wal_.reset();
    Table::removeBackingFilesFor(kTableName);
    std::remove(kWalPath);
  }

  static Table createSingleColumnTable() {
    return Table::initialize(
        kTableName, Schema(kTableName, std::vector<Column>{Column(
                                           "value", Column::Type::Varchar)}));
  }

  static std::string singleVarcharValue(const TypedRow& row) {
    if (row.values.size() != 1 ||
        !std::holds_alternative<Column::VarcharType>(row.values[0])) {
      throw std::runtime_error("Expected single varchar row.");
    }
    return std::get<Column::VarcharType>(row.values[0]);
  }

  static std::vector<WALRecord> readWalRecords(const std::string& wal_path) {
    std::ifstream wal_file(wal_path, std::ios::binary);
    if (!wal_file) {
      return {};
    }

    std::vector<char> raw_bytes((std::istreambuf_iterator<char>(wal_file)),
                                std::istreambuf_iterator<char>());
    std::vector<WALRecord> records;
    std::size_t offset = 0;

    while (offset < raw_bytes.size()) {
      const std::size_t header_size = sizeof(uint64_t) +
                                      sizeof(WALRecord::RecordType) +
                                      sizeof(uint16_t) + sizeof(uint32_t);
      if (raw_bytes.size() - offset < header_size) {
        throw std::runtime_error("Incomplete WAL header in test fixture.");
      }

      uint32_t body_size = 0;
      std::memcpy(&body_size,
                  raw_bytes.data() + offset + sizeof(uint64_t) +
                      sizeof(WALRecord::RecordType) + sizeof(uint16_t),
                  sizeof(uint32_t));

      const std::size_t record_size = header_size + body_size;
      if (raw_bytes.size() - offset < record_size) {
        throw std::runtime_error("Incomplete WAL record body in test fixture.");
      }

      std::vector<std::byte> record_bytes(record_size);
      std::memcpy(record_bytes.data(), raw_bytes.data() + offset, record_size);
      records.push_back(WALRecord::deserialize(record_bytes));
      offset += record_size;
    }

    return records;
  }
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

TEST_F(TableTest, InsertIndexFindRIDAndReadRowRoundTrip) {
  Table table = createSingleColumnTable();

  RID inserted = table.insertHeapRecord(
      *pool_, 11, TypedRow{{Column::VarcharType("table-value")}}, *wal_);
  table.insertIndexEntry(*pool_, 11, inserted);

  std::optional<RID> found = table.findRID(*pool_, 11);
  ASSERT_TRUE(found.has_value());
  EXPECT_EQ(found->heap_page_id, inserted.heap_page_id);
  EXPECT_EQ(found->slot_id, inserted.slot_id);

  TypedRow restored = table.readRow(*pool_, inserted);
  EXPECT_EQ(singleVarcharValue(restored), "table-value");
}

TEST_F(TableTest, InsertHeapRecordWithWalWritesInsertRecord) {
  Table table = createSingleColumnTable();

  RID inserted = table.insertHeapRecord(
      *pool_, 11, TypedRow{{Column::VarcharType("wal")}}, *wal_);
  table.insertIndexEntry(*pool_, 11, inserted);
  wal_->flush();

  std::vector<WALRecord> records = readWalRecords(kWalPath);
  ASSERT_EQ(records.size(), 1u);
  EXPECT_EQ(records[0].get_type(), WALRecord::RecordType::INSERT);

  WALBody body = decode_body(records[0]);
  ASSERT_TRUE(std::holds_alternative<InsertRedoBody>(body));
  const auto& insert_body = std::get<InsertRedoBody>(body);
  EXPECT_EQ(insert_body.offset, inserted.slot_id);
  EXPECT_FALSE(insert_body.tuple.empty());
}

TEST_F(TableTest, InvalidateHeapRecordWithWalWritesDeleteRecord) {
  Table table = createSingleColumnTable();
  RID inserted = table.insertHeapRecord(
      *pool_, 22, TypedRow{{Column::VarcharType("gone")}}, *wal_);
  table.insertIndexEntry(*pool_, 22, inserted);

  table.invalidateHeapRecord(*pool_, inserted, *wal_);
  wal_->flush();

  std::vector<WALRecord> records = readWalRecords(kWalPath);
  ASSERT_EQ(records.size(), 2u);
  EXPECT_EQ(records[0].get_type(), WALRecord::RecordType::INSERT);
  EXPECT_EQ(records[1].get_type(), WALRecord::RecordType::DELETE);

  WALBody body = decode_body(records[1]);
  ASSERT_TRUE(std::holds_alternative<DeleteRedoBody>(body));
  const auto& delete_body = std::get<DeleteRedoBody>(body);
  EXPECT_EQ(delete_body.offset, inserted.slot_id);

  EXPECT_THROW({ table.readRow(*pool_, inserted); }, std::runtime_error);
}