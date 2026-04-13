#include "catalog/table.h"

#include <gtest/gtest.h>

#include <array>
#include <cstdio>
#include <fstream>
#include <memory>

#include <nlohmann/json.hpp>

#include "execution/executor.h"
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
        kTableName,
        Schema(std::vector<Column>{Column("id", Column::Type::Integer),
                                   Column("value", Column::Type::Varchar)}),
        std::string("id"));
  }

  static std::string singleVarcharValue(const TypedRow& row) {
    if (row.values.size() != 2 ||
        !std::holds_alternative<Column::VarcharType>(row.values[1])) {
      throw std::runtime_error("Expected row with integer key and varchar value.");
    }
    return std::get<Column::VarcharType>(row.values[1]);
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

  Schema schema(std::vector<Column>{Column("id", Column::Type::Integer),
                                    Column("value", Column::Type::Varchar)});
  Table table = Table::initialize(kTableName, schema, std::string("id"));

  EXPECT_TRUE(Table::isPersisted(kTableName));
  EXPECT_EQ(table.indexFile().getRootPageID(), 0u);
  EXPECT_TRUE(table.indexFile().isPageIDUsed(0));
  EXPECT_TRUE(table.heapFile().isPageIDUsed(0));

  std::ifstream meta_file("data/table_test_table.meta.json");
  ASSERT_TRUE(meta_file.is_open());
  nlohmann::json metadata;
  meta_file >> metadata;
  ASSERT_TRUE(metadata.contains("indexes"));
  ASSERT_TRUE(metadata["indexes"].is_array());
  ASSERT_EQ(metadata["indexes"].size(), 1u);
  EXPECT_EQ(metadata["indexes"][0]["indexFile"].get<std::string>(),
            "data/table_test_table.id.index");
  EXPECT_EQ(metadata["indexes"][0]["indexedColumn"].get<std::string>(),
            "id");

  Table reopened = Table::getTable(kTableName);
  EXPECT_EQ(reopened.name(), kTableName);
  ASSERT_EQ(reopened.schema().columns().size(), 2u);
  EXPECT_EQ(reopened.schema().columns()[0].getName(), "id");
  EXPECT_EQ(reopened.schema().columns()[0].getType(), Column::Type::Integer);
  EXPECT_EQ(reopened.schema().columns()[1].getName(), "value");
  EXPECT_EQ(reopened.schema().columns()[1].getType(), Column::Type::Varchar);
  ASSERT_TRUE(reopened.indexedColumnName().has_value());
  EXPECT_EQ(reopened.indexedColumnName().value(), "id");

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
  TypedRow row{{Column::IntegerType(11), Column::VarcharType("table-value")}};

  executor::insert(*pool_, table, row, *wal_);
  TypedRow restored = executor::read(*pool_, table, 11);
  EXPECT_EQ(singleVarcharValue(restored), "table-value");
}

TEST_F(TableTest, InsertHeapRecordWithWalWritesInsertRecord) {
  Table table = createSingleColumnTable();
  TypedRow row{{Column::IntegerType(11), Column::VarcharType("wal")}};

  executor::insert(*pool_, table, row, *wal_);
  wal_->flush();

  std::vector<WALRecord> records = readWalRecords(kWalPath);
  ASSERT_EQ(records.size(), 1u);
  EXPECT_EQ(records[0].get_type(), WALRecord::RecordType::INSERT);

  WALBody body = decode_body(records[0]);
  ASSERT_TRUE(std::holds_alternative<InsertRedoBody>(body));
  const auto& insert_body = std::get<InsertRedoBody>(body);
  EXPECT_FALSE(insert_body.tuple.empty());
}

TEST_F(TableTest, InvalidateHeapRecordWithWalWritesDeleteRecord) {
  Table table = createSingleColumnTable();
  TypedRow row{{Column::IntegerType(22), Column::VarcharType("gone")}};

  executor::insert(*pool_, table, row, *wal_);
  executor::remove(*pool_, table, 22, *wal_);
  wal_->flush();

  std::vector<WALRecord> records = readWalRecords(kWalPath);
  ASSERT_EQ(records.size(), 2u);
  EXPECT_EQ(records[0].get_type(), WALRecord::RecordType::INSERT);
  EXPECT_EQ(records[1].get_type(), WALRecord::RecordType::DELETE);

  WALBody body = decode_body(records[1]);
  ASSERT_TRUE(std::holds_alternative<DeleteRedoBody>(body));
  const auto& delete_body = std::get<DeleteRedoBody>(body);
  EXPECT_EQ(delete_body.offset, 0);

  EXPECT_THROW({ executor::read(*pool_, table, 22); }, std::runtime_error);
}