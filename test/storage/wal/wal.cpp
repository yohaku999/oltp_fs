#include "storage/wal/wal.h"

#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <fstream>
#include <thread>
#include <vector>

#include "storage/wal/wal_record.h"

class WALTest : public ::testing::Test {
 protected:
  const char* wal_path = "testwal.log";

  void SetUp() override { std::remove(wal_path); }

  void TearDown() override { std::remove(wal_path); }

  std::vector<WALRecord> readWalRecords() const {
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

TEST_F(WALTest, FlushedLSNFollowsLastRecordLSN) {
  auto wal = WAL::initializeNew(wal_path);

  std::vector<std::byte> body1 = {std::byte{0x01}, std::byte{0x02}};
  std::vector<std::byte> body2 = {std::byte{0x10}, std::byte{0x20},
                                  std::byte{0x30}};

  wal->write(WALRecord::RecordType::INSERT, 1, body1);
  wal->write(WALRecord::RecordType::DELETE, 2, body2);

  // Flush so getFlushedLSN() reflects the written records.
  wal->flush();

  auto flushed = wal->getFlushedLSN();
  EXPECT_EQ(flushed, WALRecord::size_bytes(body1));
}

TEST_F(WALTest, InitializeNewFailsIfWalAlreadyExists) {
  auto wal = WAL::initializeNew(wal_path);
  wal.reset();

  EXPECT_THROW({
    auto reopened = WAL::initializeNew(wal_path);
  }, std::system_error);
}

TEST_F(WALTest, OpenExistingFailsIfWalDoesNotExist) {
  EXPECT_THROW({
    auto wal = WAL::openExisting(wal_path);
  }, std::system_error);
}

TEST_F(WALTest, WriteOverloadAllocatesSequentialLSNsFromRecordSizes) {
  auto wal = WAL::initializeNew(wal_path);

  std::vector<std::vector<std::byte>> bodies = {
      {std::byte{0x01}, std::byte{0x02}},
      {std::byte{0x10}, std::byte{0x20}, std::byte{0x30}},
      {std::byte{0xAA}},
  };

  for (const auto& body : bodies) {
    wal->write(WALRecord::RecordType::INSERT, 1, body);
  }
  wal->flush();

  std::vector<WALRecord> records = readWalRecords();
  ASSERT_EQ(records.size(), bodies.size());

  std::uint64_t expected_lsn = 0;
  for (std::size_t i = 0; i < records.size(); ++i) {
    EXPECT_EQ(records[i].get_lsn(), expected_lsn);
    expected_lsn += WALRecord::size_bytes(bodies[i]);
  }
}

TEST_F(WALTest, ReopenContinuesAppendingFromPersistedWalEnd) {
  std::vector<std::byte> body1 = {std::byte{0x01}, std::byte{0x02}};
  std::vector<std::byte> body2 = {std::byte{0x10}, std::byte{0x20},
                                  std::byte{0x30}};
  std::vector<std::byte> body3 = {std::byte{0xAA}, std::byte{0xBB}};

  {
    auto wal = WAL::initializeNew(wal_path);
    wal->write(WALRecord::RecordType::INSERT, 1, body1);
    wal->write(WALRecord::RecordType::INSERT, 1, body2);
    wal->flush();
  }

  {
    auto wal = WAL::openExisting(wal_path);
    EXPECT_EQ(wal->getFlushedLSN(), WALRecord::size_bytes(body1));
    wal->write(WALRecord::RecordType::INSERT, 1, body3);
    wal->flush();
  }

  std::vector<WALRecord> records = readWalRecords();
  ASSERT_EQ(records.size(), 3u);
  EXPECT_EQ(records[0].get_lsn(), 0u);
  EXPECT_EQ(records[1].get_lsn(), WALRecord::size_bytes(body1));
  EXPECT_EQ(records[2].get_lsn(),
            WALRecord::size_bytes(body1) + WALRecord::size_bytes(body2));
}