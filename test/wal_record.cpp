#include "../src/storage/wal_record.h"

#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <vector>

#include "../src/storage/wal/wal_body.h"

// Build a representative INSERT record and verify that serialization keeps
// both the record metadata and raw body bytes intact across round-trip.
TEST(WALRecordTest, SerializeDeserializeRoundTrip) {
  uint16_t page_id = 321;

  InsertRedoBody body;
  body.offset = 55;
  body.tuple = {std::byte{0x01}, std::byte{0x02}, std::byte{0x03}};
  std::vector<std::byte> encoded_body = body.encode();

  LSNAllocator allocator(0);
  WALRecord record = make_wal_record(allocator, WALRecord::RecordType::INSERT,
                                     page_id, encoded_body);

  std::vector<std::byte> serialized = record.serialize();
  WALRecord restored = WALRecord::deserialize(serialized);

  EXPECT_EQ(record.get_lsn(), restored.get_lsn());
  EXPECT_EQ(WALRecord::RecordType::INSERT, restored.get_type());
  EXPECT_EQ(page_id, restored.get_page_id());

  const auto& restored_body = restored.get_body();
  ASSERT_EQ(encoded_body.size(), restored_body.size());
  for (std::size_t i = 0; i < encoded_body.size(); ++i) {
    EXPECT_EQ(encoded_body[i], restored_body[i]);
  }
}

// Check that decode_body dispatches to the matching WALBody variant for
// each record type, while preserving the decoded payload contents.
TEST(WALRecordTest, DecodeBodyDispatchesToCorrectType) {
  LSNAllocator allocator(0);

  // INSERT
  InsertRedoBody insert_body;
  insert_body.offset = 10;
  insert_body.tuple = {std::byte{0x10}, std::byte{0x20}};
  auto insert_encoded = insert_body.encode();
  WALRecord insert_record = make_wal_record(
      allocator, WALRecord::RecordType::INSERT, 0, insert_encoded);

  WALBody insert_variant = decode_body(insert_record);
  ASSERT_TRUE(std::holds_alternative<InsertRedoBody>(insert_variant));
  const auto& insert_decoded = std::get<InsertRedoBody>(insert_variant);
  EXPECT_EQ(insert_body.offset, insert_decoded.offset);
  ASSERT_EQ(insert_body.tuple.size(), insert_decoded.tuple.size());
  for (std::size_t i = 0; i < insert_body.tuple.size(); ++i) {
    EXPECT_EQ(insert_body.tuple[i], insert_decoded.tuple[i]);
  }

  // UPDATE
  UpdateRedoBody update_body;
  update_body.offset = 20;
  update_body.before = {std::byte{0x01}};
  update_body.after = {std::byte{0x02}, std::byte{0x03}};
  auto update_encoded = update_body.encode();
  WALRecord update_record = make_wal_record(
      allocator, WALRecord::RecordType::UPDATE, 0, update_encoded);

  WALBody update_variant = decode_body(update_record);
  ASSERT_TRUE(std::holds_alternative<UpdateRedoBody>(update_variant));
  const auto& update_decoded = std::get<UpdateRedoBody>(update_variant);
  EXPECT_EQ(update_body.offset, update_decoded.offset);
  ASSERT_EQ(update_body.before.size(), update_decoded.before.size());
  for (std::size_t i = 0; i < update_body.before.size(); ++i) {
    EXPECT_EQ(update_body.before[i], update_decoded.before[i]);
  }
  ASSERT_EQ(update_body.after.size(), update_decoded.after.size());
  for (std::size_t i = 0; i < update_body.after.size(); ++i) {
    EXPECT_EQ(update_body.after[i], update_decoded.after[i]);
  }

  // DELETE
  DeleteRedoBody delete_body;
  delete_body.offset = 30;
  delete_body.before = {std::byte{0xAA}, std::byte{0xBB}};
  auto delete_encoded = delete_body.encode();
  WALRecord delete_record = make_wal_record(
      allocator, WALRecord::RecordType::DELETE, 0, delete_encoded);

  WALBody delete_variant = decode_body(delete_record);
  ASSERT_TRUE(std::holds_alternative<DeleteRedoBody>(delete_variant));
  const auto& delete_decoded = std::get<DeleteRedoBody>(delete_variant);
  EXPECT_EQ(delete_body.offset, delete_decoded.offset);
  ASSERT_EQ(delete_body.before.size(), delete_decoded.before.size());
  for (std::size_t i = 0; i < delete_body.before.size(); ++i) {
    EXPECT_EQ(delete_body.before[i], delete_decoded.before[i]);
  }
}
