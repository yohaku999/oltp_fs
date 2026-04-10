#include "../src/storage/frame_directory.h"

#include <gtest/gtest.h>

#include <array>
#include <memory>

#include "../src/storage/page.h"

class FrameDirectoryTest : public ::testing::Test {
 protected:
  FrameDirectory directory;
  std::array<char, 4096> page_buffer1;
  std::array<char, 4096> page_buffer2;
  std::array<char, 4096> page_buffer3;
  std::unique_ptr<Page> page1;
  std::unique_ptr<Page> page2;
  std::unique_ptr<Page> page3;

  void SetUp() override {
    page1 = std::make_unique<Page>(page_buffer1.data(), true, 0, 1);
    page2 = std::make_unique<Page>(page_buffer2.data(), true, 0, 2);
    page3 = std::make_unique<Page>(page_buffer3.data(), true, 0, 3);
  }
};

TEST_F(FrameDirectoryTest, TestClaimFreeFrame) {
  for (int i = 0; i < FrameDirectory::MAX_FRAME_COUNT; ++i) {
    auto frame_opt = directory.reserveFreeFrame();
    ASSERT_TRUE(frame_opt.has_value());
    EXPECT_GE(frame_opt.value(), 0);
    EXPECT_LT(frame_opt.value(), FrameDirectory::MAX_FRAME_COUNT);
  }

  auto no_frame = directory.reserveFreeFrame();
  EXPECT_FALSE(no_frame.has_value());
}

TEST_F(FrameDirectoryTest, RegisterAndFindPageByID) {
  auto frame_opt = directory.reserveFreeFrame();
  ASSERT_TRUE(frame_opt.has_value());
  int frame_id = frame_opt.value();

  directory.registerResidentPage(frame_id, 100, "test.db", std::move(page1));

  auto found_frame = directory.findResidentFrame(100, "test.db");
  ASSERT_TRUE(found_frame.has_value());
  EXPECT_EQ(frame_id, found_frame.value());

  // Registration should populate both the lookup table and the frame metadata
  // that eviction and pin tracking depend on.
  const auto& frame = directory.getFrame(frame_id);
  EXPECT_NE(nullptr, frame.page);
  EXPECT_EQ(100, frame.page_id);
  EXPECT_EQ("test.db", frame.file_path);
  EXPECT_EQ(0, frame.pin_count);

  directory.unregisterResidentPage(frame_id);
}

TEST_F(FrameDirectoryTest, FindNonExistentPageReturnsNullopt) {
  auto result = directory.findResidentFrame(999, "nonexistent.db");
  EXPECT_FALSE(result.has_value());
}

TEST_F(FrameDirectoryTest, RegisterMultiplePagesInDifferentFrames) {
  auto frame1 = directory.reserveFreeFrame();
  auto frame2 = directory.reserveFreeFrame();
  auto frame3 = directory.reserveFreeFrame();

  ASSERT_TRUE(frame1.has_value());
  ASSERT_TRUE(frame2.has_value());
  ASSERT_TRUE(frame3.has_value());

  directory.registerResidentPage(frame1.value(), 10, "file1.db",
                                 std::move(page1));
  directory.registerResidentPage(frame2.value(), 20, "file2.db",
                                 std::move(page2));
  directory.registerResidentPage(frame3.value(), 30, "file1.db",
                                 std::move(page3));

  auto found1 = directory.findResidentFrame(10, "file1.db");
  auto found2 = directory.findResidentFrame(20, "file2.db");
  auto found3 = directory.findResidentFrame(30, "file1.db");

  EXPECT_EQ(frame1.value(), found1.value());
  EXPECT_EQ(frame2.value(), found2.value());
  EXPECT_EQ(frame3.value(), found3.value());

  directory.unregisterResidentPage(frame1.value());
  directory.unregisterResidentPage(frame2.value());
  directory.unregisterResidentPage(frame3.value());
}

TEST_F(FrameDirectoryTest, UnregisterPageFreesFrame) {
  auto frame_opt = directory.reserveFreeFrame();
  ASSERT_TRUE(frame_opt.has_value());
  int frame_id = frame_opt.value();

  directory.registerResidentPage(frame_id, 100, "test.db", std::move(page1));

  auto found = directory.findResidentFrame(100, "test.db");
  ASSERT_TRUE(found.has_value());

  directory.unregisterResidentPage(frame_id);

  found = directory.findResidentFrame(100, "test.db");
  EXPECT_FALSE(found.has_value());

  const auto& frame = directory.getFrame(frame_id);
  EXPECT_EQ(nullptr, frame.page);
  EXPECT_EQ(-1, frame.page_id);
}

TEST_F(FrameDirectoryTest, CheckOccupiedStatusBeforeAndAfterRegistration) {
  auto frame_opt = directory.reserveFreeFrame();
  ASSERT_TRUE(frame_opt.has_value());
  int frame_id = frame_opt.value();

  EXPECT_TRUE(directory.getFrame(frame_id).page == nullptr);

  directory.registerResidentPage(frame_id, 100, "test.db", std::move(page1));

  EXPECT_FALSE(directory.getFrame(frame_id).page == nullptr);

  directory.unregisterResidentPage(frame_id);

  EXPECT_TRUE(directory.getFrame(frame_id).page == nullptr);
}

// Eviction-related frame reuse behavior.
TEST_F(FrameDirectoryTest, MultipleRegisterUnregisterCycles) {
  // Repeated full-capacity churn should return every frame back to the free
  // set.
  for (int cycle = 0; cycle < 3; ++cycle) {
    std::vector<int> claimed_frames;
    for (size_t i = 0; i < FrameDirectory::MAX_FRAME_COUNT; ++i) {
      auto frame_opt = directory.reserveFreeFrame();
      ASSERT_TRUE(frame_opt.has_value())
          << "Failed to claim frame in cycle " << cycle;
      int frame_id = frame_opt.value();
      claimed_frames.push_back(frame_id);

      std::array<char, 4096> buffer;
      auto page = std::make_unique<Page>(buffer.data(), true, 0, i);
      int page_id = cycle * 100 + i;

      directory.registerResidentPage(frame_id, page_id, "test.db",
                                     std::move(page));

      auto found = directory.findResidentFrame(page_id, "test.db");
      EXPECT_TRUE(found.has_value());
      EXPECT_EQ(frame_id, found.value());
    }

    auto no_frame = directory.reserveFreeFrame();
    EXPECT_FALSE(no_frame.has_value())
        << "Should have no free frames after filling all";

    for (int frame_id : claimed_frames) {
      directory.unregisterResidentPage(frame_id);
    }

    for (size_t i = 0; i < FrameDirectory::MAX_FRAME_COUNT; ++i) {
      auto frame_opt = directory.reserveFreeFrame();
      EXPECT_TRUE(frame_opt.has_value())
          << "Frame should be free after unregister in cycle " << cycle;
      if (frame_opt.has_value()) {
        directory.unregisterResidentPage(frame_opt.value());
      }
    }
  }
}

TEST_F(FrameDirectoryTest, FrameReuseAfterUnregister) {
  std::set<int> used_frame_ids;

  std::vector<int> initial_frames;
  for (size_t i = 0; i < FrameDirectory::MAX_FRAME_COUNT; ++i) {
    auto frame_opt = directory.reserveFreeFrame();
    ASSERT_TRUE(frame_opt.has_value());
    int frame_id = frame_opt.value();
    initial_frames.push_back(frame_id);
    used_frame_ids.insert(frame_id);

    std::array<char, 4096> buffer;
    auto page = std::make_unique<Page>(buffer.data(), true, 0, i);
    directory.registerResidentPage(frame_id, i, "test.db", std::move(page));
  }

  EXPECT_FALSE(directory.reserveFreeFrame().has_value());

  int unregistered_frame_id = initial_frames[0];
  directory.unregisterResidentPage(unregistered_frame_id);

  auto reused_frame_opt = directory.reserveFreeFrame();
  ASSERT_TRUE(reused_frame_opt.has_value());
  int reused_frame_id = reused_frame_opt.value();

  // The allocator should immediately recycle the exact frame that was freed.
  EXPECT_EQ(unregistered_frame_id, reused_frame_id)
      << "Reused frame ID should be the same as the unregistered frame ID";

  std::array<char, 4096> new_buffer;
  auto new_page = std::make_unique<Page>(new_buffer.data(), true, 0, 99);
  directory.registerResidentPage(reused_frame_id, 999, "new.db",
                                 std::move(new_page));

  auto found = directory.findResidentFrame(999, "new.db");
  ASSERT_TRUE(found.has_value());
  EXPECT_EQ(reused_frame_id, found.value());
}

TEST_F(FrameDirectoryTest, FindVictimFrameWhenAllFramesFilled) {
  for (size_t i = 0; i < FrameDirectory::MAX_FRAME_COUNT; ++i) {
    auto frame_opt = directory.reserveFreeFrame();
    ASSERT_TRUE(frame_opt.has_value());

    std::array<char, 4096> buffer;
    auto page = std::make_unique<Page>(buffer.data(), true, 0, i);
    directory.registerResidentPage(frame_opt.value(), i, "test.db",
                                   std::move(page));
  }

  EXPECT_FALSE(directory.reserveFreeFrame().has_value());

  // Once every frame is occupied but none are pinned, victim selection should
  // still find an eviction candidate.
  auto victim_opt = directory.findVictimFrame();
  ASSERT_TRUE(victim_opt.has_value())
      << "Should find a victim frame when all frames are filled";

  int victim_frame_id = victim_opt.value();
  EXPECT_FALSE(directory.isPinned(victim_frame_id));
}