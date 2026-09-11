#include "storage/buffer/frame_directory.h"

#include <gtest/gtest.h>

#include <array>
#include <memory>

#include "storage/page/page.h"

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
    page1 = std::make_unique<Page>(page_buffer1.data(), PageKind::Heap, 0, 1);
    page2 = std::make_unique<Page>(page_buffer2.data(), PageKind::Heap, 0, 2);
    page3 = std::make_unique<Page>(page_buffer3.data(), PageKind::Heap, 0, 3);
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

  const PageKey page_key{"test.db", 100};
  directory.registerResidentPage(frame_id, page_key, std::move(page1));

  auto found_frame = directory.findResidentFrame(page_key);
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
  auto result =
      directory.findResidentFrame(PageKey{"nonexistent.db", 999});
  EXPECT_FALSE(result.has_value());
}

TEST_F(FrameDirectoryTest, RegisterMultiplePagesInDifferentFrames) {
  auto frame1 = directory.reserveFreeFrame();
  auto frame2 = directory.reserveFreeFrame();
  auto frame3 = directory.reserveFreeFrame();

  ASSERT_TRUE(frame1.has_value());
  ASSERT_TRUE(frame2.has_value());
  ASSERT_TRUE(frame3.has_value());

  directory.registerResidentPage(frame1.value(), PageKey{"file1.db", 10},
                                 std::move(page1));
  directory.registerResidentPage(frame2.value(), PageKey{"file2.db", 20},
                                 std::move(page2));
  directory.registerResidentPage(frame3.value(), PageKey{"file1.db", 30},
                                 std::move(page3));

  auto found1 = directory.findResidentFrame(PageKey{"file1.db", 10});
  auto found2 = directory.findResidentFrame(PageKey{"file2.db", 20});
  auto found3 = directory.findResidentFrame(PageKey{"file1.db", 30});

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

  const PageKey page_key{"test.db", 100};
  directory.registerResidentPage(frame_id, page_key, std::move(page1));

  auto found = directory.findResidentFrame(page_key);
  ASSERT_TRUE(found.has_value());

  directory.unregisterResidentPage(frame_id);

  found = directory.findResidentFrame(page_key);
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

  directory.registerResidentPage(frame_id, PageKey{"test.db", 100},
                                 std::move(page1));

  EXPECT_FALSE(directory.getFrame(frame_id).page == nullptr);

  directory.unregisterResidentPage(frame_id);

  EXPECT_TRUE(directory.getFrame(frame_id).page == nullptr);
}

TEST_F(FrameDirectoryTest, ResetClearsResidentsPinsAndLookupState) {
  auto frame = directory.reserveFreeFrame();
  ASSERT_TRUE(frame.has_value());
  const PageKey page_key{"test.db", 100};
  directory.registerResidentPage(frame.value(), page_key,
                                 std::move(page1));
  directory.pin(frame.value());

  directory.reset();

  EXPECT_FALSE(directory.findResidentFrame(page_key).has_value());
  const auto stats = directory.collectStats();
  EXPECT_EQ(FrameDirectory::MAX_FRAME_COUNT, stats.frames_free);
  EXPECT_EQ(0u, stats.frames_resident);
  EXPECT_EQ(0u, stats.frames_pinned);
  for (size_t i = 0; i < FrameDirectory::MAX_FRAME_COUNT; ++i) {
    EXPECT_TRUE(directory.reserveFreeFrame().has_value());
  }
  EXPECT_FALSE(directory.reserveFreeFrame().has_value());
}

TEST_F(FrameDirectoryTest, CollectStatsCountsKindsPinnedAndDirtyPages) {
  std::array<char, 4096> heap_buffer{};
  std::array<char, 4096> leaf_buffer{};
  std::array<char, 4096> internal_buffer{};

  auto heap_page =
      std::make_unique<Page>(heap_buffer.data(), PageKind::Heap, 0, 1);
  auto leaf_page =
      std::make_unique<Page>(leaf_buffer.data(), PageKind::LeafIndex, 0, 2);
  auto internal_page = std::make_unique<Page>(internal_buffer.data(),
                                              PageKind::InternalIndex, 0, 3);

  leaf_page->clearDirty();
  internal_page->clearDirty();

  auto heap_frame = directory.reserveFreeFrame();
  auto leaf_frame = directory.reserveFreeFrame();
  auto internal_frame = directory.reserveFreeFrame();
  ASSERT_TRUE(heap_frame.has_value());
  ASSERT_TRUE(leaf_frame.has_value());
  ASSERT_TRUE(internal_frame.has_value());

  directory.registerResidentPage(heap_frame.value(), PageKey{"heap.db", 1},
                                 std::move(heap_page));
  directory.registerResidentPage(leaf_frame.value(), PageKey{"index.db", 2},
                                 std::move(leaf_page));
  directory.registerResidentPage(internal_frame.value(),
                                 PageKey{"index.db", 3},
                                 std::move(internal_page));
  directory.pin(heap_frame.value());
  directory.pin(internal_frame.value());

  const auto stats = directory.collectStats();

  EXPECT_EQ(stats.frames_resident, 3u);
  EXPECT_EQ(stats.frames_pinned, 2u);
  EXPECT_EQ(stats.frames_evictable, 1u);
  EXPECT_EQ(stats.frames_free, FrameDirectory::MAX_FRAME_COUNT - 3);

  EXPECT_EQ(stats.resident_heap_pages, 1u);
  EXPECT_EQ(stats.resident_leaf_index_pages, 1u);
  EXPECT_EQ(stats.resident_internal_index_pages, 1u);

  EXPECT_EQ(stats.pinned_heap_pages, 1u);
  EXPECT_EQ(stats.pinned_leaf_index_pages, 0u);
  EXPECT_EQ(stats.pinned_internal_index_pages, 1u);

  EXPECT_EQ(stats.dirty_heap_pages, 1u);
  EXPECT_EQ(stats.dirty_leaf_index_pages, 0u);
  EXPECT_EQ(stats.dirty_internal_index_pages, 0u);
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
      auto page = std::make_unique<Page>(buffer.data(), PageKind::Heap, 0, i);
      int page_id = cycle * 100 + i;

      const PageKey page_key{"test.db", page_id};
      directory.registerResidentPage(frame_id, page_key,
                                     std::move(page));

      auto found = directory.findResidentFrame(page_key);
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
    auto page = std::make_unique<Page>(buffer.data(), PageKind::Heap, 0, i);
    directory.registerResidentPage(
        frame_id, PageKey{"test.db", static_cast<int>(i)}, std::move(page));
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
  auto new_page =
      std::make_unique<Page>(new_buffer.data(), PageKind::Heap, 0, 99);
  directory.registerResidentPage(reused_frame_id, PageKey{"new.db", 999},
                                 std::move(new_page));

  auto found = directory.findResidentFrame(PageKey{"new.db", 999});
  ASSERT_TRUE(found.has_value());
  EXPECT_EQ(reused_frame_id, found.value());
}
