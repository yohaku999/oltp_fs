#include "../src/frame_directory.h"
#include "../src/page.h"
#include <array>
#include <memory>
#include <gtest/gtest.h>

class FrameDirectoryTest : public ::testing::Test
{
protected:
    FrameDirectory directory;
    std::array<char, 4096> page_buffer1;
    std::array<char, 4096> page_buffer2;
    std::array<char, 4096> page_buffer3;
    std::unique_ptr<Page> page1;
    std::unique_ptr<Page> page2;
    std::unique_ptr<Page> page3;

    void SetUp() override
    {
        page1 = std::make_unique<Page>(page_buffer1.data(), true, 0, 1);
        page2 = std::make_unique<Page>(page_buffer2.data(), true, 0, 2);
        page3 = std::make_unique<Page>(page_buffer3.data(), true, 0, 3);
    }
};

TEST_F(FrameDirectoryTest, TestClaimFreeFrame)
{
    // use all frames
    for (int i = 0; i < FrameDirectory::MAX_FRAME_COUNT; ++i) {
        auto frame_opt = directory.claimFreeFrame();
        ASSERT_TRUE(frame_opt.has_value());
        EXPECT_GE(frame_opt.value(), 0);
        EXPECT_LT(frame_opt.value(), FrameDirectory::MAX_FRAME_COUNT);
    }
    
    // check api returns nullopt when no free frames
    auto no_frame = directory.claimFreeFrame();
    EXPECT_FALSE(no_frame.has_value());
}

TEST_F(FrameDirectoryTest, RegisterAndFindPageByID)
{
    auto frame_opt = directory.claimFreeFrame();
    ASSERT_TRUE(frame_opt.has_value());
    int frame_id = frame_opt.value();
    
    directory.registerPage(frame_id, 100, "test.db", std::move(page1));
    
    // search frame by pageID
    auto found_frame = directory.findFrameByPage(100, "test.db");
    ASSERT_TRUE(found_frame.has_value());
    EXPECT_EQ(frame_id, found_frame.value());
    
    // checkinside frame details
    const auto& frame = directory.getFrame(frame_id);
    EXPECT_NE(nullptr, frame.page);
    EXPECT_EQ(100, frame.page_id);
    EXPECT_EQ("test.db", frame.file_path);
    EXPECT_EQ(0, frame.pin_count);
    
    // Clean up
    directory.unregisterPage(frame_id);
}

TEST_F(FrameDirectoryTest, FindNonExistentPageReturnsNullopt)
{
    auto result = directory.findFrameByPage(999, "nonexistent.db");
    EXPECT_FALSE(result.has_value());
}

TEST_F(FrameDirectoryTest, RegisterMultiplePagesInDifferentFrames)
{
    auto frame1 = directory.claimFreeFrame();
    auto frame2 = directory.claimFreeFrame();
    auto frame3 = directory.claimFreeFrame();
    
    ASSERT_TRUE(frame1.has_value());
    ASSERT_TRUE(frame2.has_value());
    ASSERT_TRUE(frame3.has_value());
    
    directory.registerPage(frame1.value(), 10, "file1.db", std::move(page1));
    directory.registerPage(frame2.value(), 20, "file2.db", std::move(page2));
    directory.registerPage(frame3.value(), 30, "file1.db", std::move(page3));
    
    // each page can be found by its pageID and filePath
    auto found1 = directory.findFrameByPage(10, "file1.db");
    auto found2 = directory.findFrameByPage(20, "file2.db");
    auto found3 = directory.findFrameByPage(30, "file1.db");
    
    // ensure reading from the correct frame
    EXPECT_EQ(frame1.value(), found1.value());
    EXPECT_EQ(frame2.value(), found2.value());
    EXPECT_EQ(frame3.value(), found3.value());
    
    // Clean up
    directory.unregisterPage(frame1.value());
    directory.unregisterPage(frame2.value());
    directory.unregisterPage(frame3.value());
}

TEST_F(FrameDirectoryTest, UnregisterPageFreesFrame)
{
    auto frame_opt = directory.claimFreeFrame();
    ASSERT_TRUE(frame_opt.has_value());
    int frame_id = frame_opt.value();
    
    directory.registerPage(frame_id, 100, "test.db", std::move(page1));
    
    // check registerd
    auto found = directory.findFrameByPage(100, "test.db");
    ASSERT_TRUE(found.has_value());
    
    // api to test
    directory.unregisterPage(frame_id);
    
    // page not found
    found = directory.findFrameByPage(100, "test.db");
    EXPECT_FALSE(found.has_value());
    
    // frame is free again
    const auto& frame = directory.getFrame(frame_id);
    EXPECT_EQ(nullptr, frame.page);
    EXPECT_EQ(-1, frame.page_id);
}


TEST_F(FrameDirectoryTest, CheckOccupiedStatusBeforeAndAfterRegistration)
{
    auto frame_opt = directory.claimFreeFrame();
    ASSERT_TRUE(frame_opt.has_value());
    int frame_id = frame_opt.value();
    
    // before registration: free
    EXPECT_TRUE(directory.getFrame(frame_id).page == nullptr);
    
    directory.registerPage(frame_id, 100, "test.db", std::move(page1));
    
    // after registration: occupied
    EXPECT_FALSE(directory.getFrame(frame_id).page == nullptr);
    
    directory.unregisterPage(frame_id);
    
    // after unregistration: free again
    EXPECT_TRUE(directory.getFrame(frame_id).page == nullptr);
}

// TEST EVICTION
TEST_F(FrameDirectoryTest, MultipleRegisterUnregisterCycles)
{
    // Perform multiple cycles of filling frames and freeing them
    for (int cycle = 0; cycle < 3; ++cycle)
    {
        // Fill all frames
        std::vector<int> claimed_frames;
        for (size_t i = 0; i < FrameDirectory::MAX_FRAME_COUNT; ++i)
        {
            auto frame_opt = directory.claimFreeFrame();
            ASSERT_TRUE(frame_opt.has_value()) << "Failed to claim frame in cycle " << cycle;
            int frame_id = frame_opt.value();
            claimed_frames.push_back(frame_id);
            
            // Create a unique page for this cycle and frame
            std::array<char, 4096> buffer;
            auto page = std::make_unique<Page>(buffer.data(), true, 0, i);
            int page_id = cycle * 100 + i;
            
            directory.registerPage(frame_id, page_id, "test.db", std::move(page));
            
            // Verify registration
            auto found = directory.findFrameByPage(page_id, "test.db");
            EXPECT_TRUE(found.has_value());
            EXPECT_EQ(frame_id, found.value());
        }
        
        // All frames should be occupied now
        auto no_frame = directory.claimFreeFrame();
        EXPECT_FALSE(no_frame.has_value()) << "Should have no free frames after filling all";
        
        // Unregister all frames
        for (int frame_id : claimed_frames)
        {
            directory.unregisterPage(frame_id);
        }
        
        // All frames should be free again
        for (size_t i = 0; i < FrameDirectory::MAX_FRAME_COUNT; ++i)
        {
            auto frame_opt = directory.claimFreeFrame();
            EXPECT_TRUE(frame_opt.has_value()) << "Frame should be free after unregister in cycle " << cycle;
            if (frame_opt.has_value())
            {
                directory.unregisterPage(frame_opt.value()); // Clean up for next iteration
            }
        }
    }
}

TEST_F(FrameDirectoryTest, FrameReuseAfterUnregister)
{
    // Track all frame IDs used
    std::set<int> used_frame_ids;
    
    // Fill all frames
    std::vector<int> initial_frames;
    for (size_t i = 0; i < FrameDirectory::MAX_FRAME_COUNT; ++i)
    {
        auto frame_opt = directory.claimFreeFrame();
        ASSERT_TRUE(frame_opt.has_value());
        int frame_id = frame_opt.value();
        initial_frames.push_back(frame_id);
        used_frame_ids.insert(frame_id);
        
        std::array<char, 4096> buffer;
        auto page = std::make_unique<Page>(buffer.data(), true, 0, i);
        directory.registerPage(frame_id, i, "test.db", std::move(page));
    }
    
    // No free frames should be available
    EXPECT_FALSE(directory.claimFreeFrame().has_value());
    
    // Unregister one frame
    int unregistered_frame_id = initial_frames[0];
    directory.unregisterPage(unregistered_frame_id);
    
    // Claim the freed frame
    auto reused_frame_opt = directory.claimFreeFrame();
    ASSERT_TRUE(reused_frame_opt.has_value());
    int reused_frame_id = reused_frame_opt.value();
    
    // The reused frame should be exactly the same frame ID that was just unregistered
    EXPECT_EQ(unregistered_frame_id, reused_frame_id)
        << "Reused frame ID should be the same as the unregistered frame ID";
    
    // Register a new page in the reused frame
    std::array<char, 4096> new_buffer;
    auto new_page = std::make_unique<Page>(new_buffer.data(), true, 0, 99);
    directory.registerPage(reused_frame_id, 999, "new.db", std::move(new_page));
    
    // Verify the new registration
    auto found = directory.findFrameByPage(999, "new.db");
    ASSERT_TRUE(found.has_value());
    EXPECT_EQ(reused_frame_id, found.value());
}

TEST_F(FrameDirectoryTest, FindVictimFrameWhenAllFramesFilled)
{
    // Fill all frames
    for (size_t i = 0; i < FrameDirectory::MAX_FRAME_COUNT; ++i)
    {
        auto frame_opt = directory.claimFreeFrame();
        ASSERT_TRUE(frame_opt.has_value());
        
        std::array<char, 4096> buffer;
        auto page = std::make_unique<Page>(buffer.data(), true, 0, i);
        directory.registerPage(frame_opt.value(), i, "test.db", std::move(page));
    }
    
    // No free frames should be available
    EXPECT_FALSE(directory.claimFreeFrame().has_value());
    
    // Should be able to find a victim frame for eviction.
    auto victim_opt = directory.findVictimFrame();
    ASSERT_TRUE(victim_opt.has_value())
        << "Should find a victim frame when all frames are filled";
    
    // The victim frame should not be pinned
    int victim_frame_id = victim_opt.value();
    EXPECT_FALSE(directory.isPinned(victim_frame_id));
}