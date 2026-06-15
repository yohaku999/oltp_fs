#include "storage/buffer/arc_replacement.h"

#include <gtest/gtest.h>

#include <optional>
#include <string>
#include <unordered_map>

#include "storage/buffer/page_key.h"

namespace {

PageKey key(int page_id) {
  return PageKey{"test.db", page_id};
}

class ARCTestHarness {
 public:
  explicit ARCTestHarness(int capacity) : arc(capacity) {}

  void loadIntoFreeFrame(int frame_id, const PageKey& page_key) {
    arc.onNoneResidentPageAccess(frame_id, page_key);
    frame_to_key.insert_or_assign(frame_id, page_key);
  }

  std::optional<int> selectVictim(const PageKey& page_key) {
    return arc.selectVictimFrame(page_key, [](int) { return true; });
  }

  std::optional<int> selectVictim(
      const PageKey& page_key,
      const std::unordered_map<int, bool>& evictable_by_frame) {
    return arc.selectVictimFrame(page_key, [&](int frame_id) {
      auto it = evictable_by_frame.find(frame_id);
      return it != evictable_by_frame.end() && it->second;
    });
  }

  int evictFor(const PageKey& page_key) {
    auto victim_frame = selectVictim(page_key);
    EXPECT_TRUE(victim_frame.has_value());
    const int frame_id = victim_frame.value();
    arc.onEviction(frame_id, frame_to_key.at(frame_id));
    frame_to_key.erase(frame_id);
    return frame_id;
  }

  void replaceWith(const PageKey& page_key) {
    const int frame_id = evictFor(page_key);
    loadIntoFreeFrame(frame_id, page_key);
  }

  void accessResident(const PageKey& page_key) {
    arc.onResidentPageAccess(page_key);
  }

  ARCReplacement arc;
  std::unordered_map<int, PageKey> frame_to_key;
};

}  // namespace

TEST(ARCReplacementTest, SelectsT1LeastRecentlyUsedForNewPage) {
  ARCTestHarness harness(2);
  const PageKey a = key(1);
  const PageKey b = key(2);
  const PageKey c = key(3);

  harness.loadIntoFreeFrame(0, a);
  harness.loadIntoFreeFrame(1, b);

  auto victim = harness.selectVictim(c);

  ASSERT_TRUE(victim.has_value());
  EXPECT_EQ(0, victim.value());
}

TEST(ARCReplacementTest, ResidentHitPromotesFromT1ToT2) {
  ARCTestHarness harness(2);
  const PageKey a = key(1);
  const PageKey b = key(2);
  const PageKey c = key(3);

  harness.loadIntoFreeFrame(0, a);
  harness.loadIntoFreeFrame(1, b);
  // T1:a, b
  harness.accessResident(a);
  // T1:b T2:a

  auto victim = harness.selectVictim(c);
  // t1_target_ == 0
  // T1:c B1:b T2:a

  ASSERT_TRUE(victim.has_value());
  EXPECT_EQ(1, victim.value());
}

TEST(ARCReplacementTest, B1HitLoadsPageIntoT2) {
  ARCTestHarness harness(2);
  const PageKey a = key(1);
  const PageKey b = key(2);
  const PageKey c = key(3);

  harness.loadIntoFreeFrame(0, a);
  harness.loadIntoFreeFrame(1, b);
  harness.replaceWith(c);
  // T1:c,b B1:a

  const int frame_id = harness.evictFor(a);
  harness.loadIntoFreeFrame(frame_id, a);
  // T1:c T2:a B1:b

  // If a is in T2, there will be no effect on LRU order.
  // T1:c T2:a B1:b
  harness.accessResident(a);
  // t1_target == 1 and it should swap a
  auto victim = harness.selectVictim(b);

  ASSERT_TRUE(victim.has_value());
  EXPECT_EQ(0, victim.value());
}

TEST(ARCReplacementTest, B2HitUsesArcReplaceDecision) {
  ARCTestHarness harness(3);
  const PageKey a = key(1);
  const PageKey b = key(2);
  const PageKey c = key(3);
  const PageKey d = key(4);
  const PageKey e = key(5);

  harness.loadIntoFreeFrame(0, a);
  harness.loadIntoFreeFrame(1, b);
  harness.loadIntoFreeFrame(2, c);
  harness.accessResident(a);
  // T1: b(frame 1), c(frame 2)
  // T2: a(frame 0)
  harness.replaceWith(d);
  // T1: c(frame 2), d(frame 1)
  // T2: a(frame 0)
  // B1: b
  harness.replaceWith(b);
  // T1: d(frame 1)
  // T2: a(frame 0), b(frame 2)
  // B1: c
  // t1_target == 1
  harness.replaceWith(e);
  // T1: d(frame 1), e(frame 2)
  // T2: b(frame 0)
  // B1: c
  // B2: a
  // t1_target == 2

  const int frame_id = harness.evictFor(a);
  // a is a B2 hit. REPLACE chooses T1, whose LRU is d(frame 1).
  EXPECT_EQ(1, frame_id);

  harness.loadIntoFreeFrame(frame_id, a);
  EXPECT_NO_THROW(harness.accessResident(a));
}

TEST(ARCReplacementTest, ThrowsWhenLoadingWithoutFreeFrame) {
  ARCReplacement arc(1);
  arc.onNoneResidentPageAccess(0, key(1));

  EXPECT_THROW(arc.onNoneResidentPageAccess(1, key(2)), std::logic_error);
}

TEST(ARCReplacementTest, ReturnsNulloptWhenSelectedListHasNoEvictableFrame) {
  ARCTestHarness harness(2);
  const PageKey a = key(1);
  const PageKey b = key(2);
  const PageKey c = key(3);

  harness.loadIntoFreeFrame(0, a);
  harness.loadIntoFreeFrame(1, b);

  auto victim = harness.selectVictim(c, {{0, false}, {1, false}});

  EXPECT_FALSE(victim.has_value());
}
