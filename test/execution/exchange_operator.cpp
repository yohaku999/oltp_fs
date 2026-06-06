#include "execution/operators/exchange/exchange_operator.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <optional>
#include <stdexcept>
#include <vector>

#include "execution/operators/exchange/exchange_coordinator.h"
#include "stub_rid_operator.h"
#include "stub_row_operator.h"

TEST(ExchangeOperatorTest, ReturnsAllRowsFromMultipleProducers) {
  std::vector<std::vector<TypedRow>> producer_rows;
  producer_rows.push_back(
      {makeStubRow(1, "alice", 10), makeStubRow(2, "bob", 20)});
  producer_rows.push_back(
      {makeStubRow(3, "carol", 30), makeStubRow(4, "dave", 40)});

  ExchangeOperator<> exchange(
      2, producer_rows.size(), 1, ExchangeOperator<>::DispatchRule::RoundRobin,
      [&producer_rows](size_t index) -> std::unique_ptr<TypedRowOperator> {
        return std::make_unique<StubRowOperator>(producer_rows[index]);
      });

  exchange.open();

  std::vector<int> ids;
  while (auto row = exchange.next()) {
    ids.push_back(std::get<Column::IntegerType>(row->values[0]));
  }

  exchange.close();

  std::sort(ids.begin(), ids.end());
  ASSERT_EQ(ids.size(), 4U);
  EXPECT_EQ(ids, (std::vector<int>{1, 2, 3, 4}));
}

TEST(ExchangeOperatorTest, ReturnsEofImmediatelyWhenNoProducersExist) {
  ExchangeOperator<> exchange(
      2, 0, 1, ExchangeOperator<>::DispatchRule::RoundRobin,
      [](size_t) -> std::unique_ptr<TypedRowOperator> { return nullptr; });

  exchange.open();
  EXPECT_FALSE(exchange.next().has_value());
  exchange.close();
}

TEST(ExchangeOperatorTest, RejectsMultipleConsumerThreads) {
  EXPECT_THROW(ExchangeOperator<>(
                   2, 1, 2, ExchangeOperator<>::DispatchRule::RoundRobin,
                   [](size_t) -> std::unique_ptr<TypedRowOperator> {
                     return std::make_unique<StubRowOperator>(
                         std::vector<TypedRow>{makeStubRow(1, "alice", 10)});
                   }),
               std::invalid_argument);
}

TEST(RidExchangeOperatorTest, ReturnsAllRidsFromMultipleProducers) {
  std::vector<std::vector<RID>> producer_rids;
  producer_rids.push_back({RID{1, 10}, RID{2, 20}});
  producer_rids.push_back({RID{3, 30}, RID{4, 40}});

  ExchangeOperator<RID> exchange(
      2, producer_rids.size(), 1,
      ExchangeOperator<RID>::DispatchRule::RoundRobin,
      [&producer_rids](size_t index) -> std::unique_ptr<RidOperator> {
        return std::make_unique<StubRidOperator>(producer_rids[index]);
      });

  exchange.open();

  std::vector<int> heap_page_ids;
  while (auto rid = exchange.next()) {
    heap_page_ids.push_back(rid->heap_page_id);
  }

  exchange.close();

  std::sort(heap_page_ids.begin(), heap_page_ids.end());
  EXPECT_EQ(heap_page_ids, (std::vector<int>{1, 2, 3, 4}));
}

TEST(ExchangeCoordinatorTest, HashPartitionRoutesRowsToMatchingConsumers) {
  std::vector<std::vector<TypedRow>> producer_rows;
  producer_rows.push_back({
      makeStubRow(1, "alice", 10),
      makeStubRow(2, "bob", 20),
      makeStubRow(3, "carol", 30),
      makeStubRow(4, "dave", 40),
  });

  ExchangeCoordinator<TypedRow> coordinator(
      2, producer_rows.size(), 2,
      ExchangeCoordinator<TypedRow>::DispatchRule::HashPartition,
      [&producer_rows](size_t index) -> std::unique_ptr<TypedRowOperator> {
        return std::make_unique<StubRowOperator>(producer_rows[index]);
      },
      [](const TypedRow& row) -> size_t {
        return static_cast<size_t>(
            std::get<Column::IntegerType>(row.values[0]));
      });

  coordinator.consumerAt(0).open();
  coordinator.consumerAt(1).open();
  coordinator.startOnce();

  std::vector<int> even_ids;
  while (auto row = coordinator.consumerAt(0).next()) {
    even_ids.push_back(std::get<Column::IntegerType>(row->values[0]));
  }

  std::vector<int> odd_ids;
  while (auto row = coordinator.consumerAt(1).next()) {
    odd_ids.push_back(std::get<Column::IntegerType>(row->values[0]));
  }

  coordinator.join();

  std::sort(even_ids.begin(), even_ids.end());
  std::sort(odd_ids.begin(), odd_ids.end());
  EXPECT_EQ(even_ids, (std::vector<int>{2, 4}));
  EXPECT_EQ(odd_ids, (std::vector<int>{1, 3}));
}

TEST(ExchangeCoordinatorTest, StartOnceIsOneShotAndDoesNotDuplicateRows) {
  std::vector<std::vector<TypedRow>> producer_rows;
  producer_rows.push_back(
      {makeStubRow(1, "alice", 10), makeStubRow(2, "bob", 20)});

  ExchangeCoordinator<TypedRow> coordinator(
      1, producer_rows.size(), 1,
      ExchangeCoordinator<TypedRow>::DispatchRule::RoundRobin,
      [&producer_rows](size_t index) -> std::unique_ptr<TypedRowOperator> {
        return std::make_unique<StubRowOperator>(producer_rows[index]);
      });

  coordinator.consumerAt(0).open();
  coordinator.startOnce();
  coordinator.startOnce();

  std::vector<int> ids;
  while (auto row = coordinator.consumerAt(0).next()) {
    ids.push_back(std::get<Column::IntegerType>(row->values[0]));
  }

  coordinator.join();

  EXPECT_EQ(ids, (std::vector<int>{1, 2}));
}