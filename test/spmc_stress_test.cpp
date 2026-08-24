#include "babq.h"
#include "test/babq/babq_check.h"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <thread>
#include <vector>

namespace
{
  constexpr uint32_t NUM_CONSUMERS = 4;
  constexpr babq::RSetEntry BASE = 0x1000;

  uint64_t g_total = 0;
  std::unique_ptr<std::atomic<uint8_t>[]> g_seen;
  std::atomic<uint64_t> g_processed_count{0};
  std::atomic<bool> g_producer_done{false};

  thread_local babq::RSetEntry t_last = 0;
  thread_local bool t_has_last = false;

  void processor(babq::RSetEntry entry)
  {
    // assert2: increase within each thread
    if (t_has_last)
    {
      BABQ_CHECK(t_last < entry);
    }
    t_last = entry;
    t_has_last = true;

    // entry = BASE + seq
    // assert3: each entry is valid
    BABQ_CHECK(entry >= BASE);
    const uint64_t seq = static_cast<uint64_t>(entry - BASE);
    BABQ_CHECK(seq < g_total);

    // To record duplication
    g_seen[seq].fetch_add(1, std::memory_order_relaxed);
    g_processed_count.fetch_add(1, std::memory_order_relaxed);
  }

  void test_spmc_stress()
  {
    // Number of items
    g_total = 5000000;

    printf("Test: SPMC stress (1 producer + %u consumers, %llu items) ... ",
           NUM_CONSUMERS, static_cast<unsigned long long>(g_total));
    fflush(stdout);
    g_seen.reset(new std::atomic<uint8_t>[g_total]());
    const auto started = std::chrono::steady_clock::now();

    std::vector<std::thread> consumers;
    for (uint32_t i = 0; i < NUM_CONSUMERS; i++)
    {
      consumers.emplace_back([]()
                             {
      while (!g_producer_done.load(std::memory_order_acquire)) {
        babq::gc_worker_drain(processor);
        std::this_thread::yield();
      }
      babq::gc_worker_drain(processor); });
    }

    std::thread producer([]()
                         {
    babq::MutatorLocal mutator{};
    babq::get_mutator_registry().register_mutator(&mutator);

    for (uint64_t seq = 0; seq < g_total; seq++) {
      babq::enqueue(mutator, BASE + static_cast<babq::RSetEntry>(seq), processor);
    }

    babq::drain_one_mutator(mutator, processor);
    BABQ_CHECK_EQ(mutator.write_index.load(), 0u);

    babq::get_mutator_registry().unregister_mutator(&mutator); });

    producer.join();
    g_producer_done.store(true, std::memory_order_release);

    for (auto &t : consumers)
    {
      t.join();
    }

    babq::gc_worker_drain(processor);

    // assert 5: ensure the ring is empty
    BABQ_CHECK(babq::get_global_ring().is_drained());

    // assert 1 : check seen_map for each entry
    uint64_t lost = 0;
    uint64_t duplicated = 0;
    for (uint64_t seq = 0; seq < g_total; seq++)
    {
      const uint8_t seen = g_seen[seq].load(std::memory_order_relaxed);
      if (seen == 1)
      {
        continue;
      }
      if (seen == 0)
      {
        lost++;
      }
      else
      {
        duplicated++;
      }
    }

    // Ensure no lost or duplication
    BABQ_CHECK_EQ(lost, 0ULL);
    BABQ_CHECK_EQ(duplicated, 0ULL);

    // check processed entries = inserted entries
    BABQ_CHECK_EQ(g_processed_count.load(), g_total);

    g_seen.reset();
  }
}
int main()
{
  test_spmc_stress();
  return 0;
}
