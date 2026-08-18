/*
 * concurrent_test — 上层：mutator 线程持续入队，GC worker 线程并发排空全局 ring，
 * Validation: 无丢失、无重复，且结束时 ring 已排空。
 *
 * 注意：NUM_MUTATORS 固定为 1
 */

#include "babq.h"

#include <atomic>
// #include <cassert>
#include "test/babq/babq_check.h"
#include <cstdio>
#include <thread>
#include <vector>

namespace
{

    // Global counter for processed entries
    std::atomic<uint64_t> g_processed_count{0};
    std::atomic<uint64_t> g_processed_sum{0};
    std::atomic<bool> g_mutators_done{false};

    void test_processor(babq::RSetEntry entry)
    {
        g_processed_count.fetch_add(1);
        g_processed_sum.fetch_add(entry);
    }

    void test_concurrent()
    {
        printf("Test: Concurrent Mutators and GC Workers... ");

        constexpr uint32_t NUM_MUTATORS = 1;
        constexpr uint32_t NUM_GC_WORKERS = 2;
        constexpr uint32_t ITEMS_PER_MUTATOR = babq::BATCH_SIZE * 1500;

        g_processed_count = 0;
        g_processed_sum = 0;
        g_mutators_done.store(false);

        std::atomic<uint64_t> total_expected_sum{0};

        std::vector<std::thread> mutator_threads;
        std::vector<std::thread> gc_threads;

        // Launch GC Workers
        for (uint32_t i = 0; i < NUM_GC_WORKERS; i++)
        {
            gc_threads.emplace_back([]()
                                    {
      // Keep draining until mutators are done
      while (!g_mutators_done.load(std::memory_order_acquire)) {
        babq::gc_worker_drain(test_processor);
        std::this_thread::yield();
      }
      // Final drain to ensure empty
      babq::gc_worker_drain(test_processor); });
            // within gc_worker_drain-> ensure ring is empty(DeqStatus:EMPTY).
        }

        // Launch Mutators
        for (uint32_t i = 0; i < NUM_MUTATORS; i++)
        {
            mutator_threads.emplace_back([&total_expected_sum, i]()
                                         {
      babq::MutatorLocal mutator{};
      babq::get_mutator_registry().register_mutator(&mutator);

      uint64_t local_sum = 0;

      uint32_t base_val = (i + 1) * 1000000;

      for (uint32_t j = 0; j < ITEMS_PER_MUTATOR; j++) {
        babq::RSetEntry entry = base_val + j;
        local_sum += entry;
        babq::enqueue(mutator, entry, test_processor);
      }

      total_expected_sum.fetch_add(local_sum, std::memory_order_relaxed);

      // Assert local buffer is fully flushed
      //assert(mutator.write_index.load() == 0);
      BABQ_CHECK_EQ(mutator.write_index.load(), 0u);

      babq::get_mutator_registry().unregister_mutator(&mutator); });
        }

        // Wait for all Mutators to finish enqueuing
        for (auto &t : mutator_threads)
        {
            t.join();
        }

        // Signal GC workers that production is done
        g_mutators_done.store(true, std::memory_order_release);

        // Wait for GC workers to finish draining
        for (auto &t : gc_threads)
        {
            t.join();
        }

        // Verify exactly all items were processed without loss or duplication
        uint64_t final_count = g_processed_count.load();
        uint64_t final_sum = g_processed_sum.load();

        // assert(final_count == NUM_MUTATORS * ITEMS_PER_MUTATOR);
        // assert(final_sum == total_expected_sum.load());
        // assert(babq::get_global_ring().size() == 0);

        BABQ_CHECK_EQ(final_count, static_cast<uint64_t>(NUM_MUTATORS) * ITEMS_PER_MUTATOR);
        BABQ_CHECK_EQ(final_sum, total_expected_sum.load());
        BABQ_CHECK(babq::get_global_ring().is_drained());

        printf("PASSED (Processed %llu items across %d threads)\n", final_count,
               NUM_MUTATORS + NUM_GC_WORKERS);
    }

} // namespace

int main()
{
    test_concurrent();
    return 0;
}
