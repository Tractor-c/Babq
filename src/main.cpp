/*
Basic test to verify:
1. Single-thread enqueue fills batch and triggers enq_global() to copy into RingBuffer
2. GC workers drain the batches from the Ring buffer
3. EagerPeek : copies local batch from Mutators and directly process the RSet inside
*/

#include "../include/babq.h"
#include <cassert>
#include <cstdio>
#include <thread>
#include <vector>

// Global counter for processed entries
static std::atomic<uint64_t> g_processed_count{0};
static std::atomic<uint64_t> g_processed_sum{0};

void test_processor(babq::RSetEntry entry)
{
    g_processed_count.fetch_add(1);
    g_processed_sum.fetch_add(entry);
}

void test_single_batch_flush()
{
    printf("Test 1: Single batch flush + Gc worker processed");
    babq::MutatorLocal mutator{};
    babq::get_mutator_registry().register_mutator(&mutator);

    g_processed_count = 0;
    g_processed_sum = 0;

    uint64_t expected_sum = 0;
    // Enqueue exactly BATCH_SIZE entries ->should trigger a enq_global()
    for (uint32_t i = 0; i < babq::BATCH_SIZE; i++)
    {
        babq::RSetEntry entry = 0x1000 + i;
        expected_sum += entry;
        babq::enqueue(mutator, entry);
    }

    // write_index should have been reset to 0 after flush
    assert(mutator.write_index.load() == 0);

    // Drain and verify
    babq::gc_worker_drain(test_processor);
    assert(g_processed_count == babq::BATCH_SIZE);
    assert(g_processed_sum == expected_sum);

    babq::get_mutator_registry().unregister_mutator(&mutator);
    printf("PASSDE\n");
}

void test_eager_peek()
{
    printf("Test 2: EagerPeek");
    babq::MutatorLocal mutator{};
    babq::get_mutator_registry().register_mutator(&mutator);

    g_processed_count = 0;
    g_processed_sum = 0;

    // Enqueue half a batch (no flush triggered)
    uint32_t partial = babq::BATCH_SIZE / 2;
    uint64_t expected_sum = 0;
    for (uint32_t i = 0; i < partial; i++)
    {
        babq::RSetEntry entry = 0x2000 + i;
        expected_sum += entry;
        babq::enqueue(mutator, entry);
    }
    assert(mutator.write_index.load() == partial);

    // EagerPeek should copy the partial batch into RingBuffer
    babq::eager_peek();

    // Drain and verify
    babq::gc_worker_drain(test_processor);
    assert(g_processed_count == partial);
    assert(g_processed_sum == expected_sum);

    // Mutator's state should be UNCHANGED (zero interference)
    assert(mutator.write_index.load() == partial);

    babq::get_mutator_registry().unregister_mutator(&mutator);
    printf("PASSED\n");
}

static std::atomic<bool> g_mutators_done{false};
void test_concurrent()
{
    printf("Test 3: Concurrent Mutators and GC Workers... ");

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
        babq::enqueue(mutator, entry);
      }

      total_expected_sum.fetch_add(local_sum, std::memory_order_relaxed);

      // Assert local buffer is fully flushed
      assert(mutator.write_index.load() == 0);

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

    assert(final_count == NUM_MUTATORS * ITEMS_PER_MUTATOR);
    assert(final_sum == total_expected_sum.load());

    printf("PASSED (Processed %llu items across %d threads)\n", final_count,
           NUM_MUTATORS + NUM_GC_WORKERS);
}

int main()
{
    test_single_batch_flush();
    test_eager_peek();
    test_concurrent();
    return 0;
}