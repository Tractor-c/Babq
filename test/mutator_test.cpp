/*
 * mutator_test — 上层：MutatorLocal 攒批 → 满批触发 enq_global → gc_worker_drain 消费。
 */

#include "babq.h"

#include <atomic>
#include <cassert>
#include <cstdio>

namespace
{

    // Global counter for processed entries
    std::atomic<uint64_t> g_processed_count{0};
    std::atomic<uint64_t> g_processed_sum{0};

    void test_processor(babq::RSetEntry entry)
    {
        g_processed_count.fetch_add(1);
        g_processed_sum.fetch_add(entry);
    }

    void test_single_batch_flush()
    {
        printf("Test: Single batch flush + Gc worker processed ... ");
        babq::MutatorLocal mutator{};
        babq::get_mutator_registry().register_mutator(&mutator);

        g_processed_count = 0;
        g_processed_sum = 0;

        uint64_t expected_sum = 0;
        // Enqueue exactly BATCH_SIZE entries ->should trigger a enq_global()
        uint64_t inserted_items = babq::BATCH_SIZE;
        for (uint64_t i = 0; i < inserted_items; i++)
        {
            babq::RSetEntry entry = 0x1000 + i;
            expected_sum += entry;
            babq::enqueue(mutator, entry);
        }

        // write_index should have been reset to 0 after fully flush
        assert(mutator.write_index.load() == 0);

        // Drain and verify
        babq::gc_worker_drain(test_processor);
        assert(g_processed_count == inserted_items);
        assert(g_processed_sum == expected_sum);

        babq::get_mutator_registry().unregister_mutator(&mutator);
        printf("PASSED\n");
    }

} // namespace

int main()
{
    test_single_batch_flush();
    return 0;
}
