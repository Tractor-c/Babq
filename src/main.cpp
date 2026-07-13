/*
Basic test to verify:
1. Single-thread enqueue fills batch and triggers enq_global() to copy into RingBuffer
2. GC workers drain the batches from the Ring buffer
3. EagerPeek : copies local batch from Mutators and directly process the RSet inside
*/

#include "../include/babq.h"
#include <cassert>
#include <cstdio>

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

int main()
{
    test_single_batch_flush();
    test_eager_peek();
    return 0;
}