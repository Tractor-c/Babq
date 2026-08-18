/*
 * eager_peek_test — 上层：EagerPeek 把未满批的本地缓冲复制进全局 ring，
 * 且不推进 mutator 的 write_index（对 mutator 零干扰）。
 */

#include "babq.h"

#include <atomic>
// #include <cassert>
#include "test/babq/babq_check.h"
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

  void test_eager_peek()
  {
    printf("Test: EagerPeek ... ");
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
      babq::enqueue(mutator, entry, test_processor);
    }
    //assert(mutator.write_index.load() == partial);
    BABQ_CHECK_EQ(mutator.write_index.load(), partial);

    // EagerPeek should copy the partial batch into RingBuffer
    babq::eager_peek();

    // Drain and verify
    babq::gc_worker_drain(test_processor);
    // assert(g_processed_count == partial);
    // assert(g_processed_sum == expected_sum);
    BABQ_CHECK_EQ(g_processed_count.load(), partial);
    BABQ_CHECK_EQ(g_processed_sum.load(), expected_sum);

    // Mutator's state should be UNCHANGED (zero interference)
    //assert(mutator.write_index.load() == partial);
    BABQ_CHECK_EQ(mutator.write_index.load(), partial);

    babq::get_mutator_registry().unregister_mutator(&mutator);
    printf("PASSED\n");
  }

} // namespace

int main()
{
  test_eager_peek();
  return 0;
}
