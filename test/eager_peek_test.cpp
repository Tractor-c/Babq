/*
 * eager_peek_test — 上层：EagerPeek直接处理读取到的Mutator's unfull batch
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
    BABQ_CHECK_EQ(mutator.write_index.load(), partial);

    // EagerPeek process the batch directly, without enq to global ring
    babq::eager_peek(test_processor);

    BABQ_CHECK_EQ(g_processed_count.load(), partial);
    BABQ_CHECK_EQ(g_processed_sum.load(), expected_sum);

    // Ensure EagerPeek will not enq the global ring: UB
    BABQ_CHECK(babq::get_global_ring().is_drained());
    babq::gc_worker_drain(test_processor);
    BABQ_CHECK_EQ(g_processed_count.load(), partial);

    // Mutator's state should be UNCHANGED (zero interference)
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
