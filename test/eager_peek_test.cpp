#include "babq.h"
#include "test/babq/babq_check.h"

#include <atomic>
#include <cstdio>
#include <vector>

namespace
{
  std::vector<babq::RSetEntry> g_seen;
  void recording_processor(babq::RSetEntry entry) { g_seen.push_back(entry); }

  void reset_world()
  {
    babq::gc_worker_drain(recording_processor);
    BABQ_CHECK(babq::get_global_ring().is_drained());
    g_seen.clear();
  }

  uint64_t count_of(babq::RSetEntry entry)
  {
    uint64_t n = 0;
    for (babq::RSetEntry e : g_seen)
    {
      if (e == entry)
      {
        n++;
      }
    }
    return n;
  }

  constexpr babq::RSetEntry BASE1 = 0x10000;
  constexpr babq::RSetEntry BASE2 = 0x20000;
  constexpr babq::RSetEntry BASE3 = 0x30000;
  constexpr babq::RSetEntry BASE4 = 0x40000;

  constexpr uint32_t PARTIAL = babq::BATCH_SIZE / 2;
  static_assert(PARTIAL > 0 && PARTIAL < babq::BATCH_SIZE);

  // test 1: eagerPeek reads Mutator's partial batch with no interference on original batch
  void test_peek_zero_interference()
  {
    printf("Test: eagerPeek reads Mutator's partial batch with no interference");
    reset_world();

    babq::MutatorLocal mutator{};
    babq::get_mutator_registry().register_mutator(&mutator);

    for (uint32_t i = 0; i < PARTIAL; i++)
    {
      babq::enqueue(mutator, BASE1 + i, recording_processor);
    }
    BABQ_CHECK_EQ(mutator.write_index.load(), PARTIAL);

    babq::eager_peek(recording_processor);

    BABQ_CHECK_EQ(g_seen.size(), static_cast<size_t>(PARTIAL));
    for (uint32_t i = 0; i < PARTIAL; i++)
    {
      BABQ_CHECK_EQ(g_seen[i], BASE1 + i);
    }

    BABQ_CHECK_EQ(mutator.write_index.load(), PARTIAL);

    babq::drain_one_mutator(mutator, recording_processor);
    babq::get_mutator_registry().unregister_mutator(&mutator);
    printf("PASSED\n");
  }

  // Test 2: [at-least-once] eagerpeek do not advance write_index; same entry processed multiple times
  void test_at_least_once()
  {
    printf("Test: at-least-once. duplicates are by design.");
    reset_world();
    babq::MutatorLocal mutator{};
    babq::get_mutator_registry().register_mutator(&mutator);

    for (uint32_t i = 0; i < PARTIAL; i++)
    {
      babq::enqueue(mutator, BASE2 + i, recording_processor);
    }

    // 1. 2times eagerPeek
    babq::eager_peek(recording_processor);
    babq::eager_peek(recording_processor);
    BABQ_CHECK_EQ(g_seen.size(), static_cast<size_t>(2 * PARTIAL));
    BABQ_CHECK_EQ(count_of(BASE2), 2u);

    // 2. Fill the partial batch to trigger flush
    for (uint32_t i = PARTIAL; i < babq::BATCH_SIZE; i++)
    {
      babq::enqueue(mutator, BASE2 + i, recording_processor);
    }
    BABQ_CHECK_EQ(mutator.write_index.load(), 0u);
    BABQ_CHECK_EQ(mutator.full_count, 0u);

    babq::gc_worker_drain(recording_processor);
    BABQ_CHECK_EQ(g_seen.size(), static_cast<size_t>(2 * PARTIAL + babq::BATCH_SIZE));

    // first half: 2*peek+1*ring; second half: 1*ringProcess
    BABQ_CHECK_EQ(count_of(BASE2), 3u);
    BABQ_CHECK_EQ(count_of(BASE2 + PARTIAL), 1u);

    BABQ_CHECK(babq::get_global_ring().is_drained());
    babq::get_mutator_registry().unregister_mutator(&mutator);
    printf("PASSED\n");
  }

  // Test 3:avoid process entries[0] under 'i <= write_index' in peek_one_mutator()
  void test_peek_empty_batch()
  {
    printf("Test: eagerPeek empty Batch is no-op");
    reset_world();

    babq::MutatorLocal mutator{};
    babq::get_mutator_registry().register_mutator(&mutator);

    BABQ_CHECK_EQ(mutator.write_index.load(), 0u);
    babq::eager_peek(recording_processor);
    BABQ_CHECK_EQ(g_seen.size(), 0u);

    for (uint32_t i = 0; i < PARTIAL; i++)
    {
      babq::enqueue(mutator, BASE3 + i, recording_processor);
    }
    babq::drain_one_mutator(mutator, recording_processor);
    BABQ_CHECK_EQ(mutator.write_index.load(), 0u);
    g_seen.clear();

    babq::eager_peek(recording_processor);
    BABQ_CHECK_EQ(g_seen.size(), 0u);

    babq::get_mutator_registry().unregister_mutator(&mutator);
    printf("PASSED\n");
  }

  // Test 4: eagerPeek should never enq batch to ring;
  void test_peek_not_enq()
  {
    printf("Test: peek leaves the ring untouched ... ");
    reset_world();

    babq::MutatorLocal mutator{};
    babq::get_mutator_registry().register_mutator(&mutator);

    for (uint32_t i = 0; i < babq::BATCH_SIZE; i++)
    {
      babq::enqueue(mutator, BASE4 + i, recording_processor);
    }
    BABQ_CHECK_EQ(mutator.write_index.load(), 0u);

    for (uint32_t i = 0; i < PARTIAL; i++)
    {
      babq::enqueue(mutator, BASE4 + babq::BATCH_SIZE + i, recording_processor);
    }

    babq::eager_peek(recording_processor);
    g_seen.clear();

    // gc_worker_drain : only process the first batch entered the ring
    babq::gc_worker_drain(recording_processor);
    BABQ_CHECK_EQ(g_seen.size(), static_cast<size_t>(babq::BATCH_SIZE));
    for (uint32_t i = 0; i < babq::BATCH_SIZE; i++)
    {
      BABQ_CHECK_EQ(g_seen[i], BASE4 + i);
    }

    babq::drain_one_mutator(mutator, recording_processor);
    BABQ_CHECK(babq::get_global_ring().is_drained());
    babq::get_mutator_registry().unregister_mutator(&mutator);
    printf("PASSED\n");
  }

}

int main()
{
  test_peek_zero_interference();
  test_at_least_once();
  test_peek_empty_batch();
  test_peek_not_enq();
  return 0;
}
