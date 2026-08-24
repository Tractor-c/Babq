/*
 * Coverage:
 * mutator Layer:  babq::enqueue / enq_global / gc_worker_drain / drain_one_mutator
 * Ring layer: Full / advance to next block / Version upgrade ->tested in ing_basic_test
 */

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

  // SPSC+ FIFO ring
  // g_seen: base, base+1, ..., base+count-1;entries been processed sequencially
  void check_seen(babq::RSetEntry base, uint64_t count)
  {
    BABQ_CHECK_EQ(g_seen.size(), count);

    for (uint64_t i = 0; i < count; i++)
    {
      const babq::RSetEntry expected = base + static_cast<babq::RSetEntry>(i);
      BABQ_CHECK_EQ(g_seen[i], expected);
    }
  }

  void check_one_count(uint32_t n, babq::RSetEntry base)
  {
    reset_world();
    babq::MutatorLocal mutator{};
    babq::get_mutator_registry().register_mutator(&mutator);

    for (uint32_t i = 0; i < n; i++)
    {
      babq::enqueue(mutator, base + i, recording_processor);
    }

    // check batch's current size
    BABQ_CHECK_EQ(mutator.write_index.load(), n % babq::BATCH_SIZE);

    // ensure no fallback process
    BABQ_CHECK_EQ(mutator.full_count, 0u);

    // exit1: Processed by gc worker via ring.
    babq::gc_worker_drain(recording_processor);
    const uint64_t via_ring = (n / babq::BATCH_SIZE) * babq::BATCH_SIZE;
    check_seen(base, via_ring);

    // exit 2: processed by 'drain_one_mutator' when n < BATCH_SIZE
    babq::drain_one_mutator(mutator, recording_processor);
    BABQ_CHECK_EQ(mutator.write_index.load(), 0u);

    check_seen(base, n);

    BABQ_CHECK(babq::get_global_ring().is_drained());
    babq::get_mutator_registry().unregister_mutator(&mutator);
  }

  void test_entry_counts()
  {
    printf("Test: entry-count boundaries around BATCH_SIZE ... ");
    const uint32_t counts[] = {0, 1, babq::BATCH_SIZE - 1, babq::BATCH_SIZE,
                               babq::BATCH_SIZE + 1, 2 * babq::BATCH_SIZE - 1,
                               2 * babq::BATCH_SIZE};

    babq::RSetEntry base = 0x10000;
    constexpr babq::RSetEntry STRIDE = 0x10000;

    for (uint32_t n : counts)
    {
      check_one_count(n, base);
      base += STRIDE;
    }
  }

  // Test: trigger 'advance to next block once'
  void test_block_crossing_at_mutator_level()
  {
    printf("Test: block crossing at mutator level ... ");
    reset_world();

    // to trigger->advance to next block once
    constexpr uint32_t BATCHES = babq::ENTRIES_PER_BLOCK + 1;
    static_assert(BATCHES <= babq::ENTRIES_PER_BLOCK * babq::NUM_BLOCKS,
                  "must stay inside ring capacity, otherwise this tests fallback");
    const uint64_t total_items = static_cast<uint64_t>(BATCHES) * babq::BATCH_SIZE;

    constexpr babq::RSetEntry BASE = 0x200000;

    babq::MutatorLocal mutator{};
    babq::get_mutator_registry().register_mutator(&mutator);

    for (uint64_t i = 0; i < total_items; i++)
    {
      babq::enqueue(mutator, BASE + static_cast<babq::RSetEntry>(i),
                    recording_processor);
    }

    BABQ_CHECK_EQ(mutator.write_index.load(), 0u);
    BABQ_CHECK_EQ(mutator.full_count, 0u);

    babq::gc_worker_drain(recording_processor);
    check_seen(BASE, total_items);

    BABQ_CHECK(babq::get_global_ring().is_drained());
    babq::get_mutator_registry().unregister_mutator(&mutator);

    printf("PASSED (%u batches = %llu entries, 1 block advance)\n", BATCHES,
           static_cast<unsigned long long>(total_items));
  }
}

int main()
{
  test_entry_counts();
  test_block_crossing_at_mutator_level();
  return 0;
}
