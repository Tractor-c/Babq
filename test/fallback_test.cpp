#include "babq.h"

#include "test/babq/babq_check.h"

#include <atomic>
#include <cstdio>

namespace {

constexpr uint32_t RING_BATCHES = babq::NUM_BLOCKS * babq::ENTRIES_PER_BLOCK;

constexpr uint32_t OVERFLOW_BATCHES = 3;

constexpr babq::RSetEntry BASE = 0x1000;

std::atomic<uint64_t> g_fallback_count{0};
std::atomic<uint64_t> g_fallback_sum{0};
std::atomic<uint64_t> g_drained_count{0};
std::atomic<uint64_t> g_drained_sum{0};

void fallback_processor(babq::RSetEntry entry) {
  g_fallback_count.fetch_add(1);
  g_fallback_sum.fetch_add(entry);
}

void drain_processor(babq::RSetEntry entry) {
  g_drained_count.fetch_add(1);
  g_drained_sum.fetch_add(entry);
}

void test_fallback_on_full() {
  printf("Test: fallback on ring FULL ... ");

  babq::MutatorLocal mutator{};

  const uint64_t total =
      static_cast<uint64_t>(RING_BATCHES + OVERFLOW_BATCHES) * babq::BATCH_SIZE;

  uint64_t expected_sum = 0;
  for (uint64_t i = 0; i < total; i++) {
    const babq::RSetEntry entry = BASE + static_cast<babq::RSetEntry>(i);
    expected_sum += entry;
    babq::enqueue(mutator, entry, fallback_processor);
  }

  BABQ_CHECK_EQ(mutator.write_index.load(), 0u);

  BABQ_CHECK_EQ(mutator.full_count, static_cast<uint64_t>(OVERFLOW_BATCHES));
  BABQ_CHECK_EQ(g_fallback_count.load(),
                static_cast<uint64_t>(OVERFLOW_BATCHES) * babq::BATCH_SIZE);

  babq::gc_worker_drain(drain_processor);
  BABQ_CHECK_EQ(g_drained_count.load(),
                static_cast<uint64_t>(RING_BATCHES) * babq::BATCH_SIZE);

  BABQ_CHECK_EQ(g_fallback_count.load() + g_drained_count.load(), total);
  BABQ_CHECK_EQ(g_fallback_sum.load() + g_drained_sum.load(), expected_sum);

  BABQ_CHECK(babq::get_global_ring().is_drained());

  printf("PASSED (%u batches via ring, %u via fallback)\n", RING_BATCHES,
         OVERFLOW_BATCHES);
}

void test_recovers_after_drain() {
  printf("Test: enqueue recovers after drain ... ");

  babq::MutatorLocal mutator{};
  const uint64_t before = g_fallback_count.load();

  for (uint32_t i = 0; i < babq::BATCH_SIZE; i++) {
    babq::enqueue(mutator, BASE + i, fallback_processor);
  }

  BABQ_CHECK_EQ(mutator.full_count, 0u);
  BABQ_CHECK_EQ(g_fallback_count.load(), before);
  BABQ_CHECK_EQ(mutator.write_index.load(), 0u);

  babq::gc_worker_drain(drain_processor);
  BABQ_CHECK(babq::get_global_ring().is_drained());

  printf("PASSED\n");
}

} // namespace

int main() {
  test_fallback_on_full();
  test_recovers_after_drain();
  return 0;
}
