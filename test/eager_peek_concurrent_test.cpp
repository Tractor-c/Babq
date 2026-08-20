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
  constexpr uint32_t NUM_DRAINERS = 2;

  constexpr babq::RSetEntry BASE = 0x1000;  // entry = BASE + seq 

  uint64_t g_total = 0;  
  std::unique_ptr<std::atomic<uint8_t>[]> g_auth_seen;

  std::atomic<uint64_t> g_peek_ok{0};  
  std::atomic<uint64_t> g_peek_bad{0}; 
  std::atomic<uint64_t> g_full_count{0}; 
  std::atomic<bool> g_producer_done{false}; 

  std::atomic<uint64_t> g_peek_passes{0}; 
  std::atomic<uint64_t> g_peek_crossing{0}; 

  thread_local babq::RSetEntry t_peek_last = 0;
  thread_local bool t_peek_has_last = false;

  // main path: gc_worker_drain ; fallback when FULL ; drain_one_mutator in STW 
  void auth_processor(babq::RSetEntry entry)
  {
    BABQ_CHECK(entry >= BASE);
    const uint64_t seq = static_cast<uint64_t>(entry - BASE);
    BABQ_CHECK(seq < g_total);
    g_auth_seen[seq].fetch_add(1, std::memory_order_relaxed);
  }

  // peek path: duplicate process of entry
  void peek_processor(babq::RSetEntry entry)
  {
    if (entry < BASE || entry >= BASE + g_total)
    {
      g_peek_bad.fetch_add(1, std::memory_order_relaxed);
      return;
    }

    // normally value of seq increase. If not, crossed overwrite detected
    if (t_peek_has_last && entry <= t_peek_last)
    {
      //entries[0..] are overwritten since current entry is smallerthan last entry
      g_peek_crossing.fetch_add(1, std::memory_order_relaxed);
    }
    t_peek_last = entry;
    t_peek_has_last = true;

    g_peek_ok.fetch_add(1, std::memory_order_relaxed);
  }

  uint64_t resolve_item_count()
  {
    uint64_t items = 500000;
    if (const char *env = std::getenv("BABQ_PEEK_ITEMS"))
    {
      const long long parsed = std::atoll(env);
      if (parsed > 0)
      {
        items = static_cast<uint64_t>(parsed);
      }
    }

    return items;
  }

  void test_eager_peek_concurrent()
  {
    g_total = resolve_item_count();

    printf("Test: EagerPeek concurrent with mutator writes "
           "(%u drainers, %llu items) ... ",
           NUM_DRAINERS, static_cast<unsigned long long>(g_total));
    fflush(stdout);

    g_auth_seen.reset(new std::atomic<uint8_t>[g_total]());

    const auto started = std::chrono::steady_clock::now();

    //gc consumer threads : invoke gc_worker_drain
    std::vector<std::thread> drainers;
    for (uint32_t i = 0; i < NUM_DRAINERS; i++)
    {
      drainers.emplace_back([]()
                            {
      while (!g_producer_done.load(std::memory_order_acquire)) {
        babq::gc_worker_drain(auth_processor);
        std::this_thread::yield();
      }
      babq::gc_worker_drain(auth_processor); });
    }

    //used to trigger eager_peek
    std::thread peeker([]()
                       {
    while (!g_producer_done.load(std::memory_order_acquire)) {
      t_peek_has_last = false;                    //avoid the first entry of batch
      babq::eager_peek(peek_processor);
      g_peek_passes.fetch_add(1, std::memory_order_relaxed);
    } });

    //Mutator: enq locally. auth_processsor used as fallback when ring is full
    std::thread producer([]()
                         {
    babq::MutatorLocal mutator{};
    babq::get_mutator_registry().register_mutator(&mutator);

    for (uint64_t seq = 0; seq < g_total; seq++) {
      babq::enqueue(mutator, BASE + static_cast<babq::RSetEntry>(seq),
                    auth_processor);
    }

    //Clear Mutator's local batch --"process directly"。
    babq::drain_one_mutator(mutator, auth_processor);
    BABQ_CHECK_EQ(mutator.write_index.load(), 0u);

    g_full_count.store(mutator.full_count); 

    babq::get_mutator_registry().unregister_mutator(&mutator); });

    producer.join();
    g_producer_done.store(true, std::memory_order_release);

    peeker.join();
    for (auto &t : drainers)
    {
      t.join();
    }

    babq::gc_worker_drain(auth_processor);

    const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - started).count();

    // Assert: No lost or duplication on main process path. 
    uint64_t lost = 0;
    uint64_t duplicated = 0;
    int shown = 0;
    for (uint64_t seq = 0; seq < g_total; seq++)
    {
      const uint8_t seen = g_auth_seen[seq].load(std::memory_order_relaxed);
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
      if (shown < 10)
      {
        fprintf(stderr, "\n  seq %llu (entry 0x%llx) auth-processed %u time(s)",
                static_cast<unsigned long long>(seq),
                static_cast<unsigned long long>(BASE + seq),
                static_cast<unsigned>(seen));
        shown++;
      }
    }
    if (lost != 0 || duplicated != 0)
    {
      fprintf(stderr,
              "\n  authoritative path: %llu lost, %llu duplicated (of %llu)\n",
              static_cast<unsigned long long>(lost),
              static_cast<unsigned long long>(duplicated),
              static_cast<unsigned long long>(g_total));
    }
    BABQ_CHECK_EQ(lost, 0u);
    BABQ_CHECK_EQ(duplicated, 0u);

    BABQ_CHECK_EQ(g_peek_bad.load(), 0u);

    BABQ_CHECK(g_peek_ok.load() > 0);

    BABQ_CHECK(babq::get_global_ring().is_drained());

    const uint64_t crossing = g_peek_crossing.load();
    const uint64_t passes = g_peek_passes.load();
    const double crossing_rate =
        (passes == 0) ? 0.0 : (100.0 * static_cast<double>(crossing) /
                               static_cast<double>(passes));

    printf("PASSED (%llu ms | peek: %llu entries in %llu passes, "
           "crossings %llu/%llu = %.3f%% | fallback: %llu batches)\n",
           static_cast<unsigned long long>(elapsed),
           static_cast<unsigned long long>(g_peek_ok.load()),
           static_cast<unsigned long long>(passes),
           static_cast<unsigned long long>(crossing),
           static_cast<unsigned long long>(passes), crossing_rate,
           static_cast<unsigned long long>(g_full_count.load()));

    if (crossing == 0)
    {
      fprintf(stderr,
              "NOTE: Targeted interaction not simulated. \n"
              "Increase BABQ_PEEK_ITEMS\n");
    }
  }

} 

int main()
{
  test_eager_peek_concurrent();
  return 0;
}
