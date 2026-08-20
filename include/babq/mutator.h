/*
- mutator.h — MutatorLocal state + enqueue / flush operations
- entries[] : Non-atomic stores. //Mutator is single writer during filling
- write_index : atomic<uint32_t>
  - Mutator
  - EagerPeek from GC

- enq_global: memcpy local entries → RingBuffer slot (copy-by-value)
*/
#ifndef BABQ_MUTATOR_H
#define BABQ_MUTATOR_H

#include "common.h"
#include "ring.h"
#include <thread>

namespace babq
{
    struct MutatorLocal
    {
        // Fixed local array for batching RSet
        std::atomic<RSetEntry> entries[BATCH_SIZE];
        // Current 'write in' position
        std::atomic<uint32_t> write_index{0};
        // How many batches took the fallback path (ring was FULL).
        uint64_t full_count{0};
    };

    // The global ring shared by all Mutators+ GC;
    // Initialization needed!
    inline SharedRingBuffer &get_global_ring()
    {
        static SharedRingBuffer instance;
        return instance;
    }

    using RSetProcessor = void (*)(RSetEntry entry);

    EnqStatus enq_global(MutatorLocal &self);

    inline void enqueue(MutatorLocal &self, RSetEntry entry, RSetProcessor fallback)
    {
        uint32_t idx = self.write_index.load(); // relaxed?

        // 1. local write
        self.entries[idx].store(entry, std::memory_order_relaxed);

        // 2. publish the new index
        self.write_index.store(idx + 1, std::memory_order_release);

        if (BABQ_UNLIKELY(idx + 1 == BATCH_SIZE))
        {
            switch (enq_global(self))
            {
            case EnqStatus::OK:
                break;

            case EnqStatus::FULL:
                self.full_count++;
                for (uint32_t i = 0; i < BATCH_SIZE; i++)
                {
                    fallback(self.entries[i].load(std::memory_order_relaxed));
                }
                self.write_index.store(0);
                break;
            }
        }
    }

    inline EnqStatus enq_global(MutatorLocal &self)
    {
        SharedRingBuffer &ring = get_global_ring();

        EnqStatus status = ring.enqueue(self.entries, BATCH_SIZE);
        if (status == EnqStatus::OK)
        {
            self.write_index.store(0); // release?
        }
        return status;
    }

    // gc_worker_drain: called by gc worker threads, to consume batch from ring
    inline void gc_worker_drain(RSetProcessor processor)
    {
        SharedRingBuffer &ring = get_global_ring();
        Batch batch;

        while (true)
        {
            DeqStatus status = ring.dequeue(batch);
            switch (status)
            {
            case DeqStatus::OK:
                for (uint32_t i = 0; i < batch.count; i++)
                {
                    processor(batch.entries[i]);
                }
                break;

            case DeqStatus::EMPTY:
                // no more batch need to consumed
                /*NEED TO FIX in integration stage:
                gc thread park ;
                */
                return;

            case DeqStatus::BUSY:
                // retry dequeue directly
                break;
            }
        }
    }

}

#endif
