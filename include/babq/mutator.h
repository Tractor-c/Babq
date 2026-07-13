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
        RSetEntry entries[BATCH_SIZE];
        // Current 'write in' position
        std::atomic<uint32_t> write_index{0};
    };

    // The global ring shared by all Mutators+ GC;
    // Initialization needed!
    SharedRingBuffer &get_global_ring()
    {
        static SharedRingBuffer instance;
        return instance;
    }

    void enq_global(MutatorLocal &self);

    void enqueue(MutatorLocal &self, RSetEntry entry)
    {
        uint32_t idx = self.write_index.load(); // relaxed?

        // 1. local write
        self.entries[idx] = entry;

        // 2. publish the new index
        self.write_index.store(idx + 1, std::memory_order_release);

        if (UNLIKELY(idx + 1 == BATCH_SIZE))
        {
            enq_global(self);
        }
    }

    void enq_global(MutatorLocal &self)
    {
        // Prepare the batch for submission
        Batch batch;
        std::memcpy(batch.entries, self.entries, BATCH_SIZE * sizeof(RSetEntry));
        batch.count = BATCH_SIZE;

        SharedRingBuffer &ring = get_global_ring();
        int spin_count = 0;

        while (true)
        {
            EnqStatus status = ring.enqueue(batch);
            switch (status)
            {
            case EnqStatus::OK:
                self.write_index.store(0); // release?
                return;

            case EnqStatus::FULL:
                std::this_thread::yield();
                break;

            case EnqStatus::BUSY:
                // Short Spin then yield;
                if (spin_count < SPIN_LIMIT)
                {
                    spin_count++;
                    cpu_relax();
                }
                else
                {
                    std::this_thread::yield();
                    spin_count = 0;
                }
                break;
            }
        }
    }

    using RSetProcessor = void (*)(RSetEntry entry);

    // gc_worker_drain: called by gc worker threads, to consume batch from ring
    void gc_worker_drain(RSetProcessor processor)
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
                return;

            case DeqStatus::BUSY:
                std::this_thread::yield();
                break;
            }
        }
    }

}

#endif
