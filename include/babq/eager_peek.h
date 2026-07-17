#ifndef BABQ_EAGER_PEEK_H
#define BABQ_EAGER_PEEK_H

#include "common.h"
#include "ring_mpmc.h"
#include "mutator.h"
#include <algorithm>
#include <vector>
#include <mutex>

namespace babq
{
    // Later replaced by MutatorManager::Instance().VisitAllMutators in mutator_manager.h
    class MuatatorRegistry
    {
    public:
        void register_mutator(MutatorLocal *m)
        {
            std::lock_guard<std::mutex> lock(mu_);
            mutators_.push_back(m);
        }

        void unregister_mutator(MutatorLocal *m)
        {
            std::lock_guard<std::mutex> lock(mu_);
            mutators_.erase(
                std::remove(mutators_.begin(), mutators_.end(), m),
                mutators_.end());
        }

        template <typename Visitor>
        void visit_all(Visitor &&visitor)
        {
            std::lock_guard<std::mutex> lock(mu_);
            for (auto *m : mutators_)
            {
                visitor(*m);
            }
        }

    private:
        std::mutex mu_;
        std::vector<MutatorLocal *> mutators_;
    };

    inline MuatatorRegistry &get_mutator_registry()
    {
        static MuatatorRegistry instance;
        return instance;
    }

    inline void peek_one_mutator(SharedRingBuffer &ring, MutatorLocal &m)
    {
        // 1. Snapshot of the current size of Mutator's entries
        uint32_t snapshot_index = m.write_index.load(); // acquire
        if (snapshot_index == 0)
        {
            return;
        }

        // 2.Memcpy the Mutator's entries into a new Batch
        Batch copied_batch;
        std::memcpy(copied_batch.entries, m.entries, snapshot_index * sizeof(RSetEntry));
        copied_batch.count = snapshot_index;

        // 3. Sumbit the copy to Ring; same in the enq_global()
        int spin_count = 0;
        while (true)
        {
            EnqStatus status = ring.enqueue(copied_batch);
            if (status == EnqStatus::OK)
            {
                break;
            }
            else if (status == EnqStatus::FULL)
            {
                std::this_thread::yield();
            }
            else
            {
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
            }
        }
    }

    inline void eager_peek()
    {
        SharedRingBuffer &ring = get_global_ring();
        get_mutator_registry().visit_all([&](MutatorLocal &m)
                                         { peek_one_mutator(ring, m); });
    }

    /*[used in STW] */

    // Read and clear a mutator's buffer
    inline void drain_one_mutator(MutatorLocal &m, RSetProcessor processor)
    {
        uint32_t idx = m.write_index.load();
        for (uint32_t i = 0; i < idx; i++)
        {
            processor(m.entries[i]);
        }
        m.write_index.store(0);
    }

}

#endif