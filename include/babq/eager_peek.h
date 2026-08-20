#ifndef BABQ_EAGER_PEEK_H
#define BABQ_EAGER_PEEK_H

#include "common.h"
#include "ring.h"
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

    inline void peek_one_mutator(MutatorLocal &m, RSetProcessor processor)
    {
        uint32_t snapshot_index = m.write_index.load(); // acquire

        for (uint32_t i = 0; i < snapshot_index; i++)
        {
            processor(m.entries[i].load(std::memory_order_relaxed));
        }
    }

    inline void eager_peek(RSetProcessor processor)
    {
        get_mutator_registry().visit_all([processor](MutatorLocal &m)
                                         { peek_one_mutator(m, processor); });
    }

    /*[used in STW] */
    // Read and clear a mutator's buffer
    inline void drain_one_mutator(MutatorLocal &m, RSetProcessor processor)
    {
        uint32_t idx = m.write_index.load();
        for (uint32_t i = 0; i < idx; i++)
        {
            processor(m.entries[i].load(std::memory_order_relaxed));
        }
        m.write_index.store(0);
    }

}

#endif