#ifndef BABQ_RING_MUTEX_H
#define BABQ_RING_MUTEX_H

#include "common.h"
#include <mutex>
#include <queue>

namespace babq
{
    class SharedRingBuffer
    {
    public:
        // Producer interface(called by Mutator/EagerPeek)
        // Function: memcpy batch into Ring buffer
        EnqStatus enqueue(const Batch &batch);

        // Consumer interface (called by GC Worker)
        // Function: consume a batch from the Ring Buffer
        DeqStatus dequeue(Batch &out);

        bool is_drained() const;

    private:
        // NEED TO BE REPLACED BY BBQ;
        // Here uses a simple mutex+std::queue for testing first.
        static constexpr size_t MAX_CAPACITY = 1024;
        mutable std::mutex mu_;
        std::queue<Batch> queue_;
    };

    inline EnqStatus SharedRingBuffer::enqueue(const Batch &batch)
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (queue_.size() >= MAX_CAPACITY)
        {
            return EnqStatus::FULL;
        }
        queue_.push(batch); // copies batch into queue
        return EnqStatus::OK;
    }

    inline DeqStatus SharedRingBuffer::dequeue(Batch &out)
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (queue_.empty())
        {
            return DeqStatus::EMPTY;
        }
        out = queue_.front(); // copies batch out
        queue_.pop();
        return DeqStatus::OK;
    }

    inline bool SharedRingBuffer::is_drained() const
    {
        std::lock_guard<std::mutex> lock(mu_);
        return queue_.empty();
    }

}

#endif