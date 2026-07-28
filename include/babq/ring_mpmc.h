#ifndef BABQ_RING_MPMC_H
#define BABQ_RING_MPMC_H

#include "common.h"
#include <cstring>

namespace babq
{

    /* BBQ configuration */

    // Number of blocks = 2^NUM_BLOCKS_LOG = 8
    static constexpr uint32_t NUM_BLOCKS_LOG = 3;
    static constexpr uint32_t NUM_BLOCKS = 1u << NUM_BLOCKS_LOG; // 0000 0001 -> 0000 1000
    static constexpr uint32_t BLOCK_IDX_MASK = NUM_BLOCKS - 1;

    // Number of Batch entries per block
    static constexpr uint32_t ENTRIES_PER_BLOCK = 8;

    // cursor structure(64 bits): [version (high bits) | local_offset (low 16 bits -- MAX65535) ]

    static constexpr uint32_t LOCAL_SPACE_BITS = 16;
    static constexpr uint64_t LOCAL_SPACE_MASK = (1ULL << LOCAL_SPACE_BITS) - 1; // low bits 16 *1

    static inline uint64_t cursor_version(uint64_t cursor)
    {
        return cursor >> LOCAL_SPACE_BITS;
    }

    static inline uint64_t cursor_local(uint64_t cursor)
    {
        return cursor & LOCAL_SPACE_MASK;
    }

    static inline uint64_t cursor_compose(uint64_t version, uint64_t local)
    {
        return (version << LOCAL_SPACE_BITS | local);
    }

    /* Block : one segment of the ring buffer */

    static constexpr uint64_t BLOCK_INIT_LOCAL = ENTRIES_PER_BLOCK;

    struct Block
    {
        // Producer cursors
        alignas(CACHELINE_SIZE) std::atomic<uint64_t> allocated{0};
        alignas(CACHELINE_SIZE) std::atomic<uint64_t> committed{0};

        // Consumer cursors
        alignas(CACHELINE_SIZE) std::atomic<uint64_t> reserved{0};
        alignas(CACHELINE_SIZE) std::atomic<uint64_t> consumed{0};

        // Data storage
        alignas(CACHELINE_SIZE) Batch entries[ENTRIES_PER_BLOCK]; //! refer to def of Batch: also align
    };

    /* SharedRingBuffer: MPMC ring buffer using BBQ */

    class SharedRingBuffer
    {
    public:
        SharedRingBuffer() { init(); }

        // Producer interface(called by Mutator/EagerPeek)
        // Function: memcpy batch into Ring buffer
        EnqStatus enqueue(const Batch &batch);

        // Consumer interface (called by GC Worker)
        // Function: consume a batch from the Ring Buffer
        DeqStatus dequeue(Batch &out);

        size_t size() const;

    private:
        void init();
        alignas(CACHELINE_SIZE) std::atomic<uint64_t> widx_{0};
        alignas(CACHELINE_SIZE) std::atomic<uint64_t> ridx_{0};
        alignas(CACHELINE_SIZE) Block blocks_[NUM_BLOCKS];

        // Check if a block has been fully consumed for a given version
        bool block_fully_consumed(Block &blk, uint64_t version);

        // Reset a block cursor to a new version with init value
        static void reset_cursor(std::atomic<uint64_t> &cursor, uint64_t new_version, uint64_t init_local);

        // Attempt to advance a global index via CAS
        void try_advance(std::atomic<uint64_t> &head, uint64_t expected);
    };

    inline void SharedRingBuffer::init()
    {
        // Block 0 is the first active block; cursors start at compose(version=0, local=0)
        uint64_t first_block_init = cursor_compose(0, 0);
        blocks_[0].allocated.store(first_block_init);
        blocks_[0].committed.store(first_block_init);
        blocks_[0].reserved.store(first_block_init);
        blocks_[0].consumed.store(first_block_init);

        // all other blocks initialized to invalid state -- 'local offset' > ENTRIES_PER_BLOCK, so appear 'full/unusable'
        for (uint32_t i = 1; i < NUM_BLOCKS; i++)
        {
            uint64_t init_val = cursor_compose(0, BLOCK_INIT_LOCAL);
            blocks_[i].allocated.store(init_val);
            blocks_[i].committed.store(init_val);
            blocks_[i].reserved.store(init_val);
            blocks_[i].consumed.store(init_val);
        }
    }

    // Reset a block cursor to a new version with init value
    inline void SharedRingBuffer::reset_cursor(std::atomic<uint64_t> &cursor, uint64_t new_version, uint64_t init_local)
    {
        uint64_t new_val = cursor_compose(new_version, init_local);
        uint64_t old_val = cursor.load();
        while (old_val < new_val)
        {
            if (cursor.compare_exchange_weak(old_val, new_val))
            {
                break;
            }
        }
    }

    // Attempt to advance a global index via CAS
    inline void SharedRingBuffer::try_advance(std::atomic<uint64_t> &head, uint64_t expected)
    {
        head.compare_exchange_strong(expected, expected + 1);
    }

    // Check if a block has been fully consumed for a given version
    inline bool SharedRingBuffer::block_fully_consumed(Block &blk, uint64_t version)
    {
        uint64_t consumed_val = blk.consumed.load();
        uint64_t consumed_local = cursor_local(consumed_val);
        uint64_t consumed_version = cursor_version(consumed_val);
        return (consumed_local == ENTRIES_PER_BLOCK && consumed_version == version) || consumed_version > version;
    }

    inline EnqStatus SharedRingBuffer::enqueue(const Batch &batch)
    {
        // 1. Read the current write block index
        uint64_t widx = widx_.load(); // relaxed
        uint32_t block_idx = static_cast<uint32_t>(widx) & BLOCK_IDX_MASK;

        Block &blk = blocks_[block_idx];

        // 2. Precheck "allocated" if this block is full
        uint64_t allocated_val = blk.allocated.load();
        uint64_t allocated_local = cursor_local(allocated_val);

        // 2.1 if current block is not full
        /*NOTE：if detected , this block is currently been written, consumer will exit. [achieved on consumer side]*/
        if (BABQ_LIKELY(allocated_local < ENTRIES_PER_BLOCK))
        {
            // 3. FAA to claim a slot
            uint64_t old_allocated = blk.allocated.fetch_add(1);
            uint64_t old_local = cursor_local(old_allocated);

            if (BABQ_LIKELY(old_local < ENTRIES_PER_BLOCK))
            {
                // 4. If a valid slot claimed, memcpy the Batch in
                std::memcpy(&blk.entries[old_local], &batch, sizeof(Batch));

                // 5. FAA on committed
                blk.committed.fetch_add(1);
                return EnqStatus::OK;
            }
        }

        // 2.2 if (failed FAA | current block is full) , ->slow path: advance to next block
        uint32_t next_block_idx = (block_idx + 1) & BLOCK_IDX_MASK;
        Block &next_blk = blocks_[next_block_idx];

        // the version when Enqueue was triggered
        uint64_t snapshot_verison = widx >> NUM_BLOCKS_LOG;

        // 3. Check whether next block is ready to write in
        if (BABQ_UNLIKELY(!(block_fully_consumed(next_blk, snapshot_verison))))
        {
            return EnqStatus::FULL;
        }

        uint64_t new_version = snapshot_verison + 1;
        reset_cursor(next_blk.committed, new_version, 0);
        reset_cursor(next_blk.allocated, new_version, 0);

        // 4. Try to advance the global write index
        try_advance(widx_, widx);

        return EnqStatus::BUSY;
    }

    inline DeqStatus SharedRingBuffer::dequeue(Batch &out)
    {
        // 1.read current read block index
        uint64_t ridx = ridx_.load();
        uint32_t block_idx = static_cast<uint32_t>(ridx) & BLOCK_IDX_MASK;
        Block &blk = blocks_[block_idx];

        // 2. Check if block is fully reserved by consumers
        uint64_t reserved_val = blk.reserved.load();
        uint64_t reserved_local = cursor_local(reserved_val);

        // 2.1 current block is not fully reserved
        if (BABQ_LIKELY(reserved_local < ENTRIES_PER_BLOCK))
        {
            // 3. Check committed cursor of producers
            uint64_t committed_val = blk.committed.load();
            uint64_t committed_local = cursor_local(committed_val);

            if (BABQ_UNLIKELY(reserved_local >= committed_local))
            {
                return DeqStatus::EMPTY;
            }

            // 4. Check if all allocated slots are committed (NO producer still writing)
            if (BABQ_UNLIKELY(committed_local != ENTRIES_PER_BLOCK))
            {
                uint64_t allocated_val = blk.allocated.load();
                uint64_t allocated_local = cursor_local(allocated_val);
                if (BABQ_LIKELY(allocated_local != committed_local))
                {
                    // A producer is still writing
                    return DeqStatus::BUSY;
                }
            }

            // 5. CAS to claim next slot: competition between consumers
            uint64_t desired = reserved_val + 1;
            if (!blk.reserved.compare_exchange_strong(reserved_val, desired))
            {
                return DeqStatus::BUSY; // Another consumer succeed
            }

            // 6. Copy the batch out
            std::memcpy(&out, &blk.entries[reserved_local], sizeof(Batch));

            // 7. consumed cursor proceed
            blk.consumed.fetch_add(1);
            return DeqStatus::OK;
        }

        // 2.2 Slow path: current block is fully reserved, try advance to next block
        uint32_t next_block_idx = (block_idx + 1) & BLOCK_IDX_MASK;
        Block &next_blk = blocks_[next_block_idx];

        // 3. version check: ensure next block has been written by producers
        uint64_t consumer_version = cursor_version(reserved_val) - (block_idx != 0 ? 1 : 0);
        uint64_t producer_version = cursor_version(next_blk.committed.load());
        if (producer_version != consumer_version + 1)
        {
            return DeqStatus::EMPTY; // Next block not ready
        }

        // 4. Reset the next block's consumer cursors
        reset_cursor(next_blk.consumed, consumer_version + 1, 0);
        reset_cursor(next_blk.reserved, consumer_version + 1, 0);

        // 5. try to advance the global read idx
        try_advance(ridx_, ridx);

        return DeqStatus::BUSY; // Caller should retry
    }

    inline size_t SharedRingBuffer::size() const
    {
        uint64_t w = widx_.load();
        uint64_t r = ridx_.load();
        return static_cast<size_t>((w - r) * ENTRIES_PER_BLOCK);
    }

} // namespace babq

#endif // BABQ_RING_MPMC_H