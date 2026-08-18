// common.h — Core types, constants, and enumerations

#ifndef BABQ_COMMON_H
#define BABQ_COMMON_H

#include <atomic>
#include <cstdint>
#include <cstddef>
#include <cstring>

namespace babq
{
    static constexpr size_t CACHELINE_SIZE = 64;

    // Number of RSet entries per Batch.
    static constexpr uint32_t BATCH_SIZE = 64;

    // Max spin iterations before yielding on BUSY status.
    inline constexpr int SPIN_LIMIT = 32;

    using RSetEntry = uintptr_t;

    //  Batch: unit of data exchanged via the RingBuffer
    struct alignas(CACHELINE_SIZE) Batch
    {
        RSetEntry entries[BATCH_SIZE];
        uint32_t count{0};
    };

    enum class EnqStatus
    {
        OK,   // Successfully enqueued
        FULL, // RingBuffer is full (GC consumers too slow)
    };

    enum class DeqStatus
    {
        OK,    // Successfully dequeued
        EMPTY, // RingBuffer is empty (no data to consume)
        BUSY   // Temporary contention; retry directly
    };

    inline void cpu_relax()
    {
#if defined(__x86_64__) || defined(_M_X64)
        __asm__ volatile("pause" ::: "memory");
#elif defined(__aarch64__)
        __asm__ volatile("yield" ::: "memory");
#else
        // Fallback: compiler fence
        std::atomic_signal_fence(std::memory_order_seq_cst);
#endif
    }

    // #if defined(__GNUC__) || defined(__clang__)
    // #define LIKELY(x) __builtin_expect(!!(x), 1)
    // #define UNLIKELY(x) __builtin_expect(!!(x), 0)
    // #else
    // #define LIKELY(x) (x)
    // #define UNLIKELY(x) (x)
    // #endif

#if defined(__GNUC__) || defined(__clang__)
#define BABQ_LIKELY(x) __builtin_expect(!!(x), 1)
#define BABQ_UNLIKELY(x) __builtin_expect(!!(x), 0)
#else
#define BABQ_LIKELY(x) (x)
#define BABQ_UNLIKELY(x) (x)
#endif

}

#endif // BABQ_COMMON_H