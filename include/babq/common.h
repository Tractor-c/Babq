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