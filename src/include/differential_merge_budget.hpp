#pragma once
#include <atomic>
#include <cstdint>
#include <stdexcept>

namespace differential {
// Shared by the staging main file and every sidecar. Count attempted writes (including
// overwrites) before they enter the byte store. This is not a physical repository quota:
// journal/SQLite overhead and DuckDB temporary files have separate resource accounting.
struct MergeWriteBudget {
    explicit MergeWriteBudget(uint64_t limit) : limit(limit) {}
    const uint64_t limit;
    std::atomic<uint64_t> used{0};
    void Reserve(uint64_t bytes) {
        auto current = used.load();
        do {
            if (bytes > limit - current)
                throw std::runtime_error("[differential:merge_limit] Staging write budget exceeded");
        } while (!used.compare_exchange_weak(current, current + bytes));
    }
};
} // namespace differential
