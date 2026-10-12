#pragma once
// Fault-injection and diagnostic probes. They are compiled only into test builds
// (-DDIFFERENTIAL_TEST_HOOKS=ON); release artifacts contain no environment-driven
// crash points, gates or raw-path probes.
#include <cstdlib>
#include <string>
#include <unistd.h>

namespace differential {
#ifdef DIFFERENTIAL_TEST_HOOKS
constexpr bool test_hooks = true;
// Read on every call: tests set variables in forked children after the parent used the store.
inline const char *Hook(const char *variable) { return std::getenv(variable); }
inline void Failpoint(const char *variable, const char *point) {
    auto selected = std::getenv(variable);
    if (selected && std::string(selected) == point)
        _exit(86);
}
#else
constexpr bool test_hooks = false;
inline const char *Hook(const char *) { return nullptr; }
inline void Failpoint(const char *, const char *) {}
#endif
} // namespace differential
