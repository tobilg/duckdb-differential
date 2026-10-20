#pragma once
// CRC32C (Castagnoli), the integrity check of local payload: segment records, objects and local pool
// files. SHA-256 stays the content identity; CRC32C verifies local reads at a small fraction of its
// cost. Hardware instructions where the CPU has them (x86-64 SSE4.2, arm64 CRC), else slicing-by-8.
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#if defined(__x86_64__)
#include <nmmintrin.h>
#elif defined(__aarch64__)
#include <arm_acle.h>
#if defined(__linux__)
#include <sys/auxv.h>
#ifndef HWCAP_CRC32
#define HWCAP_CRC32 (1 << 7)
#endif
#endif
#endif

namespace differential {
namespace crc32c_detail {
constexpr uint32_t POLYNOMIAL = 0x82F63B78u; // reflected Castagnoli
inline const std::array<std::array<uint32_t, 256>, 8> &Tables() {
    static const auto tables = [] {
        std::array<std::array<uint32_t, 256>, 8> t {};
        for (uint32_t i = 0; i < 256; i++) {
            uint32_t c = i;
            for (int k = 0; k < 8; k++)
                c = c & 1 ? (c >> 1) ^ POLYNOMIAL : c >> 1;
            t[0][i] = c;
        }
        for (uint32_t i = 0; i < 256; i++)
            for (int k = 1; k < 8; k++)
                t[k][i] = (t[k - 1][i] >> 8) ^ t[0][t[k - 1][i] & 0xFF];
        return t;
    }();
    return tables;
}
inline uint32_t Software(uint32_t crc, const unsigned char *p, size_t n) {
    auto &t = Tables();
    while (n >= 8) {
        uint64_t word;
        std::memcpy(&word, p, 8);
        word ^= crc;
        crc = t[7][word & 0xFF] ^ t[6][(word >> 8) & 0xFF] ^ t[5][(word >> 16) & 0xFF] ^ t[4][(word >> 24) & 0xFF] ^
              t[3][(word >> 32) & 0xFF] ^ t[2][(word >> 40) & 0xFF] ^ t[1][(word >> 48) & 0xFF] ^ t[0][word >> 56];
        p += 8;
        n -= 8;
    }
    while (n--)
        crc = (crc >> 8) ^ t[0][(crc ^ *p++) & 0xFF];
    return crc;
}
#if defined(__x86_64__)
__attribute__((target("sse4.2"))) inline uint32_t Hardware(uint32_t crc, const unsigned char *p, size_t n) {
    uint64_t c = crc;
    while (n >= 8) {
        uint64_t word;
        std::memcpy(&word, p, 8);
        c = _mm_crc32_u64(c, word);
        p += 8;
        n -= 8;
    }
    uint32_t c32 = uint32_t(c);
    while (n--)
        c32 = _mm_crc32_u8(c32, *p++);
    return c32;
}
inline bool HardwareAvailable() {
    static const bool available = __builtin_cpu_supports("sse4.2");
    return available;
}
#elif defined(__aarch64__)
// GCC spells the AArch64 extension "+crc"; Clang accepts "crc".
#if defined(__clang__)
#define DIFFERENTIAL_TARGET_CRC __attribute__((target("crc")))
#else
#define DIFFERENTIAL_TARGET_CRC __attribute__((target("+crc")))
#endif
DIFFERENTIAL_TARGET_CRC inline uint32_t Hardware(uint32_t crc, const unsigned char *p, size_t n) {
    while (n >= 8) {
        uint64_t word;
        std::memcpy(&word, p, 8);
        crc = __crc32cd(crc, word);
        p += 8;
        n -= 8;
    }
    while (n--)
        crc = __crc32cb(crc, *p++);
    return crc;
}
inline bool HardwareAvailable() {
#if defined(__APPLE__)
    return true; // every Apple arm64 CPU has the CRC extension
#elif defined(__linux__)
    static const bool available = (getauxval(AT_HWCAP) & HWCAP_CRC32) != 0;
    return available;
#else
    return false;
#endif
}
#else
inline uint32_t Hardware(uint32_t crc, const unsigned char *p, size_t n) { return Software(crc, p, n); }
inline bool HardwareAvailable() { return false; }
#endif
} // namespace crc32c_detail

// CRC32C of `n` bytes, continuing from `crc` (0 for a new checksum).
inline uint32_t Crc32c(const void *data, size_t n, uint32_t crc = 0) {
    auto p = static_cast<const unsigned char *>(data);
    crc = ~crc;
    crc = crc32c_detail::HardwareAvailable() ? crc32c_detail::Hardware(crc, p, n) : crc32c_detail::Software(crc, p, n);
    return ~crc;
}
} // namespace differential
