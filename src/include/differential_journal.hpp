#pragma once
// Per-process, unsynced append-only journal of buffered native writes.
// Records reach the kernel before Write() returns, so they survive process death. Power
// loss may lose or tear unsynced records; replay stops at the first invalid or
// non-consecutive record, so only a prefix of writes is ever reapplied.
#include "differential_permissions.hpp"
#include <cstdint>
#include <string>
#include <vector>

namespace differential {
struct JournalRecord {
    enum Type : uint32_t { WRITE = 1, TRUNCATE = 2 };
    Type type;
    uint64_t sequence;
    int64_t file;
    uint64_t offset; // write offset, or truncate length
    std::vector<unsigned char> data;
};
class Journal {
  public:
    // Creates and exclusively locks <directory>/<id>. The lock marks the owner live.
    Journal(const std::string &directory, const std::string &id, const Permissions &permissions = {});
    ~Journal();
    Journal(const Journal &) = delete;
    Journal &operator=(const Journal &) = delete;
    uint64_t Append(JournalRecord::Type type, int64_t file, uint64_t offset, const unsigned char *data,
                    uint64_t length);
    uint64_t Last() const { return sequence; }
    // Discard all records; caller guarantees every record is committed.
    void Reset();
    void Remove();
    // Opens a journal whose owner died (lock acquired). Returns -1 if the owner is live.
    static int TryClaim(const std::string &path);
    static std::vector<JournalRecord> ReadValid(int fd, uint64_t committed);
    const std::string path;

  private:
    int fd = -1;
    uint64_t sequence = 0;
};
} // namespace differential
