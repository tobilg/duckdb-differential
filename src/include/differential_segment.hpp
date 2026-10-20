#pragma once
// Append-only segment files: the write journal and the local payload store in one.
//
// Each file group of a process appends its buffered native writes to its own segment before
// Write() returns (unsynced, so they survive process death like writes into the kernel page
// cache). A publication references the bytes in place: a write that covers a whole chunk is that
// chunk's object, a log-file write is its byte range, and anything else is appended as an object
// record first. The segment is synced before the metadata transaction that references it commits.
//
// An active segment lives in <repository>/journal/<id> and is exclusively locked by its owner;
// once retired (rolled, closed, replayed or compacted) it moves to <repository>/segments/<id>.
// Power loss may lose or tear unsynced records; replay applies only a checksummed,
// sequence-consecutive prefix of the records after the published watermark.
#include "differential_permissions.hpp"
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace differential {
struct SegmentRecord {
    // OBJECT records hold published payload only (never replayed).
    enum Type : uint32_t { WRITE = 1, TRUNCATE = 2, OBJECT = 3 };
    Type type;
    uint64_t sequence;
    int64_t file;
    uint64_t offset;   // write offset, or truncate length
    uint64_t position; // segment offset of the payload
    uint32_t crc;      // CRC32C of the payload
    std::vector<unsigned char> data;
};
class Segment {
  public:
    static constexpr uint64_t HEADER = 48;
    static constexpr uint64_t MAX_RECORD = 64ull * 1024 * 1024;
    // Creates and exclusively locks a new active segment in `directory` (journal/) and syncs the
    // directory, so a published reference can always find the file.
    static std::unique_ptr<Segment> Create(const std::string &directory, const Permissions &permissions);
    ~Segment();
    Segment(const Segment &) = delete;
    Segment &operator=(const Segment &) = delete;
    // Appends one record; returns the segment offset of its payload.
    uint64_t Append(SegmentRecord::Type type, int64_t file, uint64_t offset, const unsigned char *data,
                    uint64_t length, uint32_t crc);
    uint64_t End() const { return end; }
    // fsync, when anything was appended since the last one.
    void Sync();
    // Moves the file to `directory` (segments/) and closes it.
    void Retire(const std::string &directory);
    const int64_t id;
    std::string path;
    // Some publication made an object or log range point into this segment. A segment nothing ever
    // referenced (writes of external-pool files) holds only published journal records: it is
    // retired and deleted early instead of waiting for collection.
    bool referenced = false;

    static std::string Name(int64_t id);
    static bool Parse(const std::string &name, int64_t &id);
    // Opens a segment whose owner died (lock acquired). Returns -1 if the owner is live.
    static int TryClaim(const std::string &path);
    // The valid, sequence-consecutive records starting at byte `from`.
    static std::vector<SegmentRecord> ReadValid(int fd, uint64_t from);
    // Reads exactly n bytes at `offset`; false at end of file.
    static bool ReadExact(int fd, void *out, size_t n, uint64_t offset);

  private:
    Segment(int64_t id, std::string path, int fd) : id(id), path(std::move(path)), fd(fd) {}
    int fd = -1;
    uint64_t end = 0, synced = 0, sequence = 0;
};
} // namespace differential
