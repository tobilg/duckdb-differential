#include "differential_segment.hpp"
#include "differential_crc.hpp"
#include <openssl/rand.h>
#include <cerrno>
#include <cstring>
#include <filesystem>
#include <stdexcept>
#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <unistd.h>

namespace differential {
namespace {
constexpr uint32_t MAGIC = 0x32475344; // "DSG2"
struct Header {
    uint32_t magic;
    uint32_t type;
    uint64_t sequence;
    int64_t file;
    uint64_t offset;
    uint64_t length;
    uint32_t crc;        // payload
    uint32_t header_crc; // the preceding 44 bytes
};
static_assert(sizeof(Header) == Segment::HEADER, "segment record header layout");
uint32_t HeaderCrc(const Header &header) { return Crc32c(&header, offsetof(Header, header_crc)); }
[[noreturn]] void Fail(const char *what) {
    auto category = errno == ENOSPC || errno == EDQUOT ? "disk_full" : "local_io";
    throw std::runtime_error(std::string("[differential:") + category + "] " + what);
}
void SyncDirectory(const std::string &path) {
    int fd = open(path.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (fd < 0)
        Fail("Cannot open segment directory for sync");
    int rc = fsync(fd);
    close(fd);
    if (rc)
        Fail("Cannot sync segment directory");
}
} // namespace
std::string Segment::Name(int64_t id) {
    static const char hex[] = "0123456789abcdef";
    std::string name(16, '0');
    for (int i = 15; i >= 0; --i, id = int64_t(uint64_t(id) >> 4))
        name[size_t(i)] = hex[id & 15];
    return name;
}
bool Segment::Parse(const std::string &name, int64_t &id) {
    if (name.size() != 16 || name.find_first_not_of("0123456789abcdef") != std::string::npos)
        return false;
    id = int64_t(std::stoull(name, nullptr, 16));
    return id > 0;
}
std::unique_ptr<Segment> Segment::Create(const std::string &directory, const Permissions &permissions) {
    for (int attempt = 0; attempt < 8; ++attempt) {
        uint64_t random = 0;
        if (RAND_bytes(reinterpret_cast<unsigned char *>(&random), sizeof(random)) != 1)
            throw std::runtime_error("Cannot allocate segment identity");
        auto id = int64_t(random >> 1); // positive; identities are never reused
        if (id == 0)
            continue;
        auto path = directory + "/" + Name(id);
        int fd = permissions.Open(path, O_EXCL | O_RDWR | O_CLOEXEC | O_NOFOLLOW);
        if (fd < 0 && errno == EEXIST)
            continue;
        if (fd < 0)
            Fail("Cannot create segment");
        if (flock(fd, LOCK_EX | LOCK_NB)) {
            close(fd);
            Fail("Cannot lock segment");
        }
        std::unique_ptr<Segment> segment(new Segment(id, path, fd));
        SyncDirectory(directory);
        return segment;
    }
    throw std::runtime_error("Cannot allocate segment identity");
}
Segment::~Segment() {
    if (fd >= 0)
        close(fd);
}
uint64_t Segment::Append(SegmentRecord::Type type, int64_t file, uint64_t offset, const unsigned char *data,
                         uint64_t length, uint32_t crc) {
    if (length > MAX_RECORD)
        throw std::runtime_error("Segment record exceeds 64 MiB bound");
    if (fd < 0)
        throw std::runtime_error("[differential:local_io] Segment retired");
    Header header{MAGIC, uint32_t(type), sequence + 1, file, offset, length, crc, 0};
    header.header_crc = HeaderCrc(header);
    size_t total = sizeof(header) + size_t(length), written = 0;
    while (written < total) {
        iovec parts[2];
        int count = 0;
        if (written < sizeof(header)) {
            parts[count++] = {reinterpret_cast<unsigned char *>(&header) + written, sizeof(header) - written};
            if (length)
                parts[count++] = {const_cast<unsigned char *>(data), size_t(length)};
        } else
            parts[count++] = {const_cast<unsigned char *>(data) + (written - sizeof(header)), total - written};
        auto r = pwritev(fd, parts, count, off_t(end + written));
        if (r < 0 && errno == EINTR)
            continue;
        if (r <= 0) {
            // Remove a partial record so a later append cannot follow torn bytes.
            if (ftruncate(fd, off_t(end)) != 0) {
            }
            Fail("Cannot append segment");
        }
        written += size_t(r);
    }
    auto position = end + sizeof(header);
    end += total;
    ++sequence;
    return position;
}
void Segment::Sync() {
    if (synced == end)
        return;
    if (fsync(fd))
        Fail("Cannot sync segment");
    synced = end;
}
void Segment::Retire(const std::string &directory) {
    auto target = directory + "/" + Name(id);
    if (rename(path.c_str(), target.c_str()) && errno != ENOENT)
        Fail("Cannot retire segment");
    path = target;
    close(fd);
    fd = -1;
}
int Segment::TryClaim(const std::string &path) {
    int fd = open(path.c_str(), O_RDWR | O_CLOEXEC | O_NOFOLLOW);
    if (fd < 0)
        return -1;
    if (flock(fd, LOCK_EX | LOCK_NB)) {
        close(fd);
        return -1;
    }
    return fd;
}
bool Segment::ReadExact(int fd, void *out, size_t n, uint64_t offset) {
    auto p = static_cast<unsigned char *>(out);
    size_t done = 0;
    while (done < n) {
        auto r = pread(fd, p + done, n - done, off_t(offset + done));
        if (r < 0 && errno == EINTR)
            continue;
        if (r <= 0)
            return false;
        done += size_t(r);
    }
    return true;
}
std::vector<SegmentRecord> Segment::ReadValid(int fd, uint64_t from) {
    std::vector<SegmentRecord> records;
    uint64_t offset = from, expected = 0;
    while (true) {
        Header header{};
        if (!ReadExact(fd, &header, sizeof(header), offset) || header.magic != MAGIC ||
            HeaderCrc(header) != header.header_crc ||
            (header.type != SegmentRecord::WRITE && header.type != SegmentRecord::TRUNCATE &&
             header.type != SegmentRecord::OBJECT) ||
            header.length > MAX_RECORD || (expected && header.sequence != expected))
            break;
        std::vector<unsigned char> data(header.length);
        if (header.length && !ReadExact(fd, data.data(), data.size(), offset + sizeof(header)))
            break;
        if (Crc32c(data.data(), data.size()) != header.crc)
            break;
        expected = header.sequence + 1;
        records.push_back({SegmentRecord::Type(header.type), header.sequence, header.file, header.offset,
                           offset + sizeof(header), header.crc, std::move(data)});
        offset += sizeof(header) + header.length;
    }
    return records;
}
} // namespace differential
