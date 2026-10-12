#include "differential_journal.hpp"
#include <cerrno>
#include <cstring>
#include <stdexcept>
#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <unistd.h>

namespace differential {
namespace {
constexpr uint32_t MAGIC = 0x314a4444; // "DDJ1"
constexpr uint64_t MAX_RECORD = 64ull * 1024 * 1024;
struct Header {
    uint32_t magic;
    uint32_t type;
    uint64_t sequence;
    int64_t file;
    uint64_t offset;
    uint64_t length;
    uint64_t checksum;
};
static_assert(sizeof(Header) == 48, "journal header layout");
// 64-bit FNV-1a variant over 8-byte words: detects torn/unwritten records, not tampering.
uint64_t Mix(uint64_t h, const unsigned char *p, uint64_t n) {
    constexpr uint64_t prime = 0x100000001b3ull;
    uint64_t i = 0;
    for (; i + 8 <= n; i += 8) {
        uint64_t word;
        std::memcpy(&word, p + i, 8);
        h = (h ^ word) * prime;
        h ^= h >> 29;
    }
    for (; i < n; i++)
        h = (h ^ p[i]) * prime;
    return h;
}
uint64_t Checksum(const Header &header, const unsigned char *data, uint64_t n) {
    Header copy = header;
    copy.checksum = 0;
    auto h = Mix(0xcbf29ce484222325ull, reinterpret_cast<const unsigned char *>(&copy), sizeof(copy));
    return Mix(h, data, n);
}
[[noreturn]] void Fail(const char *what) {
    auto category = errno == ENOSPC || errno == EDQUOT ? "disk_full" : "local_io";
    throw std::runtime_error(std::string("[differential:") + category + "] " + what);
}
bool ReadExact(int fd, void *out, size_t n, off_t offset) {
    auto p = static_cast<unsigned char *>(out);
    size_t done = 0;
    while (done < n) {
        auto r = pread(fd, p + done, n - done, offset + off_t(done));
        if (r < 0 && errno == EINTR)
            continue;
        if (r <= 0)
            return false;
        done += size_t(r);
    }
    return true;
}
} // namespace
Journal::Journal(const std::string &directory, const std::string &id, const Permissions &permissions)
    : path(directory + "/" + id) {
    fd = permissions.Open(path, O_EXCL | O_RDWR | O_APPEND | O_CLOEXEC | O_NOFOLLOW);
    if (fd < 0)
        Fail("Cannot create write journal");
    if (flock(fd, LOCK_EX | LOCK_NB)) {
        close(fd);
        fd = -1;
        Fail("Cannot lock write journal");
    }
}
Journal::~Journal() {
    if (fd >= 0)
        close(fd);
}
uint64_t Journal::Append(JournalRecord::Type type, int64_t file, uint64_t offset, const unsigned char *data,
                         uint64_t length) {
    if (length > MAX_RECORD)
        throw std::runtime_error("Journal record exceeds 64 MiB bound");
    Header header{MAGIC, uint32_t(type), sequence + 1, file, offset, length, 0};
    header.checksum = Checksum(header, data, length);
    iovec parts[2] = {{&header, sizeof(header)}, {const_cast<unsigned char *>(data), size_t(length)}};
    size_t total = sizeof(header) + size_t(length), written = 0;
    struct stat before {};
    if (fstat(fd, &before))
        Fail("Cannot inspect write journal");
    while (written < total) {
        ssize_t r;
        if (written < sizeof(header)) {
            iovec rest[2] = {{reinterpret_cast<unsigned char *>(&header) + written, sizeof(header) - written},
                             parts[1]};
            r = writev(fd, rest, length ? 2 : 1);
        } else
            r = write(fd, data + (written - sizeof(header)), total - written);
        if (r < 0 && errno == EINTR)
            continue;
        if (r <= 0) {
            // Remove a partial record so a later append cannot follow torn bytes.
            if (ftruncate(fd, before.st_size) != 0) {
            }
            Fail("Cannot append write journal");
        }
        written += size_t(r);
    }
    return ++sequence;
}
void Journal::Reset() {
    if (ftruncate(fd, 0))
        Fail("Cannot reset write journal");
}
void Journal::Remove() {
    if (unlink(path.c_str()) && errno != ENOENT)
        Fail("Cannot remove write journal");
}
int Journal::TryClaim(const std::string &path) {
    int fd = open(path.c_str(), O_RDWR | O_CLOEXEC | O_NOFOLLOW);
    if (fd < 0)
        return -1;
    if (flock(fd, LOCK_EX | LOCK_NB)) {
        close(fd);
        return -1;
    }
    return fd;
}
std::vector<JournalRecord> Journal::ReadValid(int fd, uint64_t committed) {
    std::vector<JournalRecord> records;
    off_t offset = 0;
    uint64_t expected = 0;
    while (true) {
        Header header{};
        if (!ReadExact(fd, &header, sizeof(header), offset) || header.magic != MAGIC ||
            (header.type != JournalRecord::WRITE && header.type != JournalRecord::TRUNCATE) ||
            header.length > MAX_RECORD)
            break;
        std::vector<unsigned char> data(header.length);
        if (header.length && !ReadExact(fd, data.data(), data.size(), offset + off_t(sizeof(header))))
            break;
        if (Checksum(header, data.data(), data.size()) != header.checksum)
            break;
        if (expected && header.sequence != expected)
            break;
        expected = header.sequence + 1;
        offset += off_t(sizeof(header) + header.length);
        if (header.sequence <= committed)
            continue;
        // The first uncommitted record must directly follow the committed prefix.
        if (records.empty() && header.sequence != committed + 1)
            break;
        records.push_back({JournalRecord::Type(header.type), header.sequence, header.file, header.offset,
                           std::move(data)});
    }
    return records;
}
} // namespace differential
