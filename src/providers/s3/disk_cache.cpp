#include "differential_disk_cache.hpp"
#include <openssl/evp.h>
#include <algorithm>
#include <atomic>
#include <cerrno>
#include <cstdio>
#include <filesystem>
#include <map>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>

namespace differential {
namespace {
std::atomic<uint64_t> hits{0}, misses{0}, writes{0}, written_bytes{0}, evictions{0}, corrupt{0}, errors{0}, dropped{0};
constexpr uint64_t QUEUE_LIMIT = 64ull * 1024 * 1024;
std::atomic<uint64_t> sequence{0};
constexpr int64_t TOUCH_SECONDS = 60;          // refresh an entry's age at most once a minute
constexpr int64_t STALE_TEMPORARY_SECONDS = 3600; // unfinished writes of dead processes
std::string Sha256(const unsigned char *data, size_t size) {
    unsigned char bytes[EVP_MAX_MD_SIZE];
    unsigned length = 0;
    if (EVP_Digest(data, size, bytes, &length, EVP_sha256(), nullptr) != 1)
        return std::string();
    static const char *hex = "0123456789abcdef";
    std::string out;
    for (unsigned i = 0; i < length; i++) {
        out += hex[bytes[i] >> 4];
        out += hex[bytes[i] & 15];
    }
    return out;
}
bool IsHash(const std::string &s) {
    return s.size() == 64 && s.find_first_not_of("0123456789abcdef") == std::string::npos;
}
bool WriteAll(int fd, const unsigned char *data, uint64_t size) {
    while (size > 0) {
        auto n = write(fd, data, size_t(std::min<uint64_t>(size, 1 << 20)));
        if (n < 0 && errno == EINTR)
            continue;
        if (n <= 0)
            return false;
        data += n;
        size -= uint64_t(n);
    }
    return true;
}
} // namespace

std::shared_ptr<DiskCache> DiskCache::For(const std::string &repository, const Permissions &permissions) {
    static std::mutex mutex;
    static std::map<std::string, std::weak_ptr<DiskCache>> caches;
    std::lock_guard<std::mutex> guard(mutex);
    auto directory = (std::filesystem::path(repository) / "cache").string();
    auto cache = caches[directory].lock();
    if (!cache) {
        cache = std::make_shared<DiskCache>(directory, permissions);
        caches[directory] = cache;
    }
    return cache;
}
DiskCache::DiskCache(std::string directory_p, Permissions permissions_p)
    : directory(std::move(directory_p)), permissions(permissions_p) {}
DiskCache::~DiskCache() {
    {
        std::lock_guard<std::mutex> guard(queue_mutex);
        stopping = true;
    }
    queue_signal.notify_all();
    if (writer.joinable())
        writer.join();
}
std::string DiskCache::Path(const std::string &hash) const { return directory + "/" + hash.substr(0, 2) + "/" + hash; }

std::optional<std::vector<unsigned char>> DiskCache::Read(const std::string &hash, uint64_t size) {
    if (!IsHash(hash))
        return std::nullopt;
    auto path = Path(hash);
    int fd = open(path.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW);
    if (fd < 0) {
        ++misses;
        return std::nullopt;
    }
    struct stat st {};
    std::vector<unsigned char> bytes;
    bool valid = fstat(fd, &st) == 0 && S_ISREG(st.st_mode) &&
                 (size ? uint64_t(st.st_size) == size : st.st_size > 0 && st.st_size <= (1 << 20));
    if (valid) {
        size = uint64_t(st.st_size);
        bytes.resize(size_t(size));
        uint64_t done = 0;
        while (done < size) {
            auto n = pread(fd, bytes.data() + done, size_t(size - done), off_t(done));
            if (n < 0 && errno == EINTR)
                continue;
            if (n <= 0)
                break;
            done += uint64_t(n);
        }
        valid = done == size && Sha256(bytes.data(), bytes.size()) == hash;
    }
    if (valid && time(nullptr) - st.st_mtime >= TOUCH_SECONDS)
        futimens(fd, nullptr); // recently used: evicted last
    close(fd);
    if (!valid) {
        // A torn, truncated or damaged entry is a miss; the next fetch rewrites it.
        unlink(path.c_str());
        ++corrupt;
        ++misses;
        return std::nullopt;
    }
    ++hits;
    return bytes;
}

void DiskCache::Write(const std::string &hash, Bytes bytes, uint64_t limit) {
    if (!bytes || limit == 0 || bytes->size() > limit || !IsHash(hash))
        return;
    std::lock_guard<std::mutex> guard(queue_mutex);
    if (stopping || queued_bytes + bytes->size() > QUEUE_LIMIT) {
        ++dropped;
        return;
    }
    queued_bytes += bytes->size();
    queue.push_back({hash, std::move(bytes), limit});
    if (!writer.joinable())
        writer = std::thread([this] {
            std::unique_lock<std::mutex> lock(queue_mutex);
            while (true) {
                queue_signal.wait(lock, [this] { return stopping || !queue.empty(); });
                if (queue.empty())
                    break; // stopping, and everything queued is written
                auto job = std::move(queue.front());
                queue.pop_front();
                queued_bytes -= job.bytes->size();
                lock.unlock();
                Store(job);
                lock.lock();
            }
        });
    queue_signal.notify_one();
}
void DiskCache::Store(const Job &job) {
    auto &hash = job.hash;
    auto data = job.bytes->data();
    uint64_t size = job.bytes->size(), limit = job.limit;
    auto path = Path(hash);
    auto parent = std::filesystem::path(path).parent_path();
    auto temporary = parent / ("." + hash + "." + std::to_string(getpid()) + "." + std::to_string(++sequence) + ".tmp");
    int fd = permissions.Open(temporary, O_WRONLY | O_EXCL | O_CLOEXEC | O_NOFOLLOW);
    if (fd < 0 && errno == ENOENT) {
        // First entry of this shard: create the directories, then retry.
        try {
            permissions.CreateDirectories(parent);
        } catch (const std::exception &) {
        }
        fd = permissions.Open(temporary, O_WRONLY | O_EXCL | O_CLOEXEC | O_NOFOLLOW);
    }
    if (fd < 0) {
        ++errors;
        return;
    }
    // No fsync: a lost or torn entry fails verification and is refetched.
    bool ok = WriteAll(fd, data, size);
    ok = close(fd) == 0 && ok;
    if (!ok || rename(temporary.c_str(), path.c_str()) != 0) {
        unlink(temporary.c_str());
        ++errors;
        return;
    }
    ++writes;
    written_bytes += size;
    std::lock_guard<std::mutex> guard(mutex);
    if (!scanned) {
        estimated = Scan(std::nullopt);
        scanned = true;
    } else
        estimated += size;
    auto now = std::chrono::steady_clock::now();
    // Trim when over the limit, at most once a second unless well over it.
    if (estimated > limit && (now - last_trim >= std::chrono::seconds(1) || estimated > limit + limit / 10)) {
        last_trim = now;
        estimated = Scan(limit);
    }
}

uint64_t DiskCache::Scan(std::optional<uint64_t> limit) {
    struct Entry {
        std::string path;
        uint64_t size;
        int64_t used;
    };
    std::vector<Entry> entries;
    uint64_t total = 0;
    int lock = -1;
    if (limit) {
        // One trimming process at a time; others skip and retry on a later write.
        lock = permissions.Open(directory + "/.trim.lock", O_RDWR | O_CLOEXEC);
        if (lock < 0 || flock(lock, LOCK_EX | LOCK_NB) != 0) {
            if (lock >= 0)
                close(lock);
            return estimated;
        }
    }
    auto now = time(nullptr);
    std::error_code ec;
    for (std::filesystem::directory_iterator shard(directory, ec), end; !ec && shard != end; shard.increment(ec)) {
        if (!shard->is_directory(ec))
            continue;
        std::error_code inner;
        for (std::filesystem::directory_iterator file(shard->path(), inner); !inner && file != end;
             file.increment(inner)) {
            struct stat st {};
            auto path = file->path().string();
            if (lstat(path.c_str(), &st) != 0 || !S_ISREG(st.st_mode))
                continue;
            if (file->path().filename().string().front() == '.') {
                if (now - st.st_mtime > STALE_TEMPORARY_SECONDS)
                    unlink(path.c_str());
                continue;
            }
            entries.push_back({path, uint64_t(st.st_size), int64_t(st.st_mtime)});
            total += uint64_t(st.st_size);
        }
    }
    if (limit && total > *limit) {
        std::sort(entries.begin(), entries.end(), [](const Entry &a, const Entry &b) { return a.used < b.used; });
        auto target = *limit - *limit / 10;
        for (auto &entry : entries) {
            if (total <= target)
                break;
            if (unlink(entry.path.c_str()) == 0) {
                total -= entry.size;
                ++evictions;
            }
        }
    }
    if (lock >= 0)
        close(lock);
    return total;
}

std::vector<std::pair<std::string, uint64_t>> DiskCache::Metrics() {
    return {{"remote_disk_cache_hits", hits.load()},           {"remote_disk_cache_misses", misses.load()},
            {"remote_disk_cache_writes", writes.load()},       {"remote_disk_cache_bytes_written", written_bytes.load()},
            {"remote_disk_cache_evictions", evictions.load()}, {"remote_disk_cache_corrupt_entries", corrupt.load()},
            {"remote_disk_cache_errors", errors.load()},       {"remote_disk_cache_dropped_writes", dropped.load()}};
}
} // namespace differential
