#pragma once
// Optional on-disk cache of verified remote objects (S3 pack chunks and index pages) in
// <repository>/cache/<first two hex digits>/<sha256>. Entries are named by their content hash,
// so they never need invalidation: every read re-verifies the hash, and a damaged or missing
// entry is simply a miss. Shared by every process (and, for shared repositories, every user)
// of the repository. Least-recently-used entries are evicted when the configured size is
// exceeded; the limit is approximate across processes. Off unless differential_remote_cache_size
// is set. The directory can be deleted at any time.
#include "differential_permissions.hpp"
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <thread>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace differential {
class DiskCache {
  public:
    // The cache of the repository at `repository` (one instance per directory and process).
    static std::shared_ptr<DiskCache> For(const std::string &repository, const Permissions &permissions);
    // Verified bytes of the object with SHA-256 `hash` and exactly `size` bytes (0: any size up to
    // 1 MiB), if cached.
    std::optional<std::vector<unsigned char>> Read(const std::string &hash, uint64_t size);
    using Bytes = std::shared_ptr<const std::vector<unsigned char>>;
    // Queue verified bytes for a background writer, off the reading query's path (dropped when
    // more than 64 MiB are queued); evicts least-recently-used entries beyond `limit` bytes.
    void Write(const std::string &hash, Bytes bytes, uint64_t limit);
    static std::vector<std::pair<std::string, uint64_t>> Metrics();

    DiskCache(std::string directory, Permissions permissions);
    ~DiskCache(); // writes what is still queued

  private:
    struct Job {
        std::string hash;
        Bytes bytes;
        uint64_t limit;
    };
    void Store(const Job &job);
    std::mutex queue_mutex;
    std::condition_variable queue_signal;
    std::deque<Job> queue;
    uint64_t queued_bytes = 0;
    bool stopping = false;
    std::thread writer;
    std::string Path(const std::string &hash) const;
    // Scan entries (removing stale temporary files); with `limit`, evict down to 90% of it.
    uint64_t Scan(std::optional<uint64_t> limit);
    const std::string directory;
    const Permissions permissions;
    std::mutex mutex;
    bool scanned = false;
    uint64_t estimated = 0; // this process's view of the cache size
    std::chrono::steady_clock::time_point last_trim{};
};
} // namespace differential
