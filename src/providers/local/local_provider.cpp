#include "differential_local.hpp"
#include "differential_hooks.hpp"
#include "differential_metrics.hpp"
#include "differential_store.hpp"
#include <openssl/evp.h>
#include <algorithm>
#include <filesystem>
#include <map>
#include <mutex>
#include <set>
#include <shared_mutex>
#include <stdexcept>
#include <cerrno>
#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>

namespace differential {
namespace {
struct FD {
    int value;
    explicit FD(int value) : value(value) {}
    FD(const FD &) = delete;
    ~FD() {
        if (value >= 0)
            close(value);
    }
};
[[noreturn]] void Error(const char *action) {
    auto category = errno == ENOSPC || errno == EDQUOT ? "disk_full" : "local_io";
    throw std::runtime_error(std::string("[differential:") + category + "] " + action);
}
void Crash(const char *point) { Failpoint("DIFFERENTIAL_LOCAL_CRASH", point); }
// Durable at fsync, as native DuckDB on every OS (no F_FULLFSYNC device-cache flush on macOS).
void Sync(int fd, bool) {
    if (fsync(fd))
        Error("Local pool sync failed");
}
void SyncDirectory(const std::filesystem::path &path, bool device_flush) {
    FD fd(open(path.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW));
    if (fd.value < 0)
        Error("Cannot open local pool directory");
    Sync(fd.value, device_flush);
}
#ifdef __linux__
// Linux: a batch of at least this many files is made durable with two syncfs calls (data, then
// names) instead of one fsync per file plus directory syncs. syncfs reports writeback errors of
// the file system since Linux 5.8.
constexpr size_t SYNCFS_BATCH = 8;
void SyncFileSystem(const std::string &path) {
    FD fd(open(path.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW));
    if (fd.value < 0)
        Error("Cannot open local pool directory");
    if (syncfs(fd.value))
        Error("Local pool file system sync failed");
}
#endif
void CreateDirectories(const std::filesystem::path &path, const Permissions &permissions) {
    if (std::filesystem::exists(path)) {
        if (!std::filesystem::is_directory(std::filesystem::symlink_status(path)))
            throw std::runtime_error("[differential:local_io] Local pool namespace is not a directory");
        return;
    }
    for (auto &created : permissions.CreateDirectories(path)) {
        SyncDirectory(created, true);
        SyncDirectory(created.parent_path(), true);
    }
}
bool HashName(const std::string &s) {
    return s.size() == 64 && s.find_first_not_of("0123456789abcdef") == std::string::npos;
}
bool TemporaryName(const std::string &s) {
    return s.size() == 12 && s.rfind("write-", 0) == 0 &&
           s.find_first_not_of("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789", 6) ==
               std::string::npos;
}
bool FanoutName(const std::string &s) {
    return s.size() == 2 && s.find_first_not_of("0123456789abcdef") == std::string::npos;
}
void CheckHash(const std::string &hash) {
    if (!HashName(hash))
        throw std::runtime_error("[differential:corrupt_data] Invalid local object identity");
}
void Verify(const std::string &hash, const std::vector<unsigned char> &bytes) {
    unsigned char digest[32];
    unsigned n = 0;
    if (bytes.size() != Store::CHUNK ||
        EVP_Digest(bytes.data(), bytes.size(), digest, &n, EVP_sha256(), nullptr) != 1 || n != 32)
        throw std::runtime_error("[differential:corrupt_data] Invalid local payload length/digest");
    const char hex[] = "0123456789abcdef";
    std::string actual;
    for (auto b : digest) {
        actual += hex[b >> 4];
        actual += hex[b & 15];
    }
    if (actual != hash)
        throw std::runtime_error("[differential:corrupt_data] Local object checksum mismatch");
}
std::string Namespace(const std::string &path, const std::string &repo, const std::string &provider) {
    auto uuid = [](const std::string &s) {
        return s.size() == 36 && s.find_first_not_of("0123456789abcdef-") == std::string::npos;
    };
    if (!std::filesystem::path(path).is_absolute() || !uuid(repo) || !uuid(provider))
        throw std::runtime_error("[differential:corrupt_data] Invalid local provider namespace");
    return (std::filesystem::path(path) / "differential-pools" / repo / provider).string();
}
} // namespace

struct PayloadFence::State {
    std::shared_mutex local; // in-process readers/writers vs deletion
    std::mutex counter;
    unsigned holders = 0;
    int shared_fd = -1;
    std::string path;
};
static PayloadFence::State &FenceState(const std::string &repository) {
    static std::mutex mutex;
    static std::map<std::string, std::unique_ptr<PayloadFence::State>> states;
    std::lock_guard<std::mutex> guard(mutex);
    auto &state = states[repository];
    if (!state) {
        state = std::make_unique<PayloadFence::State>();
        state->path = (std::filesystem::path(repository) / "payload.lock").string();
    }
    return *state;
}
static int OpenFence(const std::string &path) {
    int fd = Permissions::Of(std::filesystem::path(path).parent_path().string()).Open(path, O_RDWR | O_CLOEXEC | O_NOFOLLOW);
    if (fd < 0)
        Error("Cannot open payload fence");
    return fd;
}
PayloadFence::PayloadFence(const std::string &repository, bool exclusive)
    : state(&FenceState(repository)), exclusive(exclusive) {
    if (exclusive) {
        state->local.lock();
        // No in-process shared holder remains, so this process holds no shared flock.
        int fd = OpenFence(state->path);
        if (flock(fd, LOCK_EX)) {
            close(fd);
            state->local.unlock();
            Error("Cannot acquire payload fence");
        }
        std::lock_guard<std::mutex> guard(state->counter);
        state->shared_fd = fd;
        return;
    }
    state->local.lock_shared();
    std::lock_guard<std::mutex> guard(state->counter);
    if (state->holders++ == 0) {
        state->shared_fd = OpenFence(state->path);
        if (flock(state->shared_fd, LOCK_SH)) {
            close(state->shared_fd);
            state->shared_fd = -1;
            --state->holders;
            state->local.unlock_shared();
            Error("Cannot acquire payload fence");
        }
    }
}
bool PayloadFence::TryExclusive(const std::string &repository, std::unique_ptr<PayloadFence> &out) {
    auto &state = FenceState(repository);
    if (!state.local.try_lock())
        return false;
    int fd = Permissions::Of(std::filesystem::path(state.path).parent_path().string())
                 .Open(state.path, O_RDWR | O_CLOEXEC | O_NOFOLLOW);
    if (fd < 0 || flock(fd, LOCK_EX | LOCK_NB)) {
        if (fd >= 0)
            close(fd);
        state.local.unlock();
        return false;
    }
    std::unique_ptr<PayloadFence> fence(new PayloadFence());
    fence->state = &state;
    fence->exclusive = true;
    std::lock_guard<std::mutex> guard(state.counter);
    state.shared_fd = fd;
    out = std::move(fence);
    return true;
}
PayloadFence::~PayloadFence() {
    if (!state)
        return;
    if (exclusive) {
        {
            std::lock_guard<std::mutex> guard(state->counter);
            close(state->shared_fd);
            state->shared_fd = -1;
        }
        state->local.unlock();
        return;
    }
    {
        std::lock_guard<std::mutex> guard(state->counter);
        if (--state->holders == 0) {
            close(state->shared_fd);
            state->shared_fd = -1;
        }
    }
    state->local.unlock_shared();
}

std::string LocalPool::Canonical(const std::string &path) {
    if (path.empty())
        throw std::runtime_error("[differential:validation] Local provider path is empty");
    return std::filesystem::weakly_canonical(std::filesystem::absolute(path)).string();
}
LocalPool::LocalPool(const std::string &path, const std::string &repository, const std::string &provider,
                     const Permissions &permissions)
    : directory(Namespace(path, repository, provider)), permissions(permissions) {}
std::string LocalPool::Path(const std::string &hash) const {
    return (std::filesystem::path(directory) / hash.substr(0, 2) / hash).string();
}
std::vector<unsigned char> LocalPool::Get(const std::string &hash) {
    CheckHash(hash);
    FD fd(open(Path(hash).c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW));
    if (fd.value < 0) // Pre-fan-out pools stored names directly in the namespace.
        fd.value = open((std::filesystem::path(directory) / hash).c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW);
    if (fd.value < 0)
        throw std::runtime_error("[differential:unavailable_object] Local pool object unavailable");
    struct stat st {};
    if (fstat(fd.value, &st) || !S_ISREG(st.st_mode) || uint64_t(st.st_size) != Store::CHUNK)
        throw std::runtime_error("[differential:corrupt_data] Invalid local object file");
    std::vector<unsigned char> bytes(Store::CHUNK);
    size_t done = 0;
    while (done < bytes.size()) {
        auto n = read(fd.value, bytes.data() + done, bytes.size() - done);
        if (n < 0 && errno == EINTR)
            continue;
        if (n <= 0)
            Error("Short local payload read");
        done += size_t(n);
        Count(Counter::LocalPoolReadBytes, uint64_t(n));
    }
    Verify(hash, bytes);
    return bytes;
}
void LocalPool::Put(const std::string &hash, const std::vector<unsigned char> &bytes) {
    PutBatch({{hash, &bytes}});
}
void LocalPool::PutBatch(const std::vector<std::pair<std::string, const std::vector<unsigned char> *>> &objects) {
    if (objects.empty())
        return;
    CreateDirectories(directory, permissions);
    struct Staged {
        std::string temporary, target;
    };
    std::vector<Staged> staged;
    std::set<std::string> directories, seen;
    bool created_directory = false;
#ifdef __linux__
    bool batched = objects.size() >= SYNCFS_BATCH;
#else
    bool batched = false;
#endif
    try {
        for (auto &object : objects) {
            CheckHash(object.first);
            if (!seen.insert(object.first).second)
                continue;
            Verify(object.first, *object.second);
            auto target = Path(object.first);
            auto folder = std::filesystem::path(target).parent_path();
            if (!std::filesystem::exists(folder)) {
                if (mkdir(folder.c_str(), permissions.Directory()) && errno != EEXIST)
                    Error("Cannot create local pool fan-out directory");
                if (chmod(folder.c_str(), permissions.Directory()))
                    Error("Cannot set local pool directory permissions");
                created_directory = true;
            }
            struct stat existing {};
            if (stat(target.c_str(), &existing) == 0 && S_ISREG(existing.st_mode) && uint64_t(existing.st_size) == Store::CHUNK)
                continue; // immutable name already published; reads verify its digest
            auto pattern = (folder / "write-XXXXXX").string();
            std::vector<char> writable(pattern.begin(), pattern.end());
            writable.push_back(0);
            FD fd(mkstemp(writable.data()));
            if (fd.value < 0)
                Error("Cannot stage local payload");
            permissions.Apply(fd.value);
            staged.push_back({writable.data(), target});
            if (fcntl(fd.value, F_SETFD, FD_CLOEXEC))
                Error("Cannot protect local payload descriptor");
            auto &bytes = *object.second;
            size_t done = 0;
            while (done < bytes.size()) {
                auto n = write(fd.value, bytes.data() + done, bytes.size() - done);
                if (n < 0 && errno == EINTR)
                    continue;
                if (n <= 0)
                    Error("Cannot write local payload");
                done += size_t(n);
                Count(Counter::LocalPoolWriteBytes, uint64_t(n));
            }
            Crash("payload_written");
            if (!batched)
                Sync(fd.value, false);
            directories.insert(folder.string());
        }
#ifdef __linux__
        if (batched)
            SyncFileSystem(directory); // every staged file's data, before any name refers to it
#endif
        Crash("payload_synced");
        for (auto &item : staged) {
            // link is atomic and cannot replace an immutable name.
            if (link(item.temporary.c_str(), item.target.c_str()) && errno != EEXIST)
                Error("Cannot publish local payload");
            Crash("payload_linked");
            if (unlink(item.temporary.c_str()))
                Error("Cannot remove local payload staging name");
        }
        staged.clear();
        if (created_directory)
            directories.insert(directory);
        // Name publication, then one device-cache flush covering every file and directory
        // of the batch before the caller commits mappings.
#ifdef __linux__
        if (batched)
            SyncFileSystem(directory);
        else
#endif
        {
            for (auto &folder : directories)
                SyncDirectory(folder, false);
            SyncDirectory(directory, true);
        }
        Crash("payload_durable");
    } catch (...) {
        for (auto &item : staged)
            unlink(item.temporary.c_str());
        throw;
    }
}
// `entry` is an object hash, or a "<xx>/<name>" entry from List (an object or a staging file).
void LocalPool::Remove(const std::string &entry) {
    auto fanout = entry.size() > 2 && entry[2] == '/';
    auto name = fanout ? entry.substr(3) : entry;
    if (!(fanout && TemporaryName(name)))
        CheckHash(name);
    auto path = fanout ? std::filesystem::path(directory) / entry : std::filesystem::path(Path(name));
    if (unlink(path.c_str()) && errno != ENOENT)
        Error("Cannot remove unreferenced local payload");
    if (std::filesystem::exists(path.parent_path()))
        SyncDirectory(path.parent_path(), false);
}
// Entries are "<xx>/<name>" in fan-out order, after the cursor `after` ("" for the first page).
std::vector<std::string> LocalPool::List(const std::string &after, uint64_t limit) {
    std::vector<std::string> page;
    if (!std::filesystem::exists(directory) || limit == 0)
        return page;
    auto eligible = [](const std::string &name) { return HashName(name) || TemporaryName(name); };
    std::vector<std::string> folders;
    for (auto &entry : std::filesystem::directory_iterator(directory)) {
        auto name = entry.path().filename().string();
        if (FanoutName(name) && std::filesystem::is_directory(entry.symlink_status()))
            folders.push_back(name);
    }
    std::sort(folders.begin(), folders.end());
    for (auto &folder : folders) {
        auto prefix = folder + "/";
        if (!after.empty() && prefix + "~" < after)
            continue;
        std::set<std::string> entries;
        for (auto &entry : std::filesystem::directory_iterator(std::filesystem::path(directory) / folder)) {
            auto name = entry.path().filename().string();
            // Only owned immutable names are eligible. Unknown entries are never deleted.
            if (eligible(name) && std::filesystem::is_regular_file(entry.symlink_status()) &&
                (after.empty() || prefix + name > after))
                entries.insert(prefix + name);
        }
        for (auto &name : entries) {
            if (page.size() == limit)
                return page;
            page.push_back(name);
        }
    }
    return page;
}
} // namespace differential
