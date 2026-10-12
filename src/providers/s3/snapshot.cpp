// Immutable S3 export: content-addressed packs of 64 KiB chunks, paged chunk
// indexes and a self-contained descriptor; parallel, coalesced and verified range reads.
#include "differential_metrics.hpp"
#include "differential_export.hpp"
#include "differential_hooks.hpp"
#include "differential_permissions.hpp"
#include "differential_disk_cache.hpp"
#include "duckdb/main/database.hpp"
#include <openssl/evp.h>
#include <openssl/rand.h>
#include <algorithm>
#include <array>
#include <atomic>
#include <condition_variable>
#include <chrono>
#include <cstring>
#include <deque>
#include <filesystem>
#include <fstream>
#include <future>
#include <optional>
#include <set>
#include <tuple>
#include <sstream>
#include <stdexcept>
#include <thread>
#include <unordered_map>
#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>

namespace differential {
namespace {
constexpr uint64_t INDEX_CHUNKS = 4096;
// Index page ("DIX1"): u32 pack count, 32-byte pack identities, then 4096 entries
// of chunk identity (32), pack number (u16 LE, 0xFFFF = hole) and pack slot (u16 LE).
// Index page: "DIX1", u32 pack count, then per pack the 16-byte UUID of the database whose pool
// holds it and the pack's SHA-256, then 4096 chunk entries.
constexpr uint64_t INDEX_ENTRY = 36, INDEX_HEADER = 8, PACK_ENTRY = 48;
constexpr uint16_t INDEX_HOLE = 0xFFFF;
uint64_t IndexSize(uint64_t packs) { return INDEX_HEADER + PACK_ENTRY * packs + INDEX_CHUNKS * INDEX_ENTRY; }
constexpr uint64_t MAX_PACK_CHUNKS = 128; // 8 MiB packs
constexpr uint64_t REQUEST_CHUNKS = 16;   // 1 MiB: the smallest range worth a request of its own
constexpr uint64_t MAX_WINDOW_CHUNKS = 64; // 4 MiB read-ahead per sequential stream
constexpr size_t MAX_STREAMS = 16;
// Upload staging per repository (all processes): room for one full pack per parallel request,
// 64 MiB to 512 MiB (differential_s3_max_requests 1-8 to 64).
uint64_t StagingLimit() {
    constexpr uint64_t PACK = MAX_PACK_CHUNKS * Store::CHUNK;
    return std::min<uint64_t>(512ull << 20, std::max<uint64_t>(64ull << 20, RequestLimit() * PACK));
}
std::atomic<uint64_t> logical_read_bytes{0}, cache_hits{0}, cache_misses{0}, prefetched_chunks{0},
    coalesced_requests{0}, packs_uploaded{0}, chunks_uploaded{0}, chunks_reused{0}, publications{0},
    publication_failures{0};
std::string Hex(const unsigned char *bytes, size_t n) {
    static const char hex[] = "0123456789abcdef";
    std::string s;
    for (size_t i = 0; i < n; i++) {
        s += hex[bytes[i] >> 4];
        s += hex[bytes[i] & 15];
    }
    return s;
}
void Unhex(const std::string &hash, unsigned char *out) {
    auto value = [](char c) -> unsigned {
        if (c >= '0' && c <= '9')
            return unsigned(c - '0');
        if (c >= 'a' && c <= 'f')
            return unsigned(c - 'a' + 10);
        throw std::runtime_error("[differential:corrupt_data] Corrupt object identity");
    };
    if (hash.size() != 64)
        throw std::runtime_error("[differential:corrupt_data] Corrupt object identity");
    for (size_t i = 0; i < 32; i++)
        out[i] = static_cast<unsigned char>((value(hash[i * 2]) << 4) | value(hash[i * 2 + 1]));
}
std::string Hash(const unsigned char *data, size_t n) {
    unsigned char bytes[32];
    unsigned length = 0;
    if (EVP_Digest(data, n, bytes, &length, EVP_sha256(), nullptr) != 1 || length != 32)
        throw std::runtime_error("SHA256 failed");
    return Hex(bytes, length);
}
std::string Hash(const std::vector<unsigned char> &v) { return Hash(v.data(), v.size()); }
bool IsHash(const std::string &s) {
    return s.size() == 64 && s.find_first_not_of("0123456789abcdef") == std::string::npos;
}
bool IsUUID(const std::string &s) {
    return s.size() == 36 && s.find_first_not_of("0123456789abcdef-") == std::string::npos && s[8] == '-' &&
           s[13] == '-' && s[18] == '-' && s[23] == '-';
}
// 16 raw bytes <-> canonical lowercase UUID text.
void UnhexUUID(const std::string &uuid, unsigned char *out) {
    if (!IsUUID(uuid))
        throw std::runtime_error("[differential:corrupt_data] Corrupt database identity");
    std::string hex;
    for (char c : uuid)
        if (c != '-')
            hex += c;
    for (size_t i = 0; i < 16; i++)
        out[i] = static_cast<unsigned char>(std::stoul(hex.substr(i * 2, 2), nullptr, 16));
}
std::string HexUUID(const unsigned char *bytes) {
    auto hex = Hex(bytes, 16);
    return hex.substr(0, 8) + "-" + hex.substr(8, 4) + "-" + hex.substr(12, 4) + "-" + hex.substr(16, 4) + "-" +
           hex.substr(20, 12);
}
// Data pool of a database: packs and index pages it uploaded.
std::string Pool(const std::string &database) { return "dbs/" + database + "/"; }
// Commit record key: inverted, fixed-width snapshot number, so an S3 listing returns the newest
// first (LIST max-keys=1 is the latest snapshot number).
constexpr int64_t SNAPSHOT_KEY_MAX = 9999999999;
std::string CommitKey(const std::string &database, int64_t snapshot) {
    auto inverted = std::to_string(SNAPSHOT_KEY_MAX - snapshot);
    return Pool(database) + "snapshots/" + std::string(10 - inverted.size(), '0') + inverted;
}
// Percent-encoding for free text in line-based metadata objects.
std::string Escape(const std::string &text) {
    static const char *hex = "0123456789ABCDEF";
    std::string out;
    for (unsigned char c : text) {
        if (c < 0x20 || c == '%' || c == 0x7F) {
            out += '%';
            out += hex[c >> 4];
            out += hex[c & 15];
        } else
            out += char(c);
    }
    return out;
}
void Crash(const char *name) { Failpoint("DIFFERENTIAL_EXPORT_CRASH", name); }
// Derived upload staging. Files are deleted after upload; the namespace is bounded.
struct Stage {
    std::string path;
    std::string namespace_path;
    int fd = -1;
    Stage(const std::string &directory, const std::string &repository, const std::vector<unsigned char> &bytes) {
        if (bytes.size() > 16 * 1024 * 1024)
            throw std::runtime_error("Immutable upload exceeds 16 MiB staging bound");
        auto permissions = Permissions::Of(directory);
        auto folder = std::filesystem::path(directory) / "staging" / repository;
        permissions.CreateDirectories(folder);
        namespace_path = (folder / "namespace.lock").string();
        int namespace_fd = permissions.Open(namespace_path, O_RDWR | O_CLOEXEC);
        if (namespace_fd < 0 || flock(namespace_fd, LOCK_EX)) {
            if (namespace_fd >= 0)
                close(namespace_fd);
            throw std::runtime_error("Cannot lock upload staging");
        }
        try {
            const auto limit = StagingLimit();
            uint64_t active_bytes = 0;
            for (auto &entry : std::filesystem::directory_iterator(folder)) {
                auto name = entry.path().filename().string();
                std::error_code status_error;
                if (name.size() != 13 || name.rfind("upload-", 0) != 0 ||
                    name.find_first_not_of("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789", 7) !=
                        std::string::npos)
                    continue;
                auto status = entry.symlink_status(status_error);
                if (status_error && status_error != std::errc::no_such_file_or_directory)
                    throw std::runtime_error("Cannot inspect upload staging namespace");
                if (!std::filesystem::is_regular_file(status))
                    continue;
                int candidate = open(entry.path().c_str(), O_RDWR | O_CLOEXEC | O_NOFOLLOW);
                if (candidate < 0) {
                    if (errno == ENOENT)
                        continue;
                    throw std::runtime_error("Cannot inspect upload staging capacity");
                }
                bool removed = flock(candidate, LOCK_EX | LOCK_NB) == 0 && unlink(entry.path().c_str()) == 0;
                if (!removed) {
                    struct stat status {};
                    if (fstat(candidate, &status) || status.st_size < 0) {
                        close(candidate);
                        throw std::runtime_error("Cannot measure upload staging capacity");
                    }
                    auto size = uint64_t(status.st_size);
                    if (size > limit || active_bytes > limit - size) {
                        close(candidate);
                        throw std::runtime_error("[differential:staging_full] Upload staging capacity exhausted");
                    }
                    active_bytes += size;
                }
                close(candidate);
            }
            if (active_bytes + bytes.size() > limit)
                throw std::runtime_error("[differential:staging_full] Upload staging capacity exhausted; "
                                         "retry after active uploads finish");
            auto pattern = (folder / "upload-XXXXXX").string();
            std::vector<char> writable(pattern.begin(), pattern.end());
            writable.push_back(0);
            fd = mkstemp(writable.data());
            if (fd < 0)
                throw std::runtime_error("Cannot create upload staging file");
            permissions.Apply(fd);
            path = writable.data();
            if (fcntl(fd, F_SETFD, FD_CLOEXEC))
                throw std::runtime_error("Cannot protect upload staging descriptor");
            if (flock(fd, LOCK_EX | LOCK_NB))
                throw std::runtime_error("Cannot own upload staging file");
            size_t done = 0;
            while (done < bytes.size()) {
                auto n = write(fd, bytes.data() + done, bytes.size() - done);
                if (n <= 0)
                    throw std::runtime_error("Cannot write upload staging file");
                done += size_t(n);
            }
            close(namespace_fd);
        } catch (...) {
            if (fd >= 0)
                close(fd);
            if (!path.empty())
                unlink(path.c_str());
            close(namespace_fd);
            throw;
        }
    }
    Stage(const Stage &) = delete;
    ~Stage() {
        // Serialize physical release with quota accounting and name reuse. If the
        // namespace lock is unavailable, leave a closed stale file for safe cleanup.
        int namespace_fd = open(namespace_path.c_str(), O_RDWR | O_CLOEXEC);
        bool locked = namespace_fd >= 0 && flock(namespace_fd, LOCK_EX) == 0;
        if (fd >= 0)
            close(fd);
        if (locked && !path.empty())
            unlink(path.c_str());
        if (namespace_fd >= 0)
            close(namespace_fd);
    }
};
std::vector<unsigned char> Encode(const Descriptor &d) {
    std::ostringstream out;
    out << "differential-export-v1\n"
        << d.engine << '\n'
        << d.database << '\n'
        << d.branch << '\n'
        << d.snapshot << '\n'
        << d.published_at << '\n'
        << d.root << '\n'
        << d.size << '\n'
        << d.pool << '\n'
        << d.pages.size() << '\n';
    for (size_t i = 0; i < d.pages.size(); i++) {
        out << d.pages[i] << ' ' << d.page_sizes[i] << '\n';
    }
    // The first index page follows as raw bytes, so a reader resolves the file header without a
    // separate request for it.
    if (!d.pages.empty())
        out << "first-index\n";
    auto s = out.str();
    std::vector<unsigned char> bytes(s.begin(), s.end());
    bytes.insert(bytes.end(), d.first_index.begin(), d.first_index.end());
    return bytes;
}
// Commit record (differential-commit-v1): one line per field; free text percent-encoded.
std::vector<unsigned char> EncodeCommit(Control &control, const Snapshot &s, int64_t parent,
                                        const std::string &descriptor) {
    std::ostringstream out;
    out << "differential-commit-v1\n"
        << "database " << s.database << '\n'
        << "database_name " << s.database_name << '\n'
        << "snapshot " << s.id << '\n'
        << "parent " << parent << '\n'
        << "branch " << s.branch << '\n'
        << "branch_name " << control.BranchName(s.branch) << '\n'
        << "published_at " << s.published_at << '\n'
        << "descriptor " << descriptor << '\n'
        << "name " << Escape(s.name) << '\n'
        << "author " << Escape(s.author) << '\n'
        << "message " << Escape(s.message) << '\n'
        << "extra_info " << Escape(s.extra_info) << '\n';
    auto text = out.str();
    return {text.begin(), text.end()};
}
// Names index entry (differential-name-v1): names/<database name>/<database uuid>.
std::vector<unsigned char> EncodeName(const std::string &database, const std::string &name) {
    auto text = "differential-name-v1\n" + database + "\n" + name + "\n";
    return {text.begin(), text.end()};
}
Descriptor Decode(const std::vector<unsigned char> &bytes) {
    if (bytes.size() > 1024 * 1024)
        throw std::runtime_error("Export descriptor exceeds 1 MiB limit");
    std::istringstream in(std::string(bytes.begin(), bytes.end()));
    auto line = [&] {
        std::string s;
        if (!std::getline(in, s) || s.size() > 1024)
            throw std::runtime_error("[differential:corrupt_data] Corrupt export descriptor");
        return s;
    };
    auto number = [&] {
        auto s = line();
        if (s.empty() || s.size() > 19 || s.find_first_not_of("0123456789") != std::string::npos)
            throw std::runtime_error("[differential:corrupt_data] Corrupt export integer");
        return std::stoull(s);
    };
    Descriptor d;
    if (line() != "differential-export-v1")
        throw std::runtime_error("[differential:incompatible_format] Unsupported export descriptor format");
    // Any well-formed engine line is accepted (descriptors written by DuckDB 1.5.6 say
    // "duckdb-v1.5.6"). Whether the native file is readable is DuckDB's decision at open.
    d.engine = line();
    if (d.engine.rfind("duckdb-", 0) != 0 || d.engine.size() > 64 || d.engine.size() < 8 ||
        d.engine.find_first_not_of("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.-_+") !=
            std::string::npos)
        throw std::runtime_error("[differential:corrupt_data] Corrupt export engine line");
    d.database = line();
    d.branch = line();
    auto uuid = [](const std::string &s) {
        return s.size() == 36 && s.find_first_not_of("0123456789abcdef-") == std::string::npos;
    };
    if (!uuid(d.database) || !uuid(d.branch))
        throw std::runtime_error("[differential:corrupt_data] Corrupt export identity");
    d.snapshot = int64_t(number());
    d.published_at = int64_t(number());
    d.root = line();
    d.size = number();
    d.pool = line();
    if (d.snapshot <= 0 || !IsHash(d.root) || d.size > (uint64_t(1) << 40) || d.pool != Pool(d.database))
        throw std::runtime_error("[differential:corrupt_data] Corrupt export bounds/namespace");
    auto count = number();
    auto expected = (d.size + Store::CHUNK * INDEX_CHUNKS - 1) / (Store::CHUNK * INDEX_CHUNKS);
    if (count != expected || count > 4096)
        throw std::runtime_error("[differential:corrupt_data] Corrupt export index count");
    for (uint64_t i = 0; i < count; i++) {
        auto entry = line();
        auto hash = entry.substr(0, 64);
        if (!IsHash(hash))
            throw std::runtime_error("[differential:corrupt_data] Corrupt index identity");
        auto size = entry.size() > 65 && entry[64] == ' ' ? entry.substr(65) : "";
        if (size.empty() || size.size() > 7 || size.find_first_not_of("0123456789") != std::string::npos ||
            std::stoull(size) < IndexSize(0) || std::stoull(size) > IndexSize(4096) ||
            (std::stoull(size) - IndexSize(0)) % PACK_ENTRY)
            throw std::runtime_error("[differential:corrupt_data] Corrupt index size");
        d.page_sizes.push_back(std::stoull(size));
        d.pages.push_back(hash);
    }
    if (count > 0) {
        if (line() != "first-index")
            throw std::runtime_error("[differential:corrupt_data] Export descriptor lacks its first index page");
        auto start = uint64_t(in.tellg());
        d.first_index.assign(bytes.begin() + int64_t(start), bytes.end());
        if (d.first_index.size() != d.page_sizes[0] || Hash(d.first_index) != d.pages[0])
            throw std::runtime_error("[differential:corrupt_data] Corrupt inline first index page");
        return d;
    }
    if (in.peek() != EOF)
        throw std::runtime_error("[differential:corrupt_data] Trailing export descriptor bytes");
    return d;
}
std::shared_ptr<S3Provider> Client(Control &control, const std::string &id) {
    static std::mutex mutex;
    static std::map<std::string, std::weak_ptr<S3Provider>> clients;
    std::lock_guard<std::mutex> guard(mutex);
    for (auto it = clients.begin(); it != clients.end();) {
        if (it->second.expired())
            it = clients.erase(it);
        else
            ++it;
    }
    auto key = control.store->directory + "/" + id;
    auto client = clients[key].lock();
    if (!client) {
        client = std::make_shared<S3Provider>(control.Provider(id).config);
        clients[key] = client;
    }
    // Keep the most recently used clients alive between operations (an export followed by its
    // branch-head update, consecutive ATTACHes), so they reuse the client and its connections.
    static std::deque<std::shared_ptr<S3Provider>> recent;
    recent.erase(std::remove(recent.begin(), recent.end(), client), recent.end());
    recent.push_front(client);
    if (recent.size() > 8)
        recent.pop_back();
    return client;
}
// Run tasks on at most `width` threads; the first failure is rethrown after all finish.
void Parallel(size_t count, unsigned width, const std::function<void(size_t)> &task) {
    if (count == 0)
        return;
    width = unsigned(std::max<size_t>(1, std::min<size_t>(width, count)));
    std::atomic<size_t> next{0};
    std::mutex error_mutex;
    std::exception_ptr error;
    auto worker = [&] {
        while (true) {
            auto i = next++;
            if (i >= count)
                return;
            {
                std::lock_guard<std::mutex> guard(error_mutex);
                if (error)
                    return;
            }
            try {
                task(i);
            } catch (...) {
                std::lock_guard<std::mutex> guard(error_mutex);
                if (!error)
                    error = std::current_exception();
            }
        }
    };
    std::vector<std::thread> threads;
    for (unsigned i = 1; i < width; i++)
        threads.emplace_back(worker);
    worker();
    for (auto &thread : threads)
        thread.join();
    if (error)
        std::rethrow_exception(error);
}
} // namespace

// ---------------------------------------------------------------------------------------
// Export
namespace {
// The newest snapshot number published for a database: the first key of its newest-first commit
// index (one LIST max-keys=1); 0 when none.
int64_t NewestSnapshot(S3Provider &provider, const std::string &database) {
    auto within = Pool(database) + "snapshots/";
    auto newest = provider.List("", 1, within);
    if (newest.keys.empty())
        return 0;
    auto inverted = newest.keys.front().substr(within.size());
    if (inverted.size() != 10 || inverted.find_first_not_of("0123456789") != std::string::npos)
        throw std::runtime_error("[differential:corrupt_data] Corrupt commit record key");
    return SNAPSHOT_KEY_MAX - std::stoll(inverted);
}
// Serializes this repository's exports of one shared database (in order, with renumbering).
struct ExportLock {
    int fd = -1;
    ExportLock(Control &control, const std::string &database) {
        auto path = std::filesystem::path(control.store->directory) / "locks";
        control.store->permissions.CreateDirectories(path);
        fd = control.store->permissions.Open(path / ("exports-" + database), O_RDWR | O_CLOEXEC);
        if (fd < 0 || flock(fd, LOCK_EX))
            throw std::runtime_error("Cannot lock snapshot export");
    }
    ~ExportLock() {
        if (fd >= 0)
            close(fd);
    }
};
std::string ExportOne(Control &control, const std::string &database, int64_t &snapshot,
                      const std::function<bool()> &cancelled, bool shared) {
    Timer timer(Metric::Export);
    auto e = control.Export(database, snapshot);
    if (e.id.empty())
        return "";
    if (e.state == "complete")
        return e.descriptor;
    if (e.state == "abandoned")
        throw std::runtime_error("[differential:head_conflict] The export of snapshot " + std::to_string(snapshot) +
                                 " was abandoned: another writer published that snapshot number");
    auto check_cancel = [&] {
        if (cancelled && cancelled())
            throw std::runtime_error("[differential:cancelled] Export cancelled; durable pending retention preserved");
    };
    if (auto gate = Hook("DIFFERENTIAL_EXPORT_GATE")) {
        std::ofstream(std::string(gate) + ".entered") << "pending";
        auto end = std::chrono::steady_clock::now() + std::chrono::seconds(15);
        while (!std::filesystem::exists(std::string(gate) + ".release")) {
            check_cancel();
            if (std::chrono::steady_clock::now() > end)
                throw std::runtime_error("Export test gate timed out");
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }
    check_cancel();
    // EXPORT_PENDING already retains the complete graph in the publication transaction.
    auto s = control.GetSnapshot(database, snapshot, false);
    auto provider = Client(control, e.provider);
    auto &config = provider->Config();
    auto repository = control.RepositoryID();
    Descriptor d;
    auto engine = Hook("DIFFERENTIAL_TEST_ENGINE_LINE"); // test builds: simulate another writer version
    d.engine = engine ? std::string(engine) : std::string("duckdb-") + duckdb::DuckDB::LibraryVersion();
    d.database = s.database;
    d.branch = s.branch;
    d.snapshot = s.id;
    d.published_at = s.published_at;
    d.root = s.root;
    d.size = uint64_t(s.size);
    d.pool = Pool(s.database);
    control.store->RootSize(s.root);
    // Verified immutable objects in the owned namespace are trusted by their local record;
    // new objects are uploaded conditionally and read back before being recorded.
    auto upload = [&](const std::string &key, const std::vector<unsigned char> &bytes, const std::string &hash) {
        check_cancel();
        if (control.RemoteVerified(e.provider, key, hash, bytes.size()))
            return;
        {
            std::optional<Timer> staging(std::in_place, Metric::ExportStage);
            Stage stage(control.store->directory, repository, bytes);
            staging.reset();
            Timer put(Metric::ExportPut);
            provider->PutFile(key, stage.path, hash);
        }
        Timer verify(Metric::ExportVerify);
        auto stored = provider->Head(key);
        if (stored.size != bytes.size() || stored.sha256 != hash)
            throw std::runtime_error("[differential:corrupt_data] Uploaded dependency identity/length mismatch");
        check_cancel();
        std::unique_ptr<EVP_MD_CTX, decltype(&EVP_MD_CTX_free)> digest(EVP_MD_CTX_new(), EVP_MD_CTX_free);
        if (!digest || EVP_DigestInit_ex(digest.get(), EVP_sha256(), nullptr) != 1)
            throw std::runtime_error("SHA256 init failed");
        for (uint64_t offset = 0; offset < bytes.size(); offset += config.max_range_bytes) {
            auto length = std::min<uint64_t>(config.max_range_bytes, bytes.size() - offset);
            auto part = provider->ReadSpan(key, offset, length);
            if (EVP_DigestUpdate(digest.get(), part.data(), part.size()) != 1)
                throw std::runtime_error("SHA256 update failed");
        }
        unsigned char verified[32];
        unsigned n = 0;
        if (EVP_DigestFinal_ex(digest.get(), verified, &n) != 1 || n != 32 || Hex(verified, 32) != hash)
            throw std::runtime_error("[differential:corrupt_data] Uploaded dependency failed read-back verification");
        control.RecordRemoteVerified(e.provider, key, hash, bytes.size());
    };
    auto total_chunks = (d.size + Store::CHUNK - 1) / Store::CHUNK;
    auto pages = (total_chunks + INDEX_CHUNKS - 1) / INDEX_CHUNKS;
    // Pass 1: chunks this provider has never stored, in first-appearance (file) order so a
    // pack holds neighbouring file ranges and later reads coalesce.
    std::vector<std::string> missing;
    std::set<std::string> seen;
    for (uint64_t page = 0; page < pages; page++) {
        check_cancel();
        auto refs = control.store->RootChunks(s.root, page * INDEX_CHUNKS, INDEX_CHUNKS);
        std::vector<std::string> hashes;
        for (auto &ref : refs)
            if (seen.insert(ref.object).second)
                hashes.push_back(ref.object);
        auto known = control.FindRemoteChunks(e.provider, hashes);
        for (auto &hash : hashes) {
            if (known.count(hash))
                ++chunks_reused;
            else
                missing.push_back(hash);
        }
    }
    // Packs stay below the multipart threshold so product objects never use multipart.
    auto per_pack = std::max<uint64_t>(
        1, std::min<uint64_t>(MAX_PACK_CHUNKS, (config.multipart_threshold - 1) / Store::CHUNK));
    std::vector<std::vector<std::string>> groups;
    for (size_t i = 0; i < missing.size(); i += per_pack)
        groups.emplace_back(missing.begin() + int64_t(i),
                            missing.begin() + int64_t(std::min<size_t>(missing.size(), i + per_pack)));
    Parallel(groups.size(), provider->Concurrency(), [&](size_t i) {
        auto &chunks = groups[i];
        std::vector<unsigned char> pack;
        pack.reserve(chunks.size() * Store::CHUNK);
        {
            Timer build(Metric::ExportPackBuild);
            for (auto &hash : chunks) {
                auto bytes = control.store->Object(hash);
                pack.insert(pack.end(), bytes->begin(), bytes->end());
            }
        }
        auto hash = Hash(pack);
        upload(d.pool + "packs/" + hash, pack, hash);
        control.RecordPack(e.provider, s.database, hash, pack.size(), chunks);
        ++packs_uploaded;
        chunks_uploaded += chunks.size();
    });
    // Pass 2: chunk indexes resolve every logical chunk to (identity, pack, slot).
    for (uint64_t page = 0; page < pages; page++) {
        check_cancel();
        auto refs = control.store->RootChunks(s.root, page * INDEX_CHUNKS, INDEX_CHUNKS);
        std::vector<std::string> hashes;
        for (auto &ref : refs)
            hashes.push_back(ref.object);
        auto located = control.FindRemoteChunks(e.provider, hashes);
        // Packs by (pool, pack): a chunk may live in another database's pool (e.g. a clone's source).
        std::map<std::pair<std::string, std::string>, uint16_t> pack_numbers;
        std::vector<std::pair<std::string, std::string>> packs;
        for (auto &ref : refs) {
            auto found = located.find(ref.object);
            if (found == located.end())
                throw std::runtime_error("[differential:corrupt_data] Exported chunk has no recorded pack");
            auto pack = std::make_pair(found->second.pool, found->second.pack);
            if (pack_numbers.emplace(pack, uint16_t(packs.size())).second)
                packs.push_back(pack);
        }
        std::vector<unsigned char> index(IndexSize(packs.size()), 0);
        std::memcpy(index.data(), "DIX1", 4);
        for (int b = 0; b < 4; b++)
            index[4 + b] = static_cast<unsigned char>(packs.size() >> (8 * b));
        for (size_t i = 0; i < packs.size(); i++) {
            UnhexUUID(packs[i].first, index.data() + INDEX_HEADER + PACK_ENTRY * i);
            Unhex(packs[i].second, index.data() + INDEX_HEADER + PACK_ENTRY * i + 16);
        }
        auto entries = index.data() + INDEX_HEADER + PACK_ENTRY * packs.size();
        for (uint64_t slot = 0; slot < INDEX_CHUNKS; slot++) {
            entries[slot * INDEX_ENTRY + 32] = INDEX_HOLE & 0xFF;
            entries[slot * INDEX_ENTRY + 33] = INDEX_HOLE >> 8;
        }
        for (auto &ref : refs) {
            auto &location = located.at(ref.object);
            auto entry = entries + (ref.chunk % INDEX_CHUNKS) * INDEX_ENTRY;
            auto number = pack_numbers.at(std::make_pair(location.pool, location.pack));
            Unhex(ref.object, entry);
            entry[32] = static_cast<unsigned char>(number & 0xFF);
            entry[33] = static_cast<unsigned char>(number >> 8);
            entry[34] = static_cast<unsigned char>(location.slot & 0xFF);
            entry[35] = static_cast<unsigned char>(location.slot >> 8);
        }
        auto hash = Hash(index);
        upload(d.pool + "indexes/" + hash, index, hash);
        d.pages.push_back(hash);
        d.page_sizes.push_back(index.size());
        if (page == 0)
            d.first_index = index;
    }
    Crash("dependencies_uploaded");
    // The descriptor names the snapshot number, the commit record its descriptor. History: an
    // immutable commit record per exported snapshot (newest first in a listing), create-only and
    // deterministic, so a retry finds it in place.
    std::string key;
    for (int attempt = 1;; attempt++) {
        d.snapshot = s.id;
        auto bytes = Encode(d);
        auto hash = Hash(bytes);
        key = "descriptors/" + hash;
        auto commit = EncodeCommit(control, s, control.ParentSnapshot(database, s.id), key);
        auto commit_key = CommitKey(s.database, s.id);
        if (shared) {
            // Writers of a shared database number snapshots through the commit index: a number
            // another writer published since is skipped, unless the record there is this one.
            auto taken = NewestSnapshot(*provider, s.database);
            if (taken >= s.id) {
                auto existing = provider->TryHead(commit_key);
                if (!existing || existing->sha256 != Hash(commit)) {
                    snapshot += control.RenumberSnapshots(database, s.id, taken);
                    Failpoint("DIFFERENTIAL_SHARED_CRASH", "renumbered");
                    s = control.GetSnapshot(database, snapshot, false);
                    continue;
                }
            }
        }
        upload(key, bytes, hash);
        Crash("descriptor_uploaded");
        try {
            upload(commit_key, commit, Hash(commit));
            Failpoint("DIFFERENTIAL_SHARED_CRASH", "commit_recorded");
            break;
        } catch (const std::exception &error) {
            if (std::string(error.what()).find("Immutable object identity conflict") == std::string::npos)
                throw;
            // Shared: another writer took the number between the listing and this write.
            if (shared && attempt < 8)
                continue;
            if (shared)
                throw std::runtime_error("[differential:busy] Could not number snapshot " + std::to_string(s.id) +
                                         " of database " + s.database_name + ": other writers kept taking the next "
                                         "numbers. The export stays pending; retry it with differential_retry_export.");
            // Another writer with this database's identity (e.g. a copy of this repository)
            // already published this snapshot number: a split brain, not a transport error.
            throw std::runtime_error("[differential:head_conflict] Snapshot " + std::to_string(s.id) +
                                     " of database " + s.database_name + " was already published by another writer, "
                                     "for example a copy of this repository on another host; this export stays "
                                     "pending. If this repository is the only writer of the database, run "
                                     "differential_claim_branch_heads('" + s.database_name +
                                     "') and create a new snapshot.");
        }
    }
    auto name = EncodeName(s.database, s.database_name);
    upload("names/" + s.database_name + "/" + s.database, name, Hash(name));
    control.ExportComplete(e.id, key);
    Crash("completion_committed");
    return key;
}
} // namespace
std::string ExportSnapshot(Control &control, const std::string &database, int64_t &snapshot,
                           std::function<bool()> cancelled) {
    if (control.Writers(database) != "shared")
        return ExportOne(control, database, snapshot, cancelled, false);
    auto e = control.Export(database, snapshot);
    if (e.id.empty() || e.state == "complete")
        return e.descriptor;
    ExportLock lock(control, control.FindDatabase(database));
    // In order: a commit record names its parent, which must be published first. Renumbering moves
    // every unexported snapshot, the requested one included.
    while (true) {
        auto pending = control.PendingExports(database);
        if (pending.empty() || pending.front() >= snapshot)
            break;
        auto first = pending.front(), exported = first;
        ExportOne(control, database, exported, cancelled, true);
        snapshot += exported - first;
    }
    return ExportOne(control, database, snapshot, cancelled, true);
}

// ---------------------------------------------------------------------------------------
// Remote reads
namespace {
void CheckIndex(const Bytes &index) {
    uint64_t packs = 0;
    for (int b = 0; b < 4 && size_t(4 + b) < index.size(); b++)
        packs |= uint64_t(index[4 + b]) << (8 * b);
    if (index.size() < IndexSize(0) || std::memcmp(index.data(), "DIX1", 4) != 0 || index.size() != IndexSize(packs))
        throw std::runtime_error("[differential:corrupt_data] Corrupt remote index page");
}
// Recently read descriptors, process-wide and verified (immutable, named by their hash).
std::mutex descriptor_mutex;
std::list<std::pair<std::string, SharedBytes>> descriptors;
SharedBytes KnownDescriptor(const std::string &key) {
    std::lock_guard<std::mutex> guard(descriptor_mutex);
    for (auto it = descriptors.begin(); it != descriptors.end(); ++it)
        if (it->first == key) {
            descriptors.splice(descriptors.begin(), descriptors, it);
            return it->second;
        }
    return nullptr;
}
void RememberDescriptor(const std::string &key, SharedBytes bytes) {
    std::lock_guard<std::mutex> guard(descriptor_mutex);
    for (auto &entry : descriptors)
        if (entry.first == key)
            return;
    descriptors.emplace_front(key, std::move(bytes));
    if (descriptors.size() > 64)
        descriptors.pop_back();
}
} // namespace
// Fetches in progress, process-wide: concurrent readers of one object or chunk share one GET.
struct Pending {
    std::mutex mutex;
    std::unordered_map<std::string, std::shared_future<SharedBytes>> fetches;
};
Pending &InFlight() {
    static Pending pending;
    return pending;
}
RemoteSnapshot::RemoteSnapshot(Control &control, const std::string &provider_id, const std::string &key,
                               RemoteCaching caching_p)
    : caching(std::move(caching_p)) {
    Timer timer(Metric::RemoteDescriptorOpen);
    if (key.rfind("descriptors/", 0) != 0 || !IsHash(key.substr(12)))
        throw std::runtime_error("Expected immutable descriptors/<SHA256> locator");
    auto p = control.Provider(provider_id);
    provider = Client(control, p.id);
    pending_namespace = control.store->directory + "/" + p.id + "/";
    // Shown by duckdb_external_file_cache(); a scheme of its own keeps it apart from httpfs entries.
    object_prefix = "differential:s3://" + p.config.bucket + "/" + p.config.prefix;
    if (caching.disk_limit)
        disk = DiskCache::For(control.store->directory, control.store->permissions);
    // Descriptors are immutable and named by their hash: one GET, verified, then kept in process
    // memory (a branch-head ATTACH reads it twice) and in the disk cache when enabled. Both are per
    // repository, so a DuckDB instance using another repository in the same process never gets a
    // descriptor without its own S3 access.
    auto hash = key.substr(12);
    auto cache_key = pending_namespace + key;
    auto bytes = KnownDescriptor(cache_key);
    if (!bytes && disk)
        if (auto stored = disk->Read(hash, 0))
            bytes = std::make_shared<const Bytes>(std::move(*stored));
    if (!bytes) {
        bytes = std::make_shared<const Bytes>(provider->ReadObject(key, 1024 * 1024, hash));
        Keep(hash, bytes);
    }
    RememberDescriptor(cache_key, bytes);
    descriptor = Decode(*bytes);
    if (!descriptor.pages.empty()) {
        auto index = std::make_shared<const Bytes>(descriptor.first_index);
        CheckIndex(*index);
        last_index = index;
        last_index_page = 0;
        if (caching.memory && caching.fill)
            caching.memory->Insert(object_prefix + descriptor.pool + "indexes/" + descriptor.pages[0], 0,
                                   index->data(), index->size());
    }
}
// Verified bytes of an immutable object range from the memory cache.
SharedBytes RemoteSnapshot::FromMemory(const std::string &object, uint64_t offset, uint64_t length) {
    if (!caching.memory)
        return nullptr;
    auto bytes = std::make_shared<Bytes>(size_t(length));
    if (!caching.memory->Read(object_prefix + object, offset, length, bytes->data()))
        return nullptr;
    ++cache_hits;
    return bytes;
}
// ... from the repository disk cache (by content hash); a hit is promoted to memory.
SharedBytes RemoteSnapshot::FromDisk(const std::string &object, uint64_t offset, uint64_t length,
                                     const std::string &hash) {
    if (!disk)
        return nullptr;
    auto stored = disk->Read(hash, length);
    if (!stored)
        return nullptr;
    auto bytes = std::make_shared<const Bytes>(std::move(*stored));
    if (caching.memory && caching.fill)
        caching.memory->Insert(object_prefix + object, offset, bytes->data(), length);
    return bytes;
}
void RemoteSnapshot::Keep(const std::string &hash, const SharedBytes &bytes) {
    if (disk && caching.fill)
        disk->Write(hash, bytes, caching.disk_limit);
}
// One verified immutable object. Concurrent requests for the same key share one lookup and GET;
// no lock is held across disk or network I/O.
SharedBytes RemoteSnapshot::FetchObject(const std::string &key, uint64_t size, const std::string &hash) {
    if (auto cached = FromMemory(key, 0, size))
        return cached;
    auto pending_key = pending_namespace + key;
    auto &pending = InFlight();
    std::promise<SharedBytes> promise;
    {
        std::unique_lock<std::mutex> guard(pending.mutex);
        auto waiting = pending.fetches.find(pending_key);
        if (waiting != pending.fetches.end()) {
            auto future = waiting->second;
            guard.unlock();
            return future.get();
        }
        // A fetch that finished since the first check has filled the memory cache.
        if (auto cached = FromMemory(key, 0, size))
            return cached;
        pending.fetches.emplace(pending_key, promise.get_future().share());
        ++cache_misses;
    }
    try {
        SharedBytes bytes = FromDisk(key, 0, size, hash);
        if (!bytes) {
            bytes = std::make_shared<const Bytes>(provider->ReadRange(key, 0, size, size, hash));
            if (caching.memory && caching.fill)
                caching.memory->Insert(object_prefix + key, 0, bytes->data(), size);
            Keep(hash, bytes);
        }
        std::lock_guard<std::mutex> guard(pending.mutex);
        pending.fetches.erase(pending_key);
        promise.set_value(bytes);
        return bytes;
    } catch (...) {
        std::lock_guard<std::mutex> guard(pending.mutex);
        pending.fetches.erase(pending_key);
        promise.set_exception(std::current_exception());
        throw;
    }
}
SharedBytes RemoteSnapshot::Index(uint64_t page) {
    Timer lookup(Metric::RemoteManifestLookup);
    if (page >= descriptor.pages.size())
        throw std::runtime_error("[differential:corrupt_data] Remote index page out of range");
    {
        // Consecutive reads mostly stay within one index page (256 MiB of the file).
        std::lock_guard<std::mutex> guard(index_mutex);
        if (last_index && last_index_page == page)
            return last_index;
    }
    auto index = FetchObject(descriptor.pool + "indexes/" + descriptor.pages[page], descriptor.page_sizes[page],
                             descriptor.pages[page]);
    CheckIndex(*index);
    std::lock_guard<std::mutex> guard(index_mutex);
    last_index_page = page;
    last_index = index;
    return index;
}
RemoteSnapshot::Location RemoteSnapshot::Locate(const Bytes &index, uint64_t slot) const {
    Location location;
    uint64_t packs = (index.size() - IndexSize(0)) / PACK_ENTRY;
    auto entry = index.data() + INDEX_HEADER + PACK_ENTRY * packs + slot * INDEX_ENTRY;
    auto number = uint16_t(entry[32] | (entry[33] << 8));
    if (number == INDEX_HOLE)
        return location;
    if (number >= packs)
        throw std::runtime_error("[differential:corrupt_data] Corrupt remote pack reference");
    location.hole = false;
    location.hash = Hex(entry, 32);
    location.pool = HexUUID(index.data() + INDEX_HEADER + PACK_ENTRY * number);
    location.pack = Hex(index.data() + INDEX_HEADER + PACK_ENTRY * number + 16, 32);
    location.slot = uint64_t(entry[34] | (entry[35] << 8));
    if (location.slot >= MAX_PACK_CHUNKS)
        throw std::runtime_error("[differential:corrupt_data] Corrupt remote pack slot");
    return location;
}
// Fetch verified chunks. Missing chunks of one pack at consecutive slots are coalesced into
// one bounded range GET; independent ranges run concurrently within the provider permits.
void RemoteSnapshot::FetchChunks(const std::vector<uint64_t> &chunks, std::map<uint64_t, SharedBytes> &out) {
    struct Want {
        Location location;
        std::vector<uint64_t> chunks;
        std::shared_ptr<std::promise<SharedBytes>> promise;
        std::shared_future<SharedBytes> future;
    };
    std::map<std::string, Want> wanted; // by chunk identity
    std::vector<std::pair<uint64_t, std::shared_future<SharedBytes>>> waiting;
    std::map<uint64_t, SharedBytes> indexes;
    for (auto chunk : chunks) {
        auto page = chunk / INDEX_CHUNKS;
        if (!indexes.count(page))
            indexes.emplace(page, Index(page));
        auto location = Locate(*indexes.at(page), chunk % INDEX_CHUNKS);
        if (location.hole) {
            static const SharedBytes zero = std::make_shared<const Bytes>(Store::CHUNK, 0);
            out[chunk] = zero;
            continue;
        }
        auto found = wanted.find(location.hash);
        if (found != wanted.end()) {
            found->second.chunks.push_back(chunk);
            continue;
        }
        wanted.emplace(location.hash, Want{location, {chunk}, nullptr, {}});
    }
    std::vector<Want *> claimed, fetch;
    auto pack = [&](const Location &location) { return Pool(location.pool) + "packs/" + location.pack; };
    auto &pending = InFlight();
    for (auto &item : wanted) {
        auto &location = item.second.location;
        auto offset = location.slot * Store::CHUNK;
        auto bytes = FromMemory(pack(location), offset, Store::CHUNK);
        if (!bytes) {
            std::lock_guard<std::mutex> guard(pending.mutex);
            auto key = pending_namespace + "chunk:" + item.first;
            auto found = pending.fetches.find(key);
            if (found != pending.fetches.end()) {
                for (auto chunk : item.second.chunks)
                    waiting.emplace_back(chunk, found->second);
                continue;
            }
            // A fetch that finished since the first check has filled the memory cache.
            bytes = FromMemory(pack(location), offset, Store::CHUNK);
            if (!bytes) {
                item.second.promise = std::make_shared<std::promise<SharedBytes>>();
                item.second.future = item.second.promise->get_future().share();
                pending.fetches.emplace(key, item.second.future);
                ++cache_misses;
                claimed.push_back(&item.second);
                continue;
            }
        }
        for (auto chunk : item.second.chunks)
            out[chunk] = bytes;
    }
    auto finish = [&](Want *item, SharedBytes bytes) {
        std::lock_guard<std::mutex> guard(pending.mutex);
        pending.fetches.erase(pending_namespace + "chunk:" + item->location.hash);
        item->promise->set_value(bytes);
    };
    // Claimed chunks: the repository disk cache first, then S3.
    for (auto *item : claimed) {
        if (auto bytes = FromDisk(pack(item->location), item->location.slot * Store::CHUNK, Store::CHUNK,
                                  item->location.hash))
            finish(item, bytes);
        else
            fetch.push_back(item);
    }
    // Plan ranges.
    std::vector<std::vector<Want *>> ranges;
    std::sort(fetch.begin(), fetch.end(), [](Want *a, Want *b) {
        return a->location.pack != b->location.pack ? a->location.pack < b->location.pack
                                                    : a->location.slot < b->location.slot;
    });
    // Coalesce adjacent slots, but split large fetches so they spread over the parallel
    // requests: at least 1 MiB and at most MAX_RANGE_BYTES per request.
    auto width = provider->Concurrency();
    auto max_chunks = std::max<uint64_t>(1, provider->Config().max_range_bytes / Store::CHUNK);
    max_chunks = std::min(max_chunks, std::max<uint64_t>(REQUEST_CHUNKS, (fetch.size() + width - 1) / width));
    for (auto *item : fetch) {
        if (!ranges.empty()) {
            auto *last = ranges.back().back();
            if (last->location.pack == item->location.pack && last->location.slot + 1 == item->location.slot &&
                ranges.back().size() < max_chunks) {
                ranges.back().push_back(item);
                continue;
            }
        }
        ranges.push_back({item});
    }

    for (auto &range : ranges)
        if (range.size() > 1)
            ++coalesced_requests;
    std::exception_ptr failure;
    try {
        Parallel(ranges.size(), width, [&](size_t i) {
            auto &range = ranges[i];
            auto first = range.front()->location.slot;
            auto bytes = provider->ReadSpan(pack(range.front()->location), first * Store::CHUNK,
                                            range.size() * Store::CHUNK);
            for (size_t k = 0; k < range.size(); k++)
                if (Hash(bytes.data() + k * Store::CHUNK, Store::CHUNK) != range[k]->location.hash)
                    throw std::runtime_error("[differential:corrupt_data] Corrupt S3 pack chunk checksum");
            // The whole verified span is one memory-cache range (fewer, larger entries).
            if (caching.memory && caching.fill)
                caching.memory->Insert(object_prefix + pack(range.front()->location), first * Store::CHUNK,
                                       bytes.data(), bytes.size());
            for (size_t k = 0; k < range.size(); k++) {
                auto begin = bytes.data() + k * Store::CHUNK;
                auto chunk = std::make_shared<const Bytes>(begin, begin + Store::CHUNK);
                Keep(range[k]->location.hash, chunk);
                finish(range[k], chunk);
            }
        });
    } catch (...) {
        failure = std::current_exception();
    }
    // Release every claimed but unfinished request so concurrent waiters fail rather than hang.
    {
        std::lock_guard<std::mutex> guard(pending.mutex);
        for (auto *item : claimed) {
            auto key = pending_namespace + "chunk:" + item->location.hash;
            auto found = pending.fetches.find(key);
            if (found != pending.fetches.end() && item->promise) {
                pending.fetches.erase(found);
                try {
                    item->promise->set_exception(failure ? failure
                                                         : std::make_exception_ptr(std::runtime_error(
                                                               "[differential:unavailable_object] Remote fetch aborted")));
                } catch (const std::future_error &) {
                }
            }
        }
    }
    if (failure)
        std::rethrow_exception(failure);
    for (auto *item : claimed) {
        auto bytes = item->future.get();
        for (auto chunk : item->chunks)
            out[chunk] = bytes;
    }
    for (auto &pending : waiting)
        out[pending.first] = pending.second.get();
}
uint64_t RemoteSnapshot::Read(void *data, uint64_t length, uint64_t offset) {
    if (offset >= descriptor.size)
        return 0;
    length = std::min(length, descriptor.size - offset);
    if (length == 0)
        return 0;
    logical_read_bytes += length;
    auto first = offset / Store::CHUNK, last = (offset + length - 1) / Store::CHUNK;
    std::vector<uint64_t> chunks;
    for (auto c = first; c <= last; c++)
        chunks.push_back(c);
    // Read-ahead, per sequential stream (each scanning thread reads its row groups in order).
    // A stream starts after two consecutive block-sized reads; small header/metadata reads of
    // selective queries never start one. Only a read that needs data not yet requested by its
    // stream fetches ahead, together with its own chunks: one round trip per window instead of
    // one per read. The window starts at the read size and doubles per refill up to 4 MiB,
    // within the same index page. Read-ahead is pointless when nothing would keep the data.
    bool keep = (caching.memory && caching.memory->Enabled()) || disk;
    if (keep && length >= Store::CHUNK) {
        std::lock_guard<std::mutex> guard(stream_mutex);
        Stream *stream = nullptr;
        for (auto &candidate : streams)
            if (candidate.next == offset) {
                stream = &candidate;
                break;
            }
        if (!stream) {
            if (streams.size() < MAX_STREAMS)
                streams.emplace_back();
            stream = &*std::min_element(streams.begin(), streams.end(),
                                        [](const Stream &a, const Stream &b) { return a.used < b.used; });
            *stream = Stream();
        }
        stream->reads++;
        stream->next = offset + length;
        stream->used = ++stream_clock;
        if (stream->reads >= 2 && last >= stream->ahead) {
            auto read_chunks = last - first + 1;
            stream->window = std::min<uint64_t>(MAX_WINDOW_CHUNKS,
                                                stream->window ? stream->window * 2 : read_chunks);
            auto end = std::min<uint64_t>({last + stream->window, (descriptor.size - 1) / Store::CHUNK,
                                           (last / INDEX_CHUNKS + 1) * INDEX_CHUNKS - 1});
            for (auto c = std::max(last + 1, stream->ahead); c <= end; c++) {
                chunks.push_back(c);
                ++prefetched_chunks;
            }
            stream->ahead = end + 1;
        }
    }
    std::map<uint64_t, SharedBytes> fetched;
    FetchChunks(chunks, fetched);
    auto out = static_cast<unsigned char *>(data);
    uint64_t done = 0;
    while (done < length) {
        auto pos = offset + done;
        auto take = std::min(length - done, Store::CHUNK - pos % Store::CHUNK);
        std::memcpy(out + done, fetched.at(pos / Store::CHUNK)->data() + pos % Store::CHUNK, size_t(take));
        done += take;
    }
    return length;
}
Root RemoteSnapshot::Materialize(Store &store, int64_t file) {
    // Reconstruct exact sparse extent identity; copying holes as explicit zero objects would change the
    // manifest.
    for (uint64_t page = 0; page < descriptor.pages.size(); page++) {
        auto index = Index(page);
        std::vector<uint64_t> present;
        for (uint64_t slot = 0; slot < INDEX_CHUNKS; slot++) {
            auto chunk = page * INDEX_CHUNKS + slot;
            if (chunk * Store::CHUNK >= descriptor.size)
                break;
            if (!Locate(*index, slot).hole)
                present.push_back(chunk);
        }
        for (size_t i = 0; i < present.size(); i += 64) {
            std::vector<uint64_t> batch(present.begin() + int64_t(i),
                                        present.begin() + int64_t(std::min(present.size(), i + 64)));
            std::map<uint64_t, SharedBytes> fetched;
            FetchChunks(batch, fetched);
            for (auto chunk : batch) {
                auto offset = chunk * Store::CHUNK;
                store.Write(file, fetched.at(chunk)->data(), std::min<uint64_t>(Store::CHUNK, descriptor.size - offset),
                            offset);
            }
        }
    }
    store.Truncate(file, descriptor.size);
    // The sealed copy has the exported root's identity (same chunk map, same manifest pages).
    auto root = store.Seal(file);
    if (root.id != descriptor.root)
        throw std::runtime_error("[differential:corrupt_data] Recovered bytes do not match the immutable export root");
    return root;
}
std::vector<std::pair<ChunkRef, RemoteChunk>> RemoteSnapshot::Entries(uint64_t page) {
    auto index = Index(page);
    std::vector<std::pair<ChunkRef, RemoteChunk>> entries;
    for (uint64_t slot = 0; slot < INDEX_CHUNKS; slot++) {
        auto chunk = page * INDEX_CHUNKS + slot;
        if (chunk * Store::CHUNK >= descriptor.size)
            break;
        auto location = Locate(*index, slot);
        if (!location.hole)
            entries.push_back({ChunkRef{chunk, location.hash}, RemoteChunk{location.pack, location.pool, location.slot}});
    }
    return entries;
}
namespace {
std::atomic<uint64_t> remote_object_cache_limit{0};
} // namespace
void SetRemoteObjectCacheLimit(uint64_t bytes) { remote_object_cache_limit = bytes; }
std::map<std::string, SharedBytes> FetchRemoteObjects(Control &control, const std::vector<std::string> &hashes,
                                                      const RemoteCaching *caching) {
    struct Wanted {
        std::string hash;
        RemoteChunk location;
    };
    // Where each chunk is stored, by S3 location (usually one).
    std::map<std::string, std::vector<Wanted>> by_provider;
    std::set<std::string> missing(hashes.begin(), hashes.end());
    for (auto &provider : control.S3Providers()) {
        if (missing.empty())
            break;
        auto found = control.FindRemoteChunks(provider.id, std::vector<std::string>(missing.begin(), missing.end()));
        for (auto &entry : found) {
            by_provider[provider.id].push_back({entry.first, entry.second});
            missing.erase(entry.first);
        }
    }
    if (!missing.empty())
        throw std::runtime_error("[differential:unavailable_object] Remote object has no recorded S3 location");
    std::map<std::string, SharedBytes> out;
    std::mutex out_mutex;
    auto limit = caching ? caching->disk_limit : remote_object_cache_limit.load();
    auto disk = limit ? DiskCache::For(control.store->directory, control.store->permissions) : nullptr;
    auto memory = caching && caching->memory && caching->memory->Enabled() ? caching->memory : nullptr;
    bool fill = !caching || caching->fill;
    for (auto &group : by_provider) {
        auto provider = Client(control, group.first);
        auto &config = provider->Config();
        // Memory cache entries are pack ranges under the same names export reads use.
        auto object_prefix = "differential:s3://" + config.bucket + "/" + config.prefix;
        auto pack = [&](const RemoteChunk &location) { return Pool(location.pool) + "packs/" + location.pack; };
        // Consecutive slots of one pack become one range request (bounded by MAX_RANGE_BYTES).
        auto &wanted = group.second;
        std::vector<Wanted> fetch;
        for (auto &item : wanted) {
            if (memory) {
                auto bytes = std::make_shared<Bytes>(size_t(Store::CHUNK));
                if (memory->Read(object_prefix + pack(item.location), item.location.slot * Store::CHUNK,
                                 Store::CHUNK, bytes->data()) &&
                    Hash(bytes->data(), Store::CHUNK) == item.hash) {
                    ++cache_hits;
                    out[item.hash] = bytes;
                    continue;
                }
            }
            if (disk)
                if (auto cached = disk->Read(item.hash, Store::CHUNK)) {
                    auto bytes = std::make_shared<const Bytes>(std::move(*cached));
                    if (memory && fill)
                        memory->Insert(object_prefix + pack(item.location), item.location.slot * Store::CHUNK,
                                       bytes->data(), Store::CHUNK);
                    out[item.hash] = bytes;
                    continue;
                }
            fetch.push_back(item);
        }
        std::sort(fetch.begin(), fetch.end(), [](const Wanted &a, const Wanted &b) {
            return std::tie(a.location.pool, a.location.pack, a.location.slot) <
                   std::tie(b.location.pool, b.location.pack, b.location.slot);
        });
        auto max_chunks = std::max<uint64_t>(1, provider->Config().max_range_bytes / Store::CHUNK);
        std::vector<std::vector<const Wanted *>> ranges;
        for (auto &item : fetch) {
            if (!ranges.empty()) {
                auto *last = ranges.back().back();
                if (last->location.pool == item.location.pool && last->location.pack == item.location.pack &&
                    last->location.slot + 1 == item.location.slot && ranges.back().size() < max_chunks) {
                    ranges.back().push_back(&item);
                    continue;
                }
            }
            ranges.push_back({&item});
        }
        for (auto &range : ranges)
            if (range.size() > 1)
                ++coalesced_requests;
        Parallel(ranges.size(), provider->Concurrency(), [&](size_t i) {
            auto &range = ranges[i];
            auto &first = range.front()->location;
            auto bytes = provider->ReadSpan(pack(first), first.slot * Store::CHUNK, range.size() * Store::CHUNK);
            for (size_t k = 0; k < range.size(); k++)
                if (Hash(bytes.data() + k * Store::CHUNK, Store::CHUNK) != range[k]->hash)
                    throw std::runtime_error("[differential:corrupt_data] Corrupt S3 pack chunk checksum");
            // The whole verified span is one memory-cache range (as for export reads).
            if (memory && fill)
                memory->Insert(object_prefix + pack(first), first.slot * Store::CHUNK, bytes.data(), bytes.size());
            for (size_t k = 0; k < range.size(); k++) {
                auto begin = bytes.data() + k * Store::CHUNK;
                auto chunk = std::make_shared<const Bytes>(begin, begin + Store::CHUNK);
                if (disk && fill)
                    disk->Write(range[k]->hash, chunk, limit);
                std::lock_guard<std::mutex> guard(out_mutex);
                out[range[k]->hash] = chunk;
            }
        });
        cache_misses += fetch.size();
    }
    return out;
}
void InstallRemoteFetch(const std::shared_ptr<Control> &control) {
    std::weak_ptr<Control> owner = control;
    control->store->SetRemoteFetch([owner](const std::vector<std::string> &hashes, const void *context) {
        auto control = owner.lock();
        if (!control)
            throw std::runtime_error("[differential:unavailable_object] Repository closed");
        return FetchRemoteObjects(*control, hashes, static_cast<const RemoteCaching *>(context));
    });
}
std::vector<std::pair<std::string, uint64_t>> ExportMetrics() {
    std::vector<std::pair<std::string, uint64_t>> metrics = {{"remote_logical_read_bytes", logical_read_bytes.load()},
            {"remote_cache_hits", cache_hits.load()},
            {"remote_cache_misses", cache_misses.load()},
            {"remote_prefetched_chunks", prefetched_chunks.load()},
            {"remote_coalesced_requests", coalesced_requests.load()},
            {"export_packs_uploaded", packs_uploaded.load()},
            {"export_chunks_uploaded", chunks_uploaded.load()},
            {"export_chunks_reused", chunks_reused.load()},
            {"remote_staging_limit", StagingLimit()},
            {"shared_publications", publications.load()},
            {"shared_publication_failures", publication_failures.load()},
            {"sdk_max_requests_global", RequestLimit()},
            {"sdk_executor_threads_global", 4}};
    for (auto &metric : DiskCache::Metrics())
        metrics.push_back(metric);
    return metrics;
}

// ---------------------------------------------------------------------------------------
// Branch heads
namespace {
bool HeadName(const std::string &s) {
    return !s.empty() && s.size() <= 128 &&
           s.find_first_not_of("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-") == std::string::npos;
}
bool HeadUUID(const std::string &s) {
    return s.size() == 36 && s.find_first_not_of("0123456789abcdef-") == std::string::npos;
}
std::vector<unsigned char> EncodeHead(const Head &h) {
    std::ostringstream out;
    out << "differential-head-v1\n"
        << h.database << '\n'
        << h.database_name << '\n'
        << h.branch << '\n'
        << h.branch_name << '\n'
        << h.snapshot << '\n'
        << h.published_at << '\n'
        << h.descriptor << '\n'
        << "write " << h.write_id << '\n';
    auto s = out.str();
    return {s.begin(), s.end()};
}
Head DecodeHead(const std::vector<unsigned char> &bytes) {
    std::istringstream in(std::string(bytes.begin(), bytes.end()));
    std::vector<std::string> lines;
    std::string line;
    while (lines.size() < 10 && std::getline(in, line))
        lines.push_back(line);
    auto number = [](const std::string &s) {
        if (s.empty() || s.size() > 19 || s.find_first_not_of("0123456789") != std::string::npos)
            throw std::runtime_error("[differential:corrupt_data] Corrupt branch head");
        return int64_t(std::stoll(s));
    };
    if (lines.size() != 9 || lines[0] != "differential-head-v1" || lines[8].rfind("write ", 0) != 0 ||
        in.peek() != EOF)
        throw std::runtime_error("[differential:corrupt_data] Corrupt branch head");
    Head h;
    h.database = lines[1];
    h.database_name = lines[2];
    h.branch = lines[3];
    h.branch_name = lines[4];
    h.snapshot = number(lines[5]);
    h.published_at = number(lines[6]);
    h.descriptor = lines[7];
    h.write_id = lines[8].substr(6);
    if (h.write_id.size() != 32 || h.write_id.find_first_not_of("0123456789abcdef") != std::string::npos)
        throw std::runtime_error("[differential:corrupt_data] Corrupt branch head");
    if (!HeadUUID(h.database) || !HeadUUID(h.branch) || !HeadName(h.database_name) || !HeadName(h.branch_name) ||
        h.snapshot <= 0 || h.descriptor.rfind("descriptors/", 0) != 0 || !IsHash(h.descriptor.substr(12)))
        throw std::runtime_error("[differential:corrupt_data] Corrupt branch head");
    return h;
}
// The current version of a head object: whether it exists, its ETag, and its content when readable.
struct HeadVersion {
    bool exists = false;
    std::string etag;
    std::optional<Head> head;
};
HeadVersion ReadHeadVersion(S3Provider &provider, const std::string &key) {
    HeadVersion version;
    auto stat = provider.TryHead(key);
    if (!stat)
        return version;
    version.exists = true;
    version.etag = stat->etag;
    try {
        if (stat->size == 0 || stat->size > 64 * 1024)
            throw std::runtime_error("[differential:corrupt_data] Corrupt branch head");
        auto head = DecodeHead(provider.ReadSpan(key, 0, stat->size));
        if (head.Key() != key)
            throw std::runtime_error("[differential:corrupt_data] Branch head does not match its key");
        version.head = head;
    } catch (const std::exception &) {
        // Unreadable: only its ETag can still identify it as this repository's write.
    }
    return version;
}
// A shared database's dropped branch keeps a tombstone at its head key (differential-dropped-v1),
// written conditionally like a head; the name can be created again on top of it.
constexpr const char *DROPPED_FORMAT = "differential-dropped-v1\n";
bool IsDropped(const std::vector<unsigned char> &bytes) {
    auto prefix = std::string(DROPPED_FORMAT);
    return bytes.size() >= prefix.size() && std::equal(prefix.begin(), prefix.end(), bytes.begin());
}
std::vector<unsigned char> EncodeDropped(const Head &h, int64_t dropped_at) {
    std::ostringstream out;
    out << DROPPED_FORMAT << h.database << '\n'
        << h.database_name << '\n'
        << h.branch << '\n'
        << h.branch_name << '\n'
        << dropped_at << '\n'
        << "write " << h.write_id << '\n';
    auto text = out.str();
    return {text.begin(), text.end()};
}
std::optional<Head> ReadHead(S3Provider &provider, const std::string &key) {
    auto stat = provider.TryHead(key);
    if (!stat)
        return std::nullopt;
    if (stat->size == 0 || stat->size > 64 * 1024)
        throw std::runtime_error("[differential:corrupt_data] Corrupt branch head");
    auto bytes = provider.ReadSpan(key, 0, stat->size);
    if (IsDropped(bytes))
        return std::nullopt; // a dropped branch
    auto head = DecodeHead(bytes);
    if (head.Key() != key)
        throw std::runtime_error("[differential:corrupt_data] Branch head does not match its key");
    return head;
}
std::string WriteID() {
    unsigned char bytes[16];
    if (RAND_bytes(bytes, sizeof(bytes)) != 1)
        throw std::runtime_error("Cannot allocate a branch-head write identity");
    return Hex(bytes, sizeof(bytes));
}
// Serializes head publication of one database across this repository's processes.
struct HeadLock {
    int fd = -1;
    HeadLock(Control &control, const std::string &database) {
        auto path = std::filesystem::path(control.store->directory) / "locks";
        control.store->permissions.CreateDirectories(path);
        fd = control.store->permissions.Open(path / ("heads-" + database), O_RDWR | O_CLOEXEC);
        if (fd < 0 || flock(fd, LOCK_EX))
            throw std::runtime_error("Cannot lock branch-head publication");
    }
    ~HeadLock() {
        if (fd >= 0)
            close(fd);
    }
};
} // namespace

namespace {
// Shared databases: write the heads this repository advanced, each conditionally on its base (the
// version this repository last wrote or adopted). Conflicts are reported after the other heads.
void PublishSharedHeads(Control &control, const std::string &provider_id, const std::vector<BranchHead> &desired,
                        const std::string &only) {
    auto provider = Client(control, provider_id);
    std::string conflict;
    for (auto &d : desired) {
        if (!only.empty() && d.branch != only)
            continue;
        // A diverged branch is updated only by its own operations (which report the conflict).
        if (only.empty() && control.Conflict(d.branch))
            continue;
        Head head;
        head.database = d.database;
        head.database_name = d.database_name;
        head.branch = d.branch;
        head.branch_name = d.branch_name;
        head.snapshot = d.snapshot;
        head.published_at = d.published_at;
        head.descriptor = d.descriptor;
        auto key = head.Key();
        auto base = control.HeadWriteFor(provider_id, key).value_or(HeadWrite{});
        // Nothing new from this repository (a head moves only forward from its base).
        if (base.pending.empty() && !base.etag.empty() && d.snapshot <= base.snapshot)
            continue;
        head.write_id = WriteID();
        auto attempt = base;
        attempt.pending = head.write_id;
        control.RecordHeadWrite(provider_id, key, attempt); // before the PUT: a lost response stays ours
        Failpoint("DIFFERENTIAL_SHARED_CRASH", "head_pending");
        auto bytes = EncodeHead(head);
        auto etag = provider->PutMutable(key, bytes, base.etag);
        if (etag)
            Failpoint("DIFFERENTIAL_SHARED_CRASH", "head_written");
        std::optional<Head> current;
        bool dropped = false;
        for (int tries = 0; !etag && tries < 2; tries++) {
            auto now = provider->ReadMutable(key);
            current.reset();
            dropped = now && IsDropped(now->first);
            if (dropped && base.etag.empty()) {
                // A new branch reuses a dropped branch's name: write on the tombstone's version.
                base.etag = now->second;
                etag = provider->PutMutable(key, bytes, base.etag);
                continue;
            }
            if (now && !dropped)
                try {
                    current = DecodeHead(now->first);
                } catch (const std::exception &) {
                }
            if (current && current->write_id == head.write_id) {
                etag = now->second; // this write landed; an SDK retry failed its condition against it
                break;
            }
            // An earlier update whose response was lost did land: it is the base, update it.
            if (!current || base.pending.empty() || current->write_id != base.pending)
                break;
            base.etag = now->second;
            base.pending.clear();
            etag = provider->PutMutable(key, bytes, base.etag);
        }
        if (!etag) {
            base.pending.clear();
            control.RecordHeadWrite(provider_id, key, base);
            control.RecordConflict(BranchConflict{d.branch, "", d.snapshot, current ? current->snapshot : 0, 0});
            if (conflict.empty()) {
                auto state = dropped    ? std::string("was dropped in S3 by another writer")
                             : !current ? std::string("was removed from S3")
                             : base.etag.empty()
                                 ? "was created in S3 by another writer (at snapshot " + std::to_string(current->snapshot) + ")"
                                 : "moved in S3 to snapshot " + std::to_string(current->snapshot) +
                                       ", published by another writer, since this repository last saw it at snapshot " +
                                       std::to_string(base.snapshot);
                conflict = "[differential:branch_conflict] Branch " + d.branch_name + " of database " +
                           d.database_name + " " + state + ". Snapshot " + std::to_string(d.snapshot) +
                           " is exported, but the branch head was not changed. After DETACH, keep this work as "
                           "a new branch and follow S3 with differential_resolve_conflict('" + d.database_name +
                           "', keep => 'branch', branch => '" + d.branch_name + "'), or follow S3 with keep => "
                           "'remote'.";
            }
            continue;
        }
        control.RecordHeadWrite(provider_id, key, HeadWrite{*etag, head.write_id, "", d.snapshot});
        control.ClearConflict(d.branch);
    }
    if (!conflict.empty())
        throw std::runtime_error(conflict);
}
} // namespace

void PublishHeads(Control &control, const std::string &database, bool claim, const std::string &branch) {
    std::string provider_id;
    if (control.ExportedHeads(database, provider_id).empty() && provider_id.empty())
        return;
    HeadLock lock(control, control.FindDatabase(database));
    // Recomputed under the lock, so concurrent or out-of-order exports converge on the latest.
    auto desired = control.ExportedHeads(database, provider_id);
    if (control.Writers(database) == "shared")
        return PublishSharedHeads(control, provider_id, desired, branch);
    auto provider = Client(control, provider_id);
    for (auto &d : desired) {
        Head head;
        head.database = d.database;
        head.database_name = d.database_name;
        head.branch = d.branch;
        head.branch_name = d.branch_name;
        head.snapshot = d.snapshot;
        head.published_at = d.published_at;
        head.descriptor = d.descriptor;
        auto key = head.Key();
        auto record = control.HeadWriteFor(provider_id, key);
        auto current = ReadHeadVersion(*provider, key);
        // A write whose response was lost may have landed: recognise it by its write ID.
        if (record && !record->pending.empty() && current.head && current.head->write_id == record->pending) {
            *record = HeadWrite{current.etag, record->pending, "", current.head->snapshot};
            control.RecordHeadWrite(provider_id, key, *record);
        }
        // Ours: absent (never written, or deleted), or still the version this repository wrote.
        bool ours = !current.exists ||
                    (record && ((!record->etag.empty() && current.etag == record->etag) ||
                                (current.head && !record->write_id.empty() && current.head->write_id == record->write_id)));
        if (!ours && !claim)
            throw std::runtime_error(
                "[differential:head_conflict] The S3 branch head " + key +
                " was written by another writer, for example a copy of this repository on another host; it was "
                "not changed. Exports are unaffected. If this repository is the only writer of the database, run "
                "differential_claim_branch_heads('" + d.database_name + "') to take its branch heads over.");
        if (ours && current.head && current.head->branch == head.branch && current.head->snapshot == head.snapshot &&
            current.head->descriptor == head.descriptor) {
            if (!record || record->etag != current.etag)
                control.RecordHeadWrite(provider_id, key,
                                        HeadWrite{current.etag, current.head->write_id, "", current.head->snapshot});
            continue; // unchanged
        }
        head.write_id = WriteID();
        auto pending = record ? *record : HeadWrite{};
        pending.pending = head.write_id;
        control.RecordHeadWrite(provider_id, key, pending); // before the PUT: a lost response stays ours
        auto etag = provider->PutMutable(key, EncodeHead(head), current.exists ? current.etag : "");
        if (!etag) {
            // The condition failed. An SDK retry after a lost response fails it against our own
            // first attempt: the head then carries this write's ID.
            auto now = ReadHeadVersion(*provider, key);
            if (!now.head || now.head->write_id != head.write_id)
                throw std::runtime_error("[differential:head_conflict] The S3 branch head " + key +
                                         " changed while it was being updated, by another writer; it was not "
                                         "changed.");
            etag = now.etag;
        }
        control.RecordHeadWrite(provider_id, key, HeadWrite{*etag, head.write_id, "", d.snapshot});
    }
}
std::vector<int64_t> ClaimDatabase(Control &control, const std::string &database) {
    std::string provider_id;
    control.ExportedHeads(database, provider_id);
    if (provider_id.empty())
        throw std::runtime_error("[differential:not_found] Database has no S3 data path");
    if (control.Writers(database) == "shared")
        throw std::runtime_error("Database is shared by several writers: its branch heads have no single owner to "
                                 "claim them");
    auto provider = Client(control, provider_id);
    auto id = control.FindDatabase(database);
    auto taken = NewestSnapshot(*provider, id);
    // Pending exports whose numbers another writer published can never be completed.
    std::vector<int64_t> abandoned;
    for (auto snapshot : control.PendingExports(database)) {
        if (snapshot > taken)
            continue;
        auto key = CommitKey(id, snapshot);
        auto stat = provider->TryHead(key);
        if (!stat)
            continue;
        auto local = control.GetSnapshot(database, snapshot, false);
        auto remote = provider->ReadSpan(key, 0, stat->size);
        std::string text(remote.begin(), remote.end());
        bool ours = text.find("\nbranch " + local.branch + "\n") != std::string::npos &&
                    text.find("\npublished_at " + std::to_string(local.published_at) + "\n") != std::string::npos;
        if (!ours && control.AbandonExport(database, snapshot))
            abandoned.push_back(snapshot);
    }
    control.AdvanceSnapshotNumbers(database, taken);
    PublishHeads(control, database, true);
    return abandoned;
}
Head ResolveHead(Control &control, const std::string &provider_id, const std::string &path) {
    Timer timer(Metric::BranchHeadResolve);
    std::vector<std::string> parts;
    std::stringstream in(path);
    std::string part;
    while (std::getline(in, part, '/'))
        parts.push_back(part);
    if (parts.empty() || parts.size() > 3 || !HeadName(parts[0]) ||
        (parts.size() == 3 && (!HeadUUID(parts[1]) || !HeadName(parts[2]))) ||
        (parts.size() == 2 && !HeadName(parts[1])))
        throw std::runtime_error("Expected heads/<database>[/<database uuid>][/<branch>]");
    auto provider = Client(control, provider_id);
    auto database_name = parts[0];
    auto branch = parts.size() == 1 ? std::string("main") : parts.back();
    std::string database = parts.size() == 3 ? parts[1] : "";
    if (database.empty()) {
        // The names index lists every database UUID exported under this name.
        std::vector<std::string> found;
        std::string continuation;
        auto within = "names/" + database_name + "/";
        do {
            auto page = provider->List(continuation, 1000, within);
            for (auto &key : page.keys)
                if (HeadUUID(key.substr(within.size())))
                    found.push_back(key.substr(within.size()));
            continuation = page.continuation;
        } while (!continuation.empty());
        if (found.size() > 1) {
            // Only databases that have this branch count.
            std::vector<std::string> with_branch;
            for (auto &id : found) {
                Head probe;
                probe.database = id;
                probe.branch_name = branch;
                if (provider->TryHead(probe.Key()))
                    with_branch.push_back(id);
            }
            found = with_branch;
        }
        if (found.empty())
            throw std::runtime_error("[differential:not_found] No exported branch head " + database_name + "/" + branch);
        if (found.size() > 1) {
            std::string ids;
            for (auto &id : found)
                ids += (ids.empty() ? "" : ", ") + id;
            throw std::runtime_error("Several databases named " + database_name + " export to this location (" + ids +
                                     "); use heads/" + database_name + "/<database uuid>/" + branch);
        }
        database = found.front();
    }
    Head key;
    key.database = database;
    key.database_name = database_name;
    key.branch_name = branch;
    auto head = ReadHead(*provider, key.Key());
    if (!head)
        throw std::runtime_error("[differential:not_found] No exported branch head " + key.Key());
    // The descriptor the head names must describe the same database and snapshot. (Its branch is
    // the publishing branch, which differs for a branch created from or restored to a snapshot.)
    RemoteSnapshot snapshot(control, provider_id, head->descriptor);
    if (snapshot.descriptor.database != head->database || snapshot.descriptor.snapshot != head->snapshot)
        throw std::runtime_error("[differential:corrupt_data] Branch head does not match its export descriptor");
    return *head;
}
std::vector<Head> ListHeads(Control &control, const std::string &provider_id) {
    auto provider = Client(control, provider_id);
    auto list = [&](const std::string &within, const std::function<void(const std::string &)> &each) {
        std::string continuation;
        do {
            auto page = provider->List(continuation, 1000, within);
            for (auto &key : page.keys)
                each(key);
            continuation = page.continuation;
        } while (!continuation.empty());
    };
    // Every exported database (names index), then its branch heads.
    std::set<std::string> databases;
    list("names/", [&](const std::string &key) {
        auto slash = key.rfind('/');
        if (slash != std::string::npos && HeadUUID(key.substr(slash + 1)))
            databases.insert(key.substr(slash + 1));
    });
    std::vector<Head> heads;
    for (auto &database : databases)
        list(Pool(database) + "branches/", [&](const std::string &key) {
            if (auto head = ReadHead(*provider, key))
                heads.push_back(*head);
        });
    return heads;
}

// ---------------------------------------------------------------------------------------
// Shared databases
std::string SharedDatabaseID(const std::string &name) {
    auto text = "differential-shared-database-v1\n" + name;
    unsigned char bytes[32];
    unsigned length = 0;
    if (EVP_Digest(text.data(), text.size(), bytes, &length, EVP_sha256(), nullptr) != 1 || length != 32)
        throw std::runtime_error("SHA256 failed");
    bytes[6] = static_cast<unsigned char>((bytes[6] & 0x0F) | 0x80); // UUID version 8 (name-derived)
    bytes[8] = static_cast<unsigned char>((bytes[8] & 0x3F) | 0x80);
    return HexUUID(bytes);
}
namespace {
// The S3 head object of a shared database's branch: its version, and the head unless the branch
// was dropped (a tombstone).
struct SharedHead {
    std::optional<Head> head;
    std::string etag;
};
std::optional<SharedHead> ReadSharedHead(S3Provider &provider, const std::string &database, const std::string &name,
                                         const std::string &branch) {
    Head key;
    key.database = database;
    key.database_name = name;
    key.branch_name = branch;
    auto object = provider.ReadMutable(key.Key());
    if (!object)
        return std::nullopt;
    if (IsDropped(object->first))
        return SharedHead{std::nullopt, object->second};
    auto head = DecodeHead(object->first);
    if (head.Key() != key.Key() || head.database_name != name)
        throw std::runtime_error("[differential:corrupt_data] Branch head does not match its key");
    return SharedHead{head, object->second};
}
// Make a published head the local branch (see Control::AdoptRemoteHead), reading its map.
Branch Adopt(Control &control, const std::string &provider_id, const std::string &id, const std::string &branch_name,
             const Head &remote, const std::string &etag, bool replace) {
    RemoteSnapshot snapshot(control, provider_id, remote.descriptor);
    if (snapshot.descriptor.database != id || snapshot.descriptor.snapshot != remote.snapshot)
        throw std::runtime_error("[differential:corrupt_data] Branch head does not match its export descriptor");
    RemoteHead head;
    try {
        auto known = control.FindSnapshot(id, remote.snapshot, false); // its commit record, if cached
        head.info = {known.name, known.author, known.message, known.extra_info};
    } catch (const std::exception &) {
    }
    head.key = remote.Key();
    head.etag = etag;
    head.write_id = remote.write_id;
    head.branch = remote.branch;
    head.branch_name = branch_name;
    head.descriptor = remote.descriptor;
    head.root = snapshot.descriptor.root;
    head.snapshot = remote.snapshot;
    head.published_at = remote.published_at;
    head.size = snapshot.descriptor.size;
    for (uint64_t page = 0; page < snapshot.descriptor.pages.size(); page++)
        for (auto &entry : snapshot.Entries(page)) {
            head.locations[entry.first.object] = entry.second;
            head.chunks.push_back(std::move(entry.first));
        }
    return control.AdoptRemoteHead(id, provider_id, head, replace);
}
} // namespace
namespace {
// No local changes since the branch's base: no snapshot after it, and its live file (main file and
// write-ahead log) still holds the base's image. The caller holds the branch's writer lock.
bool Unchanged(Control &control, const Branch &branch, const std::optional<HeadWrite> &base) {
    // A branch whose conflicting snapshot was published as another branch is unchanged after it.
    auto conflict = control.Conflict(branch.id);
    auto expected = conflict && !conflict->kept_branch.empty() ? conflict->snapshot : base ? base->snapshot : 0;
    if (branch.head != expected)
        return false;
    auto &store = *control.store;
    for (auto suffix : {".wal", ".wal.checkpoint", ".wal.recovery"}) {
        auto name = branch.FileName() + suffix;
        if (store.Exists(name) && store.Size(store.FileId(name)) > 0)
            return false;
    }
    if (!store.Exists(branch.FileName()))
        return branch.root.empty();
    auto file = store.FileId(branch.FileName());
    if (branch.root.empty())
        return store.Size(file) == 0;
    return store.Seal(file).id == branch.root;
}
} // namespace
Branch PrepareSharedBranch(Control &control, const std::string &provider_id, const std::string &name,
                           const std::string &branch_name, bool create, bool writable) {
    if (!HeadName(name) || !HeadName(branch_name))
        throw std::runtime_error("Invalid name: use 1..128 letters, digits, underscore or hyphen");
    auto id = SharedDatabaseID(name);
    auto provider = Client(control, provider_id);
    Head key;
    key.database = id;
    key.database_name = name;
    key.branch_name = branch_name;
    // The branch head in S3: one GET, which also returns its version (ETag).
    std::optional<Head> remote;
    std::string etag;
    bool dropped = false; // by a writer in S3
    if (auto found = ReadSharedHead(*provider, id, name, branch_name)) {
        remote = found->head;
        etag = found->etag;
        dropped = !found->head;
    }
    auto local = control.FindDatabase(name);
    if (local.empty()) {
        if (!remote && !create)
            throw std::runtime_error("[differential:not_found] Database \"" + name + "\" does not exist");
        // Another database of this name in the location (not shared) would make the name ambiguous.
        auto within = "names/" + name + "/";
        std::string continuation;
        do {
            auto page = provider->List(continuation, 1000, within);
            for (auto &listed : page.keys)
                if (listed.substr(within.size()) != id)
                    throw std::runtime_error("[differential:name_conflict] A database named \"" + name +
                                             "\" that is not shared already exports to this location; choose "
                                             "another name");
            continuation = page.continuation;
        } while (!continuation.empty());
        control.CreateSharedDatabase(id, name, provider_id);
    } else if (local != id || control.Writers(local) != "shared")
        throw std::runtime_error("[differential:name_conflict] Database \"" + name + "\" in this repository is "
                                 "not shared; WRITERS is chosen when a database is created");
    std::optional<Branch> existing;
    try {
        existing = control.GetBranch(id, branch_name);
    } catch (const std::exception &) {
    }
    auto base = control.HeadWriteFor(provider_id, key.Key());
    // The local branch, under its writer lock, when S3 may replace it; nothing when a local writer
    // has it and this is a reader (which then reads what is here).
    std::shared_ptr<BranchLock> lock;
    auto acquire = [&]() -> std::optional<Branch> {
        try {
            lock = control.Acquire(*existing);
        } catch (const std::exception &error) {
            if (writable || std::string(error.what()).find("[differential:writer_busy]") == std::string::npos)
                throw;
            return std::nullopt;
        }
        return control.GetBranch(id, existing->id, true);
    };
    if (!remote) {
        if (existing && dropped) {
            // Dropped in S3 by another writer: an unchanged local branch goes too; one with changes stays,
            // and its next head update reports the conflict.
            auto current = acquire();
            if (!current || !Unchanged(control, *current, base))
                return current ? *current : *existing;
            control.DropBranchLocally(id, current->id);
            // A branch of this name created here later publishes on top of the tombstone.
            control.RecordHeadWrite(provider_id, key.Key(), HeadWrite{etag, "", "", 0});
            existing.reset();
        }
        if (existing)
            return *existing; // not published yet: its first export creates the head
        if (branch_name != "main")
            throw std::runtime_error("[differential:not_found] Branch \"" + branch_name + "\" of database \"" + name +
                                     "\" does not exist" + (dropped ? " (it was dropped)" : "") +
                                     "; create it with differential_create_branch");
        return control.CreateEmptyBranch(id, branch_name);
    }
    if (existing && base &&
        (base->etag == etag || remote->write_id == base->write_id || remote->write_id == base->pending)) {
        if (base->etag != etag) // this repository's write, whose response was lost
            control.RecordHeadWrite(provider_id, key.Key(), HeadWrite{etag, remote->write_id, "", remote->snapshot});
        return *existing; // up to date
    }
    // The head moved (or was never adopted here). A local branch without local changes moves to it;
    // one with changes stays, and its next head update reports the conflict.
    if (existing) {
        auto current = acquire();
        if (!current)
            return *existing;
        if (!Unchanged(control, *current, base))
            return *current;
    }
    // Without a local branch nobody holds its lock: a branch another session adopted meanwhile stays.
    return Adopt(control, provider_id, id, branch_name, *remote, etag, bool(existing));
}
namespace {
std::string DatabaseName(Control &control, const std::string &id) {
    for (auto &d : control.Databases())
        if (d.id == id)
            return d.name;
    throw std::runtime_error("[differential:not_found] Database not found");
}
} // namespace
bool RemoteBranchExists(Control &control, const std::string &database, const std::string &branch) {
    auto provider_id = control.Profile(database);
    if (provider_id == "local")
        return false;
    auto id = control.FindDatabase(database);
    auto found = ReadSharedHead(*Client(control, provider_id), id, DatabaseName(control, id), branch);
    return found && found->head; // a dropped branch's name is free
}
Branch FollowRemoteHead(Control &control, const std::string &database, const std::string &branch) {
    auto provider_id = control.Profile(database);
    auto id = control.FindDatabase(database);
    auto remote = ReadSharedHead(*Client(control, provider_id), id, DatabaseName(control, id), branch);
    if (!remote)
        throw std::runtime_error("[differential:not_found] Branch \"" + branch + "\" has no head in S3 to follow");
    if (!remote->head) {
        // Dropped in S3: following it means dropping it here too.
        auto local = control.GetBranch(id, branch);
        control.DropBranchLocally(id, local.id);
        Head key;
        key.database = id;
        key.branch_name = branch;
        control.RecordHeadWrite(provider_id, key.Key(), HeadWrite{remote->etag, "", "", 0});
        local.state = "deleted";
        local.head = 0;
        return local;
    }
    return Adopt(control, provider_id, id, branch, *remote->head, remote->etag, true);
}
std::string ConflictBranchName(const std::string &branch, int64_t snapshot) {
    auto suffix = "-conflict-" + std::to_string(snapshot);
    return branch.substr(0, 128 - suffix.size()) + suffix;
}

// ---------------------------------------------------------------------------------------
// Shared databases: publication
namespace {
std::mutex shared_mutex;
std::map<std::string, SharedWriting> shared_writing; // by repository directory and branch
std::set<std::string> explicit_snapshots;
std::string last_publication_error;
std::string BranchKey(Control &control, const std::string &branch) { return control.store->directory + "/" + branch; }
void RecordPublicationError(const std::string &error) {
    ++publication_failures;
    std::lock_guard<std::mutex> guard(shared_mutex);
    last_publication_error = error;
}
// One worker per process. It starts on the first request, which always follows S3 use (the
// attachment read its branch head), so it is destroyed, and joined, before the SDK runtime.
class Publisher {
  public:
    static Publisher &Instance() {
        static Publisher publisher;
        return publisher;
    }
    // An empty branch: sync the database's history instead of publishing.
    void Schedule(const std::shared_ptr<Control> &control, const std::string &database, const std::string &branch) {
        auto key = BranchKey(*control, branch.empty() ? "history-" + database : branch);
        std::lock_guard<std::mutex> guard(mutex);
        if (stopping || !queued.insert(key).second)
            return; // already waiting: it covers everything up to its run
        jobs.push_back({control, key, database, branch});
        if (!worker.joinable())
            worker = std::thread([this] { Run(); });
        signal.notify_one();
    }
    ~Publisher() {
        {
            std::lock_guard<std::mutex> guard(mutex);
            stopping = true;
        }
        signal.notify_all();
        if (worker.joinable())
            worker.join();
    }

  private:
    struct Job {
        std::weak_ptr<Control> control;
        std::string key, database, branch;
    };
    std::mutex mutex;
    std::condition_variable signal;
    std::deque<Job> jobs;
    std::set<std::string> queued;
    std::atomic<bool> stopping{false};
    std::thread worker;
    void Run() {
        std::unique_lock<std::mutex> lock(mutex);
        while (true) {
            signal.wait(lock, [this] { return stopping || !jobs.empty(); });
            if (stopping)
                return;
            auto job = jobs.front();
            jobs.pop_front();
            queued.erase(job.key); // a request arriving from now on runs again afterwards
            lock.unlock();
            if (auto control = job.control.lock()) {
                try {
                    if (job.branch.empty()) {
                        SyncHistory(*control, job.database, true);
                        lock.lock();
                        continue;
                    }
                    PublishShared(*control, job.database, job.branch, [this] { return stopping.load(); });
                    ++publications;
                } catch (const std::exception &error) {
                    RecordPublicationError(error.what());
                }
            }
            lock.lock();
        }
    }
};
} // namespace
void SetSharedWriting(Control &control, const std::string &branch, SharedWriting writing) {
    std::lock_guard<std::mutex> guard(shared_mutex);
    shared_writing[BranchKey(control, branch)] = std::move(writing);
}
SharedWriting GetSharedWriting(Control &control, const std::string &branch) {
    std::lock_guard<std::mutex> guard(shared_mutex);
    auto found = shared_writing.find(BranchKey(control, branch));
    return found == shared_writing.end() ? SharedWriting{} : found->second;
}
std::string ForkConflict(Control &control, const std::string &database, const std::string &branch) {
    auto conflict = control.Conflict(branch);
    if (!conflict)
        throw std::runtime_error("[differential:not_found] Branch has no conflict to fork");
    auto name = ConflictBranchName(control.BranchName(branch), conflict->snapshot);
    auto fork = control.CreateBranch(database, conflict->snapshot, name, "", false);
    PublishHeads(control, database, false, fork.branch);
    control.RecordConflict({branch, name, conflict->snapshot, conflict->remote_snapshot, 0});
    return name;
}
std::string PublishShared(Control &control, const std::string &database, const std::string &branch,
                          std::function<bool()> cancelled) {
    auto writing = GetSharedWriting(control, branch);
    auto conflict = control.Conflict(branch);
    if (conflict && conflict->kept_branch.empty() && writing.on_conflict != "branch")
        return ""; // diverged: nothing more until it is reconciled
    auto pending = control.PendingExports(database);
    if (!pending.empty()) {
        auto last = pending.back(); // exports every earlier one first
        ExportSnapshot(control, database, last, cancelled);
    }
    try {
        PublishHeads(control, database, false, branch);
    } catch (const std::exception &error) {
        if (writing.on_conflict != "branch" ||
            std::string(error.what()).find("[differential:branch_conflict]") == std::string::npos)
            throw;
        return ForkConflict(control, database, branch);
    }
    return "";
}
void SchedulePublication(const std::shared_ptr<Control> &control, const std::string &database,
                         const std::string &branch) {
    Publisher::Instance().Schedule(control, database, branch);
}
void PublishOnClose(Control &control, const std::string &database, const std::string &branch) {
    try {
        PublishShared(control, database, branch);
        ++publications;
    } catch (const std::exception &error) {
        RecordPublicationError(error.what());
    }
}
std::string LastPublicationError() {
    std::lock_guard<std::mutex> guard(shared_mutex);
    return last_publication_error;
}
ExplicitSnapshot::ExplicitSnapshot(Control &control, const std::string &branch) : key(BranchKey(control, branch)) {
    std::lock_guard<std::mutex> guard(shared_mutex);
    explicit_snapshots.insert(key);
}
ExplicitSnapshot::~ExplicitSnapshot() {
    std::lock_guard<std::mutex> guard(shared_mutex);
    explicit_snapshots.erase(key);
}
bool ExplicitSnapshotInProgress(Control &control, const std::string &branch) {
    std::lock_guard<std::mutex> guard(shared_mutex);
    return explicit_snapshots.count(BranchKey(control, branch)) > 0;
}

// ---------------------------------------------------------------------------------------
// Shared databases: advisory writer markers
namespace {
std::string MarkerPrefix(const std::string &database, const std::string &branch) {
    return Pool(database) + "writers/" + branch + "/";
}
// Markers expire 2 minutes after their last renewal and are renewed every 30 seconds. Test builds
// can shorten the lease (DIFFERENTIAL_TEST_MARKER_LEASE_MS).
int64_t MarkerLease() {
    if (auto lease = Hook("DIFFERENTIAL_TEST_MARKER_LEASE_MS"))
        return std::stoll(lease) * 1000;
    return 120ll * 1000 * 1000;
}
std::vector<unsigned char> EncodeMarker(const WriterMarker &m) {
    std::ostringstream out;
    out << "differential-writer-v1\n"
        << "database " << m.database << '\n'
        << "branch_name " << m.branch_name << '\n'
        << "writer " << m.writer << '\n'
        << "repository " << m.repository << '\n'
        << "host " << Escape(m.host) << '\n'
        << "process " << m.process << '\n'
        << "started_at " << m.started_at << '\n'
        << "renewed_at " << m.renewed_at << '\n'
        << "lease " << m.lease << '\n';
    auto text = out.str();
    return {text.begin(), text.end()};
}
std::optional<WriterMarker> DecodeMarker(const std::vector<unsigned char> &bytes) {
    std::istringstream in(std::string(bytes.begin(), bytes.end()));
    std::string line;
    if (!std::getline(in, line) || line != "differential-writer-v1")
        return std::nullopt;
    std::map<std::string, std::string> fields;
    while (std::getline(in, line)) {
        auto space = line.find(' ');
        if (space != std::string::npos)
            fields[line.substr(0, space)] = line.substr(space + 1);
    }
    auto number = [&](const char *name) -> int64_t {
        auto &text = fields[name];
        if (text.empty() || text.size() > 19 || text.find_first_not_of("0123456789") != std::string::npos)
            throw std::runtime_error("corrupt marker");
        return std::stoll(text);
    };
    try {
        WriterMarker m;
        m.database = fields["database"];
        m.branch_name = fields["branch_name"];
        m.writer = fields["writer"];
        m.repository = fields["repository"];
        m.host = fields["host"];
        m.process = number("process");
        m.started_at = number("started_at");
        m.renewed_at = number("renewed_at");
        m.lease = number("lease");
        if (!IsUUID(m.database) || !HeadName(m.branch_name) || m.writer.size() != 32 || !IsUUID(m.repository))
            return std::nullopt;
        return m;
    } catch (const std::exception &) {
        return std::nullopt;
    }
}
std::string HostName() {
    char name[256] = {0};
    if (gethostname(name, sizeof(name) - 1))
        return "unknown";
    std::string host(name);
    for (auto &c : host)
        if (c < 0x21 || c > 0x7E)
            c = '_';
    return host.substr(0, 128);
}
// The markers of this process's writer sessions, renewed by one thread.
class MarkerKeeper {
  public:
    static MarkerKeeper &Instance() {
        static MarkerKeeper keeper;
        return keeper;
    }
    void Add(const std::string &session, const std::shared_ptr<Control> &control, const std::string &provider,
             const WriterMarker &marker) {
        std::lock_guard<std::mutex> guard(mutex);
        sessions[session] = {control, provider, marker};
        if (!worker.joinable())
            worker = std::thread([this] { Run(); });
    }
    std::optional<std::pair<std::string, WriterMarker>> Remove(const std::string &session) {
        // A renewal in progress must not write the marker again after its session ended.
        std::lock_guard<std::mutex> renewal(renewing);
        std::lock_guard<std::mutex> guard(mutex);
        auto found = sessions.find(session);
        if (found == sessions.end())
            return std::nullopt;
        auto result = std::make_pair(found->second.provider, found->second.marker);
        sessions.erase(found);
        return result;
    }
    bool Owns(const std::string &writer) {
        std::lock_guard<std::mutex> guard(mutex);
        for (auto &entry : sessions)
            if (entry.second.marker.writer == writer)
                return true;
        return false;
    }
    ~MarkerKeeper() {
        {
            std::lock_guard<std::mutex> guard(mutex);
            stopping = true;
        }
        signal.notify_all();
        if (worker.joinable())
            worker.join();
    }

  private:
    struct Session {
        std::weak_ptr<Control> control;
        std::string provider;
        WriterMarker marker;
    };
    std::mutex renewing, mutex; // in this order
    std::condition_variable signal;
    std::map<std::string, Session> sessions;
    bool stopping = false;
    std::thread worker;
    void Run() {
        std::unique_lock<std::mutex> lock(mutex);
        while (!stopping) {
            signal.wait_for(lock, std::chrono::microseconds(MarkerLease() / 4), [this] { return stopping; });
            if (stopping)
                return;
            lock.unlock();
            std::lock_guard<std::mutex> renewal(renewing);
            lock.lock();
            auto due = sessions;
            lock.unlock();
            for (auto &entry : due) {
                auto control = entry.second.control.lock();
                if (!control)
                    continue;
                auto marker = entry.second.marker;
                marker.renewed_at = Control::Now();
                try {
                    Client(*control, entry.second.provider)->PutMarker(Pool(marker.database) + "writers/" +
                                                                           marker.branch_name + "/" + marker.writer,
                                                                       EncodeMarker(marker));
                } catch (const std::exception &) {
                    // Advisory: the next renewal tries again.
                }
            }
            lock.lock();
        }
    }
};
std::string SessionKey(Control &control, const std::string &branch) { return control.store->directory + "/" + branch; }
// The markers under `prefix` (database or branch), read with one GET each.
std::vector<std::pair<std::string, std::optional<WriterMarker>>> ReadMarkers(S3Provider &provider,
                                                                             const std::string &prefix) {
    std::vector<std::pair<std::string, std::optional<WriterMarker>>> markers;
    std::string continuation;
    do {
        auto page = provider.List(continuation, 1000, prefix);
        for (auto &key : page.keys) {
            auto object = provider.ReadMutable(key);
            if (object)
                markers.emplace_back(key, DecodeMarker(object->first));
        }
        continuation = page.continuation;
    } while (!continuation.empty());
    return markers;
}
} // namespace
std::vector<WriterMarker> AnnounceWriter(const std::shared_ptr<Control> &control, const Branch &branch,
                                         const std::string &on_busy) {
    auto provider_id = control->Profile(branch.database);
    auto provider = Client(*control, provider_id);
    auto repository = control->RepositoryID();
    auto now = Control::Now();
    std::vector<WriterMarker> others;
    for (auto &entry : ReadMarkers(*provider, MarkerPrefix(branch.database, branch.name))) {
        auto &marker = entry.second;
        // This repository holds the branch's writer lock, so its own markers are leftovers of ended
        // sessions; expired and unreadable markers belong to sessions that ended without removing them.
        if (!marker || marker->repository == repository || !marker->Live(now)) {
            try {
                provider->DeleteMarker(entry.first);
            } catch (const std::exception &) {
            }
            continue;
        }
        others.push_back(*marker);
    }
    if (!others.empty() && on_busy == "error")
        throw std::runtime_error("[differential:writer_busy] " + DescribeWriters(others) +
                                 " ATTACH with ON_BUSY 'warn' to write anyway: concurrent updates of the branch "
                                 "are detected when they are published.");
    WriterMarker mine;
    mine.database = branch.database;
    mine.branch_name = branch.name;
    mine.writer = WriteID();
    mine.repository = repository;
    mine.host = HostName();
    mine.process = int64_t(getpid());
    mine.started_at = mine.renewed_at = now;
    mine.lease = MarkerLease();
    try {
        provider->PutMarker(MarkerPrefix(branch.database, branch.name) + mine.writer, EncodeMarker(mine));
        MarkerKeeper::Instance().Add(SessionKey(*control, branch.id), control, provider_id, mine);
    } catch (const std::exception &) {
        // Advisory: writing goes on without a marker.
    }
    return others;
}
void WithdrawWriter(Control &control, const std::string &branch) {
    auto session = MarkerKeeper::Instance().Remove(SessionKey(control, branch));
    if (!session)
        return;
    auto &marker = session->second;
    try {
        Client(control, session->first)->DeleteMarker(MarkerPrefix(marker.database, marker.branch_name) + marker.writer);
    } catch (const std::exception &) {
        // It expires on its own.
    }
}
std::vector<std::pair<WriterMarker, bool>> ListWriters(Control &control, const std::string &database) {
    auto provider_id = control.Profile(database);
    std::vector<std::pair<WriterMarker, bool>> result;
    if (provider_id == "local")
        return result;
    auto provider = Client(control, provider_id);
    for (auto &entry : ReadMarkers(*provider, Pool(control.FindDatabase(database)) + "writers/"))
        if (entry.second)
            result.emplace_back(*entry.second, MarkerKeeper::Instance().Owns(entry.second->writer));
    return result;
}
std::string DescribeWriters(const std::vector<WriterMarker> &writers) {
    std::string text;
    for (auto &w : writers) {
        auto age = (Control::Now() - w.started_at) / 1000000;
        text += (text.empty() ? "" : "; ") + std::string("host ") + w.host + " (repository " + w.repository +
                ", process " + std::to_string(w.process) + ", for " + std::to_string(age) + " s)";
    }
    auto &first = writers.front();
    return "Branch " + first.branch_name + " is being written by " + std::to_string(writers.size()) +
           " other writer" + (writers.size() > 1 ? "s" : "") + ": " + text + ".";
}

// ---------------------------------------------------------------------------------------
// Shared databases: history, expiry and dropped branches
namespace {
std::string Unescape(const std::string &text) {
    std::string out;
    for (size_t i = 0; i < text.size(); i++) {
        if (text[i] == '%' && i + 2 < text.size()) {
            auto hex = text.substr(i + 1, 2);
            if (hex.size() == 2 && hex.find_first_not_of("0123456789ABCDEF") == std::string::npos) {
                out += char(std::stoi(hex, nullptr, 16));
                i += 2;
                continue;
            }
        }
        out += text[i];
    }
    return out;
}
std::optional<RemoteCommit> DecodeCommit(const std::vector<unsigned char> &bytes) {
    std::istringstream in(std::string(bytes.begin(), bytes.end()));
    std::string line;
    if (!std::getline(in, line) || line != "differential-commit-v1")
        return std::nullopt;
    std::map<std::string, std::string> fields;
    while (std::getline(in, line)) {
        auto space = line.find(' ');
        fields[line.substr(0, space)] = space == std::string::npos ? "" : line.substr(space + 1);
    }
    auto number = [&](const char *name) -> int64_t {
        auto &text = fields[name];
        if (text.empty() || text.size() > 19 || text.find_first_not_of("0123456789") != std::string::npos)
            throw std::runtime_error("[differential:corrupt_data] Corrupt commit record");
        return std::stoll(text);
    };
    RemoteCommit c;
    c.snapshot = number("snapshot");
    c.parent = number("parent");
    c.published_at = number("published_at");
    c.branch = fields["branch"];
    c.branch_name = fields["branch_name"];
    c.descriptor = fields["descriptor"];
    c.name = Unescape(fields["name"]);
    c.author = Unescape(fields["author"]);
    c.message = Unescape(fields["message"]);
    c.extra_info = Unescape(fields["extra_info"]);
    if (!IsUUID(c.branch) || c.descriptor.rfind("descriptors/", 0) != 0 || !IsHash(c.descriptor.substr(12)))
        throw std::runtime_error("[differential:corrupt_data] Corrupt commit record");
    c.database = fields["database"];
    return c;
}
// Numbers of the keys under an inverted-number index (snapshots/ or expired/).
std::vector<int64_t> ListNumbers(S3Provider &provider, const std::string &within) {
    std::vector<int64_t> numbers;
    std::string continuation;
    do {
        auto page = provider.List(continuation, 1000, within);
        for (auto &key : page.keys) {
            auto inverted = key.substr(within.size());
            if (inverted.size() != 10 || inverted.find_first_not_of("0123456789") != std::string::npos)
                throw std::runtime_error("[differential:corrupt_data] Corrupt record key " + key);
            numbers.push_back(SNAPSHOT_KEY_MAX - std::stoll(inverted));
        }
        continuation = page.continuation;
    } while (!continuation.empty());
    return numbers;
}
std::string ExpiryKey(const std::string &database, int64_t snapshot) {
    auto inverted = std::to_string(SNAPSHOT_KEY_MAX - snapshot);
    return Pool(database) + "expired/" + std::string(10 - inverted.size(), '0') + inverted;
}
std::mutex history_mutex;
std::map<std::string, std::chrono::steady_clock::time_point> history_synced;
} // namespace
void SyncHistory(Control &control, const std::string &database, bool throttled) {
    if (control.Writers(database) != "shared")
        return;
    auto id = control.FindDatabase(database);
    auto key = control.store->directory + "/" + id;
    auto now = std::chrono::steady_clock::now();
    if (throttled) {
        std::lock_guard<std::mutex> guard(history_mutex);
        auto found = history_synced.find(key);
        if (found != history_synced.end() && now - found->second < std::chrono::seconds(5))
            return;
    }
    auto provider = Client(control, control.Profile(id));
    auto cached = control.CachedCommits(id);
    std::vector<int64_t> missing;
    for (auto n : ListNumbers(*provider, Pool(id) + "snapshots/"))
        if (!cached.count(n))
            missing.push_back(n);
    std::vector<RemoteCommit> commits(missing.size());
    Parallel(missing.size(), provider->Concurrency(), [&](size_t i) {
        auto object = provider->ReadMutable(CommitKey(id, missing[i]));
        if (!object)
            throw std::runtime_error("[differential:unavailable_object] Commit record disappeared");
        auto commit = DecodeCommit(object->first);
        if (!commit || commit->database != id || commit->snapshot != missing[i])
            throw std::runtime_error("[differential:corrupt_data] Commit record does not match its key");
        commits[i] = *commit;
    });
    control.CacheCommits(id, commits);
    control.ExpireCommits(id, ListNumbers(*provider, Pool(id) + "expired/"));
    std::lock_guard<std::mutex> guard(history_mutex);
    history_synced[key] = now;
}
void ExpireShared(Control &control, const std::string &database, const std::vector<int64_t> &snapshots) {
    auto id = control.FindDatabase(database);
    auto provider = Client(control, control.Profile(id));
    // Branch heads in S3 (dropped branches have none).
    std::map<int64_t, std::string> heads;
    std::string continuation;
    auto within = Pool(id) + "branches/";
    do {
        auto page = provider->List(continuation, 1000, within);
        for (auto &key : page.keys)
            if (auto object = provider->ReadMutable(key))
                if (!IsDropped(object->first))
                    heads[DecodeHead(object->first).snapshot] = key.substr(within.size());
        continuation = page.continuation;
    } while (!continuation.empty());
    for (auto n : snapshots)
        if (heads.count(n))
            throw std::runtime_error("[differential:retired] Snapshot " + std::to_string(n) + " is the head of branch " +
                                     heads[n] + " in S3 and cannot expire");
    auto repository = control.RepositoryID();
    for (auto n : snapshots) {
        auto text = "differential-expired-v1\nsnapshot " + std::to_string(n) + "\nexpired_at " +
                    std::to_string(Control::Now()) + "\nrepository " + repository + "\n";
        provider->PutRecord(ExpiryKey(id, n), {text.begin(), text.end()}); // false: already expired
        Failpoint("DIFFERENTIAL_SHARED_CRASH", "expiry_recorded");
    }
    control.ExpireCommits(id, snapshots);
}
std::string PrepareSharedDrop(Control &control, const std::string &database, const std::string &branch) {
    auto id = control.FindDatabase(database);
    auto provider_id = control.Profile(id);
    auto remote = ReadSharedHead(*Client(control, provider_id), id, DatabaseName(control, id), branch);
    if (!remote)
        return "";
    Head key;
    key.database = id;
    key.branch_name = branch;
    auto base = control.HeadWriteFor(provider_id, key.Key());
    if (!remote->head)
        return ""; // already dropped in S3
    if (!base || base->etag != remote->etag)
        throw std::runtime_error("[differential:branch_conflict] Branch " + branch + " moved in S3 (snapshot " +
                                 std::to_string(remote->head->snapshot) + ") since this repository last saw it; "
                                 "ATTACH it to follow S3 (or reconcile it), then drop it");
    return remote->etag;
}
void TombstoneBranch(Control &control, const std::string &database, const Branch &branch, const std::string &etag) {
    if (etag.empty())
        return; // never published
    auto id = control.FindDatabase(database);
    auto provider_id = control.Profile(id);
    Head head;
    head.database = id;
    head.database_name = DatabaseName(control, id);
    head.branch = branch.id;
    head.branch_name = branch.name;
    head.write_id = WriteID();
    auto written = Client(control, provider_id)->PutMutable(head.Key(), EncodeDropped(head, Control::Now()), etag);
    if (!written)
        throw std::runtime_error("[differential:branch_conflict] Branch " + branch.name + " was dropped here, but its "
                                 "S3 head changed meanwhile and was left as it is");
    // A new branch of this name publishes on top of the tombstone.
    control.RecordHeadWrite(provider_id, head.Key(), HeadWrite{*written, head.write_id, "", 0});
}
int64_t RemoteHeadSnapshot(Control &control, const std::string &database, const std::string &branch) {
    auto id = control.FindDatabase(database);
    auto remote = ReadSharedHead(*Client(control, control.Profile(id)), id, DatabaseName(control, id), branch);
    return remote && remote->head ? remote->head->snapshot : 0;
}
void EnsureLocalSnapshot(Control &control, const std::string &database, int64_t snapshot) {
    if (control.Writers(database) != "shared")
        return;
    auto id = control.FindDatabase(database);
    Snapshot s;
    try {
        s = control.FindSnapshot(id, snapshot, false);
    } catch (const std::exception &) {
        SyncHistory(control, id);
        s = control.FindSnapshot(id, snapshot, false);
    }
    if (!s.remote)
        return;
    auto provider_id = control.Profile(id);
    RemoteSnapshot remote(control, provider_id, s.export_key);
    if (remote.descriptor.database != id || remote.descriptor.snapshot != snapshot)
        throw std::runtime_error("[differential:corrupt_data] Commit record does not match its export descriptor");
    RemoteHead head;
    head.descriptor = s.export_key;
    head.root = remote.descriptor.root;
    head.snapshot = snapshot;
    head.published_at = s.published_at;
    head.size = remote.descriptor.size;
    head.info = {s.name, s.author, s.message, s.extra_info};
    for (uint64_t page = 0; page < remote.descriptor.pages.size(); page++)
        for (auto &entry : remote.Entries(page)) {
            head.locations[entry.first.object] = entry.second;
            head.chunks.push_back(std::move(entry.first));
        }
    // Attributed to its publishing branch when that is here, else to a local branch of the database.
    std::string branch;
    for (auto &b : control.Branches(id))
        if (b.id == s.branch || (branch.empty() && b.state == "active"))
            branch = b.id;
    if (branch.empty())
        throw std::runtime_error("[differential:not_found] Database has no local branch");
    control.ImportRemoteSnapshot(id, provider_id, head, branch);
}
void ScheduleHistorySync(const std::shared_ptr<Control> &control, const std::string &database) {
    Publisher::Instance().Schedule(control, database, "");
}
} // namespace differential
