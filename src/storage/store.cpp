#include "differential_store.hpp"
#include "differential_crc.hpp"
#include "differential_local.hpp"
#include "differential_metrics.hpp"
#include "differential_segment.hpp"
#include "differential_sql.hpp"
#include <openssl/evp.h>
#include <openssl/rand.h>
#include <algorithm>
#include <array>
#include <cstring>
#include <filesystem>
#include <limits>
#include <list>
#include <set>
#include <stdexcept>
#include <unordered_map>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace differential {
namespace {
constexpr uint64_t MAX_FILE = uint64_t(1) << 40; // Explicit bound, 1 TiB logical file.
constexpr uint64_t NONE = std::numeric_limits<uint64_t>::max();
constexpr uint64_t RECORD_BYTES = 16ull * 1024 * 1024; // largest segment record of a native write
constexpr size_t MAX_IDLE_READERS = 16;
constexpr uint64_t COMPACT_MIN_BYTES = 4ull * 1024 * 1024; // smaller retired segments are left alone
void CheckRange(uint64_t n, uint64_t p) {
    if (p > MAX_FILE || n > MAX_FILE - p)
        throw std::runtime_error("differential: file range exceeds 1 TiB limit");
}
void Crash(const char *name) { Failpoint("DIFFERENTIAL_STORE_CRASH", name); }
struct Digest {
    EVP_MD_CTX *ctx = EVP_MD_CTX_new();
    Digest() {
        if (!ctx || EVP_DigestInit_ex(ctx, EVP_sha256(), nullptr) != 1) {
            EVP_MD_CTX_free(ctx);
            throw std::runtime_error("SHA256 init failed");
        }
    }
    Digest(const Digest &) = delete;
    ~Digest() { EVP_MD_CTX_free(ctx); }
    void Add(const void *p, size_t n) {
        if (EVP_DigestUpdate(ctx, p, n) != 1)
            throw std::runtime_error("SHA256 update failed");
    }
    void Add(const std::string &s) { Add(s.data(), s.size()); }
    std::string Finish() {
        std::array<unsigned char, 32> bytes;
        unsigned n;
        if (EVP_DigestFinal_ex(ctx, bytes.data(), &n) != 1 || n != bytes.size())
            throw std::runtime_error("SHA256 failed");
        constexpr char hex[] = "0123456789abcdef";
        std::string result;
        for (auto b : bytes) {
            result += hex[b >> 4];
            result += hex[b & 15];
        }
        return result;
    }
};
std::string Hash(const void *data, size_t n) {
    Digest digest;
    digest.Add(data, n);
    return digest.Finish();
}
void SyncDirectory(const std::filesystem::path &path) {
    int fd = open(path.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (fd < 0)
        throw std::runtime_error("Cannot open repository directory for sync");
    int rc = fsync(fd);
    close(fd);
    if (rc)
        throw std::runtime_error("Cannot sync repository directory");
}
const SharedBytes &ZeroChunk() {
    static const SharedBytes zero = std::make_shared<const Bytes>(Store::CHUNK, 0);
    return zero;
}
// A read that found a segment moved or deleted under it (compaction, collection): the whole read
// is retried on a new metadata snapshot.
struct StaleRead : std::runtime_error {
    StaleRead() : std::runtime_error("[differential:unavailable_object] Local payload moved during a read") {}
};
// Process-wide cache of verified immutable bytes: chunks by content identity (valid for every
// store and pool: equal identity implies equal bytes) and log-file records by repository,
// segment and position (segments are append-only and their identities are never reused).
class ChunkCache {
  public:
    static constexpr size_t LIMIT = 64ull * 1024 * 1024;
    static ChunkCache &Instance() {
        static ChunkCache cache;
        return cache;
    }
    SharedBytes Find(const std::string &key) {
        std::lock_guard<std::mutex> guard(mutex);
        auto found = entries.find(key);
        if (found == entries.end())
            return nullptr;
        age.splice(age.begin(), age, found->second.second);
        return found->second.first;
    }
    void Insert(const std::string &key, SharedBytes bytes) {
        std::lock_guard<std::mutex> guard(mutex);
        if (entries.count(key) || bytes->size() > LIMIT / 8)
            return;
        while (size + bytes->size() > LIMIT && !age.empty()) {
            auto &victim = entries.at(age.back());
            size -= victim.first->size();
            entries.erase(age.back());
            age.pop_back();
        }
        age.push_front(key);
        size += bytes->size();
        entries.emplace(key, std::make_pair(std::move(bytes), age.begin()));
    }

    void Clear() {
        std::lock_guard<std::mutex> guard(mutex);
        entries.clear();
        age.clear();
        size = 0;
    }

  private:
    std::mutex mutex;
    std::list<std::string> age;
    std::unordered_map<std::string, std::pair<SharedBytes, std::list<std::string>::iterator>> entries;
    size_t size = 0;
};
// Process-wide cache of immutable root maps, one entry per manifest page: a root identity
// determines its map.
class RootPages {
  public:
    using Page = std::shared_ptr<const std::vector<std::string>>;
    static constexpr size_t LIMIT = 4096;
    static RootPages &Instance() {
        static RootPages cache;
        return cache;
    }
    Page Find(const std::string &root, uint64_t page) {
        std::lock_guard<std::mutex> guard(mutex);
        auto found = pages.find(Key(root, page));
        return found == pages.end() ? nullptr : found->second;
    }
    void Insert(const std::string &root, uint64_t page, Page entries) {
        std::lock_guard<std::mutex> guard(mutex);
        if (pages.size() >= LIMIT)
            pages.clear();
        pages.emplace(Key(root, page), std::move(entries));
    }
    void Clear() {
        std::lock_guard<std::mutex> guard(mutex);
        pages.clear();
    }

  private:
    static std::string Key(const std::string &root, uint64_t page) { return root + ":" + std::to_string(page); }
    std::mutex mutex;
    std::unordered_map<std::string, Page> pages;
};
std::string RootHeader(uint64_t size) { return "differential-manifest-v1\n" + std::to_string(size) + "\n"; }
struct RootInfo {
    uint64_t size;
};
bool FindRoot(sqlite3 *db, const std::string &root, RootInfo &info) {
    Query q(db, "SELECT size FROM roots WHERE hash=? AND hash NOT IN (SELECT root FROM reclaimed_roots)");
    q.Text(1, root);
    if (!q.Step())
        return false;
    info = {uint64_t(q.Int(0))};
    return true;
}
// Recompute an immutable root's identity from its stored map and return its length.
uint64_t VerifyRoot(sqlite3 *db, const std::string &root) {
    RootInfo info{};
    if (!FindRoot(db, root, info))
        throw std::runtime_error("[differential:not_found] Root not found");
    Digest digest;
    digest.Add(RootHeader(info.size));
    Query pages(db, "SELECT page_no,page FROM root_pages WHERE root=? ORDER BY page_no");
    pages.Text(1, root);
    while (pages.Step())
        digest.Add(std::to_string(pages.Int(0)) + ":" + pages.Text(1) + "\n");
    if (digest.Finish() != root)
        throw std::runtime_error("[differential:corrupt_data] Corrupt immutable manifest");
    return info.size;
}
} // namespace

// ---------------------------------------------------------------------------------------
// Segment files for reads: cached descriptors and cached object locations. A cached location stays
// correct while its object exists (content addressing); after compaction or collection the open
// fails (StaleRead) and the read retries on fresh metadata, while a descriptor that is still open
// keeps reading the unlinked file's unchanged bytes.
struct Store::Location {
    int64_t segment = 0;
    uint64_t position = 0;
    uint32_t crc = 0;
};
struct Store::RangeRow {
    uint64_t start, length;
    int64_t segment;
    uint64_t record, record_length;
    uint32_t crc;
    uint64_t skip;
};
struct Store::SegmentFiles {
    struct Descriptor {
        int fd;
        explicit Descriptor(int fd) : fd(fd) {}
        ~Descriptor() { close(fd); }
    };
    static constexpr size_t MAX_FILES = 64, MAX_LOCATIONS = 1 << 20;
    explicit SegmentFiles(std::string directory) : directory(std::move(directory)) {
        std::lock_guard<std::mutex> guard(Registry().first);
        Registry().second.insert(this);
    }
    ~SegmentFiles() {
        std::lock_guard<std::mutex> guard(Registry().first);
        Registry().second.erase(this);
    }
    std::shared_ptr<Descriptor> Open(int64_t id) {
        {
            std::lock_guard<std::mutex> guard(mutex);
            auto found = files.find(id);
            if (found != files.end())
                return found->second;
        }
        auto name = Segment::Name(id);
        int fd = open((directory + "/segments/" + name).c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW);
        if (fd < 0 && errno == ENOENT)
            fd = open((directory + "/journal/" + name).c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW);
        if (fd < 0 && errno == ENOENT)
            throw StaleRead();
        if (fd < 0)
            throw std::runtime_error("[differential:local_io] Cannot open segment");
        auto descriptor = std::make_shared<Descriptor>(fd);
        std::lock_guard<std::mutex> guard(mutex);
        if (files.size() >= MAX_FILES)
            files.clear(); // readers keep the descriptors they hold
        return files.emplace(id, descriptor).first->second;
    }
    void Forget(int64_t id) {
        std::lock_guard<std::mutex> guard(mutex);
        files.erase(id);
    }
    bool FindLocation(const std::string &hash, Location &location) {
        std::shared_lock<std::shared_mutex> guard(locations_mutex);
        auto found = locations.find(hash);
        if (found == locations.end())
            return false;
        location = found->second;
        return true;
    }
    void InsertLocation(const std::string &hash, const Location &location) {
        std::unique_lock<std::shared_mutex> guard(locations_mutex);
        if (locations.size() >= MAX_LOCATIONS)
            locations.clear();
        locations[hash] = location;
    }
    void ForgetLocation(const std::string &hash) {
        std::unique_lock<std::shared_mutex> guard(locations_mutex);
        locations.erase(hash);
    }
    void Clear() {
        {
            std::lock_guard<std::mutex> guard(mutex);
            files.clear();
        }
        std::unique_lock<std::shared_mutex> guard(locations_mutex);
        locations.clear();
    }
    static std::pair<std::mutex, std::set<SegmentFiles *>> &Registry() {
        static std::pair<std::mutex, std::set<SegmentFiles *>> registry;
        return registry;
    }
    const std::string directory;
    std::mutex mutex;
    std::unordered_map<int64_t, std::shared_ptr<Descriptor>> files;
    std::shared_mutex locations_mutex;
    std::unordered_map<std::string, Location> locations;
};

void Store::DropCaches() {
    ChunkCache::Instance().Clear();
    RootPages::Instance().Clear();
    auto &registry = SegmentFiles::Registry();
    std::lock_guard<std::mutex> guard(registry.first);
    for (auto *files : registry.second)
        files->Clear();
}

// ---------------------------------------------------------------------------------------
// Pooled readers
Reader::Reader(const std::string &path) {
    if (sqlite3_open_v2(path.c_str(), &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_NOMUTEX, nullptr) != SQLITE_OK) {
        sqlite3_close_v2(db);
        db = nullptr;
        throw std::runtime_error("Cannot open differential metadata reader");
    }
    sqlite3_busy_timeout(db, 10000);
    Execute(db, "PRAGMA query_only=1; PRAGMA cache_size=-2048; PRAGMA mmap_size=1099511627776;");
}
Reader::~Reader() {
    for (auto &entry : statements)
        sqlite3_finalize(entry.second);
    if (db)
        sqlite3_close_v2(db);
}
sqlite3_stmt *Reader::Statement(const char *sql) {
    auto found = statements.find(sql);
    if (found != statements.end())
        return found->second;
    sqlite3_stmt *s = nullptr;
    if (sqlite3_prepare_v3(db, sql, -1, SQLITE_PREPARE_PERSISTENT, &s, nullptr) != SQLITE_OK)
        throw std::runtime_error(SQLiteError(db));
    statements.emplace(sql, s);
    return s;
}
void Reader::Begin() { Execute(db, "BEGIN"); }
std::unique_ptr<Reader> Store::AcquireReader() {
    {
        std::lock_guard<std::mutex> guard(readers_mutex);
        if (!readers.empty()) {
            auto reader = std::move(readers.back());
            readers.pop_back();
            return reader;
        }
    }
    return std::make_unique<Reader>((std::filesystem::path(directory) / "engine.sqlite").string());
}
void Store::ReleaseReader(std::unique_ptr<Reader> reader) {
    std::lock_guard<std::mutex> guard(readers_mutex);
    if (readers.size() < MAX_IDLE_READERS)
        readers.push_back(std::move(reader));
}
namespace {
// One read transaction on a pooled connection; returned to the pool after a clean commit.
struct ReadScope {
    std::unique_ptr<Reader> reader;
    std::function<void(std::unique_ptr<Reader>)> release;
    ReadScope(std::unique_ptr<Reader> r, std::function<void(std::unique_ptr<Reader>)> release)
        : reader(std::move(r)), release(std::move(release)) {
        reader->Begin();
    }
    ReadScope(const ReadScope &) = delete;
    ~ReadScope() {
        if (sqlite3_exec(reader->db, "COMMIT", nullptr, nullptr, nullptr) != SQLITE_OK) {
            sqlite3_exec(reader->db, "ROLLBACK", nullptr, nullptr, nullptr);
            return; // discard a connection in an unknown state
        }
        release(std::move(reader));
    }
};
} // namespace
void Store::WithReader(const std::function<void(Reader &)> &fn) {
    // A read that raced with segment compaction or collection runs again on a new snapshot.
    for (int attempt = 0;; ++attempt) {
        try {
            ReadScope scope(AcquireReader(), [this](std::unique_ptr<Reader> r) { ReleaseReader(std::move(r)); });
            fn(*scope.reader);
            return;
        } catch (const StaleRead &) {
            if (attempt >= 3)
                throw;
        }
    }
}

// ---------------------------------------------------------------------------------------
// Construction: one store format; a new repository gets the schema, anything else is refused
struct Store::Inode {
    std::shared_mutex lock;
    // Sequential read streams over remote-only chunks, one per scanning thread (DuckDB's threads share
    // a handle and interleave their reads). Least recently used first out beyond 16.
    struct Stream {
        uint64_t next = 0;   // offset the stream's next read would start at
        uint64_t ahead = 0;  // chunks before this index were already requested
        uint64_t window = 0; // read-ahead size in chunks; doubles per refill
        unsigned reads = 0;  // consecutive sequential reads
        uint64_t used = 0;
    };
    std::mutex stream_mutex;
    std::vector<Stream> streams;
    uint64_t stream_clock = 0;
    int64_t id = 0;
    std::shared_ptr<Group> group; // fixed while the inode is cached
    std::string pool, pool_path, pool_repository;
    bool log = false; // byte ranges (log file), else chunks
    bool dirty = false;
    uint64_t size = 0;
    // Committed data at or above this point is truncated: a chunk index, or a byte offset (log).
    uint64_t floor = NONE;
    // Buffered chunks; `segment` (non-zero) names the segment record that holds exactly its bytes.
    struct Chunk {
        std::shared_ptr<Bytes> bytes;
        int64_t segment = 0;
        uint64_t position = 0;
    };
    std::map<uint64_t, Chunk> chunks;
    // Buffered log-file writes by file offset: bytes [skip, skip+length) of a record payload `data`,
    // stored at `record` of `segment` (zero until written).
    struct Piece {
        SharedBytes data;
        uint64_t skip = 0, length = 0;
        int64_t segment = 0;
        uint64_t record = 0;
        uint32_t crc = 0;
    };
    std::map<uint64_t, Piece> pieces;
    // Committed chunk map, one manifest page per entry, valid while files.version is unchanged.
    std::mutex cache_mutex;
    int64_t cached_version = -1;
    std::unordered_map<uint64_t, std::shared_ptr<const std::vector<std::string>>> pages;
};
// A file and its sidecars publish together: native syncs of one database never wait for, or
// commit, the buffered writes of another.
struct Store::Group {
    std::string key;
    std::shared_mutex gate; // writes shared; publication exclusive
    std::mutex segment_mutex;
    std::unique_ptr<Segment> segment; // created by the group's first record
    std::atomic<uint64_t> dirty_bytes{0};
    std::atomic<bool> scheduled{false}; // queued for background publication
};
std::shared_ptr<Store::Group> Store::GetGroup(const std::string &key) {
    std::lock_guard<std::mutex> guard(groups_mutex);
    auto &group = groups[key];
    if (!group) {
        group = std::make_shared<Group>();
        group->key = key;
    }
    return group;
}
Store::Store(const std::string &dir, bool share) : directory(dir) {
    static std::once_flag configured;
    std::call_once(configured, [] {
        // Global allocation statistics take a process-wide mutex on every SQLite allocation.
        // Only effective before SQLite initializes; harmless (SQLITE_MISUSE) afterwards.
        sqlite3_config(SQLITE_CONFIG_MEMSTATUS, 0);
    });
    std::filesystem::path p(dir);
    if (!p.is_absolute())
        throw std::runtime_error("Repository root must be absolute");
    segment_files = std::make_unique<SegmentFiles>(dir);
    // Caller chooses a repository directory; do not overwrite another file/format. A new
    // repository is private unless sharing was requested; an existing one keeps its mode.
    bool fresh = !std::filesystem::exists(p / "engine.sqlite");
    permissions.shared = share && fresh;
    auto created_directories = permissions.CreateDirectories(p);
    for (auto &created : created_directories)
        SyncDirectory(created);
    if (!created_directories.empty())
        SyncDirectory(created_directories.front().parent_path());
    try {
        int created_fd = permissions.Open(p / "engine.sqlite", O_WRONLY | O_EXCL | O_CLOEXEC);
        if (created_fd >= 0)
            close(created_fd);
        if (sqlite3_open_v2((p / "engine.sqlite").c_str(), &db,
                            SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX,
                            nullptr) != SQLITE_OK)
            throw std::runtime_error("Cannot open differential metadata");
        sqlite3_busy_timeout(db, 10000);
        int64_t version;
        {
            Query q(db, "PRAGMA user_version");
            q.Step();
            version = q.Int(0);
            if (version != 0 && version != FORMAT)
                throw std::runtime_error("[differential:incompatible_format] Unsupported differential store format " +
                                         std::to_string(version) + " (expected " + std::to_string(FORMAT) + ")");
        }
        {
            Query app(db, "PRAGMA application_id");
            app.Step();
            auto id = app.Int(0);
            if (id != 0 && id != 1145652806)
                throw std::runtime_error("[differential:incompatible_format] Not a differential repository");
            if (id == 0) {
                Query tables(db, "SELECT count(*) FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'");
                tables.Step();
                if (tables.Int(0) != 0)
                    throw std::runtime_error(
                        "[differential:incompatible_format] Refusing to initialize an unrelated SQLite file");
                Execute(db, "PRAGMA auto_vacuum=INCREMENTAL");
            }
        }
        // SQLite reuses its WAL file after a checkpoint without shrinking it; the limit truncates
        // a WAL that a lagging checkpoint let grow beyond 64 MiB when the log restarts.
        // Durable at fsync, as native DuckDB on every OS (no F_FULLFSYNC device-cache flush on macOS).
        Execute(db, "PRAGMA foreign_keys=ON; PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA "
                    "fullfsync=OFF; PRAGMA checkpoint_fullfsync=OFF; PRAGMA cache_size=-8192; "
                    "PRAGMA journal_size_limit=67108864;");
        if (version == 0)
            CreateSchema(permissions.shared);
        // Continuous WAL shipping: tracked groups, their unshipped changes and the shipping sequence.
        Execute(db, R"SQL(
CREATE TABLE IF NOT EXISTS ship_groups(group_key TEXT PRIMARY KEY) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS ship_chunks(file INTEGER NOT NULL, chunk INTEGER NOT NULL, seq INTEGER NOT NULL, PRIMARY KEY(file,chunk)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS ship_lows(file INTEGER PRIMARY KEY, low INTEGER NOT NULL, seq INTEGER NOT NULL);
INSERT OR IGNORE INTO allocators VALUES('ship',0);
)SQL");
        {
            Query q(db, "SELECT group_key FROM ship_groups");
            while (q.Step())
                shipping_groups.insert(q.Text(0));
        }
        {
            Query mode(db, "SELECT shared FROM repository");
            if (!mode.Step())
                throw std::runtime_error("[differential:corrupt_data] Repository mode missing");
            permissions.shared = mode.Int(0) != 0;
        }
        if (share && !permissions.shared)
            throw std::runtime_error("Repository " + dir + " was created for a single user; sharing with other "
                                     "local users is chosen when a repository is created");
        Permissions::Register(directory, permissions);
        {
            Query remote(db, "SELECT EXISTS(SELECT 1 FROM remote_objects)");
            remote_objects = remote.Step() && remote.Int(0) != 0;
        }
        // Commits never copy WAL frames into the database file themselves.
        sqlite3_wal_hook(db, &Store::WalHook, this);
        SyncDirectory(p);
        SyncDirectory(p.parent_path());
        for (auto folder : {"journal", "segments"})
            for (auto &created : permissions.CreateDirectories(p / folder))
                SyncDirectory(created.parent_path());
        ReplayJournals();
    } catch (...) {
        readers.clear();
        if (db)
            sqlite3_close_v2(db);
        db = nullptr;
        throw;
    }
}
void Store::CreateSchema(bool shared) {
    Transaction tx(db, "DIFFERENTIAL_STORE_CRASH");
    // `kind`: 0 chunked, 1 log file (byte ranges). `version` changes with every update of a file's map.
    Execute(db, R"SQL(
CREATE TABLE IF NOT EXISTS files(id INTEGER PRIMARY KEY, name TEXT UNIQUE, size INTEGER NOT NULL CHECK(size>=0), kind INTEGER NOT NULL DEFAULT 0 CHECK(kind IN (0,1)), version INTEGER NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS segments(id INTEGER PRIMARY KEY, committed INTEGER NOT NULL CHECK(committed>=0), active INTEGER NOT NULL CHECK(active IN (0,1)));
CREATE TABLE IF NOT EXISTS objects(hash TEXT PRIMARY KEY, segment INTEGER REFERENCES segments(id), position INTEGER, crc INTEGER,
 CHECK((segment IS NULL AND position IS NULL AND crc IS NULL) OR (segment IS NOT NULL AND position>=0 AND crc IS NOT NULL))) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS objects_segment ON objects(segment) WHERE segment IS NOT NULL;
CREATE TABLE IF NOT EXISTS local_pools(id TEXT PRIMARY KEY, name TEXT UNIQUE NOT NULL, path TEXT NOT NULL, repository_id TEXT NOT NULL, gc_cursor TEXT NOT NULL DEFAULT '') WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS file_pools(file INTEGER PRIMARY KEY REFERENCES files(id) ON DELETE CASCADE, pool TEXT NOT NULL REFERENCES local_pools(id));
CREATE TABLE IF NOT EXISTS local_objects(hash TEXT NOT NULL REFERENCES objects(hash), pool TEXT NOT NULL REFERENCES local_pools(id), PRIMARY KEY(hash,pool)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS local_deletions(pool TEXT NOT NULL REFERENCES local_pools(id), hash TEXT NOT NULL, PRIMARY KEY(pool,hash)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS remote_objects(hash TEXT PRIMARY KEY REFERENCES objects(hash)) WITHOUT ROWID;
CREATE TRIGGER IF NOT EXISTS queue_local_deletions BEFORE DELETE ON objects BEGIN
INSERT INTO local_deletions SELECT o.pool,o.hash FROM local_objects o WHERE o.hash=OLD.hash AND NOT EXISTS(SELECT 1 FROM local_deletions d WHERE d.pool=o.pool AND d.hash=o.hash);
DELETE FROM local_objects WHERE hash=OLD.hash;
DELETE FROM remote_objects WHERE hash=OLD.hash;
END;
CREATE TABLE IF NOT EXISTS extents(file INTEGER NOT NULL REFERENCES files(id), chunk INTEGER NOT NULL CHECK(chunk>=0), object TEXT NOT NULL REFERENCES objects(hash), PRIMARY KEY(file,chunk)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS ranges(file INTEGER NOT NULL REFERENCES files(id), start INTEGER NOT NULL CHECK(start>=0), length INTEGER NOT NULL CHECK(length>0),
 segment INTEGER NOT NULL REFERENCES segments(id), record INTEGER NOT NULL CHECK(record>=0), record_length INTEGER NOT NULL, crc INTEGER NOT NULL,
 skip INTEGER NOT NULL CHECK(skip>=0 AND skip+length<=record_length), PRIMARY KEY(file,start)) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS ranges_segment ON ranges(segment,record);
CREATE TABLE IF NOT EXISTS roots(hash TEXT PRIMARY KEY, size INTEGER NOT NULL CHECK(size>=0)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS generations(file INTEGER PRIMARY KEY REFERENCES files(id), root TEXT NOT NULL REFERENCES roots(hash));
CREATE TABLE IF NOT EXISTS pins(id TEXT PRIMARY KEY, root TEXT REFERENCES roots(hash), file INTEGER REFERENCES files(id)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS reclaimed_roots(root TEXT PRIMARY KEY REFERENCES roots(hash)) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS extents_object ON extents(object);
CREATE INDEX IF NOT EXISTS pins_file ON pins(file);
CREATE INDEX IF NOT EXISTS pins_root ON pins(root);
)SQL");
    // Paged manifests, durable allocator and GC queues.
    Execute(db, R"SQL(
CREATE TABLE IF NOT EXISTS manifest_pages(hash TEXT PRIMARY KEY) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS page_entries(page TEXT NOT NULL REFERENCES manifest_pages(hash), slot INTEGER NOT NULL CHECK(slot>=0 AND slot<256), object TEXT NOT NULL REFERENCES objects(hash), PRIMARY KEY(page,slot)) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS page_entries_object ON page_entries(object);
CREATE TABLE IF NOT EXISTS root_pages(root TEXT NOT NULL REFERENCES roots(hash), page_no INTEGER NOT NULL CHECK(page_no>=0), page TEXT NOT NULL REFERENCES manifest_pages(hash), PRIMARY KEY(root,page_no)) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS root_pages_page ON root_pages(page);
CREATE TABLE IF NOT EXISTS file_pages(file INTEGER NOT NULL REFERENCES files(id), page_no INTEGER NOT NULL CHECK(page_no>=0), page TEXT REFERENCES manifest_pages(hash), dirty INTEGER NOT NULL CHECK(dirty IN (0,1)), PRIMARY KEY(file,page_no)) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS file_pages_page ON file_pages(page);
CREATE INDEX IF NOT EXISTS file_pages_dirty ON file_pages(file,page_no) WHERE dirty=1;
CREATE INDEX IF NOT EXISTS generations_root ON generations(root);
CREATE TABLE IF NOT EXISTS allocators(name TEXT PRIMARY KEY, next INTEGER NOT NULL) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS gc_objects(hash TEXT PRIMARY KEY) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS gc_pages(hash TEXT PRIMARY KEY) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS gc_roots(hash TEXT PRIMARY KEY) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS gc_files(id INTEGER PRIMARY KEY);
CREATE TRIGGER IF NOT EXISTS gc_extent_delete AFTER DELETE ON extents BEGIN
INSERT INTO gc_objects SELECT OLD.object WHERE NOT EXISTS(SELECT 1 FROM gc_objects WHERE hash=OLD.object); END;
CREATE TRIGGER IF NOT EXISTS gc_extent_update AFTER UPDATE OF object ON extents WHEN OLD.object!=NEW.object BEGIN
INSERT INTO gc_objects SELECT OLD.object WHERE NOT EXISTS(SELECT 1 FROM gc_objects WHERE hash=OLD.object); END;
CREATE TRIGGER IF NOT EXISTS gc_page_entry_delete AFTER DELETE ON page_entries BEGIN
INSERT INTO gc_objects SELECT OLD.object WHERE NOT EXISTS(SELECT 1 FROM gc_objects WHERE hash=OLD.object); END;
CREATE TRIGGER IF NOT EXISTS gc_root_page_delete AFTER DELETE ON root_pages BEGIN
INSERT INTO gc_pages SELECT OLD.page WHERE NOT EXISTS(SELECT 1 FROM gc_pages WHERE hash=OLD.page); END;
CREATE TRIGGER IF NOT EXISTS gc_file_page_delete AFTER DELETE ON file_pages WHEN OLD.page IS NOT NULL BEGIN
INSERT INTO gc_pages SELECT OLD.page WHERE NOT EXISTS(SELECT 1 FROM gc_pages WHERE hash=OLD.page); END;
CREATE TRIGGER IF NOT EXISTS gc_file_page_update AFTER UPDATE OF page ON file_pages WHEN OLD.page IS NOT NULL AND OLD.page IS NOT NEW.page BEGIN
INSERT INTO gc_pages SELECT OLD.page WHERE NOT EXISTS(SELECT 1 FROM gc_pages WHERE hash=OLD.page); END;
CREATE TRIGGER IF NOT EXISTS gc_root_insert AFTER INSERT ON roots BEGIN
INSERT INTO gc_roots SELECT NEW.hash WHERE NOT EXISTS(SELECT 1 FROM gc_roots WHERE hash=NEW.hash); END;
CREATE TRIGGER IF NOT EXISTS gc_pin_delete AFTER DELETE ON pins BEGIN
INSERT INTO gc_roots SELECT OLD.root WHERE OLD.root IS NOT NULL AND NOT EXISTS(SELECT 1 FROM gc_roots WHERE hash=OLD.root);
INSERT INTO gc_files SELECT OLD.file WHERE OLD.file IS NOT NULL AND NOT EXISTS(SELECT 1 FROM gc_files WHERE id=OLD.file); END;
CREATE TRIGGER IF NOT EXISTS gc_pin_update AFTER UPDATE ON pins BEGIN
INSERT INTO gc_roots SELECT OLD.root WHERE OLD.root IS NOT NULL AND OLD.root IS NOT NEW.root AND NOT EXISTS(SELECT 1 FROM gc_roots WHERE hash=OLD.root);
INSERT INTO gc_files SELECT OLD.file WHERE OLD.file IS NOT NULL AND OLD.file IS NOT NEW.file AND NOT EXISTS(SELECT 1 FROM gc_files WHERE id=OLD.file); END;
CREATE TRIGGER IF NOT EXISTS gc_generation_update AFTER UPDATE OF root ON generations WHEN OLD.root!=NEW.root BEGIN
INSERT INTO gc_roots SELECT OLD.root WHERE NOT EXISTS(SELECT 1 FROM gc_roots WHERE hash=OLD.root); END;
CREATE TRIGGER IF NOT EXISTS gc_generation_delete AFTER DELETE ON generations BEGIN
INSERT INTO gc_roots SELECT OLD.root WHERE NOT EXISTS(SELECT 1 FROM gc_roots WHERE hash=OLD.root); END;
CREATE TRIGGER IF NOT EXISTS gc_file_unlink AFTER UPDATE OF name ON files WHEN NEW.name IS NULL AND OLD.name IS NOT NULL BEGIN
INSERT INTO gc_files SELECT OLD.id WHERE NOT EXISTS(SELECT 1 FROM gc_files WHERE id=OLD.id);
INSERT INTO gc_roots SELECT g.root FROM generations g WHERE g.file=OLD.id AND NOT EXISTS(SELECT 1 FROM gc_roots r WHERE r.hash=g.root); END;
CREATE TRIGGER IF NOT EXISTS gc_object_forget AFTER DELETE ON objects BEGIN
DELETE FROM gc_objects WHERE hash=OLD.hash; END;
)SQL");
    Execute(db, "INSERT OR IGNORE INTO allocators VALUES('file',coalesce((SELECT max(id) FROM files),0)+1)");
    // Chosen at creation: private (owner only) or shared with the repository directory's group.
    Execute(db, "CREATE TABLE IF NOT EXISTS repository(shared INTEGER NOT NULL CHECK(shared IN (0,1)))");
    Query(db, "INSERT INTO repository VALUES(?)").Int(1, shared ? 1 : 0).Run();
    Execute(db, ("PRAGMA application_id=1145652806; PRAGMA user_version=" + std::to_string(FORMAT) + ";").c_str());
    tx.Commit();
}
namespace {
constexpr int CHECKPOINT_PAGES = 1024;      // ~4 MiB of WAL (metadata only) at 4 KiB pages: ask the background
constexpr int FOREGROUND_CHECKPOINT = 65536; // ~256 MiB: bound WAL growth if the background lags
} // namespace
int Store::WalHook(void *argument, sqlite3 *connection, const char *, int pages) {
    auto store = static_cast<Store *>(argument);
    if (pages >= FOREGROUND_CHECKPOINT) {
        int log = 0, checkpointed = 0;
        sqlite3_wal_checkpoint_v2(connection, "main", SQLITE_CHECKPOINT_PASSIVE, &log, &checkpointed);
    } else if (pages >= CHECKPOINT_PAGES)
        store->RequestCheckpoint();
    return SQLITE_OK;
}
void Store::RequestCheckpoint() {
    std::lock_guard<std::mutex> guard(checkpoint_mutex);
    if (checkpoint_stopping)
        return;
    checkpoint_wanted = true;
    if (!checkpointer.joinable())
        checkpointer = std::thread([this] {
            sqlite3 *connection = nullptr;
            std::unique_lock<std::mutex> lock(checkpoint_mutex);
            while (true) {
                checkpoint_signal.wait(lock, [this] { return checkpoint_wanted || checkpoint_stopping; });
                if (checkpoint_stopping)
                    break;
                checkpoint_wanted = false;
                lock.unlock();
                if (!connection &&
                    sqlite3_open_v2((std::filesystem::path(directory) / "engine.sqlite").c_str(), &connection,
                                    SQLITE_OPEN_READWRITE | SQLITE_OPEN_NOMUTEX, nullptr) == SQLITE_OK) {
                    sqlite3_busy_timeout(connection, 0);
                    sqlite3_exec(connection,
                                 "PRAGMA synchronous=FULL; PRAGMA fullfsync=OFF; PRAGMA checkpoint_fullfsync=OFF;",
                                 nullptr, nullptr, nullptr);
                }
                if (connection) {
                    // PASSIVE never blocks writers or readers; frames still in use are retried later.
                    int log = 0, checkpointed = 0;
                    if (sqlite3_wal_checkpoint_v2(connection, "main", SQLITE_CHECKPOINT_PASSIVE, &log, &checkpointed) ==
                            SQLITE_OK &&
                        checkpointed > 0)
                        Count(Counter::WalCheckpoints);
                }
                lock.lock();
            }
            if (connection)
                sqlite3_close_v2(connection);
        });
    checkpoint_signal.notify_one();
}
Store::~Store() {
    {
        std::lock_guard<std::mutex> guard(checkpoint_mutex);
        checkpoint_stopping = true;
    }
    checkpoint_signal.notify_all();
    if (checkpointer.joinable())
        checkpointer.join();
    {
        std::lock_guard<std::mutex> guard(publisher_mutex);
        publisher_stopping = true;
    }
    publisher_signal.notify_all();
    if (publisher.joinable())
        publisher.join();
    bool clean = false;
    try {
        Sync();
        clean = true;
    } catch (...) {
        // Unpublished bytes remain in the segments; the next store replays them.
    }
    auto retired = (std::filesystem::path(directory) / "segments").string();
    for (auto &entry : groups) {
        auto &group = *entry.second;
        if (group.segment && clean) {
            try {
                if (group.segment->End() == 0)
                    unlink(group.segment->path.c_str());
                else {
                    {
                        std::lock_guard<std::recursive_mutex> guard(mutex);
                        Transaction tx(db, "DIFFERENTIAL_STORE_CRASH", false);
                        Query(db, "INSERT INTO segments VALUES(?,?,0) ON CONFLICT(id) DO UPDATE SET "
                                  "committed=excluded.committed,active=0")
                            .Int(1, group.segment->id)
                            .Int(2, int64_t(group.segment->End()))
                            .Run();
                        tx.Commit();
                    }
                    group.segment->Retire(retired);
                    if (!group.segment->referenced)
                        DeleteSegment(group.segment->id); // only published journal records
                }
            } catch (...) {
            }
        }
        group.segment.reset();
    }
    readers.clear();
    if (db)
        sqlite3_close_v2(db);
}

// ---------------------------------------------------------------------------------------
// Segments of dead processes: their records after the published watermark are completed writes
// that were never published; they are replayed into this store's overlay and published.
void Store::ReplayJournals() {
    static std::mutex replay_mutex;
    std::lock_guard<std::mutex> serialize(replay_mutex);
    auto folder = std::filesystem::path(directory) / "journal";
    auto retired = (std::filesystem::path(directory) / "segments").string();
    std::error_code error;
    if (!std::filesystem::exists(folder, error))
        return;
    for (auto &entry : std::filesystem::directory_iterator(folder)) {
        auto name = entry.path().filename().string();
        int64_t id = 0;
        if (!Segment::Parse(name, id) || !entry.is_regular_file())
            continue;
        int fd = Segment::TryClaim(entry.path().string()); // fails for live owners, this store included
        if (fd < 0)
            continue;
        try {
            uint64_t committed = 0;
            bool known = false, active = true;
            WithReader([&](Reader &reader) {
                Query q(reader.db, reader.Statement("SELECT committed,active FROM segments WHERE id=?"));
                q.Int(1, id);
                if (q.Step()) {
                    known = true;
                    committed = uint64_t(q.Int(0));
                    active = q.Int(1) != 0;
                }
            });
            struct stat st {};
            if (fstat(fd, &st))
                throw std::runtime_error("[differential:local_io] Cannot inspect segment");
            std::vector<SegmentRecord> records;
            if (active)
                records = Segment::ReadValid(fd, committed);
            for (auto &record : records) {
                if (record.type == SegmentRecord::OBJECT)
                    continue;
                bool exists = false;
                WithReader([&](Reader &reader) {
                    Query q(reader.db, reader.Statement("SELECT 1 FROM files WHERE id=?"));
                    q.Int(1, record.file);
                    exists = q.Step();
                });
                if (!exists)
                    continue; // the inode was unlinked and collected; its bytes are unreachable
                auto inode = GetInode(record.file);
                std::shared_lock<std::shared_mutex> gate(inode->group->gate);
                std::unique_lock<std::shared_mutex> lock(inode->lock);
                if (record.type == SegmentRecord::WRITE)
                    ApplyWrite(*inode, record.data.data(), record.data.size(), record.offset, false);
                else
                    ApplyTruncate(*inode, record.offset, false);
            }
            if (active || !known) {
                // The replayed writes and the retirement commit together: a crash before the commit
                // replays the same records again.
                auto end = std::max<uint64_t>(committed, uint64_t(st.st_size));
                FlushImpl(
                    true,
                    [&] {
                        Query(db, "INSERT INTO segments VALUES(?,?,0) ON CONFLICT(id) DO UPDATE SET "
                                  "committed=excluded.committed,active=0")
                            .Int(1, id)
                            .Int(2, int64_t(end))
                            .Run();
                    },
                    nullptr);
            }
            auto target = retired + "/" + name;
            if (rename(entry.path().c_str(), target.c_str()) && errno != ENOENT)
                throw std::runtime_error("[differential:local_io] Cannot retire replayed segment");
        } catch (...) {
            close(fd);
            throw;
        }
        close(fd);
    }
}

// ---------------------------------------------------------------------------------------
// Inodes and committed reads
std::shared_ptr<Store::Inode> Store::GetInode(int64_t file) {
    {
        std::lock_guard<std::mutex> guard(inodes_mutex);
        auto found = inodes.find(file);
        if (found != inodes.end())
            return found->second;
    }
    auto inode = std::make_shared<Inode>();
    inode->id = file;
    bool exists = false;
    std::string name;
    WithReader([&](Reader &reader) {
        Query q(reader.db, reader.Statement("SELECT f.id,p.pool,l.path,l.repository_id,f.name,f.kind FROM files f LEFT "
                                            "JOIN file_pools p ON p.file=f.id LEFT JOIN local_pools l ON l.id=p.pool "
                                            "WHERE f.id=?"));
        q.Int(1, file);
        if (q.Step()) {
            exists = true;
            inode->pool = q.Text(1);
            inode->pool_path = q.Text(2);
            inode->pool_repository = q.Text(3);
            // An unlinked inode publishes on its own.
            name = q.IsNull(4) ? "#" + std::to_string(file) : GroupKey(q.Text(4));
            inode->log = q.Int(5) == 1;
        }
    });
    if (!exists)
        throw std::runtime_error("Invalid file handle");
    inode->group = GetGroup(name);
    std::lock_guard<std::mutex> guard(inodes_mutex);
    return inodes.emplace(file, inode).first->second;
}
void Store::Load(Reader &reader, Inode &inode) {
    if (inode.dirty)
        return;
    Query q(reader.db, reader.Statement("SELECT size FROM files WHERE id=?"));
    q.Int(1, inode.id);
    if (!q.Step())
        throw std::runtime_error("Invalid file handle");
    inode.size = uint64_t(q.Int(0));
    inode.floor = NONE;
}
bool Store::FindObject(Reader &reader, const std::string &hash, Location &location) {
    if (segment_files->FindLocation(hash, location))
        return true;
    Timer timer(Metric::ExtentLookup);
    Query q(reader.db, reader.Statement("SELECT segment,position,crc FROM objects WHERE hash=?"));
    q.Text(1, hash);
    if (!q.Step())
        throw std::runtime_error("[differential:unavailable_object] Object unavailable");
    if (q.IsNull(0))
        return false;
    location = {q.Int(0), uint64_t(q.Int(1)), uint32_t(q.Int(2))};
    segment_files->InsertLocation(hash, location);
    return true;
}
void Store::ReadObject(const std::string &hash, const Location &location, unsigned char *out) {
    std::shared_ptr<SegmentFiles::Descriptor> file;
    try {
        file = segment_files->Open(location.segment);
    } catch (const StaleRead &) {
        segment_files->ForgetLocation(hash);
        throw;
    }
    if (!Segment::ReadExact(file->fd, out, CHUNK, location.position) || Crc32c(out, CHUNK) != location.crc) {
        segment_files->ForgetLocation(hash);
        throw std::runtime_error("[differential:corrupt_data] Corrupt immutable object");
    }
    Count(Counter::LocalMaterializedBytes, CHUNK);
}
SharedBytes Store::ObjectBytes(Reader &reader, const std::string &hash, const std::string &pool) {
    if (auto cached = ChunkCache::Instance().Find(hash))
        return cached;
    std::shared_ptr<Bytes> bytes;
    if (!pool.empty()) {
        Query q(reader.db, reader.Statement("SELECT p.path,p.repository_id FROM local_objects o JOIN local_pools p "
                                            "ON p.id=o.pool WHERE o.hash=? AND o.pool=?"));
        q.Text(1, hash).Text(2, pool);
        if (!q.Step())
            throw std::runtime_error("[differential:unavailable_object] Local payload reference unavailable");
        bytes = std::make_shared<Bytes>(LocalPool(q.Text(0), q.Text(1), pool, permissions).Get(hash));
        Count(Counter::LocalMaterializedBytes, bytes->size());
    } else {
        Location location;
        if (!FindObject(reader, hash, location)) {
            remote_objects = true;
            return FetchRemote({hash}).at(hash); // remote-only: read on demand
        }
        bytes = std::make_shared<Bytes>(CHUNK);
        ReadObject(hash, location, bytes->data());
    }
    SharedBytes shared = std::move(bytes);
    ChunkCache::Instance().Insert(hash, shared);
    return shared;
}
void Store::CopyObject(Reader &reader, const std::string &hash, const std::string &pool, uint64_t chunk,
                       uint64_t position, uint64_t n, unsigned char *out,
                       const std::map<std::string, SharedBytes> &fetched) {
    auto remote = fetched.find(hash);
    if (remote != fetched.end()) {
        std::memcpy(out, remote->second->data() + position, n);
        return;
    }
    if (pool.empty() && chunk > 0 && n == CHUNK) {
        // A whole chunk: verified straight into the caller's buffer, without a copy or the cache.
        Location location;
        if (FindObject(reader, hash, location)) {
            ReadObject(hash, location, out);
            return;
        }
    }
    std::memcpy(out, ObjectBytes(reader, hash, pool)->data() + position, n);
}
void Store::SetRemoteFetch(RemoteFetch fetch) {
    std::lock_guard<std::mutex> guard(remote_mutex);
    remote_fetch = std::move(fetch);
}
std::map<std::string, SharedBytes> Store::FetchRemote(const std::vector<std::string> &hashes,
                                                      const std::vector<std::string> &ahead, const void *context) {
    RemoteFetch fetch;
    {
        std::lock_guard<std::mutex> guard(remote_mutex);
        fetch = remote_fetch;
    }
    if (!fetch)
        throw std::runtime_error("[differential:unavailable_object] Object is stored only in S3, which this store "
                                 "cannot reach");
    std::map<std::string, SharedBytes> fetched;
    if (!ahead.empty()) {
        auto batch = hashes;
        batch.insert(batch.end(), ahead.begin(), ahead.end());
        try {
            fetched = fetch(batch, context);
            Count(Counter::RemoteObjectAheadBytes, ahead.size() * CHUNK);
        } catch (const std::exception &) {
            fetched.clear(); // read-ahead is best effort: the read itself retries alone
        }
    }
    if (fetched.empty() && !hashes.empty())
        fetched = fetch(hashes, context);
    for (auto &hash : hashes) {
        auto found = fetched.find(hash);
        if (found == fetched.end() || !found->second || found->second->size() != CHUNK)
            throw std::runtime_error("[differential:unavailable_object] Remote object unavailable");
        Count(Counter::RemoteObjectReadBytes, CHUNK);
        ChunkCache::Instance().Insert(hash, found->second);
    }
    return fetched;
}
std::map<std::string, SharedBytes> Store::LoadObjects(Reader &reader, const std::vector<std::string> &hashes,
                                                     const std::vector<std::string> &ahead, const void *context) {
    // Only remote-only objects are loaded here (local ones are read where they are copied).
    std::map<std::string, SharedBytes> loaded;
    std::vector<std::string> remote, remote_ahead;
    auto remote_only = [&](const std::string &hash) {
        Location location;
        if (segment_files->FindLocation(hash, location))
            return false;
        Query q(reader.db, reader.Statement("SELECT segment IS NULL FROM objects WHERE hash=?"));
        q.Text(1, hash);
        return q.Step() && q.Int(0) != 0;
    };
    for (auto &hash : hashes) {
        if (loaded.count(hash) || std::find(remote.begin(), remote.end(), hash) != remote.end())
            continue;
        if (auto cached = ChunkCache::Instance().Find(hash)) {
            loaded.emplace(hash, cached);
            continue;
        }
        if (remote_only(hash))
            remote.push_back(hash);
    }
    std::set<std::string> seen(hashes.begin(), hashes.end());
    for (auto &hash : ahead)
        if (seen.insert(hash).second && !ChunkCache::Instance().Find(hash) && remote_only(hash))
            remote_ahead.push_back(hash);
    if (!remote.empty() || !remote_ahead.empty())
        for (auto &entry : FetchRemote(remote, remote_ahead, context))
            if (std::find(remote.begin(), remote.end(), entry.first) != remote.end())
                loaded.emplace(entry.first, entry.second);
    return loaded;
}
std::vector<std::string> Store::ReadAhead(Reader &reader, Inode &inode, uint64_t offset, uint64_t length,
                                          uint64_t file_length) {
    constexpr uint64_t MAX_WINDOW = (4ull << 20) / CHUNK; // 4 MiB per stream
    constexpr size_t MAX_STREAMS = 16;
    auto first = ChunkIndex(offset), last = ChunkIndex(offset + length - 1);
    uint64_t from = 0, to = 0;
    {
        // A stream starts after two consecutive block-sized reads; only a read that needs data its
        // stream has not requested yet fetches ahead (one round trip per window).
        std::lock_guard<std::mutex> guard(inode.stream_mutex);
        Inode::Stream *stream = nullptr;
        for (auto &candidate : inode.streams)
            if (candidate.next == offset) {
                stream = &candidate;
                break;
            }
        if (!stream) {
            if (inode.streams.size() < MAX_STREAMS)
                inode.streams.emplace_back();
            stream = &*std::min_element(inode.streams.begin(), inode.streams.end(),
                                        [](const Inode::Stream &a, const Inode::Stream &b) { return a.used < b.used; });
            *stream = Inode::Stream();
        }
        stream->reads++;
        stream->next = offset + length;
        stream->used = ++inode.stream_clock;
        if (stream->reads < 2 || last < stream->ahead || file_length == 0)
            return {};
        stream->window = std::min(MAX_WINDOW, stream->window ? stream->window * 2 : last - first + 1);
        from = std::max(last + 1, stream->ahead);
        to = std::min(last + stream->window, ChunkIndex(file_length - 1));
        stream->ahead = to + 1;
    }
    std::vector<std::string> ahead;
    if (from > to)
        return ahead;
    Query q(reader.db, reader.Statement("SELECT chunk,object FROM extents WHERE file=? AND chunk>=? AND chunk<=?"));
    q.Int(1, inode.id).Int(2, int64_t(from)).Int(3, int64_t(to));
    while (q.Step()) {
        auto chunk = uint64_t(q.Int(0));
        if (chunk < inode.floor && !inode.chunks.count(chunk))
            ahead.push_back(q.Text(1));
    }
    return ahead;
}
std::string Store::CommittedObject(Reader &reader, Inode &inode, int64_t version, uint64_t chunk) {
    auto page_no = chunk / PAGE_CHUNKS;
    std::lock_guard<std::mutex> guard(inode.cache_mutex);
    if (inode.cached_version != version) {
        inode.pages.clear();
        inode.cached_version = version;
    }
    auto found = inode.pages.find(page_no);
    if (found == inode.pages.end()) {
        Timer timer(Metric::ExtentLookup);
        auto page = std::make_shared<std::vector<std::string>>(PAGE_CHUNKS);
        Query q(reader.db, reader.Statement("SELECT chunk,object FROM extents WHERE file=? AND chunk>=? AND chunk<?"));
        q.Int(1, inode.id).Int(2, int64_t(page_no * PAGE_CHUNKS)).Int(3, int64_t((page_no + 1) * PAGE_CHUNKS));
        while (q.Step())
            (*page)[uint64_t(q.Int(0)) % PAGE_CHUNKS] = q.Text(1);
        if (inode.pages.size() >= 4096)
            inode.pages.clear();
        found = inode.pages.emplace(page_no, std::move(page)).first;
    }
    return (*found->second)[chunk % PAGE_CHUNKS];
}
SharedBytes Store::CommittedChunk(Reader &reader, Inode &inode, uint64_t chunk) {
    if (chunk >= inode.floor)
        return ZeroChunk();
    Timer timer(Metric::ExtentLookup);
    Query q(reader.db, reader.Statement("SELECT object FROM extents WHERE file=? AND chunk=?"));
    q.Int(1, inode.id).Int(2, int64_t(chunk));
    if (!q.Step())
        return ZeroChunk();
    auto hash = q.Text(0);
    return ObjectBytes(reader, hash, inode.pool);
}
void Store::ReadRanges(Reader &reader, int64_t file, uint64_t p, uint64_t n, uint64_t floor, unsigned char *out) {
    std::memset(out, 0, n);
    auto end = std::min(p + n, floor);
    if (end <= p)
        return;
    std::vector<RangeRow> rows;
    auto row = [](Query &q) {
        return RangeRow{uint64_t(q.Int(0)), uint64_t(q.Int(1)), q.Int(2), uint64_t(q.Int(3)),
                        uint64_t(q.Int(4)), uint32_t(q.Int(5)), uint64_t(q.Int(6))};
    };
    {
        Timer timer(Metric::ExtentLookup);
        Query before(reader.db, reader.Statement("SELECT start,length,segment,record,record_length,crc,skip FROM ranges "
                                                 "WHERE file=? AND start<=? ORDER BY start DESC LIMIT 1"));
        before.Int(1, file).Int(2, int64_t(p));
        if (before.Step())
            rows.push_back(row(before));
        Query inside(reader.db, reader.Statement("SELECT start,length,segment,record,record_length,crc,skip FROM ranges "
                                                 "WHERE file=? AND start>? AND start<? ORDER BY start"));
        inside.Int(1, file).Int(2, int64_t(p)).Int(3, int64_t(end));
        while (inside.Step())
            rows.push_back(row(inside));
    }
    for (auto &range : rows) {
        auto from = std::max(range.start, p), to = std::min(range.start + range.length, end);
        if (from >= to)
            continue;
        // Records are verified whole and cached: log files are read in small sequential steps.
        auto key = "r:" + directory + ":" + std::to_string(range.segment) + ":" + std::to_string(range.record);
        auto bytes = ChunkCache::Instance().Find(key);
        if (!bytes) {
            auto descriptor = segment_files->Open(range.segment);
            auto record = std::make_shared<Bytes>(range.record_length);
            if (!Segment::ReadExact(descriptor->fd, record->data(), record->size(), range.record) ||
                Crc32c(record->data(), record->size()) != range.crc)
                throw std::runtime_error("[differential:corrupt_data] Corrupt log record");
            Count(Counter::LocalMaterializedBytes, record->size());
            bytes = std::move(record);
            ChunkCache::Instance().Insert(key, bytes);
        }
        std::memcpy(out + (from - p), bytes->data() + range.skip + (from - range.start), to - from);
    }
}
bool Store::Exists(const std::string &name) {
    bool found = false;
    WithReader([&](Reader &reader) {
        Query q(reader.db, reader.Statement("SELECT 1 FROM files WHERE name=?"));
        q.Text(1, name);
        found = q.Step();
    });
    return found;
}
int64_t Store::FileId(const std::string &name) {
    int64_t id = -1;
    WithReader([&](Reader &reader) {
        Query q(reader.db, reader.Statement("SELECT id FROM files WHERE name=?"));
        q.Text(1, name);
        if (q.Step())
            id = q.Int(0);
    });
    if (id < 0)
        throw std::runtime_error("[differential:not_found] File not found");
    return id;
}
uint64_t Store::Size(int64_t file) {
    auto inode = GetInode(file);
    std::shared_lock<std::shared_mutex> lock(inode->lock);
    if (inode->dirty)
        return inode->size;
    uint64_t size = 0;
    WithReader([&](Reader &reader) {
        Query q(reader.db, reader.Statement("SELECT size FROM files WHERE id=?"));
        q.Int(1, file);
        if (!q.Step())
            throw std::runtime_error("Invalid file handle");
        size = uint64_t(q.Int(0));
    });
    return size;
}
uint64_t Store::ReadImpl(Reader &reader, Inode &inode, unsigned char *out, uint64_t n, uint64_t p,
                         const void *remote) {
    // One read snapshot spans the committed length and every map/object lookup; the overlay is
    // stable while the shared inode lock is held.
    uint64_t length;
    int64_t version;
    {
        Query q(reader.db, reader.Statement("SELECT size,version FROM files WHERE id=?"));
        q.Int(1, inode.id);
        if (!q.Step())
            throw std::runtime_error("[differential:not_found] File/root not found");
        length = inode.dirty ? inode.size : uint64_t(q.Int(0));
        version = q.Int(1);
    }
    uint64_t floor = inode.dirty ? inode.floor : NONE;
    if (n == 0 || p >= length)
        return 0;
    n = std::min(n, length - p);
    if (inode.log) {
        ReadRanges(reader, inode.id, p, n, floor, out);
        auto it = inode.pieces.upper_bound(p);
        if (it != inode.pieces.begin())
            --it;
        for (; it != inode.pieces.end() && it->first < p + n; ++it) {
            auto &piece = it->second;
            auto from = std::max(it->first, p), to = std::min(it->first + piece.length, p + n);
            if (from < to)
                std::memcpy(out + (from - p), piece.data->data() + piece.skip + (from - it->first), to - from);
        }
        return n;
    }
    auto first = ChunkIndex(p), last = ChunkIndex(p + n - 1);
    std::map<uint64_t, std::string> committed;
    for (auto index = first; index <= last; ++index)
        if (index < floor && !inode.chunks.count(index)) {
            auto hash = CommittedObject(reader, inode, version, index);
            if (!hash.empty())
                committed.emplace(index, std::move(hash));
        }
    // With remote-only objects in the store, the range's objects are loaded together, so the
    // remote ones are fetched in one batch (coalesced range requests).
    std::map<std::string, SharedBytes> fetched;
    if (inode.pool.empty() && remote_objects) {
        std::vector<std::string> wanted;
        for (auto &entry : committed)
            wanted.push_back(entry.second);
        std::vector<std::string> ahead;
        if (remote && n >= CHUNK)
            ahead = ReadAhead(reader, inode, p, n, length);
        fetched = LoadObjects(reader, wanted, ahead, remote);
    }
    uint64_t done = 0;
    while (done < n) {
        auto pos = p + done, index = ChunkIndex(pos);
        auto take = std::min(n - done, ChunkLength(index) - ChunkPosition(pos));
        auto dirty = inode.chunks.find(index);
        auto found = committed.find(index);
        if (dirty != inode.chunks.end())
            std::memcpy(out + done, dirty->second.bytes->data() + ChunkPosition(pos), take);
        else if (found == committed.end())
            std::memset(out + done, 0, take);
        else
            CopyObject(reader, found->second, inode.pool, index, ChunkPosition(pos), take, out + done, fetched);
        done += take;
    }
    return n;
}
uint64_t Store::Read(int64_t file, void *data, uint64_t n, uint64_t p, const void *remote) {
    Timer timer(Metric::LocalRead);
    CheckRange(n, p);
    Count(Counter::LogicalLocalReadBytes, n);
    auto inode = GetInode(file);
    std::shared_lock<std::shared_mutex> lock(inode->lock);
    std::unique_ptr<PayloadFence> fence;
    if (!inode->pool.empty())
        fence = std::make_unique<PayloadFence>(directory, false);
    uint64_t result = 0;
    WithReader([&](Reader &reader) {
        result = ReadImpl(reader, *inode, static_cast<unsigned char *>(data), n, p, remote);
    });
    return result;
}

// ---------------------------------------------------------------------------------------
// Buffered mutation
uint64_t Store::AppendRecord(Group &group, uint32_t type, int64_t file, uint64_t offset, const unsigned char *data,
                             uint64_t length, uint32_t crc) {
    std::lock_guard<std::mutex> guard(group.segment_mutex);
    if (!group.segment)
        group.segment = Segment::Create((std::filesystem::path(directory) / "journal").string(), permissions);
    return group.segment->Append(SegmentRecord::Type(type), file, offset, data, length, crc);
}
static int64_t SegmentOf(std::mutex &mutex, const std::unique_ptr<Segment> &segment) {
    std::lock_guard<std::mutex> guard(mutex);
    return segment ? segment->id : 0;
}
void Store::ApplyLogWrite(Inode &inode, const unsigned char *data, uint64_t n, uint64_t p, int64_t segment,
                          uint64_t position, uint32_t crc) {
    // Buffered pieces never overlap: the new one replaces whatever it covers.
    auto end = p + n;
    auto it = inode.pieces.lower_bound(p);
    if (it != inode.pieces.begin()) {
        auto previous = std::prev(it);
        auto previous_end = previous->first + previous->second.length;
        if (previous_end > p) {
            if (previous_end > end) {
                auto tail = previous->second;
                tail.skip += end - previous->first;
                tail.length = previous_end - end;
                inode.pieces.emplace(end, tail);
            }
            previous->second.length = p - previous->first;
        }
    }
    it = inode.pieces.lower_bound(p);
    while (it != inode.pieces.end() && it->first < end) {
        auto piece_end = it->first + it->second.length;
        if (piece_end > end) {
            auto tail = it->second;
            tail.skip += end - it->first;
            tail.length = piece_end - end;
            inode.pieces.erase(it);
            inode.pieces.emplace(end, tail);
            break;
        }
        it = inode.pieces.erase(it);
    }
    Inode::Piece piece;
    piece.data = std::make_shared<const Bytes>(data, data + n);
    piece.length = n;
    piece.segment = segment;
    piece.record = position;
    piece.crc = crc;
    inode.pieces.emplace(p, std::move(piece));
}
void Store::ApplyWrite(Inode &inode, const unsigned char *in, uint64_t n, uint64_t p, bool journaled) {
    if (inode.log) {
        WithReader([&](Reader &reader) { Load(reader, inode); });
        for (uint64_t done = 0; done < n; done += RECORD_BYTES) {
            auto part = std::min(RECORD_BYTES, n - done);
            int64_t segment = 0;
            uint64_t position = 0;
            uint32_t crc = 0;
            if (journaled) {
                crc = Crc32c(in + done, part);
                position = AppendRecord(*inode.group, SegmentRecord::WRITE, inode.id, p + done, in + done, part, crc);
                segment = SegmentOf(inode.group->segment_mutex, inode.group->segment);
            }
            ApplyLogWrite(inode, in + done, part, p + done, segment, position, crc);
        }
        inode.group->dirty_bytes += n;
        dirty_total += n;
        inode.dirty = true;
        inode.size = std::max(inode.size, p + n);
        return;
    }
    auto first = ChunkIndex(p), last = ChunkIndex(p + n - 1);
    std::map<uint64_t, std::shared_ptr<Bytes>> fresh;
    std::unique_ptr<PayloadFence> fence;
    if (!inode.pool.empty())
        fence = std::make_unique<PayloadFence>(directory, false);
    WithReader([&](Reader &reader) {
        fresh.clear();
        Load(reader, inode);
        for (auto index = first; index <= last; ++index) {
            if (inode.chunks.count(index))
                continue;
            auto offset = ChunkOffset(index);
            bool covered = p <= offset && p + n >= offset + ChunkLength(index);
            fresh.emplace(index, covered ? std::make_shared<Bytes>(CHUNK, 0)
                                         : std::make_shared<Bytes>(*CommittedChunk(reader, inode, index)));
        }
    });
    // Records of at most RECORD_BYTES; a chunk a single record covers whole is stored by that record.
    std::vector<std::pair<uint64_t, uint64_t>> parts; // file offset of each part, payload position
    int64_t segment = 0;
    if (journaled) {
        for (uint64_t done = 0; done < n; done += RECORD_BYTES) {
            auto part = std::min(RECORD_BYTES, n - done);
            parts.emplace_back(p + done, AppendRecord(*inode.group, SegmentRecord::WRITE, inode.id, p + done, in + done,
                                                      part, Crc32c(in + done, part)));
        }
        segment = SegmentOf(inode.group->segment_mutex, inode.group->segment);
    }
    inode.group->dirty_bytes += fresh.size() * CHUNK;
    dirty_total += fresh.size() * CHUNK;
    for (auto &entry : fresh)
        inode.chunks.emplace(entry.first, Inode::Chunk {std::move(entry.second)});
    uint64_t done = 0;
    while (done < n) {
        auto pos = p + done, index = ChunkIndex(pos);
        auto take = std::min(n - done, ChunkLength(index) - ChunkPosition(pos));
        auto &chunk = inode.chunks.at(index);
        std::memcpy(chunk.bytes->data() + ChunkPosition(pos), in + done, take);
        chunk.segment = 0;
        if (segment && index > 0 && take == CHUNK) {
            auto part = std::upper_bound(parts.begin(), parts.end(), std::make_pair(pos, NONE)) - 1;
            if (pos + CHUNK <= std::min(part->first + RECORD_BYTES, p + n)) {
                chunk.segment = segment;
                chunk.position = part->second + (pos - part->first);
            }
        }
        done += take;
    }
    if (!inode.dirty)
        inode.dirty = true;
    inode.size = std::max(inode.size, p + n);
}
void Store::ApplyTruncate(Inode &inode, uint64_t n, bool journaled) {
    if (inode.log) {
        WithReader([&](Reader &reader) { Load(reader, inode); });
        if (journaled)
            AppendRecord(*inode.group, SegmentRecord::TRUNCATE, inode.id, n, nullptr, 0, Crc32c(nullptr, 0));
        if (n < inode.size) {
            auto it = inode.pieces.lower_bound(n);
            inode.pieces.erase(it, inode.pieces.end());
            if (!inode.pieces.empty()) {
                auto &last = *inode.pieces.rbegin();
                if (last.first + last.second.length > n)
                    last.second.length = n - last.first;
            }
            inode.floor = std::min(inode.floor, n);
        }
        inode.size = n;
        inode.dirty = true;
        return;
    }
    std::shared_ptr<Bytes> tail;
    std::unique_ptr<PayloadFence> fence;
    if (!inode.pool.empty())
        fence = std::make_unique<PayloadFence>(directory, false);
    WithReader([&](Reader &reader) {
        Load(reader, inode);
        if (n < inode.size && ChunkPosition(n) && !inode.chunks.count(ChunkIndex(n)))
            tail = std::make_shared<Bytes>(*CommittedChunk(reader, inode, ChunkIndex(n)));
    });
    if (journaled)
        AppendRecord(*inode.group, SegmentRecord::TRUNCATE, inode.id, n, nullptr, 0, Crc32c(nullptr, 0));
    if (n < inode.size) {
        auto keep = ChunkCount(n); // chunks at or above are discarded
        for (auto it = inode.chunks.lower_bound(keep); it != inode.chunks.end();) {
            inode.group->dirty_bytes -= CHUNK;
            dirty_total -= CHUNK;
            it = inode.chunks.erase(it);
        }
        if (ChunkPosition(n)) {
            if (tail) {
                inode.chunks.emplace(ChunkIndex(n), Inode::Chunk {tail});
                inode.group->dirty_bytes += CHUNK;
                dirty_total += CHUNK;
            }
            auto &chunk = inode.chunks.at(ChunkIndex(n));
            std::fill(chunk.bytes->begin() + int64_t(ChunkPosition(n)), chunk.bytes->end(), 0);
            chunk.segment = 0;
        }
        inode.floor = std::min(inode.floor, keep);
    }
    inode.size = n;
    inode.dirty = true;
}
void Store::Write(int64_t file, const void *data, uint64_t n, uint64_t p) {
    Timer timer(Metric::LocalWrite);
    CheckRange(n, p);
    if (n == 0)
        return;
    auto inode = GetInode(file);
    auto group = inode->group;
    {
        std::shared_lock<std::shared_mutex> gate(group->gate);
        std::unique_lock<std::shared_mutex> lock(inode->lock);
        ApplyWrite(*inode, static_cast<const unsigned char *>(data), n, p, true);
    }
    auto dirty = group->dirty_bytes.load();
    if (dirty > DIRTY_LIMIT) {
        // Backpressure: the background publication did not keep up.
        std::vector<std::shared_ptr<Group>> scope {group};
        FlushImpl(false, {}, &scope);
    } else if (dirty_total.load() > TOTAL_DIRTY_LIMIT)
        FlushImpl(false, {}, nullptr);
    else if (dirty > BACKGROUND_LIMIT)
        SchedulePublication(group);
}
void Store::SchedulePublication(const std::shared_ptr<Group> &group) {
    if (group->scheduled.exchange(true))
        return;
    std::lock_guard<std::mutex> guard(publisher_mutex);
    if (publisher_stopping) {
        group->scheduled = false;
        return;
    }
    publisher_queue.push_back(group);
    if (!publisher.joinable())
        publisher = std::thread([this] {
            std::unique_lock<std::mutex> lock(publisher_mutex);
            while (true) {
                publisher_signal.wait(lock, [this] { return !publisher_queue.empty() || publisher_stopping; });
                if (publisher_stopping)
                    break;
                auto next = publisher_queue.front();
                publisher_queue.erase(publisher_queue.begin());
                lock.unlock();
                next->scheduled = false;
                try {
                    std::vector<std::shared_ptr<Group>> scope {next};
                    FlushImpl(false, {}, &scope);
                    Count(Counter::LocalBackgroundPublications);
                } catch (...) {
                    // The writes stay buffered and segment-backed; the next sync or pressure publishes them.
                }
                lock.lock();
            }
            for (auto &pending : publisher_queue)
                pending->scheduled = false;
            publisher_queue.clear();
        });
    publisher_signal.notify_one();
}
void Store::Truncate(int64_t file, uint64_t n) {
    CheckRange(0, n);
    auto inode = GetInode(file);
    std::shared_lock<std::shared_mutex> gate(inode->group->gate);
    std::unique_lock<std::shared_mutex> lock(inode->lock);
    ApplyTruncate(*inode, n, true);
}
void Store::Sync() {
    Timer timer(Metric::LocalSync);
    FlushImpl(true, {}, nullptr);
}
void Store::Sync(int64_t file) {
    Timer timer(Metric::LocalSync);
    std::vector<std::shared_ptr<Group>> scope {GetInode(file)->group};
    if (GroupCommit())
        CommitShared(scope);
    else {
        Count(Counter::LocalSyncRequests);
        Count(Counter::LocalSyncTransactions);
        FlushImpl(true, {}, &scope);
    }
}
void Store::Flush() { FlushImpl(false, {}, nullptr); }
void Store::Flush(int64_t file) {
    std::vector<std::shared_ptr<Group>> scope {GetInode(file)->group};
    FlushImpl(false, {}, &scope);
}
// A publication of some groups, prepared outside the authority connection.
struct Store::Prepared {
    std::vector<std::shared_ptr<Group>> groups;
    std::vector<std::unique_lock<std::shared_mutex>> gates; // writes excluded until destroyed
    std::vector<std::shared_ptr<Inode>> dirty;
    struct PendingChunk {
        uint64_t index;
        std::string hash;
        uint32_t crc;
        int64_t segment; // 0: an existing local copy (or an external pool)
        uint64_t position;
    };
    struct Pending {
        Inode *inode;
        std::vector<PendingChunk> chunks;
    };
    std::vector<Pending> pending;
    std::unique_ptr<PayloadFence> fence; // shared until the mappings commit
    uint64_t chunk_bytes = 0;            // published payload bytes (the group commit size cap)
    std::vector<Group *> retire;         // segments to retire once the transaction committed
    std::vector<int64_t> unreferenced;   // ... of which nothing references these: deleted at once
};
std::unique_ptr<Store::Prepared> Store::Prepare(const std::vector<std::shared_ptr<Group>> *scope) {
    auto prepared = std::make_unique<Prepared>();
    auto &selected = prepared->groups;
    if (scope)
        selected = *scope;
    else {
        std::lock_guard<std::mutex> guard(groups_mutex);
        for (auto &entry : groups)
            selected.push_back(entry.second);
    }
    // One order for every publication, so overlapping scopes cannot deadlock.
    std::sort(selected.begin(), selected.end(),
              [](const std::shared_ptr<Group> &a, const std::shared_ptr<Group> &b) { return a->key < b->key; });
    selected.erase(std::unique(selected.begin(), selected.end()), selected.end());
    for (auto &group : selected)
        prepared->gates.emplace_back(group->gate);
    std::set<const Group *> members;
    for (auto &group : selected)
        members.insert(group.get());
    {
        std::lock_guard<std::mutex> guard(inodes_mutex);
        for (auto &entry : inodes)
            if (entry.second->dirty && members.count(entry.second->group.get()))
                prepared->dirty.push_back(entry.second);
    }
    if (prepared->dirty.empty())
        return prepared;
    // Hash outside every lock that other threads need; the gates keep the overlays stable.
    std::map<std::string, std::vector<std::pair<std::string, const Bytes *>>> external;
    std::map<std::string, std::pair<std::string, std::string>> pool_locations;
    auto stored_locally = [&](const std::string &hash) {
        Location location;
        if (segment_files->FindLocation(hash, location))
            return true;
        bool found = false;
        WithReader([&](Reader &reader) {
            Query q(reader.db, reader.Statement("SELECT segment IS NOT NULL FROM objects WHERE hash=?"));
            q.Text(1, hash);
            found = q.Step() && q.Int(0) != 0;
        });
        return found;
    };
    for (auto &inode : prepared->dirty) {
        Prepared::Pending item {inode.get(), {}};
        auto &group = *inode->group;
        if (inode->log) {
            // Replayed writes have no record of their own yet.
            std::unique_lock<std::shared_mutex> lock(inode->lock);
            for (auto &entry : inode->pieces) {
                auto &piece = entry.second;
                prepared->chunk_bytes += piece.length;
                if (piece.segment)
                    continue;
                piece.crc = Crc32c(piece.data->data(), piece.data->size());
                piece.record = AppendRecord(group, SegmentRecord::OBJECT, inode->id, entry.first, piece.data->data(),
                                            piece.data->size(), piece.crc);
                piece.segment = SegmentOf(group.segment_mutex, group.segment);
            }
        } else {
            for (auto &entry : inode->chunks) {
                auto &chunk = entry.second;
                auto &bytes = *chunk.bytes;
                Prepared::PendingChunk pending {entry.first, Hash(bytes.data(), bytes.size()),
                                                Crc32c(bytes.data(), bytes.size()), 0, 0};
                prepared->chunk_bytes += CHUNK;
                if (!inode->pool.empty()) {
                    external[inode->pool].emplace_back(pending.hash, chunk.bytes.get());
                    pool_locations[inode->pool] = {inode->pool_path, inode->pool_repository};
                } else if (chunk.segment) {
                    pending.segment = chunk.segment; // the write's own record
                    pending.position = chunk.position;
                } else if (!stored_locally(pending.hash)) {
                    pending.position = AppendRecord(group, SegmentRecord::OBJECT, inode->id, entry.first,
                                                    bytes.data(), bytes.size(), pending.crc);
                    pending.segment = SegmentOf(group.segment_mutex, group.segment);
                }
                item.chunks.push_back(std::move(pending));
            }
        }
        prepared->pending.push_back(std::move(item));
    }
    // Payload before the metadata that references it (also for NORMAL commits: a later FULL commit
    // or a SQLite checkpoint can make the metadata durable at any time).
    for (auto &group : selected) {
        std::lock_guard<std::mutex> guard(group->segment_mutex);
        if (group->segment) {
            Timer timer(Metric::LocalSegmentSync);
            group->segment->Sync();
        }
    }
    if (!external.empty()) {
        // Shared until the mapping commits: collection cannot delete a just-published file.
        prepared->fence = std::make_unique<PayloadFence>(directory, false);
        for (auto &pool : external)
            LocalPool(pool_locations[pool.first].first, pool_locations[pool.first].second, pool.first, permissions)
                .PutBatch(pool.second);
    }
    return prepared;
}
void Store::RecordSegmentTx(Group &group, bool &retire) {
    std::lock_guard<std::mutex> guard(group.segment_mutex);
    retire = false;
    if (!group.segment)
        return;
    // Published up to its end: a replay starts after it. A full segment is retired, and so is one that
    // holds only published journal records (nothing references it).
    retire = group.segment->End() >= SEGMENT_LIMIT ||
             (!group.segment->referenced && group.segment->End() >= UNREFERENCED_SEGMENT_LIMIT);
    Query(db, "INSERT INTO segments VALUES(?,?,?) ON CONFLICT(id) DO UPDATE SET committed=excluded.committed,"
              "active=excluded.active")
        .Int(1, group.segment->id)
        .Int(2, int64_t(group.segment->End()))
        .Int(3, retire ? 0 : 1)
        .Run();
}
void Store::MarkReferenced(Group &group, int64_t id) {
    std::lock_guard<std::mutex> guard(group.segment_mutex);
    if (group.segment && group.segment->id == id)
        group.segment->referenced = true;
}
void Store::CutRangesTx(int64_t file, uint64_t start, uint64_t end) {
    // Ranges are disjoint: trim the one that starts before `start`, then those that start inside.
    std::vector<RangeRow> rows;
    auto row = [](Query &q) {
        return RangeRow{uint64_t(q.Int(0)), uint64_t(q.Int(1)), q.Int(2), uint64_t(q.Int(3)),
                        uint64_t(q.Int(4)), uint32_t(q.Int(5)), uint64_t(q.Int(6))};
    };
    {
        Query before(db, "SELECT start,length,segment,record,record_length,crc,skip FROM ranges WHERE file=? AND "
                         "start<? ORDER BY start DESC LIMIT 1");
        before.Int(1, file).Int(2, int64_t(start));
        if (before.Step())
            rows.push_back(row(before));
        Query inside(db, "SELECT start,length,segment,record,record_length,crc,skip FROM ranges WHERE file=? AND "
                         "start>=? AND start<?");
        inside.Int(1, file).Int(2, int64_t(start)).Int(3, int64_t(end));
        while (inside.Step())
            rows.push_back(row(inside));
    }
    for (auto &range : rows) {
        auto range_end = range.start + range.length;
        if (range_end <= start)
            continue;
        if (range.start < start)
            Query(db, "UPDATE ranges SET length=? WHERE file=? AND start=?")
                .Int(1, int64_t(start - range.start))
                .Int(2, file)
                .Int(3, int64_t(range.start))
                .Run();
        else
            Query(db, "DELETE FROM ranges WHERE file=? AND start=?").Int(1, file).Int(2, int64_t(range.start)).Run();
        if (range_end > end)
            Query(db, "INSERT INTO ranges VALUES(?,?,?,?,?,?,?,?)")
                .Int(1, file)
                .Int(2, int64_t(end))
                .Int(3, int64_t(range_end - end))
                .Int(4, range.segment)
                .Int(5, int64_t(range.record))
                .Int(6, int64_t(range.record_length))
                .Int(7, int64_t(range.crc))
                .Int(8, int64_t(range.skip + (end - range.start)))
                .Run();
    }
}
void Store::Apply(Prepared &prepared, std::set<std::string> &replaced) {
    uint64_t ship_sequence = 0; // allocated by the first tracked change of this transaction
    prepared.retire.clear();
    prepared.unreferenced.clear();
    bool retire = false;
    for (auto &group : prepared.groups)
        RecordSegmentTx(*group, retire); // rows before the references to them
    bool appended = false;
    for (auto &item : prepared.pending) {
        auto &inode = *item.inode;
        if (auto fail = Hook("DIFFERENTIAL_TEST_FAIL_APPLY_FILE"))
            if (std::to_string(inode.id) == fail)
                throw std::runtime_error("[differential:local_io] Injected publication failure");
        if (inode.log) {
            if (inode.floor != NONE) {
                CutRangesTx(inode.id, inode.floor, NONE >> 1);
                Query(db, "DELETE FROM ranges WHERE file=? AND start>=?").Int(1, inode.id).Int(2, int64_t(inode.floor)).Run();
            }
            for (auto &entry : inode.pieces) {
                auto &piece = entry.second;
                if (!piece.length)
                    continue;
                CutRangesTx(inode.id, entry.first, entry.first + piece.length);
                MarkReferenced(*inode.group, piece.segment);
                Query(db, "INSERT INTO ranges VALUES(?,?,?,?,?,?,?,?)")
                    .Int(1, inode.id)
                    .Int(2, int64_t(entry.first))
                    .Int(3, int64_t(piece.length))
                    .Int(4, piece.segment)
                    .Int(5, int64_t(piece.record))
                    .Int(6, int64_t(piece.data->size()))
                    .Int(7, int64_t(piece.crc))
                    .Int(8, int64_t(piece.skip))
                    .Run();
            }
            Crash("mapping_updated");
        } else {
            if (inode.floor != NONE) {
                Query old(db, "SELECT object FROM extents WHERE file=? AND chunk>=?");
                old.Int(1, inode.id).Int(2, int64_t(inode.floor));
                while (old.Step())
                    replaced.insert(old.Text(0));
                Query(db, "DELETE FROM extents WHERE file=? AND chunk>=?")
                    .Int(1, inode.id)
                    .Int(2, int64_t(inode.floor))
                    .Run();
                Query(db, "UPDATE file_pages SET dirty=1 WHERE file=? AND page_no>=?")
                    .Int(1, inode.id)
                    .Int(2, int64_t(inode.floor / PAGE_CHUNKS))
                    .Run();
            }
            for (auto &chunk : item.chunks) {
                auto &hash = chunk.hash;
                if (inode.pool.empty()) {
                    Query present(db, "SELECT segment IS NOT NULL FROM objects WHERE hash=?");
                    present.Text(1, hash);
                    if (!present.Step() || present.Int(0) == 0) {
                        if (!chunk.segment) {
                            // The local copy seen while preparing was collected since: store the bytes now.
                            auto &bytes = *inode.chunks.at(chunk.index).bytes;
                            chunk.position = AppendRecord(*inode.group, SegmentRecord::OBJECT, inode.id, chunk.index,
                                                          bytes.data(), bytes.size(), chunk.crc);
                            chunk.segment = SegmentOf(inode.group->segment_mutex, inode.group->segment);
                            appended = true;
                        }
                        MarkReferenced(*inode.group, chunk.segment);
                        Query(db, "INSERT INTO objects(hash,segment,position,crc) VALUES(?,?,?,?) ON CONFLICT(hash) DO "
                                  "UPDATE SET segment=excluded.segment,position=excluded.position,crc=excluded.crc")
                            .Text(1, hash)
                            .Int(2, chunk.segment)
                            .Int(3, int64_t(chunk.position))
                            .Int(4, int64_t(chunk.crc))
                            .Run();
                    }
                } else {
                    Query(db, "INSERT OR IGNORE INTO objects(hash) VALUES(?)").Text(1, hash).Run();
                    Query(db, "INSERT OR IGNORE INTO local_objects VALUES(?,?)").Text(1, hash).Text(2, inode.pool).Run();
                    Query(db, "DELETE FROM local_deletions WHERE pool=? AND hash=?").Text(1, inode.pool).Text(2, hash).Run();
                }
                Crash("object_inserted");
                Query previous(db, "SELECT object FROM extents WHERE file=? AND chunk=?");
                previous.Int(1, inode.id).Int(2, int64_t(chunk.index));
                if (previous.Step() && previous.Text(0) != hash)
                    replaced.insert(previous.Text(0));
                Query(db, "INSERT INTO extents(file,chunk,object) VALUES(?,?,?) ON CONFLICT(file,chunk) DO "
                          "UPDATE SET object=excluded.object")
                    .Int(1, inode.id)
                    .Int(2, int64_t(chunk.index))
                    .Text(3, hash)
                    .Run();
                Query(db, "INSERT INTO file_pages(file,page_no,page,dirty) VALUES(?,?,NULL,1) "
                          "ON CONFLICT(file,page_no) DO UPDATE SET dirty=1")
                    .Int(1, inode.id)
                    .Int(2, int64_t(chunk.index / PAGE_CHUNKS))
                    .Run();
                Crash("mapping_updated");
            }
        }
        if (Query(db, "UPDATE files SET size=?,version=version+1 WHERE id=?")
                .Int(1, int64_t(inode.size))
                .Int(2, inode.id)
                .Run() != 1)
            throw std::runtime_error("Invalid file handle");
        if (Shipping(inode.group->key)) {
            std::vector<uint64_t> chunks;
            uint64_t low = NONE >> 1;
            if (inode.log) {
                for (auto &entry : inode.pieces)
                    low = std::min(low, entry.first);
            } else
                for (auto &chunk : item.chunks)
                    chunks.push_back(chunk.index);
            if (inode.floor != NONE)
                low = std::min(low, inode.floor);
            TrackShipTx(inode.id, chunks, low, ship_sequence);
        }
    }
    Crash("length_updated");
    if (appended)
        for (auto &group : prepared.groups) {
            std::lock_guard<std::mutex> guard(group->segment_mutex);
            if (group->segment)
                group->segment->Sync();
        }
    for (auto &group : prepared.groups) {
        RecordSegmentTx(*group, retire);
        if (retire) {
            prepared.retire.push_back(group.get());
            std::lock_guard<std::mutex> guard(group->segment_mutex);
            if (!group->segment->referenced)
                prepared.unreferenced.push_back(group->segment->id);
        }
    }
}
void Store::Reclaim(const std::set<std::string> &replaced) {
    // Overwritten live chunks that nothing else references are reclaimed at once, so repeated small
    // commits do not accumulate garbage awaiting collection.
    for (auto &hash : replaced) {
        Query used(db, "SELECT EXISTS(SELECT 1 FROM extents WHERE object=?1) OR EXISTS(SELECT 1 FROM "
                       "page_entries WHERE object=?1)");
        used.Text(1, hash);
        if (used.Step() && used.Int(0) == 0)
            Query(db, "DELETE FROM objects WHERE hash=?").Text(1, hash).Run();
    }
}
void Store::Finish(Prepared &prepared) {
    for (auto &inode : prepared.dirty) {
        std::unique_lock<std::shared_mutex> lock(inode->lock);
        inode->chunks.clear();
        inode->pieces.clear();
        inode->dirty = false;
        inode->floor = NONE;
    }
    for (auto &group : prepared.groups)
        dirty_total -= group->dirty_bytes.exchange(0);
    {
        // Each WAL incarnation is a new inode: forget clean inodes nothing else references
        // (their state reloads from the authority on next use).
        std::lock_guard<std::mutex> guard(inodes_mutex);
        if (inodes.size() > 256)
            for (auto it = inodes.begin(); it != inodes.end();)
                it = it->second.use_count() == 1 && !it->second->dirty ? inodes.erase(it) : std::next(it);
    }
    auto retired = (std::filesystem::path(directory) / "segments").string();
    for (auto *group : prepared.retire) {
        // Recorded as retired: never appended to again, even if the move fails (a later replay moves it).
        std::lock_guard<std::mutex> guard(group->segment_mutex);
        if (!group->segment)
            continue;
        try {
            group->segment->Retire(retired);
        } catch (...) {
        }
        group->segment.reset();
    }
    prepared.retire.clear();
    for (auto id : prepared.unreferenced) {
        // Best effort: collection deletes whatever remains (DeleteSegment re-checks references).
        try {
            DeleteSegment(id);
        } catch (...) {
        }
    }
    prepared.unreferenced.clear();
}
void Store::FlushImpl(bool durable, const std::function<void()> &in_transaction,
                      const std::vector<std::shared_ptr<Group>> *scope) {
    auto prepared = Prepare(scope);
    if (prepared->dirty.empty() && !in_transaction) {
        if (durable && barrier_needed.exchange(false)) {
            // Earlier NORMAL commits become durable with the next synced WAL commit.
            std::lock_guard<std::recursive_mutex> guard(mutex);
            Transaction tx(db, "DIFFERENTIAL_STORE_CRASH");
            Execute(db, "INSERT INTO allocators VALUES('barrier',1) ON CONFLICT(name) DO UPDATE SET next=next+1");
            tx.Commit();
        }
        return;
    }
    {
        std::lock_guard<std::recursive_mutex> guard(mutex);
        Transaction tx(db, "DIFFERENTIAL_STORE_CRASH", durable);
        std::set<std::string> replaced;
        Apply(*prepared, replaced);
        if (in_transaction)
            in_transaction();
        Reclaim(replaced);
        {
            Timer timer(Metric::LocalMetadataCommit);
            tx.Commit();
        }
        barrier_needed = !durable;
    }
    Finish(*prepared);
}

// ---------------------------------------------------------------------------------------
// Group commit. A durable sync prepares its group in its own thread (holding the group's gate and
// syncing its segment), then queues. The first waiter while no commit runs leads: it commits the
// queued syncs, up to GROUP_COMMIT_BYTES of payload (a larger one alone), in one FULL transaction,
// so concurrent syncs of different databases share one WAL sync. Syncs queued meanwhile form the
// next batch.
namespace {
std::atomic<bool> group_commit {true};
} // namespace
void Store::SetGroupCommit(bool enabled) { group_commit = enabled; }
bool Store::GroupCommit() { return group_commit; }
struct Store::CommitRequest {
    Prepared *prepared;
    bool done = false;
    std::exception_ptr error;
};
void Store::CommitShared(const std::vector<std::shared_ptr<Group>> &scope) {
    auto prepared = Prepare(&scope);
    Count(Counter::LocalSyncRequests);
    if (prepared->dirty.empty() && !barrier_needed)
        return; // nothing this sync has to make durable
    CommitRequest request {prepared.get()};
    std::unique_lock<std::mutex> lock(commit_mutex);
    commit_queue.push_back(&request);
    commit_signal.notify_all();
    while (!request.done) {
        if (committing) {
            commit_signal.wait(lock);
            continue;
        }
        if (auto wait = Hook("DIFFERENTIAL_TEST_GROUP_COMMIT_WAIT")) {
            // Test builds: lead only once this many syncs are queued (a deterministic batch).
            commit_signal.wait(lock, [&] { return committing || commit_queue.size() >= std::stoul(wait); });
            if (committing)
                continue;
        }
        committing = true;
        std::vector<CommitRequest *> batch;
        uint64_t bytes = 0;
        auto taken = commit_queue.begin();
        for (; taken != commit_queue.end(); ++taken) {
            auto size = (*taken)->prepared->chunk_bytes;
            if (!batch.empty() && bytes + size > GROUP_COMMIT_BYTES)
                break;
            batch.push_back(*taken);
            bytes += size;
        }
        commit_queue.erase(commit_queue.begin(), taken);
        lock.unlock();
        CommitBatch(batch);
        lock.lock();
        for (auto *finished : batch)
            finished->done = true; // under the lock: waiters read it under the lock
        committing = false;
        commit_signal.notify_all();
    }
    lock.unlock();
    // The gates are released with `prepared`, in this thread.
    if (request.error)
        std::rethrow_exception(request.error);
}
void Store::CommitBatch(const std::vector<CommitRequest *> &batch) {
    auto commit = [&](const std::vector<CommitRequest *> &requests) {
        {
            std::lock_guard<std::recursive_mutex> guard(mutex);
            Transaction tx(db, "DIFFERENTIAL_STORE_CRASH", true);
            std::set<std::string> replaced;
            for (auto *request : requests)
                Apply(*request->prepared, replaced);
            Reclaim(replaced);
            if (requests.size() > 1)
                Crash("group_applied");
            Timer timer(Metric::LocalMetadataCommit);
            tx.Commit();
            barrier_needed = false; // a FULL commit also makes earlier NORMAL commits durable
        }
        Count(Counter::LocalSyncTransactions);
        CountMax(Counter::LocalSyncBatchMax, requests.size());
        for (auto *request : requests)
            Finish(*request->prepared);
    };
    try {
        commit(batch);
    } catch (...) {
        if (batch.size() == 1)
            batch[0]->error = std::current_exception();
        else {
            // Attribute the failure: commit each sync on its own, as without group commit.
            Count(Counter::LocalSyncBatchFallbacks);
            for (auto *request : batch) {
                try {
                    commit({request});
                } catch (...) {
                    request->error = std::current_exception();
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------------------
// Namespace
// The groups a namespace change of `names` orders after: each name's group, and the group of the
// inode currently named (which keeps the group it was opened in after a rename).
std::vector<std::shared_ptr<Store::Group>> Store::GroupsOf(const std::vector<std::string> &names) {
    std::vector<std::shared_ptr<Group>> result;
    for (auto &name : names) {
        result.push_back(GetGroup(GroupKey(name)));
        int64_t id = -1;
        WithReader([&](Reader &reader) {
            Query q(reader.db, reader.Statement("SELECT id FROM files WHERE name=?"));
            q.Text(1, name);
            if (q.Step())
                id = q.Int(0);
        });
        std::lock_guard<std::mutex> guard(inodes_mutex);
        auto found = inodes.find(id);
        if (found != inodes.end())
            result.push_back(found->second->group);
    }
    return result;
}
File Store::Open(const std::string &name, bool create, bool exclusive, bool truncate, const std::string &pin,
                 const std::string &pool) {
    Timer timer(Metric::NamespaceUpdate);
    ReplayJournals();
    File file{};
    auto body = [&] {
        Query q(db, "SELECT id,size FROM files WHERE name=?");
        q.Text(1, name);
        if (q.Step()) {
            if (exclusive)
                throw std::runtime_error("[differential:name_conflict] File already exists");
            file = {q.Int(0), uint64_t(q.Int(1))};
            if (FilePoolTx(file.id) != pool)
                throw std::runtime_error("[differential:incompatible_format] File provider cannot change");
        } else {
            if (!create)
                throw std::runtime_error("[differential:not_found] File not found");
            file = {CreateFileTx(name, 0, pool), 0};
        }
        if (truncate) {
            uint64_t sequence = 0;
            if (Shipping(GroupKey(name)))
                TrackShipTx(file.id, {}, 0, sequence);
            Query(db, "DELETE FROM extents WHERE file=?").Int(1, file.id).Run();
            Query(db, "DELETE FROM ranges WHERE file=?").Int(1, file.id).Run();
            Query(db, "DELETE FROM file_pages WHERE file=?").Int(1, file.id).Run();
            Query(db, "UPDATE files SET size=0,version=version+1 WHERE id=?").Int(1, file.id).Run();
            file.size = 0;
        }
        if (!pin.empty()) {
            if (Query(db, "UPDATE pins SET file=? WHERE id=?").Int(1, file.id).Text(2, pin).Run() != 1)
                throw std::runtime_error("File pin missing");
        }
    };
    if (truncate) {
        // Commit any overlay for this inode first, then replace its contents in one transaction.
        auto scope = GroupsOf({name});
        FlushImpl(true, body, &scope);
        return file;
    }
    std::lock_guard<std::recursive_mutex> guard(mutex);
    // Native file creation is not synced either; the next sync makes it durable.
    Transaction tx(db, "DIFFERENTIAL_STORE_CRASH", false);
    body();
    tx.Commit();
    barrier_needed = true;
    // An open inode with buffered writes reports its overlay length.
    auto inode = GetInode(file.id);
    std::shared_lock<std::shared_mutex> lock(inode->lock);
    if (inode->dirty)
        file.size = inode->size;
    return file;
}
int64_t Store::CreateFileTx(const std::string &name, uint64_t size, const std::string &pool) {
    // Inode numbers are never reused: a dead process's segment can name an unlinked inode.
    Query next(db, "UPDATE allocators SET next=next+1 WHERE name='file' RETURNING next-1");
    if (!next.Step())
        throw std::runtime_error("[differential:corrupt_data] File allocator missing");
    auto id = next.Int(0);
    while (next.Step()) {
    }
    // Sidecars ("name.<suffix>", e.g. WALs) of the default pool are log files.
    bool log = pool.empty() && name.find('.') != std::string::npos;
    auto insert = Query(db, "INSERT INTO files(id,name,size,kind) VALUES(?,?,?,?)");
    insert.Int(1, id).Int(3, int64_t(size)).Int(4, log ? 1 : 0);
    if (name.empty())
        insert.Null(2);
    else
        insert.Text(2, name);
    insert.Run();
    if (!pool.empty())
        Query(db, "INSERT INTO file_pools VALUES(?,?)").Int(1, id).Text(2, pool).Run();
    return id;
}
std::string Store::FilePoolTx(int64_t file) {
    Query q(db, "SELECT pool FROM file_pools WHERE file=?");
    q.Int(1, file);
    return q.Step() ? q.Text(0) : "";
}
bool Store::Remove(const std::string &name) {
    Timer timer(Metric::NamespaceUpdate);
    bool changed = false;
    // Native unlink is not synced by DuckDB; publish it without a sync, ordered after
    // earlier buffered writes. An unlinked inode remains addressable through open handles.
    auto scope = GroupsOf({name});
    FlushImpl(
        false,
        [&] {
            changed = Query(db, "UPDATE files SET name=NULL WHERE name=?").Text(1, name).Run() != 0;
            Crash("namespace_updated");
        },
        &scope);
    return changed;
}
void Store::Rename(const std::string &source, const std::string &target) {
    if (source == target)
        return;
    Timer timer(Metric::NamespaceUpdate);
    auto scope = GroupsOf({source, target});
    FlushImpl(false, [&] {
        Query find(db, "SELECT id FROM files WHERE name=?");
        find.Text(1, source);
        if (!find.Step())
            throw std::runtime_error("[differential:not_found] Rename source missing");
        auto id = find.Int(0);
        Query(db, "UPDATE files SET name=NULL WHERE name=?").Text(1, target).Run();
        Query(db, "UPDATE files SET name=? WHERE id=?").Text(1, target).Int(2, id).Run();
        Crash("namespace_updated");
    }, &scope);
}

// ---------------------------------------------------------------------------------------
// Paged manifests
std::string Store::PageHashTx(int64_t file, uint64_t page_no) {
    auto base = int64_t(page_no * PAGE_CHUNKS);
    Digest digest;
    digest.Add(std::string("differential-page-v1\n"));
    bool any = false;
    {
        Query q(db, "SELECT chunk,object FROM extents WHERE file=? AND chunk>=? AND chunk<? ORDER BY chunk");
        q.Int(1, file).Int(2, base).Int(3, base + int64_t(PAGE_CHUNKS));
        while (q.Step()) {
            any = true;
            digest.Add(std::to_string(q.Int(0) - base) + ":" + q.Text(1) + "\n");
        }
    }
    if (!any)
        return "";
    auto hash = digest.Finish();
    if (Query(db, "INSERT OR IGNORE INTO manifest_pages VALUES(?)").Text(1, hash).Run() == 1)
        Query(db, "INSERT INTO page_entries SELECT ?,chunk-?,object FROM extents WHERE file=? AND chunk>=? AND "
                  "chunk<?")
            .Text(1, hash)
            .Int(2, base)
            .Int(3, file)
            .Int(4, base)
            .Int(5, base + int64_t(PAGE_CHUNKS))
            .Run();
    return hash;
}
std::string Store::RootHashTx(int64_t file, uint64_t size) {
    Digest digest;
    digest.Add(RootHeader(size));
    Query pages(db, "SELECT page_no,page FROM file_pages WHERE file=? ORDER BY page_no");
    pages.Int(1, file);
    while (pages.Step())
        digest.Add(std::to_string(pages.Int(0)) + ":" + pages.Text(1) + "\n");
    return digest.Finish();
}
Root Store::SealTx(int64_t file, const std::string &pin, bool generation) {
    Query size_query(db, "SELECT size,kind FROM files WHERE id=?");
    size_query.Int(1, file);
    if (!size_query.Step())
        throw std::runtime_error("Invalid file handle");
    if (size_query.Int(1) != 0)
        throw std::runtime_error("[differential:invalid_input] Log files have no immutable generations");
    auto size = uint64_t(size_query.Int(0));
    // Only pages whose chunk map changed since the last seal are re-hashed.
    std::vector<int64_t> dirty;
    {
        Query q(db, "SELECT page_no FROM file_pages WHERE file=? AND dirty=1 ORDER BY page_no");
        q.Int(1, file);
        while (q.Step())
            dirty.push_back(q.Int(0));
    }
    for (auto page_no : dirty) {
        auto hash = PageHashTx(file, uint64_t(page_no));
        if (hash.empty())
            Query(db, "DELETE FROM file_pages WHERE file=? AND page_no=?").Int(1, file).Int(2, page_no).Run();
        else
            Query(db, "UPDATE file_pages SET page=?,dirty=0 WHERE file=? AND page_no=?")
                .Text(1, hash)
                .Int(2, file)
                .Int(3, page_no)
                .Run();
    }
    auto root = RootHashTx(file, size);
    Query(db, "INSERT OR IGNORE INTO roots(hash,size) VALUES(?,?)").Text(1, root).Int(2, int64_t(size)).Run();
    Query reclaimed(db, "SELECT 1 FROM reclaimed_roots WHERE root=?");
    reclaimed.Text(1, root);
    bool revive = reclaimed.Step();
    Query existing(db, "SELECT count(*) FROM root_pages WHERE root=?");
    existing.Text(1, root);
    existing.Step();
    if (revive || existing.Int(0) == 0)
        Query(db, "INSERT OR IGNORE INTO root_pages SELECT ?,page_no,page FROM file_pages WHERE file=?")
            .Text(1, root)
            .Int(2, file)
            .Run();
    Query(db, "DELETE FROM reclaimed_roots WHERE root=?").Text(1, root).Run();
    if (generation)
        Query(db, "INSERT INTO generations(file,root) VALUES(?,?) ON CONFLICT(file) DO UPDATE SET root=excluded.root")
            .Int(1, file)
            .Text(2, root)
            .Run();
    if (!pin.empty() && Query(db, "UPDATE pins SET root=? WHERE id=?").Text(1, root).Text(2, pin).Run() != 1)
        throw std::runtime_error("Root pin missing");
    Crash("manifest_inserted");
    return {root, size};
}
Root Store::Seal(int64_t file, const std::string &pin, bool durable) {
    Timer timer(Metric::ManifestSeal);
    Root result;
    std::vector<std::shared_ptr<Group>> scope {GetInode(file)->group};
    FlushImpl(durable, [&] { result = SealTx(file, pin); }, &scope);
    return result;
}
Root Store::Capture(int64_t file, const std::string &pin, uint64_t &sequence) {
    Timer timer(Metric::ManifestSeal);
    Root result;
    std::vector<std::shared_ptr<Group>> scope {GetInode(file)->group};
    FlushImpl(
        false,
        [&] {
            result = SealTx(file, pin, false);
            Query q(db, "SELECT next FROM allocators WHERE name='ship'");
            sequence = q.Step() ? uint64_t(q.Int(0)) : 0;
        },
        &scope);
    return result;
}
std::string Store::Generation(int64_t file) {
    std::string root;
    WithReader([&](Reader &reader) {
        Query q(reader.db, reader.Statement("SELECT root FROM generations WHERE file=?"));
        q.Int(1, file);
        if (q.Step())
            root = q.Text(0);
    });
    return root;
}
void Store::InstallRootTx(int64_t file, const std::string &root) {
    RootInfo info{};
    if (!FindRoot(db, root, info))
        throw std::runtime_error("[differential:not_found] Root not found");
    Query kind(db, "SELECT kind FROM files WHERE id=?");
    kind.Int(1, file);
    if (!kind.Step() || kind.Int(0) != 0)
        throw std::runtime_error("[differential:invalid_input] Only chunked files can install a root");
    Query(db, "INSERT INTO extents SELECT ?,r.page_no*256+e.slot,e.object FROM root_pages r JOIN page_entries e "
              "ON e.page=r.page WHERE r.root=?")
        .Int(1, file)
        .Text(2, root)
        .Run();
    Query(db, "INSERT INTO file_pages SELECT ?,page_no,page,0 FROM root_pages WHERE root=?")
        .Int(1, file)
        .Text(2, root)
        .Run();
    Query(db, "UPDATE files SET version=version+1 WHERE id=?").Int(1, file).Run();
}
uint64_t Store::RootSizeTx(const std::string &root) { return VerifyRoot(db, root); }
std::string Store::ImportRootTx(uint64_t size, const std::vector<ChunkRef> &chunks) {
    CheckRange(0, size);
    // Pages and root are hashed exactly as SealTx hashes a file's map.
    std::map<uint64_t, std::vector<const ChunkRef *>> pages;
    uint64_t next = 0;
    for (auto &ref : chunks) {
        if (ref.chunk < next || ref.chunk >= ChunkCount(size) || ref.object.size() != 64)
            throw std::runtime_error("[differential:corrupt_data] Corrupt imported chunk map");
        next = ref.chunk + 1;
        pages[ref.chunk / PAGE_CHUNKS].push_back(&ref);
        // Objects without a local copy are remote-only; local copies are kept as they are.
        Query(db, "INSERT OR IGNORE INTO objects(hash) VALUES(?)").Text(1, ref.object).Run();
        Query(db, "INSERT OR IGNORE INTO remote_objects VALUES(?)").Text(1, ref.object).Run();
    }
    Digest root_digest;
    root_digest.Add(RootHeader(size));
    std::vector<std::pair<uint64_t, std::string>> page_hashes;
    for (auto &page : pages) {
        auto base = page.first * PAGE_CHUNKS;
        Digest digest;
        digest.Add(std::string("differential-page-v1\n"));
        for (auto *ref : page.second)
            digest.Add(std::to_string(ref->chunk - base) + ":" + ref->object + "\n");
        auto hash = digest.Finish();
        if (Query(db, "INSERT OR IGNORE INTO manifest_pages VALUES(?)").Text(1, hash).Run() == 1)
            for (auto *ref : page.second)
                Query(db, "INSERT INTO page_entries VALUES(?,?,?)")
                    .Text(1, hash)
                    .Int(2, int64_t(ref->chunk - base))
                    .Text(3, ref->object)
                    .Run();
        root_digest.Add(std::to_string(page.first) + ":" + hash + "\n");
        page_hashes.emplace_back(page.first, hash);
    }
    auto root = root_digest.Finish();
    Query(db, "INSERT OR IGNORE INTO roots(hash,size) VALUES(?,?)").Text(1, root).Int(2, int64_t(size)).Run();
    Query reclaimed(db, "SELECT 1 FROM reclaimed_roots WHERE root=?");
    reclaimed.Text(1, root);
    bool revive = reclaimed.Step();
    Query existing(db, "SELECT count(*) FROM root_pages WHERE root=?");
    existing.Text(1, root);
    existing.Step();
    if (revive || existing.Int(0) == 0)
        for (auto &page : page_hashes)
            Query(db, "INSERT OR IGNORE INTO root_pages VALUES(?,?,?)")
                .Text(1, root)
                .Int(2, int64_t(page.first))
                .Text(3, page.second)
                .Run();
    Query(db, "DELETE FROM reclaimed_roots WHERE root=?").Text(1, root).Run();
    remote_objects = true;
    return root;
}
File Store::Fork(const std::string &root, const std::string &name, const std::string &pool) {
    std::lock_guard<std::recursive_mutex> guard(mutex);
    Transaction tx(db, "DIFFERENTIAL_STORE_CRASH");
    auto n = VerifyRoot(db, root); // Reject reclaimed/corrupt manifests before copying their map.
    auto id = CreateFileTx(name, n, pool);
    InstallRootTx(id, root);
    tx.Commit();
    return {id, n};
}
bool Store::HasRoot(const std::string &root) {
    bool found = false;
    WithReader([&](Reader &reader) {
        RootInfo info{};
        found = FindRoot(reader.db, root, info);
    });
    return found;
}
uint64_t Store::RootSize(const std::string &root) {
    uint64_t size = 0;
    WithReader([&](Reader &reader) { size = VerifyRoot(reader.db, root); });
    return size;
}
uint64_t Store::ReadRootImpl(Reader &reader, const std::string &root, void *data, uint64_t n, uint64_t p,
                             const std::string &pool) {
    RootInfo info{};
    if (!FindRoot(reader.db, root, info))
        throw std::runtime_error("[differential:not_found] File/root not found");
    if (n == 0 || p >= info.size)
        return 0;
    n = std::min(n, info.size - p);
    auto first = ChunkIndex(p), last = ChunkIndex(p + n - 1);
    std::map<uint64_t, std::string> map;
    for (auto page_no = first / PAGE_CHUNKS; page_no <= last / PAGE_CHUNKS; ++page_no) {
        auto page = RootPages::Instance().Find(root, page_no);
        if (!page) {
            Timer lookup(Metric::ExtentLookup);
            auto entries = std::make_shared<std::vector<std::string>>(PAGE_CHUNKS);
            Query q(reader.db, reader.Statement("SELECT e.slot,e.object FROM root_pages r JOIN page_entries e ON "
                                                "e.page=r.page WHERE r.root=? AND r.page_no=?"));
            q.Text(1, root).Int(2, int64_t(page_no));
            while (q.Step())
                (*entries)[uint64_t(q.Int(0))] = q.Text(1);
            page = entries;
            RootPages::Instance().Insert(root, page_no, page);
        }
        for (uint64_t slot = 0; slot < PAGE_CHUNKS; ++slot) {
            auto chunk = page_no * PAGE_CHUNKS + slot;
            if (chunk >= first && chunk <= last && !(*page)[slot].empty())
                map.emplace(chunk, (*page)[slot]);
        }
    }
    std::map<std::string, SharedBytes> fetched;
    if (pool.empty() && remote_objects) {
        std::vector<std::string> wanted;
        for (auto &entry : map)
            wanted.push_back(entry.second);
        fetched = LoadObjects(reader, wanted);
    }
    auto out = static_cast<unsigned char *>(data);
    uint64_t done = 0;
    while (done < n) {
        auto pos = p + done, index = ChunkIndex(pos);
        auto take = std::min(n - done, ChunkLength(index) - ChunkPosition(pos));
        auto found = map.find(index);
        if (found == map.end())
            std::memset(out + done, 0, take);
        else
            CopyObject(reader, found->second, pool, index, ChunkPosition(pos), take, out + done, fetched);
        done += take;
    }
    return n;
}
uint64_t Store::ReadRoot(const std::string &root, void *data, uint64_t n, uint64_t p, const std::string &pool) {
    Timer timer(Metric::LocalRead);
    CheckRange(n, p);
    Count(Counter::LogicalLocalReadBytes, n);
    std::unique_ptr<PayloadFence> fence;
    if (!pool.empty())
        fence = std::make_unique<PayloadFence>(directory, false);
    uint64_t result = 0;
    WithReader([&](Reader &reader) { result = ReadRootImpl(reader, root, data, n, p, pool); });
    return result;
}
std::vector<ChunkRef> Store::RootChunks(const std::string &root, uint64_t first, uint64_t count) {
    std::vector<ChunkRef> result;
    WithReader([&](Reader &reader) {
        RootInfo info{};
        if (!FindRoot(reader.db, root, info))
            throw std::runtime_error("[differential:not_found] Root not found");
        Query q(reader.db, reader.Statement("SELECT r.page_no*256+e.slot,e.object FROM root_pages r JOIN page_entries "
                                            "e ON e.page=r.page WHERE r.root=? AND r.page_no>=? AND r.page_no<=? "
                                            "ORDER BY r.page_no,e.slot"));
        q.Text(1, root).Int(2, int64_t(first / PAGE_CHUNKS)).Int(3, int64_t((first + count - 1) / PAGE_CHUNKS));
        while (q.Step()) {
            auto chunk = uint64_t(q.Int(0));
            if (chunk >= first && chunk < first + count)
                result.push_back({chunk, q.Text(1)});
        }
    });
    return result;
}
uint64_t Store::ObjectCount() {
    uint64_t count = 0;
    WithReader([&](Reader &reader) {
        Query q(reader.db, reader.Statement("SELECT count(*) FROM objects"));
        q.Step();
        count = uint64_t(q.Int(0));
    });
    return count;
}
SharedBytes Store::Object(const std::string &hash) {
    SharedBytes result;
    WithReader([&](Reader &reader) { result = ObjectBytes(reader, hash, ""); });
    return result;
}
void Store::MaterializeObjectsTx(const std::map<std::string, SharedBytes> &objects) {
    // Appended to this store's own object segment, synced before the caller's transaction commits.
    // No gate: the caller holds `mutex`, which publications take after their gates.
    auto group = GetGroup("#objects");
    std::vector<std::tuple<std::string, uint64_t, uint32_t>> stored;
    for (auto &object : objects) {
        if (!object.second || object.second->size() != CHUNK)
            throw std::runtime_error("[differential:corrupt_data] Invalid remote object");
        auto crc = Crc32c(object.second->data(), CHUNK);
        stored.emplace_back(object.first,
                            AppendRecord(*group, SegmentRecord::OBJECT, 0, 0, object.second->data(), CHUNK, crc), crc);
    }
    if (stored.empty())
        return;
    std::lock_guard<std::mutex> guard(group->segment_mutex);
    group->segment->referenced = true;
    group->segment->Sync();
    Query(db, "INSERT INTO segments VALUES(?,?,1) ON CONFLICT(id) DO UPDATE SET committed=excluded.committed")
        .Int(1, group->segment->id)
        .Int(2, int64_t(group->segment->End()))
        .Run();
    for (auto &entry : stored)
        Query(db, "UPDATE objects SET segment=?,position=?,crc=? WHERE hash=? AND segment IS NULL")
            .Int(1, group->segment->id)
            .Int(2, int64_t(std::get<1>(entry)))
            .Int(3, int64_t(std::get<2>(entry)))
            .Text(4, std::get<0>(entry))
            .Run();
}
// ---------------------------------------------------------------------------------------
// Continuous WAL shipping: change tracking
void Store::SetShipping(const std::string &group_key, bool enabled) {
    {
        std::lock_guard<std::recursive_mutex> guard(mutex);
        Transaction tx(db, "DIFFERENTIAL_STORE_CRASH", false);
        if (enabled)
            Query(db, "INSERT OR IGNORE INTO ship_groups VALUES(?)").Text(1, group_key).Run();
        else
            Query(db, "DELETE FROM ship_groups WHERE group_key=?").Text(1, group_key).Run();
        tx.Commit();
    }
    std::lock_guard<std::mutex> guard(shipping_mutex);
    if (enabled)
        shipping_groups.insert(group_key);
    else
        shipping_groups.erase(group_key);
}
bool Store::Shipping(const std::string &group_key) {
    std::lock_guard<std::mutex> guard(shipping_mutex);
    return shipping_groups.count(group_key) > 0;
}
void Store::TrackShipTx(int64_t file, const std::vector<uint64_t> &chunks, uint64_t low, uint64_t &sequence) {
    if (!sequence) {
        Query next(db, "UPDATE allocators SET next=next+1 WHERE name='ship' RETURNING next");
        if (!next.Step())
            throw std::runtime_error("[differential:corrupt_data] Shipping sequence missing");
        sequence = uint64_t(next.Int(0));
        while (next.Step()) {
        }
    }
    for (auto chunk : chunks)
        Query(db, "INSERT INTO ship_chunks VALUES(?,?,?) ON CONFLICT(file,chunk) DO UPDATE SET seq=excluded.seq")
            .Int(1, file)
            .Int(2, int64_t(chunk))
            .Int(3, int64_t(sequence))
            .Run();
    // Every published file gets a row (its size changed or not): the lowest changed byte (log files)
    // or the truncation floor (chunked files), else "none".
    Query(db, "INSERT INTO ship_lows VALUES(?,?,?) ON CONFLICT(file) DO UPDATE SET low=min(low,excluded.low),"
              "seq=excluded.seq")
        .Int(1, file)
        .Int(2, int64_t(low))
        .Int(3, int64_t(sequence))
        .Run();
}
std::vector<Store::ShipFile> Store::GroupFiles(sqlite3 *connection, const std::string &group_key) {
    std::vector<ShipFile> files;
    Query q(connection, "SELECT name,id,size,kind FROM files WHERE name=?1 OR (substr(name,1,length(?1))=?1 AND "
                        "substr(name,length(?1)+1,1)='.') ORDER BY name");
    q.Text(1, group_key);
    while (q.Step())
        files.push_back({q.Text(0), q.Int(1), uint64_t(q.Int(2)), q.Int(3) == 1});
    return files;
}
uint64_t Store::ShipSequence() {
    uint64_t sequence = 0;
    WithReader([&](Reader &reader) {
        Query q(reader.db, reader.Statement("SELECT next FROM allocators WHERE name='ship'"));
        sequence = q.Step() ? uint64_t(q.Int(0)) : 0;
    });
    return sequence;
}
uint64_t Store::ReadShipDelta(const std::string &group_key, const std::map<std::string, int64_t> &known,
                              ShipSink &sink) {
    uint64_t sequence = 0;
    WithReader([&](Reader &reader) {
        sink.Reset();
        {
            Query q(reader.db, reader.Statement("SELECT next FROM allocators WHERE name='ship'"));
            sequence = q.Step() ? uint64_t(q.Int(0)) : 0;
        }
        auto files = GroupFiles(reader.db, group_key);
        sink.Group(files);
        for (auto &file : files) {
            auto previous = known.find(file.name);
            bool full = previous == known.end() || previous->second != file.id;
            uint64_t low = NONE;
            bool touched = full;
            if (!full) {
                Query q(reader.db, reader.Statement("SELECT low FROM ship_lows WHERE file=? AND seq<=?"));
                q.Int(1, file.id).Int(2, int64_t(sequence));
                if (q.Step()) {
                    touched = true;
                    low = uint64_t(q.Int(0));
                }
            }
            if (!touched)
                continue;
            sink.File(file, full);
            if (file.log) {
                auto from = full ? 0 : std::min(low, file.size);
                std::vector<unsigned char> tail(file.size - from);
                if (!tail.empty())
                    ReadRanges(reader, file.id, from, tail.size(), NONE, tail.data());
                sink.Tail(file, from, tail);
                continue;
            }
            // Chunked: every chunk (whole file), or the changed ones plus everything from the floor.
            std::set<uint64_t> wanted;
            auto count = ChunkCount(file.size);
            if (full || low < count)
                for (auto chunk = full ? 0 : low; chunk < count; ++chunk)
                    wanted.insert(chunk);
            if (!full) {
                Query q(reader.db, reader.Statement("SELECT chunk FROM ship_chunks WHERE file=? AND seq<=?"));
                q.Int(1, file.id).Int(2, int64_t(sequence));
                while (q.Step())
                    if (uint64_t(q.Int(0)) < count)
                        wanted.insert(uint64_t(q.Int(0)));
            }
            Query extent(reader.db, reader.Statement("SELECT object FROM extents WHERE file=? AND chunk=?"));
            for (auto chunk : wanted) {
                extent.Int(1, file.id).Int(2, int64_t(chunk));
                std::string hash = extent.Step() ? extent.Text(0) : "";
                sqlite3_reset(extent.s);
                if (full && hash.empty())
                    continue; // a whole file lists its holes implicitly
                sink.Chunk(file, chunk, hash, hash.empty() ? nullptr : ObjectBytes(reader, hash, ""));
            }
        }
    });
    return sequence;
}
void Store::AckShipped(const std::string &group_key, uint64_t sequence) {
    std::lock_guard<std::recursive_mutex> guard(mutex);
    Transaction tx(db, "DIFFERENTIAL_STORE_CRASH", false);
    std::vector<int64_t> ids;
    for (auto &file : GroupFiles(db, group_key))
        ids.push_back(file.id);
    for (auto table : {"ship_chunks", "ship_lows"}) {
        for (auto id : ids)
            Query(db, (std::string("DELETE FROM ") + table + " WHERE file=? AND seq<=?").c_str())
                .Int(1, id)
                .Int(2, int64_t(sequence))
                .Run();
        // Changes of files that lost their name (or were collected) can never be shipped.
        Query(db, (std::string("DELETE FROM ") + table +
                   " WHERE seq<=? AND file NOT IN (SELECT id FROM files WHERE name IS NOT NULL)")
                      .c_str())
            .Int(1, int64_t(sequence))
            .Run();
    }
    tx.Commit();
}
void Store::Verify() {
    PayloadFence fence(directory, false);
    WithReader([&](Reader &reader) {
        auto db = reader.db;
        Query q(db, "PRAGMA integrity_check");
        if (!q.Step() || q.Text(0) != "ok")
            throw std::runtime_error("[differential:corrupt_data] SQLite integrity failure");
        Query fk(db, "PRAGMA foreign_key_check");
        if (fk.Step())
            throw std::runtime_error("[differential:corrupt_data] Dangling extent reference");
        Query roots(db, "SELECT hash FROM roots WHERE hash NOT IN (SELECT root FROM reclaimed_roots)");
        while (roots.Step())
            VerifyRoot(db, roots.Text(0));
        Query pages(db, "SELECT hash FROM manifest_pages");
        while (pages.Step()) {
            Digest digest;
            digest.Add(std::string("differential-page-v1\n"));
            Query entries(db, "SELECT slot,object FROM page_entries WHERE page=? ORDER BY slot");
            entries.Text(1, pages.Text(0));
            while (entries.Step())
                digest.Add(std::to_string(entries.Int(0)) + ":" + entries.Text(1) + "\n");
            if (digest.Finish() != pages.Text(0))
                throw std::runtime_error("[differential:corrupt_data] Corrupt manifest page");
        }
        Query clean(db, "SELECT f.file,f.page_no,f.page FROM file_pages f WHERE f.dirty=0");
        while (clean.Step()) {
            // A clean page cache entry must still describe the live map exactly.
            Digest digest;
            digest.Add(std::string("differential-page-v1\n"));
            auto base = clean.Int(1) * int64_t(PAGE_CHUNKS);
            Query entries(db, "SELECT chunk,object FROM extents WHERE file=? AND chunk>=? AND chunk<? ORDER BY chunk");
            entries.Int(1, clean.Int(0)).Int(2, base).Int(3, base + int64_t(PAGE_CHUNKS));
            while (entries.Step())
                digest.Add(std::to_string(entries.Int(0) - base) + ":" + entries.Text(1) + "\n");
            if (digest.Finish() != clean.Text(2))
                throw std::runtime_error("[differential:corrupt_data] Stale live manifest page (file " +
                                         std::to_string(clean.Int(0)) + ", page " + std::to_string(clean.Int(1)) + ")");
        }
        // Every local object: its bytes match both checksums. Every log range: its record's checksum.
        Query objects(db, "SELECT hash,segment,position,crc FROM objects WHERE segment IS NOT NULL");
        Bytes bytes(CHUNK);
        while (objects.Step()) {
            Location location {objects.Int(1), uint64_t(objects.Int(2)), uint32_t(objects.Int(3))};
            auto descriptor = segment_files->Open(location.segment);
            if (!Segment::ReadExact(descriptor->fd, bytes.data(), CHUNK, location.position) ||
                Crc32c(bytes.data(), CHUNK) != location.crc || Hash(bytes.data(), CHUNK) != objects.Text(0))
                throw std::runtime_error("[differential:corrupt_data] Corrupt immutable object");
        }
        Query records(db, "SELECT DISTINCT segment,record,record_length,crc FROM ranges");
        while (records.Step()) {
            auto descriptor = segment_files->Open(records.Int(0));
            Bytes record(uint64_t(records.Int(2)));
            if (!Segment::ReadExact(descriptor->fd, record.data(), record.size(), uint64_t(records.Int(1))) ||
                Crc32c(record.data(), record.size()) != uint32_t(records.Int(3)))
                throw std::runtime_error("[differential:corrupt_data] Corrupt log record");
        }
        Query external(db, "SELECT o.hash,o.pool,p.path,p.repository_id FROM local_objects o JOIN local_pools p ON "
                           "p.id=o.pool");
        while (external.Step())
            LocalPool(external.Text(2), external.Text(3), external.Text(1), permissions).Get(external.Text(0));
        Query missing(db, "SELECT 1 FROM objects o WHERE segment IS NULL AND NOT EXISTS(SELECT 1 FROM local_objects "
                          "l WHERE l.hash=o.hash) AND NOT EXISTS(SELECT 1 FROM remote_objects r WHERE r.hash=o.hash) "
                          "LIMIT 1");
        if (missing.Step())
            throw std::runtime_error("[differential:corrupt_data] Object has no durable location");
    });
}

// ---------------------------------------------------------------------------------------
// Segment collection. Retired segments are deleted once nothing references them; sparse ones
// (mostly overwritten or collected payload) have their live payload rewritten first.
bool Store::DeleteSegment(int64_t id) {
    auto unreferenced = [&](sqlite3 *connection) {
        Query q(connection, "SELECT EXISTS(SELECT 1 FROM segments WHERE id=?1 AND active=0) AND NOT EXISTS(SELECT 1 "
                            "FROM objects WHERE segment=?1) AND NOT EXISTS(SELECT 1 FROM ranges WHERE segment=?1)");
        q.Int(1, id);
        return q.Step() && q.Int(0) != 0;
    };
    bool deletable = false;
    WithReader([&](Reader &reader) { deletable = unreferenced(reader.db); });
    if (!deletable)
        return false;
    // Nothing adds references to a retired segment, so the file goes first: a crash leaves a row
    // without a file (deleted again), never a file that a replay would mistake for unpublished.
    auto name = Segment::Name(id);
    for (auto folder : {"segments", "journal"}) {
        auto path = (std::filesystem::path(directory) / folder / name).string();
        if (unlink(path.c_str()) && errno != ENOENT)
            throw std::runtime_error("[differential:local_io] Cannot delete segment");
    }
    segment_files->Forget(id);
    std::lock_guard<std::recursive_mutex> guard(mutex);
    Transaction tx(db, "DIFFERENTIAL_STORE_CRASH", false);
    if (!unreferenced(db))
        return false;
    Query(db, "DELETE FROM segments WHERE id=?").Int(1, id).Run();
    tx.Commit();
    Count(Counter::LocalSegmentsDeleted);
    return true;
}
uint64_t Store::CompactSegment(int64_t id) {
    std::vector<std::pair<std::string, Location>> objects;
    std::vector<RangeRow> records;
    WithReader([&](Reader &reader) {
        objects.clear();
        records.clear();
        Query q(reader.db, "SELECT hash,position,crc FROM objects WHERE segment=?");
        q.Int(1, id);
        while (q.Step())
            objects.push_back({q.Text(0), {id, uint64_t(q.Int(1)), uint32_t(q.Int(2))}});
        Query r(reader.db, "SELECT DISTINCT record,record_length,crc FROM ranges WHERE segment=?");
        r.Int(1, id);
        while (r.Step())
            records.push_back({0, 0, id, uint64_t(r.Int(0)), uint64_t(r.Int(1)), uint32_t(r.Int(2)), 0});
    });
    std::shared_ptr<SegmentFiles::Descriptor> source;
    try {
        source = segment_files->Open(id);
    } catch (const StaleRead &) {
        return 0;
    }
    auto target = Segment::Create((std::filesystem::path(directory) / "journal").string(), permissions);
    std::vector<uint64_t> object_positions, record_positions;
    Bytes bytes;
    uint64_t copied = 0;
    for (auto &object : objects) {
        bytes.resize(CHUNK);
        if (!Segment::ReadExact(source->fd, bytes.data(), CHUNK, object.second.position) ||
            Crc32c(bytes.data(), CHUNK) != object.second.crc)
            throw std::runtime_error("[differential:corrupt_data] Corrupt immutable object");
        object_positions.push_back(
            target->Append(SegmentRecord::OBJECT, 0, 0, bytes.data(), CHUNK, object.second.crc));
        copied += CHUNK;
    }
    for (auto &record : records) {
        bytes.resize(record.record_length);
        if (!Segment::ReadExact(source->fd, bytes.data(), bytes.size(), record.record) ||
            Crc32c(bytes.data(), bytes.size()) != record.crc)
            throw std::runtime_error("[differential:corrupt_data] Corrupt log record");
        record_positions.push_back(
            target->Append(SegmentRecord::OBJECT, 0, 0, bytes.data(), bytes.size(), record.crc));
        copied += bytes.size();
    }
    target->Sync();
    {
        std::lock_guard<std::recursive_mutex> guard(mutex);
        Transaction tx(db, "DIFFERENTIAL_STORE_CRASH", false);
        Query(db, "INSERT INTO segments VALUES(?,?,0)").Int(1, target->id).Int(2, int64_t(target->End())).Run();
        // Only references that still point at the old copy move.
        for (size_t i = 0; i < objects.size(); ++i)
            Query(db, "UPDATE objects SET segment=?,position=? WHERE hash=? AND segment=? AND position=?")
                .Int(1, target->id)
                .Int(2, int64_t(object_positions[i]))
                .Text(3, objects[i].first)
                .Int(4, id)
                .Int(5, int64_t(objects[i].second.position))
                .Run();
        for (size_t i = 0; i < records.size(); ++i)
            Query(db, "UPDATE ranges SET segment=?,record=? WHERE segment=? AND record=?")
                .Int(1, target->id)
                .Int(2, int64_t(record_positions[i]))
                .Int(3, id)
                .Int(4, int64_t(records[i].record))
                .Run();
        tx.Commit();
    }
    for (auto &object : objects)
        segment_files->ForgetLocation(object.first);
    target->Retire((std::filesystem::path(directory) / "segments").string());
    Count(Counter::LocalSegmentBytesCompacted, copied);
    return copied;
}
uint64_t Store::CollectSegments(uint64_t budget) {
    struct Candidate {
        int64_t id;
        uint64_t live;
    };
    std::vector<int64_t> unreferenced;
    std::vector<Candidate> retired;
    WithReader([&](Reader &reader) {
        unreferenced.clear();
        retired.clear();
        Query q(reader.db, "SELECT s.id,(SELECT count(*) FROM objects o WHERE o.segment=s.id)*?,(SELECT "
                           "coalesce(sum(record_length),0) FROM (SELECT DISTINCT record,record_length FROM ranges r "
                           "WHERE r.segment=s.id)) FROM segments s WHERE s.active=0");
        q.Int(1, int64_t(CHUNK + Segment::HEADER));
        while (q.Step()) {
            auto live = uint64_t(q.Int(1) + q.Int(2));
            if (live == 0)
                unreferenced.push_back(q.Int(0));
            else
                retired.push_back({q.Int(0), live});
        }
    });
    uint64_t deleted = 0;
    for (auto id : unreferenced)
        deleted += DeleteSegment(id) ? 1 : 0;
    uint64_t copied = 0;
    for (auto &candidate : retired) {
        if (copied >= budget)
            break;
        struct stat st {};
        auto path = (std::filesystem::path(directory) / "segments" / Segment::Name(candidate.id)).string();
        if (stat(path.c_str(), &st) || uint64_t(st.st_size) < COMPACT_MIN_BYTES ||
            candidate.live * 2 >= uint64_t(st.st_size))
            continue;
        copied += CompactSegment(candidate.id);
        deleted += DeleteSegment(candidate.id) ? 1 : 0;
    }
    return deleted;
}

// ---------------------------------------------------------------------------------------
// External pool reclamation. The caller holds the exclusive payload fence and `mutex`, with
// no open transaction.
void Store::DrainLocalDeletions(uint64_t limit) {
    Transaction tx(db, "DIFFERENTIAL_STORE_CRASH");
    struct Pending {
        std::string pool, hash, path, repo;
    };
    std::vector<Pending> batch;
    {
        Query pending(db, "SELECT d.pool,d.hash,p.path,p.repository_id FROM local_deletions d JOIN local_pools p "
                          "ON p.id=d.pool LIMIT ?");
        pending.Int(1, int64_t(limit));
        while (pending.Step())
            batch.push_back({pending.Text(0), pending.Text(1), pending.Text(2), pending.Text(3)});
    }
    for (auto &item : batch) {
        Query reused(db, "SELECT 1 FROM local_objects WHERE pool=? AND hash=?");
        reused.Text(1, item.pool).Text(2, item.hash);
        if (!reused.Step())
            LocalPool(item.path, item.repo, item.pool, permissions).Remove(item.hash);
        Failpoint("DIFFERENTIAL_LOCAL_CRASH", "payload_deleted");
        Query(db, "DELETE FROM local_deletions WHERE pool=? AND hash=?").Text(1, item.pool).Text(2, item.hash).Run();
    }
    tx.Commit();
}
uint64_t Store::SweepLocalOrphans(uint64_t limit, bool dry_run) {
    Transaction tx(db, "DIFFERENTIAL_STORE_CRASH");
    struct PoolRow {
        std::string id, path, repo, cursor;
    };
    std::vector<PoolRow> pools;
    {
        Query q(db, "SELECT id,path,repository_id,gc_cursor FROM local_pools ORDER BY id");
        while (q.Step())
            pools.push_back({q.Text(0), q.Text(1), q.Text(2), q.Text(3)});
    }
    uint64_t remaining = limit, orphans = 0;
    for (auto &row : pools) {
        if (!remaining)
            break;
        LocalPool pool(row.path, row.repo, row.id);
        auto batch = pool.List(dry_run ? "" : row.cursor, remaining);
        for (auto &name : batch) {
            auto slash = name.find('/');
            auto leaf = slash == std::string::npos ? name : name.substr(slash + 1);
            bool live = false;
            if (leaf.size() == 64) {
                Query q(db, "SELECT 1 FROM local_objects WHERE hash=? AND pool=?");
                q.Text(1, leaf).Text(2, row.id);
                live = q.Step();
            }
            if (!live) {
                ++orphans;
                if (!dry_run)
                    pool.Remove(name);
            }
        }
        if (!dry_run)
            Query(db, "UPDATE local_pools SET gc_cursor=? WHERE id=?")
                .Text(1, batch.size() < remaining ? "" : batch.back())
                .Text(2, row.id)
                .Run();
        remaining -= batch.size();
    }
    tx.Commit();
    return orphans;
}
} // namespace differential
