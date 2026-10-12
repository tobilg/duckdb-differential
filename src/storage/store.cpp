#include "differential_store.hpp"
#include "differential_journal.hpp"
#include "differential_local.hpp"
#include "differential_metrics.hpp"
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
constexpr uint64_t MAX_FILE = uint64_t(1) << 40; // Explicit v1 bound, 1 TiB logical file.
constexpr uint64_t NONE = std::numeric_limits<uint64_t>::max();
constexpr uint64_t JOURNAL_RECORD = 16ull * 1024 * 1024;
constexpr size_t MAX_IDLE_READERS = 16;
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
std::string NewUUID() {
    unsigned char bytes[16];
    if (RAND_bytes(bytes, sizeof(bytes)) != 1)
        throw std::runtime_error("Cannot allocate secure identity");
    bytes[6] = (bytes[6] & 15) | 64;
    bytes[8] = (bytes[8] & 63) | 128;
    const char *hex = "0123456789abcdef";
    std::string result;
    for (size_t i = 0; i < sizeof(bytes); ++i) {
        if (i == 4 || i == 6 || i == 8 || i == 10)
            result += '-';
        result += hex[bytes[i] >> 4];
        result += hex[bytes[i] & 15];
    }
    return result;
}
bool IsUUID(const std::string &s) {
    return s.size() == 36 && s.find_first_not_of("0123456789abcdef-") == std::string::npos;
}
void SyncDirectory(const std::filesystem::path &path) {
    int fd = open(path.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (fd < 0)
        throw std::runtime_error("Cannot open repository directory for sync");
    int rc = fsync(fd);
#ifdef __APPLE__
    if (rc == 0)
        rc = fcntl(fd, F_FULLFSYNC);
#endif
    close(fd);
    if (rc)
        throw std::runtime_error("Cannot sync repository directory");
}
const SharedBytes &ZeroChunk() {
    static const SharedBytes zero = std::make_shared<const Bytes>(Store::CHUNK, 0);
    return zero;
}
// Process-wide cache of verified immutable chunks. Content addressing makes entries valid
// for every store and pool: equal identity implies equal bytes.
class ChunkCache {
  public:
    static constexpr size_t LIMIT = 32ull * 1024 * 1024;
    static ChunkCache &Instance() {
        static ChunkCache cache;
        return cache;
    }
    SharedBytes Find(const std::string &hash) {
        std::lock_guard<std::mutex> guard(mutex);
        auto found = entries.find(hash);
        if (found == entries.end())
            return nullptr;
        age.splice(age.begin(), age, found->second.second);
        return found->second.first;
    }
    void Insert(const std::string &hash, SharedBytes bytes) {
        std::lock_guard<std::mutex> guard(mutex);
        if (entries.count(hash))
            return;
        while (size + bytes->size() > LIMIT && !age.empty()) {
            auto &victim = entries.at(age.back());
            size -= victim.first->size();
            entries.erase(age.back());
            age.pop_back();
        }
        age.push_front(hash);
        size += bytes->size();
        entries.emplace(hash, std::make_pair(std::move(bytes), age.begin()));
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

void Store::DropCaches() { ChunkCache::Instance().Clear(); }

// ---------------------------------------------------------------------------------------
// Pooled readers
Reader::Reader(const std::string &path) {
    // Readers copy whole 64 KiB blobs: memory-mapped pages avoid one pread per 4 KiB page,
    // which otherwise dominates and serializes parallel scans.
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
    ReadScope scope(AcquireReader(), [this](std::unique_ptr<Reader> r) { ReleaseReader(std::move(r)); });
    fn(*scope.reader);
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
    std::string pool, pool_path, pool_repository;
    bool dirty = false;
    uint64_t size = 0;
    uint64_t floor = NONE; // committed chunks at or above this index are truncated holes
    std::map<uint64_t, std::shared_ptr<Bytes>> chunks;
};
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
        Execute(db, "PRAGMA foreign_keys=ON; PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA "
                    "fullfsync=ON; PRAGMA checkpoint_fullfsync=ON; PRAGMA cache_size=-8192; "
                    "PRAGMA journal_size_limit=67108864;");
        if (version == 0)
            CreateSchema(permissions.shared);
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
        permissions.CreateDirectories(p / "journal");
        journal_id = NewUUID();
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
    Execute(db, R"SQL(
CREATE TABLE IF NOT EXISTS files(id INTEGER PRIMARY KEY, name TEXT UNIQUE, size INTEGER NOT NULL CHECK(size>=0));
CREATE TABLE IF NOT EXISTS objects(hash TEXT PRIMARY KEY, data BLOB CHECK(data IS NULL OR length(data)=65536)) WITHOUT ROWID;
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
CREATE TABLE IF NOT EXISTS roots(hash TEXT PRIMARY KEY, size INTEGER NOT NULL CHECK(size>=0)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS generations(file INTEGER PRIMARY KEY REFERENCES files(id), root TEXT NOT NULL REFERENCES roots(hash));
CREATE TABLE IF NOT EXISTS pins(id TEXT PRIMARY KEY, root TEXT REFERENCES roots(hash), file INTEGER REFERENCES files(id)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS reclaimed_roots(root TEXT PRIMARY KEY REFERENCES roots(hash)) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS extents_object ON extents(object);
CREATE INDEX IF NOT EXISTS pins_file ON pins(file);
CREATE INDEX IF NOT EXISTS pins_root ON pins(root);
)SQL");
    // Paged manifests, durable allocator, journal commit marks and GC queues.
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
CREATE TABLE IF NOT EXISTS journal_state(journal TEXT PRIMARY KEY, committed INTEGER NOT NULL) WITHOUT ROWID;
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
constexpr int CHECKPOINT_PAGES = 4096;      // ~16 MiB of WAL at 4 KiB pages: ask the background
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
                                 "PRAGMA synchronous=FULL; PRAGMA fullfsync=ON; PRAGMA checkpoint_fullfsync=ON;",
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
    bool clean = false;
    try {
        Sync();
        clean = true;
    } catch (...) {
        // Unsynced bytes remain in the journal; the next store replays them.
    }
    if (journal && clean) {
        try {
            std::lock_guard<std::recursive_mutex> guard(mutex);
            journal->Remove();
            Transaction tx(db, "DIFFERENTIAL_STORE_CRASH");
            Query(db, "DELETE FROM journal_state WHERE journal=?").Text(1, journal_id).Run();
            tx.Commit();
        } catch (...) {
        }
    }
    journal.reset();
    readers.clear();
    if (db)
        sqlite3_close_v2(db);
}

// ---------------------------------------------------------------------------------------
// Journals of dead processes
void Store::ReplayJournals() {
    static std::mutex replay_mutex;
    std::lock_guard<std::mutex> serialize(replay_mutex);
    auto folder = std::filesystem::path(directory) / "journal";
    std::error_code error;
    if (!std::filesystem::exists(folder, error))
        return;
    for (auto &entry : std::filesystem::directory_iterator(folder)) {
        auto name = entry.path().filename().string();
        if (!IsUUID(name) || name == journal_id || !entry.is_regular_file())
            continue;
        int fd = Journal::TryClaim(entry.path().string());
        if (fd < 0)
            continue; // owner is live
        try {
            uint64_t committed = 0;
            WithReader([&](Reader &reader) {
                Query q(reader.db, reader.Statement("SELECT committed FROM journal_state WHERE journal=?"));
                q.Text(1, name);
                if (q.Step())
                    committed = uint64_t(q.Int(0));
            });
            auto records = Journal::ReadValid(fd, committed);
            if (!records.empty()) {
                {
                    std::shared_lock<std::shared_mutex> gate(write_gate);
                    for (auto &record : records) {
                        bool exists = false;
                        WithReader([&](Reader &reader) {
                            Query q(reader.db, reader.Statement("SELECT 1 FROM files WHERE id=?"));
                            q.Int(1, record.file);
                            exists = q.Step();
                        });
                        if (!exists)
                            continue; // the inode was unlinked and collected; its bytes are unreachable
                        auto inode = GetInode(record.file);
                        std::unique_lock<std::shared_mutex> lock(inode->lock);
                        if (record.type == JournalRecord::WRITE)
                            ApplyWrite(*inode, record.data.data(), record.data.size(), record.offset, false);
                        else
                            ApplyTruncate(*inode, record.offset, false);
                    }
                }
                auto last = records.back().sequence;
                FlushImpl(true, [&] {
                    Query(db, "INSERT INTO journal_state VALUES(?,?) ON CONFLICT(journal) DO UPDATE SET "
                              "committed=excluded.committed")
                        .Text(1, name)
                        .Int(2, int64_t(last))
                        .Run();
                });
            }
            if (unlink(entry.path().c_str()) && errno != ENOENT)
                throw std::runtime_error("[differential:local_io] Cannot remove replayed write journal");
            {
                std::lock_guard<std::recursive_mutex> guard(mutex);
                Transaction tx(db, "DIFFERENTIAL_STORE_CRASH");
                Query(db, "DELETE FROM journal_state WHERE journal=?").Text(1, name).Run();
                tx.Commit();
            }
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
    WithReader([&](Reader &reader) {
        Query q(reader.db, reader.Statement("SELECT f.id,p.pool,l.path,l.repository_id FROM files f LEFT JOIN "
                                            "file_pools p ON p.file=f.id LEFT JOIN local_pools l ON l.id=p.pool "
                                            "WHERE f.id=?"));
        q.Int(1, file);
        if (q.Step()) {
            exists = true;
            inode->pool = q.Text(1);
            inode->pool_path = q.Text(2);
            inode->pool_repository = q.Text(3);
        }
    });
    if (!exists)
        throw std::runtime_error("Invalid file handle");
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
    } else {
        Query q(reader.db, reader.Statement("SELECT data FROM objects WHERE hash=?"));
        q.Text(1, hash);
        if (!q.Step())
            throw std::runtime_error("[differential:unavailable_object] Object unavailable");
        if (sqlite3_column_type(q.s, 0) == SQLITE_NULL) {
            remote_objects = true;
            return FetchRemote({hash}).at(hash); // remote-only: read on demand
        }
        return InlineObject(q.s, hash);
    }
    Count(Counter::LocalMaterializedBytes, bytes->size());
    SharedBytes shared = std::move(bytes);
    ChunkCache::Instance().Insert(hash, shared);
    return shared;
}
SharedBytes Store::InlineObject(sqlite3_stmt *row, const std::string &hash) {
    auto n = sqlite3_column_bytes(row, 0);
    auto p = static_cast<const unsigned char *>(sqlite3_column_blob(row, 0));
    if (n != int(CHUNK) || Hash(p, size_t(n)) != hash)
        throw std::runtime_error("[differential:corrupt_data] Corrupt immutable object");
    Count(Counter::LocalMaterializedBytes, CHUNK);
    SharedBytes shared = std::make_shared<const Bytes>(p, p + n);
    ChunkCache::Instance().Insert(hash, shared);
    return shared;
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
    std::map<std::string, SharedBytes> loaded;
    std::vector<std::string> remote, remote_ahead;
    for (auto &hash : hashes) {
        if (loaded.count(hash))
            continue;
        if (auto cached = ChunkCache::Instance().Find(hash)) {
            loaded.emplace(hash, cached);
            continue;
        }
        Query q(reader.db, reader.Statement("SELECT data FROM objects WHERE hash=?"));
        q.Text(1, hash);
        if (!q.Step())
            continue; // ObjectBytes reports it
        if (sqlite3_column_type(q.s, 0) == SQLITE_NULL) {
            if (std::find(remote.begin(), remote.end(), hash) == remote.end())
                remote.push_back(hash);
            continue;
        }
        loaded.emplace(hash, InlineObject(q.s, hash));
    }
    // Ahead: only remote-only objects nobody holds yet (typeof() does not load a stored blob).
    std::set<std::string> seen(hashes.begin(), hashes.end());
    for (auto &hash : ahead) {
        if (!seen.insert(hash).second || ChunkCache::Instance().Find(hash))
            continue;
        Query q(reader.db, reader.Statement("SELECT typeof(data)='null' FROM objects WHERE hash=?"));
        q.Text(1, hash);
        if (q.Step() && q.Int(0))
            remote_ahead.push_back(hash);
    }
    if (!remote.empty() || !remote_ahead.empty())
        for (auto &entry : FetchRemote(remote, remote_ahead, context))
            if (std::find(remote.begin(), remote.end(), entry.first) != remote.end())
                loaded.emplace(entry.first, entry.second);
    return loaded;
}
std::vector<std::string> Store::ReadAhead(Reader &reader, Inode &inode, uint64_t offset, uint64_t length,
                                          uint64_t file_length) {
    constexpr uint64_t MAX_WINDOW = 64; // 4 MiB per stream
    constexpr size_t MAX_STREAMS = 16;
    auto first = offset / CHUNK, last = (offset + length - 1) / CHUNK;
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
        to = std::min(last + stream->window, (file_length - 1) / CHUNK);
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
    // One read snapshot spans the committed length and every extent/object lookup; the
    // overlay is stable while the shared inode lock is held.
    WithReader([&](Reader &reader) {
        uint64_t length = inode->size;
        uint64_t floor = inode->floor;
        if (!inode->dirty) {
            Query q(reader.db, reader.Statement("SELECT size FROM files WHERE id=?"));
            q.Int(1, file);
            if (!q.Step())
                throw std::runtime_error("[differential:not_found] File/root not found");
            length = uint64_t(q.Int(0));
            floor = NONE;
        }
        if (p >= length)
            return;
        n = std::min(n, length - p);
        auto first = p / CHUNK, last = (p + n - 1) / CHUNK;
        std::map<uint64_t, std::string> committed;
        {
            Timer lookup(Metric::ExtentLookup);
            Query q(reader.db, reader.Statement("SELECT chunk,object FROM extents WHERE file=? AND chunk>=? AND chunk<=?"));
            q.Int(1, file).Int(2, int64_t(first)).Int(3, int64_t(last));
            while (q.Step())
                committed.emplace(uint64_t(q.Int(0)), q.Text(1));
        }
        // With remote-only objects in the store, the range's objects are loaded together, so the
        // remote ones are fetched in one batch (coalesced range requests).
        std::map<std::string, SharedBytes> fetched;
        if (inode->pool.empty() && remote_objects) {
            std::vector<std::string> wanted;
            for (auto &entry : committed)
                if (entry.first < floor && !inode->chunks.count(entry.first))
                    wanted.push_back(entry.second);
            std::vector<std::string> ahead;
            if (remote && n >= CHUNK)
                ahead = ReadAhead(reader, *inode, p, n, length);
            fetched = LoadObjects(reader, wanted, ahead, remote);
        }
        auto out = static_cast<unsigned char *>(data);
        uint64_t done = 0;
        while (done < n) {
            auto pos = p + done, index = pos / CHUNK;
            auto take = std::min(n - done, CHUNK - pos % CHUNK);
            const unsigned char *source;
            SharedBytes held;
            auto dirty = inode->chunks.find(index);
            if (dirty != inode->chunks.end())
                source = dirty->second->data();
            else {
                auto found = committed.find(index);
                auto remote = found == committed.end() ? fetched.end() : fetched.find(found->second);
                held = index >= floor || found == committed.end() ? ZeroChunk()
                       : remote != fetched.end()                  ? remote->second
                                                                   : ObjectBytes(reader, found->second, inode->pool);
                source = held->data();
            }
            std::memcpy(out + done, source + pos % CHUNK, take);
            done += take;
        }
        result = n;
    });
    return result;
}

// ---------------------------------------------------------------------------------------
// Buffered mutation
void Store::ApplyWrite(Inode &inode, const unsigned char *in, uint64_t n, uint64_t p, bool journaled) {
    auto first = p / CHUNK, last = (p + n - 1) / CHUNK;
    std::map<uint64_t, std::shared_ptr<Bytes>> fresh;
    std::unique_ptr<PayloadFence> fence;
    if (!inode.pool.empty())
        fence = std::make_unique<PayloadFence>(directory, false);
    WithReader([&](Reader &reader) {
        Load(reader, inode);
        for (auto index = first; index <= last; ++index) {
            if (inode.chunks.count(index))
                continue;
            auto offset = index * CHUNK;
            bool covered = p <= offset && p + n >= offset + CHUNK;
            fresh.emplace(index, covered ? std::make_shared<Bytes>(CHUNK, 0)
                                         : std::make_shared<Bytes>(*CommittedChunk(reader, inode, index)));
        }
    });
    if (journaled) {
        std::lock_guard<std::mutex> guard(journal_mutex);
        if (!journal)
            journal = std::make_unique<Journal>((std::filesystem::path(directory) / "journal").string(), journal_id,
                                                permissions);
        for (uint64_t done = 0; done < n; done += JOURNAL_RECORD) {
            auto length = std::min(JOURNAL_RECORD, n - done);
            journal->Append(JournalRecord::WRITE, inode.id, p + done, in + done, length);
        }
    }
    dirty_bytes += fresh.size() * CHUNK;
    for (auto &entry : fresh)
        inode.chunks.emplace(entry.first, std::move(entry.second));
    uint64_t done = 0;
    while (done < n) {
        auto pos = p + done;
        auto take = std::min(n - done, CHUNK - pos % CHUNK);
        std::memcpy(inode.chunks.at(pos / CHUNK)->data() + pos % CHUNK, in + done, take);
        done += take;
    }
    if (!inode.dirty)
        inode.dirty = true;
    inode.size = std::max(inode.size, p + n);
}
void Store::ApplyTruncate(Inode &inode, uint64_t n, bool journaled) {
    std::shared_ptr<Bytes> tail;
    std::unique_ptr<PayloadFence> fence;
    if (!inode.pool.empty())
        fence = std::make_unique<PayloadFence>(directory, false);
    WithReader([&](Reader &reader) {
        Load(reader, inode);
        if (n < inode.size && n % CHUNK && !inode.chunks.count(n / CHUNK))
            tail = std::make_shared<Bytes>(*CommittedChunk(reader, inode, n / CHUNK));
    });
    if (journaled) {
        std::lock_guard<std::mutex> guard(journal_mutex);
        if (!journal)
            journal = std::make_unique<Journal>((std::filesystem::path(directory) / "journal").string(), journal_id,
                                                permissions);
        journal->Append(JournalRecord::TRUNCATE, inode.id, n, nullptr, 0);
    }
    if (n < inode.size) {
        auto keep = (n + CHUNK - 1) / CHUNK; // chunks at or above are discarded
        for (auto it = inode.chunks.lower_bound(keep); it != inode.chunks.end();) {
            dirty_bytes -= CHUNK;
            it = inode.chunks.erase(it);
        }
        if (n % CHUNK) {
            if (tail) {
                inode.chunks.emplace(n / CHUNK, tail);
                dirty_bytes += CHUNK;
            }
            auto &chunk = *inode.chunks.at(n / CHUNK);
            std::fill(chunk.begin() + int64_t(n % CHUNK), chunk.end(), 0);
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
    {
        std::shared_lock<std::shared_mutex> gate(write_gate);
        auto inode = GetInode(file);
        std::unique_lock<std::shared_mutex> lock(inode->lock);
        ApplyWrite(*inode, static_cast<const unsigned char *>(data), n, p, true);
    }
    if (dirty_bytes.load() > DIRTY_LIMIT)
        FlushImpl(false, {});
}
void Store::Truncate(int64_t file, uint64_t n) {
    CheckRange(0, n);
    std::shared_lock<std::shared_mutex> gate(write_gate);
    auto inode = GetInode(file);
    std::unique_lock<std::shared_mutex> lock(inode->lock);
    ApplyTruncate(*inode, n, true);
}
void Store::Sync() {
    Timer timer(Metric::LocalSync);
    FlushImpl(true, {});
}
void Store::Flush() { FlushImpl(false, {}); }
void Store::FlushImpl(bool durable, const std::function<void()> &in_transaction) {
    std::unique_lock<std::shared_mutex> gate(write_gate);
    std::vector<std::shared_ptr<Inode>> dirty;
    {
        std::lock_guard<std::mutex> guard(inodes_mutex);
        for (auto &entry : inodes)
            if (entry.second->dirty)
                dirty.push_back(entry.second);
    }
    if (dirty.empty() && !in_transaction) {
        if (durable && barrier_needed.exchange(false)) {
            // Earlier NORMAL commits become durable with the next synced WAL commit.
            std::lock_guard<std::recursive_mutex> guard(mutex);
            Transaction tx(db, "DIFFERENTIAL_STORE_CRASH");
            Execute(db, "INSERT INTO allocators VALUES('barrier',1) ON CONFLICT(name) DO UPDATE SET next=next+1");
            tx.Commit();
        }
        return;
    }
    // Hash outside every lock that other threads need; the gate keeps the overlay stable.
    struct Pending {
        Inode *inode;
        std::vector<std::pair<uint64_t, std::string>> chunks;
    };
    std::vector<Pending> pending;
    std::map<std::string, std::vector<std::pair<std::string, const Bytes *>>> external;
    std::map<std::string, std::pair<std::string, std::string>> pool_locations;
    for (auto &inode : dirty) {
        Pending item{inode.get(), {}};
        for (auto &chunk : inode->chunks) {
            auto hash = Hash(chunk.second->data(), chunk.second->size());
            item.chunks.emplace_back(chunk.first, hash);
            if (!inode->pool.empty()) {
                external[inode->pool].emplace_back(hash, chunk.second.get());
                pool_locations[inode->pool] = {inode->pool_path, inode->pool_repository};
            }
        }
        pending.push_back(std::move(item));
    }
    std::unique_ptr<PayloadFence> fence;
    if (!external.empty()) {
        // Shared until the mapping commits: collection cannot delete a just-published file.
        fence = std::make_unique<PayloadFence>(directory, false);
        for (auto &pool : external)
            LocalPool(pool_locations[pool.first].first, pool_locations[pool.first].second, pool.first, permissions)
                .PutBatch(pool.second);
    }
    {
        std::lock_guard<std::recursive_mutex> guard(mutex);
        Transaction tx(db, "DIFFERENTIAL_STORE_CRASH", durable);
        std::set<std::string> replaced;
        for (auto &item : pending) {
            auto &inode = *item.inode;
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
                auto &hash = chunk.second;
                if (inode.pool.empty()) {
                    Query present(db, "SELECT data IS NOT NULL FROM objects WHERE hash=?");
                    present.Text(1, hash);
                    if (!present.Step() || present.Int(0) == 0) {
                        auto &bytes = *inode.chunks.at(chunk.first);
                        Query(db, "INSERT INTO objects(hash,data) VALUES(?,?) ON CONFLICT(hash) DO UPDATE SET "
                                  "data=coalesce(objects.data,excluded.data)")
                            .Text(1, hash)
                            .Blob(2, bytes.data(), int(bytes.size()))
                            .Run();
                    }
                } else {
                    Query(db, "INSERT OR IGNORE INTO objects(hash,data) VALUES(?,NULL)").Text(1, hash).Run();
                    Query(db, "INSERT OR IGNORE INTO local_objects VALUES(?,?)").Text(1, hash).Text(2, inode.pool).Run();
                    Query(db, "DELETE FROM local_deletions WHERE pool=? AND hash=?")
                        .Text(1, inode.pool)
                        .Text(2, hash)
                        .Run();
                }
                Crash("object_inserted");
                Query previous(db, "SELECT object FROM extents WHERE file=? AND chunk=?");
                previous.Int(1, inode.id).Int(2, int64_t(chunk.first));
                if (previous.Step() && previous.Text(0) != hash)
                    replaced.insert(previous.Text(0));
                Query(db, "INSERT INTO extents(file,chunk,object) VALUES(?,?,?) ON CONFLICT(file,chunk) DO "
                          "UPDATE SET object=excluded.object")
                    .Int(1, inode.id)
                    .Int(2, int64_t(chunk.first))
                    .Text(3, hash)
                    .Run();
                Query(db, "INSERT INTO file_pages(file,page_no,page,dirty) VALUES(?,?,NULL,1) "
                          "ON CONFLICT(file,page_no) DO UPDATE SET dirty=1")
                    .Int(1, inode.id)
                    .Int(2, int64_t(chunk.first / PAGE_CHUNKS))
                    .Run();
                Crash("mapping_updated");
            }
            if (Query(db, "UPDATE files SET size=? WHERE id=?").Int(1, int64_t(inode.size)).Int(2, inode.id).Run() != 1)
                throw std::runtime_error("Invalid file handle");
        }
        Crash("length_updated");
        if (journal)
            Query(db, "INSERT INTO journal_state VALUES(?,?) ON CONFLICT(journal) DO UPDATE SET "
                      "committed=excluded.committed")
                .Text(1, journal_id)
                .Int(2, int64_t(journal->Last()))
                .Run();
        if (in_transaction)
            in_transaction();
        // Overwritten live chunks that nothing else references are reclaimed at once, so
        // repeated small commits do not accumulate garbage awaiting collection.
        for (auto &hash : replaced) {
            Query used(db, "SELECT EXISTS(SELECT 1 FROM extents WHERE object=?1) OR EXISTS(SELECT 1 FROM "
                           "page_entries WHERE object=?1)");
            used.Text(1, hash);
            if (used.Step() && used.Int(0) == 0)
                Query(db, "DELETE FROM objects WHERE hash=?").Text(1, hash).Run();
        }
        tx.Commit();
        barrier_needed = !durable;
    }
    for (auto &inode : dirty) {
        std::unique_lock<std::shared_mutex> lock(inode->lock);
        inode->chunks.clear();
        inode->dirty = false;
        inode->floor = NONE;
    }
    dirty_bytes = 0;
    {
        // Each WAL incarnation is a new inode: forget clean inodes nothing else references
        // (their state reloads from the authority on next use).
        std::lock_guard<std::mutex> guard(inodes_mutex);
        if (inodes.size() > 256)
            for (auto it = inodes.begin(); it != inodes.end();)
                it = it->second.use_count() == 1 && !it->second->dirty ? inodes.erase(it) : std::next(it);
    }
    std::lock_guard<std::mutex> guard(journal_mutex);
    if (journal)
        journal->Reset();
}

// ---------------------------------------------------------------------------------------
// Namespace
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
            Query(db, "DELETE FROM extents WHERE file=?").Int(1, file.id).Run();
            Query(db, "DELETE FROM file_pages WHERE file=?").Int(1, file.id).Run();
            Query(db, "UPDATE files SET size=0 WHERE id=?").Int(1, file.id).Run();
            file.size = 0;
        }
        if (!pin.empty()) {
            if (Query(db, "UPDATE pins SET file=? WHERE id=?").Int(1, file.id).Text(2, pin).Run() != 1)
                throw std::runtime_error("File pin missing");
        }
    };
    if (truncate) {
        // Commit any overlay for this inode first, then replace its contents in one transaction.
        FlushImpl(true, body);
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
    // Inode numbers are never reused: a dead process's journal can name an unlinked inode.
    Query next(db, "UPDATE allocators SET next=next+1 WHERE name='file' RETURNING next-1");
    if (!next.Step())
        throw std::runtime_error("[differential:corrupt_data] File allocator missing");
    auto id = next.Int(0);
    while (next.Step()) {
    }
    auto insert = Query(db, "INSERT INTO files(id,name,size) VALUES(?,?,?)");
    insert.Int(1, id).Int(3, int64_t(size));
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
    FlushImpl(false, [&] {
        changed = Query(db, "UPDATE files SET name=NULL WHERE name=?").Text(1, name).Run() != 0;
        Crash("namespace_updated");
    });
    return changed;
}
void Store::Rename(const std::string &source, const std::string &target) {
    if (source == target)
        return;
    Timer timer(Metric::NamespaceUpdate);
    FlushImpl(false, [&] {
        Query find(db, "SELECT id FROM files WHERE name=?");
        find.Text(1, source);
        if (!find.Step())
            throw std::runtime_error("[differential:not_found] Rename source missing");
        auto id = find.Int(0);
        Query(db, "UPDATE files SET name=NULL WHERE name=?").Text(1, target).Run();
        Query(db, "UPDATE files SET name=? WHERE id=?").Text(1, target).Int(2, id).Run();
        Crash("namespace_updated");
    });
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
Root Store::SealTx(int64_t file, const std::string &pin) {
    Query size_query(db, "SELECT size FROM files WHERE id=?");
    size_query.Int(1, file);
    if (!size_query.Step())
        throw std::runtime_error("Invalid file handle");
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
    FlushImpl(durable, [&] { result = SealTx(file, pin); });
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
    Query(db, "INSERT INTO extents SELECT ?,r.page_no*256+e.slot,e.object FROM root_pages r JOIN page_entries e "
              "ON e.page=r.page WHERE r.root=?")
        .Int(1, file)
        .Text(2, root)
        .Run();
    Query(db, "INSERT INTO file_pages SELECT ?,page_no,page,0 FROM root_pages WHERE root=?")
        .Int(1, file)
        .Text(2, root)
        .Run();
}
uint64_t Store::RootSizeTx(const std::string &root) { return VerifyRoot(db, root); }
std::string Store::ImportRootTx(uint64_t size, const std::vector<ChunkRef> &chunks) {
    CheckRange(0, size);
    // Pages and root are hashed exactly as SealTx hashes a file's map.
    std::map<uint64_t, std::vector<const ChunkRef *>> pages;
    uint64_t next = 0;
    for (auto &ref : chunks) {
        if (ref.chunk < next || ref.chunk * CHUNK >= size || ref.object.size() != 64)
            throw std::runtime_error("[differential:corrupt_data] Corrupt imported chunk map");
        next = ref.chunk + 1;
        pages[ref.chunk / PAGE_CHUNKS].push_back(&ref);
        // Objects without a local copy are remote-only; local copies are kept as they are.
        Query(db, "INSERT OR IGNORE INTO objects(hash,data) VALUES(?,NULL)").Text(1, ref.object).Run();
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
    if (p >= info.size)
        return 0;
    n = std::min(n, info.size - p);
    auto first = p / CHUNK, last = (p + n - 1) / CHUNK;
    std::map<uint64_t, std::string> map;
    {
        Timer lookup(Metric::ExtentLookup);
        Query q(reader.db, reader.Statement("SELECT r.page_no*256+e.slot,e.object FROM root_pages r JOIN page_entries "
                                            "e ON e.page=r.page WHERE r.root=? AND r.page_no>=? AND r.page_no<=?"));
        q.Text(1, root).Int(2, int64_t(first / PAGE_CHUNKS)).Int(3, int64_t(last / PAGE_CHUNKS));
        while (q.Step()) {
            auto chunk = uint64_t(q.Int(0));
            if (chunk >= first && chunk <= last)
                map.emplace(chunk, q.Text(1));
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
        auto pos = p + done, index = pos / CHUNK;
        auto take = std::min(n - done, CHUNK - pos % CHUNK);
        auto found = map.find(index);
        auto remote = found == map.end() ? fetched.end() : fetched.find(found->second);
        auto bytes = found == map.end()      ? ZeroChunk()
                     : remote != fetched.end() ? remote->second
                                               : ObjectBytes(reader, found->second, pool);
        std::memcpy(out + done, bytes->data() + pos % CHUNK, take);
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
                throw std::runtime_error("[differential:corrupt_data] Stale live manifest page");
        }
        Query objects(db, "SELECT hash,data FROM objects WHERE data IS NOT NULL");
        while (objects.Step()) {
            auto ptr = sqlite3_column_blob(objects.s, 1);
            auto n = sqlite3_column_bytes(objects.s, 1);
            if (n != int(CHUNK) || Hash(ptr, size_t(n)) != objects.Text(0))
                throw std::runtime_error("[differential:corrupt_data] Corrupt immutable object");
        }
        Query external(db, "SELECT o.hash,o.pool,p.path,p.repository_id FROM local_objects o JOIN local_pools p ON "
                           "p.id=o.pool");
        while (external.Step())
            LocalPool(external.Text(2), external.Text(3), external.Text(1), permissions).Get(external.Text(0));
        Query missing(db, "SELECT 1 FROM objects o WHERE data IS NULL AND NOT EXISTS(SELECT 1 FROM local_objects "
                          "l WHERE l.hash=o.hash) AND NOT EXISTS(SELECT 1 FROM remote_objects r WHERE r.hash=o.hash) "
                          "LIMIT 1");
        if (missing.Step())
            throw std::runtime_error("[differential:corrupt_data] Object has no durable location");
    });
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
