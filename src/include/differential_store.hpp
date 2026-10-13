#pragma once
#include "differential_permissions.hpp"
// DuckDB-independent durable byte store. All offsets are bytes, never DuckDB block IDs.
//
// Write model: native writes are applied to an in-process overlay of whole
// 64 KiB chunks and appended to an unsynced per-process journal, so a completed write
// survives process death exactly like a write into the kernel page cache. Sync() is the
// durability boundary: one SQLite FULL transaction publishes every buffered chunk,
// mapping and length. Memory pressure commits with synchronous=NORMAL instead.
#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <thread>
#include <vector>
struct sqlite3;
struct sqlite3_stmt;
namespace differential {
using Bytes = std::vector<unsigned char>;
using SharedBytes = std::shared_ptr<const Bytes>;
struct File {
    int64_t id;
    uint64_t size;
};
struct Root {
    std::string id;
    uint64_t size;
};
struct ChunkRef {
    uint64_t chunk;
    std::string object;
};
// Verified bytes of objects whose durable copy is remote: an immutable S3 export the store
// references without a local copy (for example the base snapshot of a shared writer, read on
// demand). Returns every requested identity, or throws. `context` is the reader's (Read's
// `remote` argument, e.g. its caches; null for other reads).
using RemoteFetch =
    std::function<std::map<std::string, SharedBytes>(const std::vector<std::string> &hashes, const void *context)>;
class Journal;
// A pooled read-only SQLite connection with cached statements (WAL concurrent readers).
class Reader {
  public:
    explicit Reader(const std::string &path);
    ~Reader();
    Reader(const Reader &) = delete;
    sqlite3 *db = nullptr;
    sqlite3_stmt *Statement(const char *sql);
    void Begin();

  private:
    std::map<const char *, sqlite3_stmt *> statements;
};
class Store {
  public:
    static constexpr uint64_t CHUNK = 65536;
    // The format isolates the three 4 KiB native headers. Objects still have exactly
    // CHUNK bytes; the unused suffix of object 0 is zero. This byte geometry applies
    // to every file (including WALs), independent of names or native block IDs.
    static constexpr uint64_t HEADER = 12288;
    static constexpr uint64_t ChunkIndex(uint64_t offset) {
        return offset < HEADER ? 0 : 1 + (offset - HEADER) / CHUNK;
    }
    static constexpr uint64_t ChunkOffset(uint64_t index) {
        return index == 0 ? 0 : HEADER + (index - 1) * CHUNK;
    }
    static constexpr uint64_t ChunkLength(uint64_t index) { return index == 0 ? HEADER : CHUNK; }
    static constexpr uint64_t ChunkPosition(uint64_t offset) { return offset - ChunkOffset(ChunkIndex(offset)); }
    static constexpr uint64_t ChunkCount(uint64_t size) { return size == 0 ? 0 : ChunkIndex(size - 1) + 1; }
    static constexpr uint64_t PAGE_CHUNKS = 256;                // manifest page: 256 chunk slots
    static constexpr uint64_t DIRTY_LIMIT = 64ull * 1024 * 1024; // overlay bound before a NORMAL commit
    static constexpr int FORMAT = 1;
    // File modes for everything this repository creates (private, or shared with its group).
    Permissions permissions;
    // `share` applies only when this call creates the repository (differential_repository_shared);
    // an existing repository keeps the mode recorded at its creation.
    explicit Store(const std::string &absolute_directory, bool share = false);
    ~Store();
    Store(const Store &) = delete;
    Store &operator=(const Store &) = delete;
    bool Exists(const std::string &name);
    File Open(const std::string &name, bool create, bool exclusive, bool truncate,
              const std::string &pin = "", const std::string &pool = "");
    int64_t FileId(const std::string &name);
    uint64_t Size(int64_t file);
    // `remote`: the caller's context for remote-only objects (passed to the remote fetch). With it,
    // sequential reads also fetch the following remote-only chunks ahead (read-ahead), which the
    // fetch is expected to keep in the caller's caches.
    uint64_t Read(int64_t file, void *data, uint64_t length, uint64_t offset, const void *remote = nullptr);
    void Write(int64_t file, const void *data, uint64_t length, uint64_t offset);
    void Truncate(int64_t file, uint64_t length);
    // Durability boundary for every buffered write of this store instance.
    void Sync();
    // Commit buffered writes without a sync (survives process death, not power loss).
    void Flush();
    bool Remove(const std::string &name);
    void Rename(const std::string &source, const std::string &target);
    // durable=false publishes with WAL synchronous=NORMAL: correct when a later FULL commit
    // (for example the snapshot publication that references the root) follows.
    Root Seal(int64_t file, const std::string &pin = "", bool durable = true);
    std::string Generation(int64_t file);
    uint64_t ReadRoot(const std::string &root, void *data, uint64_t length, uint64_t offset,
                      const std::string &pool = "");
    File Fork(const std::string &root, const std::string &name, const std::string &pool = "");
    bool HasRoot(const std::string &root);
    uint64_t RootSize(const std::string &root);
    // Ordered non-hole chunks of an immutable root in [first, first+count).
    std::vector<ChunkRef> RootChunks(const std::string &root, uint64_t first, uint64_t count);
    uint64_t ObjectCount();
    SharedBytes Object(const std::string &hash);
    void Verify();
    void ReplayJournals();
    // Forget the process-wide verified chunk cache (tests that corrupt stored bytes).
    static void DropCaches();
    // Run fn with a pooled read connection inside one read transaction.
    void WithReader(const std::function<void(Reader &)> &fn);
    // Reads of remote-only objects go through `fetch` (installed by the extension; without it they
    // are unavailable).
    void SetRemoteFetch(RemoteFetch fetch);
    const std::string directory;

    // Authority-transaction helpers. The caller holds `mutex` and an open transaction.
    int64_t CreateFileTx(const std::string &name, uint64_t size, const std::string &pool);
    void InstallRootTx(int64_t file, const std::string &root);
    uint64_t RootSizeTx(const std::string &root);
    // The immutable root of a `size`-byte file whose non-hole chunks are `chunks` (ascending), built
    // from its map alone: objects without a local copy are recorded as remote-only. Returns the
    // root identity (as Seal would compute it); no bytes are read.
    std::string ImportRootTx(uint64_t size, const std::vector<ChunkRef> &chunks);

  private:
    friend class Control;
    friend struct Pin;
    struct Inode;
    sqlite3 *db = nullptr;
    std::recursive_mutex mutex; // authority write connection
    std::mutex readers_mutex;
    std::vector<std::unique_ptr<Reader>> readers;
    std::mutex inodes_mutex;
    std::map<int64_t, std::shared_ptr<Inode>> inodes;
    std::shared_mutex write_gate; // writes shared; flush exclusive
    std::atomic<uint64_t> dirty_bytes{0};
    std::atomic<bool> barrier_needed{false};
    std::unique_ptr<Journal> journal;
    std::string journal_id;
    std::mutex remote_mutex;
    RemoteFetch remote_fetch;
    std::atomic<bool> remote_objects{false}; // some objects may be remote-only: batch their reads
    std::mutex journal_mutex;
    // SQLite WAL checkpoints run on a dedicated connection off the commit path.
    std::mutex checkpoint_mutex;
    std::condition_variable checkpoint_signal;
    bool checkpoint_wanted = false, checkpoint_stopping = false;
    std::thread checkpointer;
    static int WalHook(void *, sqlite3 *, const char *, int pages);
    void RequestCheckpoint();
    std::unique_ptr<Reader> AcquireReader();
    void ReleaseReader(std::unique_ptr<Reader>);
    std::shared_ptr<Inode> GetInode(int64_t file);
    void Load(Reader &, Inode &);
    SharedBytes CommittedChunk(Reader &, Inode &, uint64_t chunk);
    SharedBytes ObjectBytes(Reader &, const std::string &hash, const std::string &pool);
    SharedBytes InlineObject(sqlite3_stmt *row, const std::string &hash);
    // `ahead`: fetched in the same batch for later reads; kept by the fetch's caches, not here.
    std::map<std::string, SharedBytes> FetchRemote(const std::vector<std::string> &hashes,
                                                   const std::vector<std::string> &ahead = {},
                                                   const void *context = nullptr);
    // Objects of the default pool among `hashes`, with the remote-only ones (and the remote-only,
    // uncached ones among `ahead`) fetched in one batch.
    std::map<std::string, SharedBytes> LoadObjects(Reader &, const std::vector<std::string> &hashes,
                                                   const std::vector<std::string> &ahead = {},
                                                   const void *context = nullptr);
    // Read-ahead of a live file's remote-only chunks: the objects of the chunks after [first, last]
    // that a sequential stream of reads will want next.
    std::vector<std::string> ReadAhead(Reader &, Inode &, uint64_t offset, uint64_t length, uint64_t file_length);
    uint64_t ReadRootImpl(Reader &, const std::string &root, void *data, uint64_t n, uint64_t p,
                          const std::string &pool);
    void ApplyWrite(Inode &, const unsigned char *data, uint64_t n, uint64_t p, bool journaled);
    void ApplyTruncate(Inode &, uint64_t n, bool journaled);
    void FlushImpl(bool durable, const std::function<void()> &in_transaction);
    Root SealTx(int64_t file, const std::string &pin);
    std::string PageHashTx(int64_t file, uint64_t page_no);
    std::string RootHashTx(int64_t file, uint64_t size);
    std::string FilePoolTx(int64_t file);
    void CreateSchema(bool shared);
    void DrainLocalDeletions(uint64_t limit);
    uint64_t SweepLocalOrphans(uint64_t limit, bool dry_run = false);
};
} // namespace differential
