#pragma once
#include "differential_permissions.hpp"
// DuckDB-independent durable byte store. All offsets are bytes, never DuckDB block IDs.
//
// Write model: native writes are applied to an in-process overlay and appended to the file
// group's unsynced segment (differential_segment.hpp), so a completed write survives process
// death exactly like a write into the kernel page cache. Files are published in groups: a
// file and its sidecars ("name" and "name.<suffix>", e.g. a database and its WAL) share one
// write gate, segment and overlay bound. Sync(file) is the durability boundary of the file's
// group: its segment is synced, then one SQLite FULL transaction publishes the mappings (into
// the segment) and lengths, while other groups keep writing. Overlay pressure publishes in the
// background with synchronous=NORMAL instead.
//
// Two file kinds: chunked files (main database files, and every file of an external pool) map
// 256 KiB content-addressed chunks, which snapshots and exports share; log files (sidecars such
// as WALs) map byte ranges onto the segment records that wrote them, so an append stores only
// the appended bytes.
#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <set>
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
class Segment;
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
    // One native 256 KiB block per chunk: the format isolates the three 4 KiB native headers, so
    // chunk n >= 1 is native block n - 1. Objects have exactly CHUNK bytes; the unused suffix of
    // object 0 is zero. The geometry applies to every chunked file, independent of names.
    static constexpr uint64_t CHUNK = 262144;
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
    // Overlay of one group: published in the background beyond BACKGROUND_LIMIT, by the writer itself
    // (backpressure) beyond DIRTY_LIMIT; all groups together beyond TOTAL_DIRTY_LIMIT.
    static constexpr uint64_t BACKGROUND_LIMIT = 16ull * 1024 * 1024;
    static constexpr uint64_t DIRTY_LIMIT = 64ull * 1024 * 1024;
    static constexpr uint64_t TOTAL_DIRTY_LIMIT = 4 * DIRTY_LIMIT;
    // An active segment is retired at the first publication beyond this size.
    static constexpr uint64_t SEGMENT_LIMIT = 64ull * 1024 * 1024;
    // ... or, if no publication references it, beyond this size; it is then deleted at once.
    static constexpr uint64_t UNREFERENCED_SEGMENT_LIMIT = 4ull * 1024 * 1024;
    static constexpr int FORMAT = 2;
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
    // ... of the buffered writes of `file`'s group (the file and its sidecars).
    void Sync(int64_t file);
    // Commit buffered writes without a sync (survives process death, not power loss): all of them,
    // or those of `file`'s group.
    void Flush();
    void Flush(int64_t file);
    // Group commit (differential_group_commit, process-wide, default on): concurrent durable syncs of
    // different file groups share one FULL transaction. Off, each sync commits on its own.
    static void SetGroupCommit(bool enabled);
    static bool GroupCommit();
    // New object data one shared transaction takes at most; a larger sync commits on its own.
    static constexpr uint64_t GROUP_COMMIT_BYTES = 16ull * 1024 * 1024;
    // The publication group of a file name: the name up to its first '.'.
    static std::string GroupKey(const std::string &name) { return name.substr(0, name.find('.')); }
    bool Remove(const std::string &name);
    void Rename(const std::string &source, const std::string &target);
    // durable=false publishes with WAL synchronous=NORMAL: correct when a later FULL commit
    // (for example the snapshot publication that references the root) follows.
    Root Seal(int64_t file, const std::string &pin = "", bool durable = true);
    // An immutable root of the file's current published state that is not its generation (not a
    // checkpoint image): the base of a shipping chain. `sequence` receives the shipping sequence the
    // root includes. Published with synchronous=NORMAL.
    Root Capture(int64_t file, const std::string &pin, uint64_t &sequence);
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
    // Replays the segments of dead processes (their unpublished writes) and retires them.
    void ReplayJournals();
    // Segment collection, a bounded step: deletes retired segments nothing references and rewrites
    // up to `budget` bytes of sparse ones. Takes `mutex` per transaction (the caller must not hold
    // an open transaction). Returns the number of segments deleted.
    uint64_t CollectSegments(uint64_t budget);
    // Store verified copies of remote-only objects locally (inside the caller's transaction, with
    // `mutex` held).
    void MaterializeObjectsTx(const std::map<std::string, SharedBytes> &objects);
    // Continuous WAL shipping. Publications of tracked file groups record what changed (chunks of
    // chunked files, the lowest changed byte of log files, truncations) in the same transaction,
    // numbered by a store-wide shipping sequence; ReadShipDelta reads a group's unshipped changes in
    // one snapshot and AckShipped forgets them once shipped.
    void SetShipping(const std::string &group_key, bool enabled);
    bool Shipping(const std::string &group_key);
    struct ShipFile {
        std::string name;
        int64_t id = 0;
        uint64_t size = 0;
        bool log = false;
    };
    // Receives a delta, possibly several times (a read that raced with segment collection restarts
    // after Reset).
    struct ShipSink {
        virtual ~ShipSink() = default;
        virtual void Reset() = 0;
        // Every named file of the group (a file missing here was removed or renamed).
        virtual void Group(const std::vector<ShipFile> &files) = 0;
        // A file of the group: `full` when it is sent whole, else only its changes.
        virtual void File(const ShipFile &file, bool full) = 0;
        // Chunked files: a chunk's object (null bytes: a hole).
        virtual void Chunk(const ShipFile &file, uint64_t chunk, const std::string &hash, const SharedBytes &bytes) = 0;
        // Log files: the bytes from `offset` to the file's end.
        virtual void Tail(const ShipFile &file, uint64_t offset, const std::vector<unsigned char> &bytes) = 0;
    };
    // The group's named files and their unshipped changes, as published; files whose identity is not
    // `known` (name -> file id) are sent whole. Returns the shipping sequence the delta includes.
    uint64_t ReadShipDelta(const std::string &group_key, const std::map<std::string, int64_t> &known, ShipSink &sink);
    // Forget the group's changes up to `sequence` (they are shipped).
    void AckShipped(const std::string &group_key, uint64_t sequence);
    uint64_t ShipSequence();
    // Forget the process-wide verified chunk cache and every store's cached locations and maps
    // (tests that corrupt stored bytes).
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
    struct Group;
    struct Prepared;
    struct CommitRequest;
    struct Location;
    struct RangeRow;
    struct SegmentFiles;
    sqlite3 *db = nullptr;
    std::recursive_mutex mutex; // authority write connection
    std::mutex readers_mutex;
    std::vector<std::unique_ptr<Reader>> readers;
    std::mutex inodes_mutex;
    std::map<int64_t, std::shared_ptr<Inode>> inodes;
    std::mutex groups_mutex;
    std::map<std::string, std::shared_ptr<Group>> groups;
    std::atomic<uint64_t> dirty_total{0}; // overlay bytes of every group
    std::atomic<bool> barrier_needed{false};
    // Background publication of groups beyond BACKGROUND_LIMIT (A NORMAL commit per group).
    std::mutex publisher_mutex;
    std::condition_variable publisher_signal;
    std::vector<std::shared_ptr<Group>> publisher_queue;
    bool publisher_stopping = false;
    std::thread publisher;
    void SchedulePublication(const std::shared_ptr<Group> &group);
    // Read access to segment files (cached descriptors) and cached object locations.
    std::unique_ptr<SegmentFiles> segment_files;
    // Group commit: durable syncs waiting for a shared transaction, and whether one is committing.
    std::mutex commit_mutex;
    std::condition_variable commit_signal;
    std::vector<CommitRequest *> commit_queue;
    bool committing = false;
    std::mutex remote_mutex;
    RemoteFetch remote_fetch;
    std::atomic<bool> remote_objects{false}; // some objects may be remote-only: batch their reads
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
    std::shared_ptr<Group> GetGroup(const std::string &key);
    std::vector<std::shared_ptr<Group>> GroupsOf(const std::vector<std::string> &names);
    std::mutex shipping_mutex;
    std::set<std::string> shipping_groups;
    // Record a published change of a tracked group's file (inside the transaction).
    void TrackShipTx(int64_t file, const std::vector<uint64_t> &chunks, uint64_t low, uint64_t &sequence);
    std::vector<ShipFile> GroupFiles(sqlite3 *connection, const std::string &group_key);
    void Load(Reader &, Inode &);
    // The committed object of a chunked file's chunk ("" for a hole), through the inode's map cache.
    std::string CommittedObject(Reader &, Inode &, int64_t version, uint64_t chunk);
    SharedBytes CommittedChunk(Reader &, Inode &, uint64_t chunk);
    // Verified bytes of an object (through the process-wide chunk cache).
    SharedBytes ObjectBytes(Reader &, const std::string &hash, const std::string &pool);
    // Where an object's local copy is; false for a remote-only object.
    bool FindObject(Reader &, const std::string &hash, Location &location);
    // Reads a local object's verified bytes into `out` (CHUNK bytes).
    void ReadObject(const std::string &hash, const Location &location, unsigned char *out);
    // Copies `chunk`'s bytes [position, position+n) of committed object `hash` to `out`.
    void CopyObject(Reader &, const std::string &hash, const std::string &pool, uint64_t chunk, uint64_t position,
                    uint64_t n, unsigned char *out, const std::map<std::string, SharedBytes> &fetched);
    // Log files: committed bytes [p, p+n) into `out` (holes are zero), below `floor`.
    void ReadRanges(Reader &, int64_t file, uint64_t p, uint64_t n, uint64_t floor, unsigned char *out);
    uint64_t ReadImpl(Reader &, Inode &, unsigned char *out, uint64_t n, uint64_t p, const void *remote);
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
    // Publish the overlays of `scope` (nullptr: every group) in one transaction, with `in_transaction`.
    void FlushImpl(bool durable, const std::function<void()> &in_transaction,
                   const std::vector<std::shared_ptr<Group>> *scope);
    // The phases of a publication. Prepare locks the groups' write gates (until the Prepared is
    // destroyed, in the same thread), collects their dirty inodes, hashes their chunks and makes
    // external payload durable. Apply writes its mappings inside the caller's transaction (with
    // `mutex` held) and collects overwritten objects; Reclaim deletes those nothing references any
    // more; Finish clears the published overlays and journals after the commit.
    std::unique_ptr<Prepared> Prepare(const std::vector<std::shared_ptr<Group>> *scope);
    void Apply(Prepared &, std::set<std::string> &replaced);
    void Reclaim(const std::set<std::string> &replaced);
    void Finish(Prepared &);
    // A durable sync through the group commit queue.
    void CommitShared(const std::vector<std::shared_ptr<Group>> &scope);
    // Commit `batch` (leader of the queue): in one transaction, else one sync at a time. Never throws:
    // failures are stored in the requests.
    void CommitBatch(const std::vector<CommitRequest *> &batch);
    // Append a record to the group's active segment (created on first use); returns its payload's
    // position. The caller holds the group's gate.
    uint64_t AppendRecord(Group &, uint32_t type, int64_t file, uint64_t offset, const unsigned char *data,
                          uint64_t length, uint32_t crc);
    // Record the group's active segment as published up to its end (inside the transaction); a
    // segment beyond SEGMENT_LIMIT is marked retired and moved once the transaction commits.
    void RecordSegmentTx(Group &, bool &retire);
    // Note that a publication references segment `id` of the group (if it is the active one).
    void MarkReferenced(Group &, int64_t id);
    void ApplyLogWrite(Inode &, const unsigned char *data, uint64_t n, uint64_t p, int64_t segment,
                       uint64_t position, uint32_t crc);
    void CutRangesTx(int64_t file, uint64_t start, uint64_t end);
    Root SealTx(int64_t file, const std::string &pin, bool generation = true);
    std::string PageHashTx(int64_t file, uint64_t page_no);
    std::string RootHashTx(int64_t file, uint64_t size);
    std::string FilePoolTx(int64_t file);
    void CreateSchema(bool shared);
    void DrainLocalDeletions(uint64_t limit);
    // Deletes one retired segment nothing references (row first, then the file).
    bool DeleteSegment(int64_t id);
    // Rewrites the live payload of retired segment `id` into a new segment; returns bytes copied.
    uint64_t CompactSegment(int64_t id);
    uint64_t SweepLocalOrphans(uint64_t limit, bool dry_run = false);
};
} // namespace differential
