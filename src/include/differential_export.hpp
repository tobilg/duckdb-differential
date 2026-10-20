#pragma once
#include "differential_control.hpp"
#include <list>
#include <map>
#include <mutex>

namespace differential {
// Remote snapshot descriptor ("differential-export-v2"). Index pages carry a table of the
// immutable packs they reference and map each 256 KiB chunk to (identity, pack, slot).
struct Descriptor {
    // "duckdb-<version>" of the writer. Recorded, not enforced: DuckDB itself decides at open
    // whether it can read the stored native file.
    std::string engine;
    std::string database, branch, root, pool;
    int64_t snapshot = 0, published_at = 0;
    uint64_t size = 0;
    std::vector<std::string> pages;
    std::vector<uint64_t> page_sizes; // exact index object sizes
    // The first index page itself, so ATTACH reads the file header without another round trip.
    std::vector<unsigned char> first_index;
};
// Export a snapshot (and, for a shared database, every earlier unexported one first). For a shared
// database the snapshot can be renumbered above numbers other writers published meanwhile:
// `snapshot` then holds its final number.
std::string ExportSnapshot(Control &, const std::string &database, int64_t &snapshot,
                           std::function<bool()> cancelled = {});
std::vector<std::pair<std::string, uint64_t>> ExportMetrics();
// Export settings, process-wide (the last value set applies): verify each upload by reading it back
// (differential_export_verification = 'read_back'; default: the service verifies its checksum), and
// upload from memory instead of staging files (differential_export_staging = 'memory').
void SetExportVerification(bool read_back);
void SetExportStaging(bool in_memory);
bool ExportReadsBack();
bool ExportStagesInMemory();

// Branch heads: a small mutable object per exported branch, dbs/<database uuid>/branches/<branch
// name>, naming the latest exported snapshot in the branch's history and its descriptor, so remote
// readers can attach "the latest of sales/main" without the writer's repository.
struct Head {
    std::string database, database_name, branch, branch_name, descriptor;
    std::string write_id; // random per write: tells this repository's writes from any other writer's
    int64_t snapshot = 0, published_at = 0;
    // S3 object of the head, and the path readers ATTACH it by (differential:s3://<prefix>/heads/…).
    std::string Key() const { return "dbs/" + database + "/branches/" + branch_name; }
    std::string AttachPath() const { return "heads/" + database_name + "/" + database + "/" + branch_name; }
};
// Publish the heads of every active branch of `database` (no-op without S3). Serialized per
// database within the repository; never moves a head to an older snapshot of the same branch.
// A head is only overwritten while it is still the version this repository last wrote (S3
// If-Match); a head written by anyone else (e.g. a copy of this repository on another host)
// raises [differential:head_conflict]. `claim` takes such heads over deliberately.
// Shared databases: only heads this repository advanced are written, each on the version it
// last wrote or adopted (its base); a head another writer moved meanwhile raises
// [differential:branch_conflict] after the other heads were written. `branch` (a branch UUID)
// limits a shared database's update to that branch's head.
void PublishHeads(Control &, const std::string &database, bool claim = false, const std::string &branch = "");
// Recovery after a split brain (e.g. this repository restored on another host): number new
// snapshots above the newest one published in S3, abandon pending exports whose numbers another
// writer published (returned), and take the database's branch heads over.
std::vector<int64_t> ClaimDatabase(Control &, const std::string &database);
// Shared databases (WRITERS 'shared'): several repositories write one database through S3. Its
// identity is derived from its name, so every repository that attaches it agrees on it.
std::string SharedDatabaseID(const std::string &name);
// Prepare an attachment of a branch of a shared database (`provider`: its S3 location): join or
// create the database locally (`create`: a new database may be created), adopt the branch's S3
// head when the local branch is missing or has no local changes, and return the local branch.
// Nothing is downloaded: the adopted snapshot is read from S3 on demand. A writer attachment
// (`writable`) needs the branch free of local writers.
Branch PrepareSharedBranch(Control &, const std::string &provider, const std::string &name, const std::string &branch,
                           bool create, bool writable);
// Make the local branch of a shared database its S3 head again, replacing local state (the caller
// holds the branch's writer lock and has preserved what it keeps). Clears the branch's conflict.
Branch FollowRemoteHead(Control &, const std::string &database, const std::string &branch);
// Name of the branch that keeps a conflicting snapshot: "<branch>-conflict-<snapshot>" (snapshot
// numbers are unique in a database, so writers never collide on it).
std::string ConflictBranchName(const std::string &branch, int64_t snapshot);
// How this process writes a shared database's branch (ATTACH options of its writable attachment).
struct SharedWriting {
    std::string on_conflict = "fail"; // or "branch": a conflicting snapshot becomes a branch of its own
    bool publish_on_checkpoint = true; // each completed DuckDB checkpoint is a snapshot published in the background
    bool publish_on_detach = false;    // publish synchronously when the branch's main file closes
    std::string on_busy = "warn";      // another live writer at ATTACH: "warn", "error" or "ignore"
};
// Advisory "being written" marker of one writer session of a shared branch:
// dbs/<database uuid>/writers/<branch name>/<writer id>, renewed while the session lasts and
// deleted when it ends. Correctness never depends on it (heads are written conditionally).
struct WriterMarker {
    std::string database, branch_name, writer, repository, host;
    int64_t process = 0, started_at = 0, renewed_at = 0, lease = 0; // microseconds
    bool Live(int64_t now) const { return now < renewed_at + lease; }
};
// Writable attachment of a shared branch (the caller holds the branch's writer lock): look for
// other live writers, remove expired markers and this repository's own leftovers, then write
// this session's marker and keep it renewed. Returns the other live writers; with on_busy
// "error" they refuse the attachment ([differential:writer_busy]) before a marker is written.
std::vector<WriterMarker> AnnounceWriter(const std::shared_ptr<Control> &, const Branch &, const std::string &on_busy);
// End the session's marker (best effort).
void WithdrawWriter(Control &, const std::string &branch);
// Every marker of a database in S3, with whether it belongs to this process.
std::vector<std::pair<WriterMarker, bool>> ListWriters(Control &, const std::string &database);
std::string DescribeWriters(const std::vector<WriterMarker> &);
void SetSharedWriting(Control &, const std::string &branch, SharedWriting);
SharedWriting GetSharedWriting(Control &, const std::string &branch);
// Publish a shared branch: export the database's unexported snapshots (in order), then update the
// branch head. A diverged branch publishes nothing unless its policy forks. Returns the name of a
// branch created for a conflicting snapshot (ON_CONFLICT 'branch'), else "".
std::string PublishShared(Control &, const std::string &database, const std::string &branch,
                          std::function<bool()> cancelled = {});
// ON_CONFLICT 'branch': publish the branch's conflicting snapshot as "<branch>-conflict-<snapshot>"
// and record the fork, so the branch follows S3 again at its next unchanged ATTACH.
std::string ForkConflict(Control &, const std::string &database, const std::string &branch);
// Run PublishShared on the process's background publisher (one worker; requests coalesce per
// repository and branch). Failures are counted and kept as the last publication error.
void SchedulePublication(const std::shared_ptr<Control> &, const std::string &database, const std::string &branch);
// PublishShared for a closing main file (PUBLISH_ON_DETACH): errors are recorded, not thrown.
void PublishOnClose(Control &, const std::string &database, const std::string &branch);
std::string LastPublicationError();
// While alive, checkpoints of the branch create no automatic snapshot (an explicit one follows).
struct ExplicitSnapshot {
    ExplicitSnapshot(Control &, const std::string &branch);
    ~ExplicitSnapshot();
    std::string key;
};
bool ExplicitSnapshotInProgress(Control &, const std::string &branch);
// Shared databases: cache the commit records (and expiries) published in S3 that are not cached
// yet: listings, then one GET per new commit record.
// `throttled`: skip when this process synced the database in the last 5 seconds. Incremental: commit
// records are listed newest first down to a cached one older than 15 minutes, expiries from the
// time-ordered log (dbs/<uuid>/expiries/) since the last read; `full` (and at most a day after the last
// full listing) lists every commit and expiry record.
void SyncHistory(Control &, const std::string &database, bool throttled = false, bool full = false);
// SyncHistory on the background publisher (after a writable ATTACH), errors recorded.
void ScheduleHistorySync(const std::shared_ptr<Control> &, const std::string &database);
// Shared databases: expire published snapshots for every writer (create-only records at
// dbs/<uuid>/expired/<9999999999 - n>). A snapshot that is a branch head in S3 cannot expire.
void ExpireShared(Control &, const std::string &database, const std::vector<int64_t> &snapshots);
// Any S3 database: record the expiry of exported snapshots in S3 (create-only
// dbs/<uuid>/expired/<9999999999 - n>), so remote collection may reclaim what only they reference.
void ExpireExported(Control &, const std::string &database, const std::vector<int64_t> &snapshots);
// Remote garbage collection of one S3 location (every database in it). Roots: branch heads, commit
// records without an expiry record, and this repository's retained exports and S3-only objects.
// Unreachable data objects older than `older_than` (microseconds) are condemned (gc/condemned/
// records); objects condemned before `older_than` and still unreachable are deleted: packs, index
// pages whose packs are all deleted, and descriptors with such a page. Exports never reference a
// condemned object, so the grace period only has to cover an export's last seconds.
struct RemoteCollection {
    int64_t roots = 0, live_objects = 0, unreachable_objects = 0, unreachable_bytes = 0, condemned_objects = 0,
            deleted_objects = 0, deleted_bytes = 0;
};
RemoteCollection CollectRemote(Control &, const std::string &provider, int64_t older_than, bool dry_run);
// The last failure to copy S3-only local objects out of condemned packs ("" when none).
std::string LastRemoteCollectionError();
// Shared databases, dropping a branch: before (the S3 head must still be this repository's base;
// returns its version, "" when the branch was never published) and after the local drop (the head
// becomes a tombstone, conditionally on that version; the name is free again).
std::string PrepareSharedDrop(Control &, const std::string &database, const std::string &branch);
void TombstoneBranch(Control &, const std::string &database, const Branch &, const std::string &etag);
// Shared databases: make another writer's published snapshot local (its map; no data), so a branch
// can start from it or a restore return to it. No-op for local snapshots and other databases.
void EnsureLocalSnapshot(Control &, const std::string &database, int64_t snapshot);
// The snapshot of a shared database's branch head in S3 (0: no head).
int64_t RemoteHeadSnapshot(Control &, const std::string &database, const std::string &branch);
// Whether a branch head of `database` named `branch` exists in its S3 location.
bool RemoteBranchExists(Control &, const std::string &database, const std::string &branch);
class RemoteMemoryCache;
struct RemoteCaching;
// The store's reader of remote-only objects: verified 256 KiB chunks from the packs recorded for them,
// through the reader's caches when given (memory cache keyed like export reads, so they share
// entries; the disk cache), else the disk cache at SetRemoteObjectCacheLimit.
std::map<std::string, SharedBytes> FetchRemoteObjects(Control &, const std::vector<std::string> &hashes,
                                                      const RemoteCaching *caching = nullptr);
// Installs FetchRemoteObjects as the store's remote fetch.
void InstallRemoteFetch(const std::shared_ptr<Control> &);
// Disk cache size for remote-only objects read through the store (differential_remote_cache_size;
// process-wide, the last value applies).
void SetRemoteObjectCacheLimit(uint64_t bytes);
// Resolve "<database>[/<database uuid>][/<branch>]" (branch defaults to main) under a provider.
Head ResolveHead(Control &, const std::string &provider, const std::string &path);
std::vector<Head> ListHeads(Control &, const std::string &provider);
// Memory cache for verified, immutable remote bytes, addressed by object URL and byte offset.
// The extension uses DuckDB's external file cache (buffer-managed, within memory_limit).
// Implementations are thread-safe and best effort: a failed insert is not an error.
class RemoteMemoryCache {
  public:
    virtual ~RemoteMemoryCache() = default;
    // False when inserts are discarded (e.g. enable_external_file_cache = false).
    virtual bool Enabled() const = 0;
    // Copy [offset, offset + length) of `object` into `out`; false unless fully cached.
    virtual bool Read(const std::string &object, uint64_t offset, uint64_t length, unsigned char *out) = 0;
    virtual void Insert(const std::string &object, uint64_t offset, const unsigned char *data, uint64_t length) = 0;
    // Brackets the life of each file handle that reads through this cache; inserts are kept only
    // while one is open, and the last Release discards what is still pending.
    virtual void Retain() {}
    virtual void Release() {}
};
class DiskCache;
// How a remote snapshot caches what it reads.
struct RemoteCaching {
    std::shared_ptr<RemoteMemoryCache> memory; // none: no memory cache (DuckDB's buffers still apply)
    uint64_t disk_limit = 0;                   // repository disk cache size in bytes; 0 = off
    bool fill = true; // false for bulk copies (rehydration, recovery): use caches, never fill them
};
class RemoteSnapshot {
  public:
    RemoteSnapshot(Control &, const std::string &provider, const std::string &descriptor, RemoteCaching caching = {});
    uint64_t Read(void *, uint64_t length, uint64_t offset);
    // Copy the exported sparse map into `file` and seal it under the descriptor's root identity.
    Root Materialize(Store &, int64_t file);
    // The exported chunks of index page `page`: logical chunk and identity, and where it is stored.
    std::vector<std::pair<ChunkRef, RemoteChunk>> Entries(uint64_t page);
    Descriptor descriptor;

  private:
    struct Location {
        bool hole = true;
        std::string hash, pack, pool; // pool: UUID of the database whose data pool holds the pack
        uint64_t slot = 0;
    };
    std::shared_ptr<S3Provider> provider;
    RemoteCaching caching;
    std::shared_ptr<DiskCache> disk;
    std::string pending_namespace, object_prefix;
    // Recently used index pages, most recent first (parallel scans touch several at once).
    static constexpr size_t INDEX_CACHE_PAGES = 16;
    std::mutex index_mutex;
    std::list<std::pair<uint64_t, SharedBytes>> indexes;
    // Sequential read streams, one per scanning thread (a file handle is shared by DuckDB's
    // threads, whose reads interleave). Most recently used first out when more than 16.
    struct Stream {
        uint64_t next = 0;    // offset the stream's next read would start at
        uint64_t ahead = 0;   // chunks before this index were already requested
        uint64_t window = 0;  // read-ahead size in chunks; doubles per refill
        unsigned reads = 0;   // consecutive sequential reads
        uint64_t used = 0;
    };
    std::mutex stream_mutex;
    std::vector<Stream> streams;
    uint64_t stream_clock = 0;
    SharedBytes Index(uint64_t page);
    Location Locate(const Bytes &index, uint64_t slot) const;
    SharedBytes FromMemory(const std::string &object, uint64_t offset, uint64_t length);
    SharedBytes FromDisk(const std::string &object, uint64_t offset, uint64_t length, const std::string &hash);
    void Keep(const std::string &hash, const SharedBytes &bytes);
    SharedBytes FetchObject(const std::string &key, uint64_t size, const std::string &hash);
    void FetchChunks(const std::vector<uint64_t> &chunks, std::map<uint64_t, SharedBytes> &out);
};
} // namespace differential
