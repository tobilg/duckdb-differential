#pragma once
#include "differential_store.hpp"
#include "differential_s3.hpp"
#include <condition_variable>
#include <optional>
#include <set>
#include <functional>
#include <thread>

namespace differential {
// Version of the control tables (databases, branches, snapshots, operations, exports).
constexpr int CONTROL_FORMAT = 1;
struct MetadataCell {
    bool null = true;
    int64_t number = 0;
    std::string text;
};
class MetadataCursor {
  public:
    MetadataCursor(const std::string &directory, const std::string &sql,
                   const std::vector<std::string> &parameters);
    ~MetadataCursor();
    bool Next(std::vector<MetadataCell> &row);

  private:
    struct Impl;
    std::unique_ptr<Impl> impl;
};
struct Branch {
    std::string database, database_name, id, name, state, root, source_database;
    int64_t epoch = 0, head = 0, created_at = 0, source_snapshot = 0;
    std::string FileName() const { return "managed/" + id; }
    std::string URI() const { return "diff://db/" + database + "/branches/" + id; }
};
// DuckLake-style commit metadata recorded with a public snapshot.
struct SnapshotInfo {
    std::string name, author, message, extra_info;
};
struct Snapshot {
    std::string database, database_name, branch, name, root;
    std::string author, message, extra_info;
    std::string source_database;
    int64_t source_snapshot = 0;
    std::string export_state = "not_required", export_key, export_provider;
    int64_t id = 0, published_at = 0, captured_at = 0, event_at = 0, size = 0;
    bool retired = false, evicted = false;
    // Shared databases: published by another writer and known only from its S3 commit record (read
    // through its export; no local root).
    bool remote = false;
    int64_t parent = 0; // remote: the record's parent
    std::string URI() const { return "diff://db/" + database + "/snapshots/" + std::to_string(id); }
};
// A shared database's S3 commit record (differential-commit-v1), cached locally: records are
// immutable, so the cache never needs invalidation; only expiry is added later.
struct RemoteCommit {
    int64_t snapshot = 0, parent = 0, published_at = 0;
    std::string database, branch, branch_name, descriptor, name, author, message, extra_info;
    bool expired = false;
};
struct ProviderRecord {
    std::string id, name;
    S3Config config;
};
struct ExportRecord {
    std::string id, provider, database, root, state, descriptor;
    int64_t snapshot = 0;
};
// The latest exported snapshot in an active branch's history (published as an S3 branch head).
struct BranchHead {
    std::string database, database_name, branch, branch_name, descriptor;
    int64_t snapshot = 0, published_at = 0;
};
// This repository's last write of a branch head: the S3 ETag and write ID it produced, and the
// write ID of an attempt whose outcome is unknown (a lost response).
// Location of an exported chunk inside an immutable remote pack: the pack lives in the data pool
// of database `pool` (dbs/<pool>/packs/<pack>).
struct RemoteChunk {
    std::string pack, pool;
    uint64_t slot = 0;
};
// For a shared database, `snapshot` is the head's snapshot as last written or adopted: the base
// the next update must still find in S3.
struct HeadWrite {
    std::string etag, write_id, pending;
    int64_t snapshot = 0;
};
struct DatabaseRecord {
    std::string id, name, provider;
    int64_t last_snapshot = 0, created_at = 0;
    std::string writers = "single";
};
// A shared database's branch whose head update failed because another writer moved the head:
// `snapshot` could not become the head (the head was at `remote_snapshot`). With `kept_branch`, the
// snapshot was published as that branch (ON_CONFLICT 'branch'), so the branch follows S3 again at
// its next ATTACH if it has no changes after the snapshot.
struct BranchConflict {
    std::string branch, kept_branch;
    int64_t snapshot = 0, remote_snapshot = 0, detected_at = 0;
};
// A branch head another writer published in S3 (shared databases), with the map of its snapshot.
struct RemoteHead {
    std::string key, etag, write_id; // the head object and the version read
    std::string branch, branch_name, descriptor, root;
    int64_t snapshot = 0, published_at = 0;
    uint64_t size = 0;
    std::vector<ChunkRef> chunks;                 // the snapshot's non-hole chunks
    std::map<std::string, RemoteChunk> locations; // where each of them is stored in S3
    SnapshotInfo info;                            // from its commit record, when known
};
struct RetentionRoot {
    std::string reason, identity, root;
};
struct Operation {
    std::string id, request, kind, payload, state, database, branch;
    int64_t snapshot = 0, preservation = 0;
};
struct ResolvedPath {
    bool managed = false, historical = false;
    bool remote = false;
    std::string provider, descriptor;
    std::string canonical, file, root, suffix;
    Branch branch;
    Snapshot snapshot;
};
struct Pin {
    std::shared_ptr<Store> store;
    std::string id, root;
    int fd = -1;
    ~Pin();
};
struct Collection {
    std::string operation;
    int64_t candidate_objects = 0, candidate_bytes = 0, deleted_objects = 0;
    int64_t live_pins = 0, stale_pins = 0;
    int64_t remaining_candidates = 0;
    std::vector<std::string> released_pins; // lock files removed only after the commit
};
struct Compaction : Collection {
    int64_t roots = 0, payload_bytes = 0, export_retained_bytes = 0, page_size = 0;
};
struct BranchLock {
    int fd = -1;
    int management_fd = -1;
    Branch branch;
    ~BranchLock();
    BranchLock() = default;
    BranchLock(const BranchLock &) = delete;
    BranchLock &operator=(const BranchLock &) = delete;
};
class Control {
  public:
    explicit Control(std::shared_ptr<Store> store, std::function<int64_t()> clock = {});
    ~Control();
    Control(const Control &) = delete;
    Control &operator=(const Control &) = delete;
    std::shared_ptr<Store> store;
    std::string RepositoryID();
    Operation CreateDatabase(const std::string &name, const std::string &request,
                             const std::string &provider = "local");
    // A database several repositories write through S3 (WRITERS 'shared'), with the identity all of
    // them derive from its name. Without branches: they come from S3 or are created empty.
    void CreateSharedDatabase(const std::string &id, const std::string &name, const std::string &provider);
    // "single" or "shared".
    std::string Writers(const std::string &database);
    // An empty local branch of a shared database (its S3 head is created by its first export).
    Branch CreateEmptyBranch(const std::string &database, const std::string &name);
    // Make a branch head published in S3 the local branch `head.branch_name` (shared databases):
    // its map is imported without downloading data (reads go to S3 until local copies exist), its
    // snapshot is recorded as exported, and the head version becomes the base of this repository's
    // next update. With `replace`, an existing local branch of that name is replaced (it must have no
    // local changes, and the caller holds its writer lock); without, an existing one is returned.
    Branch AdoptRemoteHead(const std::string &database, const std::string &provider, const RemoteHead &head,
                           bool replace);
    // Shared databases: move every snapshot numbered `from` or above (all unexported) above `taken`,
    // because another writer published those numbers. Returns how far they moved (0: not needed).
    int64_t RenumberSnapshots(const std::string &database, int64_t from, int64_t taken);
    // Another writer's published snapshot as a local one (shared databases), without its bytes, so
    // branches can start from it and restores can return to it. Attributed to `branch`.
    void ImportRemoteSnapshot(const std::string &database, const std::string &provider, const RemoteHead &snapshot,
                              const std::string &branch);
    Operation GetOperation(const std::string &id);
    // Shared databases: the cache of S3 commit records (other writers' history).
    std::set<int64_t> CachedCommits(const std::string &database);
    void CacheCommits(const std::string &database, const std::vector<RemoteCommit> &commits);
    // Snapshots expired in S3: marked in the cache, and retired here when they exist locally.
    void ExpireCommits(const std::string &database, const std::vector<int64_t> &snapshots);
    // A public snapshot of the database: local, or (shared databases) known from its commit record.
    Snapshot FindSnapshot(const std::string &database, int64_t snapshot, bool retained = true);
    // A shared database's branch that was dropped in S3 by another writer stops being local too.
    void DropBranchLocally(const std::string &database, const std::string &branch);
    // Divergence of shared branches from their S3 heads (cleared by adopting a head).
    void RecordConflict(const BranchConflict &);
    std::optional<BranchConflict> Conflict(const std::string &branch);
    void ClearConflict(const std::string &branch);
    // An idempotent operation record for multi-step requests outside this class (request IDs).
    Operation BeginOperation(const std::string &kind, const std::vector<std::string> &payload,
                             const std::string &request);
    Operation CompleteOperation(Operation);
    // Database UUID for a name or UUID, or "" when absent.
    std::string FindDatabase(const std::string &name);
    // Idempotently register the provider behind a DATA_PATH. S3 locations are named
    // s3://bucket/prefix/; explicitly given options must match an existing registration.
    std::string EnsureS3Provider(const S3Config &config, const std::vector<std::string> &explicit_options);
    std::string EnsureLocalPool(const std::string &path);
    // The DATA_PATH a database was created with ("" for the default pool).
    std::string DataPath(const std::string &database);
    Operation BeginSnapshot(const Branch &, const SnapshotInfo &, const std::string &request);
    Operation PublishSnapshot(const Operation &, const Branch &, const Root &, int64_t captured_at);
    Operation CreateBranch(const std::string &database, int64_t snapshot, const std::string &name,
                           const std::string &request, bool clone);
    Operation Retire(const std::string &database, int64_t snapshot, const std::string &request);
    Operation Evict(const std::string &database, int64_t snapshot, const std::string &request);
    // Snapshots selected by explicit IDs and/or publication time (strictly older), for bulk
    // expiry (retirement) or eviction. Age selection skips snapshots already expired/evicted
    // and, for eviction, those without a completed export.
    std::vector<Snapshot> SelectSnapshots(const std::string &database, const std::vector<int64_t> &versions,
                                          std::optional<int64_t> older_than, bool for_eviction);
    // Head of a branch, or the database's latest snapshot; 0 when none exists.
    int64_t CurrentSnapshot(const std::string &database, const std::string &branch = "");
    // Publish the just-sealed checkpoint generation of a writer's branch as an unnamed public
    // snapshot (differential_snapshot_on_checkpoint). Skips unchanged roots and stale leases.
    Operation CheckpointSnapshot(const Branch &lease, const Root &root);
    std::vector<int64_t> PendingExports(const std::string &database);
    Operation RetryExport(const std::string &database, int64_t snapshot, const std::string &request);
    std::pair<Operation, std::shared_ptr<BranchLock>>
    PrepareCompound(const std::string &database, const std::string &branch, int64_t target,
                    const std::string &request, bool restore);
    Operation FinishCompound(const Operation &, const Branch &, const Root &preserved, bool restore);
    Branch GetBranch(const std::string &database, const std::string &branch, bool stable = false);
    Snapshot GetSnapshot(const std::string &database, int64_t snapshot, bool retained = true);
    // The branch's snapshot at `timestamp`. Shared databases: the newest retained snapshot published at
    // or before it along the branch's ancestry (commit-record parents; local history for unpublished
    // snapshots), starting at `start` (default: the local branch's head; the commit cache must cover it).
    Snapshot SnapshotAt(const std::string &database, const std::string &branch, int64_t timestamp,
                        int64_t start = 0);
    ResolvedPath Resolve(const std::string &uri);
    std::shared_ptr<BranchLock> Acquire(const Branch &);
    std::vector<Snapshot> Snapshots(const std::string &database);
    std::vector<Branch> Branches(const std::string &database);
    std::vector<Operation> Operations();
    std::vector<DatabaseRecord> Databases();
    std::vector<ProviderRecord> Providers();
    std::vector<RetentionRoot> RetentionRoots();
    std::vector<std::pair<std::string, uint64_t>> Statistics();
    std::unique_ptr<MetadataCursor> List(const std::string &kind, const std::string &database = "");
    std::shared_ptr<Pin> NewPin(const std::string &root = "", const std::string &database = "",
                                int64_t snapshot = 0);
    // Diagnostic probes only: a pin without a lock file, which collection never releases.
    void RetainRootPermanently(const std::string &root);
    Collection Collect(bool dry_run, int64_t limit, const std::string &request);
    // Bounded background collection without an operation record: short transactions of
    // `budget` units until the queues are empty or `milliseconds` elapse. Returns false when
    // the payload fence or authority is busy; it never waits for either.
    bool Maintain(int64_t budget = 1024, int64_t milliseconds = 100);
    // Request Maintain() on this authority's background worker (started on first use) so
    // collection never extends a native sync or checkpoint.
    void ScheduleMaintenance();
    Compaction Compact(int64_t pages, const std::string &request);
    // Owned files in registered local pools that no mapping references (left by crashes).
    // Bounded by `limit` names examined; dry_run only counts.
    uint64_t DeleteOrphanedFiles(bool dry_run, int64_t limit);
    std::string LocalProvider(const std::string &name);
    std::string LocalDatabasePool(const std::string &database);
    ProviderRecord Provider(const std::string &name);
    std::vector<ProviderRecord> S3Providers();
    ExportRecord Export(const std::string &database, int64_t snapshot);
    // Per active branch: the latest non-retired snapshot in its history whose S3 export completed.
    // `provider` receives the database's S3 provider ("" for databases without S3).
    std::vector<BranchHead> ExportedHeads(const std::string &database, std::string &provider);
    std::optional<HeadWrite> HeadWriteFor(const std::string &provider, const std::string &key);
    void RecordHeadWrite(const std::string &provider, const std::string &key, const HeadWrite &write);
    void ExportComplete(const std::string &id, const std::string &descriptor);
    bool RemoteVerified(const std::string &provider, const std::string &key, const std::string &hash,
                        uint64_t size);
    void RecordRemoteVerified(const std::string &provider, const std::string &key, const std::string &hash,
                              uint64_t size);
    // Recorded remote locations for the given chunk identities (absent when not yet exported).
    std::map<std::string, RemoteChunk> FindRemoteChunks(const std::string &provider,
                                                        const std::vector<std::string> &hashes);
    void RecordPack(const std::string &provider, const std::string &pool, const std::string &pack, uint64_t size,
                    const std::vector<std::string> &chunks);
    // The snapshot before `snapshot` in the history of the branch that published it (0: none).
    int64_t ParentSnapshot(const std::string &database, int64_t snapshot);
    // Name of a branch by UUID, also for dropped branches ("" when unknown).
    std::string BranchName(const std::string &branch);
    // Give up a pending export whose snapshot number another writer already published (recovery
    // after a split brain): it is never retried and its retention root is released.
    bool AbandonExport(const std::string &database, int64_t snapshot);
    // New snapshots of the database get numbers above `taken`.
    void AdvanceSnapshotNumbers(const std::string &database, int64_t taken);
    Operation BeginImport(const std::string &name, const std::string &source, const std::string &request,
                          const std::string &provider = "local", const std::string &kind = "recover_export");
    Operation CompleteImport(const Operation &, const Root &, const std::string &source_database,
                             int64_t source_snapshot, const std::string &provider = "local");
    std::string Profile(const std::string &database);
    static int64_t Now();

  private:
    std::function<int64_t()> clock;
    std::mutex maintenance_mutex;
    std::condition_variable maintenance_signal;
    bool maintenance_requested = false, maintenance_stopping = false;
    std::thread maintenance_worker;
    int64_t Timestamp() const { return clock ? clock() : Now(); }
    std::string DatabaseID(const std::string &name);
    Operation Begin(const std::string &kind, const std::string &payload, const std::string &request);
    void Complete(Operation &);
    void Validate(const Branch &);
    void AssignProvider(const std::string &database, const std::string &provider);
    Snapshot Publish(const Branch &, const Root &, const SnapshotInfo &, int64_t captured_at);
    Branch NewBranch(const std::string &database, const std::string &name, const std::string &root,
                     int64_t head, const std::string &source_db, int64_t source_snapshot);
    void Event(const Branch &, int64_t snapshot, int64_t timestamp);
    int64_t RenumberTx(const std::string &database, int64_t from, int64_t taken);
    std::string ImportMapTx(const std::string &provider, const RemoteHead &);
    void InsertRemoteSnapshotTx(const std::string &database, const std::string &provider, const RemoteHead &,
                                const std::string &root, const std::string &branch);
    void UnlinkBranchFiles(const Branch &);
    void ReconcilePins(Collection &, int64_t limit, bool apply);
    int64_t CollectStep(int64_t budget, int64_t &deleted);
    int64_t DryRun(Collection &);
};
} // namespace differential
