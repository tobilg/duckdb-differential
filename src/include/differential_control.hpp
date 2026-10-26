#pragma once
#include "differential_store.hpp"
#include "differential_s3.hpp"
#include "differential_merge_budget.hpp"
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
// A validated S3 merge awaiting publication. The local destination is not advanced yet.
// The companion operation and export_pending root survive process death.
struct MergePublication {
    Operation record;
    Snapshot snapshot;
    std::string merge_id, destination, provider, descriptor, write_id;
    std::string action = "created_branch", expected_etag;
    int64_t ours = 0, theirs = 0;
    bool head_attempted = false;
};
// Bounded latest-attempt diagnostics; no roots or user error text are retained.
struct MergeAttempt {
    std::string id, state, phase, error_code;
    int64_t count = 0, started_at = 0, updated_at = 0, finished_at = 0;
};
struct MergeRequest {
    Operation operation;
    std::string database, destination, state, root, descriptor, recovery;
    std::string schema_mode, schema_algorithm, action;
    std::string ours_branch_name, theirs_branch_name, ours_branch_id, theirs_branch_id;
    std::optional<MergeAttempt> attempt;
    int64_t base = 0, ours = 0, theirs = 0, result = 0, retained_bytes = 0, created_at = 0;
    bool reserved = false, head_attempted = false, inputs_guarded = false;
};
struct SnapshotParents {
    std::string kind;
    int64_t first = 0, second = 0;
};
struct MergeAncestor {
    int64_t snapshot = 0;
    SnapshotParents parents;
    unsigned reach = 0;
    bool known = false, candidate = false, retired = false, readable = false;
    std::string branch;
};
struct MergeAncestry {
    std::vector<MergeAncestor> nodes;
    std::string state;
    int64_t selected = 0;
};
// A branch request resolves once to immutable inputs. Pending/completed retries use
// the recorded IDs even when branch heads advance or names are deleted and reused.
struct BranchMergeInputs {
    Operation operation;
    std::string ours_branch, theirs_branch;
    std::string selection = "unique-base-v1";
    int64_t base = 0, ours = 0, theirs = 0;
    int64_t ancestry_nodes = 0;
    double selection_ms = 0;
};
struct MergeExpectedInputs {
    int64_t base = 0, ours = 0, theirs = 0; // All zero disables the guard; otherwise all positive.
    bool Enabled() const { return base != 0 || ours != 0 || theirs != 0; }
    void Validate() const;
    void Check(int64_t actual_base, int64_t actual_ours, int64_t actual_theirs) const;
};
struct MergeDiagnostic {
    std::string phase, code, input, object_type, object_name, message, operation_id, recovery_action;
};
struct MergeDestinationCheck {
    std::string target_state = "not_checked";
    std::vector<MergeDiagnostic> issues;
};
struct ResolvedPath {
    bool managed = false, historical = false;
    bool remote = false;
    std::string provider, descriptor;
    std::string canonical, file, root, suffix;
    Branch branch;
    Snapshot snapshot;
    bool merge_stage = false;
    bool merge_input = false;
    std::string merge_pool;
    std::shared_ptr<MergeWriteBudget> merge_budget;
};
struct Pin {
    bool merge_stage = false;
    std::shared_ptr<MergeWriteBudget> merge_budget;
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
    // Continuous WAL shipping recovery: a new single-writer database `name` in S3 location `provider`
    // (with `database_id`/`branch_id` when not empty: the original identities, a failover) whose
    // branch `branch_name` starts from the shipped base `base` (its map imported without downloading;
    // not a snapshot, so the branch has no root until its first checkpoint).
    Branch CreateFromShippedBase(const std::string &name, const std::string &provider,
                                 const std::string &database_id, const std::string &branch_id,
                                 const std::string &branch_name, const RemoteHead &base);
    // Continuous WAL shipping metadata (formats owned by differential_shipping.hpp).
    struct ShipChainRow {
        int64_t chain = 0, epoch = -1, sequence = 0, bytes = 0, shipped_at = 0;
        std::string hash, base, known, pending;
    };
    // {mode, interval_ms} of a shipping database, or nothing.
    std::optional<std::pair<std::string, int64_t>> ShipConfigOf(const std::string &database);
    void SetShipConfigOf(const std::string &database, const std::string &mode, int64_t interval_ms);
    std::optional<ShipChainRow> ShipChainOf(const std::string &branch);
    void SetShipChainOf(const std::string &database, const std::string &branch, const ShipChainRow &);
    int64_t Clock() const { return Timestamp(); }
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
    // How far this repository has read a shared database's history in S3: the last full listings of
    // its commit records and expiry records (microseconds), and the newest expiry log key read.
    struct RemoteSyncState {
        int64_t commits_full_at = 0, expiries_full_at = 0;
        std::string expiries_watermark;
    };
    RemoteSyncState GetRemoteSyncState(const std::string &database);
    void SetRemoteSyncState(const std::string &database, const RemoteSyncState &);
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
    // `reusable`: only locations an export may reference (packs not condemned by remote collection).
    std::map<std::string, RemoteChunk> FindRemoteChunks(const std::string &provider,
                                                        const std::vector<std::string> &hashes, bool reusable = false);
    // Locations of a newly uploaded pack; they replace earlier locations of the same chunks.
    void RecordPack(const std::string &provider, const std::string &pool, const std::string &pack, uint64_t size,
                    const std::vector<std::string> &chunks);
    // Remote garbage collection (S3). Condemnation records (gc/condemned/...) name unreachable objects
    // that a later collection deletes once they stayed condemned for its grace period.
    bool KnownCondemnation(const std::string &provider, const std::string &record);
    // Apply a record (S3 LastModified `at`, microseconds): exports stop reusing its objects, their
    // upload verifications are forgotten, and pack locations no local object still reads are dropped.
    // `learned`: the record is done (its S3-only local objects were copied) and is not read again.
    void RecordCondemnation(const std::string &provider, const std::string &record, int64_t at,
                            const std::vector<std::string> &keys, bool learned);
    // Every condemned key of the location, with its earliest condemnation time.
    std::map<std::string, int64_t> Condemnations(const std::string &provider);
    bool Condemned(const std::string &provider, const std::string &key);
    // Collection roots this repository holds: descriptors of its retained completed exports, and the
    // packs of local objects stored only in S3 (read on demand).
    std::vector<std::string> LiveExportDescriptors(const std::string &provider);
    std::set<std::string> RemoteOnlyPacks(const std::string &provider);
    // Local objects stored only in S3 whose recorded pack is one of `packs` (pack keys).
    std::vector<std::string> RemoteOnlyObjectsIn(const std::string &provider, const std::set<std::string> &packs);
    // Store the verified bytes of remote-only objects locally (they stop depending on S3).
    void MaterializeRemoteObjects(const std::map<std::string, SharedBytes> &objects);
    // Objects this collection deleted from S3: forget their verifications and pack locations.
    void ForgetRemoteObjects(const std::string &provider, const std::vector<std::string> &keys);
    // What immutable S3 objects reference, cached for remote collection (they never change, so entries
    // are only removed with their objects): index page -> packs, descriptor -> index pages (with sizes),
    // commit record -> descriptor.
    struct RemoteReferences {
        std::map<std::string, std::vector<std::string>> index_packs;
        std::map<std::string, std::vector<std::pair<std::string, uint64_t>>> descriptor_pages;
        std::map<std::string, std::string> commit_descriptors;
    };
    RemoteReferences LoadRemoteReferences(const std::string &provider);
    void StoreRemoteReferences(const std::string &provider, const RemoteReferences &references);
    // Delete completed operation records created before `older_than` (request IDs stop replaying)
    // and history entries of retired snapshots that took effect before it. Returns rows per table.
    std::vector<std::pair<std::string, int64_t>> PruneMetadata(int64_t older_than, bool dry_run);
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
    // Process-local, pin-owned staging namespace, never a public branch.
    std::shared_ptr<Pin> MergeStage(const std::string &root, const std::string &pool,
                                  std::shared_ptr<MergeWriteBudget> budget);
    std::shared_ptr<Pin> MergeReader(const std::string &root);
    std::pair<Snapshot, std::shared_ptr<Pin>> PinMergeInput(const std::string &database, int64_t snapshot);
    Operation BeginMerge(const std::string &database, int64_t base, int64_t ours, int64_t theirs,
                         const std::string &name, const std::string &request, const std::string &resolution_spec = "",
                         const std::string &schema_mode = "strict");
    BranchMergeInputs ResolveMergeBranches(const std::string &database, const std::string &ours,
                                           const std::string &theirs, const std::string &destination,
                                           const std::string &request, int64_t max_nodes = 100000,
                                           const std::function<void()> &check = {}, const std::string &resolution_spec = "",
                                           const std::string &schema_mode = "strict",
                                           const MergeExpectedInputs &expected = {}, bool inspect_pending = false);
    MergeDestinationCheck InspectMergeDestination(const std::string &database, const std::string &ours,
                                                  const std::string &theirs, const std::string &destination,
                                                  const BranchMergeInputs *selected,
                                                  const std::function<void()> &check);
    Operation PublishMerge(const Operation &, const Root &, const std::string &provenance);
    static std::string MergeAction(const Operation &);
    std::shared_ptr<BranchLock> PrepareMergeTarget(const Operation &);
    void CheckMergeTargetClean(const Branch &, const std::string &expected_root);
    void ReleaseMergeTarget(const Operation &);
    bool MergeTargetPending(const std::string &branch);
    Operation PublishMergeTarget(const Operation &, const Branch &, const Root &, const std::string &provenance);
    // Caller holds the metadata FULL transaction and target fence.
    void FinishMergeTargetTx(Operation &, const Branch &, const Root &, const std::string &provenance,
                             const Snapshot *published = nullptr);
    std::optional<MergePublication> PendingMergePublication(const Operation &);
    MergePublication PrepareMergePublication(const Operation &, const Root &, const std::string &provenance,
                                              const std::string &expected_etag = "");
    void MergeUploadComplete(const Operation &, const std::string &descriptor);
    Operation FinishMergePublication(const Operation &);
    void MergeHeadAttempt(const Operation &);
    // Lock order: request lock, then S3 head lock, then short metadata transactions.
    // Never wait for a file lock while holding store->mutex.
    std::shared_ptr<BranchLock> LockMerge(const Operation &, const std::function<void()> &check);
    bool MergeAborted(const Operation &);
    void CheckMergeActive(const Operation &);
    // Caller holds the merge request lock. Publication completes diagnostics in
    // the same FULL transaction as the result; completed replays never start one.
    MergeAttempt BeginMergeAttempt(const Operation &);
    void UpdateMergeAttempt(const Operation &, const std::string &attempt_id, const std::string &phase,
                            const std::string &error_code = "");
    static std::string SafeMergeErrorCode(const std::string &);
    static std::string CurrentMergeSchemaAlgorithm(const std::string &mode);
    std::pair<std::string, std::string> MergeSchemaPolicy(const Operation &);
    // First execution freezes native expression settings; retries reuse them.
    std::pair<std::string, std::string> MergeExpressionContext(const Operation &,
                                                              const std::string &timezone,
                                                              const std::string &calendar);
    Operation FindMergeOperation(const std::string &database, const std::string &operation_id);
    std::vector<MergeRequest> MergeRequests(const std::string &database, const std::string &request,
                                           const std::string &after, int64_t limit,
                                           const std::function<void()> &check);
    Operation BeginAbortMerge(const Operation &, const std::string &request);
    Operation FinishAbortMerge(const Operation &abort, const Operation &merge);
    MergeAncestry InspectMergeAncestry(const std::string &database, int64_t ours, int64_t theirs,
                                      int64_t max_nodes, const std::function<void()> &check = {});
    std::optional<std::string> MergeResolutions(const Operation &, const std::string &specification,
                                               std::optional<std::string> data = std::nullopt);
    std::string ReadMergeResolutions(const std::string &database, const std::string &request);
    static int64_t Now();

  private:
    std::function<int64_t()> clock;
    std::map<std::string, std::pair<std::weak_ptr<Pin>, std::string>> merge_stages;
    std::mutex maintenance_mutex;
    std::condition_variable maintenance_signal;
    bool maintenance_requested = false, maintenance_stopping = false;
    std::thread maintenance_worker;
    int64_t Timestamp() const { return clock ? clock() : Now(); }
    std::string DatabaseID(const std::string &name);
    Operation Begin(const std::string &kind, const std::string &payload, const std::string &request);
    void Complete(Operation &);
    std::optional<MergeAttempt> ReadMergeAttempt(const Operation &);
    void WriteMergeAttempt(const Operation &, const MergeAttempt &);
    void CompleteMergeAttempt(const Operation &);
    void Validate(const Branch &);
    void AssignProvider(const std::string &database, const std::string &provider);
    Snapshot Publish(const Branch &, const Root &, const SnapshotInfo &, int64_t captured_at,
                     std::optional<SnapshotParents> parents = std::nullopt);
    void RecordAncestry(const Snapshot &, SnapshotParents);
    void ReserveMergeResolutions(const Operation &, const std::string &specification);
    void ReserveMergeSchema(const Operation &, const std::string &mode, bool create);
    void ReserveMergeExpectations(const Operation &, const MergeExpectedInputs &, bool create);
    void CheckMergeDestination(const std::string &database, const std::string &name,
                               const std::string &owner = "");
    std::string MergeDestinationOwner(const std::string &database, const std::string &name);
    MergeAncestry WalkMergeAncestry(const std::string &database, int64_t ours, int64_t theirs,
                                   int64_t max_nodes, const std::function<void()> &check);
    int64_t FindMergeBase(const std::string &database, int64_t ours, int64_t theirs, int64_t max_nodes,
                         const std::function<void()> &check, int64_t &visited);
    Branch NewBranch(const std::string &database, const std::string &name, const std::string &root,
                     int64_t head, const std::string &source_db, int64_t source_snapshot,
                     const std::string &reserved_id = "");
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
