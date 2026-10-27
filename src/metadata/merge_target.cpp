#include "control_internal.hpp"
#include "differential_metrics.hpp"
#include <filesystem>
#include <fcntl.h>
#include <limits>
#include <sys/file.h>

namespace differential {
using namespace control;
namespace {
std::vector<std::string> TargetArguments(const Operation &op) {
    auto f = DecodePayload(op.payload);
    if (op.kind != "merge_branch" || f.size() != 11 || f[1] != f[3] || f[5] != "logical-cell-v1")
        throw std::runtime_error("[differential:merge_request] Not an existing-target merge");
    return f;
}
void NoWAL(sqlite3 *db, const Branch &b) {
    Query wal(db, "SELECT 1 FROM files WHERE name IN (?,?,?) AND size>0 LIMIT 1");
    wal.Text(1, b.FileName() + ".wal").Text(2, b.FileName() + ".wal.checkpoint").Text(3, b.FileName() + ".wal.recovery");
    if (wal.Step())
        throw std::runtime_error("[differential:merge_target_dirty] Target has a pending native WAL; recover, snapshot and detach it before a new merge request");
}
} // namespace

std::string Control::MergeAction(const Operation &op) {
    auto f = DecodePayload(op.payload);
    if (op.kind != "merge_branch" || f.size() != 11 || f[1] != f[3]) return "created_branch";
    if (f[8] == f[10]) return "already_up_to_date";
    if (f[8] == f[9]) return "fast_forward";
    return "merged";
}

std::shared_ptr<BranchLock> Control::PrepareMergeTarget(const Operation &op) {
    auto f = TargetArguments(op);
    // Request lock precedes this nonblocking branch lock. Never wait on a file
    // lock while holding the metadata mutex. Other branches remain writable.
    auto lease = std::make_shared<BranchLock>();
    auto path = std::filesystem::path(store->directory) / "locks";
    store->permissions.CreateDirectories(path);
    lease->fd = store->permissions.Open(path / ("branch-" + f[6]), O_RDWR | O_CLOEXEC);
    if (lease->fd < 0 || flock(lease->fd, LOCK_EX | LOCK_NB)) {
        Count(Counter::WriterConflicts);
        throw std::runtime_error("[differential:active_attachment_or_transaction] Merge target must be detached with no transaction holding it");
    }
    store->ReplayJournals();
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    Transaction tx(store->db);
    CheckMergeActive(op);
    if (Writers(f[0]) != "single")
        throw std::runtime_error("[differential:merge_unsupported] Existing-target merge requires single-writer storage");
    Query identity(store->db, "SELECT 1 FROM branches WHERE database_id=? AND id=? AND name=? AND state='active' AND head=?");
    identity.Text(1, f[0]).Text(2, f[6]).Text(3, f[1]).Int(4, std::stoll(f[9]));
    if (!identity.Step())
        throw std::runtime_error("[differential:merge_target_changed] Target identity or head changed; abort and use a new request_id");
    auto b = GetBranch(f[0], f[6], true);
    Query pending(store->db, "SELECT operation_id,expected_head FROM compound_intents WHERE branch_id=?");
    pending.Text(1, b.id);
    if (pending.Step()) {
        if (pending.Text(0) != op.id)
            throw std::runtime_error("[differential:writer_busy] Target has another pending lifecycle operation");
        if (pending.Int(1) != b.head)
            throw std::runtime_error("[differential:merge_target_changed] Pending target head changed");
    } else {
        Query(store->db, "INSERT INTO compound_intents VALUES(?,?,?,?,?)")
            .Text(1, op.id).Text(2, b.id).Int(3, b.head).Int(4, b.epoch).Text(5, b.root).Run();
    }
    if (b.epoch == std::numeric_limits<int64_t>::max()) throw std::runtime_error("Writer epoch exhausted");
    ++b.epoch;
    Query(store->db, "UPDATE branches SET epoch=? WHERE id=?").Int(1, b.epoch).Text(2, b.id).Run();
    Query(store->db, "UPDATE compound_intents SET expected_epoch=? WHERE operation_id=?")
        .Int(1, b.epoch).Text(2, op.id).Run();
    Query(store->db, "UPDATE operations SET database_id=?,branch_id=? WHERE id=?")
        .Text(1, b.database).Text(2, b.id).Text(3, op.id).Run();
    lease->branch = b;
    tx.Commit();
    return lease;
}

void Control::CheckMergeTargetClean(const Branch &b, const std::string &expected_root) {
    int64_t file = 0;
    {
        std::lock_guard<std::recursive_mutex> guard(store->mutex);
        Validate(b);
        NoWAL(store->db, b);
        Query q(store->db, "SELECT id FROM files WHERE name=?");
        q.Text(1, b.FileName());
        if (q.Step()) file = q.Int(0);
    }
    // Flush the store's overlay/journal before comparing the live image. A
    // metadata generation alone need not include the last native writes.
    auto root = file ? store->Seal(file).id : b.root;
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    Validate(b);
    NoWAL(store->db, b);
    if (root != expected_root || b.root != expected_root)
        throw std::runtime_error("[differential:merge_target_dirty] Target differs from its published snapshot; snapshot and detach it before a new merge request");
}

void Control::ReleaseMergeTarget(const Operation &op) {
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    Transaction tx(store->db);
    // An uploaded/uncertain S3 result must keep its local target fenced, including
    // after cancellation. Only final publication or a proven-safe abort releases it.
    auto p = PendingMergePublication(op);
    if (p && p->record.state == "pending") { tx.Commit(); return; }
    Query(store->db, "DELETE FROM compound_intents WHERE operation_id=?").Text(1, op.id).Run();
    tx.Commit();
}

bool Control::MergeTargetPending(const std::string &branch) {
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    Query q(store->db, "SELECT 1 FROM compound_intents c JOIN operations o ON o.id=c.operation_id "
                       "WHERE c.branch_id=? AND o.kind='merge_branch'");
    q.Text(1, branch);
    return q.Step();
}

MergeDestinationCheck Control::InspectMergeDestination(const std::string &database, const std::string &ours,
                                                       const std::string &theirs, const std::string &destination,
                                                       const BranchMergeInputs *selected,
                                                       const std::function<void()> &check) {
    ValidateName(ours); ValidateName(theirs); ValidateName(destination);
    MergeDestinationCheck out;
    const bool into_target = destination == ours;
    out.target_state = into_target ? "not_checked" : "not_applicable";
    auto issue = [&](const std::string &phase, const std::string &code, const std::string &input,
                     const std::string &message, const std::string &operation, const std::string &recovery) {
        out.issues.push_back({phase, code, input, "branch", input == "ours" ? ours : input == "theirs" ? theirs : destination,
                              message, operation, recovery});
    };
    Branch left, right;
    auto pending = [&](const Branch &branch, const std::string &side) {
        Query q(store->db, "SELECT o.id,o.kind FROM compound_intents c JOIN operations o ON o.id=c.operation_id WHERE c.branch_id=?");
        q.Text(1, branch.id);
        if (!q.Step()) return false;
        issue("ownership", "merge_pending_operation", side, "Branch has a pending " + q.Text(1) + " operation", q.Text(0),
              q.Text(1) == "merge_branch" ? "Retry the original merge request, or call differential_abort_merge with this operation_id"
                                         : "Retry the original lifecycle request to complete it");
        return true;
    };
    {
        std::lock_guard<std::recursive_mutex> guard(store->mutex);
        Transaction tx(store->db, "DIFFERENTIAL_CONTROL_CRASH", false);
        auto id = DatabaseID(database);
        if (Writers(id) != "single")
            throw std::runtime_error("[differential:merge_unsupported] Branch preflight requires single-writer storage");
        left = GetBranch(id, ours); right = GetBranch(id, theirs);
        if (selected) {
            if (left.id != selected->ours_branch || left.head != selected->ours ||
                right.id != selected->theirs_branch || right.head != selected->theirs) {
                out.target_state = into_target ? "changed" : "not_applicable";
                issue("inputs", "merge_inputs_changed", "", "Branch identity or head changed during preflight", "", "Run preflight again");
                tx.Commit(); return out;
            }
        }
        bool left_pending = pending(left, "ours");
        if (right.id != left.id) pending(right, "theirs");
        if (into_target && left_pending) out.target_state = "pending";
        if (!into_target) {
            auto owner = MergeDestinationOwner(id, destination);
            if (!owner.empty()) {
                out.target_state = "reserved";
                issue("destination", "merge_pending_operation", "", "Destination is reserved by a pending S3 merge", owner,
                      "Retry the original merge request, or call differential_abort_merge with this operation_id");
            }
            Query exists(store->db, "SELECT 1 FROM branches WHERE database_id=? AND name=? AND state='active'");
            exists.Text(1, id).Text(2, destination);
            if (exists.Step()) {
                out.target_state = "exists";
                issue("destination", "name_conflict", "", "Merge destination already exists", "",
                      "Choose a new name, or use ours as the destination for a target merge");
            }
        }
        tx.Commit();
    }
    if (!into_target || out.target_state == "pending") return out;
    check();
    if (left.head <= 0) {
        out.target_state = "unsnapshotted";
        issue("target", "merge_target_dirty", "ours", "Target has no published snapshot", "", "Create a public snapshot and detach the target");
        return out;
    }
    // This is only a temporary lease. Do not use Acquire/PrepareMergeTarget:
    // preflight must not advance epochs, create intents or reserve requests.
    auto lease = std::make_shared<BranchLock>();
    auto path = std::filesystem::path(store->directory) / "locks";
    store->permissions.CreateDirectories(path);
    lease->fd = store->permissions.Open(path / ("branch-" + left.id), O_RDWR | O_CLOEXEC);
    if (lease->fd < 0) throw std::runtime_error("Cannot open preflight target lock");
    if (flock(lease->fd, LOCK_EX | LOCK_NB)) {
        if (errno != EWOULDBLOCK && errno != EAGAIN && errno != EINTR)
            throw std::runtime_error("Cannot lock preflight target");
        out.target_state = "busy";
        issue("target", "active_attachment_or_transaction", "ours", "Target has an active attachment, transaction or management owner", "",
              "Detach the target and finish transactions holding its attachment; retry after any management operation finishes");
        return out;
    }
    {
        std::lock_guard<std::recursive_mutex> guard(store->mutex);
        Transaction tx(store->db, "DIFFERENTIAL_CONTROL_CRASH", false);
        if (pending(left, "ours")) { out.target_state = "pending"; tx.Commit(); return out; }
        tx.Commit();
    }
    store->ReplayJournals();
    check();
    auto expected_root = GetSnapshot(left.database, selected ? selected->ours : left.head).root;
    try {
        CheckMergeTargetClean(left, expected_root);
        out.target_state = "clean";
    } catch (const std::runtime_error &error) {
        std::string message = error.what();
        if (message.find("[differential:merge_target_dirty]") != std::string::npos) {
            out.target_state = "dirty";
            issue("target", "merge_target_dirty", "ours", message, "", "Recover/checkpoint if needed, create a public snapshot, detach and run preflight again");
        } else if (message.find("[differential:stale_epoch]") != std::string::npos) {
            out.target_state = "changed";
            issue("target", "merge_inputs_changed", "ours", "Target ownership or head changed during preflight", "", "Run preflight again");
        } else throw;
    }
    check();
    return out;
}

Operation Control::PublishMergeTarget(const Operation &operation, const Branch &b, const Root &root,
                                      const std::string &provenance) {
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    Transaction tx(store->db);
    CheckMergeActive(operation);
    auto op = GetOperation(operation.id);
    if (op.state == "complete") { tx.Commit(); return op; }
    FinishMergeTargetTx(op, b, root, provenance);
    Failpoint("DIFFERENTIAL_MERGE_CRASH", "publication_transaction");
    tx.Commit();
    Failpoint("DIFFERENTIAL_MERGE_CRASH", "published");
    return op;
}

void Control::FinishMergeTargetTx(Operation &op, const Branch &b, const Root &root,
                                  const std::string &provenance, const Snapshot *published) {
    auto f = TargetArguments(op);
    auto action = MergeAction(op);
    Validate(b);
    Query intent(store->db, "SELECT expected_head,expected_epoch FROM compound_intents WHERE operation_id=? AND branch_id=?");
    intent.Text(1, op.id).Text(2, b.id);
    if (!intent.Step() || intent.Int(0) != b.head || intent.Int(1) != b.epoch || b.id != f[6])
        throw std::runtime_error("[differential:stale_epoch] Stale merge target intent");
    NoWAL(store->db, b);
    op.database = b.database;
    op.branch = b.id;
    if (action == "already_up_to_date") {
        op.snapshot = b.head;
    } else {
        // All native handles to this branch were excluded before staging. Old
        // immutable readers retain independent inodes/pins. Replace the namespace
        // and public head in the same FULL transaction, never a restore operation.
        Query(store->db, "UPDATE files SET name=NULL WHERE name IN (?,?,?,?)")
            .Text(1, b.FileName()).Text(2, b.FileName() + ".wal")
            .Text(3, b.FileName() + ".wal.checkpoint").Text(4, b.FileName() + ".wal.recovery").Run();
        auto file = store->CreateFileTx(b.FileName(), root.size, LocalDatabasePool(b.database));
        store->InstallRootTx(file, root.id);
        Failpoint("DIFFERENTIAL_MERGE_CRASH", "target_installed");
        if (action == "fast_forward" || published) {
            op.snapshot = published ? published->id : std::stoll(f[10]);
            // Pins, acquired before target ownership, keep this exact input alive.
            Query source(store->db, "SELECT root,retired FROM snapshots WHERE database_id=? AND id=?");
            source.Text(1, b.database).Int(2, op.snapshot);
            if (!source.Step() || source.Text(0) != root.id)
                throw std::runtime_error("[differential:corrupt_data] Fast-forward source root mismatch");
            if (source.Int(1))
                throw std::runtime_error("[differential:retired] Fast-forward source expired before publication");
            Query(store->db, "UPDATE branches SET root=?,head=? WHERE id=?")
                .Text(1, root.id).Int(2, op.snapshot).Text(3, b.id).Run();
            Event(b, op.snapshot, Timestamp());
        } else {
            op.snapshot = Publish(b, root, {"", "", "logical merge into target", provenance}, Timestamp(),
                                  SnapshotParents{"merge", std::stoll(f[9]), std::stoll(f[10])}).id;
        }
        Failpoint("DIFFERENTIAL_MERGE_CRASH", "ancestry_recorded");
    }
    // Fast-forward/no-op reuse existing snapshots: their provenance belongs to
    // this request, never to a rewritten source snapshot. Text roots do not retain
    // ancestor payload. The companion and completion commit atomically.
    Query(store->db, "INSERT INTO operations(id,kind,payload,state,database_id,branch_id,snapshot_id,created_at) "
                    "VALUES(?,'merge_target_result',?,'complete',?,?,?,?)")
        .Text(1, UUID()).Text(2, Payload({published ? "s3-merge-target-v1" : "local-merge-target-v1", op.id, action, provenance}))
        .Text(3, b.database).Text(4, b.id).Int(5, op.snapshot).Int(6, Timestamp()).Run();
    if (b.epoch == std::numeric_limits<int64_t>::max()) throw std::runtime_error("Writer epoch exhausted");
    Query(store->db, "UPDATE branches SET epoch=epoch+1 WHERE id=?").Text(1, b.id).Run();
    Query(store->db, "DELETE FROM compound_intents WHERE operation_id=?").Text(1, op.id).Run();
    Complete(op);
}
} // namespace differential
