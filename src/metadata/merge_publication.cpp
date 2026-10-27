#include "control_internal.hpp"
#include <openssl/evp.h>
#include <algorithm>
#include <chrono>
#include <filesystem>
#include <fcntl.h>
#include <limits>
#include <set>
#include <sys/file.h>
#include <unistd.h>

namespace differential {
using namespace control;
namespace {
// Reserved UUIDv8 identities in the existing operations table; no user request IDs.
std::string Identity(const std::string &domain, const std::string &key) {
    auto text = "differential-s3-merge-v1\n" + domain + "\n" + key;
    unsigned char bytes[32];
    unsigned length = 0;
    if (EVP_Digest(text.data(), text.size(), bytes, &length, EVP_sha256(), nullptr) != 1 || length != 32)
        throw std::runtime_error("Cannot hash merge publication identity");
    bytes[6] = (bytes[6] & 15) | 128;
    bytes[8] = (bytes[8] & 63) | 128;
    std::string id;
    const char *hex = "0123456789abcdef";
    for (size_t i = 0; i < 16; i++) {
        if (i == 4 || i == 6 || i == 8 || i == 10) id += '-';
        id += hex[bytes[i] >> 4];
        id += hex[bytes[i] & 15];
    }
    return id;
}
std::string Reservation(const std::string &database, const std::string &name) {
    return Identity("destination", database + "/" + name);
}
std::string Encode(const MergePublication &p) {
    return Payload({"s3-merge-publication-v3", p.merge_id, p.destination, p.snapshot.root,
                    std::to_string(p.snapshot.size), std::to_string(p.snapshot.published_at),
                    p.snapshot.extra_info, p.provider, p.descriptor, p.write_id,
                    std::to_string(p.ours), std::to_string(p.theirs), p.head_attempted ? "1" : "0",
                    p.action, p.expected_etag});
}
std::vector<std::string> MergeArguments(const Operation &op) {
    auto f = DecodePayload(op.payload);
    if (op.kind == "merge" && f.size() == 6) return f;
    if (op.kind == "merge_branch" && f.size() == 11)
        return {f[0], f[8], f[9], f[10], f[3], f[5]};
    throw std::runtime_error("[differential:merge_request] Not a supported merge operation");
}
bool AttemptPhase(const std::string &phase) {
    static const std::set<std::string> phases = {
        "request_locked", "context_frozen", "target_locked", "target_checked", "materializing", "pinned",
        "catalog_validated", "schema_planned", "column_drop_check", "column_drops_checked",
        "table_drop_check", "comparison", "changes_detected", "resolutions_frozen", "staging",
        "views_dropping", "views_dropped", "table_dropped", "table_rebuild", "table_inserted",
        "view_created", "views_created", "checkpointing", "staged", "prepared", "publishing",
        "uploading", "uploaded", "head_checking", "head_creating", "head_updating", "head_published", "finalizing", "completed"};
    return phases.count(phase) != 0;
}
[[noreturn]] void InvalidAttempt() {
    throw std::runtime_error("[differential:corrupt_data] Invalid merge attempt record");
}
int64_t AttemptNumber(const std::string &value) {
    try {
        auto n = std::stoll(value);
        if (n < 0 || std::to_string(n) != value) InvalidAttempt();
        return n;
    } catch (...) { InvalidAttempt(); }
}
void AttemptFault(const char *point) {
    auto fault = Hook("DIFFERENTIAL_MERGE_DIAGNOSTICS_ERROR");
    if (fault && std::string(fault) == point)
        throw std::runtime_error("[differential:disk_full] Injected merge diagnostics write failure");
}
} // namespace

std::string Control::SafeMergeErrorCode(const std::string &code) {
    // Only these constants may enter durable diagnostics. Never persist an error
    // message, SQL, key/value, object name, provider response or credential.
    static const std::set<std::string> codes = {
        "merge_error", "cancelled", "out_of_memory", "merge_timeout", "merge_limit",
        "merge_unsupported", "merge_conflict", "merge_schema_conflict", "merge_constraint",
        "merge_application", "merge_resolution", "merge_request", "merge_aborted",
        "merge_target_changed", "merge_target_dirty", "merge_publication_uncertain",
        "merge_target_unpublished", "merge_export_required", "merge_pending_operation",
        "active_attachment_or_transaction", "writer_busy", "busy", "name_conflict",
        "disk_full", "staging_full", "io_error", "corrupt_data", "not_found", "retired",
        "stale_epoch", "head_conflict", "branch_conflict", "incompatible_format", "unavailable_object"};
    return codes.count(code) ? code : "merge_error";
}

std::optional<MergeAttempt> Control::ReadMergeAttempt(const Operation &op) {
    // Called under store->mutex and a transaction. Absence is valid for requests
    // that never acquired the execution/request locks.
    Query q(store->db, "SELECT kind,payload,state,database_id FROM operations WHERE id=?");
    q.Text(1, Identity("attempt", op.id));
    if (!q.Step()) return std::nullopt;
    auto payload = q.Text(1);
    if (q.Text(0) != "merge_attempt" || q.Text(2) != "complete" ||
        q.Text(3) != MergeArguments(op)[0] || payload.size() > 1024) InvalidAttempt();
    auto f = DecodePayload(payload);
    if (f.size() != 10 || f[0] != "merge-attempt-v1" || f[1] != op.id || !IsUUID(f[2])) InvalidAttempt();
    MergeAttempt a;
    a.id = f[2]; a.count = AttemptNumber(f[3]); a.started_at = AttemptNumber(f[4]);
    a.updated_at = AttemptNumber(f[5]); a.finished_at = AttemptNumber(f[6]);
    a.state = f[7]; a.phase = f[8]; a.error_code = f[9];
    if (!a.count || !a.started_at || a.updated_at < a.started_at || !AttemptPhase(a.phase) ||
        (!a.error_code.empty() && SafeMergeErrorCode(a.error_code) != a.error_code)) InvalidAttempt();
    if (a.state == "incomplete") {
        if (a.finished_at || !a.error_code.empty() || a.phase == "completed") InvalidAttempt();
    } else if (a.state == "failed" || a.state == "succeeded") {
        if (a.finished_at != a.updated_at || (a.state == "failed") != !a.error_code.empty() ||
            (a.state == "succeeded") != (a.phase == "completed")) InvalidAttempt();
    } else InvalidAttempt();
    return a;
}

void Control::WriteMergeAttempt(const Operation &op, const MergeAttempt &a) {
    auto payload = Payload({"merge-attempt-v1", op.id, a.id, std::to_string(a.count),
        std::to_string(a.started_at), std::to_string(a.updated_at), std::to_string(a.finished_at),
        a.state, a.phase, a.error_code});
    Query(store->db, "INSERT INTO operations(id,kind,payload,state,database_id,created_at) "
                    "VALUES(?,'merge_attempt',?,'complete',?,?) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload")
        .Text(1, Identity("attempt", op.id)).Text(2, payload).Text(3, MergeArguments(op)[0])
        .Int(4, a.started_at).Run();
}

MergeAttempt Control::BeginMergeAttempt(const Operation &op) {
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    Transaction tx(store->db);
    CheckMergeActive(op);
    if (GetOperation(op.id).state == "complete")
        throw std::runtime_error("[differential:merge_request] Cannot start a completed merge");
    auto prior = ReadMergeAttempt(op);
    if (prior && prior->count == std::numeric_limits<int64_t>::max())
        throw std::runtime_error("[differential:merge_limit] Merge attempt counter exhausted");
    MergeAttempt a;
    a.id = UUID(); a.count = prior ? prior->count + 1 : 1;
    a.started_at = a.updated_at = std::max(Timestamp(), prior ? prior->updated_at : int64_t(1));
    a.state = "incomplete"; a.phase = "request_locked";
    AttemptFault("begin");
    WriteMergeAttempt(op, a);
    tx.Commit();
    return a;
}

void Control::UpdateMergeAttempt(const Operation &op, const std::string &id, const std::string &phase,
                                 const std::string &error_code) {
    if (!AttemptPhase(phase) || phase == "completed") InvalidAttempt();
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    Transaction tx(store->db);
    auto a = ReadMergeAttempt(op);
    // A failure after commit cannot turn a published result into a failed one.
    // The attempt token also prevents a stale observer overwriting a retry.
    if (GetOperation(op.id).state != "complete" && a && a->id == id && a->state == "incomplete") {
        a->phase = phase; a->updated_at = std::max(Timestamp(), a->updated_at);
        if (!error_code.empty()) {
            a->state = "failed"; a->error_code = SafeMergeErrorCode(error_code);
            a->finished_at = a->updated_at;
        }
        AttemptFault(error_code.empty() ? "progress" : "failure");
        WriteMergeAttempt(op, *a);
    }
    tx.Commit();
}

void Control::CompleteMergeAttempt(const Operation &op) {
    // Part of the caller's publication transaction, including S3 recovery via
    // abort. Do not backfill history for requests written before diagnostics.
    auto a = ReadMergeAttempt(op);
    if (!a || a->state == "succeeded") return;
    a->state = "succeeded"; a->phase = "completed"; a->error_code.clear();
    a->updated_at = a->finished_at = std::max(Timestamp(), a->updated_at);
    AttemptFault("complete");
    WriteMergeAttempt(op, *a);
}

std::shared_ptr<BranchLock> Control::LockMerge(const Operation &op, const std::function<void()> &check) {
    if (!IsUUID(op.id)) throw std::runtime_error("[differential:merge_request] Invalid merge operation ID");
    auto lock = std::make_shared<BranchLock>();
    auto path = std::filesystem::path(store->directory) / "locks";
    store->permissions.CreateDirectories(path);
    lock->fd = store->permissions.Open(path / ("merge-" + op.id), O_RDWR | O_CLOEXEC);
    if (lock->fd < 0) throw std::runtime_error("Cannot lock merge request");
    while (flock(lock->fd, LOCK_EX | LOCK_NB)) {
        if (errno != EWOULDBLOCK && errno != EAGAIN && errno != EINTR)
            throw std::runtime_error("Cannot lock merge request");
        check();
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    check();
    return lock;
}

bool Control::MergeAborted(const Operation &op) {
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    Query q(store->db, "SELECT kind,payload,state FROM operations WHERE id=?");
    q.Text(1, Identity("aborted", op.id));
    if (!q.Step()) return false;
    if (q.Text(0) != "merge_aborted" || q.Text(1) != op.id || q.Text(2) != "complete")
        throw std::runtime_error("[differential:corrupt_data] Invalid merge abort marker");
    return true;
}
void Control::CheckMergeActive(const Operation &op) {
    if (MergeAborted(op))
        throw std::runtime_error("[differential:merge_aborted] Merge request was aborted; use a new request_id");
}
std::pair<std::string, std::string> Control::MergeSchemaPolicy(const Operation &op) {
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    Query q(store->db, "SELECT kind,payload,state FROM operations WHERE id=?");
    q.Text(1, Identity("schema", op.id));
    if (!q.Step()) throw std::runtime_error("[differential:corrupt_data] Merge schema policy missing");
    auto f = DecodePayload(q.Text(1));
    if (q.Text(0) != "merge_schema" || q.Text(2) != "complete" || f.size() != 4 ||
        f[0] != "merge-schema-policy-v1" || f[1] != op.id ||
        (f[2] != "strict" && f[2] != "additive" && f[2] != "catalog"))
        throw std::runtime_error("[differential:corrupt_data] Invalid merge schema policy");
    return {f[2], f[3]};
}
std::string Control::CurrentMergeSchemaAlgorithm(const std::string &mode) {
    if (mode != "strict" && mode != "additive" && mode != "catalog")
        throw std::runtime_error("[differential:merge_unsupported] Unknown schema_mode");
    return mode == "strict" ? "strict-v1" : mode == "additive" ? "additive-tables-v2" : "catalog-v1";
}
std::pair<std::string, std::string> Control::MergeExpressionContext(const Operation &op,
                                                                 const std::string &timezone,
                                                                 const std::string &calendar) {
    // Caller holds the request lock. No snapshot roots or arbitrary settings (and
    // especially no credentials) enter this bounded native-expression context.
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    Transaction tx(store->db);
    CheckMergeActive(op);
    auto id = Identity("expression-context", op.id);
    Query q(store->db, "SELECT kind,payload,state,database_id FROM operations WHERE id=?");
    q.Text(1, id);
    auto database = MergeArguments(op)[0];
    if (q.Step()) {
        auto payload = q.Text(1);
        if (payload.size() > 4096) throw std::runtime_error("[differential:corrupt_data] Oversized merge expression context");
        auto f = DecodePayload(payload);
        if (q.Text(0) != "merge_expression_context" || q.Text(2) != "complete" || q.Text(3) != database ||
            f.size() != 4 || f[1] != op.id || f[2].size() > 1024 || f[3].size() > 1024)
            throw std::runtime_error("[differential:corrupt_data] Invalid merge expression context");
        if (f[0] != "native-expression-context-v1")
            throw std::runtime_error("[differential:merge_unsupported] Merge expression context version is unavailable");
        tx.Commit();
        return {f[2], f[3]};
    }
    if (timezone.size() > 1024 || calendar.size() > 1024)
        throw std::runtime_error("[differential:merge_limit] Merge expression setting exceeds 1024 bytes");
    Query(store->db, "INSERT INTO operations(id,kind,payload,state,database_id,created_at) "
                    "VALUES(?,'merge_expression_context',?,'complete',?,?)")
        .Text(1, id).Text(2, Payload({"native-expression-context-v1", op.id, timezone, calendar}))
        .Text(3, database).Int(4, Timestamp()).Run();
    tx.Commit();
    return {timezone, calendar};
}
void Control::ReserveMergeSchema(const Operation &op, const std::string &mode, bool create) {
    // Called inside BeginMerge/ResolveMergeBranches' FULL transaction. A completed
    // companion freezes policy, not a result. No input/ancestor retention is added.
    auto version = CurrentMergeSchemaAlgorithm(mode);
    Query exists(store->db, "SELECT 1 FROM operations WHERE id=?");
    exists.Text(1, Identity("schema", op.id));
    if (exists.Step() || !create) {
        auto prior = MergeSchemaPolicy(op);
        if (prior.first != mode) throw std::runtime_error("Request ID reused with different schema_mode");
        if (op.state != "complete" && prior.second != version && !PendingMergePublication(op))
            throw std::runtime_error("[differential:merge_unsupported] Pending schema merge algorithm is unavailable");
        return;
    }
    Query(store->db, "INSERT INTO operations(id,kind,payload,state,database_id,created_at) "
                    "VALUES(?,'merge_schema',?,'complete',?,?)")
        .Text(1, Identity("schema", op.id)).Text(2, Payload({"merge-schema-policy-v1", op.id, mode, version}))
        .Text(3, MergeArguments(op)[0]).Int(4, Timestamp()).Run();
}
void MergeExpectedInputs::Validate() const {
    if (Enabled() && (base <= 0 || ours <= 0 || theirs <= 0))
        throw std::runtime_error("[differential:merge_expected_inputs] Supply all three positive expected snapshot IDs");
}
void MergeExpectedInputs::Check(int64_t actual_base, int64_t actual_ours, int64_t actual_theirs) const {
    Validate();
    if (Enabled() && (base != actual_base || ours != actual_ours || theirs != actual_theirs))
        throw std::runtime_error("[differential:merge_inputs_changed] Branch merge inputs differ from the reviewed snapshots; preview again (selected base=" +
            std::to_string(actual_base) + ", ours=" + std::to_string(actual_ours) + ", theirs=" + std::to_string(actual_theirs) + ")");
}
void Control::ReserveMergeExpectations(const Operation &op, const MergeExpectedInputs &expected, bool create) {
    expected.Validate();
    auto payload = Payload({"merge-expected-inputs-v1", op.id, std::to_string(expected.base),
                            std::to_string(expected.ours), std::to_string(expected.theirs)});
    Query q(store->db, "SELECT kind,payload,state FROM operations WHERE id=?");
    q.Text(1, Identity("expected-inputs", op.id));
    if (q.Step()) {
        if (q.Text(0) != "merge_expected_inputs" || q.Text(2) != "complete")
            throw std::runtime_error("[differential:corrupt_data] Invalid expected-input record");
        if (q.Text(1) != payload)
            throw std::runtime_error("Request ID reused with different expected snapshot IDs");
    } else if (expected.Enabled()) {
        if (!create) throw std::runtime_error("Request ID reused with different expected snapshot IDs");
        Query(store->db, "INSERT INTO operations(id,kind,payload,state,database_id,created_at) "
                        "VALUES(?,'merge_expected_inputs',?,'complete',?,?)")
            .Text(1, Identity("expected-inputs", op.id)).Text(2, payload)
            .Text(3, MergeArguments(op)[0]).Int(4, Timestamp()).Run();
    }
    // Absence is the unguarded identity, including requests from before this API.
}

Operation Control::FindMergeOperation(const std::string &database, const std::string &id) {
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    auto op = GetOperation(id);
    if (MergeArguments(op)[0] != DatabaseID(database))
        throw std::runtime_error("[differential:merge_request] Merge operation belongs to another database");
    return op;
}

std::vector<MergeRequest> Control::MergeRequests(const std::string &database, const std::string &request,
                                               const std::string &after, int64_t limit,
                                               const std::function<void()> &check) {
    if (limit < 1 || limit > 1000)
        throw std::runtime_error("[differential:merge_limit] Merge request limit must be 1..1000");
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    // One consistent metadata view; no input pins, decoding, credentials or remote I/O.
    Transaction tx(store->db, "DIFFERENTIAL_CONTROL_CRASH", false);
    auto db = DatabaseID(database);
    Query q(store->db, "SELECT id,request,kind,payload,state,database_id,branch_id,snapshot_id,preservation,created_at "
                      "FROM operations WHERE kind IN ('merge','merge_branch') AND id>? AND (?='' OR request=?) ORDER BY id");
    q.Text(1, after).Text(2, request).Text(3, request);
    std::vector<MergeRequest> rows;
    while (rows.size() < size_t(limit) && q.Step()) {
        check();
        MergeRequest r;
        r.operation = ReadOperation(q);
        auto f = MergeArguments(r.operation);
        if (f[0] != db) continue;
        auto policy = MergeSchemaPolicy(r.operation);
        r.schema_mode = policy.first; r.schema_algorithm = policy.second;
        r.action = MergeAction(r.operation);
        r.database = db; r.destination = f[4]; r.created_at = q.Int(9);
        r.base = std::stoll(f[1]); r.ours = std::stoll(f[2]); r.theirs = std::stoll(f[3]);
        if (r.operation.kind == "merge_branch") {
            auto branch_args = DecodePayload(r.operation.payload);
            r.ours_branch_name = branch_args[1]; r.theirs_branch_name = branch_args[2];
            r.ours_branch_id = branch_args[6]; r.theirs_branch_id = branch_args[7];
            Query expected(store->db, "SELECT 1 FROM operations WHERE id=?");
            expected.Text(1, Identity("expected-inputs", r.operation.id));
            r.inputs_guarded = expected.Step();
            if (r.inputs_guarded) ReserveMergeExpectations(r.operation, {r.base, r.ours, r.theirs}, false);
        }
        r.attempt = ReadMergeAttempt(r.operation);
        auto p = PendingMergePublication(r.operation);
        const bool aborted = MergeAborted(r.operation);
        if (p) {
            r.result = p->snapshot.id; r.root = p->snapshot.root;
            r.descriptor = p->descriptor; r.head_attempted = p->head_attempted;
        }
        if (aborted) {
            r.state = "aborted"; r.recovery = "Use a new request_id for a new merge; immutable uploads are not deleted";
        } else if (r.operation.state == "complete") {
            r.state = "complete"; r.result = r.operation.snapshot;
            Query root(store->db, "SELECT root FROM snapshots WHERE database_id=? AND id=?");
            root.Text(1, db).Int(2, r.result);
            if (root.Step()) r.root = root.Text(0);
            r.recovery = "Replay the original request to return its completed result";
        } else {
            r.state = p ? (p->head_attempted ? "publication_uncertain" : p->descriptor.empty() ? "prepared" : "uploaded") : "pending";
            r.retained_bytes = p ? p->snapshot.size : 0;
            r.reserved = bool(p);
            r.recovery = p && p->head_attempted
                ? "Retry the merge; abort can recover an existing head but cannot discard an uncertain write"
                : p ? "Retry to publish the retained result, or abort to release its reservation and local retention"
                    : "Retry with the original inputs and arguments, or abort";
        }
        if (!aborted && r.operation.state != "complete") {
            Query target(store->db, "SELECT 1 FROM compound_intents WHERE operation_id=?");
            target.Text(1, r.operation.id);
            if (!p && r.action != "created_branch" && target.Step()) {
                r.state = "target_locked"; r.reserved = true;
                r.recovery = "Target is fenced; retry with original inputs, or abort to release it";
            }
        }
        rows.push_back(std::move(r));
    }
    tx.Commit();
    return rows;
}

Operation Control::BeginAbortMerge(const Operation &merge, const std::string &request) {
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    Transaction tx(store->db);
    auto f = MergeArguments(merge);
    auto abort = Begin("abort_merge", Payload({f[0], merge.id, "merge-abort-v1"}), request);
    tx.Commit();
    return abort;
}

Operation Control::FinishAbortMerge(const Operation &action, const Operation &merge) {
    // Caller holds the request lock and, for a prepared S3 result, has checked the
    // remote head. No other publisher can start or advance until this FULL commit.
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    Transaction tx(store->db);
    auto abort = GetOperation(action.id);
    if (abort.state == "complete") { tx.Commit(); return abort; }
    auto op = GetOperation(merge.id);
    auto f = MergeArguments(op);
    if (abort.kind != "abort_merge" || DecodePayload(abort.payload) !=
            std::vector<std::string>{f[0], op.id, "merge-abort-v1"})
        throw std::runtime_error("[differential:corrupt_data] Invalid merge abort action");
    abort.database = f[0];
    if (op.state == "complete") {
        abort.branch = op.branch; abort.snapshot = op.snapshot;
    } else if (!MergeAborted(op)) {
        Query(store->db, "INSERT INTO operations(id,kind,payload,state,database_id,created_at) "
                        "VALUES(?,'merge_aborted',?,'complete',?,?)")
            .Text(1, Identity("aborted", op.id)).Text(2, op.id).Text(3, f[0]).Int(4, Timestamp()).Run();
        if (auto p = PendingMergePublication(op)) {
            Query(store->db, "UPDATE export_roots SET state='abandoned' WHERE export_id=? AND state='pending'")
                .Text(1, p->record.id).Run();
            Query(store->db, "UPDATE operations SET state='complete' WHERE id=? AND payload=?")
                .Text(1, Reservation(f[0], f[4])).Text(2, op.id).Run();
            // Completion of the companion means its work is terminal, not published.
            Complete(p->record);
        }
    }
    // The request lock excludes a running target owner/publisher. Abort needs
    // neither the input payload nor ownership of any unrelated branch.
    Query(store->db, "DELETE FROM compound_intents WHERE operation_id=?").Text(1, op.id).Run();
    Complete(abort);
    Failpoint("DIFFERENTIAL_MERGE_CRASH", "abort_transaction");
    tx.Commit();
    Failpoint("DIFFERENTIAL_MERGE_CRASH", "aborted");
    return abort;
}

std::pair<Snapshot, std::shared_ptr<Pin>> Control::PinMergeInput(const std::string &database, int64_t snapshot) {
    auto pin = NewPin();
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    Transaction tx(store->db, "DIFFERENTIAL_CONTROL_CRASH", false);
    auto s = GetSnapshot(database, snapshot);
    // An evicted root's map may already be reclaimed. Retain its metadata and any
    // surviving pages, then reconstruct it from the frozen immutable descriptor.
    if (!s.evicted) store->RootSizeTx(s.root);
    Query(store->db, "UPDATE pins SET root=? WHERE id=?").Text(1, s.root).Text(2, pin->id).Run();
    pin->root = s.root;
    tx.Commit();
    return {s, pin};
}

std::string Control::MergeDestinationOwner(const std::string &database, const std::string &name) {
    Query q(store->db, "SELECT kind,state,payload FROM operations WHERE id=?");
    q.Text(1, Reservation(database, name));
    if (!q.Step()) return "";
    if (q.Text(0) != "merge_destination")
        throw std::runtime_error("[differential:corrupt_data] Invalid merge destination reservation");
    return q.Text(1) == "pending" ? q.Text(2) : "";
}
void Control::CheckMergeDestination(const std::string &database, const std::string &name, const std::string &owner) {
    auto reserved = MergeDestinationOwner(database, name);
    if (!reserved.empty() && reserved != owner)
        throw std::runtime_error("[differential:name_conflict] Destination reserved by a pending S3 merge; retry its request ID");
}

std::optional<MergePublication> Control::PendingMergePublication(const Operation &op) {
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    Query q(store->db, (std::string(OP_COLUMNS) + " WHERE id=?").c_str());
    q.Text(1, Identity("publication", op.id));
    if (!q.Step()) return std::nullopt;
    MergePublication p;
    p.record = ReadOperation(q);
    auto f = DecodePayload(p.record.payload);
    if (p.record.kind != "merge_publication" || f.size() != 15 || f[0] != "s3-merge-publication-v3" ||
        (f[12] != "0" && f[12] != "1") || f[1] != op.id)
        throw std::runtime_error("[differential:corrupt_data] Invalid merge publication record");
    p.merge_id = f[1]; p.destination = f[2]; p.snapshot.root = f[3];
    p.snapshot.size = std::stoll(f[4]);
    p.snapshot.published_at = p.snapshot.captured_at = std::stoll(f[5]);
    p.snapshot.extra_info = f[6]; p.provider = f[7]; p.descriptor = f[8]; p.write_id = f[9];
    p.ours = std::stoll(f[10]); p.theirs = std::stoll(f[11]);
    p.head_attempted = f[12] == "1";
    p.action = f[13]; p.expected_etag = f[14];
    if (p.action != MergeAction(op) || (p.action == "created_branch") != p.expected_etag.empty() ||
        (p.action == "already_up_to_date" && p.head_attempted))
        throw std::runtime_error("[differential:corrupt_data] Invalid S3 merge target publication");
    p.snapshot.database = p.record.database; p.snapshot.branch = p.record.branch; p.snapshot.id = p.record.snapshot;
    p.snapshot.message = "logical merge";
    Query name(store->db, "SELECT name FROM databases WHERE id=?");
    name.Text(1, p.record.database);
    if (!name.Step()) throw std::runtime_error("[differential:corrupt_data] Merge database missing");
    p.snapshot.database_name = name.Text(0);
    return p;
}

MergePublication Control::PrepareMergePublication(const Operation &operation, const Root &root,
                                                  const std::string &provenance, const std::string &expected_etag) {
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    Transaction tx(store->db);
    CheckMergeActive(operation);
    if (auto prior = PendingMergePublication(operation)) {
        tx.Commit();
        return *prior;
    }
    auto op = GetOperation(operation.id);
    auto f = MergeArguments(op);
    if (op.state != "pending" || f[5] != "logical-cell-v1")
        throw std::runtime_error("[differential:corrupt_data] Invalid S3 merge request");
    auto provider = Profile(f[0]);
    if (provider == "local" || Writers(f[0]) != "single")
        throw std::runtime_error("[differential:merge_unsupported] S3 merge requires single-writer storage");
    MergePublication p;
    p.action = MergeAction(op); p.expected_etag = expected_etag;
    const bool target = p.action != "created_branch";
    const bool reuse = p.action == "fast_forward" || p.action == "already_up_to_date";
    if (target == expected_etag.empty())
        throw std::runtime_error("[differential:corrupt_data] Invalid S3 merge expected head");
    CheckMergeDestination(f[0], f[4], op.id);
    if (target) {
        auto args = DecodePayload(op.payload);
        auto b = GetBranch(f[0], args[6], true);
        Query intent(store->db, "SELECT 1 FROM compound_intents WHERE operation_id=? AND branch_id=? "
                               "AND expected_head=? AND expected_epoch=?");
        intent.Text(1, op.id).Text(2, b.id).Int(3, std::stoll(f[2])).Int(4, b.epoch);
        if (!intent.Step() || b.head != std::stoll(f[2]))
            throw std::runtime_error("[differential:stale_epoch] S3 merge target fence changed");
        p.record.branch = b.id;
    } else {
        Query exists(store->db, "SELECT 1 FROM branches WHERE database_id=? AND name=? AND state='active'");
        exists.Text(1, f[0]).Text(2, f[4]);
        if (exists.Step()) throw std::runtime_error("[differential:name_conflict] Merge destination already exists");
        p.record.branch = UUID();
    }
    if (store->RootSizeTx(root.id) != root.size)
        throw std::runtime_error("[differential:corrupt_data] Merge root size mismatch");
    p.merge_id = op.id; p.destination = f[4]; p.provider = provider;
    p.ours = std::stoll(f[2]); p.theirs = std::stoll(f[3]);
    p.write_id = UUID();
    p.write_id.erase(std::remove(p.write_id.begin(), p.write_id.end(), '-'), p.write_id.end());
    p.record.id = Identity("publication", op.id);
    p.record.database = p.snapshot.database = f[0];
    Query database(store->db, "SELECT name,last_snapshot FROM databases WHERE id=?");
    database.Text(1, f[0]); database.Step();
    if (reuse) {
        p.snapshot = GetSnapshot(f[0], p.action == "fast_forward" ? p.theirs : p.ours);
        if (p.snapshot.root != root.id || p.snapshot.size != int64_t(root.size))
            throw std::runtime_error("[differential:corrupt_data] Merge reused snapshot root mismatch");
        auto e = Export(f[0], p.snapshot.id);
        if (e.state != "complete" || e.descriptor.empty())
            throw std::runtime_error("[differential:merge_export_required] Fast-forward/no-op requires a completed input export; retry its export first");
        p.descriptor = e.descriptor;
    } else {
        if (database.Int(1) >= 9999999999LL) throw std::runtime_error("S3 snapshot ID exhausted");
        p.snapshot.id = database.Int(1) + 1;
        p.snapshot.root = root.id; p.snapshot.size = int64_t(root.size);
        p.snapshot.published_at = p.snapshot.captured_at = Timestamp();
        // Reserve a number, not a public snapshot. Ordinary snapshots skip it.
        Query(store->db, "UPDATE databases SET last_snapshot=? WHERE id=?")
            .Int(1, p.snapshot.id).Text(2, f[0]).Run();
    }
    p.record.snapshot = p.snapshot.id;
    p.snapshot.database_name = database.Text(0);
    p.snapshot.branch = p.record.branch;
    p.snapshot.message = "logical merge"; p.snapshot.extra_info = provenance;
    Query(store->db, "INSERT INTO operations(id,kind,payload,state,database_id,branch_id,snapshot_id,created_at) "
                     "VALUES(?,'merge_publication',?,'pending',?,?,?,?)")
        .Text(1, p.record.id).Text(2, Encode(p)).Text(3, f[0]).Text(4, p.record.branch)
        .Int(5, p.snapshot.id).Int(6, Timestamp()).Run();
    Query(store->db, "INSERT INTO operations(id,kind,payload,state,database_id,created_at) "
                     "VALUES(?,'merge_destination',?,'pending',?,?) ON CONFLICT(id) DO UPDATE SET "
                     "payload=excluded.payload,state='pending',created_at=excluded.created_at")
        .Text(1, Reservation(f[0], f[4])).Text(2, op.id).Text(3, f[0]).Int(4, Timestamp()).Run();
    Query(store->db, "INSERT INTO export_roots VALUES(?,?,'pending')").Text(1, p.record.id).Text(2, root.id).Run();
    tx.Commit();
    return *PendingMergePublication(op);
}

void Control::MergeUploadComplete(const Operation &op, const std::string &descriptor) {
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    Transaction tx(store->db);
    CheckMergeActive(op);
    auto p = PendingMergePublication(op);
    if (!p || descriptor.empty() || (!p->descriptor.empty() && p->descriptor != descriptor))
        throw std::runtime_error("[differential:corrupt_data] Merge export identity mismatch");
    p->descriptor = descriptor;
    Query(store->db, "UPDATE operations SET payload=? WHERE id=?").Text(1, Encode(*p)).Text(2, p->record.id).Run();
    // Keep the export root pending until local branch publication, even after upload.
    tx.Commit();
}

void Control::MergeHeadAttempt(const Operation &op) {
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    Transaction tx(store->db);
    CheckMergeActive(op);
    auto p = PendingMergePublication(op);
    if (!p || p->descriptor.empty()) throw std::runtime_error("[differential:corrupt_data] Merge upload missing");
    p->head_attempted = true;
    Query(store->db, "UPDATE operations SET payload=? WHERE id=?").Text(1, Encode(*p)).Text(2, p->record.id).Run();
    tx.Commit();
}

Operation Control::FinishMergePublication(const Operation &operation) {
    std::lock_guard<std::recursive_mutex> guard(store->mutex);
    Transaction tx(store->db);
    CheckMergeActive(operation);
    auto op = GetOperation(operation.id);
    if (op.state == "complete") { tx.Commit(); return op; }
    auto p = PendingMergePublication(op);
    if (!p || p->descriptor.empty()) throw std::runtime_error("[differential:corrupt_data] Merge upload missing");
    auto &s = p->snapshot;
    CheckMergeDestination(s.database, p->destination, op.id);
    Query(store->db, "UPDATE operations SET state='complete' WHERE id=?")
        .Text(1, Reservation(s.database, p->destination)).Run();
    const bool target = p->action != "created_branch";
    const bool reuse = p->action == "fast_forward" || p->action == "already_up_to_date";
    auto b = target ? GetBranch(s.database, s.branch, true)
                    : NewBranch(s.database, p->destination, s.root, s.id, s.database, p->ours, s.branch);
    if (!reuse) {
        Query(store->db, "INSERT INTO snapshots(database_id,id,branch_id,root,published_at,captured_at,commit_message,"
                         "commit_extra_info) VALUES(?,?,?,?,?,?,?,?)")
            .Text(1, s.database).Int(2, s.id).Text(3, s.branch).Text(4, s.root).Int(5, s.published_at)
            .Int(6, s.captured_at).Text(7, s.message).Text(8, s.extra_info).Run();
        RecordAncestry(s, {"merge", p->ours, p->theirs});
        Query(store->db, "INSERT INTO exports(id,provider_id,database_id,snapshot_id,descriptor) VALUES(?,?,?,?,?)")
            .Text(1, p->record.id).Text(2, p->provider).Text(3, s.database).Int(4, s.id).Text(5, p->descriptor).Run();
    }
    Query(store->db, "UPDATE export_roots SET state='complete' WHERE export_id=?").Text(1, p->record.id).Run();
    if (target) {
        FinishMergeTargetTx(op, b, {s.root, uint64_t(s.size)}, s.extra_info, &s);
    } else {
        Event(b, s.id, s.published_at);
        op.database = s.database; op.branch = s.branch; op.snapshot = s.id;
        Complete(op);
    }
    Complete(p->record);
    Failpoint("DIFFERENTIAL_MERGE_CRASH", "publication_transaction");
    tx.Commit();
    Failpoint("DIFFERENTIAL_MERGE_CRASH", "published");
    return op;
}
} // namespace differential
