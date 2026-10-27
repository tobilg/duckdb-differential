// SQL management surface, aligned with DuckLake conventions: everything that is not
// ATTACH or ordinary SQL is a table function whose first argument names the database (an
// attachment alias or a database name/UUID), with named parameters for options. All effects
// happen once, at execution; binding and planning never touch the repository.
#include "differential_metrics.hpp"
#include "differential_hooks.hpp"
#include "duckdb.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/transaction/transaction_manager.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "differential_control.hpp"
#include "differential_repository.hpp"
#include "differential_export.hpp"
#include "differential_error.hpp"
#include "differential_recovery.hpp"
#include "sql_options.hpp"
#include "secrets.hpp"
#include "remote_cache.hpp"
#include "differential_catalog.hpp"
#include "follow.hpp"
#include "differential_shipping.hpp"
#include "logical_merge.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include <openssl/evp.h>
#include <array>
#include <chrono>
#include <filesystem>

namespace duckdb {
namespace {
enum class Command {
    // introspection (read-only)
    SNAPSHOTS,
    CURRENT_SNAPSHOT,
    BRANCHES,
    DATABASES,
    OPERATIONS,
    PROVIDERS,
    ROOTS,
    REMOTE_HEADS,
    WRITERS,
    FOLLOWERS,
    STATS,
    MERGE_STATUS,
    MERGE_ANCESTRY,
    MERGE_RESOLUTIONS,
    MERGE_REQUESTS,
    MERGE_PREVIEW,
    MERGE_BRANCH_PREFLIGHT,
    MERGE_BRANCH_PREVIEW,
    // lifecycle
    CREATE_SNAPSHOT,
    CREATE_BRANCH,
    CLONE_DATABASE,
    RESTORE,
    DROP_BRANCH,
    EXPIRE_SNAPSHOTS,
    EVICT_SNAPSHOTS,
    RETRY_EXPORT,
    CLAIM_HEADS,
    RESOLVE_CONFLICT,
    IMPORT_DATABASE,
    RECOVER_EXPORT,
    RECOVER_BRANCH,
    MERGE,
    MERGE_BRANCH,
    ABORT_MERGE,
    // maintenance
    CLEANUP_OLD_FILES,
    DELETE_ORPHANED_FILES,
    COMPACT,
    CLEANUP_REMOTE_FILES,
    CLEANUP_METADATA
};
bool ReadOnly(Command c) { return c <= Command::MERGE_BRANCH_PREVIEW; }
bool IsMerge(Command c) {
    return c == Command::MERGE || c == Command::MERGE_PREVIEW ||
           c == Command::MERGE_BRANCH || c == Command::MERGE_BRANCH_PREVIEW || c == Command::MERGE_BRANCH_PREFLIGHT;
}
struct Info : TableFunctionInfo {
    std::shared_ptr<differential::Repository> repository;
    Command command;
    // Catalog-scoped form (sales.snapshots()): the owning catalog is the implicit first argument.
    optional_ptr<Catalog> catalog;
    Info(std::shared_ptr<differential::Repository> repository, Command command)
        : repository(std::move(repository)), command(command) {}
};
struct Binding : TableFunctionData {
    std::shared_ptr<differential::Repository> repository;
    Command command;
    vector<Value> args;
    vector<LogicalType> types;
    string request;
    string after;
    differential::SnapshotInfo snapshot_info;
    std::optional<int64_t> snapshot, older_than;
    vector<int64_t> versions;
    string branch, keep;
    bool dry_run = false, cleanup_all = false, defer_export = false, full_refresh = false, same_identity = false;
    int64_t limit = 1000;
    MergeOptions merge;
    DataPathOptions data;
};
struct State : GlobalTableFunctionState {
    bool executed = false;
    idx_t offset = 0;
    vector<vector<Value>> rows;
    std::unique_ptr<differential::MetadataCursor> cursor;
};
Value Optional(const string &s) { return s.empty() ? Value(LogicalType::VARCHAR) : Value(s); }
Value Time(int64_t t) { return t ? Value::TIMESTAMPTZ(timestamp_tz_t(t)) : Value(LogicalType::TIMESTAMP_TZ); }
const LogicalType V = LogicalType::VARCHAR, I = LogicalType::BIGINT, T = LogicalType::TIMESTAMP_TZ,
                  B = LogicalType::BOOLEAN;
// DuckLake's snapshots() columns first, then differential-specific ones.
vector<Identifier> SnapshotNames() {
    return {"snapshot_id",       "snapshot_time",  "snapshot_name",      "author",       "commit_message",
            "commit_extra_info", "branch_id",      "captured_at",        "root",         "logical_size",
            "retired",           "publication_state", "export_state",    "export_locator", "uri",
            "database_id",       "database_name",  "source_database_id", "source_snapshot_id"};
}
vector<LogicalType> SnapshotTypes() { return {I, T, V, V, V, V, V, T, V, I, B, V, V, V, V, V, V, V, I}; }
vector<Value> SnapshotRow(const differential::Snapshot &s) {
    return {Value::BIGINT(s.id),
            Time(s.published_at),
            Optional(s.name),
            Optional(s.author),
            Optional(s.message),
            Optional(s.extra_info),
            Value(s.branch),
            Time(s.captured_at),
            Optional(s.root),
            s.remote ? Value(LogicalType::BIGINT) : Value::BIGINT(s.size),
            Value::BOOLEAN(s.retired),
            Value(s.remote ? "remote" : s.evicted ? "local_evicted" : "local_published"),
            Value(s.export_state),
            Optional(s.export_key),
            Value(s.URI()),
            Value(s.database),
            Value(s.database_name),
            Optional(s.source_database),
            Value::BIGINT(s.source_snapshot)};
}
unique_ptr<FunctionData> Bind(ClientContext &, TableFunctionBindInput &input, vector<LogicalType> &types,
                              vector<Identifier> &names) {
    auto &info = input.info->Cast<Info>();
    auto b = make_uniq<Binding>();
    b->repository = info.repository;
    b->command = info.command;
    if (info.command == Command::MERGE_BRANCH_PREFLIGHT) b->merge.validate = true;
    b->args = input.inputs;
    if (info.catalog)
        b->args.insert(b->args.begin(), Value(info.catalog->GetName()));
    for (auto &v : b->args)
        if (v.IsNull())
            throw InvalidInputException("Required management argument cannot be NULL");
    for (auto &entry : input.named_parameters) {
        auto name = StringUtil::Lower(entry.first.GetIdentifierName());
        auto &value = entry.second;
        const bool expected_id = name == "expected_base_snapshot_id" || name == "expected_ours_snapshot_id" || name == "expected_theirs_snapshot_id";
        if (expected_id && (value.IsNull() || value.GetValue<int64_t>() <= 0))
            throw InvalidInputException("Expected snapshot IDs must be positive and not NULL");
        if (value.IsNull())
            continue;
        if (name == "expected_base_snapshot_id") b->merge.expected.base = value.GetValue<int64_t>();
        else if (name == "expected_ours_snapshot_id") b->merge.expected.ours = value.GetValue<int64_t>();
        else if (name == "expected_theirs_snapshot_id") b->merge.expected.theirs = value.GetValue<int64_t>();
        else if (name == "request_id")
            b->request = value.ToString();
        else if (name == "after_operation_id")
            b->after = value.ToString();
        else if (name == "schema_mode") {
            b->merge.schema_mode = StringUtil::Lower(value.ToString());
            if (b->merge.schema_mode != "strict" && b->merge.schema_mode != "additive" && b->merge.schema_mode != "catalog")
                throw InvalidInputException("schema_mode must be 'strict', 'additive' or 'catalog'");
        }
        else if (name == "name")
            b->snapshot_info.name = value.ToString();
        else if (name == "author")
            b->snapshot_info.author = value.ToString();
        else if (name == "message")
            b->snapshot_info.message = value.ToString();
        else if (name == "extra_info")
            b->snapshot_info.extra_info = value.ToString();
        else if (name == "snapshot")
            b->snapshot = value.GetValue<int64_t>();
        else if (name == "branch")
            b->branch = value.ToString();
        else if (name == "versions")
            for (auto &item : ListValue::GetChildren(value))
                b->versions.push_back(item.GetValue<int64_t>());
        else if (name == "older_than")
            b->older_than = value.GetValue<timestamp_tz_t>().value;
        else if (name == "keep") {
            b->keep = StringUtil::Lower(value.ToString());
            if (b->keep != "branch" && b->keep != "remote")
                throw InvalidInputException("keep must be 'branch' or 'remote'");
        } else if (name == "refresh") {
            auto refresh = StringUtil::Lower(value.ToString());
            if (refresh != "incremental" && refresh != "full")
                throw InvalidInputException("refresh must be 'incremental' or 'full'");
            b->full_refresh = refresh == "full";
        } else if (name == "dry_run")
            b->dry_run = value.GetValue<bool>();
        else if (name == "cleanup_all")
            b->cleanup_all = value.GetValue<bool>();
        else if (name == "same_identity")
            b->same_identity = value.GetValue<bool>();
        else if (name == "max_objects" || name == "max_pages" || name == "max_results")
            b->limit = value.GetValue<int64_t>();
        else if (name == "resolution_tables") {
            for (auto &pair : ListValue::GetChildren(value)) {
                auto &fields = StructValue::GetChildren(pair);
                if (fields[0].IsNull() || fields[1].IsNull())
                    throw InvalidInputException("resolution_tables entries must not be NULL");
                b->merge.resolution_tables.emplace(fields[0].GetValue<string>(), fields[1].GetValue<string>());
            }
        }
        else if (name == "conflict_limit")
            b->merge.conflict_limit = value.GetValue<int64_t>();
        else if (name == "conflict_offset")
            b->merge.conflict_offset = value.GetValue<int64_t>();
        else if (name == "conflict_bytes")
            b->merge.conflict_bytes = value.GetValue<int64_t>();
        else if (name == "validate")
            b->merge.validate = value.GetValue<bool>();
        else if (name == "max_input_rows")
            b->merge.max_input_rows = value.GetValue<int64_t>();
        else if (name == "max_input_bytes")
            b->merge.max_input_bytes = value.GetValue<int64_t>();
        else if (name == "max_stage_write_bytes")
            b->merge.max_stage_write_bytes = value.GetValue<int64_t>();
        else if (name == "timeout_ms")
            b->merge.timeout_ms = value.GetValue<int64_t>();
        else if (name == "max_ancestry_nodes")
            b->merge.max_ancestry_nodes = value.GetValue<int64_t>();
        else if (name == "export_mode") {
            auto mode = value.ToString();
            if (mode != "sync" && mode != "deferred")
                throw InvalidInputException("export_mode must be 'sync' or 'deferred'");
            b->defer_export = mode == "deferred";
        } else if (!b->data.Take(name, value))
            throw InvalidInputException("Unknown parameter %s", name);
    }
    if (info.command == Command::MERGE_REQUESTS && (b->limit < 1 || b->limit > 1000))
        throw InvalidInputException("Merge request limit must be 1..1000");
    if (IsMerge(info.command) || info.command == Command::MERGE_ANCESTRY || info.command == Command::ABORT_MERGE) {
        auto &m = b->merge;
        m.expected.Validate();
        if (m.conflict_limit < 0 || m.conflict_limit > 1000 || m.conflict_offset < 0 ||
            m.conflict_offset > INT64_MAX - 1000 || m.conflict_bytes < 0 || m.conflict_bytes > 16777216)
            throw InvalidInputException("conflict_limit must be 0..1000; conflict_offset must be nonnegative "
                                        "and below BIGINT_MAX-1000; conflict_bytes must be 0..16777216");
        if (m.max_input_rows < 1 || m.max_input_bytes < 1 || m.max_stage_write_bytes < 1 || m.timeout_ms < 1)
            throw InvalidInputException("merge input, staging write and timeout limits must be positive");
        if (m.max_ancestry_nodes < 1 || m.max_ancestry_nodes > 1000000)
            throw InvalidInputException("max_ancestry_nodes must be 1..1000000");
    }
    switch (info.command) {
    case Command::MERGE_REQUESTS:
        names = {"operation_id", "request_id", "merge_kind", "state", "database_id", "destination",
                 "base_snapshot_id", "ours_snapshot_id", "theirs_snapshot_id", "result_snapshot_id", "result_root",
                 "retained_logical_bytes", "destination_reserved", "remote_descriptor", "publication_attempted",
                 "recovery_action", "created_at", "schema_mode", "schema_algorithm", "merge_action",
                 "ours_branch_name", "theirs_branch_name", "ours_branch_id", "theirs_branch_id", "inputs_guarded",
                 "attempt_count", "last_attempt_id", "last_attempt_state", "last_attempt_phase", "last_attempt_error_code",
                 "last_attempt_started_at", "last_attempt_updated_at", "last_attempt_finished_at"};
        types = {V, V, V, V, V, V, I, I, I, I, V, I, B, V, B, V, T, V, V, V,
                 V, V, V, V, B, I, V, V, V, V, T, T, T};
        break;
    case Command::ABORT_MERGE:
        names = {"operation_id", "merge_operation_id", "state", "snapshot_id"};
        types = {V, V, V, I};
        break;
    case Command::MERGE_ANCESTRY:
        names = {"snapshot_id", "branch_id", "kind", "parents_known", "parents", "reachable_from",
                 "candidate", "retired", "readable", "selection_state", "selected_base"};
        types = {I, V, V, B, LogicalType::LIST(I), V, B, B, B, V, I};
        break;
    case Command::MERGE_RESOLUTIONS:
        names = StringsToIdentifiers(MergeResolutionNames());
        types = MergeResolutionTypes();
        break;
    case Command::MERGE_STATUS:
        names = StringsToIdentifiers(MergeStatusNames());
        types = MergeStatusTypes();
        break;
    case Command::MERGE_BRANCH_PREFLIGHT:
        names = StringsToIdentifiers(MergePreflightNames());
        types = MergePreflightTypes();
        break;
    case Command::MERGE_PREVIEW:
    case Command::MERGE_BRANCH_PREVIEW:
        names = StringsToIdentifiers(MergePreviewNames());
        types = MergePreviewTypes();
        break;
    case Command::MERGE:
    case Command::MERGE_BRANCH:
        names = {"operation_id", "branch_name", "snapshot_id", "comparison_ms", "application_ms",
                 "base_snapshot_id", "ours_snapshot_id", "theirs_snapshot_id", "selection_ms", "ancestry_nodes",
                 "materialization_ms", "publication_ms", "merge_action", "target_check_ms"};
        types = {V, V, I, LogicalType::DOUBLE, LogicalType::DOUBLE, I, I, I, LogicalType::DOUBLE, I,
                 LogicalType::DOUBLE, LogicalType::DOUBLE, V, LogicalType::DOUBLE};
        break;
    case Command::SNAPSHOTS:
        names = SnapshotNames();
        types = SnapshotTypes();
        break;
    case Command::CURRENT_SNAPSHOT:
        names = {"id"};
        types = {I};
        break;
    case Command::BRANCHES:
        names = {"database_id",        "database_name", "branch_id",          "branch_name",
                 "state",              "root",          "writer_epoch",       "head_snapshot_id",
                 "created_at",         "source_database_id", "source_snapshot_id", "uri",
                 "remote_state",       "remote_snapshot_id", "kept_branch",        "ship_wal",
                 "shipped_chain",      "shipped_epoch", "shipped_at",         "ship_pending"};
        types = {V, V, V, V, V, V, I, I, T, V, I, V, V, I, V, V, I, I, T, B};
        break;
    case Command::DATABASES:
        names = {"database_id", "database_name", "data_path", "last_snapshot_id", "created_at", "writers"};
        types = {V, V, V, I, T, V};
        break;
    case Command::OPERATIONS:
        names = {"operation_id", "request_id", "kind", "state", "database_id", "branch_id", "snapshot_id",
                 "preservation_snapshot_id"};
        types = {V, V, V, V, V, V, I, I};
        break;
    case Command::PROVIDERS:
        names = {"provider_id",        "data_path",          "profile",         "region",
                 "endpoint",           "credential_source",  "max_requests",    "retries",
                 "connect_timeout_ms", "request_timeout_ms", "max_range_bytes", "multipart_threshold"};
        types = {V, V, V, V, V, V, I, I, I, I, I, I};
        break;
    case Command::ROOTS:
        names = {"reason", "identity", "root"};
        types = {V, V, V};
        break;
    case Command::REMOTE_HEADS:
        names = {"database_name", "database_id", "branch_name", "branch_id",
                 "snapshot_id",   "snapshot_time", "export_locator", "head"};
        types = {V, V, V, V, I, T, V, V};
        break;
    case Command::WRITERS:
        names = {"branch_name", "writer_id", "host", "repository_id", "process_id", "started_at", "renewed_at",
                 "live", "this_process"};
        types = {V, V, V, V, I, T, T, B, B};
        break;
    case Command::STATS:
        names = {"metric", "value"};
        types = {V, I};
        break;
    case Command::FOLLOWERS:
        names = {"alias",  "database_name", "branch_name",  "follows",    "follow_interval_ms", "chain", "epoch",
                 "snapshot_id", "state_time", "checked_at", "polls", "changes", "error"};
        types = {V, V, V, V, I, I, I, I, T, T, I, I, V};
        break;
    case Command::RECOVER_BRANCH:
        names = {"database_id", "database_name", "branch_id", "branch_name", "shipped_chain", "shipped_epoch",
                 "uri"};
        types = {V, V, V, V, I, I, V};
        break;
    case Command::EXPIRE_SNAPSHOTS:
    case Command::EVICT_SNAPSHOTS:
        names = {"snapshot_id", "snapshot_time", "snapshot_name", "dry_run", "operation_id", "export_locator"};
        types = {I, T, V, B, V, V};
        break;
    case Command::RETRY_EXPORT:
        names = {"snapshot_id", "operation_id", "export_state", "export_locator"};
        types = {I, V, V, V};
        break;
    case Command::CLAIM_HEADS:
        names = {"branch_name", "snapshot_id", "head"};
        types = {V, I, V};
        break;
    case Command::RESOLVE_CONFLICT:
        names = {"operation_id", "branch_name", "remote_snapshot_id", "kept_snapshot_id", "kept_branch"};
        types = {V, V, I, I, V};
        break;
    case Command::CLEANUP_OLD_FILES:
        names = {"operation_id", "dry_run", "candidate_objects", "candidate_bytes", "deleted_objects", "live_pins",
                 "stale_pins"};
        types = {V, B, I, I, I, I, I};
        break;
    case Command::DELETE_ORPHANED_FILES:
        names = {"dry_run", "orphaned_files"};
        types = {B, I};
        break;
    case Command::CLEANUP_REMOTE_FILES:
        names = {"dry_run",           "roots",           "live_objects",   "unreachable_objects",
                 "unreachable_bytes", "condemned_objects", "deleted_objects", "deleted_bytes"};
        types = {B, I, I, I, I, I, I, I};
        break;
    case Command::CLEANUP_METADATA:
        names = {"dry_run", "table_name", "rows"};
        types = {B, V, I};
        break;
    case Command::COMPACT:
        names = {"operation_id",    "free_pages_before", "free_bytes_before", "pages_reclaimed",
                 "bytes_reclaimed", "manifest_roots",    "roots_unchanged",   "payload_bytes",
                 "export_retained_bytes"};
        types = {V, I, I, I, I, I, B, I, I};
        break;
    default:
        names = {"operation_id", "database_id",   "branch_id", "snapshot_id",  "preservation_snapshot_id",
                 "uri",          "snapshot_time", "state",     "export_state", "export_locator"};
        types = {V, V, V, I, I, V, T, V, V, V};
        break;
    }
    b->types = types;
    return std::move(b);
}
unique_ptr<GlobalTableFunctionState> Init(ClientContext &, TableFunctionInitInput &) {
    return make_uniq<State>();
}
// The database (and, for an attached branch, the branch) a first argument names: an
// attachment alias of a differential database, or a database name/UUID.
struct Target {
    string database, branch;
    int64_t snapshot = 0; // attached snapshot (read-only attachments)
};
Target ResolveTarget(ClientContext &context, differential::Control &c, const string &argument) {
    Target target;
    auto attached = DatabaseManager::Get(context).GetDatabase(context, Identifier(argument));
    if (attached && !attached->IsSystem() && !attached->IsTemporary()) {
        auto path = attached->GetStorageManager().GetDBPath();
        if (path.rfind("diff://", 0) == 0) {
            auto resolved = c.Resolve(path);
            if (resolved.remote && !resolved.historical)
                throw InvalidInputException("\"%s\" is an attached S3 export, not a differential database", argument);
            target.database = resolved.historical ? resolved.snapshot.database : resolved.branch.database;
            if (resolved.historical)
                target.snapshot = resolved.snapshot.id;
            else
                target.branch = resolved.branch.id;
            return target;
        }
    }
    target.database = c.FindDatabase(argument);
    if (target.database.empty())
        throw InvalidInputException("[differential:not_found] No differential database or attachment named \"%s\"",
                                    argument);
    return target;
}
// Exports are complete at this point; a head failure only leaves remote discovery stale. `branch`
// limits a shared database's update to the head of the branch an operation changed.
[[noreturn]] void ThrowHeadError(const std::exception &error, const string &database) {
    // Another writer owns or moved the head: retrying cannot help; the message says what can.
    if (differential::Is(error, differential::ErrorCode::HeadConflict) ||
        differential::Is(error, differential::ErrorCode::BranchConflict))
        throw IOException("%s", error.what());
    throw IOException("[differential:incomplete_export] Snapshot export completed, but the S3 branch heads "
                      "were not updated: %s. Run differential_retry_export('%s') to publish them.",
                      error.what(), database);
}
void PublishBranchHeads(differential::Control &c, const string &database, const string &branch = "") {
    differential::Timer timer(differential::Metric::BranchHeadPublish);
    try {
        differential::PublishHeads(c, database, false, branch);
    } catch (const std::exception &error) {
        ThrowHeadError(error, database);
    }
}
// Pin the local root of a retained snapshot for the duration of a lifecycle operation. An
// evicted snapshot whose local copy was already collected is rehydrated from its verified
// S3 export first, under the snapshot's own immutable root identity.
// Bulk copies read the repository disk cache but never fill a cache: a whole database would
// only displace data that queries use.
differential::RemoteCaching BulkCaching(ClientContext &context) {
    differential::RemoteCaching caching;
    caching.disk_limit = RemoteDiskCacheLimit(context);
    caching.fill = false;
    return caching;
}
std::shared_ptr<differential::Pin> RetainSnapshotRoot(ClientContext &context, differential::Control &c,
                                                      const string &database, int64_t snapshot) {
    // Retirement and request replay are decided by the operation itself (a pending restore
    // intent retains its target root even after the snapshot is retired).
    auto s = c.GetSnapshot(database, snapshot, false);
    if (!s.evicted || s.retired)
        return nullptr;
    try {
        return c.NewPin(s.root);
    } catch (const std::exception &) {
        // Collected locally; fall through to rehydration.
    }
    differential::RemoteSnapshot remote(c, s.export_provider, s.export_key, BulkCaching(context));
    if (remote.descriptor.root != s.root || remote.descriptor.database != s.database ||
        remote.descriptor.snapshot != s.id)
        throw IOException("[differential:corrupt_data] Export descriptor does not describe the evicted snapshot");
    auto file_pin = c.NewPin();
    auto name = "rehydrate/" + std::to_string(differential::Control::Now()) + "-" + file_pin->id;
    auto file = c.store->Open(name, true, true, false, file_pin->id, c.LocalDatabasePool(s.database));
    auto root = remote.Materialize(*c.store, file.id);
    auto pin = c.NewPin(root.id);
    c.store->Remove(name);
    return pin;
}
// Snapshot for branch/clone creation: explicit, else the source branch's latest snapshot.
int64_t SourceSnapshot(differential::Control &c, const Binding &b, const Target &target) {
    if (b.snapshot)
        return *b.snapshot;
    auto branch = !b.branch.empty()        ? c.GetBranch(target.database, b.branch).id
                  : !target.branch.empty() ? target.branch
                                           : c.GetBranch(target.database, "main").id;
    auto head = c.CurrentSnapshot(target.database, branch);
    if (!head)
        throw InvalidInputException("[differential:not_found] The source branch has no snapshot yet; create one with "
                                    "differential_create_snapshot or pass snapshot => <id>");
    return head;
}
string Quote(const string &s) {
    string result = "'";
    for (char ch : s) {
        result += ch;
        if (ch == '\'')
            result += ch;
    }
    return result + "'";
}
differential::Operation ImportNative(ClientContext &context, differential::Control &c, const string &request,
                                     const string &source, const string &name, const string &provider) {
    auto supplied = std::filesystem::path(source);
    if (!supplied.is_absolute())
        throw InvalidInputException("Import requires an absolute offline native file path");
    auto op = c.BeginImport(name, "native:" + supplied.lexically_normal().string(), request, provider, "import_native");
    if (op.state == "complete")
        return op;
    auto path = std::filesystem::canonical(supplied).string();
    if (!std::filesystem::is_regular_file(path))
        throw InvalidInputException("Import requires a regular native file");
    auto local = FileSystem::CreateLocal();
    auto no_wal = [&] {
        for (auto suffix : {".wal", ".wal.checkpoint", ".wal.recovery"})
            if (local->FileExists(path + suffix))
                throw IOException("Import requires a checkpoint-consistent source with no WAL sidecars; "
                                  "recover, CHECKPOINT and close the source first");
    };
    no_wal();
    Connection source_connection(*context.db);
    auto alias = "\"_differential_import_" + op.id + "\"";
    auto query = [&](const string &sql) {
        auto result = source_connection.Query(sql);
        if (result->HasError())
            result->ThrowError();
    };
    bool attached = false;
    unique_ptr<FileHandle> handle;
    try {
        // Native READ_ONLY attach validates the header/catalog, tracks the path in this host,
        // and holds DuckDB's own read lock against external native writers throughout the copy.
        query("ATTACH " + Quote(path) + " AS " + alias + " (READ_ONLY)");
        attached = true;
        no_wal();
        // Not FileFlags::FILE_FLAGS_READ: see differential_extension.cpp (duplicate definition on GNU ld).
        handle = local->OpenFile(path, FileOpenFlags(FileOpenFlags::FILE_FLAGS_READ));
        auto size = local->GetFileSize(*handle);
        auto modified = local->GetLastModifiedTime(*handle);
        if (size < 0 || uint64_t(size) > (uint64_t(1) << 40))
            throw IOException("Import exceeds logical file limit");
        auto pin = c.NewPin();
        auto file = c.store->Open("imports/" + op.id, true, false, true, pin->id, c.LocalProvider(provider));
        auto pass = [&](bool copy) {
            std::unique_ptr<EVP_MD_CTX, decltype(&EVP_MD_CTX_free)> digest(EVP_MD_CTX_new(), EVP_MD_CTX_free);
            if (!digest || EVP_DigestInit_ex(digest.get(), EVP_sha256(), nullptr) != 1)
                throw IOException("Import checksum initialization failed");
            std::array<unsigned char, 65536> bytes;
            for (uint64_t offset = 0; offset < uint64_t(size);) {
                if (context.IsInterrupted())
                    throw InterruptException();
                auto n = std::min<uint64_t>(bytes.size(), uint64_t(size) - offset);
                local->Read(*handle, bytes.data(), int64_t(n), offset);
                if (EVP_DigestUpdate(digest.get(), bytes.data(), size_t(n)) != 1)
                    throw IOException("Import checksum failed");
                if (copy)
                    c.store->Write(file.id, bytes.data(), n, offset);
                offset += n;
            }
            std::array<unsigned char, 32> hash;
            unsigned n;
            if (EVP_DigestFinal_ex(digest.get(), hash.data(), &n) != 1 || n != 32)
                throw IOException("Import checksum failed");
            return hash;
        };
        auto copied = pass(true);
        auto verified = pass(false);
        no_wal();
        if (copied != verified || size != local->GetFileSize(*handle) ||
            modified != local->GetLastModifiedTime(*handle))
            throw IOException("Source changed during import");
        auto root = c.store->Seal(file.id);
        query("DETACH " + alias);
        attached = false;
        handle.reset();
        return c.CompleteImport(op, root, "", 0, provider);
    } catch (...) {
        if (attached)
            source_connection.Query("DETACH " + alias);
        handle.reset();
        throw;
    }
}
// The sealed image of a branch nobody has attached, after native recovery and a checkpoint on a
// private connection that uses the caller's lease ("" when the branch has no live file yet).
string CaptureBranch(ClientContext &context, differential::Control &c,
                     const std::shared_ptr<differential::BranchLock> &lease, const string &name) {
    Connection recovery(*context.db);
    auto alias = "\"_differential_recovery_" + name + "\"";
    auto query = [&](const string &sql) {
        auto result = recovery.Query(sql);
        if (result->HasError())
            result->ThrowError();
    };
    bool attached = false;
    recovery.context->registered_state->Insert("differential_recovery",
                                               make_shared_ptr<DifferentialRecoveryState>(lease));
    string root;
    try {
        query("ATTACH '" + lease->branch.URI() + "' AS " + alias + " (TYPE differential)");
        attached = true;
        query("CHECKPOINT " + alias);
        query("DETACH " + alias);
        attached = false;
        // No live writer can enter this namespace while the management lease is held.
        // CHECKPOINT and DETACH completed on the recovery connection; seal the stable file.
        auto file = c.store->Open(lease->branch.FileName(), false, false, false, "",
                                  c.LocalDatabasePool(lease->branch.database));
        root = c.store->Seal(file.id).id;
        recovery.context->registered_state->Remove("differential_recovery");
    } catch (...) {
        if (attached)
            recovery.Query("DETACH " + alias);
        recovery.context->registered_state->Remove("differential_recovery");
        throw;
    }
    if (root.empty())
        throw IOException("Recovery did not capture a completed checkpoint");
    return root;
}
differential::Operation Compound(ClientContext &context, differential::Control &c, const string &database,
                                 const string &branch, int64_t target, const string &request, bool restore) {
    auto retained = restore ? RetainSnapshotRoot(context, c, database, target) : nullptr;
    auto prepared = c.PrepareCompound(database, branch, restore ? target : 0, request, restore);
    auto op = prepared.first;
    if (op.state == "complete")
        return op;
    auto lease = prepared.second;
    auto root = CaptureBranch(context, c, lease, op.id);
    return c.FinishCompound(op, lease->branch, {root, c.store->RootSize(root)}, restore);
}
// Reconcile a shared database's branch with its S3 head: preserve its current contents as a
// snapshot, optionally publish them as a new branch, then make the branch its S3 head again.
differential::Operation ResolveConflict(ClientContext &context, differential::Control &c, const string &database,
                                        const string &branch_name, const string &keep, const string &name,
                                        const string &request, string &kept_branch) {
    auto branch = c.GetBranch(database, branch_name);
    auto op = c.BeginOperation("resolve_conflict", {database, branch.id, keep, name}, request);
    if (op.state == "complete") {
        kept_branch = op.branch.empty() ? "" : c.BranchName(op.branch);
        return op;
    }
    std::shared_ptr<differential::BranchLock> lease;
    try {
        lease = c.Acquire(branch);
    } catch (const std::exception &error) {
        if (!differential::Is(error, differential::ErrorCode::WriterBusy))
            throw;
        throw IOException("[differential:active_attachment_or_transaction] Branch \"%s\" is attached or being "
                          "written; DETACH it first",
                          branch_name);
    }
    // Preserve the current contents, unless the latest snapshot already holds them.
    int64_t kept = lease->branch.head;
    if (c.store->Exists(lease->branch.FileName())) {
        auto root = CaptureBranch(context, c, lease, op.id);
        if (!kept || c.GetSnapshot(database, kept, false).root != root) {
            auto preserve = c.BeginSnapshot(lease->branch, {"", "", "preserved before conflict resolution", ""}, "");
            kept = c.PublishSnapshot(preserve, lease->branch, {root, c.store->RootSize(root)},
                                     differential::Control::Now())
                       .snapshot;
        }
    }
    differential::Head key;
    key.database = database;
    key.branch_name = branch_name;
    auto base = c.HeadWriteFor(c.Profile(database), key.Key());
    // Local work is whatever this branch has beyond the S3 head it last wrote or adopted.
    if (keep == "branch" && kept && (!base || kept != base->snapshot)) {
        differential::ExportSnapshot(c, database, kept, [&] { return context.IsInterrupted(); });
        kept_branch = name.empty() ? differential::ConflictBranchName(branch_name, kept) : name;
        std::optional<differential::Branch> earlier; // created by this request before an interruption
        try {
            earlier = c.GetBranch(database, kept_branch);
        } catch (const std::exception &) {
        }
        if (earlier && earlier->head == kept)
            op.branch = earlier->id;
        else {
            if (differential::RemoteBranchExists(c, database, kept_branch))
                throw InvalidInputException("[differential:name_conflict] Branch \"%s\" already exists in S3",
                                            kept_branch);
            op.branch = c.CreateBranch(database, kept, kept_branch, "", false).branch;
        }
        PublishBranchHeads(c, database, op.branch);
        differential::Failpoint("DIFFERENTIAL_SHARED_CRASH", "conflict_forked");
    }
    auto followed = differential::FollowRemoteHead(c, database, branch_name);
    op.database = database;
    op.snapshot = kept;
    op.preservation = followed.head;
    return c.CompleteOperation(op);
}
void Run(ClientContext &context, const Binding &b, State &state) {
    if (!ReadOnly(b.command) && !context.transaction.IsAutoCommit())
        throw TransactionException(
            "[differential:active_attachment_or_transaction] Differential management requires autocommit");
    auto active_control = b.repository->Get();
    auto &c = *active_control;
    if (b.command == Command::MERGE_REQUESTS || b.command == Command::ABORT_MERGE) {
        auto database = ResolveTarget(context, c, b.args[0].GetValue<string>()).database;
        if (b.command == Command::ABORT_MERGE) {
            auto id = b.args[1].GetValue<string>();
            auto action = AbortLogicalMerge(context, c, database, id, b.request, b.merge);
            state.rows.push_back({Value(action.id), Value(id), Value(action.snapshot ? "complete" : "aborted"),
                                  action.snapshot ? Value::BIGINT(action.snapshot) : Value(I)});
        } else {
            for (auto &r : c.MergeRequests(database, b.request, b.after, b.limit, [&] {
                if (context.IsInterrupted()) throw InterruptException();
            })) {
                state.rows.push_back({Value(r.operation.id), Optional(r.operation.request), Value(r.operation.kind),
                    Value(r.state), Value(r.database), Value(r.destination), Value::BIGINT(r.base),
                    Value::BIGINT(r.ours), Value::BIGINT(r.theirs), r.result ? Value::BIGINT(r.result) : Value(I),
                    Optional(r.root), Value::BIGINT(r.retained_bytes), Value(r.reserved), Optional(r.descriptor),
                    Value(r.head_attempted), Value(r.recovery), Time(r.created_at), Value(r.schema_mode), Value(r.schema_algorithm), Value(r.action),
                    Optional(r.ours_branch_name), Optional(r.theirs_branch_name), Optional(r.ours_branch_id), Optional(r.theirs_branch_id),
                    r.operation.kind == "merge_branch" ? Value(r.inputs_guarded) : Value(B),
                    Value::BIGINT(r.attempt ? r.attempt->count : 0), r.attempt ? Value(r.attempt->id) : Value(V),
                    r.attempt ? Value(r.attempt->state) : Value(V), r.attempt ? Value(r.attempt->phase) : Value(V),
                    r.attempt ? Optional(r.attempt->error_code) : Value(V), Time(r.attempt ? r.attempt->started_at : 0),
                    Time(r.attempt ? r.attempt->updated_at : 0), Time(r.attempt ? r.attempt->finished_at : 0)});
            }
        }
        return;
    }
    if (b.command == Command::MERGE_STATUS) {
        state.rows = MergeStatus(c);
        return;
    }
    if (b.command == Command::MERGE_ANCESTRY || b.command == Command::MERGE_RESOLUTIONS) {
        auto database = ResolveTarget(context, c, b.args[0].GetValue<string>()).database;
        if (b.command == Command::MERGE_RESOLUTIONS) {
            state.rows = MergeResolutionRows(c.ReadMergeResolutions(database, b.args[1].GetValue<string>()));
            return;
        }
        auto start = std::chrono::steady_clock::now();
        auto ancestry = c.InspectMergeAncestry(database, b.args[1].GetValue<int64_t>(), b.args[2].GetValue<int64_t>(),
            b.merge.max_ancestry_nodes, [&] {
                if (context.IsInterrupted()) throw InterruptException();
                if (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now()-start).count() >= b.merge.timeout_ms)
                    throw IOException("[differential:merge_timeout] Ancestry inspection deadline exceeded");
            });
        for (auto &node : ancestry.nodes) {
            vector<Value> parents;
            for (auto id : {node.parents.first, node.parents.second}) if (id) parents.push_back(Value::BIGINT(id));
            state.rows.push_back({Value::BIGINT(node.snapshot), Optional(node.branch),
                Value(node.known ? node.parents.kind : "unknown"), Value(node.known), Value::LIST(I, std::move(parents)),
                Value(node.reach == 3 ? "both" : node.reach == 1 ? "ours" : "theirs"), Value(node.candidate),
                Value(node.retired), Value(node.readable), Value(ancestry.state),
                ancestry.selected ? Value::BIGINT(ancestry.selected) : Value(I)});
        }
        return;
    }
    if (IsMerge(b.command)) {
        auto database = ResolveTarget(context, c, b.args[0].GetValue<string>()).database;
        if (b.command == Command::MERGE_BRANCH_PREFLIGHT) {
            if (b.args[3].GetValue<string>().empty()) throw InvalidInputException("Merge destination must not be empty");
            state.rows.push_back(MergePreflight(context, c, database, b.args[1].GetValue<string>(),
                                               b.args[2].GetValue<string>(), b.args[3].GetValue<string>(), b.merge));
            return;
        }
        const bool branch = b.command == Command::MERGE_BRANCH || b.command == Command::MERGE_BRANCH_PREVIEW;
        const bool preview = b.command == Command::MERGE_PREVIEW || b.command == Command::MERGE_BRANCH_PREVIEW;
        auto destination = preview ? string() : b.args[branch ? 3 : 4].GetValue<string>();
        if (!preview && destination.empty())
            throw InvalidInputException("Merge destination must not be empty");
        LogicalMergeResult result;
        if (branch) {
            auto started = std::chrono::steady_clock::now();
            auto elapsed = [&] {
                return std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::steady_clock::now() - started).count();
            };
            auto check = [&] {
                if (context.IsInterrupted())
                    throw InterruptException();
                if (elapsed() >= b.merge.timeout_ms)
                    throw IOException("[differential:merge_timeout] Merge ancestry selection deadline exceeded");
            };
            auto inputs = c.ResolveMergeBranches(database, b.args[1].GetValue<string>(),
                                                b.args[2].GetValue<string>(), destination, b.request,
                                                b.merge.max_ancestry_nodes, check, MergeResolutionSpecification(b.merge), b.merge.schema_mode, b.merge.expected);
            auto options = b.merge;
            if (inputs.operation.state != "complete") {
                check();
                options.timeout_ms -= elapsed();
            }
            if (!preview && inputs.operation.state != "complete")
                differential::Failpoint("DIFFERENTIAL_MERGE_CRASH", "resolved");
            result = LogicalMerge(context, c, database, inputs.base, inputs.ours, inputs.theirs,
                                  destination, b.request, options, &inputs);
        } else
            result = LogicalMerge(context, c, database, b.args[1].GetValue<int64_t>(),
                                  b.args[2].GetValue<int64_t>(), b.args[3].GetValue<int64_t>(), destination,
                                  b.request, b.merge);
        if (preview)
            state.rows = std::move(result.preview);
        else
            state.rows.push_back({Value(result.operation.id), Value(destination),
                                  Value::BIGINT(result.operation.snapshot), Value(result.comparison_ms),
                                  Value(result.application_ms), Value::BIGINT(result.base),
                                  Value::BIGINT(result.ours), Value::BIGINT(result.theirs),
                                  Value(result.selection_ms), Value::BIGINT(result.ancestry_nodes),
                                  Value(result.materialization_ms), Value(result.publication_ms), Value(result.action), Value(result.target_check_ms)});
        return;
    }
    // Secrets are read at execution: current credentials for every S3 provider, and defaults
    // from the differential secret for options this call did not give.
    RefreshS3Credentials(context, c);
    auto data = b.data;
    ApplyDifferentialSecret(context, data);
    auto text = [&](idx_t i) { return b.args[i].GetValue<string>(); };
    switch (b.command) {
    case Command::SNAPSHOTS: {
        // Shared databases: other writers' snapshots come from S3 commit records (read here).
        auto database = ResolveTarget(context, c, text(0)).database;
        differential::SyncHistory(c, database, false, b.full_refresh);
        for (auto &s : c.Snapshots(database))
            state.rows.push_back(SnapshotRow(s));
        return;
    }
    case Command::CURRENT_SNAPSHOT: {
        // DuckLake semantics: the snapshot an attachment reads, else the branch's latest one.
        auto target = ResolveTarget(context, c, text(0));
        auto id = target.snapshot ? target.snapshot : c.CurrentSnapshot(target.database, target.branch);
        state.rows.push_back({id ? Value::BIGINT(id) : Value(LogicalType::BIGINT)});
        return;
    }
    case Command::REMOTE_HEADS: {
        // Branch heads exported to an S3 location; reads S3 only here, at execution.
        auto location = text(0);
        if (!differential::S3Location::Is(location))
            throw InvalidInputException("Expected an s3://bucket/prefix/ location");
        if (location.back() != '/')
            location += '/';
        auto provider = EnsureS3(context, c, data, location);
        for (auto &h : differential::ListHeads(c, provider))
            state.rows.push_back({Value(h.database_name), Value(h.database), Value(h.branch_name), Value(h.branch),
                                  Value::BIGINT(h.snapshot), Time(h.published_at), Value(h.descriptor),
                                  Value(location + h.AttachPath())});
        return;
    }
    case Command::WRITERS: {
        // Writer markers of a shared database in S3 (reads S3 at execution only).
        auto now = differential::Control::Now();
        for (auto &entry : differential::ListWriters(c, ResolveTarget(context, c, text(0)).database)) {
            auto &w = entry.first;
            state.rows.push_back({Value(w.branch_name), Value(w.writer), Value(w.host), Value(w.repository),
                                  Value::BIGINT(w.process), Time(w.started_at), Time(w.renewed_at),
                                  Value::BOOLEAN(w.Live(now)), Value::BOOLEAN(entry.second)});
        }
        return;
    }
    case Command::FOLLOWERS:
        for (auto &f : Followers())
            state.rows.push_back({Value(f.alias), Value(f.database), Value(f.branch), Value(f.mode),
                                  Value::BIGINT(f.interval_ms), f.mode == "commits" ? Value::BIGINT(f.chain) : Value(I),
                                  f.mode == "commits" ? Value::BIGINT(f.epoch) : Value(I),
                                  f.mode == "snapshots" ? Value::BIGINT(f.snapshot) : Value(I), Time(f.published_at),
                                  Time(f.checked_at), Value::BIGINT(f.polls), Value::BIGINT(f.changes),
                                  Optional(f.error)});
        return;
    case Command::STATS:
        for (auto &m : differential::Timings())
            state.rows.push_back({Value(m.first), Value::UBIGINT(m.second)});
        for (auto &m : c.Statistics())
            state.rows.push_back({Value(m.first), Value::UBIGINT(m.second)});
        for (auto &m : differential::S3Provider::Metrics())
            state.rows.push_back({Value(m.first), Value::UBIGINT(m.second)});
        for (auto &m : differential::ExportMetrics())
            state.rows.push_back({Value(m.first), Value::UBIGINT(m.second)});
        for (auto &m : differential::ShippingMetrics())
            state.rows.push_back({Value(m.first), Value::UBIGINT(m.second)});
        state.rows.push_back({Value("local_payload_objects"), Value::UBIGINT(c.store->ObjectCount())});
        return;
    case Command::COMPACT: {
        auto r = c.Compact(b.limit, b.request);
        state.rows.push_back({Value(r.operation), Value::BIGINT(r.candidate_objects), Value::BIGINT(r.candidate_bytes),
                              Value::BIGINT(r.deleted_objects), Value::BIGINT(r.deleted_objects * r.page_size),
                              Value::BIGINT(r.roots), Value::BOOLEAN(true), Value::BIGINT(r.payload_bytes),
                              Value::BIGINT(r.export_retained_bytes)});
        return;
    }
    case Command::CLEANUP_OLD_FILES: {
        // One bounded step, or (cleanup_all) steps until the collection queues are empty.
        for (int step = 0; step < 100000; ++step) {
            auto request = b.request.empty() || !step ? b.request : b.request + "/" + std::to_string(step);
            auto r = c.Collect(b.dry_run, b.limit, request);
            state.rows.push_back({Optional(r.operation), Value::BOOLEAN(b.dry_run), Value::BIGINT(r.candidate_objects),
                                  Value::BIGINT(r.candidate_bytes), Value::BIGINT(r.deleted_objects),
                                  Value::BIGINT(r.live_pins), Value::BIGINT(r.stale_pins)});
            if (b.dry_run || !b.cleanup_all || r.remaining_candidates == 0)
                break;
        }
        return;
    }
    case Command::DELETE_ORPHANED_FILES:
        state.rows.push_back({Value::BOOLEAN(b.dry_run), Value::BIGINT(c.DeleteOrphanedFiles(b.dry_run, b.limit))});
        return;
    case Command::CLEANUP_REMOTE_FILES: {
        // Every database in the location: packs are shared between them. Reads and writes S3 here only.
        auto location = text(0);
        if (!differential::S3Location::Is(location))
            throw InvalidInputException("Expected an s3://bucket/prefix/ location");
        if (location.back() != '/')
            location += '/';
        auto provider = EnsureS3(context, c, data, location);
        constexpr int64_t WEEK = 7ll * 24 * 3600 * 1000000;
        auto older_than = b.older_than ? *b.older_than : differential::Control::Now() - WEEK;
        auto r = differential::CollectRemote(c, provider, older_than, b.dry_run);
        state.rows.push_back({Value::BOOLEAN(b.dry_run), Value::BIGINT(r.roots), Value::BIGINT(r.live_objects),
                              Value::BIGINT(r.unreachable_objects), Value::BIGINT(r.unreachable_bytes),
                              Value::BIGINT(r.condemned_objects), Value::BIGINT(r.deleted_objects),
                              Value::BIGINT(r.deleted_bytes)});
        return;
    }
    case Command::CLEANUP_METADATA:
        if (!b.older_than)
            throw InvalidInputException("differential_cleanup_metadata needs older_than => <timestamp>");
        for (auto &row : c.PruneMetadata(*b.older_than, b.dry_run))
            state.rows.push_back({Value::BOOLEAN(b.dry_run), Value(row.first), Value::BIGINT(row.second)});
        return;
    case Command::EXPIRE_SNAPSHOTS:
    case Command::EVICT_SNAPSHOTS: {
        bool evict = b.command == Command::EVICT_SNAPSHOTS;
        auto database = ResolveTarget(context, c, text(0)).database;
        bool shared = c.Writers(database) == "shared";
        if (shared)
            differential::SyncHistory(c, database);
        auto selected = c.SelectSnapshots(database, b.versions, b.older_than, evict);
        // Published snapshots expire in S3 first: for every writer of a shared database, and for
        // remote collection, which reclaims what only expired snapshots reference.
        if (!evict && !b.dry_run) {
            vector<int64_t> published;
            for (auto &s : selected)
                if (s.export_state == "complete")
                    published.push_back(s.id);
            if (shared)
                differential::ExpireShared(c, database, published);
            else
                differential::ExpireExported(c, database, published);
        }
        for (auto &s : selected) {
            string operation;
            if (!b.dry_run && !s.remote) { // another writer's snapshot has no local copy to retire or evict
                auto request = b.request.empty() ? "" : b.request + "/" + std::to_string(s.id);
                operation = (evict ? c.Evict(database, s.id, request) : c.Retire(database, s.id, request)).id;
            }
            state.rows.push_back({Value::BIGINT(s.id), Time(s.published_at), Optional(s.name),
                                  Value::BOOLEAN(b.dry_run), Optional(operation), Optional(s.export_key)});
        }
        // An expired snapshot stops being a branch head; the head moves to the latest retained export.
        if (!evict && !b.dry_run)
            PublishBranchHeads(c, database);
        return;
    }
    case Command::RETRY_EXPORT: {
        auto database = ResolveTarget(context, c, text(0)).database;
        bool all = b.args.size() == 1;
        // One snapshot, or every pending one, lowest first (a shared database's export can
        // renumber the ones after it, so the next is looked up each time).
        for (bool first = true;; first = false) {
            int64_t id;
            if (!all) {
                if (!first)
                    break;
                id = b.args[1].GetValue<int64_t>();
            } else {
                auto pending = c.PendingExports(database);
                if (pending.empty())
                    break;
                id = pending.front();
            }
            auto request = b.request.empty() || !all ? b.request : b.request + "/" + std::to_string(id);
            auto op = c.RetryExport(database, id, request);
            string key;
            try {
                key = differential::ExportSnapshot(c, database, id, [&] { return context.IsInterrupted(); });
            } catch (const std::exception &error) {
                throw IOException("[differential:incomplete_export] Snapshot %d remains pending: %s", id, error.what());
            }
            state.rows.push_back({Value::BIGINT(id), Value(op.id), Value("complete"), Optional(key)});
        }
        // Also repairs branch heads a previous head update could not publish.
        PublishBranchHeads(c, database);
        return;
    }
    case Command::CLAIM_HEADS: {
        // Take the database's S3 branch heads over, even if another writer wrote them last (for
        // example after restoring this repository from a backup on a new host).
        auto database = ResolveTarget(context, c, text(0)).database;
        string provider;
        c.ExportedHeads(database, provider);
        if (provider.empty() || provider == "local")
            throw InvalidInputException("[differential:not_found] Database \"%s\" has no S3 data path", text(0));
        if (c.Writers(database) == "shared")
            throw InvalidInputException("Database \"%s\" is shared by several writers: its branch heads have no "
                                        "single owner to claim them", text(0));
        differential::ClaimDatabase(c, database); // abandoned exports show as export_state 'abandoned'
        auto config = c.Provider(provider).config;
        for (auto &h : c.ExportedHeads(database, provider)) {
            if (c.MergeTargetPending(h.branch)) continue; // Not claimed while its merge owns publication.
            differential::Head head;
            head.database = h.database;
            head.database_name = h.database_name;
            head.branch_name = h.branch_name;
            state.rows.push_back({Value(h.branch_name), Value::BIGINT(h.snapshot),
                                  Value("s3://" + config.bucket + "/" + config.prefix + head.AttachPath())});
        }
        return;
    }
    case Command::RESOLVE_CONFLICT: {
        auto target = ResolveTarget(context, c, text(0));
        if (c.Writers(target.database) != "shared")
            throw InvalidInputException("Database \"%s\" is not shared (WRITERS 'shared'): it has no conflicts to "
                                        "resolve",
                                        text(0));
        if (b.keep.empty())
            throw InvalidInputException("differential_resolve_conflict needs keep => 'branch' or 'remote'");
        auto branch = !b.branch.empty()        ? b.branch
                      : !target.branch.empty() ? c.GetBranch(target.database, target.branch, true).name
                                               : string("main");
        string kept_branch;
        auto op = ResolveConflict(context, c, target.database, branch, b.keep, b.snapshot_info.name, b.request,
                                  kept_branch);
        state.rows.push_back({Value(op.id), Value(branch), Value::BIGINT(op.preservation),
                              op.snapshot ? Value::BIGINT(op.snapshot) : Value(LogicalType::BIGINT),
                              Optional(kept_branch)});
        return;
    }
    default:
        break;
    }
    differential::Operation op;
    // Shared databases: the attachment's conflict policy (ATTACH ON_CONFLICT).
    string on_conflict = "fail", forked_branch;
    switch (b.command) {
    case Command::CREATE_SNAPSHOT: {
        auto attachment = DatabaseManager::Get(context).GetDatabase(context, Identifier(text(0)));
        if (!attachment || attachment->IsReadOnly() ||
            attachment->GetStorageManager().GetDBPath().rfind("diff://", 0) != 0)
            throw InvalidInputException("Expected a writable differential attachment (ATTACH 'differential:...')");
        auto resolved = c.Resolve(attachment->GetStorageManager().GetDBPath());
        if (!resolved.managed || resolved.historical || resolved.remote || !resolved.suffix.empty())
            throw InvalidInputException("Public snapshots require an attached database branch");
        auto branch = resolved.branch;
        bool shared = c.Writers(branch.database) == "shared";
        if (shared)
            on_conflict = differential::GetSharedWriting(c, branch.id).on_conflict;
        if (shared && on_conflict == "fail") {
            // A diverged branch publishes nothing more until it is reconciled.
            auto conflict = c.Conflict(branch.id);
            if (conflict && conflict->kept_branch.empty())
                throw IOException("[differential:branch_conflict] Branch %s of database %s diverged from S3: its "
                                  "snapshot %d could not become the branch head (S3 was at snapshot %d). DETACH, "
                                  "then reconcile it with differential_resolve_conflict('%s', keep => 'branch' or "
                                  "'remote', branch => '%s').",
                                  branch.name, branch.database_name, conflict->snapshot, conflict->remote_snapshot,
                                  branch.database_name, branch.name);
        }
        op = c.BeginSnapshot(branch, b.snapshot_info, b.request);
        if (op.state == "complete")
            break;
        try {
            differential::Timer checkpoint(differential::Metric::SnapshotCheckpoint);
            // This checkpoint is published as the explicit snapshot below, not as a checkpoint snapshot.
            differential::ExplicitSnapshot explicit_snapshot(c, branch.id);
            attachment->GetTransactionManager().Checkpoint(context, false);
        } catch (TransactionException &error) {
            throw TransactionException("[differential:active_attachment_or_transaction] Snapshot needs a completed "
                                       "checkpoint (%s). Retry the same request_id once concurrent write "
                                       "transactions finish.",
                                       ErrorData(error).RawMessage());
        }
        // The checkpoint folded every committed transaction into the main file. Any image of
        // the main file is crash-consistent with its latest header, so sealing it now (under a
        // capture pin) publishes exactly that checkpoint plus unreferenced free blocks.
        auto pin = c.NewPin();
        // NORMAL commit: the FULL publication commit below syncs this capture with it.
        auto root = c.store->Seal(c.store->FileId(branch.FileName()), pin->id, false);
        op = c.PublishSnapshot(op, branch, root, differential::Control::Now());
        break;
    }
    case Command::CREATE_BRANCH:
    case Command::CLONE_DATABASE: {
        auto target = ResolveTarget(context, c, text(0));
        auto snapshot = SourceSnapshot(c, b, target);
        // Branch names of a shared database are taken in S3: its head is created only if absent.
        if (b.command == Command::CREATE_BRANCH && c.Writers(target.database) == "shared" &&
            differential::RemoteBranchExists(c, target.database, text(1)))
            throw InvalidInputException("[differential:name_conflict] Branch \"%s\" already exists in S3; ATTACH "
                                        "it with BRANCH '%s'",
                                        text(1), text(1));
        differential::EnsureLocalSnapshot(c, target.database, snapshot); // another writer's snapshot
        auto retained = RetainSnapshotRoot(context, c, target.database, snapshot);
        op = c.CreateBranch(target.database, snapshot, text(1), b.request, b.command == Command::CLONE_DATABASE);
        break;
    }
    case Command::RESTORE: {
        auto target = ResolveTarget(context, c, text(0));
        auto branch = !b.branch.empty()        ? b.branch
                      : !target.branch.empty() ? c.GetBranch(target.database, target.branch, true).name
                                               : string("main");
        differential::EnsureLocalSnapshot(c, target.database, b.args[1].GetValue<int64_t>());
        op = Compound(context, c, target.database, branch, b.args[1].GetValue<int64_t>(), b.request, true);
        break;
    }
    case Command::DROP_BRANCH: {
        auto database = ResolveTarget(context, c, text(0)).database;
        if (c.Writers(database) != "shared") {
            op = Compound(context, c, database, text(1), 0, b.request, false);
            break;
        }
        // Shared databases: the branch must still be at this repository's base in S3; after the local
        // drop its head becomes a tombstone, for every writer.
        differential::Branch branch;
        try {
            branch = c.GetBranch(database, text(1));
        } catch (const std::exception &) {
            // Dropped here already (a repeated request, or one interrupted before its tombstone): the
            // latest dropped branch of that name finishes in S3 if its head is still this repository's.
            op = Compound(context, c, database, text(1), 0, b.request, false);
            for (auto &candidate : c.Branches(database))
                if (candidate.name == text(1) && candidate.state == "deleted")
                    branch = candidate;
            if (!branch.id.empty())
                differential::TombstoneBranch(c, database, branch,
                                              differential::PrepareSharedDrop(c, database, branch.name));
            break;
        }
        auto etag = differential::PrepareSharedDrop(c, database, branch.name);
        op = Compound(context, c, database, branch.name, 0, b.request, false);
        differential::Failpoint("DIFFERENTIAL_SHARED_CRASH", "branch_dropped");
        differential::TombstoneBranch(c, database, branch, etag);
        break;
    }
    case Command::RECOVER_BRANCH: {
        // The newest shipped state of a branch, from s3://…/heads/<database>[/<uuid>][/<branch>].
        std::pair<string, string> head_location;
        if (!SplitHeadLocation(text(0), head_location))
            throw InvalidInputException("Expected s3://<bucket>/<prefix>/heads/<database>[/<database uuid>][/<branch>]");
        auto provider = EnsureS3(context, c, data, head_location.first);
        auto location = differential::ResolveBranchLocation(c, provider, head_location.second);
        auto existing = c.FindDatabase(text(1));
        if (!existing.empty() && !b.request.empty()) {
            // A repeated request: the database it created.
            auto branch = c.GetBranch(existing, location.branch_name);
            state.rows.push_back({Value(existing), Value(text(1)), Value(branch.id), Value(branch.name), Value(I),
                                  Value(I), Value(branch.URI())});
            return;
        }
        auto recovered = differential::RecoverBranch(c, provider, location.database, location.branch_name, text(1),
                                                     b.same_identity);
        auto branch = c.GetBranch(recovered.database, recovered.branch, true);
        state.rows.push_back({Value(recovered.database), Value(text(1)), Value(branch.id), Value(branch.name),
                              Value::BIGINT(recovered.chain), Value::BIGINT(recovered.epoch), Value(branch.URI())});
        return;
    }
    case Command::IMPORT_DATABASE:
        op = ImportNative(context, c, b.request, text(0), text(1), EnsureDataPath(context, c, data));
        break;
    case Command::RECOVER_EXPORT: {
        auto location = SplitExportLocation(text(0));
        auto source_provider = EnsureS3(context, c, data, location.first);
        // DATA_PATH chooses the recovered database's own storage (default: the local pool). The
        // source's S3 options apply to it as defaults only.
        DataPathOptions destination_options;
        if (!data.data_path.empty())
            destination_options.Default("data_path", Value(data.data_path));
        destination_options.s3 = data.s3;
        auto destination = EnsureDataPath(context, c, destination_options);
        op = c.BeginImport(text(1), source_provider + "/" + location.second, b.request, destination);
        if (op.state == "complete")
            break;
        differential::RemoteSnapshot source(c, source_provider, location.second, BulkCaching(context));
        auto pin = c.NewPin();
        auto file = c.store->Open("imports/" + op.id, true, false, false, pin->id, c.LocalProvider(destination));
        auto root = source.Materialize(*c.store, file.id);
        op = c.CompleteImport(op, root, source.descriptor.database, source.descriptor.snapshot, destination);
        break;
    }
    default:
        throw InternalException("Unknown differential management command");
    }
    string export_state = "not_required", export_key;
    if (!b.defer_export && b.command != Command::CREATE_BRANCH) {
        try {
            for (bool preservation : {true, false}) {
                auto id = preservation ? op.preservation : op.snapshot;
                if (!id)
                    continue;
                auto key = differential::ExportSnapshot(c, op.database, id, [&] { return context.IsInterrupted(); });
                // A shared database's export can renumber the operation's snapshots.
                op = c.GetOperation(op.id);
                if (!key.empty()) {
                    export_state = "complete";
                    export_key = key;
                }
            }
        } catch (const std::exception &error) {
            throw IOException("[differential:incomplete_export] Local operation %s completed; S3 export "
                              "remains pending: %s",
                              op.id, error.what());
        }
        if (!export_key.empty()) {
            differential::Timer timer(differential::Metric::BranchHeadPublish);
            try {
                differential::PublishHeads(c, op.database, false, op.branch);
            } catch (const std::exception &error) {
                if (on_conflict != "branch" || !differential::Is(error, differential::ErrorCode::BranchConflict))
                    ThrowHeadError(error, op.database);
                // ON_CONFLICT 'branch': the snapshot becomes a branch of its own; this branch follows S3
                // again at its next ATTACH if it has no changes after the snapshot.
                forked_branch = c.GetBranch(op.database, differential::ForkConflict(c, op.database, op.branch)).id;
            }
        }
    }
    // A new branch starts at an already exported snapshot: publish its head right away.
    if (b.command == Command::CREATE_BRANCH)
        PublishBranchHeads(c, op.database, op.branch);
    string uri;
    Value published(LogicalType::TIMESTAMP_TZ);
    if (op.snapshot && b.command != Command::CREATE_BRANCH) {
        auto s = c.GetSnapshot(op.database, op.snapshot, false);
        uri = s.URI();
        published = Time(s.published_at);
        if (export_state == "not_required" && s.export_state != "not_required") {
            export_state = s.export_state;
            export_key = s.export_key;
        }
    } else
        uri = "diff://db/" + op.database + "/branches/" + op.branch;
    // A snapshot published as a branch of its own reports that branch and state.
    state.rows.push_back({Value(op.id), Value(op.database), Value(forked_branch.empty() ? op.branch : forked_branch),
                          Value::BIGINT(op.snapshot), Value::BIGINT(op.preservation), Value(uri), published,
                          Value(forked_branch.empty() ? op.state : string("published_as_branch")),
                          Value(export_state), Optional(export_key)});
}
void Execute(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
    auto &state = input.global_state->Cast<State>();
    auto &binding = input.bind_data->Cast<Binding>();
    if (!state.executed) {
        state.executed = true;
        string kind;
        switch (binding.command) {
        case Command::BRANCHES:
            kind = "branches";
            break;
        case Command::OPERATIONS:
            kind = "operations";
            break;
        case Command::DATABASES:
            kind = "databases";
            break;
        case Command::PROVIDERS:
            kind = "providers";
            break;
        case Command::ROOTS:
            kind = "roots";
            break;
        default:
            break;
        }
        if (!kind.empty()) {
            auto control = binding.repository->Get();
            if (kind == "providers")
                RefreshS3Credentials(context, *control);
            auto database = kind == "branches"
                                ? ResolveTarget(context, *control, binding.args[0].GetValue<string>()).database
                                : string();
            state.cursor = control->List(kind, database);
        } else
            Run(context, binding, state);
    }
    if (state.cursor) {
        idx_t count = 0;
        std::vector<differential::MetadataCell> row;
        while (count < STANDARD_VECTOR_SIZE && state.cursor->Next(row)) {
            for (idx_t i = 0; i < row.size(); i++) {
                Value value(binding.types[i]);
                if (!row[i].null) {
                    if (binding.types[i] == LogicalType::VARCHAR) {
                        value = Value(row[i].text);
                        // differential_providers: name the DuckDB secret currently supplying credentials.
                        if (binding.command == Command::PROVIDERS && i == 5 && row[i].text == "AWS SDK default chain" &&
                            !row[1].null) {
                            auto identity = row[1].text + (row[4].null ? string() : " @ " + row[4].text);
                            if (auto credentials = differential::SecretCredentialsFor(identity))
                                value = Value("DuckDB secret " + credentials->secret_name);
                        }
                    } else if (binding.types[i] == LogicalType::TIMESTAMP_TZ)
                        value = Time(row[i].number);
                    else if (binding.types[i] == LogicalType::BOOLEAN)
                        value = Value::BOOLEAN(row[i].number != 0);
                    else
                        value = Value::BIGINT(row[i].number);
                }
                output.SetValue(i, count, value);
            }
            ++count;
        }
        output.SetCardinality(count);
        return;
    }
    auto n = MinValue<idx_t>(STANDARD_VECTOR_SIZE, state.rows.size() - state.offset);
    for (idx_t i = 0; i < n; i++)
        for (idx_t j = 0; j < state.rows[state.offset + i].size(); j++)
            output.SetValue(j, i, state.rows[state.offset + i][j]);
    state.offset += n;
    output.SetCardinality(n);
}
} // namespace

// ---------------------------------------------------------------------------------------
// Documentation in duckdb_functions(): a description, one runnable example, categories, and the
// names of the positional arguments (named parameters keep their own names). Attached to the
// registered catalog entries, so the parameter names follow each overload's own parameter order.
namespace {
struct FunctionDoc {
    const char *name;
    const char *description;
    const char *example;
    vector<string> positional;
    vector<string> categories;
};
const vector<FunctionDoc> &FunctionDocs() {
    static const vector<FunctionDoc> docs = {
        {"differential_merge_ancestry",
         "Inspects recorded local snapshot parents and best common ancestor candidates, including expired history. "
         "Incomplete and ambiguous history is reported without guessing a base.",
         "SELECT * FROM differential_merge_ancestry('sales', 2, 3);",
         {"database", "ours", "theirs"}, {"differential", "introspection"}},
        {"differential_merge_resolutions",
         "Reads the frozen typed resolution choices of a merge request. Does not read mutable resolution source tables.",
         "SELECT * FROM differential_merge_resolutions('sales', 'merge-1');",
         {"database", "request"}, {"differential", "introspection"}},
        {"differential_merge_branch_preflight",
         "Checks local or single-writer S3 branch merge readiness without publishing or recording a request. Returns selected input IDs, "
         "expected action, destination/target state, conflict counts and bounded diagnostics including pending recovery operations. "
         "Native final-state validation defaults to true. Checks are advisory; use expected snapshot IDs when executing.",
         "SELECT * FROM differential_merge_branch_preflight('sales', 'ours', 'theirs', 'ours');",
         {"database", "ours", "theirs", "destination"}, {"differential", "introspection"}},
        {"differential_merge_branch_preview",
         "Previews published branch heads using a unique proven best common ancestor. "
         "Returns resolved input IDs, counts and bounded typed conflicts; validate => true checks final constraints. "
         "schema_mode => 'additive' allows supported new tables and nullable column additions; 'catalog' also merges table/nullable-column removals and view definitions. Catalog conflicts are reported separately.",
         "SELECT * FROM differential_merge_branch_preview('sales', 'ours', 'theirs', validate => true);",
         {"database", "ours", "theirs"}, {"differential", "introspection"}},
        {"differential_merge_branch",
         "Merges published local or single-writer S3 branch heads using a unique proven best common ancestor. "
         "Destination equal to ours updates that detached, clean target; otherwise creates a new branch. "
         "Returns merge_action (created_branch, merged, fast_forward, already_up_to_date). "
         "Optional expected_base_snapshot_id/expected_ours_snapshot_id/expected_theirs_snapshot_id must be supplied together and guard the reviewed selection. "
         "The request records its input snapshot selection and schema_mode once; source live changes are excluded. "
         "schema_mode => 'additive' allows supported new tables and nullable column additions; 'catalog' also merges table/nullable-column removals and view definitions.",
         "SELECT * FROM differential_merge_branch('sales', 'ours', 'theirs', 'merged', request_id => 'merge-1');",
         {"database", "ours", "theirs", "destination"}, {"differential", "lifecycle"}},
        {"differential_merge_snapshots_preview",
         "Compares three explicit snapshots of a local or single-writer S3 database using typed, cell-level merge rules. Returns "
         "per-table counts and byte-bounded typed conflict details; validate => true checks final constraints in disposable staging. "
         "schema_mode => 'additive' allows supported new tables and nullable column additions; 'catalog' also merges table/nullable-column removals and view definitions. Catalog conflicts are reported separately.",
         "SELECT * FROM differential_merge_snapshots_preview('sales', 1, 2, 3);",
         {"database", "base", "ours", "theirs"},
         {"differential", "introspection"}},
        {"differential_merge_status",
         "Lists active merges and previews in this process and repository, with phase, progress and resource counters. "
         "Durable request outcomes are in differential_merge_requests().",
         "SELECT phase, tables_completed, tables_total FROM differential_merge_status();",
         {}, {"differential", "introspection"}},
        {"differential_merge_requests",
         "Inspects a bounded page of durable merge requests without S3 access. Includes inputs, destination, retained "
         "result, frozen schema policy, original branch identities, input guards, target fences and recovery guidance. "
         "Latest-attempt diagnostics include a count, phase, safe error code and timestamps; incomplete does not imply liveness. "
         "Ordered by operation_id; after_operation_id is an exclusive pagination cursor.",
         "SELECT * FROM differential_merge_requests('sales', request_id => 'merge-1');",
         {"database"}, {"differential", "introspection"}},
        {"differential_abort_merge",
         "Aborts an unpublished merge by operation_id, releasing its target fence, reservation and retained result. Recovers a "
         "published S3 result instead; uncertain remote outcomes retain state. Serialized with merge attempts.",
         "SELECT * FROM differential_abort_merge('sales', 'merge-operation-uuid', request_id => 'abort-1');",
         {"database", "operation_id"}, {"differential", "lifecycle"}},
        {"differential_merge_snapshots",
         "Merges three explicit snapshots into a new branch after native constraint validation and checkpointing. "
         "Supports local and single-writer S3 storage; S3 publication is synchronous and recoverable. Conflicts prevent publication. "
         "schema_mode => 'catalog' also merges table/nullable-column removals and view definitions; 'additive' only allows supported table/column additions. The default is strict.",
         "SELECT * FROM differential_merge_snapshots('sales', 1, 2, 3, 'merged', request_id => 'merge-1');",
         {"database", "base", "ours", "theirs", "destination"},
         {"differential", "lifecycle"}},
        {"differential_snapshots",
         "Lists every snapshot of a differential database (for a shared database also other writers' snapshots, "
         "read from S3) with commit metadata, publication and S3 export state.",
         "SELECT snapshot_id, snapshot_name, export_state FROM differential_snapshots('sales');",
         {"database"}, {"differential", "introspection"}},
        {"differential_current_snapshot",
         "Returns the snapshot an attachment reads, or the latest snapshot of the attached branch or database.",
         "SELECT id FROM differential_current_snapshot('sales');", {"database"}, {"differential", "introspection"}},
        {"differential_branches",
         "Lists the branches of a differential database with their head snapshot and, for shared databases, their "
         "state relative to the S3 branch head.",
         "SELECT branch_name, head_snapshot_id, remote_state FROM differential_branches('sales');", {"database"},
         {"differential", "introspection"}},
        {"differential_databases", "Lists the databases of the differential repository with their data path and writer mode.",
         "SELECT database_name, data_path, writers FROM differential_databases();", {}, {"differential", "introspection"}},
        {"differential_operations",
         "Lists the durable operation records of the repository, used to replay requests by request_id.",
         "SELECT kind, state, snapshot_id FROM differential_operations();", {}, {"differential", "introspection"}},
        {"differential_providers",
         "Lists the repository's storage locations (metadata store, local pools, S3 locations) with their S3 "
         "configuration and credential source, never credentials.",
         "SELECT data_path, credential_source FROM differential_providers();", {}, {"differential", "introspection"}},
        {"differential_retention_roots", "Lists why each stored database image is retained (snapshots, branches, pins, exports).",
         "SELECT reason, identity FROM differential_retention_roots();", {}, {"differential", "introspection"}},
        {"differential_storage_stats",
         "Returns storage, S3, cache and timing counters of the repository and process as metric/value rows.",
         "SELECT metric, value FROM differential_storage_stats() WHERE metric = 'local_payload_bytes';", {},
         {"differential", "introspection"}},
        {"differential_status", "Reports the extension's capabilities, durability profiles, limits and last background "
                                "publication error as capability/status rows.",
         "SELECT capability, status FROM differential_status();", {}, {"differential", "introspection"}},
        {"differential_remote_heads",
         "Lists the branch heads exported to an S3 location, each with the path to ATTACH its latest snapshot.",
         "SELECT database_name, branch_name, head FROM differential_remote_heads('s3://my-bucket/lake/');",
         {"location"}, {"differential", "s3", "introspection"}},
        {"differential_writers",
         "Lists the advisory 'being written' markers of a shared database in S3: which hosts are writing which "
         "branches.",
         "SELECT branch_name, host, live FROM differential_writers('team');", {"database"},
         {"differential", "shared", "introspection"}},
        {"differential_followers",
         "Lists this process's followers (ATTACH ... FOLLOW true): what each follows, the state it reads, when it "
         "last checked S3 and its last error.",
         "SELECT alias, follows, epoch, checked_at FROM differential_followers();", {},
         {"differential", "s3", "introspection"}},
        {"differential_create_snapshot",
         "Checkpoints an attached writable branch and publishes it as an immutable snapshot with optional commit "
         "metadata; exports it to S3 and, for shared databases, moves the S3 branch head.",
         "SELECT snapshot_id, export_state FROM differential_create_snapshot('sales', name => 'v1', message => 'first "
         "load');",
         {"database"}, {"differential", "lifecycle"}},
        {"differential_create_branch", "Creates a writable branch from a snapshot, sharing all data with it.",
         "SELECT branch_id FROM differential_create_branch('sales', 'dev', snapshot => 1);", {"database", "name"},
         {"differential", "lifecycle"}},
        {"differential_clone_database",
         "Creates an independent database from a snapshot of another one, on the same data path and sharing its "
         "data.",
         "SELECT database_id FROM differential_clone_database('sales', 'sales_copy');", {"database", "name"},
         {"differential", "lifecycle"}},
        {"differential_restore",
         "Resets a detached branch to a snapshot after preserving its current contents as a snapshot.",
         "SELECT snapshot_id, preservation_snapshot_id FROM differential_restore('sales', 1, branch => 'dev');",
         {"database", "snapshot_id"}, {"differential", "lifecycle"}},
        {"differential_drop_branch",
         "Preserves a detached branch's contents as a snapshot and drops the branch (in a shared database, for "
         "every writer).",
         "SELECT preservation_snapshot_id FROM differential_drop_branch('sales', 'dev');", {"database", "branch"},
         {"differential", "lifecycle"}},
        {"differential_expire_snapshots",
         "Retires snapshots selected by version list and/or age so their local data can be collected; S3 exports "
         "remain.",
         "SELECT snapshot_id FROM differential_expire_snapshots('sales', versions => [1], dry_run => true);",
         {"database"}, {"differential", "lifecycle"}},
        {"differential_evict_snapshots",
         "Drops the local copy of snapshots whose S3 export completed; they are then read through S3.",
         "SELECT snapshot_id FROM differential_evict_snapshots('sales', versions => [1], dry_run => true);",
         {"database"}, {"differential", "s3"}},
        {"differential_retry_export",
         "Finishes pending S3 exports of a database (all, or one snapshot) and republishes its branch heads.",
         "SELECT snapshot_id, export_state FROM differential_retry_export('sales');", {"database", "snapshot_id"},
         {"differential", "s3"}},
        {"differential_claim_branch_heads",
         "Takes a single-writer database's S3 branch heads over after a copy of the repository wrote them.",
         "SELECT branch_name, snapshot_id FROM differential_claim_branch_heads('sales');", {"database"},
         {"differential", "s3"}},
        {"differential_resolve_conflict",
         "Reconciles a diverged branch of a shared database with its S3 head, keeping local work as a new branch "
         "or as a local snapshot.",
         "SELECT kept_snapshot_id, kept_branch FROM differential_resolve_conflict('team', keep => 'branch');",
         {"database"}, {"differential", "shared"}},
        {"differential_import_database",
         "Copies an offline native DuckDB file into a new differential database and publishes it as snapshot 1.",
         "SELECT database_id FROM differential_import_database('/data/native.duckdb', 'imported');",
         {"source_path", "name"}, {"differential", "import"}},
        {"differential_recover_export",
         "Copies an exported snapshot from S3 into a new writable differential database, verifying every chunk.",
         "SELECT database_id FROM differential_recover_export('s3://my-bucket/lake/descriptors/<sha256>', "
         "'restored');",
         {"location", "name"}, {"differential", "s3", "import"}},
        {"differential_recover_branch",
         "Recovers the newest shipped state of a branch (SHIP_WAL) from S3 into a new local database: its chain's "
         "base, read on demand, plus every shipped epoch; DuckDB replays the WAL at the first ATTACH. "
         "same_identity => true keeps the original identities (a failover of the writer).",
         "SELECT database_id, shipped_epoch FROM differential_recover_branch('s3://my-bucket/lake/heads/sales/main', "
         "'sales_recovered');",
         {"location", "name"}, {"differential", "s3", "import"}},
        {"differential_cleanup_old_files",
         "Collects local data that no snapshot, branch, pin or pending export references any more.",
         "SELECT deleted_objects FROM differential_cleanup_old_files(cleanup_all => true);", {},
         {"differential", "maintenance"}},
        {"differential_delete_orphaned_files",
         "Removes files in local pools that no mapping references, left behind by crashes.",
         "SELECT orphaned_files FROM differential_delete_orphaned_files(dry_run => true);", {},
         {"differential", "maintenance"}},
        {"differential_compact", "Returns free pages of the repository's metadata store to the file system.",
         "SELECT pages_reclaimed FROM differential_compact(max_pages => 1000);", {}, {"differential", "maintenance"}},
        {"differential_cleanup_remote_files",
         "Garbage-collects an S3 location in two phases: condemns data no branch head or unexpired snapshot "
         "references, and deletes what stayed condemned and unreachable since before older_than (default: a "
         "week ago).",
         "SELECT condemned_objects, deleted_bytes FROM differential_cleanup_remote_files('s3://my-bucket/lake/', "
         "dry_run => true);",
         {"location"}, {"differential", "s3", "maintenance"}},
        {"differential_cleanup_metadata",
         "Deletes completed operation records and history entries of expired snapshots older than older_than; "
         "request IDs of deleted operations no longer replay.",
         "SELECT table_name, rows FROM differential_cleanup_metadata(older_than => now() - INTERVAL 30 DAY, "
         "dry_run => true);",
         {}, {"differential", "maintenance"}},
    };
    return docs;
}
// Descriptions of the catalog-scoped functions (<catalog>.snapshots() and friends).
FunctionDescription ScopedDescription(const string &name) {
    FunctionDescription description;
    description.categories = {"differential", "introspection"};
    if (name == "snapshots") {
        description.description = "Lists every snapshot of this differential attachment's database.";
        description.examples = {"SELECT snapshot_id, snapshot_name FROM sales.snapshots();"};
    } else if (name == "current_snapshot") {
        description.description = "Returns the snapshot this attachment reads, or its branch's latest snapshot.";
        description.examples = {"SELECT id FROM sales.current_snapshot();"};
    } else {
        description.description = "Lists the branches of this differential attachment's database.";
        description.examples = {"SELECT branch_name, head_snapshot_id FROM sales.branches();"};
    }
    return description;
}
} // namespace
// Names a control function's positional parameters after its FunctionDocs entry (they stay
// positional-only) and declares its named parameters as typed options.
void DeclareParameters(TableFunction &f, const vector<std::pair<const char *, LogicalType>> &named) {
    auto &signature = f.GetSignature();
    for (auto &doc : FunctionDocs()) {
        if (f.GetName() != doc.name)
            continue;
        for (idx_t i = 0; i < signature.GetParameterCount() && i < doc.positional.size(); i++)
            signature.GetParameter(i).SetName(Identifier(doc.positional[i]));
    }
    if (!named.empty())
        signature.WithTypedKwargs("options", [&](TypedKwargs &options) {
            for (auto &entry : named)
                options.Add(entry.first, entry.second);
        });
}
void DescribeFunctions(DatabaseInstance &db) {
    auto &catalog = Catalog::GetSystemCatalog(db);
    auto transaction = CatalogTransaction::GetSystemTransaction(db);
    auto &schema = catalog.GetSchema(transaction, Identifier::DefaultSchema());
    for (auto &doc : FunctionDocs()) {
        auto found = schema.GetEntry(transaction, CatalogType::TABLE_FUNCTION_ENTRY, Identifier(doc.name));
        if (!found)
            continue; // not registered in this build
        auto &entry = found->Cast<TableFunctionCatalogEntry>();
        entry.descriptions.clear();
        // One description per overload, matched by its positional types (parameter names come
        // from the signature, see DeclareParameters).
        for (idx_t i = 0; i < entry.functions.Size(); i++) {
            auto &signature = entry.functions.GetFunctionByOffset(i)->GetSignature();
            FunctionDescription description;
            for (idx_t argument = 0; argument < signature.GetPositionalParameterCount(); argument++)
                description.parameter_types.push_back(signature.GetParameter(argument).GetType());
            description.description = doc.description;
            description.examples = {doc.example};
            description.categories = doc.categories;
            entry.descriptions.push_back(std::move(description));
        }
    }
}
const vector<string> &ScopedFunctionNames() {
    static const vector<string> names = {"snapshots", "current_snapshot", "branches"};
    return names;
}
unique_ptr<CatalogEntry> CreateScopedFunction(std::shared_ptr<differential::Repository> repository, Catalog &catalog,
                                              SchemaCatalogEntry &schema, const string &name) {
    Command command;
    if (name == "snapshots")
        command = Command::SNAPSHOTS;
    else if (name == "current_snapshot")
        command = Command::CURRENT_SNAPSHOT;
    else if (name == "branches")
        command = Command::BRANCHES;
    else
        return nullptr;
    TableFunction function(Identifier(name), {}, Execute, Bind, Init);
    auto info = make_shared_ptr<Info>(std::move(repository), command);
    info->catalog = &catalog;
    function.function_info = std::move(info);
    CreateTableFunctionInfo create(function);
    create.descriptions.push_back(ScopedDescription(name));
    create.internal = true;
    auto entry = make_uniq<TableFunctionCatalogEntry>(catalog, schema, create);
    entry->internal = true;
    return std::move(entry);
}
void RegisterControlFunctions(ExtensionLoader &loader, std::shared_ptr<differential::Repository> repository) {
    auto add = [&](const char *name, Command command, vector<LogicalType> arguments,
                   vector<std::pair<const char *, LogicalType>> named) {
        TableFunction f(name, std::move(arguments), Execute, Bind, Init);
        f.function_info = make_shared_ptr<Info>(repository, command);
        if (!ReadOnly(command))
            named.insert(named.begin(), {"request_id", V});
        DeclareParameters(f, named);
        loader.RegisterFunction(f);
    };
    vector<std::pair<const char *, LogicalType>> data_path = {
        {"data_path", V},          {"region", V},           {"endpoint", V},           {"credential_profile", V},
        {"path_style", B},         {"allow_http", B},       {"max_requests", I},       {"retries", I},
        {"connect_timeout_ms", I}, {"request_timeout_ms", I}, {"max_range_bytes", I}, {"multipart_threshold", I},
        {"secret", V}};
    vector<std::pair<const char *, LogicalType>> selection = {
        {"versions", LogicalType::LIST(I)}, {"older_than", T}, {"dry_run", B}};
    // Introspection
    add("differential_snapshots", Command::SNAPSHOTS, {V}, {{"refresh", V}});
    add("differential_current_snapshot", Command::CURRENT_SNAPSHOT, {V}, {});
    add("differential_branches", Command::BRANCHES, {V}, {});
    add("differential_databases", Command::DATABASES, {}, {});
    add("differential_operations", Command::OPERATIONS, {}, {});
    add("differential_providers", Command::PROVIDERS, {}, {});
    add("differential_retention_roots", Command::ROOTS, {}, {});
    add("differential_storage_stats", Command::STATS, {}, {});
    add("differential_writers", Command::WRITERS, {V}, {});
    add("differential_followers", Command::FOLLOWERS, {}, {});
    {
        // S3 options as for DATA_PATH (the location is the positional argument).
        vector<std::pair<const char *, LogicalType>> s3(data_path.begin() + 1, data_path.end());
        add("differential_remote_heads", Command::REMOTE_HEADS, {V}, s3);
    }
    // Lifecycle
    vector<std::pair<const char *, LogicalType>> merge_limits = {
        {"max_input_rows", I}, {"max_input_bytes", I}, {"max_stage_write_bytes", I}, {"timeout_ms", I},
        {"resolution_tables", LogicalType::MAP(V, V)}, {"schema_mode", V}};
    add("differential_merge_snapshots", Command::MERGE, {V, I, I, I, V}, merge_limits);
    auto branch_limits = merge_limits;
    branch_limits.emplace_back("max_ancestry_nodes", I);
    auto guarded_branch = branch_limits;
    guarded_branch.insert(guarded_branch.end(), {{"expected_base_snapshot_id", I}, {"expected_ours_snapshot_id", I},
                                                {"expected_theirs_snapshot_id", I}});
    add("differential_merge_branch", Command::MERGE_BRANCH, {V, V, V, V}, guarded_branch);
    auto preflight_options = branch_limits;
    preflight_options.emplace_back("validate", B);
    add("differential_merge_branch_preflight", Command::MERGE_BRANCH_PREFLIGHT, {V, V, V, V}, preflight_options);
    merge_limits.insert(merge_limits.end(), {{"conflict_limit", I}, {"conflict_offset", I},
                                            {"conflict_bytes", I}, {"validate", B}});
    add("differential_merge_snapshots_preview", Command::MERGE_PREVIEW, {V, I, I, I}, merge_limits);
    branch_limits.insert(branch_limits.end(), {{"conflict_limit", I}, {"conflict_offset", I},
                                              {"conflict_bytes", I}, {"validate", B}});
    add("differential_merge_branch_preview", Command::MERGE_BRANCH_PREVIEW, {V, V, V}, branch_limits);
    add("differential_merge_status", Command::MERGE_STATUS, {}, {});
    add("differential_merge_requests", Command::MERGE_REQUESTS, {V}, {{"request_id", V}, {"max_results", I}, {"after_operation_id", V}});
    add("differential_abort_merge", Command::ABORT_MERGE, {V, V}, {{"timeout_ms", I}});
    add("differential_merge_ancestry", Command::MERGE_ANCESTRY, {V, I, I}, {{"max_ancestry_nodes", I}, {"timeout_ms", I}});
    add("differential_merge_resolutions", Command::MERGE_RESOLUTIONS, {V, V}, {});
    add("differential_create_snapshot", Command::CREATE_SNAPSHOT, {V},
        {{"name", V}, {"author", V}, {"message", V}, {"extra_info", V}, {"export_mode", V}});
    add("differential_create_branch", Command::CREATE_BRANCH, {V, V}, {{"snapshot", I}, {"branch", V}});
    add("differential_clone_database", Command::CLONE_DATABASE, {V, V},
        {{"snapshot", I}, {"branch", V}, {"export_mode", V}});
    add("differential_restore", Command::RESTORE, {V, I}, {{"branch", V}, {"export_mode", V}});
    add("differential_drop_branch", Command::DROP_BRANCH, {V, V}, {{"export_mode", V}});
    add("differential_expire_snapshots", Command::EXPIRE_SNAPSHOTS, {V}, selection);
    add("differential_evict_snapshots", Command::EVICT_SNAPSHOTS, {V}, selection);
    {
        // Overloads: all pending exports of a database, or one snapshot.
        TableFunctionSet retry("differential_retry_export");
        for (auto arguments : {vector<LogicalType> {V}, vector<LogicalType> {V, I}}) {
            TableFunction f("differential_retry_export", arguments, Execute, Bind, Init);
            f.function_info = make_shared_ptr<Info>(repository, Command::RETRY_EXPORT);
            DeclareParameters(f, {{"request_id", V}});
            retry.AddFunction(f);
        }
        loader.RegisterFunction(retry);
    }
    add("differential_claim_branch_heads", Command::CLAIM_HEADS, {V}, {});
    add("differential_resolve_conflict", Command::RESOLVE_CONFLICT, {V}, {{"keep", V}, {"branch", V}, {"name", V}});
    add("differential_import_database", Command::IMPORT_DATABASE, {V, V}, data_path);
    add("differential_recover_export", Command::RECOVER_EXPORT, {V, V}, data_path);
    {
        vector<std::pair<const char *, LogicalType>> s3(data_path.begin() + 1, data_path.end());
        s3.push_back({"same_identity", B});
        add("differential_recover_branch", Command::RECOVER_BRANCH, {V, V}, s3);
    }
    // Maintenance
    add("differential_cleanup_old_files", Command::CLEANUP_OLD_FILES, {},
        {{"dry_run", B}, {"cleanup_all", B}, {"max_objects", I}});
    add("differential_delete_orphaned_files", Command::DELETE_ORPHANED_FILES, {}, {{"dry_run", B}, {"max_objects", I}});
    add("differential_compact", Command::COMPACT, {}, {{"max_pages", I}});
    {
        vector<std::pair<const char *, LogicalType>> remote(data_path.begin() + 1, data_path.end());
        remote.push_back({"older_than", T});
        remote.push_back({"dry_run", B});
        add("differential_cleanup_remote_files", Command::CLEANUP_REMOTE_FILES, {V}, remote);
    }
    add("differential_cleanup_metadata", Command::CLEANUP_METADATA, {}, {{"older_than", T}, {"dry_run", B}});
}
} // namespace duckdb
