#define DUCKDB_EXTENSION_MAIN
#include "differential_extension.hpp"
#include "differential_metrics.hpp"
#include "differential_hooks.hpp"
#include "duckdb.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/catalog/duck_catalog.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/planner/operator/logical_attach.hpp"
#include "duckdb/planner/operator/logical_set.hpp"
#include "duckdb/planner/planner_extension.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include <cstdlib>
#include <optional>
#include <cstdio>
#include <filesystem>
#include "differential_store.hpp"
#include "differential_control.hpp"
#include "differential_repository.hpp"
#include "differential_export.hpp"
#include "duckdb_adapter/sql_options.hpp"
#include "duckdb_adapter/secrets.hpp"
#include "duckdb_adapter/remote_cache.hpp"
#include "duckdb_adapter/differential_catalog.hpp"
#include "duckdb_adapter/follow.hpp"
#include "differential_shipping.hpp"
#include "duckdb_adapter/spill_policy.hpp"
#include "duckdb_adapter/logical_merge.hpp"
#include <map>
#include <mutex>
#include <memory>

namespace duckdb {
void RegisterProbeFileSystem(ExtensionLoader &loader);
void RegisterControlFunctions(ExtensionLoader &, std::shared_ptr<differential::Repository>);
void DescribeFunctions(DatabaseInstance &);
std::shared_ptr<differential::Repository> RegisterDurableFileSystem(ExtensionLoader &loader);
void ProbeTrace(const char *op, const string &path) {
    auto trace = differential::Hook("DIFFERENTIAL_PROBE_TRACE");
    if (trace) {
        auto f = std::fopen(trace, "a");
        if (!f)
            throw IOException("Cannot open probe trace");
        std::fprintf(f, "%s\t%s\t0\t0\n", op, path.c_str());
        std::fclose(f);
    }
}
// Preserve every native operation; the subclass only measures synchronous exported calls.
class MeasuredNativeTransactions : public DuckTransactionManager {
  public:
    explicit MeasuredNativeTransactions(AttachedDatabase &db) : DuckTransactionManager(db) {}
    ErrorData CommitTransaction(ClientContext &context, Transaction &transaction) override {
        differential::Timer timer(differential::Metric::NativeCommit);
        return DuckTransactionManager::CommitTransaction(context, transaction);
    }
    void Checkpoint(ClientContext &context, bool force) override {
        differential::Timer timer(differential::Metric::ExplicitCheckpoint);
        DuckTransactionManager::Checkpoint(context, force);
    }
};
namespace {
// SET callbacks are plain function pointers: find the repository of the calling instance.
std::mutex registry_mutex;
std::map<DatabaseInstance *, std::weak_ptr<differential::Repository>> registry;
std::shared_ptr<differential::Repository> RepositoryFor(DatabaseInstance &db) {
    std::lock_guard<std::mutex> guard(registry_mutex);
    auto found = registry.find(&db);
    return found == registry.end() ? nullptr : found->second.lock();
}
void SetRepository(ClientContext &context, SetScope, Value &parameter) {
    auto repository = RepositoryFor(*context.db);
    if (!repository)
        throw InvalidInputException("Differential durable backend is not loaded");
    if (parameter.IsNull())
        throw InvalidInputException("differential_repository cannot be NULL");
    repository->Select(parameter.ToString());
}
// Process-wide: every DuckDB instance in the process shares one budget of parallel S3 requests.
void SetS3MaxRequests(ClientContext &, SetScope, Value &parameter) {
    if (parameter.IsNull())
        throw InvalidInputException("differential_s3_max_requests cannot be NULL");
    auto value = BigIntValue::Get(parameter.DefaultCastAs(LogicalType::BIGINT));
    if (value < 1 || value > int64_t(differential::MAX_PARALLEL_REQUESTS))
        throw InvalidInputException("differential_s3_max_requests must be between 1 and %d",
                                    int(differential::MAX_PARALLEL_REQUESTS));
    differential::SetRequestLimit(unsigned(value));
}
// Process-wide export settings: every DuckDB instance in the process shares them (the last SET applies).
string Choice(const Value &parameter, const char *setting, std::initializer_list<const char *> choices) {
    if (parameter.IsNull())
        throw InvalidInputException("%s cannot be NULL", setting);
    auto value = StringUtil::Lower(parameter.ToString());
    for (auto choice : choices)
        if (value == choice)
            return value;
    throw InvalidInputException("%s must be one of: %s", setting, StringUtil::Join(vector<string>(choices.begin(), choices.end()), ", "));
}
void SetExportVerification(ClientContext &, SetScope, Value &parameter) {
    differential::SetExportVerification(
        Choice(parameter, "differential_export_verification", {"checksum", "read_back"}) == "read_back");
}
void SetExportStaging(ClientContext &, SetScope, Value &parameter) {
    differential::SetExportStaging(Choice(parameter, "differential_export_staging", {"disk", "memory"}) == "memory");
}
void SetGroupCommit(ClientContext &, SetScope, Value &parameter) {
    if (parameter.IsNull())
        throw InvalidInputException("differential_group_commit cannot be NULL");
    differential::Store::SetGroupCommit(BooleanValue::Get(parameter.DefaultCastAs(LogicalType::BOOLEAN)));
}
void SetRemoteCacheSize(ClientContext &, SetScope, Value &parameter) {
    ParseRemoteCacheSize(parameter); // validate only; read when a remote file is opened
}
void SetRepositoryShared(ClientContext &context, SetScope, Value &parameter) {
    auto repository = RepositoryFor(*context.db);
    if (!repository)
        throw InvalidInputException("Differential durable backend is not loaded");
    repository->Share(!parameter.IsNull() && BooleanValue::Get(parameter.DefaultCastAs(LogicalType::BOOLEAN)));
}
struct DifferentialStorageInfo : StorageExtensionInfo {
    std::shared_ptr<differential::Repository> repository;
    std::shared_ptr<TimeTravelCache> time_travel = std::make_shared<TimeTravelCache>();
    explicit DifferentialStorageInfo(std::shared_ptr<differential::Repository> repository)
        : repository(std::move(repository)) {}
};
// PhysicalAttach normally strips "differential:" before DatabaseManager checks for remote
// paths. That exposes "s3://" to DuckDB's httpfs autoloader before our storage callback runs.
// Keep the prefix and supply TYPE explicitly so only our AWS SDK reader handles this path.
// This hook changes the plan only: repository access and S3 work remain in ATTACH execution.
void PlanDifferential(PlannerExtensionInput &, BoundStatement &statement) {
    if (!statement.plan)
        return;
    if (statement.plan->type == LogicalOperatorType::LOGICAL_SET) {
        auto &setting = statement.plan->Cast<LogicalSet>();
        if (setting.scope != SetScope::VARIABLE && setting.name == "temp_directory" &&
            !setting.value.IsNull())
            ValidateDifferentialTempDirectory(setting.value.ToString());
        return;
    }
    if (statement.plan->type != LogicalOperatorType::LOGICAL_ATTACH)
        return;
    auto &info = *statement.plan->Cast<LogicalAttach>().info;
    auto type = info.options.find("type");
    bool prefixed = StringUtil::CIEquals(info.path.substr(0, 13), "differential:") &&
                    differential::S3Location::Is(info.path.substr(13));
    if (prefixed && (type == info.options.end() || type->second.ToString().empty()))
        info.options["type"] = Value("differential");
    else if (type != info.options.end() && StringUtil::CIEquals(type->second.ToString(), "differential") &&
             differential::S3Location::Is(info.path))
        info.path = "differential:" + info.path;
}
// ATTACH 'differential:<database>' [AS alias] (BRANCH, SNAPSHOT_VERSION | SNAPSHOT_TIME | SNAPSHOT_NAME,
// READ_ONLY, CREATE_IF_NOT_EXISTS, DATA_PATH, S3 options), or 'differential:s3://…/descriptors/<sha256>'.
// The callback runs while ATTACH executes, before DuckDB opens the file: it resolves (or creates)
// the target and rewrites the path to the internal diff:// URI.
unique_ptr<Catalog> AttachDifferential(optional_ptr<StorageExtensionInfo> storage_info, ClientContext &context,
                                       AttachedDatabase &db, const string &, AttachInfo &info, AttachOptions &options) {
    differential::Timer timer(differential::Metric::AttachResolve);
    ValidateDifferentialTempDirectory(BufferManager::GetBufferManager(context).GetTemporaryDirectory());
    auto target = info.path;
    if (StringUtil::CIEquals(target.substr(0, 13), "differential:") &&
        differential::S3Location::Is(target.substr(13)))
        target = target.substr(13);
    auto storage = storage_info ? &static_cast<DifferentialStorageInfo &>(*storage_info) : nullptr;
    auto repository = storage ? storage->repository : nullptr;
    auto catalog = [&](bool time_travel_snapshot = false) -> unique_ptr<Catalog> {
        return make_uniq<DifferentialCatalog>(db, repository, storage->time_travel, time_travel_snapshot,
                                              info.path.rfind("diff://export/", 0) == 0 ||
                                                  info.path.rfind("diff://follow/", 0) == 0);
    };
    if (target.rfind("diff://", 0) == 0) {
        // Internal URI; native options pass through unchanged.
        bool time_travel_snapshot = options.options.erase(TIME_TRAVEL_OPTION) > 0;
        if (!repository)
            return make_uniq<DuckCatalog>(db); // test-build pass-through probe backend
        if (repository->Configuration().open)
            RefreshS3Credentials(context, *repository->Get());
        return catalog(time_travel_snapshot);
    }
    if (!repository)
        throw InvalidInputException("Differential durable backend is not loaded");
    std::optional<differential::Timer> opening(std::in_place, differential::Metric::AttachRepositoryOpen);
    auto control = repository->Get();
    opening.reset();
    RefreshS3Credentials(context, *control);
    string branch, snapshot_name, writers, on_conflict, on_busy;
    std::optional<bool> publish_on_detach, publish_on_checkpoint, follow;
    std::optional<string> ship_wal;
    std::optional<int64_t> ship_interval, follow_interval; // milliseconds
    std::optional<int64_t> version, time;
    bool create = true;
    DataPathOptions data;
    // Consume differential options; native ones (BLOCK_SIZE, STORAGE_VERSION, ...) stay for the
    // native storage manager, which rejects anything still unknown.
    for (auto it = options.options.begin(); it != options.options.end();) {
        auto name = StringUtil::Lower(it->first);
        auto &value = it->second;
        bool consumed = true;
        if (name == "branch" || name == "snapshot_version" || name == "snapshot_time" || name == "snapshot_name" ||
            name == "create_if_not_exists" || name == "writers" || name == "on_conflict" ||
            name == "publish_on_detach" || name == "publish_on_checkpoint" || name == "on_busy" ||
            name == "ship_wal" || name == "ship_interval" || name == "follow" || name == "follow_interval") {
            if (value.IsNull())
                throw BinderException("ATTACH option %s cannot be NULL", name);
            if (name == "branch")
                branch = value.ToString();
            else if (name == "snapshot_version")
                version = BigIntValue::Get(value.DefaultCastAs(LogicalType::BIGINT));
            else if (name == "snapshot_time")
                time = Microseconds(value);
            else if (name == "snapshot_name")
                snapshot_name = value.ToString();
            else if (name == "publish_on_detach")
                publish_on_detach = BooleanValue::Get(value.DefaultCastAs(LogicalType::BOOLEAN));
            else if (name == "publish_on_checkpoint")
                publish_on_checkpoint = BooleanValue::Get(value.DefaultCastAs(LogicalType::BOOLEAN));
            else if (name == "follow")
                follow = BooleanValue::Get(value.DefaultCastAs(LogicalType::BOOLEAN));
            else if (name == "ship_wal") {
                ship_wal = StringUtil::Lower(value.ToString());
                if (*ship_wal != "off" && *ship_wal != "async" && *ship_wal != "sync")
                    throw BinderException("SHIP_WAL must be 'off', 'async' or 'sync'");
            } else if (name == "ship_interval" || name == "follow_interval") {
                auto seconds = DoubleValue::Get(value.DefaultCastAs(LogicalType::DOUBLE));
                bool shipping = name == "ship_interval";
                if (!(seconds >= (shipping ? 0.0 : 1.0)) || seconds > 3600)
                    throw BinderException(shipping ? "SHIP_INTERVAL must be between 0 and 3600 seconds"
                                                   : "FOLLOW_INTERVAL must be between 1 and 3600 seconds");
                (shipping ? ship_interval : follow_interval) = int64_t(seconds * 1000);
            }
            else if (name == "on_busy") {
                on_busy = StringUtil::Lower(value.ToString());
                if (on_busy != "warn" && on_busy != "error" && on_busy != "ignore")
                    throw BinderException("ON_BUSY must be 'warn', 'error' or 'ignore'");
            } else if (name == "on_conflict") {
                on_conflict = StringUtil::Lower(value.ToString());
                if (on_conflict != "fail" && on_conflict != "branch")
                    throw BinderException("ON_CONFLICT must be 'fail' or 'branch'");
            } else if (name == "writers") {
                writers = StringUtil::Lower(value.ToString());
                if (writers != "single" && writers != "shared")
                    throw BinderException("WRITERS must be 'single' or 'shared'");
            } else
                create = BooleanValue::Get(value.DefaultCastAs(LogicalType::BOOLEAN));
        } else
            consumed = data.Take(name, value);
        it = consumed ? options.options.erase(it) : std::next(it);
    }
    int selectors = int(bool(version)) + int(bool(time)) + int(!snapshot_name.empty());
    if (selectors > 1)
        throw BinderException("Use only one of SNAPSHOT_VERSION, SNAPSHOT_TIME and SNAPSHOT_NAME");
    auto read_only = [&] {
        options.access_mode = AccessMode::READ_ONLY;
        db.SetReadOnlyDatabase();
    };
    if (follow_interval && !follow.value_or(false))
        throw BinderException("FOLLOW_INTERVAL applies to FOLLOW true");
    if (differential::S3Location::Is(target)) {
        if (!branch.empty() || selectors || data.data_path_given || !writers.empty() || !on_conflict.empty() ||
            publish_on_detach || publish_on_checkpoint || !on_busy.empty() || ship_wal || ship_interval)
            throw BinderException("An export attachment takes only S3 provider options (and FOLLOW)");
        ApplyDifferentialSecret(context, data);
        std::pair<string, string> head_location;
        if (follow.value_or(false)) {
            // A follower moves to the branch's newest shipped commit (or exported snapshot).
            if (!SplitHeadLocation(target, head_location))
                throw BinderException("FOLLOW needs a branch path: 'differential:s3://<bucket>/<prefix>/heads/"
                                      "<database>[/<database uuid>][/<branch>]'");
            auto provider = EnsureS3(context, *control, data, head_location.first);
            std::shared_ptr<Follower> follower;
            try {
                auto location = differential::ResolveBranchLocation(*control, provider, head_location.second);
                follower = std::make_shared<Follower>(db.GetDatabase(), control, provider, location,
                                                      follow_interval.value_or(10000), db.GetName().GetIdentifierName());
                info.path = follower->Start();
            } catch (const Exception &) {
                throw;
            } catch (const std::exception &error) {
                throw BinderException(error.what());
            }
            read_only();
            auto result = catalog();
            result->Cast<DifferentialCatalog>().SetFollower(follower);
            Follow(follower);
            return result;
        }
        if (SplitHeadLocation(target, head_location)) {
            // Latest exported snapshot of a branch, through its S3 branch head.
            auto provider = EnsureS3(context, *control, data, head_location.first);
            differential::Head head;
            try {
                head = differential::ResolveHead(*control, provider, head_location.second);
            } catch (const Exception &) {
                throw;
            } catch (const std::exception &error) {
                throw BinderException(error.what());
            }
            info.path = "diff://export/" + provider + "/" + head.descriptor;
        } else {
            auto location = SplitExportLocation(target);
            auto provider = EnsureS3(context, *control, data, location.first);
            info.path = "diff://export/" + provider + "/" + location.second;
        }
        read_only();
        return catalog();
    }
    if (follow)
        throw BinderException("FOLLOW applies to S3 branch paths ('differential:s3://…/heads/<database>/…')");
    if (target.empty())
        throw BinderException("Expected ATTACH 'differential:<database>'");
    if (target.find(':') != string::npos || target.find('/') != string::npos)
        throw BinderException("Invalid differential URI: expected 'differential:<database>' or a diff:// URI");
    auto database = control->FindDatabase(target);
    auto branch_name = branch.empty() ? string("main") : branch;
    if (!database.empty() && !writers.empty() && writers != control->Writers(database))
        throw BinderException("Database \"%s\" uses WRITERS '%s'; WRITERS is chosen when a database is created",
                              target, control->Writers(database));
    if (writers == "shared" || (!database.empty() && control->Writers(database) == "shared")) {
        // Several repositories write this database through S3: start from (or move to) the branch's
        // S3 head. Snapshot selectors attach local snapshots as for any database.
        ApplyDifferentialSecret(context, data);
        string provider;
        if (database.empty()) {
            if (!differential::S3Location::Is(data.data_path))
                throw BinderException("WRITERS 'shared' needs an S3 DATA_PATH ('s3://bucket/prefix/')");
            provider = EnsureDataPath(context, *control, data);
        } else {
            if (data.data_path_given) {
                auto config = data.ConfigFor(data.data_path);
                auto existing = control->DataPath(database);
                if (!differential::S3Location::Is(data.data_path) ||
                    "s3://" + config.bucket + "/" + config.prefix != existing)
                    throw BinderException("Database \"%s\" uses DATA_PATH '%s'; the data path cannot change", target,
                                          existing);
                if (!data.explicit_s3.empty())
                    EnsureDataPath(context, *control, data);
            }
            provider = control->Profile(database);
        }
        if (ship_wal || ship_interval)
            throw BinderException("SHIP_WAL applies to single-writer databases; shared databases publish through S3 "
                                  "already");
        if (!selectors) {
            bool read_only_attach = options.access_mode == AccessMode::READ_ONLY;
            if (read_only_attach && (!on_conflict.empty() || publish_on_detach || publish_on_checkpoint ||
                                     !on_busy.empty()))
                throw BinderException("ON_CONFLICT, ON_BUSY, PUBLISH_ON_CHECKPOINT and PUBLISH_ON_DETACH apply to "
                                      "writable attachments of shared databases");
            differential::Branch prepared;
            try {
                prepared = differential::PrepareSharedBranch(*control, provider, target, branch_name, create,
                                                             !read_only_attach);
            } catch (const Exception &) {
                throw;
            } catch (const std::exception &error) {
                throw BinderException(error.what());
            }
            if (read_only_attach) {
                auto id = control->CurrentSnapshot(prepared.database, prepared.id);
                if (!id)
                    throw BinderException("[differential:not_found] Branch \"%s\" has no snapshot to attach READ_ONLY",
                                          branch_name);
                info.path = "diff://db/" + prepared.database + "/snapshots/" + std::to_string(id);
                read_only();
                return catalog();
            }
            info.path = prepared.URI();
            differential::SharedWriting writing;
            writing.on_conflict = on_conflict.empty() ? "fail" : on_conflict;
            writing.publish_on_detach = publish_on_detach.value_or(false);
            writing.publish_on_checkpoint = publish_on_checkpoint.value_or(true);
            writing.on_busy = on_busy.empty() ? "warn" : on_busy;
            differential::SetSharedWriting(*control, prepared.id, writing);
            // Snapshots a previous session could not publish (for example it exited first).
            if (!control->PendingExports(prepared.database).empty())
                differential::SchedulePublication(control, prepared.database, prepared.id);
            // Other writers' history, for listings and AT, in the background.
            differential::ScheduleHistorySync(control, prepared.database);
            return catalog();
        }
        if (database.empty())
            throw BinderException("[differential:not_found] Database \"%s\" does not exist", target);
        // Snapshot selectors also reach other writers' snapshots (from S3 commit records). A time on a
        // branch that is not here starts at its S3 head.
        try {
            try {
                differential::SyncHistory(*control, database);
            } catch (const std::exception &) {
                // S3 unreachable: local snapshots and the cached history still resolve.
            }
            if (time) {
                int64_t start = 0;
                try {
                    control->GetBranch(database, branch_name);
                } catch (const std::exception &) {
                    start = differential::RemoteHeadSnapshot(*control, database, branch_name);
                    if (!start)
                        throw;
                }
                version = control->SnapshotAt(database, branch_name, *time, start).id;
                time.reset();
            }
        } catch (const Exception &) {
            throw;
        } catch (const std::exception &error) {
            throw BinderException(error.what());
        }
    }
    if (!on_conflict.empty() || publish_on_detach || publish_on_checkpoint || !on_busy.empty())
        throw BinderException("ON_CONFLICT, ON_BUSY, PUBLISH_ON_CHECKPOINT and PUBLISH_ON_DETACH apply to writable "
                              "attachments of shared databases (WRITERS 'shared')");
    if (database.empty()) {
        if (!create || selectors || options.access_mode == AccessMode::READ_ONLY || branch_name != "main")
            throw BinderException("[differential:not_found] Database \"%s\" does not exist", target);
        // Settings from a differential secret apply when a database is created.
        ApplyDifferentialSecret(context, data);
        auto provider = EnsureDataPath(context, *control, data);
        try {
            database = control->CreateDatabase(target, "", provider).database;
        } catch (...) {
            database = control->FindDatabase(target); // created concurrently by another session
            if (database.empty())
                throw;
        }
    } else if (data.data_path_given) {
        auto requested = differential::S3Location::Is(data.data_path)
                             ? [&] {
                                   auto config = data.ConfigFor(data.data_path);
                                   return "s3://" + config.bucket + "/" + config.prefix;
                               }()
                             : differential::LocalPool::Canonical(data.data_path);
        auto existing = control->DataPath(database);
        if (requested != existing)
            throw BinderException("Database \"%s\" uses DATA_PATH '%s'; the data path cannot change", target,
                                  existing.empty() ? string("(default)") : existing);
        if (!data.explicit_s3.empty())
            EnsureDataPath(context, *control, data); // verifies the options match the registration
    }
    if (ship_wal || ship_interval) {
        // Stored per database: every later writable attachment ships until SHIP_WAL 'off'.
        if (selectors || options.access_mode == AccessMode::READ_ONLY)
            throw BinderException("SHIP_WAL and SHIP_INTERVAL apply to writable attachments");
        auto config = differential::GetShipConfig(*control, database);
        if (ship_wal)
            config.mode = *ship_wal;
        if (ship_interval) {
            if (config.mode == "off")
                throw BinderException("SHIP_INTERVAL needs SHIP_WAL 'async' or 'sync'");
            config.interval_ms = *ship_interval;
        }
        try {
            differential::SetShipConfig(*control, database, config);
        } catch (const std::exception &error) {
            throw BinderException(error.what());
        }
    }
    if (selectors || options.access_mode == AccessMode::READ_ONLY) {
        int64_t id;
        if (version)
            id = *version;
        else if (!snapshot_name.empty())
            id = control->Resolve("diff://db/" + database + "/snapshot-names/" + snapshot_name).snapshot.id;
        else if (time)
            id = control->SnapshotAt(database, branch_name, *time).id;
        else {
            // READ_ONLY on a branch reads its latest public snapshot: live files have one writer.
            id = control->CurrentSnapshot(database, control->GetBranch(database, branch_name).id);
            if (!id)
                throw BinderException("[differential:not_found] Branch \"%s\" has no snapshot to attach READ_ONLY",
                                      branch_name);
        }
        info.path = "diff://db/" + database + "/snapshots/" + std::to_string(id);
        read_only();
    } else {
        auto attached = control->GetBranch(database, branch_name);
        differential::StartShipping(control, attached); // when the database ships its WAL
        info.path = attached.URI();
    }
    return catalog();
}
} // namespace
// The differential storage type keeps the native catalog and adds timing instrumentation.
// Durability, internal generations and snapshots are implemented below the filesystem
// boundary, so a raw ATTACH 'diff://…' (internal URI) behaves identically.
class DifferentialStorage : public StorageExtension {
  public:
    explicit DifferentialStorage(std::shared_ptr<differential::Repository> repository) {
        storage_info = make_shared_ptr<DifferentialStorageInfo>(std::move(repository));
        attach = AttachDifferential;
        create_transaction_manager = [](optional_ptr<StorageExtensionInfo>, AttachedDatabase &db,
                                        Catalog &) -> unique_ptr<TransactionManager> {
            return make_uniq<MeasuredNativeTransactions>(db);
        };
    }
    void OnCheckpointStart(AttachedDatabase &db, CheckpointOptions) override {
        ProbeTrace("checkpoint.start", db.GetStorageManager().GetDBPath());
    }
    void OnCheckpointEnd(AttachedDatabase &db, CheckpointOptions) override {
        ProbeTrace("checkpoint.end", db.GetStorageManager().GetDBPath());
    }
};
struct ProbeInfo : TableFunctionInfo {
    std::shared_ptr<differential::Repository> repository;
    explicit ProbeInfo(std::shared_ptr<differential::Repository> repository) : repository(std::move(repository)) {}
};
struct ProbeBind : TableFunctionData {
    string alias;
    std::shared_ptr<differential::Repository> repository;
    ProbeBind(string alias, std::shared_ptr<differential::Repository> repository)
        : alias(std::move(alias)), repository(std::move(repository)) {}
};
struct ProbeState : GlobalTableFunctionState {
    bool done = false;
};
unique_ptr<FunctionData> BindProbe(ClientContext &, TableFunctionBindInput &input, vector<LogicalType> &types,
                                   vector<Identifier> &names) {
    if (input.inputs[0].IsNull())
        throw InvalidInputException("Alias cannot be NULL");
    types.push_back(LogicalType::BOOLEAN);
    names.push_back("checkpoint_completed");
    types.push_back(LogicalType::VARCHAR);
    names.push_back("captured_root");
    return make_uniq<ProbeBind>(input.inputs[0].GetValue<string>(), input.info->Cast<ProbeInfo>().repository);
}
unique_ptr<GlobalTableFunctionState> InitProbe(ClientContext &, TableFunctionInitInput &) {
    return make_uniq<ProbeState>();
}
// Test-only diagnostic checkpoint (compiled with DIFFERENTIAL_TEST_HOOKS).
void ExecuteProbe(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
    auto &state = input.global_state->Cast<ProbeState>();
    if (state.done)
        return;
    state.done = true;
    if (!context.transaction.IsAutoCommit())
        throw TransactionException("Management requires autocommit");
    auto &binding = input.bind_data->Cast<ProbeBind>();
    auto db = DatabaseManager::Get(context).GetDatabase(context, Identifier(binding.alias));
    if (!db || !db->GetStorageExtension())
        throw InvalidInputException("Expected differential attachment");
    auto path = db->GetStorageManager().GetDBPath();
    ProbeTrace("control.execute", path);
    db->GetTransactionManager().Checkpoint(context, false);
    string captured;
    if (differential::Hook("DIFFERENTIAL_PROBE_CAPTURE")) {
        if (binding.repository) {
            // Durable backend: the checkpoint's header sync sealed the latest generation.
            // Retain it with a lock-file-less pin, which collection treats as permanently live.
            auto control = binding.repository->Get();
            auto file = control->store->FileId(path.substr(7));
            captured = control->store->Generation(file);
            control->RetainRootPermanently(captured);
        } else {
            auto root = differential::Hook("DIFFERENTIAL_PROBE_ROOT");
            auto local = FileSystem::CreateLocal();
            // Flags from FileOpenFlags' integer constants: FileFlags' FileOpenFlags constants are
            // defined in DuckDB's (C++11) library, and referencing them from this C++17 code makes GNU
            // ld report a second definition.
            auto source = local->OpenFile(string(root) + "/" + path.substr(7),
                                          FileOpenFlags(FileOpenFlags::FILE_FLAGS_READ));
            auto dest = local->OpenFile(string(root) + "/captured.snapshot",
                                        FileOpenFlags(FileOpenFlags::FILE_FLAGS_WRITE |
                                                      FileOpenFlags::FILE_FLAGS_FILE_CREATE |
                                                      FileOpenFlags::FILE_FLAGS_EXCLUSIVE_CREATE));
            char buffer[65536];
            int64_t n;
            while ((n = source->Read(buffer, sizeof(buffer))) != 0)
                dest->Write(buffer, n);
            dest->Sync();
            ProbeTrace("capture.completed", path);
        }
    }
    ProbeTrace("control.completed", path);
    output.SetValue(0, 0, Value::BOOLEAN(true));
    output.SetValue(1, 0, captured.empty() ? Value() : Value(captured));
    output.SetCardinality(1);
}
struct StatusInfo : TableFunctionInfo {
    std::shared_ptr<differential::Repository> repository;
    explicit StatusInfo(std::shared_ptr<differential::Repository> repository) : repository(std::move(repository)) {}
};
struct StatusBinding : TableFunctionData {
    std::shared_ptr<differential::Repository> repository;
    explicit StatusBinding(std::shared_ptr<differential::Repository> repository)
        : repository(std::move(repository)) {}
};
unique_ptr<FunctionData> BindStatus(ClientContext &, TableFunctionBindInput &input, vector<LogicalType> &types,
                                    vector<Identifier> &names) {
    types = {LogicalType::VARCHAR, LogicalType::VARCHAR};
    names = {"capability", "status"};
    return make_uniq<StatusBinding>(input.info->Cast<StatusInfo>().repository);
}
void ExecuteStatus(ClientContext &, TableFunctionInput &input, DataChunk &output) {
    auto &state = input.global_state->Cast<ProbeState>();
    if (state.done)
        return;
    state.done = true;
    auto repository = input.bind_data->Cast<StatusBinding>().repository;
    auto configuration = repository ? repository->Configuration() : differential::Repository::State {};
    vector<std::pair<string, string>> rows = {
        {"v1_complete", "false"},
        {"duckdb_target", string(DuckDB::LibraryVersion()) + " / " + DuckDB::SourceID()},
        {"backend", repository ? "durable SQLite chunks and native filesystem" : "pass-through probe"},
        {"repository_root", configuration.directory},
        {"repository_open", configuration.open ? "true" : "false"},
        {"repository_shared", configuration.shared ? "true (group-writable, for several local users)"
                                                   : "false (owner only)"},
        {"local_store_format", "2 (256 KiB block-aligned chunks, log-file byte ranges, paged manifests, "
                               "append-only segments: write journal and payload in one)"},
        {"local_providers",
         "chunks and log ranges in repository segments by default; immutable-file pools with DATA_PATH "
         "'/directory'"},
        {"qualified_catalog_definitions",
         "native catalog names retained in views/macros; use original attachment alias when required"},
        {"local_durability", "native-equivalent: writes appended to a segment before returning (survive process "
                             "death); durable at native FileSync: segment fsync, then SQLite FULL (fsync, as native "
                             "DuckDB on every OS); local reads verified by CRC32C; "
                             "power loss untested"},
        {"group_commit", string(differential::Store::GroupCommit() ? "on" : "off") +
                             ": concurrent syncs of different databases in this process share one durable "
                             "transaction (up to 16 MiB of new data; a larger sync commits alone)"},
        {"public_snapshot_identity_and_lifecycle", "local snapshots, branches/clones, compound "
                                                   "restore/delete and retirement; broader races under test"},
        {"sql_surface", "DuckLake-aligned: ATTACH 'differential:<database>' options, SET differential_*, "
                        "database-first differential_* functions; AT (VERSION|TIMESTAMP) via hidden snapshot attachments "
                        "(reader pin at bind); <catalog>.snapshots()/current_snapshot()/branches()"},
        {"retention", "local process pins; incremental queued GC (automatic bounded steps after checkpoints, "
                      "SET differential_auto_gc=false to disable) and incremental compaction"},
        {"s3_transport", "AWS SDK 1.11.820; MinIO and protocol faults tested; real AWS untested"},
        {"s3_credentials", "matching TYPE s3 secret (by SCOPE), else CREDENTIAL_PROFILE or the AWS SDK default "
                           "chain; never accepted as ATTACH or function options"},
        {"s3_snapshot_profile", "locally durable commits; remotely durable published snapshots in 8 MiB packs; "
                                "pending exports retained; completed exports may drop local copies"},
        {"export_uploads", string("verified by ") +
                               (differential::ExportReadsBack() ? "reading them back (read_back)"
                                                                : "the service's SHA-256 checksum (checksum)") +
                               "; staged " + (differential::ExportStagesInMemory() ? "in memory" : "on disk")},
        {"distributed_writers", "optional per database (ATTACH ... WRITERS 'shared', S3 DATA_PATH): optimistic "
                                "commits to S3 branch heads; a conflicting update fails and keeps its snapshot"},
        {"shared_publication", "checkpoints of shared databases are snapshots published in the background; "
                               "last error: " + [] {
                                   auto error = differential::LastPublicationError();
                                   return error.empty() ? std::string("none") : error;
                               }()},
        {"metadata_authority", "one local repository authority per host; one writer process per branch and "
                               "repository; shared databases: S3 branch heads and commit records"},
        {"remote_gc", "explicit, per S3 location (differential_cleanup_remote_files): unreachable data is condemned, "
                      "then deleted once condemned and unreachable for the grace period; last error: " + [] {
                          auto error = differential::LastRemoteCollectionError();
                          return error.empty() ? std::string("none") : error;
                      }()},
        {"wal_shipping", "opt-in per single-writer S3 database (ATTACH ... SHIP_WAL 'async' | 'sync', SHIP_INTERVAL "
                         "seconds): every published state of a writable branch is shipped to S3 as numbered epochs "
                         "(dbs/<uuid>/log/...); recover with differential_recover_branch"},
        {"followers", "ATTACH 'differential:s3://.../heads/<database>/...' (FOLLOW true, FOLLOW_INTERVAL seconds): "
                      "read-only, moves to the newest shipped commit or exported snapshot; one request per interval "
                      "when nothing changed; differential_followers()"},
        {"test_hooks", differential::test_hooks ? "compiled (test build)" : "absent"},
        {"limits", "256 KiB chunks; 64 MiB manifest pages; 1 TiB logical file; write overlay per database "
                   "published in the background beyond 16 MiB, by the writer beyond 64 MiB; 64 MiB segments; "
                   "64 MiB local verified chunk cache; 8 MiB SQLite cache per store; remote reads cached in "
                   "DuckDB's external file cache (memory_limit) and optionally on disk "
                   "(differential_remote_cache_size); upload staging 8 MiB per parallel request; differential_s3_max_requests "
                   "(default 16) parallel S3 requests per process; no "
                   "authority disk quota"}};
    for (idx_t i = 0; i < rows.size(); i++) {
        output.SetValue(0, i, Value(rows[i].first));
        output.SetValue(1, i, Value(rows[i].second));
    }
    output.SetCardinality(rows.size());
}
} // namespace duckdb
namespace duckdb {
// Shared by the static build (DifferentialExtension::Load) and the loadable entry point.
static void LoadInternal(ExtensionLoader &loader) {
    auto store = duckdb::RegisterDurableFileSystem(loader);
    if (!store && differential::test_hooks)
        duckdb::RegisterProbeFileSystem(loader);
    if (store)
        duckdb::RegisterControlFunctions(loader, store);
    auto &config = duckdb::DBConfig::GetConfig(loader.GetDatabaseInstance());
    if (store) {
        std::lock_guard<std::mutex> guard(duckdb::registry_mutex);
        duckdb::registry[&loader.GetDatabaseInstance()] = store;
    }
    duckdb::RegisterDifferentialSecret(loader);
    if (!config.HasExtensionOption("differential_repository"))
        config.AddExtensionOption("differential_repository",
                                  "Differential repository root (relative paths resolve against the working directory); set before first use",
                                  duckdb::LogicalType::VARCHAR,
                                  store ? duckdb::Value(store->Configuration().directory) : duckdb::Value(),
                                  duckdb::SetRepository, duckdb::SetScope::GLOBAL);
    if (!config.HasExtensionOption("differential_repository_shared"))
        config.AddExtensionOption("differential_repository_shared",
                                  "Create the repository for several local users (group-writable); set before first use",
                                  duckdb::LogicalType::BOOLEAN, duckdb::Value::BOOLEAN(false),
                                  duckdb::SetRepositoryShared, duckdb::SetScope::GLOBAL);
    if (!config.HasExtensionOption("differential_remote_cache_size"))
        config.AddExtensionOption("differential_remote_cache_size",
                                  "Size of the repository's disk cache for data read from S3 (e.g. '4GB'); 0 = off. "
                                  "Read when a remote file is opened",
                                  duckdb::LogicalType::VARCHAR, duckdb::Value("0"), duckdb::SetRemoteCacheSize,
                                  duckdb::SetScope::GLOBAL);
    if (!config.HasExtensionOption("differential_s3_max_requests"))
        config.AddExtensionOption("differential_s3_max_requests",
                                  "Parallel S3 requests per process (1-64), shared by all S3 locations and DuckDB "
                                  "instances in the process",
                                  duckdb::LogicalType::BIGINT, duckdb::Value::BIGINT(16), duckdb::SetS3MaxRequests,
                                  duckdb::SetScope::GLOBAL);
    if (!config.HasExtensionOption("differential_group_commit"))
        config.AddExtensionOption("differential_group_commit",
                                  "Commit concurrent native syncs of different databases in one durable transaction "
                                  "(one device flush). Process-wide",
                                  duckdb::LogicalType::BOOLEAN, duckdb::Value::BOOLEAN(true), duckdb::SetGroupCommit,
                                  duckdb::SetScope::GLOBAL);
    if (!config.HasExtensionOption("differential_export_verification"))
        config.AddExtensionOption("differential_export_verification",
                                  "How exports verify each uploaded object: 'checksum' (the S3 service checks its "
                                  "SHA-256) or 'read_back' (also download and hash it). Process-wide",
                                  duckdb::LogicalType::VARCHAR, duckdb::Value("checksum"),
                                  duckdb::SetExportVerification, duckdb::SetScope::GLOBAL);
    if (!config.HasExtensionOption("differential_export_staging"))
        config.AddExtensionOption("differential_export_staging",
                                  "Where exports stage objects before uploading them: 'disk' (the repository's "
                                  "staging directory) or 'memory'. Process-wide",
                                  duckdb::LogicalType::VARCHAR, duckdb::Value("disk"), duckdb::SetExportStaging,
                                  duckdb::SetScope::GLOBAL);
    if (!config.HasExtensionOption("differential_merge_max_concurrent"))
        config.AddExtensionOption("differential_merge_max_concurrent",
                                  "Concurrent merge/preview/abort executions per process (1-16); excess calls fail with merge_busy",
                                  duckdb::LogicalType::BIGINT, duckdb::Value::BIGINT(2),
                                  [](duckdb::ClientContext &, duckdb::SetScope, duckdb::Value &value) {
                                      duckdb::SetMergeConcurrency(value.GetValue<int64_t>());
                                  }, duckdb::SetScope::GLOBAL);
    if (!config.HasExtensionOption("differential_auto_gc"))
        config.AddExtensionOption("differential_auto_gc",
                                  "Run bounded differential garbage collection after native checkpoints",
                                  duckdb::LogicalType::BOOLEAN, duckdb::Value::BOOLEAN(true));
    if (!config.HasExtensionOption("differential_snapshot_on_checkpoint"))
        config.AddExtensionOption("differential_snapshot_on_checkpoint",
                                  "Publish every completed native checkpoint of a writable branch as a public snapshot",
                                  duckdb::LogicalType::BOOLEAN, duckdb::Value::BOOLEAN(false));
    duckdb::StorageExtension::Register(config, "differential", duckdb::make_shared_ptr<duckdb::DifferentialStorage>(store));
    duckdb::PlannerExtension planner;
    planner.post_bind_function = duckdb::PlanDifferential;
    duckdb::PlannerExtension::Register(config, std::move(planner));
    duckdb::TableFunction status("differential_status", vector<duckdb::LogicalType> {}, duckdb::ExecuteStatus, duckdb::BindStatus,
                                 duckdb::InitProbe);
    status.function_info = duckdb::make_shared_ptr<duckdb::StatusInfo>(store);
    loader.RegisterFunction(status);
    // Descriptions, examples and parameter names for duckdb_functions().
    duckdb::DescribeFunctions(loader.GetDatabaseInstance());
    if (differential::test_hooks) {
        duckdb::TableFunction probe("differential_probe_checkpoint", {duckdb::LogicalType::VARCHAR},
                                    duckdb::ExecuteProbe, duckdb::BindProbe, duckdb::InitProbe);
        probe.function_info = duckdb::make_shared_ptr<duckdb::ProbeInfo>(store);
        loader.RegisterFunction(probe);
    }
}
void DifferentialExtension::Load(ExtensionLoader &loader) { LoadInternal(loader); }
std::string DifferentialExtension::Name() { return "differential"; }
std::string DifferentialExtension::Version() const { return "v1.0.0"; }
} // namespace duckdb
extern "C" {
DUCKDB_CPP_EXTENSION_ENTRY(differential, loader) { duckdb::LoadInternal(loader); }
}
