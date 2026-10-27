// logical-cell-v1: state comparison, reconciliation, and application are separate phases.
#include "logical_merge.hpp"
#include "merge_types.hpp"
#include "differential_hooks.hpp"
#include "differential_export.hpp"
#include "secrets.hpp"
#include "spill_policy.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/parser/constraints/check_constraint.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/index_catalog_entry.hpp"
#include "duckdb/catalog/default/default_types.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/parser/qualified_name.hpp"
#include <set>
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/type_visitor.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/file_system.hpp"
#include <chrono>
#include <map>
#include <filesystem>
#include <thread>
#include <mutex>
#include <atomic>
#include <array>
#include <functional>

namespace duckdb {
namespace {
string Q(const string &s, char quote = '"') {
    string r(1, quote);
    for (auto c : s) {
        r += c;
        if (c == quote)
            r += c;
    }
    return r + quote;
}
string Join(const vector<string> &v, const string &separator = ",") { return StringUtil::Join(v, separator); }
[[noreturn]] void Unsupported(const string &reason) {
    throw InvalidInputException("[differential:merge_unsupported] %s", reason);
}
using Clock = std::chrono::steady_clock;
double Millis(Clock::time_point start) {
    return std::chrono::duration<double, std::milli>(Clock::now() - start).count();
}
struct Progress {
    string repository, id, operation, database, mode, phase = "starting", table;
    int64_t tables = 0, completed = 0, input_rows = 0, input_bytes = 0;
    int64_t changed_rows = 0, conflicts = 0, detail_bytes = 0, timeout_ms = 0;
    Clock::time_point started = Clock::now();
    std::shared_ptr<differential::MergeWriteBudget> writes;
};
std::mutex progress_mutex;
std::map<uint64_t, std::shared_ptr<Progress>> progress_registry;
uint64_t next_execution = 0;
std::atomic<int64_t> max_concurrent_merges{2};
struct Execution {
    ClientContext &caller;
    differential::Control &control;
    std::shared_ptr<Progress> progress;
    uint64_t token;
    differential::Operation attempt_operation;
    string attempt_id, attempt_phase = "request_locked";
    std::set<string> recorded_phases;
    Execution(ClientContext &caller, differential::Control &control, const string &database,
              const string &mode, const string &operation, const MergeOptions &options)
        : caller(caller), control(control), progress(std::make_shared<Progress>()) {
        progress->repository = control.store->directory;
        progress->database = database;
        progress->operation = operation;
        progress->mode = mode;
        progress->timeout_ms = options.timeout_ms;
        progress->writes = std::make_shared<differential::MergeWriteBudget>(options.max_stage_write_bytes);
        std::lock_guard<std::mutex> guard(progress_mutex);
        if (progress_registry.size() >= uint64_t(max_concurrent_merges.load()))
            throw IOException("[differential:merge_busy] Concurrent merge/preview limit reached; retry later");
        token = ++next_execution;
        progress->id = to_string(getpid()) + ":" + to_string(token);
        progress_registry.emplace(token, progress);
    }
    ~Execution() {
        std::lock_guard<std::mutex> guard(progress_mutex);
        progress_registry.erase(token);
    }
    template <class F> void Update(F fn) {
        std::lock_guard<std::mutex> guard(progress_mutex);
        fn(*progress);
    }
    void Check() {
        if (caller.IsInterrupted())
            throw InterruptException();
        if (Millis(progress->started) >= progress->timeout_ms)
            throw IOException("[differential:merge_timeout] Merge execution deadline exceeded");
    }
    void StartAttempt(const differential::Operation &operation) {
        attempt_operation = operation;
        attempt_id = control.BeginMergeAttempt(operation).id;
        recorded_phases.insert("request_locked");
    }
    void Failed(const std::exception *error) noexcept {
        if (attempt_id.empty()) return;
        try {
            string code = "merge_error";
            if (error) {
                ErrorData data(*error);
                const auto &message = data.RawMessage();
                if (data.Type() == ExceptionType::INTERRUPT) code = "cancelled";
                else if (data.Type() == ExceptionType::OUT_OF_MEMORY) code = "out_of_memory";
                else if (data.Type() == ExceptionType::CONSTRAINT) code = "merge_constraint";
                else if (message.compare(0, 14, "[differential:") == 0) {
                    auto end = message.find(']', 14);
                    if (end != string::npos && end <= 78)
                        code = differential::Control::SafeMergeErrorCode(message.substr(14, end - 14));
                }
            }
            control.UpdateMergeAttempt(attempt_operation, attempt_id, attempt_phase, code);
        } catch (...) {
            // Preserve the original exception (including cancellation/disk full).
            // An unwritable diagnostic leaves the last durable milestone incomplete.
        }
    }
    void Gate(const string &path) {
        auto fd = control.store->permissions.Open(path + ".entered", O_WRONLY | O_CLOEXEC);
        if (fd < 0)
            throw IOException("Cannot create merge test gate");
        close(fd);
        auto deadline = Clock::now() + std::chrono::seconds(30);
        while (!std::filesystem::exists(path + ".release")) {
            Check();
            if (Clock::now() > deadline)
                throw IOException("Merge test gate timed out");
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }
    void Phase(const string &phase, const string &table = "") {
        // Session teardown changes live progress to cleanup during unwinding;
        // retain the phase that actually failed for the durable attempt record.
        attempt_phase = phase;
        Update([&](Progress &p) { p.phase = phase; p.table = table; });
        Check();
        // Fixed, coarse milestones, at most once per attempt. Per-table work only
        // updates the process-local registry; it must not add a durable write/row.
        static const std::set<string> milestones = {"context_frozen", "target_locked", "materializing", "pinned", "comparison",
            "staging", "staged", "prepared", "uploading", "uploaded", "head_creating", "head_published", "publishing"};
        if (!attempt_id.empty() && milestones.count(phase) && recorded_phases.insert(phase).second)
            control.UpdateMergeAttempt(attempt_operation, attempt_id, phase);
        differential::Failpoint("DIFFERENTIAL_MERGE_CRASH", phase.c_str());
        if (auto selected = differential::Hook("DIFFERENTIAL_MERGE_PHASE_GATE")) {
            string gate(selected);
            auto separator = gate.find(':');
            if (gate.substr(0, separator) == phase && separator != string::npos)
                Gate(gate.substr(separator + 1));
        }
    }
};
// Fixed diagnostic types keep binding repository-free.
LogicalType DetailType() {
    auto row = LogicalType::LIST(MergeCellType());
    return LogicalType::STRUCT({{"key", row},
                                {"kind", LogicalType::VARCHAR},
                                {"columns", LogicalType::LIST(LogicalType::VARCHAR)},
                                {"base_present", LogicalType::BOOLEAN},
                                {"ours_present", LogicalType::BOOLEAN},
                                {"theirs_present", LogicalType::BOOLEAN},
                                {"base_values", row},
                                {"ours_values", row},
                                {"theirs_values", row}});
}
struct ColumnRemoval {
    string name;
    LogicalType type;
    std::array<bool, 3> input_present {{true, false, false}};
    std::array<string, 3> input_definitions;
};
struct Table {
    string schema, name, ddl;
    vector<string> columns;
    vector<LogicalType> types;
    vector<idx_t> keys;
    std::map<string, string> indexes;
    std::map<string, std::shared_ptr<CreateIndexInfo>> index_definitions;
    std::shared_ptr<CreateTableInfo> definition;
    vector<std::set<string>> input_columns;
    vector<Value> schema_changes, schema_conflicts;
    std::array<bool, 3> input_present {{true, true, true}};
    string table_change_source;
    bool rebuild_schema = false;
    bool drop_table = false;
    std::array<string, 3> input_definitions;
    vector<ColumnRemoval> removed_columns;
    string Qualified(const string &catalog) const { return Q(catalog) + "." + Q(schema) + "." + Q(name); }
    string InputRelation(const string &catalog, idx_t side) const {
        if (input_present.at(side)) return Qualified(catalog);
        vector<string> empty;
        for (idx_t i = 0; i < columns.size(); i++)
            empty.push_back("CAST(NULL AS " + types[i].ToString() + ") AS " + Q(columns[i]));
        return "(SELECT " + Join(empty) + " WHERE false)";
    }
    string InputColumn(idx_t column, idx_t side, const string &alias) const {
        return input_columns.at(side).count(StringUtil::Lower(columns[column]))
            ? alias + "." + Q(columns[column]) : "CAST(NULL AS " + types[column].ToString() + ")";
    }
};
LogicalType SchemaChangeType() {
    return LogicalType::STRUCT({{"column_name", LogicalType::VARCHAR}, {"source", LogicalType::VARCHAR},
                                {"sql_type", LogicalType::VARCHAR}, {"action", LogicalType::VARCHAR}});
}
LogicalType SchemaConflictType() {
    return LogicalType::STRUCT({{"column_name", LogicalType::VARCHAR}, {"kind", LogicalType::VARCHAR},
                                {"base_definition", LogicalType::VARCHAR}, {"ours_definition", LogicalType::VARCHAR},
                                {"theirs_definition", LogicalType::VARCHAR}, {"object_name", LogicalType::VARCHAR},
                                {"reason", LogicalType::VARCHAR}});
}
LogicalType TableIndexType() {
    return LogicalType::STRUCT({{"index_name", LogicalType::VARCHAR}, {"definition", LogicalType::VARCHAR}});
}
LogicalType TableChangeType() {
    return LogicalType::STRUCT({{"source", LogicalType::VARCHAR}, {"definition", LogicalType::VARCHAR},
                                {"indexes", LogicalType::LIST(TableIndexType())}});
}
using ViewShapes = std::map<string, std::pair<vector<Identifier>, vector<LogicalType>>>;
struct View {
    string name, ddl, canonical, action, source;
    std::set<string> dependencies;
    std::pair<vector<Identifier>, vector<LogicalType>> shape;
    std::array<string, 3> input_definitions;
    std::array<std::pair<vector<Identifier>, vector<LogicalType>>, 3> input_shapes;
    std::map<string, uint8_t> native_dependencies;
    std::array<bool, 3> input_tracks {{false, false, false}};
    vector<Value> schema_conflicts;
    bool drop = false;
};
using Views = std::map<string, View>; // Folded names; display/DDL retain native spelling.
LogicalType ViewChangeType() {
    auto columns = LogicalType::LIST(LogicalType::STRUCT({{"column_name", LogicalType::VARCHAR}, {"sql_type", LogicalType::VARCHAR}}));
    return LogicalType::STRUCT({{"action", LogicalType::VARCHAR}, {"source", LogicalType::VARCHAR},
                                {"definition", LogicalType::VARCHAR}, {"dependencies", LogicalType::LIST(LogicalType::VARCHAR)},
                                {"base_columns", columns}, {"ours_columns", columns}, {"theirs_columns", columns},
                                {"base_tracks_dependencies", LogicalType::BOOLEAN}, {"ours_tracks_dependencies", LogicalType::BOOLEAN},
                                {"theirs_tracks_dependencies", LogicalType::BOOLEAN}});
}
std::map<string, uint8_t> NativeViewDependencies(const CreateViewInfo &info) {
    std::map<string, uint8_t> result;
    for (auto &dep : info.dependencies.Set()) {
        if ((dep.entry.type != CatalogType::TABLE_ENTRY && dep.entry.type != CatalogType::VIEW_ENTRY) ||
            dep.entry.schema_path.size() != 1 || dep.entry.schema_path[0] != "main" ||
            dep.flags != DependencyDependentFlags().SetBlocking())
            Unsupported("view has unsupported native dependency metadata");
        result.emplace(CatalogTypeToString(dep.entry.type) + ":" + StringUtil::Lower(dep.entry.name.GetIdentifierName()), dep.flags.RawValue());
    }
    return result;
}
string CatalogReason(string message) {
    if (message.size() > 4096) {
        idx_t cut = 4096;
        while (cut && (static_cast<unsigned char>(message[cut]) & 0xc0) == 0x80) --cut;
        message = message.substr(0, cut) + " [truncated]";
    }
    return message;
}
Value CatalogConflict(const string &name, const string &kind, const std::array<string, 3> &definitions,
                      const string &reason, const string &column = "") {
    auto value = [&](idx_t side) { return definitions[side].empty() ? Value(LogicalType::VARCHAR) : Value(definitions[side]); };
    return Value::STRUCT(SchemaConflictType(), {column.empty() ? Value(LogicalType::VARCHAR) : Value(column), Value(kind), value(0), value(1), value(2),
                                              Value(name), Value(CatalogReason(reason))});
}
// Validate the complete selected dependency graph, independently of DuckDB's optional
// dependency tracking. No CASCADE and no catalog/search-path fallback for missing names.
bool ViewOrder(const Views &views, const std::set<string> &tables, vector<string> &order,
               string &bad_name, string &reason, const std::function<void()> &check) {
    std::map<string, int> state;
    std::map<string, idx_t> heights;
    std::function<bool(const string &, idx_t)> visit = [&](const string &name, idx_t depth) {
        check();
        if (state[name] == 1) {
            bad_name = name; reason = "Cycle in selected view dependencies at " + name; return false;
        }
        if (state[name] == 2) {
            if (depth + heights[name] - 1 > 64)
                throw IOException("[differential:merge_limit] View dependency depth exceeds 64");
            return true;
        }
        if (depth > 64) throw IOException("[differential:merge_limit] View dependency depth exceeds 64");
        state[name] = 1;
        idx_t height = 1;
        for (auto &dep : views.at(name).dependencies) {
            auto found = views.find(dep);
            if (found != views.end() && !found->second.drop) {
                if (!visit(dep, depth + 1)) return false;
                height = std::max(height, heights[dep] + 1);
            } else if (!tables.count(dep)) {
                bad_name = name; reason = "View references missing relation " + dep; return false;
            }
        }
        state[name] = 2; heights[name] = height; order.push_back(name); return true;
    };
    for (auto &entry : views)
        if (!entry.second.drop && !visit(entry.first, 1)) return false;
    return true;
}
// Type modifiers can themselves contain expressions in DuckDB 2.0. Inspect them
// before native binding can resolve a custom constructor or evaluate a function.
void CheckCastTarget(const ParsedExpression &expr, idx_t depth, idx_t &nodes) {
    if (depth > 32 || ++nodes > 1024)
        throw IOException("[differential:merge_limit] Cast type exceeds 32 nested levels or 1024 type nodes");
    if (expr.GetExpressionClass() == ExpressionClass::CONSTANT) return;
    if (expr.GetExpressionClass() != ExpressionClass::TYPE)
        Unsupported("cast type modifiers must be constant values or built-in types");
    auto &type = expr.Cast<TypeExpression>();
    if (!type.GetQualifiedName().QualificationToString().empty() ||
        DefaultTypeGenerator::GetDefaultType(type.GetTypeName()) == LogicalTypeId::INVALID)
        Unsupported("cast target must use unqualified built-in types: " + type.ToString());
    for (auto &child : type.GetChildren()) CheckCastTarget(*child, depth + 1, nodes);
}
// Deliberately bounded expression language. Temporal casts/arithmetic use the
// frozen native context; no sequence, macro, UDF, subquery or external function.
void CheckExpression(ClientContext &context, const ParsedExpression &expr, bool columns, bool view = false, idx_t depth = 0) {
    if (depth > 64)
        Unsupported("expression nesting exceeds 64");
    switch (expr.GetExpressionClass()) {
    case ExpressionClass::CONSTANT:
    case ExpressionClass::COMPARISON:
    case ExpressionClass::CONJUNCTION:
    case ExpressionClass::OPERATOR:
    case ExpressionClass::CASE:
    case ExpressionClass::BETWEEN:
        break;
    case ExpressionClass::CAST: {
        auto &target = expr.Cast<CastExpression>().TargetType();
        idx_t nodes = 0;
        CheckCastTarget(target, 0, nodes);
        // Native binding supplies widths/scales and recursive child types. The
        // same allowlist as table columns rejects JSON, ENUM and aliased leaves.
        CheckMergeType(TransformStringToLogicalType(target.ToString(), context));
        break;
    }
    case ExpressionClass::COLUMN_REF:
        if (!columns || expr.Cast<ColumnRefExpression>().ColumnNames().size() > 2)
            Unsupported("qualified or nonconstant expression");
        break;
    case ExpressionClass::STAR: {
        auto &star = expr.Cast<StarExpression>();
        if (!view || star.IsColumns() || !star.ReplaceList().empty() || star.Expression())
            Unsupported("extended star expression");
        break;
    }
    case ExpressionClass::FUNCTION: {
        auto &f = expr.Cast<FunctionExpression>();
        static const std::set<string> scalar = {"+", "-", "*", "/", "//", "%", "abs", "length", "lower", "upper"};
        static const std::set<string> aggregate = {"count", "count_star", "sum", "min", "max", "avg"};
        if (!f.GetQualifiedName().Catalog().empty() || !f.GetQualifiedName().Schema().empty() ||
            (!scalar.count(StringUtil::Lower(f.FunctionName().GetIdentifierName())) &&
             !(view && aggregate.count(StringUtil::Lower(f.FunctionName().GetIdentifierName())))) || f.Filter() || (f.OrderBy() && !f.OrderBy()->orders.empty()))
            Unsupported("expression function " + f.FunctionName());
        break;
    }
    default:
        Unsupported("expression " + expr.ToString());
    }
    ParsedExpressionIterator::EnumerateChildren(expr, [&](const ParsedExpression &child) {
        CheckExpression(context, child, columns, view, depth + 1);
    });
}
std::pair<string, string> NativeExpressionContext(ClientContext &context) {
    auto setting = [&](const string &name) {
        Value value;
        if (!context.TryGetCurrentSetting(Identifier(name), value)) return string();
        if (value.IsNull() || value.type() != LogicalType::VARCHAR || value.GetValue<string>().empty())
            Unsupported("native expression setting is unavailable: " + name);
        return value.GetValue<string>();
    };
    return {setting("TimeZone"), setting("Calendar")};
}
struct Session {
    ClientContext &caller;
    differential::Control &control;
    Connection conn;
    string default_catalog;
    vector<string> attachments;
    vector<std::shared_ptr<differential::Pin>> pins;
    std::shared_ptr<differential::Pin> stage;
    Execution &execution;
    explicit Session(ClientContext &caller, differential::Control &c, Execution &execution,
                     const std::pair<string, string> &expression_context)
        : caller(caller), control(c), conn(*caller.db), execution(execution) {
        default_catalog = DatabaseManager::GetDefaultDatabase(*conn.context).GetIdentifierName();
        // The caller's persistent in-memory catalog can contain macros that shadow
        // count_star, strlen, list_sort, etc. Generated reconciliation SQL must bind
        // built-ins. Temporary relations remain visible; stage USE selects a catalog
        // whose user functions/macros were explicitly rejected by Schema().
        Query("SET SESSION search_path='system.main'");
        // Comparisons must not inherit an implicit collation or caller search_path.
        Query("SET SESSION default_collation=''");
        Query("SET SESSION default_order='ASC'");
        // Parsed native definitions already encode / versus //. Do not reinterpret
        // serialized / operators if the host has enabled integer_division globally.
        Query("SET SESSION integer_division=false");
        for (auto &setting : {std::make_pair(string("TimeZone"), expression_context.first),
                              std::make_pair(string("Calendar"), expression_context.second)}) {
            Value current;
            if (setting.second.empty()) {
                if (conn.context->TryGetCurrentSetting(Identifier(setting.first), current))
                    Unsupported("native expression setting availability changed; use a new request_id: " + setting.first);
            } else Query("SET SESSION " + Q(setting.first) + "=" + Q(setting.second, '\''));
        }
        // DuckDB 2.0 makes this setting local to a connection. Carry the caller's
        // external-execution diagnostic into the worker so spill tests exercise the
        // actual comparison/application operators rather than only the outer scan.
        Value force_external;
        if (caller.TryGetCurrentSetting("debug_force_external", force_external))
            Query(string("SET SESSION debug_force_external=") + (force_external.GetValue<bool>() ? "true" : "false"));
    }
    ~Session() {
        execution.Update([](Progress &p) { p.phase = "cleanup"; });
        conn.Query("ROLLBACK");
        conn.Query("USE " + Q(default_catalog));
        for (auto it = attachments.rbegin(); it != attachments.rend(); ++it)
            conn.Query("DETACH " + Q(*it));
    }
    unique_ptr<QueryResult> Query(const string &sql) {
        execution.Check();
        auto result = conn.Submit(sql);
        result->ThrowIfError();
        result->Materialize();
        while (true) {
            try {
                execution.Check();
            } catch (...) {
                conn.Interrupt();
                throw;
            }
            auto state = result->ExecuteTask();
            if (IsTerminal(state))
                break;
            if (state == QueryResultState::BLOCKED || state == QueryResultState::NO_TASKS_AVAILABLE)
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        result->ThrowIfError();
        return result;
    }
    void Attach(const string &uri, const string &alias, bool read_only) {
        Query("ATTACH " + Q(uri, '\'') + " AS " + Q(alias) +
              (read_only ? " (READ_ONLY, HIDDEN true)" : " (HIDDEN true)"));
        attachments.push_back(alias);
    }
    std::map<string, string> views;
    ViewShapes view_shapes;
    Views view_info;
    vector<string> view_order;
    ViewShapes BindViews(const string &alias, const std::map<string, string> &definitions,
                         const string &failure = "view cannot bind after column additions") {
        ViewShapes shapes;
        for (auto &entry : definitions) {
            try {
                auto header = Query("SELECT * FROM " + Q(alias) + ".main." + Q(entry.first) + " LIMIT 0");
                shapes.emplace(entry.first, std::make_pair(header->GetNames(), header->GetTypes()));
            } catch (const Exception &error) {
                auto type = ErrorData(error).Type();
                if (!failure.empty() && (type == ExceptionType::BINDER || type == ExceptionType::CATALOG))
                    Unsupported(failure + ": " + entry.first);
                throw;
            }
        }
        return shapes;
    }
    string schema_object, schema_object_type;
    std::map<string, Table> Schema(const string &alias) {
        views.clear();
        view_shapes.clear();
        view_info.clear(); view_order.clear();
        vector<unique_ptr<CreateInfo>> objects;
        std::map<string, Table> result;
        idx_t column_count = 0, ddl_bytes = 0;
        Query("BEGIN");
        auto &catalog = Catalog::GetCatalog(*conn.context, Identifier(alias));
        auto transaction = catalog.GetCatalogTransaction(*conn.context);
        catalog.ScanSchemas(*conn.context, [&](SchemaCatalogEntry &schema) {
            schema_object = schema.name.GetIdentifierName(); schema_object_type = "schema";
            if (schema.name != "main" || schema.GetParentSchema())
                Unsupported("only the main schema is supported: " + schema.name);
            if (!schema.comment.IsNull() || !schema.tags.empty())
                Unsupported("schema comments/tags");
            for (auto kind :
                 {CatalogType::SEQUENCE_ENTRY,
                  CatalogType::TYPE_ENTRY, CatalogType::MACRO_ENTRY, CatalogType::TABLE_MACRO_ENTRY,
                  CatalogType::COLLATION_ENTRY, CatalogType::COORDINATE_SYSTEM_ENTRY,
                  CatalogType::COPY_FUNCTION_ENTRY, CatalogType::PRAGMA_FUNCTION_ENTRY,
                  CatalogType::SCALAR_FUNCTION_ENTRY, CatalogType::TABLE_FUNCTION_ENTRY,
                  CatalogType::AGGREGATE_FUNCTION_ENTRY, CatalogType::WINDOW_FUNCTION_ENTRY})
                schema.Scan(transaction, kind, [&](CatalogEntry &entry) {
                    if (entry.type == kind && !entry.internal) {
                        schema_object = schema.name.GetIdentifierName() + "." + entry.name.GetIdentifierName();
                        schema_object_type = StringUtil::Lower(CatalogTypeToString(kind));
                        Unsupported("catalog object " + CatalogTypeToString(kind) + " " + entry.name);
                    }
                });
            for (auto kind : {CatalogType::VIEW_ENTRY, CatalogType::INDEX_ENTRY})
                schema.Scan(transaction, kind, [&](CatalogEntry &entry) {
                    if (entry.type != kind || entry.internal) return;
                    schema_object = schema.name.GetIdentifierName() + "." + entry.name.GetIdentifierName();
                    schema_object_type = StringUtil::Lower(CatalogTypeToString(kind));
                    if (objects.size() >= 1024 || entry.name.GetIdentifierName().size() > 1024)
                        throw IOException("[differential:merge_limit] Too many views/indexes or oversized identifier");
                    auto info = entry.GetInfo();
                    if (!info->comment.IsNull() || !info->tags.empty()) Unsupported("catalog object comments/tags");
                    info->SetCatalog(Identifier());
                    objects.push_back(std::move(info));
                });
            schema.Scan(transaction, CatalogType::TABLE_ENTRY, [&](CatalogEntry &entry) {
                if (entry.type != CatalogType::TABLE_ENTRY)
                    return;
                schema_object = schema.name.GetIdentifierName() + "." + entry.name.GetIdentifierName();
                schema_object_type = "table";
                if (entry.internal)
                    Unsupported("internal table " + entry.name);
                auto &table = entry.Cast<TableCatalogEntry>();
                if (result.size() >= 1024 || table.name.GetIdentifierName().size() > 1024)
                    throw IOException("[differential:merge_limit] Catalog exceeds 1024 tables or identifier bytes");
                if (!table.IsDuckTable())
                    Unsupported("non-native table " + entry.name);
                table.ScanTriggers(transaction, [&](CatalogEntry &trigger) { Unsupported("trigger " + trigger.name); });
                auto info = table.GetInfo();
                auto &create = info->Cast<CreateTableInfo>();
                if (!create.comment.IsNull() || !create.tags.empty())
                    Unsupported("table comments/tags");
                create.SetCatalog(Identifier());
                Table t;
                t.schema = schema.name.GetIdentifierName();
                t.name = table.name.GetIdentifierName();
                for (auto &col : create.columns.Logical()) {
                    schema_object = t.schema + "." + t.name + "." + col.Name().GetIdentifierName(); schema_object_type = "column";
                    if (++column_count > 4096 || t.columns.size() >= 256 || col.Name().GetIdentifierName().size() > 1024)
                        throw IOException("[differential:merge_limit] Catalog exceeds 4096 columns, 256 columns per table, or 1024 identifier bytes");
                    if (col.Generated())
                        Unsupported("generated columns: " + t.name);
                    if (col.HasDefaultValue())
                        CheckExpression(*conn.context, col.DefaultValue(), false);
                    if (!col.Comment().IsNull() || !col.Tags().empty())
                        Unsupported("column comments/tags: " + t.name);
                    if (col.CompressionType() != CompressionType::COMPRESSION_AUTO)
                        Unsupported("explicit column compression: " + t.name);
                    auto type = col.Type();
                    CheckMergeType(type);
                    t.columns.push_back(col.Name().GetIdentifierName());
                    t.types.push_back(type);
                }
                schema_object = t.schema + "." + t.name; schema_object_type = "table";
                for (auto &constraint : create.constraints) {
                    if (constraint->type == ConstraintType::UNIQUE) {
                        auto &unique = constraint->Cast<UniqueConstraint>();
                        if (unique.IsPrimaryKey())
                            for (auto key : unique.GetLogicalIndexes(create.columns))
                                t.keys.push_back(key.index);
                    } else if (constraint->type == ConstraintType::CHECK)
                        CheckExpression(*conn.context, *constraint->Cast<CheckConstraint>().expression, true);
                    else if (constraint->type != ConstraintType::NOT_NULL)
                        Unsupported("only PRIMARY KEY, UNIQUE, NOT NULL and supported CHECK constraints are supported: " +
                                    t.name);
                }
                for (auto key : t.keys)
                    if (MergeNestedType(t.types[key])) Unsupported("nested primary keys");
                if (t.keys.empty())
                    Unsupported("declared primary key required: " + t.name);
                t.ddl = create.ToString();
                t.definition.reset(static_cast<CreateTableInfo *>(info.release()));
                ddl_bytes += t.ddl.size();
                if (ddl_bytes > 16777216)
                    throw IOException("[differential:merge_limit] Catalog definitions exceed 16 MiB");
                result.emplace(t.name, std::move(t));
            });
        });
        std::set<string> relations;
        for (auto &entry : result) relations.insert(StringUtil::Lower(entry.first));
        for (auto &object : objects)
            if (object->type == CatalogType::VIEW_ENTRY)
                relations.insert(StringUtil::Lower(object->Cast<CreateViewInfo>().GetViewName().GetIdentifierName()));
        for (auto &object : objects) {
            ddl_bytes += object->ToString().size();
            if (ddl_bytes > 16777216) throw IOException("[differential:merge_limit] Catalog definitions exceed 16 MiB");
            if (object->type == CatalogType::VIEW_ENTRY) {
                auto &v = object->Cast<CreateViewInfo>();
                schema_object = "main." + v.GetViewName().GetIdentifierName(); schema_object_type = "view";
                if (v.binding_mode != CreateViewBindingMode::BIND_ON_CREATE || v.types.empty())
                    Unsupported("unbound/deferred views must be bound before capturing an input snapshot");
                if (!v.column_comments_map.empty() || !v.query->node->cte_map.map.empty() ||
                    v.query->node->type != QueryNodeType::SELECT_NODE)
                    Unsupported("view comments, CTEs or non-SELECT view");
                View info;
                info.name = v.GetViewName().GetIdentifierName(); info.ddl = v.ToString();
                info.native_dependencies = NativeViewDependencies(v);
                for (auto &dep : info.native_dependencies)
                    info.dependencies.insert(dep.first.substr(dep.first.find(':') + 1));
                auto canonical = v.Copy();
                canonical->Cast<CreateViewInfo>().SetViewName(Identifier(StringUtil::Lower(info.name)));
                info.canonical = canonical->ToString();
                ParsedExpressionIterator::EnumerateQueryNodeChildren(*v.query->node,
                    [&](unique_ptr<ParsedExpression> &e) { CheckExpression(*conn.context, *e, true, true); },
                    [&](TableRef &ref) {
                        if (ref.type == TableReferenceType::BASE_TABLE) {
                            auto &b = ref.Cast<BaseTableRef>();
                            auto &q = b.GetQualifiedName();
                            if (!q.Catalog().empty() || (!q.Schema().empty() && q.Schema() != "main") ||
                                !relations.count(StringUtil::Lower(q.Name().GetIdentifierName())) || b.at_clause)
                                Unsupported("view must reference only local unqualified/main relations");
                            info.dependencies.insert(StringUtil::Lower(q.Name().GetIdentifierName()));
                        } else if (ref.type != TableReferenceType::JOIN && ref.type != TableReferenceType::EMPTY_FROM)
                            Unsupported("view table reference");
                    });
                views.emplace(v.GetViewName().GetIdentifierName(), v.ToString());
                view_info.emplace(StringUtil::Lower(info.name), std::move(info));
            } else {
                auto &index = object->Cast<CreateIndexInfo>();
                schema_object = "main." + index.GetIndexName().GetIdentifierName(); schema_object_type = "index";
                if (index.index_type != "ART" || !index.options.empty()) Unsupported("index type/options");
                for (auto &expr : index.parsed_expressions)
                    if (expr->GetExpressionClass() != ExpressionClass::COLUMN_REF)
                        Unsupported("expression index");
                auto table = result.find(index.table.GetIdentifierName());
                if (table == result.end()) Unsupported("index on unknown table");
                table->second.indexes.emplace(index.GetIndexName().GetIdentifierName(), index.ToString());
                table->second.index_definitions.emplace(index.GetIndexName().GetIdentifierName(),
                    std::shared_ptr<CreateIndexInfo>(static_cast<CreateIndexInfo *>(object.release())));
            }
        }
        Query("COMMIT");
        {
            std::set<string> tables;
            for (auto &entry : result) tables.insert(StringUtil::Lower(entry.first));
            string bad, reason;
            if (!ViewOrder(view_info, tables, view_order, bad, reason, [&]() { execution.Check(); }))
                Unsupported("invalid input view " + bad + ": " + reason);
        }
        view_shapes = BindViews(alias, views, "input view cannot bind");
        idx_t view_columns = 0;
        for (auto &entry : view_info) {
            entry.second.shape = view_shapes.at(entry.second.name);
            for (auto &type : entry.second.shape.second) CheckMergeType(type);
            view_columns += entry.second.shape.first.size();
            if (entry.second.shape.first.size() > 256 || view_columns > 4096)
                throw IOException("[differential:merge_limit] Catalog exceeds 256 output columns per view or 4096 total view output columns");
        }
        return result;
    }
};
// Compare catalog definitions before touching row data. Additions never change
// existing columns/constraints/indexes: reconstruct the only allowed side DDL
// from the base plus plain nullable columns and compare it with the actual DDL.
std::map<string, Table> PlanSchema(const vector<std::map<string, Table>> &inputs, bool additive) {
    auto result = inputs[0];
    idx_t total_columns = 0, ddl_bytes = 0;
    for (idx_t side = 1; side < 3; side++)
        if (inputs[side].size() != result.size()) Unsupported("schema mismatch: table additions/deletions are unsupported");
    for (auto &entry : result) {
        auto &target = entry.second;
        const auto &base = inputs[0].at(entry.first);
        // The map key uses DuckDB's case folding; the spelling comes from ours
        // when both sides add the same name. Added columns have a deterministic order.
        std::map<string, std::pair<const ColumnDefinition *, const ColumnDefinition *>> additions;
        for (idx_t side = 0; side < 3; side++) {
            auto found = inputs[side].find(entry.first);
            if (found == inputs[side].end()) Unsupported("schema mismatch: table rename/addition/deletion: " + entry.first);
            auto &table = found->second;
            std::set<string> names;
            for (auto &name : table.columns) names.insert(StringUtil::Lower(name));
            target.input_columns.push_back(std::move(names));
            if (!side) continue;
            if (table.indexes != base.indexes) Unsupported("schema mismatch: index definitions changed: " + entry.first);
            if (!additive) {
                if (table.ddl != base.ddl) Unsupported("schema mismatch: " + entry.first);
                continue;
            }
            if (table.columns.size() < base.columns.size())
                Unsupported("schema mismatch: column deletion/rename is unsupported: " + entry.first);
            auto expected = base.definition->Copy();
            auto &create = expected->Cast<CreateTableInfo>();
            create.columns = ColumnList();
            std::map<string, idx_t> base_columns, side_columns;
            for (idx_t column = 0; column < base.columns.size(); column++)
                base_columns.emplace(StringUtil::Lower(base.columns[column]), column);
            for (idx_t column = 0; column < table.columns.size(); column++) {
                auto &definition = table.definition->columns.GetColumn(LogicalIndex(column));
                auto name = StringUtil::Lower(table.columns[column]);
                side_columns.emplace(name, column);
                auto original = base_columns.find(name);
                if (original != base_columns.end()) {
                    auto copy = base.definition->columns.GetColumn(LogicalIndex(original->second)).Copy();
                    copy.SetName(definition.Name()); // DuckDB identifiers are case-insensitive.
                    create.columns.AddColumn(std::move(copy));
                    continue;
                }
                if (definition.HasDefaultValue())
                    Unsupported("additive columns cannot have defaults: " + entry.first + "." + table.columns[column]);
                create.columns.AddColumn(ColumnDefinition(definition.Name(), definition.Type()));
                auto &pair = additions[name];
                (side == 1 ? pair.first : pair.second) = &definition;
            }
            for (auto &original : base_columns)
                if (!side_columns.count(original.first))
                    Unsupported("schema mismatch: base column deletion/rename is unsupported: " + entry.first);
            // Prior merges can reorder additions relative to either parent. Compare
            // definitions by name, remapping native positional constraints to the
            // side's order. The result retains base order, then sorted additions.
            auto remap = [&](LogicalIndex index) {
                return LogicalIndex(side_columns.at(StringUtil::Lower(base.columns[index.index])));
            };
            for (auto &constraint : create.constraints) {
                if (constraint->type == ConstraintType::NOT_NULL) {
                    auto &nn = constraint->Cast<NotNullConstraint>();
                    nn.index = remap(nn.index);
                } else if (constraint->type == ConstraintType::UNIQUE) {
                    auto &unique = constraint->Cast<UniqueConstraint>();
                    if (unique.HasIndex()) unique.SetIndex(remap(unique.GetIndex()));
                }
            }
            if (create.ToString() != table.ddl)
                Unsupported("schema mismatch: additive mode requires unchanged base columns/constraints/options "
                            "and nullable non-key additions: " + entry.first);
        }
        auto merged = base.definition->Copy();
        auto &create = merged->Cast<CreateTableInfo>();
        for (auto &addition : additions) {
            auto ours = addition.second.first, theirs = addition.second.second;
            auto column = ours ? ours : theirs;
            auto name = column->Name().GetIdentifierName();
            if (ours && theirs && ours->Type() != theirs->Type()) {
                target.schema_conflicts.push_back(Value::STRUCT(SchemaConflictType(),
                    {Value(name), Value("column_definition"), Value(LogicalType::VARCHAR),
                     Value(ours->ToSQLString()), Value(theirs->ToSQLString()), Value(target.name), Value(LogicalType::VARCHAR)}));
            } else {
                target.schema_changes.push_back(Value::STRUCT(SchemaChangeType(),
                    {Value(name), Value(ours && theirs ? "both" : ours ? "ours" : "theirs"), Value(column->Type().ToString()), Value("add")}));
            }
            create.columns.AddColumn(column->Copy());
            target.columns.push_back(name);
            target.types.push_back(column->Type());
        }
        target.ddl = create.ToString();
        target.definition.reset(static_cast<CreateTableInfo *>(merged.release()));
        target.rebuild_schema = target.ddl != inputs[1].at(entry.first).ddl;
        total_columns += target.columns.size();
        ddl_bytes += target.ddl.size();
        for (auto &index : target.indexes) ddl_bytes += index.second.size();
        if (total_columns > 4096 || target.columns.size() > 256 || ddl_bytes > 16777216)
            throw IOException("[differential:merge_limit] Combined schema exceeds catalog column/definition limits");
    }
    return result;
}
// Canonical definitions for independently created same-name tables. Normalize
// table/index identifier spelling only; columns, order, constraints, defaults and
// index expressions must agree exactly. Never infer renames or reconcile DDL here.
string AddedTableDefinition(const Table &table, bool canonical = false) {
    string definition = table.ddl;
    if (canonical) {
        auto copy = table.definition->Copy();
        copy->Cast<CreateTableInfo>().SetTableName(Identifier(StringUtil::Lower(table.name)));
        definition = copy->ToString();
    }
    std::map<string, string> indexes;
    for (auto &index : table.indexes) {
        auto ddl = index.second;
        if (canonical) {
            auto copy = table.index_definitions.at(index.first)->Copy();
            auto &create = copy->Cast<CreateIndexInfo>();
            create.table = Identifier(StringUtil::Lower(table.name));
            create.SetIndexName(Identifier(StringUtil::Lower(index.first)));
            ddl = create.ToString();
        }
        indexes.emplace(StringUtil::Lower(index.first), std::move(ddl));
    }
    for (auto &index : indexes) definition += "\n" + index.second;
    return definition;
}
std::map<string, Table> PlanTables(const vector<std::map<string, Table>> &inputs,
                                  const vector<std::map<string, string>> &views, bool &name_conflicts, bool check_names = true) {
    using Tables = std::map<string, Table>;
    vector<std::map<string, const Table *>> by_name(3);
    for (idx_t side = 0; side < 3; side++)
        for (auto &entry : inputs[side]) by_name[side].emplace(StringUtil::Lower(entry.first), &entry.second);
    // Preserve the existing column planner for tables that existed in the base.
    vector<Tables> existing(3);
    existing[0] = inputs[0];
    for (idx_t side = 1; side < 3; side++)
        for (auto &entry : inputs[0]) {
            auto found = by_name[side].find(StringUtil::Lower(entry.first));
            if (found != by_name[side].end()) existing[side].emplace(entry.first, *found->second);
        }
    auto result = PlanSchema(existing, true); // Still rejects table deletions/renames.
    std::map<string, string> added; // Folded name -> chosen output spelling.
    for (idx_t side = 1; side < 3; side++)
        for (auto &entry : by_name[side])
            if (!by_name[0].count(entry.first)) added.emplace(entry.first, entry.second->name);
    if (result.size() + added.size() > 1024)
        throw IOException("[differential:merge_limit] Combined schema exceeds 1024 tables");
    for (auto &entry : added) {
        auto find = [&](idx_t side) -> const Table * {
            auto found = by_name[side].find(entry.first);
            return found == by_name[side].end() ? nullptr : found->second;
        };
        auto ours = find(1), theirs = find(2);
        Table table = *(ours ? ours : theirs);
        table.input_present = {{false, ours != nullptr, theirs != nullptr}};
        table.rebuild_schema = true; // Includes empty additions and absent staging tables.
        table.table_change_source = ours && theirs ? "both" : ours ? "ours" : "theirs";
        for (idx_t side = 0; side < 3; side++) {
            std::set<string> columns;
            auto input = find(side);
            if (input)
                for (auto &column : input->columns) columns.insert(StringUtil::Lower(column));
            table.input_columns.push_back(std::move(columns));
        }
        if (ours && theirs && AddedTableDefinition(*ours, true) != AddedTableDefinition(*theirs, true))
            table.schema_conflicts.push_back(Value::STRUCT(SchemaConflictType(),
                {Value(LogicalType::VARCHAR), Value("table_definition"), Value(LogicalType::VARCHAR),
                 Value(AddedTableDefinition(*ours)), Value(AddedTableDefinition(*theirs)), Value(table.name), Value(LogicalType::VARCHAR)}));
        result.emplace(table.name, std::move(table));
    }
    // Native indexes have a separate namespace, but this additive planner refuses
    // new cross-kind ambiguity as well as actual relation/index collisions. Existing
    // unchanged cross-kind names remain supported. Report each conflicting name once.
    struct NameUse {
        string owner;
        bool added = false;
        std::array<string, 3> definitions;
    };
    std::map<string, std::map<string, NameUse>> names;
    auto use = [&](idx_t side, const string &name, const string &kind, const string &owner, const string &ddl) {
        auto folded = StringUtil::Lower(name), table = StringUtil::Lower(owner);
        auto &object = names[folded][kind + ":" + table];
        object.owner = table;
        object.added = !owner.empty() && added.count(table);
        object.definitions[side] = ddl;
    };
    for (idx_t side = 0; side < 3; side++) {
        for (auto &entry : inputs[side]) {
            auto &table = entry.second;
            use(side, table.name, "table", table.name, table.ddl);
            for (auto &index : table.indexes) use(side, index.first, "index", table.name, index.second);
        }
        for (auto &view : views[side]) use(side, view.first, "view", "", view.second);
    }
    if (check_names) for (auto &entry : names) {
        if (entry.second.size() < 2) continue;
        string owner;
        vector<string> definitions[3];
        for (auto &object : entry.second) {
            if (owner.empty() && object.second.added) owner = object.second.owner;
            for (idx_t side = 0; side < 3; side++)
                if (!object.second.definitions[side].empty()) definitions[side].push_back(object.second.definitions[side]);
        }
        if (owner.empty()) continue;
        auto text = [&](idx_t side) { return definitions[side].empty() ? Value(LogicalType::VARCHAR) : Value(Join(definitions[side], "\n")); };
        result.at(added.at(owner)).schema_conflicts.push_back(Value::STRUCT(SchemaConflictType(),
            {Value(LogicalType::VARCHAR), Value("object_name_collision"), text(0), text(1), text(2), Value(entry.first), Value(LogicalType::VARCHAR)}));
        name_conflicts = true;
    }
    idx_t columns = 0, objects = views[0].size(), bytes = 0;
    for (auto &view : views[0]) bytes += view.second.size();
    for (auto &entry : result) {
        auto &table = entry.second;
        columns += table.columns.size(); objects += table.indexes.size(); bytes += table.ddl.size();
        for (auto &index : table.indexes) bytes += index.second.size();
    }
    if (columns > 4096 || objects > 1024 || bytes > 16777216)
        throw IOException("[differential:merge_limit] Combined schema exceeds catalog column/object/definition limits");
    return result;
}
bool ReferencesColumn(const ParsedExpression &expression, const string &name) {
    if (expression.GetExpressionClass() == ExpressionClass::COLUMN_REF &&
        StringUtil::Lower(expression.Cast<ColumnRefExpression>().GetColumnName().GetIdentifierName()) == name)
        return true;
    bool found = false;
    ParsedExpressionIterator::EnumerateChildren(expression, [&](const ParsedExpression &child) {
        if (ReferencesColumn(child, name)) found = true;
    });
    return found;
}
// This first removal scope accepts only plain nullable columns. Never erase a
// dependent constraint/index as a side effect of projecting a native definition.
string ColumnRemovalDependency(const Table &table, idx_t column) {
    if (table.definition->columns.GetColumn(LogicalIndex(column)).HasDefaultValue()) return "default";
    auto name = StringUtil::Lower(table.columns[column]);
    for (auto &constraint : table.definition->constraints) {
        if (constraint->type == ConstraintType::NOT_NULL && constraint->Cast<NotNullConstraint>().index.index == column)
            return "NOT NULL constraint";
        if (constraint->type == ConstraintType::UNIQUE) {
            auto &unique = constraint->Cast<UniqueConstraint>();
            for (auto index : unique.GetLogicalIndexes(table.definition->columns))
                if (index.index == column) return unique.IsPrimaryKey() ? "PRIMARY KEY constraint" : "UNIQUE constraint";
        }
        if (constraint->type == ConstraintType::CHECK &&
            ReferencesColumn(*constraint->Cast<CheckConstraint>().expression, name)) return "CHECK constraint";
    }
    for (auto &index : table.index_definitions)
        for (auto &expression : index.second->parsed_expressions)
            if (ReferencesColumn(*expression, name)) return "index " + index.first;
    return "";
}
Table ProjectColumns(const Table &table, const std::set<string> &removed) {
    Table result = table;
    auto copy = table.definition->Copy();
    auto &create = copy->Cast<CreateTableInfo>();
    create.columns = ColumnList();
    result.columns.clear(); result.types.clear(); result.keys.clear();
    std::map<string, idx_t> positions;
    for (auto &column : table.definition->columns.Logical()) {
        auto name = StringUtil::Lower(column.Name().GetIdentifierName());
        if (removed.count(name)) continue;
        positions.emplace(name, result.columns.size());
        result.columns.push_back(column.Name().GetIdentifierName()); result.types.push_back(column.Type());
        create.columns.AddColumn(column.Copy());
    }
    auto remap = [&](LogicalIndex index) {
        return LogicalIndex(positions.at(StringUtil::Lower(table.columns.at(index.index))));
    };
    for (auto &constraint : create.constraints) {
        if (constraint->type == ConstraintType::NOT_NULL) {
            auto &nn = constraint->Cast<NotNullConstraint>(); nn.index = remap(nn.index);
        } else if (constraint->type == ConstraintType::UNIQUE) {
            auto &unique = constraint->Cast<UniqueConstraint>();
            if (unique.HasIndex()) unique.SetIndex(remap(unique.GetIndex()));
            if (unique.IsPrimaryKey())
                for (auto index : unique.GetLogicalIndexes(create.columns)) result.keys.push_back(index.index);
        }
    }
    result.ddl = create.ToString();
    result.definition.reset(static_cast<CreateTableInfo *>(copy.release()));
    return result;
}
// Catalog mode projects allowed column removals before additive planning. The
// original native inputs remain intact; proofs later compare their typed values.
// A missing base table is a catalog deletion, never an empty row relation.
std::map<string, Table> PlanCatalogTables(const vector<std::map<string, Table>> &inputs) {
    vector<std::map<string, Table>> retained = inputs;
    std::map<string, Table> removed;
    for (auto &entry : inputs[0]) {
        std::array<const Table *, 3> sides {{&entry.second, nullptr, nullptr}};
        for (idx_t side = 1; side < 3; side++)
            for (auto &candidate : inputs[side])
                if (StringUtil::Lower(candidate.first) == StringUtil::Lower(entry.first)) sides[side] = &candidate.second;
        if (sides[1] && sides[2]) continue;
        Table table = entry.second;
        bool changed_definition = false;
        table.drop_table = true;
        table.table_change_source = !sides[1] && !sides[2] ? "both" : !sides[1] ? "ours" : "theirs";
        for (idx_t side = 0; side < 3; side++) {
            table.input_present[side] = sides[side] != nullptr;
            if (!sides[side]) continue;
            retained[side].erase(sides[side]->name);
            table.input_definitions[side] = AddedTableDefinition(*sides[side]);
            if (side && AddedTableDefinition(*sides[side], true) != AddedTableDefinition(entry.second, true))
                changed_definition = true;
        }
        // Fill both side definitions before emitting the conflict envelope.
        if (changed_definition) {
            table.schema_conflicts.push_back(CatalogConflict(table.name, "table_delete_modify", table.input_definitions,
                "Table removal conflicts with a changed surviving definition"));
        }
        removed.emplace(entry.first, std::move(table));
    }
    std::map<string, Table> column_plans;
    vector<string> names;
    for (auto &entry : retained[0]) names.push_back(entry.first);
    for (auto &name : names) {
        std::array<Table *, 3> sides {{nullptr, nullptr, nullptr}};
        vector<std::map<string, idx_t>> columns(3);
        for (idx_t side = 0; side < 3; side++) {
            for (auto &entry : retained[side])
                if (StringUtil::Lower(entry.first) == StringUtil::Lower(name)) sides[side] = &entry.second;
            if (!sides[side]) throw InternalException("Missing retained merge table");
            for (idx_t c = 0; c < sides[side]->columns.size(); c++) columns[side].emplace(StringUtil::Lower(sides[side]->columns[c]), c);
        }
        auto &base = *sides[0];
        Table plan = base;
        std::set<string> dropped;
        for (idx_t c = 0; c < base.columns.size(); c++) {
            auto folded = StringUtil::Lower(base.columns[c]);
            if (columns[1].count(folded) && columns[2].count(folded)) continue;
            auto dependency = ColumnRemovalDependency(base, c);
            if (!dependency.empty()) Unsupported("column removal requires a plain nullable non-key column without defaults or dependent constraints/indexes: " +
                                                 base.name + "." + base.columns[c] + " (" + dependency + ")");
            dropped.insert(folded);
            ColumnRemoval removal;
            removal.name = base.columns[c]; removal.type = base.types[c];
            auto &original = base.definition->columns.GetColumn(LogicalIndex(c));
            string conflict;
            for (idx_t side = 0; side < 3; side++) {
                auto found = columns[side].find(folded);
                removal.input_present[side] = found != columns[side].end();
                if (!removal.input_present[side]) continue;
                auto &column = sides[side]->definition->columns.GetColumn(LogicalIndex(found->second));
                removal.input_definitions[side] = column.ToSQLString();
                if (!side) continue;
                auto normalized = column.Copy(); normalized.SetName(original.Name());
                auto dependent = ColumnRemovalDependency(*sides[side], found->second);
                if (!dependent.empty()) conflict = "Column removal conflicts with a surviving " + dependent;
                else if (normalized.ToSQLString() != original.ToSQLString())
                    conflict = "Column removal conflicts with a changed surviving definition";
            }
            if (!conflict.empty())
                plan.schema_conflicts.push_back(CatalogConflict(base.name, "column_delete_modify", removal.input_definitions, conflict, removal.name));
            auto source = !removal.input_present[1] && !removal.input_present[2] ? "both" : !removal.input_present[1] ? "ours" : "theirs";
            plan.schema_changes.push_back(Value::STRUCT(SchemaChangeType(),
                {Value(removal.name), Value(source), Value(removal.type.ToString()), Value("drop")}));
            plan.removed_columns.push_back(std::move(removal));
        }
        if (dropped.empty()) continue;
        // Keep the original ours DDL: comparing only the projected schemas would
        // miss an incoming schema-only removal and incorrectly skip rebuilding.
        plan.input_definitions[1] = sides[1]->ddl;
        if (plan.schema_conflicts.empty()) {
            for (auto side : sides) *side = ProjectColumns(*side, dropped);
        } else {
            // A blocked table needs no executable projected definition. All inputs
            // were validated already; preserve the original metadata for preview.
            for (idx_t side = 0; side < 3; side++) { auto key = sides[side]->name; retained[side].erase(key); }
        }
        column_plans.emplace(name, std::move(plan));
    }
    bool ignored = false;
    auto result = PlanTables(retained, vector<std::map<string, string>>(3), ignored, false);
    for (auto &entry : column_plans) {
        auto &plan = entry.second;
        if (!plan.schema_conflicts.empty()) { result.emplace(entry.first, std::move(plan)); continue; }
        auto &table = result.at(entry.first);
        table.removed_columns = std::move(plan.removed_columns);
        table.schema_changes.insert(table.schema_changes.begin(), plan.schema_changes.begin(), plan.schema_changes.end());
        table.rebuild_schema = table.ddl != plan.input_definitions[1];
    }
    for (auto &entry : removed) result.emplace(entry.first, std::move(entry.second));
    return result;
}
Views PlanViews(const vector<Views> &inputs) {
    std::set<string> names;
    for (auto &input : inputs) for (auto &entry : input) names.insert(entry.first);
    Views result;
    for (auto &name : names) {
        std::array<const View *, 3> sides {{nullptr, nullptr, nullptr}};
        for (idx_t side = 0; side < 3; side++) {
            auto found = inputs[side].find(name);
            if (found != inputs[side].end()) sides[side] = &found->second;
        }
        auto equal = [](const View *a, const View *b) {
            return !a || !b ? a == b : a->canonical == b->canonical && a->shape == b->shape && a->native_dependencies == b->native_dependencies;
        };
        auto base = sides[0], ours = sides[1], theirs = sides[2];
        const View *selected = nullptr;
        string source;
        bool conflict = false;
        if (equal(ours, theirs)) { selected = ours; if (!equal(base, ours)) source = "both"; }
        else if (equal(ours, base)) { selected = theirs; source = "theirs"; }
        else if (equal(theirs, base)) { selected = ours; source = "ours"; }
        else { conflict = true; selected = ours ? ours : theirs; source = "both"; }
        View target = *(selected ? selected : base);
        target.drop = !selected;
        target.source = source;
        target.action = conflict ? "conflict" : source.empty() ? "" : !base ? "add" : !selected ? "drop" : "replace";
        for (idx_t side = 0; side < 3; side++)
            if (sides[side]) {
                target.input_definitions[side] = sides[side]->ddl;
                target.input_shapes[side] = sides[side]->shape;
                target.input_tracks[side] = !sides[side]->native_dependencies.empty();
            }
        if (conflict)
            target.schema_conflicts.push_back(CatalogConflict(target.name,
                !ours || !theirs ? "view_delete_modify" : "view_definition", target.input_definitions,
                "Both sides changed the view differently (definition, bound output or native dependencies)"));
        result.emplace(name, std::move(target));
    }
    return result;
}
idx_t CatalogConflicts(const std::map<string, Table> &tables, const Views &views) {
    idx_t count = 0;
    for (auto &entry : tables) count += entry.second.schema_conflicts.size();
    for (auto &entry : views) count += entry.second.schema_conflicts.size();
    return count;
}
void CheckCatalogNames(std::map<string, Table> &tables, Views &views,
                       const vector<std::map<string, Table>> &inputs, const vector<Views> &input_views) {
    auto &base_tables = inputs[0];
    auto &base_views = input_views[0];
    auto definitions = [&](const string &name) {
        std::array<string, 3> result;
        for (idx_t side = 0; side < 3; side++) {
            vector<string> ddl;
            for (auto &entry : inputs[side]) {
                if (StringUtil::Lower(entry.first) == name) ddl.push_back(entry.second.ddl);
                for (auto &index : entry.second.indexes)
                    if (StringUtil::Lower(index.first) == name) ddl.push_back(index.second);
            }
            auto view = input_views[side].find(name);
            if (view != input_views[side].end()) ddl.push_back(view->second.ddl);
            result[side] = Join(ddl, "\n");
        }
        return result;
    };
    using Names = std::map<string, std::map<string, string>>;
    auto collect = [](const std::map<string, Table> &tables, const Views &views) {
        Names names;
        for (auto &entry : tables) {
            auto &t = entry.second;
            if (t.drop_table) continue;
            auto owner = StringUtil::Lower(t.name);
            names[owner]["table:" + owner] = entry.first;
            for (auto &index : t.indexes) names[StringUtil::Lower(index.first)]["index:" + owner] = entry.first;
        }
        for (auto &entry : views)
            if (!entry.second.drop) names[entry.first]["view:" + entry.first] = entry.first;
        return names;
    };
    auto before = collect(base_tables, base_views), after = collect(tables, views);
    for (auto &entry : after) {
        if (entry.second.size() < 2 || entry.second == before[entry.first]) continue;
        auto first = entry.second.begin();
        auto view = views.find(entry.first);
        if (view != views.end() && !view->second.drop)
            view->second.schema_conflicts.push_back(CatalogConflict(view->second.name, "object_name_collision",
                definitions(entry.first), "Selected view name collides with a table or index: " + entry.first));
        else {
            auto &table = tables.at(first->second);
            table.schema_conflicts.push_back(CatalogConflict(entry.first, "object_name_collision", definitions(entry.first),
                "Selected table/index names collide at " + entry.first));
        }
    }
    for (auto &entry : base_tables) {
        auto view = views.find(StringUtil::Lower(entry.first));
        if (view != views.end() && !view->second.drop)
            view->second.schema_conflicts.push_back(CatalogConflict(view->second.name, "object_kind", definitions(StringUtil::Lower(entry.first)),
                "Replacing a table with a view is unsupported"));
    }
    for (auto &entry : tables)
        if (!entry.second.drop_table && base_views.count(StringUtil::Lower(entry.first)))
            entry.second.schema_conflicts.push_back(CatalogConflict(entry.first, "object_kind", definitions(StringUtil::Lower(entry.first)),
                "Replacing a view with a table is unsupported"));
    idx_t table_count = 0, columns = 0, objects = 0, bytes = 0, view_columns = 0;
    for (auto &entry : tables) {
        auto &t = entry.second;
        if (t.drop_table) continue;
        table_count++; columns += t.columns.size(); objects += t.indexes.size(); bytes += t.ddl.size();
        for (auto &index : t.indexes) bytes += index.second.size();
    }
    for (auto &entry : views) if (!entry.second.drop) {
        objects++; bytes += entry.second.ddl.size(); view_columns += entry.second.shape.first.size();
    }
    if (table_count > 1024 || columns > 4096 || objects > 1024 || bytes > 16777216 || view_columns > 4096)
        throw IOException("[differential:merge_limit] Combined schema exceeds catalog limits");
}
void CheckTableRemovals(Session &s, std::map<string, Table> &tables, const vector<string> &aliases) {
    for (auto &entry : tables) {
        auto &t = entry.second;
        if (!t.drop_table || (!t.input_present[1] && !t.input_present[2])) continue;
        auto side = t.input_present[1] ? 1 : 2;
        s.execution.Phase("table_drop_check", t.name);
        vector<string> keys, changed;
        for (auto k : t.keys) keys.push_back("b." + Q(t.columns[k]) + "=s." + Q(t.columns[k]));
        changed.push_back("b." + Q(t.columns[t.keys[0]]) + " IS NULL");
        changed.push_back("s." + Q(t.columns[t.keys[0]]) + " IS NULL");
        for (auto &column : t.columns)
            changed.push_back("b." + Q(column) + " IS DISTINCT FROM s." + Q(column));
        auto different = s.Query("SELECT EXISTS(SELECT 1 FROM " + t.Qualified(aliases[0]) + " b FULL OUTER JOIN " +
            t.Qualified(aliases[side]) + " s ON " + Join(keys, " AND ") + " WHERE " + Join(changed, " OR ") + ")")
            ->Collection().GetValue(0, 0).GetValue<bool>();
        if (different)
            t.schema_conflicts.push_back(CatalogConflict(t.name, "table_delete_modify", t.input_definitions,
                "Table removal conflicts with changed surviving rows (insert, update or delete)"));
    }
}
void CheckColumnRemovals(Session &s, std::map<string, Table> &tables, const vector<string> &aliases) {
    for (auto &entry : tables) {
        auto &t = entry.second;
        if (t.removed_columns.empty()) continue;
        s.execution.Phase("column_drop_check", t.name);
        vector<string> keys;
        for (auto k : t.keys) keys.push_back("b." + Q(t.columns[k]) + "=s." + Q(t.columns[k]));
        auto missing = "b." + Q(t.columns[t.keys[0]]) + " IS NULL";
        for (idx_t side = 1; side < 3; side++) {
            vector<const ColumnRemoval *> columns;
            vector<string> counts {"count(*) FILTER(WHERE " + missing + ")"};
            for (auto &column : t.removed_columns) if (column.input_present[side]) {
                columns.push_back(&column);
                counts.push_back("count(*) FILTER(WHERE NOT (" + missing + ") AND b." + Q(column.name) +
                                 " IS DISTINCT FROM s." + Q(column.name) + ")");
            }
            if (columns.empty()) continue;
            // One bounded aggregate per table/retaining side. Deleted rows are
            // handled by ordinary row-existence reconciliation; new keys conflict
            // even when their removed-column values are NULL.
            auto result = s.Query("SELECT " + Join(counts) + " FROM " + t.Qualified(aliases[side]) +
                                  " s LEFT JOIN " + t.Qualified(aliases[0]) + " b ON " + Join(keys, " AND "));
            auto inserted = result->Collection().GetValue(0, 0).GetValue<int64_t>();
            for (idx_t c = 0; c < columns.size(); c++) {
                auto changed = result->Collection().GetValue(c + 1, 0).GetValue<int64_t>();
                if (!inserted && !changed) continue;
                auto &column = *columns[c];
                t.schema_conflicts.push_back(CatalogConflict(t.name, "column_delete_modify", column.input_definitions,
                    "Column removal conflicts with " + to_string(changed) + " changed existing values and " +
                    to_string(inserted) + " newly inserted rows on " + (side == 1 ? "ours" : "theirs"), column.name));
            }
        }
        s.execution.Phase("column_drops_checked", t.name);
    }
}
void CreatePlannedView(Session &s, const View &view) {
    // Preserve native dependency registration per view, independent of the caller's
    // global setting. This changes only the private worker connection.
    s.Query(string("SET SESSION enable_view_dependencies=") + (view.native_dependencies.empty() ? "false" : "true"));
    s.Query(view.ddl);
}
std::map<string, uint8_t> ReadViewDependencies(Session &s, const string &alias, const string &name) {
    s.Query("BEGIN");
    auto &entry = Catalog::GetEntry<ViewCatalogEntry>(*s.conn.context,
        QualifiedName(Identifier(alias), Identifier("main"), Identifier(name)));
    auto info = entry.GetInfo();
    auto result = NativeViewDependencies(info->Cast<CreateViewInfo>());
    s.Query("COMMIT");
    return result;
}
// Bind the selected definitions against empty native tables, before reading row changes.
// Preserve the selected input's view output contract, including wildcard output.
void ValidateCatalogViews(Session &s, const std::map<string, Table> &tables, Views &views, vector<string> &order) {
    std::set<string> names;
    for (auto &entry : tables) if (!entry.second.drop_table) names.insert(StringUtil::Lower(entry.first));
    string bad, reason;
    if (!ViewOrder(views, names, order, bad, reason, [&]() { s.execution.Check(); })) {
        auto &v = views.at(bad);
        v.schema_conflicts.push_back(CatalogConflict(v.name, "view_dependency", v.input_definitions, reason));
        return;
    }
    if (order.empty()) return;
    auto alias = "_merge_catalog_" + s.pins[0]->id;
    s.Attach(":memory:", alias, false);
    s.Query("USE " + Q(alias));
    for (auto &entry : tables) if (!entry.second.drop_table) s.Query(entry.second.ddl);
    for (auto &name : order) {
        auto &v = views.at(name);
        try {
            CreatePlannedView(s, v);
            if (ReadViewDependencies(s, alias, v.name) != v.native_dependencies) {
                v.schema_conflicts.push_back(CatalogConflict(v.name, "view_dependency", v.input_definitions,
                    "Combined catalog changes the selected view's native dependency registrations"));
                break;
            }
            auto actual = s.Query("SELECT * FROM " + Q(alias) + ".main." + Q(v.name) + " LIMIT 0");
            if (std::make_pair(actual->GetNames(), actual->GetTypes()) != v.shape) {
                v.schema_conflicts.push_back(CatalogConflict(v.name, "view_output", v.input_definitions,
                    "Combined catalog changes the selected view's output names/types; recreate it explicitly in an input"));
                break;
            }
        } catch (const Exception &error) {
            auto data = ErrorData(error);
            if (data.Type() != ExceptionType::BINDER && data.Type() != ExceptionType::CATALOG) throw;
            v.schema_conflicts.push_back(CatalogConflict(v.name, "view_binding", v.input_definitions, data.RawMessage()));
            break;
        }
    }
    s.Query("USE " + Q(s.default_catalog));
    s.Query("SET SESSION search_path='system.main'");
    s.Query("DETACH " + Q(alias)); s.attachments.pop_back();
}
[[noreturn]] void BadResolution(const string &message) {
    throw InvalidInputException("[differential:merge_resolution] %s", message);
}
string EncodeResolutions(const Value &value) {
    // Fixed capacity makes the persisted envelope's limit independent of allocator growth.
    vector<uint8_t> buffer(8388608);
    MemoryStream stream(buffer.data(), buffer.size());
    try {
        BinarySerializer::Serialize(value, stream);
    } catch (const SerializationException &) {
        throw IOException("[differential:merge_limit] Serialized resolution data exceeds 8 MiB");
    }
    return string(reinterpret_cast<const char *>(buffer.data()), stream.GetPosition());
}
Value DecodeResolutions(const string &bytes) {
    if (bytes.empty()) return Value();
    if (bytes.size() > 8388608) BadResolution("oversized persisted resolution envelope");
    MemoryStream stream(reinterpret_cast<data_ptr_t>(const_cast<char *>(bytes.data())), bytes.size());
    BinaryDeserializer decoder(stream);
    decoder.Begin();
    auto result = Value::Deserialize(decoder);
    decoder.End();
    if (stream.GetPosition() != bytes.size()) BadResolution("invalid persisted resolution envelope");
    return result;
}
string ResolutionSpecification(const MergeOptions &options) {
    string spec;
    for (auto &entry : options.resolution_tables)
        for (auto &text : {entry.first, entry.second})
            spec += to_string(text.size()) + ":" + text;
    if (spec.size() > 65536 || options.resolution_tables.size() > 1024)
        BadResolution("resolution_tables exceeds 64 KiB or 1024 tables");
    return spec;
}
Value CaptureResolutions(Session &s, const std::map<string, Table> &schema, const MergeOptions &options,
                         int64_t base, int64_t ours, int64_t theirs) {
    child_list_t<Value> tables;
    int64_t rows = 0, bytes = 0;
    std::set<string> targets;
    s.Query("BEGIN");
    for (auto &mapping : options.resolution_tables) {
        auto target = QualifiedName::Parse(mapping.first);
        auto found = schema.find(target.Name().GetIdentifierName());
        if (!target.Catalog().empty() || target.Schema() != "main" || found == schema.end() || found->second.drop_table)
            BadResolution("target must name a merged table as main.table");
        auto &t = found->second;
        if (!targets.insert(StringUtil::Lower(t.name)).second) BadResolution("duplicate target table");
        auto source = QualifiedName::Parse(mapping.second);
        if (source.Catalog().empty() || source.Schema().empty() || source.Catalog() == "temp")
            BadResolution("resolution source must be a committed table named catalog.schema.table");
        auto path = Catalog::GetCatalog(*s.conn.context, source.Catalog()).GetDBPath();
        if (path.rfind("diff://", 0) == 0) {
            auto resolved = s.control.Resolve(path);
            auto database = resolved.historical ? resolved.snapshot.database : resolved.branch.database;
            if (resolved.remote || database.empty() || s.control.Profile(database) != "local" ||
                s.control.Writers(database) != "single")
                Unsupported("resolution sources must use local single-writer storage");
        } else if (FileSystem::IsRemoteFile(path))
            Unsupported("resolution sources must use local storage");
        auto &entry = Catalog::GetEntry<TableCatalogEntry>(*s.conn.context, source);
        if (!entry.IsDuckTable() || entry.temporary) BadResolution("resolution source must be a native persistent table");
        auto from = source.ToString();
        auto header = s.Query("SELECT * FROM " + from + " LIMIT 0");
        auto types = t.types;
        auto names = t.columns;
        if (std::find(names.begin(), names.end(), "_merge_action") != names.end() ||
            std::find(names.begin(), names.end(), "_merge_columns") != names.end())
            BadResolution("target uses reserved resolution column names");
        types.push_back(LogicalType::VARCHAR);
        types.push_back(LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR));
        names.push_back("_merge_action");
        names.push_back("_merge_columns");
        if (header->GetTypes() != types || header->GetNames() != StringsToIdentifiers(names))
            BadResolution("source must have the target's exact column names/order/types, followed by "
                          "_merge_action VARCHAR and _merge_columns MAP(VARCHAR,VARCHAR)");
        s.Query("CREATE TEMP TABLE merge_decision_capture AS SELECT * FROM " + from + " LIMIT 10001");
        vector<string> sizes{to_string(1024 + 512 * types.size())};
        vector<string> order;
        for (auto k : t.keys) order.push_back(Q(t.columns[k]));
        for (idx_t c = 0; c < types.size(); c++)
            sizes.push_back(MergeValueBytesSQL(types[c], Q(names[c])));
        auto size = s.Query("SELECT count(*),coalesce(sum(" + Join(sizes, "+") + "),0)::BIGINT FROM merge_decision_capture");
        rows += size->Collection().GetValue(0, 0).GetValue<int64_t>();
        bytes += size->Collection().GetValue(1, 0).GetValue<int64_t>();
        if (rows > 10000 || bytes > 8388608)
            throw IOException("[differential:merge_limit] Resolution data exceeds 10000 rows or 8 MiB logical budget");
        auto result = s.Query("SELECT * FROM merge_decision_capture ORDER BY " + Join(order));
        child_list_t<LogicalType> fields;
        for (idx_t c = 0; c < names.size(); c++) fields.emplace_back(names[c], types[c]);
        auto row_type = LogicalType::STRUCT(std::move(fields));
        vector<Value> values;
        for (idx_t r = 0; r < result->RowCount(); r++) {
            s.execution.Check();
            vector<Value> cells;
            for (idx_t c = 0; c < names.size(); c++) cells.push_back(result->Collection().GetValue(c, r));
            values.push_back(Value::STRUCT(row_type, std::move(cells)));
        }
        vector<Value> keys;
        for (auto k : t.keys) keys.push_back(Value(t.columns[k]));
        tables.emplace_back(t.name, Value::STRUCT({{"keys", Value::LIST(LogicalType::VARCHAR, std::move(keys))},
                                                   {"rows", Value::LIST(row_type, std::move(values))},
                                                   {"base", Value::BIGINT(base)}, {"ours", Value::BIGINT(ours)},
                                                   {"theirs", Value::BIGINT(theirs)}}));
        s.Query("DROP TABLE merge_decision_capture");
    }
    s.Query("COMMIT");
    return tables.empty() ? Value() : Value::STRUCT(std::move(tables));
}
void ResolveConflicts(Session &s, const Table &t, const Value &decisions) {
    if (decisions.IsNull()) return;
    auto &fields = StructType::GetChildTypes(decisions.type());
    const Value *table = nullptr;
    for (idx_t i = 0; i < fields.size(); i++)
        if (fields[i].first == t.name) table = &StructValue::GetChildren(decisions)[i];
    if (!table) return;
    auto &rows = StructValue::GetChildren(*table)[1];
    auto &row_type = ListType::GetChildType(rows.type());
    auto &cols = StructType::GetChildTypes(row_type);
    if (cols.size() != t.columns.size() + 2) BadResolution("frozen decision schema mismatch");
    vector<string> ddl, join, keys, nullkeys;
    for (auto &col : cols) ddl.push_back(Q(col.first.GetIdentifierName()) + " " + col.second.ToString());
    for (idx_t c = 0; c < t.columns.size(); c++)
        if (cols[c].first != t.columns[c] || cols[c].second != t.types[c])
            BadResolution("frozen decision schema mismatch");
    s.Query("CREATE TEMP TABLE merge_decisions(" + Join(ddl) + ")");
    {
        Appender appender(s.conn, Identifier("temp"), Identifier("main"), Identifier("merge_decisions"));
        for (auto &row : ListValue::GetChildren(rows)) {
            s.execution.Check();
            appender.BeginRow();
            for (auto &cell : StructValue::GetChildren(row)) appender.Append(cell);
            appender.EndRow();
        }
        appender.Close();
    }
    for (auto k : t.keys) {
        keys.push_back(Q(t.columns[k]));
        join.push_back("d." + Q(t.columns[k]) + "=m.k" + to_string(k));
        nullkeys.push_back(Q(t.columns[k]) + " IS NULL");
    }
    auto invalid = [&](const string &sql, const string &message) {
        if (s.Query("SELECT EXISTS(" + sql + ")")->Collection().GetValue(0, 0).GetValue<bool>())
            BadResolution(t.name + ": " + message);
    };
    invalid("SELECT 1 FROM merge_decisions WHERE " + Join(nullkeys, " OR "), "NULL resolution key");
    invalid("SELECT 1 FROM merge_decisions GROUP BY " + Join(keys) + " HAVING count(*)>1", "duplicate resolution key");
    auto from = " FROM merge_decisions d LEFT JOIN merge_result m ON " + Join(join, " AND ");
    invalid("SELECT 1" + from + " WHERE m.conflict_kind IS NULL", "key is not a conflicting row");
    invalid("SELECT 1" + from + " WHERE d._merge_action IS NULL OR CASE WHEN m.conflict_kind='cell' THEN "
            "d._merge_action!='cells' OR list_sort(map_keys(d._merge_columns)) IS DISTINCT FROM list_sort(m.conflict_columns) "
            "OR EXISTS(SELECT 1 FROM unnest(map_values(d._merge_columns)) v(choice) WHERE choice IS NULL OR choice NOT IN ('ours','theirs','custom')) "
            "ELSE d._merge_action NOT IN ('ours','theirs','custom','delete') OR coalesce(cardinality(d._merge_columns),0)!=0 END",
            "use exact conflicting columns with cells, or an explicit row choice for existence conflicts");
    vector<string> assignments;
    for (idx_t c = 0; c < t.columns.size(); c++) {
        auto i = to_string(c);
        auto choice = "CASE WHEN d._merge_action='cells' THEN map_extract_value(d._merge_columns," +
                      Q(t.columns[c], '\'') + ") ELSE d._merge_action END";
        assignments.push_back("r" + i + "=CASE " + choice + " WHEN 'ours' THEN m.o" + i +
                              " WHEN 'theirs' THEN m.t" + i + " WHEN 'custom' THEN d." + Q(t.columns[c]) +
                              " ELSE m.r" + i + " END");
    }
    assignments.push_back("keep_row=CASE d._merge_action WHEN 'ours' THEN m.op WHEN 'theirs' THEN m.tp "
                          "WHEN 'delete' THEN false WHEN 'custom' THEN true ELSE m.keep_row END");
    assignments.push_back("conflict_kind=NULL");
    assignments.push_back("conflict_columns=[]::VARCHAR[]");
    s.Query("UPDATE merge_result m SET " + Join(assignments) + " FROM merge_decisions d WHERE " + Join(join, " AND "));
    s.Query("DROP TABLE merge_decisions");
}
// A typed relational changeset: key kN; old bN/new oN,tN values; separate row
// presence flags; side operations; and per-column changed flags ocN/tcN.
void Detect(Session &s, const Table &t, const vector<string> &aliases) {
    vector<string> keys, joins[3], projection;
    for (auto k : t.keys)
        keys.push_back(Q(t.columns[k]));
    vector<string> key_scans;
    for (idx_t n = 0; n < 3; n++)
        key_scans.push_back("SELECT " + Join(keys) + " FROM " + t.InputRelation(aliases[n], n));
    for (auto k : t.keys)
        projection.push_back("k." + Q(t.columns[k]) + " AS k" + to_string(k));
    const vector<string> side{"b", "o", "t"};
    for (idx_t n = 0; n < 3; n++) {
        auto a = side[n];
        projection.push_back(a + "." + keys[0] + " IS NOT NULL AS " + a + "p");
        for (idx_t c = 0; c < t.columns.size(); c++)
            projection.push_back(t.InputColumn(c, n, a) + " AS " + a + to_string(c));
        for (auto k : t.keys)
            joins[n].push_back("k." + Q(t.columns[k]) + "=" + a + "." + Q(t.columns[k]));
    }
    string from = " FROM (" + Join(key_scans, " UNION ") + ") k";
    for (idx_t n = 0; n < 3; n++)
        from += " LEFT JOIN " + t.InputRelation(aliases[n], n) + " " + side[n] + " ON " + Join(joins[n], " AND ");
    string input = "SELECT " + Join(projection) + from, ctes;
    bool arrays = false;
    for (auto &type : t.types) arrays |= TypeVisitor::Contains(type, LogicalTypeId::ARRAY);
    if (arrays) {
        // Stock DuckDB 2.0 can fail gathering NULL ARRAY payloads when it reorders
        // this chain of outer hash joins (including arrays nested in containers).
        // Carry those payloads through the joins as native LISTs, materialize in
        // DuckDB, then restore the exact declared types BEFORE change detection.
        // No string/JSON encoding, equality substitution or global setting change;
        // NULL containers and elements survive the lossless native casts and spill.
        vector<string> joined, restored;
        for (auto k : t.keys) {
            joined.push_back("k." + Q(t.columns[k]) + " AS k" + to_string(k));
            restored.push_back("k" + to_string(k));
        }
        string aligned = " FROM (" + Join(key_scans, " UNION ") + ") k";
        for (idx_t n = 0; n < 3; n++) {
            vector<string> fields;
            auto a = side[n];
            joined.push_back(a + "." + keys[0] + " IS NOT NULL AS " + a + "p");
            restored.push_back(a + "p");
            for (idx_t c = 0; c < t.columns.size(); c++) {
                auto value = t.InputColumn(c, n, "s");
                auto name = a + to_string(c);
                auto payload = ArrayType::ConvertToList(t.types[c]);
                if (payload != t.types[c]) value = "CAST(" + value + " AS " + payload.ToString() + ")";
                fields.push_back(value + " AS " + Q(t.columns[c]));
                joined.push_back(a + "." + Q(t.columns[c]) + " AS " + name);
                restored.push_back(payload == t.types[c] ? name :
                    "CAST(" + name + " AS " + t.types[c].ToString() + ") AS " + name);
            }
            aligned += " LEFT JOIN (SELECT " + Join(fields) + " FROM " + t.InputRelation(aliases[n], n) +
                       " s) " + a + " ON " + Join(joins[n], " AND ");
        }
        ctes = "WITH merge_joined AS MATERIALIZED (SELECT " + Join(joined) + aligned + ") ";
        input = "SELECT " + Join(restored) + " FROM merge_joined";
    }
    vector<string> comparisons, flags;
    for (auto a : {string("o"), string("t")}) {
        vector<string> eq;
        for (idx_t c = 0; c < t.columns.size(); c++) {
            auto i = to_string(c);
            eq.push_back(a + i + " IS NOT DISTINCT FROM b" + i);
            flags.push_back("(" + a + i + " IS DISTINCT FROM b" + i + ") AS " + a + "c" + i);
        }
        comparisons.push_back("CASE WHEN NOT bp AND " + a + "p THEN 'insert' WHEN bp AND NOT " + a +
                              "p THEN 'delete' WHEN bp AND " + a + "p AND NOT (" + Join(eq, " AND ") +
                              ") THEN 'update' ELSE NULL END AS " + a + "_op");
    }
    s.Query("CREATE TEMP TABLE merge_changes AS " + ctes + "SELECT * FROM (SELECT *," + Join(flags) + "," +
            Join(comparisons) + " FROM (" + input +
            ")) WHERE o_op IS NOT NULL OR t_op IS NOT NULL");
}
// Pure reconciliation of the changeset, independent of how changes were detected.
void Reconcile(Session &s, const Table &t) {
    vector<string> differs, conflicts, values, names;
    for (idx_t c = 0; c < t.columns.size(); c++) {
        auto i = to_string(c);
        auto diff = "(o" + i + " IS DISTINCT FROM t" + i + ")";
        differs.push_back(diff);
        auto conflict = "(oc" + i + " AND tc" + i + " AND " + diff + ")";
        conflicts.push_back(conflict);
        names.push_back("CASE WHEN (bp AND op != tp AND CASE WHEN op THEN oc" + i + " ELSE tc" + i +
                        " END) OR (NOT bp AND " + diff + ") OR (bp AND " + conflict + ") THEN [" +
                        Q(t.columns[c], '\'') + "] ELSE []::VARCHAR[] END");
        values.push_back("CASE WHEN NOT op THEN t" + i + " WHEN NOT tp THEN o" + i + " WHEN o" + i +
                         " IS NOT DISTINCT FROM t" + i + " THEN o" + i + " WHEN NOT oc" + i + " THEN t" + i +
                         " ELSE o" + i + " END AS r" + i);
    }
    string kind =
        "CASE WHEN NOT bp AND op AND tp AND (" + Join(differs, " OR ") +
        ") THEN 'insert_insert' "
        "WHEN bp AND ((NOT op AND t_op='update') OR (NOT tp AND o_op='update')) THEN 'delete_update' "
        "WHEN bp AND op AND tp AND (" +
        Join(conflicts, " OR ") + ") THEN 'cell' ELSE NULL END";
    s.Query("CREATE TEMP TABLE merge_result AS SELECT *," + kind + " AS conflict_kind," + "list_concat(" +
            Join(names) + ") AS conflict_columns," +
            "CASE WHEN bp THEN op AND tp ELSE op OR tp END AS keep_row," + Join(values) +
            " FROM merge_changes");
}
struct DetailBatch {
    Value details;
    int64_t count = 0, bytes = 0;
};
DetailBatch Details(Session &s, const Table &t, int64_t limit, int64_t offset, int64_t byte_limit) {
    if (!limit || !byte_limit)
        return {Value::LIST(DetailType(), {})};
    vector<string> order, sizes, join;
    // Conservative logical output budget: scalar fields, recursive container nodes,
    // UTF-8 and binary payloads. Estimate in SQL before fetching values into C++.
    sizes.push_back("256::BIGINT");
    for (idx_t c = 0; c < t.columns.size(); c++) {
        auto overhead = to_string(128 + t.columns[c].size() * (MergeNestedType(t.types[c]) ? 2 : 1) +
                                  t.types[c].ToString().size());
        sizes.push_back(to_string(64 + t.columns[c].size())); // conflict-column names
        for (auto a : {string("b"), string("o"), string("t")}) {
            auto bytes = "+" + MergeValueBytesSQL(t.types[c], a + to_string(c));
            sizes.push_back("CASE WHEN " + a + "p THEN " + overhead + bytes + " ELSE 0 END");
        }
    }
    for (auto k : t.keys)
    {
        order.push_back("k" + to_string(k));
        join.push_back("r.k" + to_string(k) + "=p.k" + to_string(k));
        sizes.push_back(to_string(128 + t.columns[k].size() + t.types[k].ToString().size()) +
                        "+" + MergeValueBytesSQL(t.types[k], "k" + to_string(k)));
    }
    s.Query("CREATE TEMP TABLE merge_detail_page AS SELECT * FROM (SELECT *,"
            "sum(_detail_bytes) OVER(ORDER BY " + Join(order) + " ROWS UNBOUNDED PRECEDING) AS _running_bytes "
            "FROM (SELECT " + Join(order) + ",(" + Join(sizes, "+") + ") AS _detail_bytes "
            "FROM merge_result WHERE conflict_kind IS NOT NULL ORDER BY " + Join(order) +
            " LIMIT " + to_string(limit) + " OFFSET " + to_string(offset) + ")) WHERE _running_bytes<=" +
            to_string(byte_limit));
    vector<string> qualified_order;
    for (auto &key : order)
        qualified_order.push_back("r." + key);
    auto result = s.Query("SELECT r.*,p._detail_bytes FROM merge_result r JOIN merge_detail_page p ON " +
                          Join(join, " AND ") + " ORDER BY " + Join(qualified_order));
    std::map<string, idx_t> cols;
    for (idx_t c = 0; c < result->GetNames().size(); c++)
        cols[result->GetNames()[c].GetIdentifierName()] = c;
    auto result_rows = result->Collection().GetRows();
    vector<Value> details;
    int64_t bytes = 0;
    for (idx_t r = 0; r < result->RowCount(); r++) {
        auto get = [&](const string &name) { return result_rows.GetValue(cols.at(name), r); };
        bytes += get("_detail_bytes").GetValue<int64_t>();
        vector<Value> key, rows;
        for (auto k : t.keys)
            key.push_back(MergeCell(t.columns[k], t.types[k], get("k" + to_string(k))));
        for (auto a : {string("b"), string("o"), string("t")}) {
            vector<Value> cells;
            if (get(a + "p").GetValue<bool>())
                for (idx_t c = 0; c < t.columns.size(); c++)
                    cells.push_back(MergeCell(t.columns[c], t.types[c], get(a + to_string(c))));
            rows.push_back(Value::LIST(MergeCellType(), std::move(cells)));
        }
        details.push_back(
            Value::STRUCT(DetailType(), {Value::LIST(MergeCellType(), std::move(key)), get("conflict_kind"),
                                         get("conflict_columns"), get("bp"), get("op"), get("tp"), rows[0],
                                         rows[1], rows[2]}));
    }
    auto count = int64_t(details.size());
    s.Query("DROP TABLE merge_detail_page");
    return {Value::LIST(DetailType(), std::move(details)), count, bytes};
}
void Apply(Session &s, const Table &t, const string &stage) {
    vector<string> join, columns, unchanged;
    for (auto k : t.keys)
        join.push_back("a." + Q(t.columns[k]) + "=r.k" + to_string(k));
    for (idx_t c = 0; c < t.columns.size(); c++) {
        columns.push_back("r" + to_string(c));
        unchanged.push_back(t.InputColumn(c, 1, "a") + " AS " + Q(t.columns[c]));
    }
    s.Query("CREATE TEMP TABLE merge_final AS SELECT " + Join(unchanged) + " FROM " + t.InputRelation(stage, 1) +
            " a WHERE NOT EXISTS "
            "(SELECT 1 FROM merge_result r WHERE " +
            Join(join, " AND ") + ") UNION ALL SELECT " + Join(columns) +
            " FROM merge_result WHERE keep_row");
    s.execution.Phase("table_rebuild", t.name);
    // Fresh indexes avoid DuckDB's eager uniqueness checks against deleted/updated old rows.
    if (t.input_present[1]) s.Query("DROP TABLE " + t.Qualified(stage));
    s.Query("USE " + Q(stage));
    s.Query(t.ddl);
    try {
        s.Query("INSERT INTO " + t.Qualified(stage) + " SELECT * FROM merge_final");
        for (auto &index : t.indexes)
            s.Query(index.second);
    } catch (const Exception &exception) {
        ErrorData error(exception);
        if (error.Type() == ExceptionType::CONSTRAINT) {
            auto message = error.RawMessage();
            if (message.size() > 4096) {
                idx_t cut = 4096;
                while (cut && (static_cast<unsigned char>(message[cut]) & 0xc0) == 0x80)
                    --cut;
                message = message.substr(0, cut) + " [truncated]";
            }
            throw ConstraintException("[differential:merge_constraint] Final combined state of %s: %s",
                                      t.name, message);
        }
        throw;
    }
    s.Query("DROP TABLE merge_final");
    s.execution.Phase("table_inserted", t.name);
}
} // namespace
string MergeResolutionSpecification(const MergeOptions &options) { return ResolutionSpecification(options); }
vector<string> MergePreviewNames() {
    return {"schema_name", "table_name", "ours_inserts", "ours_updates", "ours_deletes",
            "theirs_inserts", "theirs_updates", "theirs_deletes", "conflicts", "conflict_details",
            "validation_state", "validation_error", "detail_bytes", "next_conflict_offset", "details_truncated",
            "base_snapshot_id", "ours_snapshot_id", "theirs_snapshot_id", "selection_ms", "ancestry_nodes", "resolved_conflicts",
            "schema_mode", "schema_changes", "schema_conflicts", "schema_conflict_details", "table_change", "schema_algorithm",
            "object_type", "object_name", "table_action", "view_change"};
}
vector<LogicalType> MergePreviewTypes() {
    return {LogicalType::VARCHAR, LogicalType::VARCHAR,
            LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT,
            LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT,
            LogicalType::LIST(DetailType()), LogicalType::VARCHAR, LogicalType::VARCHAR,
            LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BOOLEAN,
            LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::DOUBLE, LogicalType::BIGINT, LogicalType::BIGINT,
            LogicalType::VARCHAR, LogicalType::LIST(SchemaChangeType()), LogicalType::BIGINT, LogicalType::LIST(SchemaConflictType()),
            TableChangeType(), LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, ViewChangeType()};
}
vector<string> MergeResolutionNames() {
    return {"schema_name", "table_name", "key", "action", "columns", "values",
            "base_snapshot_id", "ours_snapshot_id", "theirs_snapshot_id"};
}
vector<LogicalType> MergeResolutionTypes() {
    return {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::LIST(MergeCellType()), LogicalType::VARCHAR,
            LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR), LogicalType::LIST(MergeCellType()),
            LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT};
}
vector<vector<Value>> MergeResolutionRows(const string &data) {
    auto decisions = DecodeResolutions(data);
    vector<vector<Value>> result;
    if (decisions.IsNull()) return result;
    auto &tables = StructType::GetChildTypes(decisions.type());
    for (idx_t i = 0; i < tables.size(); i++) {
        auto &envelope = StructValue::GetChildren(StructValue::GetChildren(decisions)[i]);
        std::set<string> names;
        for (auto &key : ListValue::GetChildren(envelope[0])) names.insert(key.GetValue<string>());
        for (auto &row : ListValue::GetChildren(envelope[1])) {
            vector<Value> keys, values;
            auto &types = StructType::GetChildTypes(row.type());
            auto &cells = StructValue::GetChildren(row);
            for (idx_t c = 0; c + 2 < cells.size(); c++) {
                auto cell = MergeCell(types[c].first.GetIdentifierName(), types[c].second, cells[c]);
                if (names.count(types[c].first.GetIdentifierName())) keys.push_back(cell);
                values.push_back(std::move(cell));
            }
            result.push_back({Value("main"), Value(tables[i].first), Value::LIST(MergeCellType(), std::move(keys)),
                              cells[cells.size()-2], cells.back(), Value::LIST(MergeCellType(), std::move(values)),
                              envelope[2], envelope[3], envelope[4]});
        }
    }
    return result;
}
void SetMergeConcurrency(int64_t limit) {
    if (limit < 1 || limit > 16)
        throw InvalidInputException("differential_merge_max_concurrent must be 1..16");
    max_concurrent_merges.store(limit);
}
vector<string> MergeStatusNames() {
    return {"execution_id", "operation_id", "database_id", "mode", "phase", "table_name",
            "tables_total", "tables_completed", "input_rows", "input_bytes", "changed_rows",
            "conflicts", "detail_bytes", "stage_write_bytes", "max_stage_write_bytes", "elapsed_ms", "timeout_ms"};
}
vector<LogicalType> MergeStatusTypes() {
    vector<LogicalType> types(6, LogicalType::VARCHAR);
    types.insert(types.end(), 9, LogicalType::BIGINT);
    types.push_back(LogicalType::DOUBLE);
    types.push_back(LogicalType::BIGINT);
    return types;
}
vector<vector<Value>> MergeStatus(differential::Control &control) {
    vector<vector<Value>> rows;
    std::lock_guard<std::mutex> guard(progress_mutex);
    for (auto &entry : progress_registry) {
        auto &p = *entry.second;
        if (p.repository != control.store->directory)
            continue;
        vector<Value> row{Value(p.id), p.operation.empty() ? Value(LogicalType::VARCHAR) : Value(p.operation),
                          Value(p.database), Value(p.mode), Value(p.phase), Value(p.table)};
        for (auto number : {p.tables, p.completed, p.input_rows, p.input_bytes, p.changed_rows, p.conflicts,
                            p.detail_bytes, int64_t(p.writes->used.load()), int64_t(p.writes->limit)})
            row.push_back(Value::BIGINT(number));
        row.push_back(Value(Millis(p.started)));
        row.push_back(Value::BIGINT(p.timeout_ms));
        rows.push_back(std::move(row));
    }
    return rows;
}
differential::Operation AbortLogicalMerge(ClientContext &context, differential::Control &c, const string &database,
                                          const string &operation_id, const string &request, const MergeOptions &options) {
    auto merge = c.FindMergeOperation(database, operation_id);
    auto action = c.BeginAbortMerge(merge, request);
    if (action.state == "complete") return action;
    Execution execution(context, c, database, "abort", merge.id, options);
    execution.Phase("abort_waiting");
    auto lock = c.LockMerge(merge, [&] { execution.Check(); });
    execution.Phase("abort_locked");
    action = c.GetOperation(action.id);
    if (action.state == "complete") return action;
    merge = c.GetOperation(merge.id);
    if (merge.state != "complete" && !c.MergeAborted(merge) && c.PendingMergePublication(merge)) {
        RefreshS3Credentials(context, c);
        differential::CheckAbortMergeS3(c, merge, [&](const string &phase) {
            if (phase.empty()) execution.Check(); else execution.Phase(phase);
        });
    }
    execution.Phase("abort_ready");
    return c.FinishAbortMerge(action, merge);
}

LogicalMergeResult LogicalMerge(ClientContext &context, differential::Control &c, const string &database,
                                int64_t base, int64_t ours, int64_t theirs, const string &destination,
                                const string &request, const MergeOptions &options,
                                const differential::BranchMergeInputs *branches,
                                vector<differential::MergeDiagnostic> *catalog_issues) {
    LogicalMergeResult out;
    out.base = base;
    out.ours = ours;
    out.theirs = theirs;
    if (branches) {
        out.selection_ms = branches->selection_ms;
        out.ancestry_nodes = branches->ancestry_nodes;
    }
    const bool preview = destination.empty();
    auto resolution_spec = ResolutionSpecification(options);
    std::optional<string> frozen_resolutions;
    const bool remote = c.Profile(database) != "local";
    if (c.Writers(database) != "single")
        Unsupported("single-writer database required; shared-writer S3 merging is unsupported");
    if (!preview) {
        out.operation = branches ? branches->operation : c.BeginMerge(database, base, ours, theirs, destination, request,
                                                                     resolution_spec, options.schema_mode);
        out.action = differential::Control::MergeAction(out.operation);
        if (out.operation.state == "complete")
            return out; // Begin validated identity; completed replay needs no decision decoder.
    }
    Execution execution(context, c, database, preview ? (options.validate ? "validate" : "preview") : "merge",
                        out.operation.id, options);
    std::shared_ptr<differential::BranchLock> request_lock;
    if (!preview) {
        execution.Phase("request_waiting");
        request_lock = c.LockMerge(out.operation, [&] { execution.Check(); });
        c.CheckMergeActive(out.operation);
        out.operation = c.GetOperation(out.operation.id);
        if (out.operation.state == "complete") return out;
        execution.StartAttempt(out.operation);
    }
    try {
    if (!preview) execution.Phase("request_locked");
    const bool into_target = !preview && out.action != "created_branch";
    struct TargetOwner {
        differential::Control &control;
        differential::Operation operation;
        std::shared_ptr<differential::BranchLock> lease;
        ~TargetOwner() {
            if (lease) {
                // Pre-preparation failure releases the fence. Prepared S3 output
                // stays fenced until publication or a proven-safe abort.
                try { control.ReleaseMergeTarget(operation); } catch (...) { }
            }
        }
    } target{c, out.operation, nullptr};
    auto publish_remote = [&] {
        auto start = Clock::now();
        out.operation = differential::PublishMergeS3(c, out.operation, [&](const string &phase) {
            if (phase.empty()) execution.Check(); else execution.Phase(phase);
        });
        out.publication_ms = Millis(start);
    };
    if (remote) {
        RefreshS3Credentials(context, c);
        if (!preview && c.PendingMergePublication(out.operation)) {
            if (into_target) {
                target.lease = c.PrepareMergeTarget(out.operation);
                c.CheckMergeTargetClean(target.lease->branch, target.lease->branch.root);
            }
            publish_remote();
            return out; // Validated durable output: inputs/decision sources may have expired since.
        }
    }
    if (!preview) frozen_resolutions = c.MergeResolutions(out.operation, resolution_spec);
    ValidateDifferentialTempDirectory(BufferManager::GetBufferManager(context).GetTemporaryDirectory());
    auto expression_context = NativeExpressionContext(context);
    if (!preview)
        expression_context = c.MergeExpressionContext(out.operation, expression_context.first, expression_context.second);
    execution.Phase("context_frozen");
    Session s(context, c, execution, expression_context);
    vector<differential::Snapshot> snapshots;
    vector<string> aliases;
    std::map<int64_t, string> attached;
    int64_t input_bytes = 0, input_rows = 0;
    auto started = Clock::now();
    // Freeze all metadata and take local reader pins before downloading anything.
    // Evicted inputs have immutable S3 descriptors; their downloaded files get pins
    // below. Remote objects are never deleted by this extension.
    for (auto id : {base, ours, theirs}) {
        auto input = c.PinMergeInput(database, id);
        auto snapshot = input.first;
        if (snapshot.remote || (snapshot.evicted && !remote)) Unsupported("unsupported remote input");
        if (snapshot.evicted && (snapshot.export_state != "complete" || snapshot.export_key.empty()))
            throw IOException("[differential:corrupt_data] Evicted merge input has no completed export");
        auto pin = input.second;
        s.pins.push_back(pin);
        snapshots.push_back(snapshot);
        if (!attached.count(id)) {
            if (snapshot.size > options.max_input_bytes - input_bytes)
                throw IOException("[differential:merge_limit] Input logical byte limit exceeded");
            input_bytes += snapshot.size;
            attached[id] = "_merge_" + pin->id;
        }
        aliases.push_back(attached[id]);
    }
    string expected_remote_etag;
    if (into_target) {
        auto check_started = Clock::now();
        target.lease = c.PrepareMergeTarget(out.operation);
        execution.Phase("target_locked");
        c.CheckMergeTargetClean(target.lease->branch, snapshots[1].root);
        if (remote)
            expected_remote_etag = differential::CheckMergeRemoteTarget(c, target.lease->branch, [&] { execution.Check(); });
        execution.Phase("target_checked");
        out.target_check_ms = Millis(check_started);
    }
    if (remote && !preview && !into_target) differential::CheckMergeRemoteDestination(c, database, destination);
    std::set<int64_t> opened;
    for (idx_t i = 0; i < snapshots.size(); i++) {
        auto &snapshot = snapshots[i];
        if (!opened.insert(snapshot.id).second) continue;
        if (snapshot.evicted) {
            auto start = Clock::now();
            execution.Phase("materializing");
            differential::RemoteCaching caching;
            caching.fill = false;
            differential::RemoteSnapshot input(c, snapshot.export_provider, snapshot.export_key, caching);
            if (input.descriptor.database != database || input.descriptor.snapshot != snapshot.id ||
                input.descriptor.root != snapshot.root || input.descriptor.size != uint64_t(snapshot.size))
                throw IOException("[differential:corrupt_data] Merge input descriptor identity mismatch");
            auto pin = s.pins[i];
            pin->merge_stage = true;
            auto file = c.store->Open("merge_stage/" + pin->id, true, true, false, pin->id);
            auto root = input.Materialize(*c.store, file.id, [&](uint64_t bytes) {
                execution.Check();
                execution.progress->writes->Reserve(bytes);
            });
            auto reader = c.MergeReader(root.id);
            s.pins.push_back(reader);
            s.Attach("diff://merge_input/" + reader->id, aliases[i], true);
            out.materialization_ms += Millis(start);
        } else s.Attach(snapshot.URI(), aliases[i], true);
    }
    execution.Update([&](Progress &p) { p.input_bytes = input_bytes; });
    execution.Phase("pinned");
    auto schema_policy = preview ? std::make_pair(options.schema_mode, differential::Control::CurrentMergeSchemaAlgorithm(options.schema_mode))
                                 : c.MergeSchemaPolicy(out.operation);
    const bool catalog_mode = schema_policy.second == "catalog-v1";
    const bool add_tables = schema_policy.second == "additive-tables-v2";
    vector<std::map<string, Table>> catalogs;
    vector<std::map<string, string>> input_views;
    vector<Views> input_view_info;
    vector<string> ours_view_order, selected_view_order;
    std::map<string, string> views;
    ViewShapes view_shapes;
    bool view_mismatch = false, name_conflicts = false;
    for (idx_t n = 0; n < 3; n++) {
        try {
            catalogs.push_back(s.Schema(aliases[n]));
        } catch (const Exception &error) {
            auto message = ErrorData(error).RawMessage();
            if (!catalog_issues || message.find("[differential:merge_unsupported]") == string::npos) throw;
            // Stop at the first unsupported object in each input. Do not skip it
            // and plan a partial catalog. Other input diagnostics remain useful.
            catalog_issues->push_back({"catalog", "merge_unsupported", vector<string>{"base", "ours", "theirs"}[n],
                                      s.schema_object_type, s.schema_object, CatalogReason(message), "",
                                      "Use supported catalog objects and capture new input snapshots"});
            s.conn.Query("ROLLBACK");
            catalogs.emplace_back();
        }
        input_views.push_back(s.views); input_view_info.push_back(s.view_info);
        if (n == 0) { views = s.views; view_shapes = s.view_shapes; }
        if (n == 1) ours_view_order = s.view_order;
        if (views != s.views || view_shapes != s.view_shapes) view_mismatch = true;
        if (n) for (auto &entry : input_view_info[0]) {
            auto found = s.view_info.find(entry.first);
            if (found == s.view_info.end() || found->second.native_dependencies != entry.second.native_dependencies)
                view_mismatch = true;
        }
    }
    if (catalog_issues && !catalog_issues->empty()) {
        out.comparison_ms = Millis(started) - out.materialization_ms - out.target_check_ms;
        return out;
    }
    if (!catalog_mode && !add_tables && view_mismatch) Unsupported("schema mismatch: view definitions/output/dependencies must be unchanged");
    auto schema = catalog_mode ? PlanCatalogTables(catalogs)
                              : add_tables ? PlanTables(catalogs, input_views, name_conflicts) : PlanSchema(catalogs, false);
    Views planned_views;
    if (catalog_mode) {
        planned_views = PlanViews(input_view_info);
        CheckCatalogNames(schema, planned_views, catalogs, input_view_info);
        if (!CatalogConflicts(schema, planned_views)) ValidateCatalogViews(s, schema, planned_views, selected_view_order);
        views.clear(); view_shapes.clear();
        for (auto &entry : planned_views) if (!entry.second.drop) {
            views.emplace(entry.second.name, entry.second.ddl);
            view_shapes.emplace(entry.second.name, entry.second.shape);
        }
        execution.Phase("catalog_validated");
    } else {
        if (view_mismatch && !name_conflicts) Unsupported("schema mismatch: view definitions/output/dependencies must be unchanged");
        selected_view_order = ours_view_order;
    }
    const auto &result_views = catalog_mode ? planned_views : input_view_info[1];
    catalogs.clear(); input_views.clear();
    idx_t schema_bytes = 0;
    for (auto &entry : schema) if (!entry.second.drop_table) {
        schema_bytes += entry.second.ddl.size();
        for (auto &index : entry.second.indexes) schema_bytes += index.second.size();
    }
    for (auto &view : views) schema_bytes += view.second.size();
    if (schema_bytes > 16777216) throw IOException("[differential:merge_limit] Combined catalog definitions exceed 16 MiB");
    execution.Phase("schema_planned");
    auto append_schema = [&](vector<Value> &row, const Table &t) {
        row.push_back(Value(options.schema_mode));
        row.push_back(Value::LIST(SchemaChangeType(), t.schema_changes));
        row.push_back(Value::BIGINT(t.schema_conflicts.size()));
        row.push_back(Value::LIST(SchemaConflictType(), t.schema_conflicts));
        Value table_change(TableChangeType());
        if (!t.table_change_source.empty()) {
            vector<Value> indexes;
            for (auto &index : t.indexes)
                indexes.push_back(Value::STRUCT(TableIndexType(), {Value(index.first), Value(index.second)}));
            table_change = Value::STRUCT(TableChangeType(),
                {Value(t.table_change_source), Value(t.ddl), Value::LIST(TableIndexType(), std::move(indexes))});
        }
        row.push_back(std::move(table_change));
        row.push_back(Value(schema_policy.second));
        row.push_back(Value("table")); row.push_back(Value(t.name));
        row.push_back(t.table_change_source.empty() ? Value(LogicalType::VARCHAR) : Value(t.drop_table ? "drop" : "add"));
        row.push_back(Value(ViewChangeType()));
    };
    auto catalog_row = [&](const Value &table, const string &state) {
        vector<Value> row{Value("main"), table};
        for (idx_t i = 0; i < 7; i++) row.push_back(Value(LogicalType::BIGINT));
        row.push_back(Value::LIST(DetailType(), {}));
        row.push_back(Value(state)); row.push_back(Value(LogicalType::VARCHAR));
        row.push_back(Value::BIGINT(0)); row.push_back(Value::BIGINT(0)); row.push_back(Value(false));
        row.push_back(Value::BIGINT(base)); row.push_back(Value::BIGINT(ours)); row.push_back(Value::BIGINT(theirs));
        row.push_back(Value(out.selection_ms)); row.push_back(Value::BIGINT(out.ancestry_nodes));
        row.push_back(Value(LogicalType::BIGINT));
        return row;
    };
    auto append_views = [&](bool blocked) {
        for (auto &entry : planned_views) {
            auto &v = entry.second;
            auto row = catalog_row(Value(LogicalType::VARCHAR), blocked
                ? (v.schema_conflicts.empty() ? "blocked_by_schema_conflicts" : "schema_conflicts") : "not_checked");
            row.push_back(Value(options.schema_mode)); row.push_back(Value::LIST(SchemaChangeType(), {}));
            row.push_back(Value::BIGINT(v.schema_conflicts.size())); row.push_back(Value::LIST(SchemaConflictType(), v.schema_conflicts));
            row.push_back(Value(TableChangeType())); row.push_back(Value(schema_policy.second));
            row.push_back(Value("view")); row.push_back(Value(v.name)); row.push_back(Value(LogicalType::VARCHAR));
            Value change(ViewChangeType());
            if (!v.action.empty()) {
                vector<Value> dependencies;
                for (auto &dep : v.dependencies) dependencies.push_back(Value(dep));
                vector<Value> fields{Value(v.action), Value(v.source), v.drop ? Value(LogicalType::VARCHAR) : Value(v.ddl),
                                     Value::LIST(LogicalType::VARCHAR, std::move(dependencies))};
                auto column_type = LogicalType::STRUCT({{"column_name", LogicalType::VARCHAR}, {"sql_type", LogicalType::VARCHAR}});
                for (idx_t side = 0; side < 3; side++) {
                    vector<Value> columns;
                    for (idx_t c = 0; c < v.input_shapes[side].first.size(); c++)
                        columns.push_back(Value::STRUCT(column_type, {Value(v.input_shapes[side].first[c].GetIdentifierName()),
                                                                      Value(v.input_shapes[side].second[c].ToString())}));
                    fields.push_back(v.input_definitions[side].empty() ? Value(LogicalType::LIST(column_type))
                                                                     : Value::LIST(column_type, std::move(columns)));
                }
                for (idx_t side = 0; side < 3; side++)
                    fields.push_back(v.input_definitions[side].empty() ? Value(LogicalType::BOOLEAN) : Value(v.input_tracks[side]));
                change = Value::STRUCT(ViewChangeType(), std::move(fields));
            }
            row.push_back(std::move(change)); out.preview.push_back(std::move(row));
        }
    };
    auto report_schema_conflicts = [&]() {
        auto conflicts = CatalogConflicts(schema, planned_views);
        if (!conflicts) return false;
        if (!preview)
            throw InvalidInputException("[differential:merge_schema_conflict] %llu schema conflicts; use merge preview",
                                        (unsigned long long)conflicts);
        for (auto &entry : schema) {
            auto &t = entry.second;
            auto row = catalog_row(Value(t.name), t.schema_conflicts.empty() ? "blocked_by_schema_conflicts" : "schema_conflicts");
            append_schema(row, t); out.preview.push_back(std::move(row));
        }
        append_views(true);
        out.comparison_ms = Millis(started) - out.materialization_ms - out.target_check_ms;
        return true;
    };
    if (report_schema_conflicts()) return out;
    auto count_inputs = [&]() {
        for (auto &entry : schema) for (auto &attachment : attached) {
            auto side = std::find(aliases.begin(), aliases.end(), attachment.second) - aliases.begin();
            if (!entry.second.input_present.at(side)) continue;
            auto count = s.Query("SELECT count(*) FROM " + entry.second.Qualified(attachment.second))
                             ->Collection().GetValue(0, 0).GetValue<int64_t>();
            if (count > options.max_input_rows - input_rows)
                throw IOException("[differential:merge_limit] Input row limit exceeded at table %s", entry.first);
            input_rows += count;
        }
    };
    if (catalog_mode) {
        count_inputs();
        execution.Update([&](Progress &p) { p.input_rows = input_rows; });
        CheckTableRemovals(s, schema, aliases); CheckColumnRemovals(s, schema, aliases);
        if (report_schema_conflicts()) return out;
    }
    Value decisions;
    if (frozen_resolutions)
        decisions = DecodeResolutions(*frozen_resolutions);
    else {
        decisions = CaptureResolutions(s, schema, options, base, ours, theirs);
        if (!preview) {
            auto bytes = decisions.IsNull() ? string() : EncodeResolutions(decisions);
            decisions = DecodeResolutions(*c.MergeResolutions(out.operation, resolution_spec, bytes));
        }
    }
    if (!decisions.IsNull())
        for (auto &table : StructValue::GetChildren(decisions)) {
            auto &envelope = StructValue::GetChildren(table);
            if (envelope.size() != 5 || envelope[2] != Value::BIGINT(base) ||
                envelope[3] != Value::BIGINT(ours) || envelope[4] != Value::BIGINT(theirs))
                BadResolution("frozen input identity mismatch");
        }
    execution.Phase("resolutions_frozen");
    execution.Update([&](Progress &p) { p.tables = schema.size(); });
    if (!catalog_mode) count_inputs();
    execution.Update([&](Progress &p) { p.input_rows = input_rows; });
    out.comparison_ms = Millis(started) - out.materialization_ms - out.target_check_ms;
    int64_t total_conflicts = 0, returned_details = 0;
    auto detail_limit = options.conflict_limit;
    auto detail_offset = options.conflict_offset;
    auto detail_bytes = options.conflict_bytes;
    idx_t table_ordinal = 0;
    for (auto &entry : schema) {
        auto &t = entry.second;
        if (t.drop_table) {
            if (preview) { auto row = catalog_row(Value(t.name), "not_checked"); append_schema(row, t); out.preview.push_back(std::move(row)); }
            table_ordinal++; continue;
        }
        started = Clock::now();
        execution.Phase("comparison", t.name);
        Detect(s, t, aliases);
        execution.Phase("changes_detected", t.name);
        Reconcile(s, t);
        auto unresolved = s.Query("SELECT count(*) FROM merge_result WHERE conflict_kind IS NOT NULL")
                          ->Collection().GetValue(0, 0).GetValue<int64_t>();
        ResolveConflicts(s, t, decisions);
        auto counts = s.Query(
            "SELECT count(*) FILTER(WHERE o_op='insert'),count(*) FILTER(WHERE o_op='update'),"
            "count(*) FILTER(WHERE o_op='delete'),count(*) FILTER(WHERE t_op='insert'),"
            "count(*) FILTER(WHERE t_op='update'),count(*) FILTER(WHERE t_op='delete'),"
            "count(*) FILTER(WHERE conflict_kind IS NOT NULL),count(*) FROM merge_result");
        auto conflicts = counts->Collection().GetValue(6, 0).GetValue<int64_t>();
        auto changed = counts->Collection().GetValue(7, 0).GetValue<int64_t>();
        total_conflicts += conflicts;
        execution.Update([&](Progress &p) { p.changed_rows += changed; p.conflicts += conflicts; });
        if (preview) {
            vector<Value> row{Value(t.schema), Value(t.name)};
            for (idx_t n = 0; n < 7; n++)
                row.push_back(counts->Collection().GetValue(n, 0));
            auto skipped = std::min(detail_offset, conflicts);
            detail_offset -= skipped;
            auto take = std::min(detail_limit, conflicts - skipped);
            auto details = Details(s, t, take, skipped, detail_bytes);
            row.push_back(std::move(details.details));
            row.push_back(Value(conflicts ? "data_conflicts" : "not_checked"));
            row.push_back(Value(LogicalType::VARCHAR));
            row.push_back(Value::BIGINT(details.bytes));
            row.push_back(Value::BIGINT(0)); // Filled with the global continuation offset below.
            row.push_back(Value::BOOLEAN(conflicts > skipped + details.count));
            row.push_back(Value::BIGINT(base));
            row.push_back(Value::BIGINT(ours));
            row.push_back(Value::BIGINT(theirs));
            row.push_back(Value(out.selection_ms));
            row.push_back(Value::BIGINT(out.ancestry_nodes));
            row.push_back(Value::BIGINT(unresolved - conflicts));
            append_schema(row, t);
            out.preview.push_back(std::move(row));
            returned_details += details.count;
            detail_limit -= details.count;
            detail_bytes -= details.bytes;
            // Keep a contiguous prefix in global conflict order, including when one row
            // is larger than the byte budget. Never skip that row to return smaller later rows.
            if (details.count < take)
                detail_limit = 0;
            execution.Update([&](Progress &p) { p.detail_bytes += details.bytes; });
        }
        out.comparison_ms += Millis(started);
        if (preview && !options.validate)
            s.Query("DROP TABLE merge_result");
        else
            s.Query("ALTER TABLE merge_result RENAME TO " + Q("merge_result_" + to_string(table_ordinal++)));
        s.Query("DROP TABLE merge_changes");
    }
    if (preview) append_views(false);
    for (auto &row : out.preview)
        row[13] = Value::BIGINT(options.conflict_offset + returned_details);
    if (preview && (!options.validate || total_conflicts)) {
        if (options.validate && total_conflicts)
            for (auto &row : out.preview)
                if (row[8].IsNull() || !row[8].GetValue<int64_t>())
                    row[10] = Value("blocked_by_conflicts");
        return out;
    }
    if (total_conflicts)
        throw InvalidInputException("[differential:merge_conflict] %lld conflicting rows; use differential_merge_snapshots_preview",
                                     (long long)total_conflicts);
    string provenance = "{\"algorithm\":\"logical-cell-v1\",\"operation_id\":\"" + out.operation.id + "\"";
    if (branches)
        provenance += ",\"base_selection\":\"" + branches->selection + "\",\"ours_branch_id\":\"" + branches->ours_branch +
                      "\",\"theirs_branch_id\":\"" + branches->theirs_branch + "\"";
    for (idx_t n = 0; n < 3; n++) {
        auto name = vector<string>{"base", "ours", "theirs"}[n];
        provenance += ",\"" + name + "\":{\"snapshot\":" + to_string(snapshots[n].id) + ",\"root\":\"" +
                      snapshots[n].root + "\"}";
    }
    provenance += ",\"schema_mode\":\"" + schema_policy.first + "\",\"schema_algorithm\":\"" + schema_policy.second +
                  "\",\"expression_context\":\"native-expression-context-v1\",\"resolutions\":\"typed-resolutions-duckdb-2-v1\"}";
    if (into_target && out.action != "merged") {
        // Ancestry proves that the result is an already validated native image.
        // Scope checks/comparison still run; no table rebuild or new snapshot.
        auto &input = snapshots[out.action == "fast_forward" ? 2 : 1];
        execution.Phase("publishing");
        auto publish_started = Clock::now();
        if (remote) {
            c.PrepareMergePublication(out.operation, {input.root, uint64_t(input.size)}, provenance, expected_remote_etag);
            execution.Phase("prepared");
            publish_remote();
        } else
            out.operation = c.PublishMergeTarget(out.operation, target.lease->branch,
                                                 {input.root, uint64_t(input.size)}, provenance);
        out.publication_ms = Millis(publish_started);
        target.lease.reset();
        return out;
    }
    started = Clock::now();
    execution.Phase("staging");
    s.stage = c.MergeStage(snapshots[1].root, c.LocalDatabasePool(database), execution.progress->writes);
    auto stage_alias = "_merge_stage_" + s.stage->id;
    s.Attach("diff://merge_stage/" + s.stage->id, stage_alias, false);
    if (!ours_view_order.empty()) {
        execution.Phase("views_dropping");
        for (auto it = ours_view_order.rbegin(); it != ours_view_order.rend(); ++it)
            s.Query("DROP VIEW " + Q(stage_alias) + ".main." + Q(input_view_info[1].at(*it).name));
        execution.Phase("views_dropped");
    }
    if (catalog_mode) {
        // Free names/owned indexes before creating any retained/new tables.
        for (auto &entry : schema) if (entry.second.drop_table) {
            if (entry.second.input_present[1]) s.Query("DROP TABLE " + entry.second.Qualified(stage_alias));
            execution.Phase("table_dropped", entry.first);
        }
    }
    idx_t ordinal = 0;
    for (auto &entry : schema) {
        if (entry.second.drop_table) {
            if (preview) out.preview[ordinal][10] = Value("valid");
            ordinal++; execution.Update([&](Progress &p) { p.completed = ordinal; }); continue;
        }
        s.Query("ALTER TABLE " + Q("merge_result_" + to_string(ordinal)) + " RENAME TO merge_result");
        try {
            if (entry.second.rebuild_schema ||
                s.Query("SELECT count(*) FROM merge_result")->Collection().GetValue(0, 0).GetValue<int64_t>())
                Apply(s, entry.second, stage_alias);
            if (preview)
                out.preview[ordinal][10] = Value("valid");
        } catch (const ConstraintException &error) {
            if (!preview)
                throw;
            out.preview[ordinal][10] = Value("constraint_violation");
            out.preview[ordinal][11] = Value(error.what());
        }
        s.Query("DROP TABLE IF EXISTS merge_final");
        s.Query("DROP TABLE merge_result");
        ordinal++;
        execution.Update([&](Progress &p) { p.completed = ordinal; });
    }
    if (!result_views.empty()) {
        s.Query("USE " + Q(stage_alias));
        try {
            for (auto &name : selected_view_order) {
                auto &view = result_views.at(name);
                CreatePlannedView(s, view);
                if (ReadViewDependencies(s, stage_alias, view.name) != view.native_dependencies)
                    throw IOException("[differential:merge_application] Staged native view dependencies differ from the validated catalog plan");
                execution.Phase("view_created", view.name);
            }
            if (s.BindViews(stage_alias, views, "") != view_shapes)
                throw IOException("[differential:merge_application] Staged view output differs from the validated catalog plan");
        } catch (const Exception &error) {
            auto data = ErrorData(error);
            if (data.Type() == ExceptionType::BINDER || data.Type() == ExceptionType::CATALOG)
                throw IOException("[differential:merge_application] Staged view application failed: %s", CatalogReason(data.RawMessage()));
            throw;
        }
        if (preview && catalog_mode) for (idx_t i = schema.size(); i < out.preview.size(); i++) out.preview[i][10] = Value("valid");
        execution.Phase("views_created");
    }
    execution.Phase("checkpointing");
    s.Query("CHECKPOINT " + Q(stage_alias));
    s.Query("USE " + Q(s.default_catalog));
    s.Query("DETACH " + Q(stage_alias));
    s.attachments.pop_back();
    if (preview) {
        out.application_ms = Millis(started);
        return out;
    }
    auto root = c.store->Seal(c.store->FileId("merge_stage/" + s.stage->id), s.stage->id);
    execution.Phase("staged");
    if (auto gate = differential::Hook("DIFFERENTIAL_MERGE_GATE"))
        execution.Gate(gate);
    execution.Check();
    if (remote) {
        c.PrepareMergePublication(out.operation, root, provenance, expected_remote_etag);
        execution.Phase("prepared");
        out.application_ms += Millis(started);
        publish_remote();
    } else {
        execution.Phase("publishing");
        auto publish_started = Clock::now();
        out.operation = into_target ? c.PublishMergeTarget(out.operation, target.lease->branch, root, provenance)
                                    : c.PublishMerge(out.operation, root, provenance);
        out.publication_ms = Millis(publish_started);
        out.application_ms += Millis(started) - out.publication_ms;
        target.lease.reset();
    }
    return out;
    } catch (const std::exception &error) {
        // Request lock remains held through cleanup and failure recording.
        execution.Failed(&error);
        throw;
    } catch (...) {
        execution.Failed(nullptr);
        throw;
    }
}
} // namespace duckdb
