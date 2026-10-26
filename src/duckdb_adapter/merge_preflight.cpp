#include "logical_merge.hpp"
#include "differential_export.hpp"
#include "secrets.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/error_data.hpp"
#include <chrono>

namespace duckdb {
namespace {
LogicalType IssueType() {
    return LogicalType::STRUCT({{"phase", LogicalType::VARCHAR}, {"code", LogicalType::VARCHAR},
        {"input", LogicalType::VARCHAR}, {"object_type", LogicalType::VARCHAR}, {"object_name", LogicalType::VARCHAR},
        {"message", LogicalType::VARCHAR}, {"operation_id", LogicalType::VARCHAR}, {"recovery_action", LogicalType::VARCHAR}});
}
string Clip(const string &text, bool &truncated) {
    if (text.size() <= 4096) return text;
    idx_t n = 4096;
    while (n && (static_cast<unsigned char>(text[n]) & 0xc0) == 0x80) --n;
    truncated = true;
    return text.substr(0, n);
}
string ErrorCode(const string &message) {
    auto begin = message.find("[differential:");
    if (begin == string::npos) return "merge_preflight_error";
    begin += 14;
    auto end = message.find(']', begin);
    return end == string::npos ? "merge_preflight_error" : message.substr(begin, end - begin);
}
} // namespace

vector<string> MergePreflightNames() {
    return {"database_id", "destination", "base_snapshot_id", "ours_snapshot_id", "theirs_snapshot_id",
            "ours_branch_id", "theirs_branch_id", "merge_action", "ready", "target_state", "validation_state",
            "data_conflicts", "schema_conflicts", "issues", "issues_truncated", "selection_ms", "ancestry_nodes",
            "target_check_ms", "comparison_ms", "application_ms"};
}
vector<LogicalType> MergePreflightTypes() {
    auto V = LogicalType::VARCHAR, I = LogicalType::BIGINT, B = LogicalType::BOOLEAN, D = LogicalType::DOUBLE;
    return {V, V, I, I, I, V, V, V, B, V, V, I, I, LogicalType::LIST(IssueType()), B, D, I, D, D, D};
}

vector<Value> MergePreflight(ClientContext &context, differential::Control &control, const string &database,
                            const string &ours, const string &theirs, const string &destination,
                            const MergeOptions &options) {
    using Clock = std::chrono::steady_clock;
    auto start = Clock::now();
    auto elapsed = [&] { return std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - start).count(); };
    auto check = [&] {
        if (context.IsInterrupted()) throw InterruptException();
        if (elapsed() >= options.timeout_ms) throw IOException("[differential:merge_timeout] Preflight deadline exceeded");
    };
    vector<Value> issues;
    idx_t issue_bytes = 0;
    bool truncated = false, blocked = false;
    auto add = [&](const differential::MergeDiagnostic &issue) {
        blocked = true;
        if (issues.size() >= 100) { truncated = true; return; }
        vector<Value> fields;
        idx_t bytes = 0;
        for (auto &text : {issue.phase, issue.code, issue.input, issue.object_type, issue.object_name,
                          issue.message, issue.operation_id, issue.recovery_action}) {
            auto clipped = Clip(text, truncated);
            bytes += clipped.size();
            fields.push_back(clipped.empty() ? Value(LogicalType::VARCHAR) : Value(clipped));
        }
        if (bytes > 1048576 - issue_bytes) { truncated = true; return; }
        issue_bytes += bytes;
        issues.push_back(Value::STRUCT(IssueType(), std::move(fields)));
    };
    auto report = [&](const string &phase, const std::exception &error) {
        check(); // Cancellation/deadlines remain SQL errors, never a successful preflight row.
        ErrorData data(error);
        if (data.Type() == ExceptionType::INTERRUPT || data.Type() == ExceptionType::INTERNAL || data.Type() == ExceptionType::FATAL ||
            ErrorCode(data.RawMessage()) == "merge_timeout")
            data.Throw();
        auto message = data.RawMessage();
        add({phase, ErrorCode(message), "", "", "", message, "", "Address this diagnostic and run preflight again"});
    };
    differential::BranchMergeInputs inputs;
    bool selected = false, compared = false;
    string action, validation = "not_checked", target_state = "not_checked";
    int64_t data_conflicts = 0, schema_conflicts = 0;
    LogicalMergeResult result;
    try {
        // Pending lifecycle operations are inspected separately; the selection is
        // still only their current published snapshots, never their staged state.
        inputs = control.ResolveMergeBranches(database, ours, theirs, "", "", options.max_ancestry_nodes,
                                               check, "", options.schema_mode, {}, true);
        selected = true;
        action = destination != ours ? "created_branch" : inputs.base == inputs.theirs ? "already_up_to_date"
               : inputs.base == inputs.ours ? "fast_forward" : "merged";
    } catch (const std::exception &error) { report("inputs", error); }
    if (selected) {
        try {
            auto preview = options;
            check(); preview.timeout_ms -= elapsed();
            // Preflight has aggregate diagnostics; typed row values stay in the
            // existing paginated preview API. No operation or resolution record.
            preview.conflict_limit = 0; preview.conflict_bytes = 0; preview.conflict_offset = 0;
            vector<differential::MergeDiagnostic> unsupported;
            result = LogicalMerge(context, control, database, inputs.base, inputs.ours, inputs.theirs, "", "", preview,
                                  &inputs, &unsupported);
            if (!unsupported.empty()) {
                validation = "unsupported";
                for (auto &issue : unsupported) add(issue);
            } else {
                compared = true;
                validation = options.validate ? "valid" : "not_checked";
                for (auto &row : result.preview) {
                    // Stable columns from MergePreviewNames/Types. Every object
                    // is counted even when the bounded diagnostic list fills.
                    auto object_type = row[27].IsNull() ? string("table") : row[27].GetValue<string>();
                    auto object_name = row[28].IsNull() ? string() : row[28].GetValue<string>();
                    if (!row[8].IsNull()) {
                        auto count = row[8].GetValue<int64_t>();
                        data_conflicts += count;
                        if (count) add({"data", "merge_conflict", "", object_type, object_name,
                            std::to_string(count) + " unresolved conflicting rows", "",
                            "Query merge preview for typed conflict details; supply explicit resolution_tables"});
                    }
                    if (!row[23].IsNull()) schema_conflicts += row[23].GetValue<int64_t>();
                    for (auto &detail : ListValue::GetChildren(row[24])) {
                        auto &fields = StructValue::GetChildren(detail);
                        add({"catalog", "merge_schema_conflict", "", object_type, object_name,
                             fields[1].GetValue<string>() + ": " + fields[6].GetValue<string>(), "",
                             "Resolve schema conflicts in branches and capture new snapshots; row resolutions cannot resolve schema conflicts"});
                    }
                    auto state = row[10].GetValue<string>();
                    if (state == "constraint_violation") {
                        validation = "constraint_violation";
                        add({"validation", "merge_constraint", "", object_type, object_name, ErrorData(row[11].GetValue<string>()).RawMessage(), "",
                             "Correct the invalid final constraint state before merging"});
                    }
                }
                if (schema_conflicts) validation = "schema_conflicts";
                else if (data_conflicts) validation = "data_conflicts";
            }
        } catch (const std::exception &error) {
            validation = ErrorCode(error.what()) == "merge_unsupported" ? "unsupported" : "error";
            report("comparison", error);
        }
    }
    auto target_start = Clock::now();
    try {
        check();
        auto target = control.InspectMergeDestination(database, ours, theirs, destination, selected ? &inputs : nullptr, check);
        target_state = target.target_state;
        for (auto &issue : target.issues) add(issue);
        if (selected && target.issues.empty() && control.Profile(database) != "local") {
            RefreshS3Credentials(context, control);
            if (destination == ours) {
                differential::CheckMergeRemoteTarget(control, control.GetBranch(database, inputs.ours_branch, true), check);
                if (action == "fast_forward" && control.Export(database, inputs.theirs).state != "complete")
                    add({"target", "merge_export_required", "theirs", "branch", theirs,
                         "Fast-forward source has no completed S3 export", "", "Retry its export before merging"});
            } else differential::CheckMergeRemoteDestination(control, database, destination);
        }
    } catch (const std::exception &error) { report("target", error); }
    auto target_ms = std::chrono::duration<double, std::milli>(Clock::now() - target_start).count();
    check();
    auto id = [&](int64_t n) { return n > 0 ? Value::BIGINT(n) : Value(LogicalType::BIGINT); };
    auto text = [&](const string &s) { return s.empty() ? Value(LogicalType::VARCHAR) : Value(s); };
    return {Value(database), Value(destination), id(inputs.base), id(inputs.ours), id(inputs.theirs),
            text(inputs.ours_branch), text(inputs.theirs_branch), text(action), Value(selected && compared && !blocked),
            Value(target_state), Value(validation), compared && !schema_conflicts ? Value::BIGINT(data_conflicts) : Value(LogicalType::BIGINT),
            compared ? Value::BIGINT(schema_conflicts) : Value(LogicalType::BIGINT), Value::LIST(IssueType(), std::move(issues)),
            Value(truncated), Value(inputs.selection_ms), Value::BIGINT(inputs.ancestry_nodes), Value(target_ms),
            Value(result.comparison_ms), Value(result.application_ms)};
}
} // namespace duckdb
