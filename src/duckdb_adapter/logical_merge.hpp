#pragma once
#include "duckdb.hpp"
#include "differential_control.hpp"
namespace duckdb {
vector<LogicalType> MergePreviewTypes();
vector<string> MergePreviewNames();
vector<LogicalType> MergeStatusTypes();
vector<string> MergeStatusNames();
vector<vector<Value>> MergeStatus(differential::Control &);
vector<LogicalType> MergeResolutionTypes();
vector<string> MergeResolutionNames();
vector<vector<Value>> MergeResolutionRows(const string &data);
vector<LogicalType> MergePreflightTypes();
vector<string> MergePreflightNames();
void SetMergeConcurrency(int64_t limit);
struct MergeOptions {
    std::map<string, string> resolution_tables;
    string schema_mode = "strict";
    bool validate = false;
    int64_t conflict_limit = 100, conflict_offset = 0, conflict_bytes = 1048576;
    int64_t max_input_rows = 10000000, max_input_bytes = 1073741824;
    int64_t max_stage_write_bytes = 1073741824, timeout_ms = 600000;
    int64_t max_ancestry_nodes = 100000;
    differential::MergeExpectedInputs expected;
};
vector<Value> MergePreflight(ClientContext &, differential::Control &, const string &database,
                            const string &ours, const string &theirs, const string &destination,
                            const MergeOptions &);
string MergeResolutionSpecification(const MergeOptions &);
differential::Operation AbortLogicalMerge(ClientContext &, differential::Control &, const string &database,
                                          const string &operation_id, const string &request, const MergeOptions &);
struct LogicalMergeResult {
    differential::Operation operation;
    vector<vector<Value>> preview;
    int64_t base = 0, ours = 0, theirs = 0;
    int64_t ancestry_nodes = 0;
    double selection_ms = 0;
    double comparison_ms = 0, application_ms = 0;
    double materialization_ms = 0, publication_ms = 0, target_check_ms = 0;
    string action = "created_branch";
};
LogicalMergeResult LogicalMerge(ClientContext &, differential::Control &, const string &database,
                                int64_t base, int64_t ours, int64_t theirs, const string &destination,
                                const string &request, const MergeOptions &options,
                                const differential::BranchMergeInputs *branches = nullptr,
                                vector<differential::MergeDiagnostic> *catalog_issues = nullptr);
} // namespace duckdb
