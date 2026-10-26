#pragma once
#include "duckdb.hpp"

namespace duckdb {
// Native equality is implemented by the relational merge engine. These helpers only
// validate the supported types, expose typed diagnostics and account for payloads.
void CheckMergeType(const LogicalType &type);
bool MergeNestedType(const LogicalType &type);
LogicalType MergeCellType();
Value MergeCell(const string &name, const LogicalType &type, const Value &value);
string MergeValueBytesSQL(const LogicalType &type, const string &expression);
} // namespace duckdb
