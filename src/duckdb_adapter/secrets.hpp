#pragma once
// DuckDB secrets: the TYPE differential secret (DATA_PATH and S3 options, no credentials) and
// TYPE s3 secrets (credentials and connection settings for an S3 location, matched by scope).
//
// Precedence for every option: given in the statement > the differential secret (named with
// SECRET, else the unnamed default) > the best-matching s3 secret > built-in default. Options
// from secrets are defaults: they never change an existing database's data path or provider
// registration. Credentials are looked up again on every ATTACH and management call and kept in
// process memory only.
#include "duckdb.hpp"
#include "sql_options.hpp"

namespace duckdb {
class ExtensionLoader;

void RegisterDifferentialSecret(ExtensionLoader &loader);
// Fill unset options from the TYPE differential secret named by SECRET, else the unnamed default.
void ApplyDifferentialSecret(ClientContext &context, DataPathOptions &options);
// Register (or verify) the S3 provider for a location, with settings from the matching s3 secret.
string EnsureS3(ClientContext &context, differential::Control &control, const DataPathOptions &options,
                const string &location);
// The provider for a new database: "local", a local pool, or an S3 provider.
string EnsureDataPath(ClientContext &context, differential::Control &control, const DataPathOptions &options);
// Look up the s3 secret of every registered S3 provider, so exports, reads and background work in
// this process use current secret credentials.
void RefreshS3Credentials(ClientContext &context, differential::Control &control);
} // namespace duckdb
