#pragma once
// Caching of remote (S3) reads: DuckDB's external file cache as the memory layer, and the
// differential_remote_cache_size setting for the repository's disk cache.
#include "duckdb.hpp"
#include "differential_export.hpp"

namespace duckdb {
class FileOpener;
// The memory cache of one DuckDB instance: its ExternalFileCache (buffer-managed, evicted under
// memory pressure, listed by duckdb_external_file_cache(), off with enable_external_file_cache).
std::shared_ptr<differential::RemoteMemoryCache> ExternalFileMemoryCache(DatabaseInstance &db);
// differential_remote_cache_size in bytes ("4GB", "512MiB", 0 = off).
uint64_t ParseRemoteCacheSize(const Value &value);
uint64_t RemoteDiskCacheLimit(optional_ptr<FileOpener> opener);
uint64_t RemoteDiskCacheLimit(ClientContext &context);
} // namespace duckdb
