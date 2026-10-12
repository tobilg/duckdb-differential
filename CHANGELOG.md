# Changelog

All notable changes to the DuckDB differential extension are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-10-12

### Added

- Versioned native DuckDB databases over a durable, content-addressed differential store:
  snapshots with commit metadata, branches, clones, restore, branch deletion and snapshot expiry.
- Every function is discoverable from SQL: `duckdb_functions()` returns its description, an
  example, categories and real parameter names (also for `<catalog>.snapshots()`,
  `current_snapshot()` and `branches()`).
- DuckLake-aligned SQL: `ATTACH 'differential:<db>'` options (`BRANCH`, `SNAPSHOT_VERSION`,
  `SNAPSHOT_TIME`, `SNAPSHOT_NAME`, `READ_ONLY`, `CREATE_IF_NOT_EXISTS`, `DATA_PATH`),
  `AT (VERSION|TIMESTAMP)` time travel, `<catalog>.snapshots()`, `current_snapshot()` and
  `branches()`, and `SET differential_*` settings.
- Optional snapshot on every checkpoint, incremental background garbage collection and
  compaction.
- S3 snapshot export in content-addressed packs through the AWS SDK for C++, attachable by
  locator on any host, with local eviction, rehydration and writable recovery.
- Builds on DuckDB's C++ extension template and `extension-ci-tools` for Linux and macOS.
- Repositories shared between local users (`SET differential_repository_shared = true` at
  creation; private by default).
- S3 branch heads: remote hosts attach the latest exported snapshot of a branch
  (`ATTACH 'differential:s3://…/heads/<database>/<branch>'`) and list them with
  `differential_remote_heads`.
- S3 layout per database: `dbs/<uuid>/` holds the branch heads, commit records (one per
  exported snapshot, keyed by the inverted snapshot number so a listing returns the newest
  first) and the database's data pool; `names/<name>/<uuid>` indexes database names. Index pages
  reference packs in any database's pool, so a clone's export reuses its source's packs.
- Guarded S3 branch heads: heads are written with S3 conditional writes, so a copy of a
  repository on another host cannot overwrite them (`head_conflict`);
  `differential_claim_branch_heads` takes them over deliberately.
- Shared databases: `ATTACH … (DATA_PATH 's3://…', WRITERS 'shared')` lets several repositories
  (hosts) write one database through S3, with no lock or server.
  - The identity is derived from the name.
  - ATTACH starts from the branch head and reads its data from S3 on demand, with read-ahead for
    scans and DuckDB's external file cache.
  - Snapshot numbers are allocated through the commit index.
  - A branch head moves only from the version its writer started from; otherwise the snapshot
    is kept and the call fails with `branch_conflict`.
  - `differential_resolve_conflict(db, keep => 'branch' | 'remote')` reconciles a diverged branch.
  - `ATTACH … (ON_CONFLICT 'branch')` publishes conflicting snapshots as
    `<branch>-conflict-<n>` instead of failing.
  - `differential_branches()` reports `remote_state`, `remote_snapshot_id` and `kept_branch`.
  - Every DuckDB checkpoint of a shared database is a snapshot published in the background
    (`PUBLISH_ON_CHECKPOINT`, default on). `PUBLISH_ON_DETACH` makes DETACH wait for it.
    Snapshots left unpublished are published at the next ATTACH.
  - Every writer sees the whole history: other writers' snapshots are listed, attachable by
    version, name and time, usable for `AT`, branches and restores. Expiry and branch drops apply
    to every writer (S3 expiry records, head tombstones).
  - Interruptions at any step of a publication, expiry, branch drop, conflict resolution or
    adoption are finished by repeating the operation (fault-tested).
  - Advisory "being written" markers in S3: attaching a branch another host is writing warns
    (`ON_BUSY 'error'` refuses), and `differential_writers()` lists the writers.
  - `differential_databases()` reports `writers`.
- DuckDB secrets: `TYPE s3` secrets (matched by `SCOPE`) supply S3 credentials and connection
  settings, so ATTACH needs no S3 options; a `TYPE differential` secret (`SECRET name`, or the
  unnamed default) holds `DATA_PATH` and S3 options. Credentials are never stored.
- Remote read caching: data read from S3 is cached in DuckDB's external file cache (within
  `memory_limit`, survives DETACH), and optionally in a content-addressed disk cache in the
  repository (`differential_remote_cache_size`, off by default).
- `differential_s3_max_requests` (1–64, default 16) sets the number of parallel S3 requests per
  process (upload staging grows with it, 8 MiB per request); cold scans read ahead per scanning
  thread, with one round trip per window of up to 4 MiB instead of one per block; S3 ATTACH needs
  fewer round trips (one GET per descriptor, which embeds the first index page, and descriptors
  are reused), and creating S3 clients never waits for the EC2 metadata service; `benchmarks/s3_throughput.py` measures upload and download throughput per value
  against your own bucket.
