# Changelog

All notable changes to the DuckDB differential extension are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Agent skill for coding agents, published as a Claude Code plugin marketplace in this repository
  (`/plugin marketplace add tobilg/duckdb-differential`): `SKILL.md` covers setup, the local lifecycle,
  S3 exports, followers, WAL shipping and failover, shared writers and common pitfalls; its references
  are copies of the docs (`scripts/sync_skill.py`, checked by `make check_skill` in CI), and its examples
  run in `test/integration/doc_examples.py`.
- Installation for other coding agents: the same skill directory works with agents that discover
  `~/.agents/skills/` or `.agents/skills/` (Codex, pi, Amp, Cursor, GitHub Copilot, OpenCode, Gemini CLI,
  Devin Desktop / Windsurf Cascade) and with Cline (`~/.cline/skills/`); a `package.json` makes the
  repository a pi package (`pi install git:github.com/tobilg/duckdb-differential`). See
  `README.md#agent-skill`.
- `scripts/check_skill.py` statically validates the skill's frontmatter, bundled links and the Claude and
  pi manifests (paths, names, versions), with regression tests in `test/skill/`; `make check_skill` runs
  it in a separate CI job (PyYAML pinned in `scripts/requirements-skill.txt`).
- `make bump-version` also updates the skill metadata and the plugin and pi package manifests.
- Branch merging for local and single-writer S3 databases (`docs/branch-merge.md`):
  `differential_merge_branch` (into a new branch or the detached `ours` branch, with fast-forward and
  already-up-to-date outcomes), `differential_merge_snapshots` with an explicit base, their `_preview`
  functions, `differential_merge_branch_preflight` with expected-snapshot guards, typed three-way cell
  and row reconciliation with explicit `resolution_tables`, `strict`/`additive`/`catalog` schema modes,
  automatic best-common-ancestor selection (`differential_merge_ancestry`), durable request inspection
  and safe abort (`differential_merge_requests`, `differential_abort_merge`,
  `differential_merge_resolutions`, `differential_merge_status`) and the
  `differential_merge_max_concurrent` setting. Every new snapshot records its parents for base
  selection; snapshots written by 1.0.0 have no parent records, so a repeated merge that reaches them
  needs an explicit base (the first merge of 1.0.0 sibling branches is proven by their fork records).
  See `docs/branch-merge.md#upgrading-from-100`. **Downgrading a repository to 1.0.0 after this version
  has written to it is not supported** (`docs/branch-merge.md#downgrading-is-not-supported`).
- Merge qualification tooling (`docs/merge-qualification.md`, `make test_merge_qualification`) and an
  immutable compatibility fixture written by the published v1.0.0 release.

### Fixed

- `AT (VERSION => …)` and followers (`FOLLOW true`) resolve tables in nested schemas
  (`catalog.main.nested.table`); they looked up only the last schema name and failed.

### Changed

- `differential_cleanup_metadata` never deletes snapshot parent records, and deletes the records of a
  merge request (frozen policy, guards, choices, attempts, publication, abort marker) only together
  with the request, so pending and aborted merges keep their retry and terminal state.

## [1.0.0] - 2026-10-21

### Added

- Continuous WAL shipping, opt-in per single-writer S3 database: `ATTACH … (SHIP_WAL 'async' | 'sync',
  SHIP_INTERVAL seconds)`. Published states of writable branches ship to S3 as create-only, hash-chained
  epochs (`dbs/<uuid>/log/…`, format `differential-epoch-v1`) with WAL bytes and changed chunks inline;
  chains restart from an internal base export after each checkpoint (or 256 epochs / 64 MiB). `sync`
  makes a commit wait until it is in S3. `differential_recover_branch(location, name, same_identity =>)`
  recovers a branch on another host (lazily; `same_identity` for a failover). `differential_branches`
  reports `ship_wal`, `shipped_chain`, `shipped_epoch`, `shipped_at`, `ship_pending`; remote collection
  keeps each branch's newest chain.
- Followers: `ATTACH 'differential:s3://…/heads/…' (FOLLOW true, FOLLOW_INTERVAL seconds)` moves to every
  shipped commit (or exported snapshot) on its own; one request per interval while nothing changes, one
  state per transaction, no S3 request while binding. `differential_followers()` lists them.
- Latency distributions for every timing (`_stddev`, `_p50`, `_p95`, `_p99`), S3 requests timed per
  operation type (`s3_request_*`), and the segment and metadata halves of a durable sync.
- Benchmarks:
  - `benchmarks/tpch.py` and `tpch_report.py` (`make bench-tpch`): TPC-H Q1–Q22 at a scale factor on every
    backend and S3 read path, warm and in new processes, plus `lineitem` writes, with result hashes checked
    against native; `tpch_report.py baseline.json after.json` compares two runs
    ([docs/benchmarks-tpch.md](docs/benchmarks-tpch.md));
  - `benchmarks/scale.py` and `scale_report.py`: gigabyte-scale workloads (concurrent commits, bulk load,
    cold scans, S3 export settings, remote collection, shared history, `ship_latency`) against a
    disk-backed MinIO, also as a manual GitHub workflow (`Benchmarks.yml`;
    [docs/benchmarks-scale.md](docs/benchmarks-scale.md));
  - `benchmarks/disk_cache.py`: the repository disk cache versus local reads.
- Remote garbage collection: `differential_cleanup_remote_files(location)` condemns S3 data that no
  branch head or unexpired snapshot reaches and deletes it in a later run after a grace period
  (default a week). Exports never reference condemned data. Expiry now records exported snapshots
  of every S3 database (not only shared ones) as expired in S3.
- Shared databases read history incrementally: commit records newest first down to known ones,
  expiries from a new time-ordered log (`dbs/<uuid>/expiries/`); a full listing runs at most daily
  or with `differential_snapshots(..., refresh => 'full')`.
- Remote collection caches what index pages, descriptors and commit records reference (they are
  immutable), so repeated runs read only branch heads and new objects.
- `differential_cleanup_metadata(older_than => …)` deletes old completed operation records and
  history entries of expired snapshots.

### Changed

- Targets DuckDB 2.0 (submodule pinned to commit `d54cbbe3df`, which the community repository builds as
  `v2.0.0`, until the `v2.0.0` tag exists; `extension-ci-tools` `v2.0-cyanoptera`); DuckDB 1.5.6 is
  no longer built. Store, control and export formats are unchanged. `duckdb_functions()` lists the
  named parameters of `differential_*` functions in declaration order.
- Store format 2 (repositories and exports of format 1 are refused; there are no migrations):
  - payload moved out of SQLite into append-only segment files (`journal/` while active,
    `segments/` once retired), which are also the write journal: a native block is written once
    and published where it was written, and SQLite holds only metadata;
  - 256 KiB chunks, one per native data block (12 KiB header chunk unchanged); exports are
    `differential-export-v2` with `DIX2` index pages and 8 MiB packs of 32 chunks;
  - WAL sidecars are log files stored as byte ranges, so an append stores only its bytes;
  - local reads are verified with CRC32C (hardware-accelerated) instead of SHA-256, which stays the
    content identity; whole-chunk reads go straight into DuckDB's buffer;
  - retired segments are deleted once unreferenced and compacted when sparse, as part of collection;
    a segment no publication references (external-pool databases) is deleted as soon as it is
    retired (beyond 4 MiB), so pool writes do not leave a second copy awaiting collection.
- Buffered writes are published in the background beyond 16 MiB per database (the writer itself
  publishes only beyond 64 MiB); chunk maps, root pages and object locations are cached in memory.
- Exports stage pack files without holding the staging lock while writing, upload them without
  hashing them again, and record pack and verification metadata without a sync of their own.
  Remote reads keep the 16 most recently used index pages and load up to 16 pages in parallel at
  attach.
- Sync points use `fsync` on every OS, as native DuckDB (macOS no longer uses `F_FULLFSYNC`, which
  also flushed the device's write cache and cost several milliseconds per commit).
- Group commit: concurrent native syncs of different databases in one process share one durable
  SQLite transaction and device flush (up to 16 MiB of new data per transaction; a larger sync
  commits alone; a failed batch is retried one sync at a time). Durability per sync is unchanged.
  `SET differential_group_commit = false` commits every sync on its own. New counters
  `local_sync_requests`, `local_sync_transactions`, `local_sync_batch_max`, `local_sync_batch_fallbacks`.
- Exports no longer download every uploaded object again to verify it: the S3 service verifies its
  SHA-256 (`x-amz-checksum-sha256`). `SET differential_export_verification = 'read_back'` restores
  read-back verification; `SET differential_export_staging = 'memory'` uploads without staging
  files on disk (default `'disk'`).
- `AT (...)` opens snapshots outside the lock of the hidden-attachment cache, so binds of other
  snapshots no longer wait for S3 reads of an evicted one.
- Writes are published per file group (a database file and its WAL sidecars): a native sync
  commits only its own group's buffered writes, and other databases keep writing meanwhile. Each
  group has its own journal and a 64 MiB overlay bound (256 MiB for all groups together).
- On Linux, local pools (`DATA_PATH '/directory'`) make batches of 8 or more files durable with
  two `syncfs` calls instead of one fsync per 64 KiB file.
- Errors that callers act on (writer busy, head and branch conflicts, S3 object conflicts) carry a
  typed code instead of being recognised by their message text. Messages are unchanged.
- `SnapshotAt` (`SNAPSHOT_TIME`, `AT (TIMESTAMP => …)`) reads under a read transaction instead of
  the metadata store's write lock.

- Documentation warns against opening `engine.sqlite` with a second SQLite library (DuckDB's `sqlite`
  extension, Python's `sqlite3`) in a process that has the extension loaded: the copies ignore each
  other's locks, which can crash the process or corrupt the store.
- Testing on DuckDB 2.0: the upstream SQLLogicTest comparison runs each test with a 3 GB memory limit
  (`--memory-limit`), installs the official `quack` and `encodings` extensions when a test requires
  them, and lists the new 2.0 tests individually (exclusions audited on both backends). Integration
  suites read the metadata store from a separate process (`test/integration/metadata_store.py`).

### Fixed

- Reads of S3 exports under memory pressure could hang: the memory cache pinned and allocated buffers
  while DuckDB held the lock of the block being loaded, and eviction could then wait on that same lock.
  Reads now use only resident cache blocks, and a worker thread fills the cache.
- A transaction reading an `AT (...)` snapshot that another connection evicted from the hidden
  attachment cache, then reading it again, failed with a DuckDB internal error ("Database name …
  was already used by a different database for this meta transaction"); it now keeps using its
  attachment.
- Dropping a branch of a single-writer database leaves a tombstone at its S3 head (also repaired by
  `differential_retry_export`); before, the head stayed and kept the branch's data reachable.
- A shared database's advisory writer marker is withdrawn when opening the database file fails
  after the marker was written; it was renewed for the life of the process.

## [0.1.0] - 2026-10-12

### Added

- Native DuckDB block prefetch for S3 exports, with local-handle classification preserved.
- Full upstream SQLLogicTest discovery with a native baseline, harness checks and an expanded
  PR profile in CI, manual exclusion audits and manual full-suite execution with optional sharding.
  No default per-test time limit.
- Local-only spill-directory validation and out-of-core execution coverage.
- Header-aligned chunks: a 12 KiB first chunk followed by 64 KiB chunks, all stored as
  padded 64 KiB objects. The sole unpublished store/export format remains format 1.

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
