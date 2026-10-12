# Project instructions

- Target DuckDB v1.5.6 (the community repository's current stable). One C++ extension on
  DuckDB's C++ extension template, native storage semantics. Linux and macOS only.
- All S3 operations must use the AWS SDK for C++ pinned through vcpkg (`VCPKG_COMMIT`).
- Preserve native sync/namespace ordering, durable mappings, and all retention roots.
- Never perform persistent management actions during bind or planning. The one approved
  exception: an `AT (...)` clause opens a hidden read-only snapshot attachment and takes its
  reader pin at bind time. Do not extend it to identities, snapshots or exports.
- Use only disposable directories and provider namespaces for destructive tests.
- Keep `docs/implementation-status.md` current. Record actual commands/results;
  distinguish passed probes from full acceptance criteria and untested guarantees.
- Preserve unrelated user edits.
- Do not adopt a core patch, fork, daemon, or weaker durability without a product decision.
- Credentials come from DuckDB `TYPE s3` secrets, a named profile or the AWS SDK chain. Never
  accept them as ATTACH or function options, and never write them anywhere (metadata store,
  descriptors, heads, listings, status, errors). Commit only when asked.

## Product decisions in force

Each decision was made by the repository owner; changing one needs the owner's agreement.

- **Native-equivalent durability.** Writes go to an overlay and journal, and are
  durable at native `FileSync` (SQLite FULL + `F_FULLFSYNC`).
- **Local eviction.** The local copy of a completed S3 export may be evicted; reads
  then go through S3, and branch/clone/restore rehydrate.
- **SQL surface.** The surface is DuckLake-aligned, and the old functions were
  replaced outright, with no aliases. Time travel addresses public snapshots, not individual
  commits. Snapshot on every checkpoint is opt-in.
- **Catalog functions and AT.** Catalog-scoped functions and `AT` time travel are
  provided, with bind-time reader pins. A `TYPE differential` secret was not adopted.

- **Community extension (2026-10-11).** Published through duckdb/community-extensions:
  - built with DuckDB's C++ extension template; the standalone build is retired;
  - dependencies from vcpkg at `cd61e1e` (AWS SDK 1.11.820, SQLite 3.53.2, OpenSSL 3.6.3);
  - Linux amd64/arm64 and macOS amd64/arm64 only;
  - export descriptors record the writer's DuckDB version, but readers accept any version.
- **Sharing (2026-10-11).**
  - Repositories are private by default. `differential_repository_shared` creates a
    group-writable repository and is fixed at creation; every file or directory a repository
    creates must go through the store's `Permissions`.
  - S3 layout: `names/<name>/<uuid>`, and per database `dbs/<uuid>/{branches,snapshots,packs,indexes}/`
    (commit records keyed by the inverted snapshot number, newest first; index pages reference packs
    in any database's pool). ATTACH keeps the `…/heads/<db>[/<uuid>][/<branch>]` path syntax.
  - S3 branch heads (`dbs/<uuid>/branches/<branch>`) are the only mutable S3 objects (besides the
    advisory writer markers of shared databases), republished
    after exports, branch creation, expiry and `differential_retry_export`. They are written
    conditionally (If-Match on the ETag this repository last wrote; `branch_head_writes`), so a copy
    of the repository gets `head_conflict`; `differential_claim_branch_heads` takes them over.
  - One writer host per database, unless created as a shared database (below).
- **Shared databases (2026-10-12).** Optional multi-host writers, per database
  (`WRITERS 'shared'`, fixed at creation, S3 only), with metadata in S3 and no catalog database.
  - Optimistic: a head moves only from its writer's base (If-Match). A conflict fails and keeps
    the snapshot (`branch_conflict`), and the branch is diverged until
    `differential_resolve_conflict(keep => 'branch' | 'remote')`. `ON_CONFLICT 'branch'` (ATTACH,
    opt-in) forks to `<branch>-conflict-<snapshot>` instead.
  - ATTACH reads on demand. Unchanged branches fast-forward. Snapshot numbers come from the
    commit index, and unexported snapshots are renumbered.
  - Publication: synchronous on `differential_create_snapshot`. Each DuckDB checkpoint is a
    snapshot published by a background worker (`PUBLISH_ON_CHECKPOINT`, default on).
    `PUBLISH_ON_DETACH` (opt-in) publishes when the main file closes; failures are recorded,
    not raised.
  - History lives in S3: commit records, cached locally (`remote_commits`); `AT` uses the cache and
    never reads S3 while binding. Expiry: create-only `dbs/<uuid>/expired/<n>` records (heads cannot
    expire). Dropping a branch writes a tombstone head (If-Match), and the name can be reused.
  - Advisory "being written" markers (`dbs/<uuid>/writers/<branch>/<writer>`, renewed, deleted at
    close, 2-minute lease; `ON_BUSY 'warn' | 'error' | 'ignore'`; `differential_writers`). They are
    mutable and deletable, and correctness never depends on them.
- **DuckDB secrets (2026-10-11).**
  - `TYPE s3` secrets matched by SCOPE supply credentials and connection settings
    (region, endpoint, URL style, SSL), looked up at every ATTACH and management call and
    held in process memory only.
  - `TYPE differential` secrets hold DATA_PATH and S3 options (no credentials, no SCOPE); used
    by `SECRET name` or as the unnamed default; they apply when a database is created.
  - Precedence: statement > differential secret > s3 secret > default.
- **Remote read caching (2026-10-11).** DuckDB's `ExternalFileCache` is the memory layer for
  S3 reads (per instance, within memory_limit). The repository disk cache
  (`<repository>/cache/<2 hex>/<sha256>`, `differential_remote_cache_size`, default off) is
  content-addressed, verified on read, LRU-trimmed and never authoritative. Bulk copies never
  fill caches.
- **S3 parallelism (2026-10-11).** `differential_s3_max_requests` (process-wide, 1–64, default 16,
  chosen from a real-AWS run of `benchmarks/s3_throughput.py`); per-location `MAX_REQUESTS` is
  an optional lower cap (0/unset = follow the setting).

## Implementation map

| Area | Files |
| --- | --- |
| Entry, ATTACH callback, settings, status | `src/differential_extension.cpp` |
| Catalog subclass (scoped functions, AT) | `src/duckdb_adapter/differential_catalog.{hpp,cpp}` |
| SQL functions | `src/duckdb_adapter/control_functions.cpp` |
| `DATA_PATH`/S3 option parsing | `src/duckdb_adapter/sql_options.hpp` |
| DuckDB secrets (`TYPE differential`, `TYPE s3` lookup) | `src/duckdb_adapter/secrets.{hpp,cpp}` |
| Filesystem adapter (generations, leases, snapshot on checkpoint) | `src/duckdb_adapter/durable_filesystem.cpp` |
| Byte store, journal, manifests | `src/storage/` |
| Metadata store (SQLite, control format 1) | `src/metadata/control.cpp`, `retention.cpp`, `resolve.cpp` |
| S3 transport, packed exports, remote reads, disk cache, branch heads, shared databases | `src/providers/s3/` |
| Remote memory cache (DuckDB `ExternalFileCache`) | `src/duckdb_adapter/remote_cache.{hpp,cpp}` |
| Local payload pools | `src/providers/local/` |

Storage formats (repository layout, schema, hashes, journal, S3 packs/indexes/descriptors) are
specified in `docs/storage-format.md`. Keep it in sync with any format change, and bump the
relevant version.

SQL surface (details in `docs/api.md`; `docs/sql-reference.md` lists every statement, option, setting
and function with an example, and must stay complete; its examples and those of
`docs/shared-databases.md` are executed by `test/integration/doc_examples.py`):

- **Attaching.** `ATTACH 'differential:<db>'` with `BRANCH`, `SNAPSHOT_VERSION`/`SNAPSHOT_TIME`/
  `SNAPSHOT_NAME`, `READ_ONLY`, `CREATE_IF_NOT_EXISTS`, `DATA_PATH` (local directory or
  `s3://bucket/prefix/`), S3 options and `SECRET`. `ATTACH 'differential:s3://…/descriptors/<sha256>'`
  attaches an export, `…/heads/<db>[/<branch>]` a branch's latest export.
- **Settings.** `SET differential_repository | differential_repository_shared | differential_auto_gc |
  differential_snapshot_on_checkpoint | differential_remote_cache_size | differential_s3_max_requests`.
- **Secrets.** `CREATE SECRET (TYPE differential, DATA_PATH …, <S3 options>)`; `TYPE s3` secrets
  (httpfs) are read for credentials and connection settings.
- **Functions.** `differential_*` table functions take the database or alias first, plus named
  options. Mutations accept `request_id`.
- **Per-catalog.** `<catalog>.snapshots()/current_snapshot()/branches()` and
  `FROM t AT (VERSION|TIMESTAMP => …)`.
- **Internal paths.** `diff://db/<uuid>/…` stays internal and is attachable with
  `TYPE differential`. A plain `diff://` attach gets the stock catalog: no scoped functions,
  no AT.

## Build and verification

The build follows DuckDB's C++ extension template (`duckdb/` and `extension-ci-tools/`
submodules, `extension_config.cmake`, `vcpkg.json`). See `README.md#development` and
`docs/UPDATING.md`.

```sh
make vcpkg && export VCPKG_TOOLCHAIN_PATH=$PWD/build/vcpkg/scripts/buildsystems/vcpkg.cmake GEN=ninja
make release && make test                 # community-CI equivalent: SQL logic tests in test/sql
make integration && make test_integration # hooks build; C++ unit tests + Python suites
make test_minio                           # S3 suites, disposable MinIO (Docker)
```

- `make integration` compiles failpoints and probes (`-DDIFFERENTIAL_TEST_HOOKS=ON`) and does not
  link the extension into its CLI/libduckdb, so suites LOAD the loadable artifact. Release and
  community builds never contain the hooks; the SQL tests assert `test_hooks = absent`.
- Add SQL-level behaviour to `test/sql/*.test` (deterministic output only); add process-death,
  fault, race and S3 suites to `scripts/run_integration.py` or `test/integration/minio.py`.
  Artifact paths come from `test/integration/paths.py`.
- Tests and builds write result transcripts to the untracked `build/evidence/`. Do not track
  transcripts; record commands and results in `docs/implementation-status.md` instead.
- There is exactly one store, control and export format, with no migrations
  (nothing was published before 0.1.0). Once released, any format change needs a version bump,
  a migration and tests against data written by the previous release.
- `make bump-version V=X.Y.Z` keeps all version references consistent; `community/description.yml`
  is the community-extensions descriptor draft (submit only when the owner asks).
- MinIO and coexistence tests need Docker and network access (pinned image, official DuckDB
  extensions). Real AWS was only exercised manually by the owner (exports, reads, heads,
  `benchmarks/s3_throughput.py`); the capability and fault suites have not run on AWS.
  `test/integration/s3_shared.py aws <dir>` (with `DIFFERENTIAL_AWS_LOCATION`) runs the
  shared-database suite on AWS in a fresh prefix; only the owner runs it.
- IDE clang diagnostics in this repo lack a compile database. Trust the CMake build instead.
