# duckdb-differential

An experimental C++ extension for **stock DuckDB 2.0** that adds versioning to ordinary
DuckDB databases: snapshots, time travel, branches, clones, restore and S3 export. Native
catalogs, SQL, transactions, indexes and compression run unchanged over a durable
differential filesystem that shares unchanged data between all versions. The SQL surface
follows [DuckLake](https://ducklake.select)'s conventions.

```sql
INSTALL differential FROM community;
LOAD differential;
ATTACH 'differential:sales' AS sales;              -- created on first use
CREATE TABLE sales.t AS SELECT 42 AS value;
CALL differential_create_snapshot('sales', name => 'baseline', message => 'initial load');
UPDATE sales.t SET value = 99;
ATTACH 'differential:sales' AS old (SNAPSHOT_NAME 'baseline');
SELECT value FROM old.t;                           -- 42
SELECT value FROM sales.t AT (VERSION => 1);       -- 42, DuckLake-style time travel
FROM sales.snapshots();
```

**Status: experimental** Build from source and load the local artifact
([development](#development)); community installation commands describe the planned release.
Full v1 acceptance is not complete. Only macOS arm64 and a MinIO S3
endpoint are fully tested; real AWS was exercised manually (exports, reads, branch heads), and
other platforms are untested.

## Documentation

| Guide | Covers |
| --- | --- |
| [Lifecycle guide](docs/lifecycle.md) | One database from creation to cleanup, S3 export included, with runnable SQL |
| [Shared databases](docs/shared-databases.md) | Several hosts writing one database through S3: joining, publishing, conflicts and recovery strategies, failure playbook |
| [SQL reference](docs/sql-reference.md) | Every statement, ATTACH option, setting and function: parameters, defaults, result columns and an example each |
| [SQL API](docs/api.md) | Semantics and rules: ATTACH, time travel, S3 profile, caching, secrets, shared databases, limits and errors |
| [Deployment](docs/deployment.md) | Processes, users and hosts; local storage vs S3; branch heads |
| [Concurrency and multitenancy](docs/concurrency-and-multitenancy.md) | What runs concurrently, guarded heads, shared databases, tenant isolation |
| [Durability and recovery](docs/durability-and-recovery.md) | The durable write path and the evidence for crash recovery |
| [Storage formats](docs/storage-format.md) | Repository layout, metadata schema, hashes, segments, S3 layout and formats |
| [Architecture](docs/architecture.md) | How the extension sits below DuckDB's native storage |
| [Compatibility](docs/compatibility.md) · [Benchmarks](docs/benchmarks.md) ([TPC-H](docs/benchmarks-tpch.md)) | Verified DuckDB build, dependencies and platforms; measurements |
| [Changelog](CHANGELOG.md) · [License](LICENSE) · [Third-party notices](THIRD_PARTY_NOTICES.md) | |

## Contents

- [SQL capabilities](#sql-capabilities): settings, ATTACH, S3 and secrets, time travel, functions
- [Usage lifecycle](#usage-lifecycle): from creation to cleanup
- [Installation](#installation)
- [Durability and storage](#durability-and-storage)
- [Processes, users and hosts](#processes-users-and-hosts): what can be shared
- [Agent skill](#agent-skill): setup for coding agents
- [Development](#development): building from source, tests, CI and releases

## Installation

`differential` targets DuckDB 2.0 and is prepared for community publication. Until it is
published, [build and load it locally](#build). The planned community installation is:

```sql
INSTALL differential FROM community;
LOAD differential;
```

`INSTALL` is needed once per DuckDB installation; `LOAD` is needed in every session before
the first `ATTACH 'differential:…'` or `SET differential_*`. The extension bundles its own
SQLite and AWS SDK. For S3 it uses DuckDB's `TYPE s3` secrets, or your usual AWS credentials
(environment, `~/.aws`, instance roles) when no secret matches ([S3 and secrets](#s3-and-secrets)).
To update, run `UPDATE EXTENSIONS (differential);`.

## SQL capabilities

The full reference is [docs/api.md](docs/api.md).

### Settings

| Setting | Default | Purpose |
| --- | --- | --- |
| `differential_repository` | `$DIFFERENTIAL_STORE_ROOT` or `~/.duckdb/differential` | Local repository directory (metadata and default data). Fixed after first use. |
| `differential_auto_gc` | `true` | Collect unreferenced data in small background steps after checkpoints. |
| `differential_snapshot_on_checkpoint` | `false` | Publish every changed checkpoint as a snapshot. |
| `differential_repository_shared` | `false` | Create the repository for several local users (group-writable). Only applies when the repository is created. |
| `differential_s3_max_requests` | `16` | Parallel S3 requests per process (1–64). Measure your link with `benchmarks/s3_throughput.py`. |
| `differential_group_commit` | `true` | Concurrent commits of different databases share one durable transaction and sync. |
| `differential_export_verification` | `'checksum'` | `'read_back'` also downloads and hashes every uploaded object (for S3-compatible services that ignore upload checksums). |
| `differential_export_staging` | `'disk'` | `'memory'` uploads without staging files on disk. |
| `differential_remote_cache_size` | `0` (off) | Keep data read from S3 in a disk cache in the repository, up to this size (e.g. `'4GB'`). |

### ATTACH

```sql
ATTACH 'differential:<database>' [AS alias] [(option value, ...)];
ATTACH 'differential:s3://bucket/prefix/descriptors/<sha256>' AS alias [(S3 options)];
ATTACH 'differential:s3://bucket/prefix/heads/<database>[/<branch>]' AS alias [(S3 options)];
```

| Option | Effect |
| --- | --- |
| *(none)* | Writable `main` branch; the database is created if missing. |
| `BRANCH 'name'` | Writable branch. |
| `SNAPSHOT_VERSION n` · `SNAPSHOT_NAME 'name'` · `SNAPSHOT_TIME 'ts'` | Read-only snapshot, by ID, name or time (within the branch). |
| `READ_ONLY` | Read-only latest snapshot of the branch. |
| `CREATE_IF_NOT_EXISTS false` | Attach existing databases only. |
| `DATA_PATH '/dir'` · `DATA_PATH 's3://bucket/prefix/'` | Payload placement, fixed at creation: a local file pool, or local storage plus S3 export. |
| `SECRET 'name'` | Use the settings of a `TYPE differential` secret ([below](#s3-and-secrets)). |
| `REGION`, `ENDPOINT`, `CREDENTIAL_PROFILE`, `PATH_STYLE`, `ALLOW_HTTP`, limits | S3 options, usually unnecessary with a `TYPE s3` secret. Credentials are never ATTACH options. |

The second form attaches one S3 export read-only on any host, without the writer's repository.
The third attaches the latest exported snapshot of a branch through its S3 branch head (`main`
when omitted). Only the database's own repository updates its heads, with S3 conditional writes,
so a copy of the repository elsewhere cannot repoint them
([details](docs/concurrency-and-multitenancy.md#guarded-branch-heads)). Native options such as `BLOCK_SIZE` pass through to DuckDB.

### S3 and secrets

Configure S3 once with DuckDB secrets, as for httpfs or DuckLake, and keep ATTACH short:

```sql
-- Credentials and connection settings for a bucket (needs httpfs, which DuckDB autoloads)
CREATE SECRET (TYPE s3, KEY_ID '…', SECRET '…', REGION 'eu-central-1', SCOPE 's3://my-bucket/lake/');

-- Writer: snapshots are exported to S3
ATTACH 'differential:sales' AS sales (DATA_PATH 's3://my-bucket/lake/');

-- Any other host with the same s3 secret: the latest snapshot of sales/main
ATTACH 'differential:s3://my-bucket/lake/heads/sales' AS latest;
```

| Secret | Holds | Applies to |
| --- | --- | --- |
| `TYPE s3` | Credentials, `REGION`, `ENDPOINT`, `URL_STYLE`, `USE_SSL` | Every S3 location its `SCOPE` matches (longest match wins) |
| `TYPE differential` | `DATA_PATH` and S3 options, no credentials | `ATTACH … (SECRET name)`; an unnamed one is used when no `SECRET` is given. Applies when a database is created |

- Options in the statement win over a `differential` secret, which wins over an `s3` secret.
- Without a matching `s3` secret, the AWS SDK finds credentials as usual (environment, `~/.aws`,
  instance roles, or `CREDENTIAL_PROFILE`).
- Credentials are read when you ATTACH or call a function, and never stored by the extension.
  DuckDB stores `PERSISTENT` secrets unencrypted; prefer temporary secrets or
  `PROVIDER credential_chain`.

Data read from S3 is cached in memory by DuckDB's external file cache, so re-attaching an export
or head in the same session does not download it again. To keep it across sessions and
processes too, give the repository a disk cache:

```sql
SET differential_remote_cache_size = '4GB';   -- off by default; least recently used data is evicted
```

The memory cache belongs to one DuckDB instance, so reuse one instance (or a connection pool).
Applications that open a new in-memory instance per request, such as `duckdb.connect()` per
request in Python, should enable the disk cache ([details](docs/api.md#caching-s3-reads)).

### Time travel and catalog functions

```sql
SELECT * FROM sales.t AT (VERSION => 3);                         -- any snapshot of the database
SELECT * FROM sales.t AT (TIMESTAMP => now() - INTERVAL 1 DAY);  -- latest snapshot of the branch at that time
FROM sales.snapshots();  FROM sales.current_snapshot();  FROM sales.branches();
```

`AT` reads the snapshot's own schema, views and macros, and is read-only. The snapshot is
opened, and pinned against collection, when the query is bound; `DETACH` releases it.
Catalog functions also work unqualified after `USE sales`.

### Functions

Functions take the database name or attachment alias first, then named options (`=>`).
Every mutation accepts `request_id => '…'`, making it safely retryable.

| Purpose | Functions |
| --- | --- |
| Inspect | `differential_snapshots(db)`, `differential_current_snapshot(db)`, `differential_branches(db)`, `differential_databases()`, `differential_operations()`, `differential_providers()`, `differential_retention_roots()`, `differential_storage_stats()`, `differential_status()`, `differential_writers(db)`, `differential_followers()` |
| Snapshot | `differential_create_snapshot(db, name =>, author =>, message =>, extra_info =>, export_mode =>)` |
| Fork | `differential_create_branch(db, name, snapshot =>, branch =>)`, `differential_clone_database(db, name, snapshot =>, branch =>)` |
| Restore and delete | `differential_restore(db, snapshot_id, branch =>)`, `differential_drop_branch(db, branch)`, `differential_expire_snapshots(db, versions =>, older_than =>, dry_run =>)` |
| Import and recover | `differential_import_database(path, name, data_path =>)`, `differential_recover_export('s3://…/descriptors/<sha256>', name)`, `differential_recover_branch('s3://…/heads/<db>/<branch>', name, same_identity =>)` |
| S3 | `differential_retry_export(db [, snapshot_id])` (also republishes branch heads), `differential_evict_snapshots(db, versions =>, older_than =>, dry_run =>)`, `differential_remote_heads('s3://bucket/prefix/')`, `differential_claim_branch_heads(db)` |
| Shared databases | `differential_resolve_conflict(db, keep => 'branch' \| 'remote', branch =>, name =>)`, `differential_writers(db)` |
| Maintenance | `differential_cleanup_old_files(dry_run =>, cleanup_all =>, max_objects =>)`, `differential_delete_orphaned_files(dry_run =>)`, `differential_compact(max_pages =>)` |

Functions that take S3 options also accept `secret => 'name'` for a `TYPE differential` secret.
Every function, statement, option and setting, with an example: [docs/sql-reference.md](docs/sql-reference.md).

Rules:
- Management functions run only in autocommit mode, and only when executed. Binding,
  `PREPARE` and `EXPLAIN` change nothing.
- Errors carry stable prefixes such as `[differential:not_found]`.
- Merging branches and deleting a whole database are not supported.

## Usage lifecycle

The [lifecycle guide](docs/lifecycle.md) walks one database through every stage with runnable
SQL:

| Stage | In short |
| --- | --- |
| [Set up](docs/lifecycle.md#1-set-up) | `INSTALL differential FROM community; LOAD differential;` then optionally `SET differential_repository`. |
| [Create](docs/lifecycle.md#2-create-a-database) | `ATTACH 'differential:sales'` creates it. Choose `DATA_PATH` now, or copy in a native file with `differential_import_database`. |
| [Work](docs/lifecycle.md#3-work-with-a-live-branch) | Plain DuckDB SQL. Commits are durable as in native DuckDB. One writer process per branch. |
| [Snapshot](docs/lifecycle.md#4-take-snapshots) | `differential_create_snapshot` publishes an immutable version with name, author and message. |
| [Read history](docs/lifecycle.md#5-read-history) | `AT (VERSION/TIMESTAMP)` in queries, or `ATTACH … (SNAPSHOT_NAME 'v1')`. |
| [Fork](docs/lifecycle.md#6-fork-branches-and-clones) | `differential_create_branch` (same database) or `differential_clone_database` (new database). Both share data until they diverge. |
| [Detach](docs/lifecycle.md#7-detach) | `DETACH` releases the writer, reader pins and AT snapshots. |
| [Restore](docs/lifecycle.md#8-restore-a-branch) | `differential_restore` resets a detached branch, after preserving its current state. |
| [Delete](docs/lifecycle.md#9-delete-branches-and-expire-snapshots) | `differential_drop_branch` and `differential_expire_snapshots`. Contents are preserved or retained until nothing references them. |
| [Reclaim space](docs/lifecycle.md#10-reclaim-space) | Automatic background collection, plus `differential_cleanup_old_files` and `differential_compact`. |
| [S3](docs/lifecycle.md#11-publish-to-s3) | Export on every snapshot; evict local copies; attach or recover exports anywhere. |
| [Operate](docs/lifecycle.md#12-operate-and-recover) | Retry with the same `request_id`; inspect `differential_operations()` and `differential_storage_stats()`. |

Several hosts writing one database through S3 (`WRITERS 'shared'`), with conflicts, recovery
strategies and failure handling, are walked through in [shared databases](docs/shared-databases.md).

## Durability and storage

- **Metadata.** Branches, snapshots, operations and chunk maps live in an embedded SQLite
  metadata store inside the repository. This follows DuckLake's catalog guidance: a DuckDB file
  serves a single client, SQLite serves "multiple local clients". Several DuckDB processes can
  share one repository: writers on different branches, readers and maintenance.
  See [why SQLite for metadata](docs/architecture.md#why-sqlite-for-metadata).
- **Writes.** Native writes are buffered and appended to segment files, then published at native
  sync points (WAL appends store only their bytes; native blocks are stored once, where written).
  Acknowledged commits are durable at `fsync`, as with native DuckDB on every OS. Completed but unsynced writes survive process death and, like
  native writes, may be lost on power loss.
- **Snapshots** are immutable and share unchanged 256 KiB chunks (native blocks) with every other version.
  Branches and clones copy maps, not data.
- **S3 profile.** Commits stay locally durable. A snapshot is remotely durable once its export
  completes; failed or deferred exports keep their local data for retry.
  - Exports are retained until their snapshots expire and `differential_cleanup_remote_files`
    collects them: two phases, with a grace period, safe against concurrent exports.
  - A completed export's local copy can be collected after expiry, or evicted explicitly.
    Reads then go through S3, and branch or restore download it again.
  - **WAL shipping** (opt-in, `SHIP_WAL 'async' | 'sync'`): every commit is shipped to S3 as well, so
    another host recovers the branch with `differential_recover_branch` (in `sync` mode without losing
    an acknowledged commit).
  - **Followers** (`FOLLOW true` on a head path) stay current on their own: every shipped commit, or
    every exported snapshot.
- **Formats.** The repository layout, metadata schema, hashing, segments, local pools and S3 layout
  (packs, `DIX2` index pages, descriptors, shipping epochs) are specified in [docs/storage-format.md](docs/storage-format.md).
- **Topology.** One metadata store per repository and one writer process per branch.
  Failover of a single writer is a recovery of its shipped WAL (`same_identity => true`); readers
  refresh by attaching a newer snapshot or follow the branch.
  Shared databases (`WRITERS 'shared'`) are written by several repositories through S3, with
  optimistic branch-head updates.

## Processes, users and hosts

| Question | Answer |
| --- | --- |
| Several DuckDB processes on one machine? | **Yes:** one writer per branch, any number of snapshot readers, maintenance alongside |
| Several writers on one branch? | **No:** one writer per branch (any process or DuckDB instance gets `writer_busy`); writers on different branches are fine |
| Several local OS users on one repository? | **Yes, if created shared:** `SET differential_repository_shared = true` (group-writable); default is private |
| Repository on a network filesystem? | **No** |
| Several hosts reading S3 exports? | **Yes:** by locator, or the latest of a branch via `…/heads/<database>/<branch>` |
| Several hosts writing the same database? | **Only as a shared database:** `ATTACH 'differential:db' (DATA_PATH 's3://…', WRITERS 'shared')` on every host. Hosts write locally and publish optimistically; one whose branch head moved meanwhile gets `branch_conflict` and keeps its snapshot. Otherwise one repository (and host) owns a database, and others fork with `differential_recover_export` |
| A copy of a repository (restored backup, cloned VM)? | **Detected:** branch heads are written with S3 conditional writes, so the copy's head updates fail with `head_conflict` instead of overwriting the original's. `differential_claim_branch_heads(db)` takes them over deliberately |
| Several tenants? | **One repository per tenant**, ideally with its own OS user and S3 prefix; there are no per-database permissions |

The full comparison (same machine, local storage vs S3, remote hosts) and how sharing and branch
heads work is in [docs/deployment.md](docs/deployment.md). What runs concurrently, how branch
heads are guarded, and how to isolate tenants is in
[docs/concurrency-and-multitenancy.md](docs/concurrency-and-multitenancy.md).

## Agent skill

The repository includes one portable [Agent Skills](https://agentskills.io) skill for coding agents.
It teaches the extension's setup, local lifecycle, S3 replication, followers,
failover, shared writers and common pitfalls. All installations use
[`plugins/duckdb-differential/skills/duckdb-differential`](plugins/duckdb-differential/skills/duckdb-differential),
including its bundled `references/` directory.

Installation guidance follows the linked official harness documentation, reviewed on 2026-10-21.
Compatibility is documented on that basis. Local and CI validation checks our skill files and
manifests; it does not install or execute coding-agent harnesses, their loaders or model workflows.

**Claude Code:** install from this repository's plugin marketplace:

```
/plugin install duckdb-differential --marketplace tobilg/duckdb-differential
```

(or `/plugin marketplace add tobilg/duckdb-differential`, then
`/plugin install duckdb-differential@duckdb-differential`).

**Shared installation:** Codex, pi, Amp, Cursor, GitHub Copilot, OpenCode, Gemini CLI and
Devin Desktop / Windsurf Cascade document discovery from `~/.agents/skills/` (personal skills) or
a project's `.agents/skills/`. From a checkout of this repository, copy the complete skill directory:

```sh
mkdir -p ~/.agents/skills
cp -R plugins/duckdb-differential/skills/duckdb-differential ~/.agents/skills/
```

For project-only use, replace `~/.agents/skills/` with that project's `.agents/skills/`.
A checkout by itself does not register the nested skill. Copies must be refreshed after updating
the checkout. Restart Codex if the skill does not appear; in pi, use `/reload`; in Gemini CLI,
use `/skills reload`; in Amp, ask it to reload skills.

For hosted agents, place the skill in the target repository or its remote environment. A local
personal installation is not a cloud installation: Cursor explicitly does not transfer
`~/.agents/skills/` to Cloud Agents. Copilot's repository installation can also use `.github/skills/`.
See the official guides in the table below for each product's scope and discovery rules.

**Cline:** use its documented personal skills directory instead:

```sh
mkdir -p ~/.cline/skills
cp -R plugins/duckdb-differential/skills/duckdb-differential ~/.cline/skills/
```

For project-only use, replace `~/.cline/skills/` with that project's `.cline/skills/`.

**Pi package installation:** pi can also install directly from this repository, using the skill
path declared in [`package.json`](package.json):

```sh
pi install git:github.com/tobilg/duckdb-differential
```

Use `pi install --local git:github.com/tobilg/duckdb-differential` for a project install, or
`pi install ./path/to/duckdb-differential` for an existing checkout. Choose either the package
installation or the shared skill-directory installation to avoid duplicate skill names.

| Agent | Finding or using the installed skill | Official guide |
| --- | --- | --- |
| Claude Code | `/duckdb-differential:duckdb-differential` after marketplace installation | [Skills](https://code.claude.com/docs/en/skills) |
| Codex | `$duckdb-differential` (also listed under `/skills`) | [Skills](https://developers.openai.com/codex/skills) |
| pi | `/skill:duckdb-differential` | [Skills](https://github.com/badlogic/pi-mono/blob/main/packages/coding-agent/docs/skills.md) |
| Amp | `amp skills list`, then ask Amp to use the `duckdb-differential` skill | [Skills](https://ampcode.com/docs/customize/skills) |
| Cursor | `/duckdb-differential`; view available skills in Customize → Skills | [Skills](https://cursor.com/docs/skills) |
| GitHub Copilot | Ask Copilot to use the `duckdb-differential` skill for the task | [Agent skills](https://docs.github.com/en/copilot/how-tos/copilot-on-github/customize-copilot/customize-cloud-agent/add-skills) |
| OpenCode | Ask the agent to load `duckdb-differential` with its `skill` tool | [Agent skills](https://opencode.ai/docs/skills/) |
| Gemini CLI | `/skills list`, then ask Gemini to use `duckdb-differential` | [Managing skills](https://geminicli.com/docs/cli/using-agent-skills/) |
| Cline | Enabled when discovered (toggle it in the Skills menu); ask Cline to use it | [Skills](https://docs.cline.bot/customization/skills) |
| Devin Desktop / Windsurf Cascade | `@duckdb-differential` | [Cascade skills](https://docs.devin.ai/desktop/cascade/skills) |

Agents can also select the skill automatically when a request matches its description.
The directory installations use the same `SKILL.md` and references directly. The Claude marketplace
and pi package installations use the manifests already included in this repository.

## Development

The sections below cover building, loading and verifying the extension before publication.

The repository follows DuckDB's C++ [extension template](https://github.com/duckdb/extension-template):
DuckDB (`duckdb/`, v2.0.0) and `extension-ci-tools/` are submodules, and CI uses the same
`extension-ci-tools` distribution pipeline as the community repository. Dependencies come from
vcpkg at a pinned commit (`VCPKG_COMMIT` in the `Makefile`): AWS SDK for C++ 1.11.820 (S3
only), SQLite 3.53.2 and OpenSSL 3.6.3, statically linked into the extension; their licenses
and notices are in [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).
Linux (amd64, arm64) and macOS (amd64, arm64) are supported; Windows and wasm are not.

### Build

```sh
git submodule update --init
make vcpkg                       # vcpkg checkout at VCPKG_COMMIT in build/vcpkg
export VCPKG_TOOLCHAIN_PATH=$PWD/build/vcpkg/scripts/buildsystems/vcpkg.cmake
export GEN=ninja                 # optional, faster
make release                     # build/release: DuckDB CLI with the extension, unittest, loadable
make test                        # SQL logic tests in test/sql (what community CI runs)
```

The release loadable is `build/release/extension/differential/differential.duckdb_extension`.
A locally built extension is unsigned: start DuckDB 2.0 with `-unsigned` and load it by path
instead of `LOAD differential;`.

See [upstream testing](docs/upstream-testing.md) for the full upstream SQLLogicTest inventory,
PR profile, manual full runs and individually audited exclusions. Evidence is written to untracked
`build/evidence/`.

### Integration suites

```sh
make integration                 # build/integration: test hooks on, extension NOT linked into the CLI
make test_integration            # C++ unit tests + crash/fault/race/lifecycle suites (Python)
make test_upstream_harness       # verify the SQLLogicTest adapter
make test_upstream_pr            # PR coverage: native baseline + Differential
make test_upstream               # manual full upstream SQLLogicTest inventory, including slow tests
make test_upstream_skips         # manual audit of individually documented exclusions
docker pull ghcr.io/coollabsio/minio@sha256:69b55a1c1c5dc285ce04db96689f5b2102317fc77a50680a1874ca6efd1c87f9
make test_minio                  # S3 suites against a disposable pinned MinIO container (Docker)
```

`make integration` compiles the fault-injection hooks (`-DDIFFERENTIAL_TEST_HOOKS=ON`) and
builds a CLI and `libduckdb` without Differential linked in, so suites LOAD its loadable artifact. Release
builds never contain the hooks. Scripts find artifacts through `test/integration/paths.py`;
override with `DIFFERENTIAL_BUILD_DIR`, `DIFFERENTIAL_EXTENSION`, `DUCKDB_BINARY` or
`DUCKDB_LIBRARY`. Test scripts write result transcripts to `build/evidence/` (not tracked).

To validate the agent skill without building DuckDB, install the pinned validation dependency
in a virtual environment and run the same check as CI:

```sh
python3 -m venv build/skill-venv
. build/skill-venv/bin/activate
python3 -m pip install -r scripts/requirements-skill.txt
make check_skill
```

This performs static checks of documentation synchronization, function coverage, YAML metadata,
bundled link targets, manifest paths and package versions, and runs our validator's regression tests.
It does not install or execute coding-agent harnesses, their loaders or model workflows, locally or
in CI. Harness setup guidance is maintained from official documentation. If Claude Code is installed,
`claude plugin validate .` can additionally check the marketplace and plugin manifests (manually, not in
CI). After editing the bundled source docs, run `python3 scripts/sync_skill.py` to refresh the copies
before checking them.

CI runs the upstream DuckDB SQLLogicTest harness checks and smaller PR profile. Run the complete
suite manually
with `make test_upstream`, and audit exclusions with `make test_upstream_skips` when updating
DuckDB, changing the adapter or reviewing exclusions. Both commands are outside CI.

Upstream comparisons install required official test extensions into a build cache and use
network access for public fixtures. Tests have no default per-test time limit. The three
individually documented excessive-disk workloads are excluded without executing them during audits.

- **MinIO tests** use only a disposable tmpfs container, a loopback port and generated
  credentials; every S3 call uses the AWS SDK. They cover export response loss and fault
  injection, retirement and GC, a fresh reader after the writer's metadata is deleted, writable
  recovery, exports written by another DuckDB version, and coexistence with DuckDB's official
  `aws`, `httpfs` and `sqlite` extensions in one process (downloads them from
  extensions.duckdb.org). No cloud infrastructure is provisioned.
- **Existing bucket.** `examples/s3.py --bucket YOUR_BUCKET --prefix YOUR_OWNED_PREFIX/
  --region us-east-1` runs the S3 example in an existing authorized namespace, and leaves the
  immutable export in place. `test/integration/existing_provider.py --help` describes the SDK
  capability check, which has not been run on AWS yet.
- **Benchmarks.** `DIFFERENTIAL_EXTENSION=build/release/extension/differential/differential.duckdb_extension
  python3 benchmarks/local.py` (also `lifecycle.py`, `operations.py`), then
  `python3 benchmarks/report.py`.
- **S3 throughput.** `python3 benchmarks/s3_throughput.py --bucket YOUR_BUCKET --prefix YOUR_PREFIX/
  --region REGION [--profile PROFILE] --requests 4,8,16,32` exports and cold-scans one database
  per request limit in your bucket and prints upload and download MB/s, plus where the slowest
  ATTACH spent its time (DNS, TLS, credentials, descriptor reads). It leaves its objects
  under a new random subprefix and prints the command to delete them.

### Versioning and DuckDB updates

`make bump-version V=X.Y.Z` updates every version reference (CMake, extension configs, the
reported version, the SQL test, the community descriptor draft in `community/`, and the agent
skill's metadata, Claude plugin manifest and pi package manifest). See
[docs/UPDATING.md](docs/UPDATING.md) for moving to a new DuckDB release, and
[CHANGELOG.md](CHANGELOG.md).
