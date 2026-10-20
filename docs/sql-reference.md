# SQL reference

Every statement, ATTACH option, setting and function of the extension: parameters, defaults,
results and an example each. Rules that apply across functions are in the [SQL API](api.md);
task-oriented walkthroughs are the [lifecycle guide](lifecycle.md) (one host) and
[shared databases](shared-databases.md) (several writers).

The examples build on each other: run them in order. Each block is one session; its first line
names the host. The bucket `s3://my-bucket/lake/` stands for a prefix you own, with S3 credentials
from a `TYPE s3` secret ([secrets](api.md#secrets)). The blocks are executed as written by the
test suite against MinIO (`test/integration/doc_examples.py`), except those marked "not run".

**Contents**
- Statements: [settings](#settings) · [ATTACH](#attach) ([WAL shipping](#wal-shipping),
  [followers](#followers)) · [DETACH](#detach) ·
  [AT](#at-time-travel) · [catalog functions](#catalog-functions) ·
  [CREATE SECRET (TYPE differential)](#create-secret-type-differential)
- Conventions: [parameters](#conventions) · [S3 option parameters](#s3-option-parameters) ·
  [operation result](#operation-result-columns)
- Introspection: [differential_snapshots](#differential_snapshots) ·
  [differential_current_snapshot](#differential_current_snapshot) ·
  [differential_branches](#differential_branches) · [differential_databases](#differential_databases) ·
  [differential_operations](#differential_operations) · [differential_providers](#differential_providers) ·
  [differential_retention_roots](#differential_retention_roots) ·
  [differential_storage_stats](#differential_storage_stats) · [differential_status](#differential_status) ·
  [differential_remote_heads](#differential_remote_heads) · [differential_writers](#differential_writers) ·
  [differential_followers](#differential_followers)
- Snapshots and branches: [differential_create_snapshot](#differential_create_snapshot) ·
  [differential_create_branch](#differential_create_branch) ·
  [differential_clone_database](#differential_clone_database) · [differential_restore](#differential_restore) ·
  [differential_drop_branch](#differential_drop_branch) ·
  [differential_expire_snapshots](#differential_expire_snapshots)
- S3: [differential_retry_export](#differential_retry_export) ·
  [differential_evict_snapshots](#differential_evict_snapshots) ·
  [differential_claim_branch_heads](#differential_claim_branch_heads) ·
  [differential_resolve_conflict](#differential_resolve_conflict)
- Import and recovery: [differential_import_database](#differential_import_database) ·
  [differential_recover_export](#differential_recover_export) ·
  [differential_recover_branch](#differential_recover_branch)
- Maintenance: [differential_cleanup_old_files](#differential_cleanup_old_files) ·
  [differential_delete_orphaned_files](#differential_delete_orphaned_files) ·
  [differential_compact](#differential_compact) ·
  [differential_cleanup_remote_files](#differential_cleanup_remote_files) ·
  [differential_cleanup_metadata](#differential_cleanup_metadata)

## Settings

| Setting | Type | Default | Description |
| --- | --- | --- | --- |
| `differential_repository` | VARCHAR | `$DIFFERENTIAL_STORE_ROOT`, else `~/.duckdb/differential` | Local repository root. Relative paths resolve against the working directory. Creates nothing; cannot change after first use in a DuckDB instance. Global. |
| `differential_repository_shared` | BOOLEAN | `false` | Create the repository group-writable, for several local users. Only applies when the repository is created. Global. |
| `differential_auto_gc` | BOOLEAN | `true` | Run bounded garbage collection in the background after checkpoints. Read when a file is opened. |
| `differential_snapshot_on_checkpoint` | BOOLEAN | `false` | Publish every completed checkpoint of a writable branch as a snapshot (`commit_message = 'checkpoint'`). Read when a file is opened. Shared databases do this by default (`PUBLISH_ON_CHECKPOINT`). |
| `differential_remote_cache_size` | VARCHAR | `'0'` (off) | Size of the repository's disk cache for S3 data, in `memory_limit` syntax (`'4GB'`, `'512MiB'`). Global. |
| `differential_s3_max_requests` | BIGINT | `16` | Parallel S3 requests per process, 1–64, shared by all S3 locations and DuckDB instances. Global; the last value set applies. |
| `differential_group_commit` | BOOLEAN | `true` | Concurrent native syncs of different databases in this process share one durable transaction and sync (up to 16 MiB of new data each; a larger sync commits alone). Durability per sync is unchanged. Process-wide; the last value set applies. |
| `differential_export_verification` | VARCHAR | `'checksum'` | `'checksum'`: the S3 service verifies each uploaded object's SHA-256. `'read_back'`: exports also download and hash every object they upload, for S3-compatible services that ignore upload checksums. Process-wide; the last value set applies. |
| `differential_export_staging` | VARCHAR | `'disk'` | `'disk'`: exports stage each object as a file under the repository before uploading it (quota: 8 MiB per parallel request, 64–512 MiB). `'memory'`: uploads stream from memory within the same budget per process. Process-wide; the last value set applies. |

```sql
-- main
SET differential_repository = '/absolute/repository';
SET differential_auto_gc = true;
SET differential_snapshot_on_checkpoint = false;
SET differential_remote_cache_size = '1GB';
SET differential_s3_max_requests = 16;
SET differential_group_commit = true;
SET differential_export_verification = 'checksum';
SET differential_export_staging = 'disk';
SELECT current_setting('differential_s3_max_requests') AS max_requests;
```

DuckDB's `temp_directory` must be local (or empty to disable spilling). Nonlocal paths
produce `local_temp_directory_required` at SET after loading Differential, or at ATTACH
when configured earlier. Remote export handles use native prefetch by default; its setting
is independent of Differential's cache-backed read-ahead.

```sql
-- main
SET temp_directory = '/data/spill';
SET storage_block_prefetch = 'REMOTE_ONLY';
```

## ATTACH

```sql
ATTACH 'differential:<database>' [AS alias] [(option value, ...)]
ATTACH 'differential:s3://<bucket>/<prefix>/descriptors/<sha256>' [AS alias] [(S3 options)]
ATTACH 'differential:s3://<bucket>/<prefix>/heads/<database>[/<database uuid>][/<branch>]' [AS alias] [(S3 options)]
ATTACH 'differential:s3://<bucket>/<prefix>/heads/<database>[/<database uuid>][/<branch>]' [AS alias] (FOLLOW true [, FOLLOW_INTERVAL seconds] [, S3 options])
ATTACH 'diff://db/<database uuid>/{branches|snapshots}/<id>' [AS alias] (TYPE differential)
```

- `differential:<database>` attaches a database of the repository (created on first use) or, with
  `WRITERS 'shared'`, joins a shared database from S3.
- `…/descriptors/<sha256>` attaches one exported snapshot read-only, without a repository of its
  writer. The locator is a snapshot's `export_locator`.
- `…/heads/<database>[/<uuid>][/<branch>]` attaches the latest exported snapshot of a branch
  (default `main`) read-only. The UUID is needed only when two databases of that name export to
  the prefix.
- With `FOLLOW true` a head path is a follower: read-only, and it moves to the branch's newest state on
  its own, every shipped commit when the database ships its WAL (`SHIP_WAL`), else every exported
  snapshot. See [followers](#followers).
- `diff://…` are the internal URIs from the `uri` columns; they stay valid after names are reused.

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `BRANCH` | VARCHAR | `'main'` | Branch to attach (writable), or to resolve `SNAPSHOT_TIME` and `READ_ONLY` in. |
| `SNAPSHOT_VERSION` | BIGINT | | Attach snapshot `n` read-only. |
| `SNAPSHOT_NAME` | VARCHAR | | Attach the snapshot with this name read-only. |
| `SNAPSHOT_TIME` | TIMESTAMP | | Attach the branch's latest snapshot published at or before this time, read-only. |
| `READ_ONLY` | BOOLEAN | `false` | Without a snapshot option: attach the branch's latest snapshot read-only. |
| `CREATE_IF_NOT_EXISTS` | BOOLEAN | `true` | Create the database (with branch `main`) when missing. |
| `DATA_PATH` | VARCHAR | the metadata store | Where data is stored, fixed at creation: an absolute directory (local pool) or `s3://bucket/prefix/`. Repeating it later must match. |
| `SECRET` | VARCHAR | the unnamed `TYPE differential` secret | `TYPE differential` secret with defaults for `DATA_PATH` and S3 options. |
| `REGION`, `ENDPOINT`, `CREDENTIAL_PROFILE`, `PATH_STYLE`, `ALLOW_HTTP`, `MAX_REQUESTS`, `RETRIES`, `CONNECT_TIMEOUT_MS`, `REQUEST_TIMEOUT_MS`, `MAX_RANGE_BYTES`, `MULTIPART_THRESHOLD` | | | S3 options, see [S3 option parameters](#s3-option-parameters). Fixed when the S3 location is first registered. |
| `WRITERS` | VARCHAR | `'single'` | `'shared'`: several repositories write the database through S3. Fixed at creation; needs an S3 `DATA_PATH`. |
| `ON_CONFLICT` | VARCHAR | `'fail'` | Shared databases, writable: when another writer moved the branch head, `'fail'` reports `branch_conflict`; `'branch'` publishes the snapshot as `<branch>-conflict-<n>`. This attachment only. |
| `ON_BUSY` | VARCHAR | `'warn'` | Shared databases, writable: another live writer of the branch at ATTACH prints a warning (`'warn'`), refuses with `writer_busy` (`'error'`) or is ignored (`'ignore'`). |
| `PUBLISH_ON_CHECKPOINT` | BOOLEAN | `true` | Shared databases, writable: every completed checkpoint becomes a snapshot published in the background. |
| `PUBLISH_ON_DETACH` | BOOLEAN | `false` | Shared databases, writable: when the branch's file closes, wait until its final checkpoint is published. |
| `SHIP_WAL` | VARCHAR | `'off'` | Single-writer databases with an S3 `DATA_PATH`, writable: ship every published state of the attached branch to S3 continuously ([WAL shipping](#wal-shipping)). `'async'`: a commit returns after its local commit and is shipped within `SHIP_INTERVAL`; `'sync'`: a commit also waits until it is in S3. Stored per database: later writable attachments ship until `SHIP_WAL 'off'`. |
| `SHIP_INTERVAL` | DOUBLE | `1` | Seconds (0–3600) between two shipped epochs of a branch in `async` mode, bounding S3 requests. Stored with `SHIP_WAL`. |
| `FOLLOW` | BOOLEAN | `false` | Head paths only: a [follower](#followers) that moves to the branch's newest state. |
| `FOLLOW_INTERVAL` | DOUBLE | `10` | Seconds (1–3600) between a follower's checks of S3; one request per check when nothing changed. |
| `TYPE` | VARCHAR | | `differential` for `diff://` URIs; gives the attachment catalog functions and `AT`. |

Native options such as `BLOCK_SIZE` pass through to DuckDB; unknown options fail. At most one
snapshot option may be given.

A database, its branches and its snapshots:

```sql
-- main
ATTACH 'differential:sales' AS sales;                       -- created on first use, branch main
CREATE TABLE sales.t AS SELECT range AS id, 'v1' AS v FROM range(10);
CALL differential_create_snapshot('sales', name => 'v1');
UPDATE sales.t SET v = 'v2';
CALL differential_create_snapshot('sales', name => 'v2');
CALL differential_create_branch('sales', 'dev');
ATTACH 'differential:sales' AS dev (BRANCH 'dev');           -- a writable branch
ATTACH 'differential:sales' AS s1 (SNAPSHOT_VERSION 1);      -- read-only snapshots
ATTACH 'differential:sales' AS v1 (SNAPSHOT_NAME 'v1');
ATTACH 'differential:sales' AS by_time (SNAPSHOT_TIME '2100-01-01 00:00:00');
SELECT (SELECT v FROM s1.t LIMIT 1) AS version_1, (SELECT v FROM by_time.t LIMIT 1) AS latest_by_time;
DETACH dev;
```

```sql
-- main
ATTACH 'differential:sales' AS latest (READ_ONLY);           -- the branch's latest snapshot
SELECT count(*) AS n FROM latest.t;
```

```sql
-- main
-- expect error: not_found
ATTACH 'differential:absent' AS absent (CREATE_IF_NOT_EXISTS false);
```

Data paths and S3 options are fixed when a database is created:

```sql
-- main
ATTACH 'differential:pooled' AS pooled (DATA_PATH '/data/pool');     -- an immutable-file local pool
ATTACH 'differential:lake_sales' AS lake (DATA_PATH 's3://my-bucket/lake/', REGION 'us-east-1',
    MAX_REQUESTS 8, RETRIES 3, CONNECT_TIMEOUT_MS 2000, REQUEST_TIMEOUT_MS 10000,
    MAX_RANGE_BYTES 8388608, MULTIPART_THRESHOLD 16777216);
CREATE TABLE lake.events AS SELECT range AS id FROM range(1000);
SELECT snapshot_id, export_state FROM differential_create_snapshot('lake');
```

```sql
-- not run: an S3-compatible service without a secret
ATTACH 'differential:minio_sales' AS m (DATA_PATH 's3://test-bucket/lake/', ENDPOINT 'http://localhost:9000',
    PATH_STYLE true, ALLOW_HTTP true, CREDENTIAL_PROFILE 'minio');
```

Exports and branch heads, from any host with read access:

```sql
-- reader
ATTACH 'differential:s3://my-bucket/lake/heads/lake_sales/main' AS lake_latest;
SELECT count(*) AS n FROM lake_latest.events;
```

```sql
-- not run: needs a real export locator (export_locator of a snapshot) and internal UUIDs (uri columns)
ATTACH 'differential:s3://my-bucket/lake/descriptors/<sha256>' AS q3;
ATTACH 'diff://db/<database uuid>/branches/<branch uuid>' AS raw (TYPE differential);
```

A shared database ([guide](shared-databases.md)):

```sql
-- main
ATTACH 'differential:team' AS team (DATA_PATH 's3://my-bucket/lake/', WRITERS 'shared',
    ON_CONFLICT 'branch', ON_BUSY 'warn', PUBLISH_ON_CHECKPOINT true, PUBLISH_ON_DETACH true);
CREATE TABLE team.t AS SELECT range AS id FROM range(100);
CALL differential_create_snapshot('team');
```

### WAL shipping

A single-writer S3 database can ship its committed state to S3 continuously, so another host can
recover a branch as of its last shipped commit with
[differential_recover_branch](#differential_recover_branch) (and DuckDB replays the WAL natively).
Each publication of the branch's files becomes part of an epoch under
`dbs/<uuid>/log/<branch>/…`; `differential_branches` reports what was shipped. Losing the host then
loses only commits not yet shipped: none acknowledged in `'sync'` mode, at most `SHIP_INTERVAL` of
commits in `'async'` mode. The [storage format](storage-format.md#continuous-wal-shipping)
describes epochs and chains.

S3 requests cost money (Amazon S3 Standard in `us-east-1`: $0.005 per 1,000 PUTs, $0.0004 per 1,000
GETs/HEADs). A busy branch costs one PUT per `SHIP_INTERVAL` in `async` mode (default 1 s: about
$13 a month; 10 s: about $1.30) or one PUT per commit in `sync` mode (10 commits per second: about
$130 a month); an idle branch costs nothing. The [cost table](api.md#s3-request-costs) lists every
configuration.

```sql
-- main
ATTACH 'differential:orders' AS orders (DATA_PATH 's3://my-bucket/lake/', SHIP_WAL 'sync');
CREATE TABLE orders.t AS SELECT range AS id FROM range(100);
INSERT INTO orders.t VALUES (100);
SELECT ship_wal, shipped_epoch IS NOT NULL AS shipped, ship_pending FROM differential_branches('orders');
-- expect output: "ship_wal":"sync"
```

```sql
-- main
ATTACH 'differential:orders' AS orders (SHIP_WAL 'async', SHIP_INTERVAL 5);   -- stored: later attachments ship too
INSERT INTO orders.t VALUES (101);
DETACH orders;                                                                 -- ships what is left
```

### Followers

`FOLLOW true` on a head path attaches the branch read-only and keeps it current: a background
thread checks S3 every `FOLLOW_INTERVAL` seconds (one GET, or one HEAD for a branch that only
exports, when nothing changed) and opens a newer state as soon as there is one. Each transaction
reads one state; the next transaction reads the newest. `AT` is not available on a follower.
[differential_followers](#differential_followers) lists them. An idle follower costs one HEAD per
interval: about $0.10 a month at the default 10 s, $1.04 at 1 s ([costs](api.md#s3-request-costs)).

```sql
-- reader
ATTACH 'differential:s3://my-bucket/lake/heads/orders/main' AS orders_live (FOLLOW true, FOLLOW_INTERVAL 10);
SELECT count(*) AS n FROM orders_live.t;
SELECT alias, follows FROM differential_followers();
-- expect output: "follows":"commits"
```

## DETACH

`DETACH alias` releases a writable branch (its writer lock), the reader pins of snapshot
attachments and the snapshots its `AT` clauses opened. For a shared database it also removes the
attachment's "being written" marker and, with `PUBLISH_ON_DETACH true`, waits for publication.

## AT (time travel)

```sql
FROM <table> AT (VERSION => <snapshot id>)
FROM <table> AT (TIMESTAMP => <timestamp>)
```

| Unit | Type | Reads |
| --- | --- | --- |
| `VERSION` | BIGINT | Public snapshot `n` of the attachment's database (any branch). |
| `TIMESTAMP` | TIMESTAMP or TIMESTAMPTZ | The attached branch's latest snapshot at or before the time (shared databases: along the branch's ancestry by publication time). |

The snapshot's own schema applies; AT is read-only and opens the snapshot (with a reader pin) when
the query is bound. It works on branch and snapshot attachments, not on S3 exports.

```sql
-- main
ATTACH 'differential:sales' AS sales;
SELECT v FROM sales.t AT (VERSION => 1) LIMIT 1;
SELECT v FROM sales.t AT (TIMESTAMP => TIMESTAMP '2100-01-01 00:00:00') LIMIT 1;
```

## Catalog functions

`<catalog>.snapshots()`, `<catalog>.current_snapshot()` and `<catalog>.branches()` are
[differential_snapshots](#differential_snapshots), [differential_current_snapshot](#differential_current_snapshot)
and [differential_branches](#differential_branches) bound to an attachment; unqualified after
`USE <catalog>`. They take no parameters and return the same columns.

```sql
-- main
ATTACH 'differential:sales' AS sales;
SELECT snapshot_id, snapshot_name FROM sales.snapshots();
SELECT id FROM sales.current_snapshot();
SELECT branch_name FROM sales.branches();
USE sales;
SELECT count(*) AS snapshots FROM snapshots();
```

## CREATE SECRET (TYPE differential)

```sql
CREATE [PERSISTENT] SECRET [name] (TYPE differential, DATA_PATH '…' [, S3 options])
```

Holds defaults for `DATA_PATH` and the [S3 options](#s3-option-parameters) (no credentials, no
`SCOPE`). A named secret applies where `SECRET name` (ATTACH) or `secret => 'name'` (functions)
names it; the unnamed one applies when none is named. Statement options win over the secret.

```sql
-- main
CREATE SECRET lake_settings (TYPE differential, DATA_PATH 's3://my-bucket/lake/', MAX_REQUESTS 8);
ATTACH 'differential:metrics' AS metrics (SECRET lake_settings);
SELECT data_path FROM differential_databases() WHERE database_name = 'metrics';
SELECT count(*) AS heads FROM differential_remote_heads('s3://my-bucket/lake/', secret => 'lake_settings');
```

## Conventions

- **Calling.** All functions are table functions: `SELECT … FROM f(…)`, `FROM f(…)` or `CALL f(…)`.
  Named parameters accept `=>` and `:=`. A `NULL` named parameter counts as not given; a `NULL`
  positional argument fails.
- **`database`.** The first argument of most functions: a database name or UUID, or the alias of
  an attached differential database (a branch or snapshot attachment resolves to its database
  and, where a function uses one, its branch).
- **`request_id => VARCHAR`** (every mutation, at most 256 bytes): repeating a call with the same ID
  returns the original result and never applies twice; reusing an ID with other arguments fails.
  Bulk functions derive per-item IDs (`<request_id>/<snapshot id>`).
- **Autocommit.** Mutations run only in autocommit mode. Binding, `PREPARE` and `EXPLAIN` change
  nothing.
- **Discoverable from SQL.** `duckdb_functions()` lists every function with a description, an
  example, categories and its parameter names and types:
  `SELECT function_name, description, parameters, parameter_types, examples FROM duckdb_functions() WHERE function_name LIKE 'differential%';`

### S3 option parameters

Accepted by ATTACH (as options), `differential_remote_heads`, `differential_import_database`,
`differential_recover_export` and `differential_cleanup_remote_files` (as named parameters). They are fixed when an S3 location is first
registered; given again, they must match. A `TYPE s3` secret can supply region, endpoint, URL
style and SSL instead.

| Parameter | Type | Default | Description |
| --- | --- | --- | --- |
| `data_path` | VARCHAR | | `s3://bucket/prefix/` or an absolute local directory (not for `differential_remote_heads`). |
| `region` | VARCHAR | `'us-east-1'` | S3 region. |
| `endpoint` | VARCHAR | AWS | Endpoint of an S3-compatible service (`'http://host:port'` or `'host:port'`). |
| `credential_profile` | VARCHAR | | AWS profile for credentials (instead of a secret or the SDK's default chain). |
| `path_style` | BOOLEAN | `false` | Path-style URLs (`endpoint/bucket/key`). |
| `allow_http` | BOOLEAN | `false` | Allow `http://` endpoints (development only). |
| `max_requests` | BIGINT | `0` | Parallel requests to this location; `0` follows `differential_s3_max_requests`. |
| `retries` | BIGINT | `2` | Retries of a failed request. |
| `connect_timeout_ms` | BIGINT | `1000` | Connection timeout. |
| `request_timeout_ms` | BIGINT | `5000` | Request timeout. |
| `max_range_bytes` | BIGINT | `8388608` | Largest range read in one request. |
| `multipart_threshold` | BIGINT | `16777216` | Uploads from this size use multipart upload. |
| `secret` | VARCHAR | | `TYPE differential` secret with defaults for the parameters above. |

### Operation result columns

[differential_create_snapshot](#differential_create_snapshot),
[differential_create_branch](#differential_create_branch),
[differential_clone_database](#differential_clone_database), [differential_restore](#differential_restore),
[differential_drop_branch](#differential_drop_branch),
[differential_import_database](#differential_import_database) and
[differential_recover_export](#differential_recover_export) return one row:

| Column | Type | Description |
| --- | --- | --- |
| `operation_id` | VARCHAR | The durable operation record (`differential_operations`). |
| `database_id` | VARCHAR | Database UUID (the new one for clones, imports and recoveries). |
| `branch_id` | VARCHAR | Branch UUID (the new branch; for `ON_CONFLICT 'branch'`, the fork). |
| `snapshot_id` | BIGINT | The snapshot published, or `0` when none. |
| `preservation_snapshot_id` | BIGINT | Snapshot preserving a branch's previous contents (restore, drop), or `0`. |
| `uri` | VARCHAR | Internal URI of the snapshot (or of the branch when no snapshot was published). |
| `snapshot_time` | TIMESTAMPTZ | Publication time of the snapshot. |
| `state` | VARCHAR | `complete`; `published_as_branch` when a conflicting snapshot became a fork. |
| `export_state` | VARCHAR | `not_required` (no S3), `pending` or `complete`. |
| `export_locator` | VARCHAR | `descriptors/<sha256>` of the S3 export, when complete. |

## Introspection functions

### differential_snapshots

```sql
differential_snapshots(database VARCHAR, refresh => VARCHAR)
```

All snapshots of a database, retired ones included. For a shared database it first reads new
commit records and expiries from S3, so other writers' snapshots are listed too
(`publication_state = 'remote'`). That read is incremental; `refresh => 'full'` lists all of the
database's commit and expiry records (otherwise done at most once a day).

| Column | Type | Description |
| --- | --- | --- |
| `snapshot_id` | BIGINT | Snapshot number (unique per database). |
| `snapshot_time` | TIMESTAMPTZ | Publication time. |
| `snapshot_name`, `author`, `commit_message`, `commit_extra_info` | VARCHAR | Commit metadata (NULL when not given). |
| `branch_id` | VARCHAR | Branch that published it. |
| `captured_at` | TIMESTAMPTZ | When its checkpoint completed. |
| `root` | VARCHAR | Root identity of its image (NULL for remote snapshots). |
| `logical_size` | BIGINT | Size of the database file in bytes (NULL for remote snapshots). |
| `retired` | BOOLEAN | Expired. |
| `publication_state` | VARCHAR | `local_published`, `local_evicted` (local copy dropped) or `remote` (another writer's). |
| `export_state` | VARCHAR | `not_required`, `pending`, `complete` or `abandoned`. |
| `export_locator` | VARCHAR | `descriptors/<sha256>` when exported. |
| `uri` | VARCHAR | Internal URI (`diff://db/<uuid>/snapshots/<id>`). |
| `database_id`, `database_name` | VARCHAR | The database. |
| `source_database_id`, `source_snapshot_id` | VARCHAR, BIGINT | Where the publishing branch came from (branches, clones, imports). |

```sql
-- main
SELECT snapshot_id, snapshot_name, publication_state, export_state FROM differential_snapshots('sales');
```

### differential_current_snapshot

```sql
differential_current_snapshot(database VARCHAR)
```

For a snapshot attachment, its snapshot; for a branch attachment, the branch's latest snapshot;
for a database name, the latest snapshot of the database. Returns `id BIGINT` (NULL when there is none).

```sql
-- main
SELECT id FROM differential_current_snapshot('sales');
```

### differential_branches

```sql
differential_branches(database VARCHAR)
```

Every branch of a database, dropped ones included. Reads no S3.

| Column | Type | Description |
| --- | --- | --- |
| `database_id`, `database_name` | VARCHAR | The database. |
| `branch_id`, `branch_name` | VARCHAR | The branch. |
| `state` | VARCHAR | `active` or `deleted`. |
| `root` | VARCHAR | Root of its latest snapshot. |
| `writer_epoch` | BIGINT | Increases with every writer; fences stale writers. |
| `head_snapshot_id` | BIGINT | Latest snapshot in the branch's history. |
| `created_at` | TIMESTAMPTZ | Creation time. |
| `source_database_id`, `source_snapshot_id` | VARCHAR, BIGINT | Where it was created from. |
| `uri` | VARCHAR | Internal URI (`diff://db/<uuid>/branches/<uuid>`). |
| `remote_state` | VARCHAR | Shared databases: `in_sync`, `ahead` (unpublished snapshots), `unpublished`, `diverged` or `forked`; else NULL. |
| `remote_snapshot_id` | BIGINT | Shared databases: the S3 head's snapshot as last seen. |
| `kept_branch` | VARCHAR | Shared databases: the fork a conflicting snapshot was published as. |
| `ship_wal` | VARCHAR | Single-writer databases: `off`, `async` or `sync` ([WAL shipping](#wal-shipping)); NULL for shared databases. |
| `shipped_chain`, `shipped_epoch` | BIGINT | The newest epoch this repository shipped for the branch. |
| `shipped_at` | TIMESTAMPTZ | When it was shipped. |
| `ship_pending` | BOOLEAN | Changes of the branch not shipped yet (NULL when not shipping). |

```sql
-- main
SELECT branch_name, state, head_snapshot_id, remote_state FROM differential_branches('sales');
```

### differential_databases

```sql
differential_databases()
```

| Column | Type | Description |
| --- | --- | --- |
| `database_id`, `database_name` | VARCHAR | The database. |
| `data_path` | VARCHAR | Its `DATA_PATH` (NULL: the metadata store). |
| `last_snapshot_id` | BIGINT | Highest snapshot number allocated. |
| `created_at` | TIMESTAMPTZ | Creation time. |
| `writers` | VARCHAR | `single` or `shared`. |

```sql
-- main
SELECT database_name, data_path, writers FROM differential_databases();
```

### differential_operations

```sql
differential_operations()
```

Durable records of every mutation, for request replay and inspection.

| Column | Type | Description |
| --- | --- | --- |
| `operation_id`, `request_id` | VARCHAR | The operation and its request ID (NULL when none was given). |
| `kind` | VARCHAR | `create_database`, `snapshot`, `branch`, `clone`, `restore`, `drop_branch`, `retire`, `evict`, `retry_export`, `import_native`, `recover_export`, `resolve_conflict`, `gc`, `compact`. |
| `state` | VARCHAR | `pending`, `complete` or `local_complete_export_pending`. |
| `database_id`, `branch_id` | VARCHAR | What it changed. |
| `snapshot_id`, `preservation_snapshot_id` | BIGINT | Snapshots it published. |

```sql
-- main
SELECT kind, state, snapshot_id FROM differential_operations();
```

### differential_providers

```sql
differential_providers()
```

The repository's storage locations: the default store, local pools and S3 locations, with their
S3 configuration (never credentials).

| Column | Type | Description |
| --- | --- | --- |
| `provider_id` | VARCHAR | Location identity. |
| `data_path` | VARCHAR | `s3://bucket/prefix/` or the pool directory (NULL: the metadata store). |
| `profile` | VARCHAR | Durability profile. |
| `region`, `endpoint` | VARCHAR | S3 region and endpoint. |
| `credential_source` | VARCHAR | The DuckDB secret in use, the AWS profile, or the AWS SDK default chain. |
| `max_requests`, `retries`, `connect_timeout_ms`, `request_timeout_ms`, `max_range_bytes`, `multipart_threshold` | BIGINT | S3 limits. |

```sql
-- main
SELECT data_path, credential_source FROM differential_providers();
```

### differential_retention_roots

```sql
differential_retention_roots()
```

Why each stored image is kept: `reason VARCHAR` (`snapshot`, `branch`, `reader_pin`,
`generation`, `compound_target`, `export_pending`, `native_file`), `identity VARCHAR` (what holds
it) and `root VARCHAR`.

```sql
-- main
SELECT reason, identity FROM differential_retention_roots();
```

### differential_storage_stats

```sql
differential_storage_stats()
```

Counters as `metric VARCHAR`, `value BIGINT`: timings (`<name>_calls`, `<name>_nanoseconds`,
`<name>_max_nanoseconds`), local storage (`local_*`), S3 requests and bytes (`s3_*`), remote reads
and caches (`remote_*`), exports (`export_*`) and shared databases (`shared_*`). SDK and cache
counters are process-wide. Group commit: `local_sync_requests` (durable syncs),
`local_sync_transactions` (transactions that committed them), `local_sync_batch_max` (most syncs in
one transaction) and `local_sync_batch_fallbacks` (batches retried one sync at a time). Every timing
also reports its distribution (`<name>_stddev_nanoseconds`, `_p50_`, `_p95_` and `_p99_nanoseconds`), and
each S3 operation type is timed on its own (`s3_request_get`, `s3_request_range_get`, `s3_request_put`,
`s3_request_conditional_put`, `s3_request_multipart_part`, `s3_request_multipart_control`,
`s3_request_head`, `s3_request_list`, `s3_request_delete`), as are both halves of a durable publication
(`local_segment_sync`, `local_metadata_commit`). WAL shipping: `ship_epochs`, `ship_epoch_bytes`,
`ship_chains_started`, `ship_rounds`, `ship_failures`; followers: `follow_epochs_applied`. Segments:
`local_journal_bytes` (active segments), `local_segment_bytes` (retired ones), `local_segments`,
`local_background_publications`, `local_segment_bytes_compacted` and `local_segments_deleted`. The [SQL API](api.md#introspection) describes the timing names.

```sql
-- main
SELECT metric, value FROM differential_storage_stats() WHERE metric = 'local_payload_bytes';
```

### differential_status

```sql
differential_status()
```

`capability VARCHAR`, `status VARCHAR`: implementation scope, durability profiles, limits, whether
test hooks are compiled in, the last background publication error (`shared_publication`), and how
WAL shipping (`wal_shipping`) and followers (`followers`) work.
Does not open the repository.

```sql
-- main
SELECT capability, status FROM differential_status();
```

### differential_remote_heads

```sql
differential_remote_heads(location VARCHAR, <S3 option parameters>, secret => VARCHAR)
```

Every branch head exported to an S3 location (`s3://bucket/prefix/`), read from S3 at execution.
Takes the [S3 option parameters](#s3-option-parameters) except `data_path`.

| Column | Type | Description |
| --- | --- | --- |
| `database_name`, `database_id` | VARCHAR | The database. |
| `branch_name`, `branch_id` | VARCHAR | The branch. |
| `snapshot_id` | BIGINT | Its latest exported snapshot. |
| `snapshot_time` | TIMESTAMPTZ | Its publication time. |
| `export_locator` | VARCHAR | `descriptors/<sha256>`. |
| `head` | VARCHAR | Attachable path: `s3://…/heads/<database>/<uuid>/<branch>`. |

```sql
-- main
SELECT database_name, branch_name, snapshot_id FROM differential_remote_heads('s3://my-bucket/lake/');
```

### differential_writers

```sql
differential_writers(database VARCHAR)
```

A shared database's "being written" markers in S3, one per writable attachment (empty for other
databases). Read from S3 at execution.

| Column | Type | Description |
| --- | --- | --- |
| `branch_name` | VARCHAR | Branch being written. |
| `writer_id` | VARCHAR | Random per writer session. |
| `host`, `repository_id` | VARCHAR | Where the writer runs. |
| `process_id` | BIGINT | Its process. |
| `started_at`, `renewed_at` | TIMESTAMPTZ | When the session started and last renewed its marker. |
| `live` | BOOLEAN | Renewed within the last 2 minutes (by the reader's clock). |
| `this_process` | BOOLEAN | One of this process's sessions. |

```sql
-- main
SELECT branch_name, host, live FROM differential_writers('team');
```

### differential_followers

```sql
differential_followers()
```

This process's [followers](#followers). Reads no S3.

| Column | Type | Description |
| --- | --- | --- |
| `alias` | VARCHAR | The follower's attachment. |
| `database_name`, `branch_name` | VARCHAR | What it follows. |
| `follows` | VARCHAR | `commits` (the branch ships its WAL) or `snapshots` (exported snapshots). |
| `follow_interval_ms` | BIGINT | Time between checks. |
| `chain`, `epoch` | BIGINT | `commits`: the shipped state it reads. |
| `snapshot_id` | BIGINT | `snapshots`: the snapshot it reads. |
| `state_time` | TIMESTAMPTZ | When that state was shipped or published. |
| `checked_at` | TIMESTAMPTZ | Its last check of S3. |
| `polls`, `changes` | BIGINT | Checks, and newer states opened. |
| `error` | VARCHAR | The last failed check, NULL after a success. |

```sql
-- main
SELECT alias, follows, epoch, checked_at FROM differential_followers();
```

## Snapshot and branch functions

### differential_create_snapshot

```sql
differential_create_snapshot(database VARCHAR, name => VARCHAR, author => VARCHAR, message => VARCHAR,
                             extra_info => VARCHAR, export_mode => VARCHAR, request_id => VARCHAR)
```

Checkpoints an attached writable branch and publishes its state as an immutable snapshot. With an
S3 data path it also exports it; for a shared database it then moves the S3 branch head (or
reports `branch_conflict`).

| Parameter | Type | Default | Description |
| --- | --- | --- | --- |
| `database` | VARCHAR | | Alias of a writable branch attachment. |
| `name` | VARCHAR | | Snapshot name: 1–128 letters, digits, `_` or `-`; unique in the database (also after expiry). |
| `author` | VARCHAR | | Commit author (up to 1 KiB). |
| `message` | VARCHAR | | Commit message (up to 64 KiB). |
| `extra_info` | VARCHAR | | Free-form metadata, for example JSON (up to 64 KiB). |
| `export_mode` | VARCHAR | `'sync'` | `'deferred'` returns after the local snapshot; the export stays `pending`. |
| `request_id` | VARCHAR | | See [conventions](#conventions). |

Returns the [operation result](#operation-result-columns). For a shared database, `snapshot_id` is
the final number (renumbered if another writer took the next one).

```sql
-- main
ATTACH 'differential:sales' AS sales;
UPDATE sales.t SET v = 'v3';
SELECT snapshot_id FROM differential_create_snapshot('sales', name => 'v3', author => 'ada',
    message => 'third version', extra_info => '{"ticket": 42}', request_id => 'sales-v3');
```

### differential_create_branch

```sql
differential_create_branch(database VARCHAR, name VARCHAR, snapshot => BIGINT, branch => VARCHAR,
                           request_id => VARCHAR)
```

Creates a writable branch from a snapshot, sharing all data with it. In a shared database, the
branch's S3 head is created at once (the name must be free in S3).

| Parameter | Type | Default | Description |
| --- | --- | --- | --- |
| `database` | VARCHAR | | Database name or alias. |
| `name` | VARCHAR | | New branch name. |
| `snapshot` | BIGINT | latest snapshot of `branch` | Snapshot to start from (another writer's, in a shared database). |
| `branch` | VARCHAR | the attached branch, else `main` | Branch whose latest snapshot is used when `snapshot` is not given. |
| `request_id` | VARCHAR | | See [conventions](#conventions). |

Returns the [operation result](#operation-result-columns) (`snapshot_id` is the starting snapshot).

```sql
-- main
CALL differential_create_branch('sales', 'hotfix', snapshot => 1);
CALL differential_create_branch('sales', 'from-dev', branch => 'dev');
```

### differential_clone_database

```sql
differential_clone_database(database VARCHAR, name VARCHAR, snapshot => BIGINT, branch => VARCHAR,
                            export_mode => VARCHAR, request_id => VARCHAR)
```

Creates a new, independent database (new UUID, snapshots from 1) from a snapshot, on the same data
path and sharing all data. A clone of a shared database is a single-writer database.

| Parameter | Type | Default | Description |
| --- | --- | --- | --- |
| `database` | VARCHAR | | Source database name or alias. |
| `name` | VARCHAR | | New database name. |
| `snapshot` | BIGINT | latest snapshot of `branch` | Snapshot to clone. |
| `branch` | VARCHAR | the attached branch, else `main` | Branch whose latest snapshot is used. |
| `export_mode` | VARCHAR | `'sync'` | `'deferred'`: do not wait for the clone's first export. |
| `request_id` | VARCHAR | | See [conventions](#conventions). |

Returns the [operation result](#operation-result-columns).

```sql
-- main
SELECT database_id FROM differential_clone_database('sales', 'sales_copy', snapshot => 2);
```

### differential_restore

```sql
differential_restore(database VARCHAR, snapshot_id BIGINT, branch => VARCHAR, export_mode => VARCHAR,
                     request_id => VARCHAR)
```

Resets a detached branch to a snapshot. Its current contents are first preserved as a snapshot
(`preserved before restore`) unless an existing snapshot holds them; then the restored state is
published as a new snapshot (`restored to snapshot N`).

| Parameter | Type | Default | Description |
| --- | --- | --- | --- |
| `database` | VARCHAR | | Database name or alias. |
| `snapshot_id` | BIGINT | | Snapshot to restore (another writer's, in a shared database). |
| `branch` | VARCHAR | the attached branch, else `main` | Branch to reset; must not be attached. |
| `export_mode` | VARCHAR | `'sync'` | `'deferred'`: do not wait for the exports. |
| `request_id` | VARCHAR | | See [conventions](#conventions). |

Returns the [operation result](#operation-result-columns).

```sql
-- main
SELECT snapshot_id, preservation_snapshot_id FROM differential_restore('sales', 1, branch => 'dev');
```

### differential_drop_branch

```sql
differential_drop_branch(database VARCHAR, branch VARCHAR, export_mode => VARCHAR, request_id => VARCHAR)
```

Preserves a detached branch's contents as a snapshot (unless one holds them) and removes the
branch; its name can be reused. The last active branch cannot be dropped. With an S3 data path its
S3 head becomes a tombstone, so remote readers no longer find it and remote collection can reclaim
its data once its snapshots expire (with `export_mode => 'deferred'`, when the export is retried).
In a shared database the S3 head must still be this repository's last version; the tombstone then
drops the branch for every writer.

| Parameter | Type | Default | Description |
| --- | --- | --- | --- |
| `database` | VARCHAR | | Database name or alias. |
| `branch` | VARCHAR | | Branch to drop; must not be attached. |
| `export_mode` | VARCHAR | `'sync'` | `'deferred'`: do not wait for the preservation snapshot's export. |
| `request_id` | VARCHAR | | See [conventions](#conventions). |

Returns the [operation result](#operation-result-columns).

```sql
-- main
CALL differential_drop_branch('sales', 'from-dev');
```

### differential_expire_snapshots

```sql
differential_expire_snapshots(database VARCHAR, versions => BIGINT[], older_than => TIMESTAMPTZ,
                              dry_run => BOOLEAN, request_id => VARCHAR)
```

Retires snapshots: they can no longer be attached, named or found by time, and their data becomes
collectible once nothing else needs it. Open attachments keep reading them. Exported snapshots are
also recorded as expired in S3, so
[differential_cleanup_remote_files](#differential_cleanup_remote_files) can reclaim what only they
reference. In a shared database, published snapshots expire for every writer, and a snapshot that
is a branch head in S3 cannot expire.

| Parameter | Type | Default | Description |
| --- | --- | --- | --- |
| `database` | VARCHAR | | Database name or alias. |
| `versions` | BIGINT[] | | Snapshots to expire (always selected). |
| `older_than` | TIMESTAMPTZ | | Also select non-retired snapshots published before this time. At least one of `versions` and `older_than` is required. |
| `dry_run` | BOOLEAN | `false` | Only list the selection. |
| `request_id` | VARCHAR | | See [conventions](#conventions); per snapshot `<request_id>/<id>`. |

| Column | Type | Description |
| --- | --- | --- |
| `snapshot_id` | BIGINT | Selected snapshot. |
| `snapshot_time` | TIMESTAMPTZ | Its publication time. |
| `snapshot_name` | VARCHAR | Its name. |
| `dry_run` | BOOLEAN | Whether this was a dry run. |
| `operation_id` | VARCHAR | Operation that expired it (NULL for a dry run or another writer's snapshot). |
| `export_locator` | VARCHAR | Its export, which is kept. |

```sql
-- main
SELECT snapshot_id FROM differential_expire_snapshots('sales', older_than => TIMESTAMPTZ '2000-01-01', dry_run => true);
SELECT snapshot_id FROM differential_expire_snapshots('sales', versions => [1]);
```

## S3 functions

### differential_retry_export

```sql
differential_retry_export(database VARCHAR [, snapshot_id BIGINT], request_id => VARCHAR)
```

Finishes pending S3 exports: all of them (lowest first), or one. Allocates no snapshot number, works
for retired snapshots and dropped branches, and republishes the database's branch heads (shared
databases: except diverged branches).

| Parameter | Type | Default | Description |
| --- | --- | --- | --- |
| `database` | VARCHAR | | Database name or alias. |
| `snapshot_id` | BIGINT | all pending | One snapshot to export (positional). |
| `request_id` | VARCHAR | | See [conventions](#conventions). |

Returns one row per exported snapshot: `snapshot_id BIGINT` (its final number), `operation_id
VARCHAR`, `export_state VARCHAR` (`complete`), `export_locator VARCHAR`.

```sql
-- main
ATTACH 'differential:lake_sales' AS lake;
INSERT INTO lake.events SELECT range FROM range(1000, 1100);
SELECT snapshot_id, export_state FROM differential_create_snapshot('lake', export_mode => 'deferred');
DETACH lake;
SELECT snapshot_id, export_state FROM differential_retry_export('lake_sales');
```

### differential_evict_snapshots

```sql
differential_evict_snapshots(database VARCHAR, versions => BIGINT[], older_than => TIMESTAMPTZ,
                             dry_run => BOOLEAN, request_id => VARCHAR)
```

Drops the local copy of snapshots whose S3 export completed. They stay attachable and are read
through S3; branching or restoring from them downloads them again. Parameters and result columns
as for [differential_expire_snapshots](#differential_expire_snapshots); `older_than` selects only
snapshots with a completed export that are still local.

```sql
-- main
SELECT snapshot_id FROM differential_evict_snapshots('lake_sales', versions => [1]);
```

### differential_claim_branch_heads

```sql
differential_claim_branch_heads(database VARCHAR, request_id => VARCHAR)
```

Takes a single-writer database's S3 branch heads over, after a copy of the repository (a restored
backup, a cloned VM) wrote them: pending exports whose numbers the other copy published are
abandoned, new snapshots are numbered above the newest published one, and every head is rewritten.
Refused for shared databases. Returns `branch_name VARCHAR`, `snapshot_id BIGINT` and `head
VARCHAR` (the attachable path) per branch.

```sql
-- main
SELECT branch_name, snapshot_id FROM differential_claim_branch_heads('lake_sales');
```

### differential_resolve_conflict

```sql
differential_resolve_conflict(database VARCHAR, keep => VARCHAR, branch => VARCHAR, name => VARCHAR,
                              request_id => VARCHAR)
```

Shared databases: reconciles a detached branch with its S3 head, typically after
`branch_conflict`. The branch's current contents are first preserved as a snapshot (including
changes not yet in one) unless its latest snapshot holds them. Then the branch becomes its S3 head
again (or, if the branch was dropped in S3, is dropped here).

| Parameter | Type | Default | Description |
| --- | --- | --- | --- |
| `database` | VARCHAR | | Database name or alias. |
| `keep` | VARCHAR | (required) | `'branch'`: publish the preserved work as a new branch; `'remote'`: keep it only as a local snapshot. |
| `branch` | VARCHAR | the attached branch, else `main` | Branch to reconcile; must not be attached. |
| `name` | VARCHAR | `<branch>-conflict-<snapshot>` | Name of the new branch (`keep => 'branch'`). |
| `request_id` | VARCHAR | | See [conventions](#conventions). |

| Column | Type | Description |
| --- | --- | --- |
| `operation_id` | VARCHAR | The operation record. |
| `branch_name` | VARCHAR | The reconciled branch. |
| `remote_snapshot_id` | BIGINT | The S3 head it now follows (0 if it was dropped). |
| `kept_snapshot_id` | BIGINT | The snapshot holding the branch's previous contents (NULL if none). |
| `kept_branch` | VARCHAR | The new branch (`keep => 'branch'`, when there was local work). |

```sql
-- main
SELECT branch_name, remote_snapshot_id, kept_snapshot_id, kept_branch
FROM differential_resolve_conflict('team', keep => 'remote', branch => 'main');
```

## Import and recovery functions

### differential_import_database

```sql
differential_import_database(source_path VARCHAR, name VARCHAR, <S3 option parameters>, request_id => VARCHAR)
```

Copies an offline, checkpoint-consistent native DuckDB file into a new database, verifies the copy
and publishes it as snapshot 1. Files with WAL sidecars, or that change during the copy, are
refused.

| Parameter | Type | Default | Description |
| --- | --- | --- | --- |
| `source_path` | VARCHAR | | Absolute path of the native file. |
| `name` | VARCHAR | | New database name. |
| `data_path` and S3 options | | the metadata store | The new database's data path ([S3 option parameters](#s3-option-parameters)). |
| `request_id` | VARCHAR | | See [conventions](#conventions). |

Returns the [operation result](#operation-result-columns).

```sql
-- main
ATTACH '/data/native.duckdb' AS native;                    -- an ordinary DuckDB file
CREATE TABLE native.t AS SELECT 1 AS x;
DETACH native;
SELECT database_id FROM differential_import_database('/data/native.duckdb', 'imported');
```

### differential_recover_export

```sql
differential_recover_export(location VARCHAR, name VARCHAR, <S3 option parameters>, request_id => VARCHAR)
```

Copies an exported snapshot (`s3://bucket/prefix/descriptors/<sha256>`) into a new writable
database with a new identity, verifying every chunk. The S3 options describe the source location;
`data_path` chooses the new database's storage (default: the metadata store), and the source's S3
options apply to it as defaults.

| Parameter | Type | Default | Description |
| --- | --- | --- | --- |
| `location` | VARCHAR | | Export locator: `s3://bucket/prefix/descriptors/<sha256>`. |
| `name` | VARCHAR | | New database name. |
| `data_path` and S3 options | | the metadata store | See above and [S3 option parameters](#s3-option-parameters). |
| `request_id` | VARCHAR | | See [conventions](#conventions). |

Returns the [operation result](#operation-result-columns).

```sql
-- not run: needs a real export locator
SELECT database_id FROM differential_recover_export('s3://my-bucket/lake/descriptors/<sha256>', 'restored',
    data_path => 's3://my-bucket/lake/');
```

### differential_recover_branch

```sql
differential_recover_branch(location VARCHAR, name VARCHAR, <S3 option parameters>, same_identity => BOOLEAN,
                            request_id => VARCHAR)
```

Recovers the newest shipped state of a branch ([WAL shipping](#wal-shipping)) from
`s3://bucket/prefix/heads/<database>[/<database uuid>][/<branch>]` into a new local database `name`:
the newest chain's base is imported without downloading (read from S3 on demand) and every
consecutive, verified epoch is applied. DuckDB replays the WAL at the first ATTACH. Takes the
[S3 option parameters](#s3-option-parameters) except `data_path`; the new database uses the same S3
location.

| Parameter | Type | Default | Description |
| --- | --- | --- | --- |
| `location` | VARCHAR | | Branch path: `s3://bucket/prefix/heads/<database>[/<uuid>][/<branch>]` (branch `main` by default). |
| `name` | VARCHAR | | New database name. |
| `same_identity` | BOOLEAN | `false` | Keep the original database and branch identities: a failover of the writer. Turn shipping on again with `SHIP_WAL`; it continues in a newer chain. A repository still shipping the branch then gets `head_conflict`. |
| S3 options | | | See [S3 option parameters](#s3-option-parameters). |
| `request_id` | VARCHAR | | A repeated request returns the database recovered under `name`. |

| Column | Type | Description |
| --- | --- | --- |
| `database_id`, `database_name` | VARCHAR | The recovered database. |
| `branch_id`, `branch_name` | VARCHAR | Its branch. |
| `shipped_chain`, `shipped_epoch` | BIGINT | The newest epoch applied. |
| `uri` | VARCHAR | Internal URI of the branch. |

```sql
-- recovery
SELECT database_name, shipped_epoch FROM differential_recover_branch('s3://my-bucket/lake/heads/orders/main',
    'orders_recovered');
ATTACH 'differential:orders_recovered' AS recovered;
SELECT count(*) AS n FROM recovered.t;
-- expect output: "n":102
```

## Maintenance functions

### differential_cleanup_old_files

```sql
differential_cleanup_old_files(dry_run => BOOLEAN, cleanup_all => BOOLEAN, max_objects => BIGINT,
                               request_id => VARCHAR)
```

Collects local data that nothing references any more (expired snapshots, dropped branches,
replaced chunks). Runs automatically in small steps after checkpoints (`differential_auto_gc`).
S3 data is collected separately by [differential_cleanup_remote_files](#differential_cleanup_remote_files).

| Parameter | Type | Default | Description |
| --- | --- | --- | --- |
| `dry_run` | BOOLEAN | `false` | Count exactly, change nothing (no locks). |
| `cleanup_all` | BOOLEAN | `false` | Repeat steps until nothing is queued (one row per step). |
| `max_objects` | BIGINT | `1000` | Work units per step, 1–10000. |
| `request_id` | VARCHAR | | See [conventions](#conventions); per step `<request_id>/<n>`. |

| Column | Type | Description |
| --- | --- | --- |
| `operation_id` | VARCHAR | The step's operation (NULL for a dry run). |
| `dry_run` | BOOLEAN | Whether this was a dry run. |
| `candidate_objects` | BIGINT | Objects deleted plus still queued. |
| `candidate_bytes` | BIGINT | Payload bytes of the deleted objects. |
| `deleted_objects` | BIGINT | Objects deleted. |
| `live_pins`, `stale_pins` | BIGINT | Reader pins of live and of dead processes. |

```sql
-- main
SELECT candidate_objects FROM differential_cleanup_old_files(dry_run => true);
SELECT deleted_objects FROM differential_cleanup_old_files(cleanup_all => true, max_objects => 1000);
```

### differential_delete_orphaned_files

```sql
differential_delete_orphaned_files(dry_run => BOOLEAN, max_objects => BIGINT, request_id => VARCHAR)
```

Removes files in local pools that no mapping references (left by crashes between a file write and
its commit).

| Parameter | Type | Default | Description |
| --- | --- | --- | --- |
| `dry_run` | BOOLEAN | `false` | Only count. |
| `max_objects` | BIGINT | `1000` | Files examined, 1–10000. |
| `request_id` | VARCHAR | | See [conventions](#conventions). |

Returns `dry_run BOOLEAN` and `orphaned_files BIGINT`.

```sql
-- main
SELECT orphaned_files FROM differential_delete_orphaned_files(dry_run => true, max_objects => 1000);
```

### differential_compact

```sql
differential_compact(max_pages => BIGINT, request_id => VARCHAR)
```

Returns free pages of the metadata store to the file system (SQLite incremental vacuum). Roots and
pins are preserved.

| Parameter | Type | Default | Description |
| --- | --- | --- | --- |
| `max_pages` | BIGINT | `1000` | Pages to release, 1–10000. |
| `request_id` | VARCHAR | | See [conventions](#conventions). |

| Column | Type | Description |
| --- | --- | --- |
| `operation_id` | VARCHAR | The operation record. |
| `free_pages_before`, `free_bytes_before` | BIGINT | Free space before. |
| `pages_reclaimed`, `bytes_reclaimed` | BIGINT | Space returned. |
| `manifest_roots` | BIGINT | Images stored. |
| `roots_unchanged` | BOOLEAN | Always true: compaction never changes images. |
| `payload_bytes` | BIGINT | Data bytes stored locally. |
| `export_retained_bytes` | BIGINT | Bytes kept for pending S3 exports. |

```sql
-- main
SELECT pages_reclaimed FROM differential_compact(max_pages => 1000);
```

### differential_cleanup_remote_files

```sql
differential_cleanup_remote_files(location VARCHAR, older_than => TIMESTAMPTZ, dry_run => BOOLEAN,
                                  <S3 options>, request_id => VARCHAR)
```

Garbage-collects an S3 location: every database exported there, by any repository. Never runs
automatically. Data is reachable from branch heads and from snapshots without an expiry record
(see [differential_expire_snapshots](#differential_expire_snapshots)), and from this repository's
retained exports and objects it reads only from S3. Collection works in two phases, so it never
races an export:
1. unreachable packs, index pages and descriptors last modified before `older_than` are
   *condemned* (recorded under `gc/condemned/`); exports stop referencing them at once;
2. a later run deletes what was condemned before `older_than` and is still unreachable.

So data is deleted one grace period after it became unreachable at the earliest: run it
regularly, for example weekly. Repositories whose shared-database branches read their base
snapshot from S3 copy condemned data locally when they next attach or publish; one that stays
offline for longer than the grace period can lose such data.

| Parameter | Type | Default | Description |
| --- | --- | --- | --- |
| `older_than` | TIMESTAMPTZ | a week ago | Grace cutoff for both phases. |
| `dry_run` | BOOLEAN | `false` | Count only: no record, no deletion. |
| S3 options | | | See [S3 option parameters](#s3-option-parameters) (no `data_path`). |

| Column | Type | Description |
| --- | --- | --- |
| `dry_run` | BOOLEAN | Whether this was a dry run. |
| `roots` | BIGINT | Descriptors that keep data alive. |
| `live_objects` | BIGINT | Reachable packs, index pages and descriptors. |
| `unreachable_objects`, `unreachable_bytes` | BIGINT | Unreachable objects older than `older_than`. |
| `condemned_objects` | BIGINT | Objects condemned by this run. |
| `deleted_objects`, `deleted_bytes` | BIGINT | Objects deleted by this run. |

```sql
-- main
SELECT unreachable_objects, deleted_bytes FROM differential_cleanup_remote_files('s3://my-bucket/lake/',
    dry_run => true);
```

### differential_cleanup_metadata

```sql
differential_cleanup_metadata(older_than => TIMESTAMPTZ, dry_run => BOOLEAN, request_id => VARCHAR)
```

Deletes metadata the repository no longer needs: completed operation records created before
`older_than` (their request IDs no longer replay) and history entries of expired snapshots that
took effect before it. Pending operations are kept.

| Parameter | Type | Default | Description |
| --- | --- | --- | --- |
| `older_than` | TIMESTAMPTZ | (required) | Cutoff. |
| `dry_run` | BOOLEAN | `false` | Count only. |

Returns one row per table: `dry_run BOOLEAN`, `table_name VARCHAR` (`operations`, `history`) and
`rows BIGINT`.

```sql
-- main
SELECT table_name, rows FROM differential_cleanup_metadata(older_than => now() - INTERVAL 30 DAY,
    dry_run => true);
```
