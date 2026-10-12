# SQL reference

Every statement, ATTACH option, setting and function of the extension, each with an example.
Semantics, result columns and rules are in the [SQL API](api.md); task-oriented walkthroughs
are the [lifecycle guide](lifecycle.md) (one host) and [shared databases](shared-databases.md)
(several writers).

The examples build on each other: run them in order. Each block is one session. The bucket
`s3://my-bucket/lake/` stands for a prefix you own; S3 credentials come from a `TYPE s3` secret
(see [Secrets](api.md#secrets)). The blocks are executed as written by the test suite (against
MinIO, `test/integration/doc_examples.py`), except those marked "not run".

Contents: [settings](#settings) · [ATTACH](#attach) · [AT](#at-time-travel) ·
[catalog functions](#catalog-functions) · [secrets](#create-secret-type-differential) ·
[introspection](#introspection-functions) · [lifecycle](#lifecycle-functions) ·
[S3 functions](#s3-functions) · [shared databases](#shared-database-functions) ·
[import and recovery](#import-and-recovery) · [maintenance](#maintenance-functions)

## Settings

| Setting | Default | Example |
| --- | --- | --- |
| `differential_repository` | `$DIFFERENTIAL_STORE_ROOT` or `~/.duckdb/differential` | `SET differential_repository = '/absolute/repository';` |
| `differential_repository_shared` | `false` | `SET differential_repository_shared = true;` (before the repository is created) |
| `differential_auto_gc` | `true` | `SET differential_auto_gc = false;` |
| `differential_snapshot_on_checkpoint` | `false` | `SET differential_snapshot_on_checkpoint = true;` |
| `differential_remote_cache_size` | `0` (off) | `SET differential_remote_cache_size = '4GB';` |
| `differential_s3_max_requests` | `16` | `SET differential_s3_max_requests = 32;` |

```sql
-- main
SET differential_repository = '/absolute/repository';
SET differential_auto_gc = true;
SET differential_snapshot_on_checkpoint = false;
SET differential_remote_cache_size = '1GB';
SET differential_s3_max_requests = 16;
SELECT current_setting('differential_s3_max_requests') AS max_requests;
```

## ATTACH

```sql
ATTACH 'differential:<database>' [AS alias] [(option value, ...)]
ATTACH 'differential:s3://<bucket>/<prefix>/descriptors/<sha256>' [AS alias] [(S3 options)]
ATTACH 'differential:s3://<bucket>/<prefix>/heads/<database>[/<database uuid>][/<branch>]' [AS alias] [(S3 options)]
ATTACH 'diff://db/<database uuid>/{branches|snapshots}/<id>' [AS alias] (TYPE differential)
```

| Option | Example |
| --- | --- |
| `BRANCH 'name'` | `ATTACH 'differential:sales' AS dev (BRANCH 'dev')` |
| `SNAPSHOT_VERSION n` | `ATTACH 'differential:sales' AS s1 (SNAPSHOT_VERSION 1)` |
| `SNAPSHOT_NAME 'name'` | `ATTACH 'differential:sales' AS v1 (SNAPSHOT_NAME 'v1')` |
| `SNAPSHOT_TIME 'timestamp'` | `ATTACH 'differential:sales' AS by_time (SNAPSHOT_TIME '2026-01-01 12:00:00')` |
| `READ_ONLY` | `ATTACH 'differential:sales' AS latest (READ_ONLY)` |
| `CREATE_IF_NOT_EXISTS bool` | `ATTACH 'differential:sales' AS sales (CREATE_IF_NOT_EXISTS false)` |
| `DATA_PATH 'path'` | `ATTACH 'differential:lake_sales' AS lake (DATA_PATH 's3://my-bucket/lake/')` |
| `SECRET name` | `ATTACH 'differential:metrics' AS metrics (SECRET lake_settings)` |
| `REGION`, `ENDPOINT`, `CREDENTIAL_PROFILE`, `PATH_STYLE`, `ALLOW_HTTP` | `… (DATA_PATH 's3://…', REGION 'eu-central-1', CREDENTIAL_PROFILE 'analytics')` |
| `MAX_REQUESTS`, `RETRIES`, `CONNECT_TIMEOUT_MS`, `REQUEST_TIMEOUT_MS`, `MAX_RANGE_BYTES`, `MULTIPART_THRESHOLD` | `… (DATA_PATH 's3://…', MAX_REQUESTS 8, RETRIES 3)` |
| `WRITERS 'single' \| 'shared'` | `… (DATA_PATH 's3://…', WRITERS 'shared')` |
| `ON_CONFLICT 'fail' \| 'branch'` | `ATTACH 'differential:team' AS team (ON_CONFLICT 'branch')` |
| `ON_BUSY 'warn' \| 'error' \| 'ignore'` | `ATTACH 'differential:team' AS team (ON_BUSY 'error')` |
| `PUBLISH_ON_CHECKPOINT bool` | `ATTACH 'differential:team' AS team (PUBLISH_ON_CHECKPOINT false)` |
| `PUBLISH_ON_DETACH bool` | `ATTACH 'differential:team' AS team (PUBLISH_ON_DETACH true)` |
| `TYPE differential` | `ATTACH 'diff://db/<uuid>/snapshots/3' AS raw (TYPE differential)` |

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

Shared databases ([guide](shared-databases.md)):

```sql
-- main
ATTACH 'differential:team' AS team (DATA_PATH 's3://my-bucket/lake/', WRITERS 'shared',
    ON_CONFLICT 'branch', ON_BUSY 'warn', PUBLISH_ON_CHECKPOINT true, PUBLISH_ON_DETACH true);
CREATE TABLE team.t AS SELECT range AS id FROM range(100);
CALL differential_create_snapshot('team');
```

## AT (time travel)

```sql
-- main
ATTACH 'differential:sales' AS sales;
SELECT v FROM sales.t AT (VERSION => 1) LIMIT 1;
SELECT v FROM sales.t AT (TIMESTAMP => TIMESTAMP '2100-01-01 00:00:00') LIMIT 1;
```

## Catalog functions

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

A `TYPE differential` secret holds `DATA_PATH` and S3 options for new databases (no credentials):

```sql
-- main
CREATE SECRET lake_settings (TYPE differential, DATA_PATH 's3://my-bucket/lake/', MAX_REQUESTS 8);
ATTACH 'differential:metrics' AS metrics (SECRET lake_settings);
SELECT data_path FROM differential_databases() WHERE database_name = 'metrics';
SELECT count(*) AS heads FROM differential_remote_heads('s3://my-bucket/lake/', secret => 'lake_settings');
```

## Introspection functions

| Function | Returns |
| --- | --- |
| `differential_snapshots(db)` | Every snapshot (other writers' too, for shared databases) |
| `differential_current_snapshot(db)` | The attached or latest snapshot ID |
| `differential_branches(db)` | Branches, with `remote_state` for shared databases |
| `differential_databases()` | Databases, data paths and writer modes |
| `differential_operations()` | Durable operation records (request IDs) |
| `differential_providers()` | S3 locations and local pools, with the credential source |
| `differential_retention_roots()` | Why each stored root is kept |
| `differential_storage_stats()` | Counters and timings |
| `differential_status()` | Capabilities and limits |
| `differential_remote_heads(location, S3 options, secret =>)` | Branch heads in an S3 location |
| `differential_writers(db)` | "Being written" markers of a shared database |

```sql
-- main
SELECT snapshot_id, snapshot_name, export_state FROM differential_snapshots('sales');
SELECT id FROM differential_current_snapshot('sales');
SELECT branch_name, head_snapshot_id, remote_state FROM differential_branches('sales');
SELECT database_name, data_path, writers FROM differential_databases();
SELECT kind, state FROM differential_operations();
SELECT data_path, credential_source FROM differential_providers();
SELECT reason, identity FROM differential_retention_roots();
SELECT metric, value FROM differential_storage_stats() WHERE metric = 'local_payload_bytes';
SELECT capability, status FROM differential_status();
SELECT database_name, branch_name, snapshot_id FROM differential_remote_heads('s3://my-bucket/lake/');
SELECT branch_name, host, live FROM differential_writers('team');
```

## Lifecycle functions

Every mutation accepts `request_id => '…'`: repeating a call with the same ID returns the original
result and never applies twice.

| Function | Example |
| --- | --- |
| `differential_create_snapshot(db, name =>, author =>, message =>, extra_info =>, export_mode =>)` | below |
| `differential_create_branch(db, name, snapshot =>, branch =>)` | below |
| `differential_clone_database(db, name, snapshot =>, branch =>, export_mode =>)` | below |
| `differential_restore(db, snapshot_id, branch =>, export_mode =>)` | below |
| `differential_drop_branch(db, branch, export_mode =>)` | below |
| `differential_expire_snapshots(db, versions =>, older_than =>, dry_run =>)` | below |

```sql
-- main
ATTACH 'differential:sales' AS sales;
UPDATE sales.t SET v = 'v3';
SELECT snapshot_id FROM differential_create_snapshot('sales', name => 'v3', author => 'ada',
    message => 'third version', extra_info => '{"ticket": 42}', request_id => 'sales-v3');
DETACH sales;
CALL differential_create_branch('sales', 'hotfix', snapshot => 1);
CALL differential_create_branch('sales', 'from-dev', branch => 'dev');
SELECT database_id FROM differential_clone_database('sales', 'sales_copy', snapshot => 2);
SELECT snapshot_id, preservation_snapshot_id FROM differential_restore('sales', 1, branch => 'dev');
CALL differential_drop_branch('sales', 'from-dev');
SELECT snapshot_id FROM differential_expire_snapshots('sales', older_than => TIMESTAMPTZ '2000-01-01', dry_run => true);
SELECT snapshot_id FROM differential_expire_snapshots('sales', versions => [1]);
```

## S3 functions

| Function | Example |
| --- | --- |
| `differential_retry_export(db [, snapshot_id])` | below |
| `differential_evict_snapshots(db, versions =>, older_than =>, dry_run =>)` | below |
| `differential_claim_branch_heads(db)` | below |
| `differential_remote_heads(location, S3 options, secret =>)` | [introspection](#introspection-functions) |

```sql
-- main
ATTACH 'differential:lake_sales' AS lake;
INSERT INTO lake.events SELECT range FROM range(1000, 1100);
SELECT snapshot_id, export_state FROM differential_create_snapshot('lake', export_mode => 'deferred');
DETACH lake;
SELECT snapshot_id, export_state FROM differential_retry_export('lake_sales');
SELECT snapshot_id FROM differential_evict_snapshots('lake_sales', versions => [1]);
SELECT branch_name, snapshot_id FROM differential_claim_branch_heads('lake_sales');
```

## Shared database functions

| Function | Example |
| --- | --- |
| `differential_resolve_conflict(db, keep => 'branch' \| 'remote', branch =>, name =>)` | below; see [conflicts](shared-databases.md#8-when-two-hosts-change-the-same-branch) |
| `differential_writers(db)` | [introspection](#introspection-functions) |

```sql
-- main
SELECT branch_name, remote_snapshot_id, kept_snapshot_id, kept_branch
FROM differential_resolve_conflict('team', keep => 'remote', branch => 'main');
```

## Import and recovery

| Function | Example |
| --- | --- |
| `differential_import_database(path, name, data_path =>, S3 options, secret =>)` | below |
| `differential_recover_export('s3://…/descriptors/<sha256>', name, data_path =>, S3 options, secret =>)` | below (not run) |

```sql
-- main
ATTACH '/data/native.duckdb' AS native;                    -- an ordinary DuckDB file
CREATE TABLE native.t AS SELECT 1 AS x;
DETACH native;
SELECT database_id FROM differential_import_database('/data/native.duckdb', 'imported');
```

```sql
-- not run: needs a real export locator
SELECT database_id FROM differential_recover_export('s3://my-bucket/lake/descriptors/<sha256>', 'restored',
    data_path => 's3://my-bucket/lake/');
```

## Maintenance functions

| Function | Example |
| --- | --- |
| `differential_cleanup_old_files(dry_run =>, cleanup_all =>, max_objects =>)` | below |
| `differential_delete_orphaned_files(dry_run =>, max_objects =>)` | below |
| `differential_compact(max_pages =>)` | below |

```sql
-- main
SELECT candidate_objects FROM differential_cleanup_old_files(dry_run => true);
SELECT deleted_objects FROM differential_cleanup_old_files(cleanup_all => true, max_objects => 1000);
SELECT orphaned_files FROM differential_delete_orphaned_files(dry_run => true, max_objects => 1000);
SELECT pages_reclaimed FROM differential_compact(max_pages => 1000);
```
