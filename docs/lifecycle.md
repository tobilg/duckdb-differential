# Usage lifecycle

This guide follows one database from creation to cleanup. The local sections form one
continuous session: run them in order in a single DuckDB process, and the IDs shown are the
ones you get. The S3 section needs an existing bucket. Function and option details are in the
[SQL API](api.md).

| Stage | What you do | Main SQL |
| --- | --- | --- |
| [1. Set up](#1-set-up) | Install and load the extension, choose a repository | `INSTALL`, `LOAD`, `SET differential_repository` |
| [2. Create](#2-create-a-database) | Create a database by attaching it, or import a native file | `ATTACH 'differential:<db>'`, `differential_import_database` |
| [3. Work](#3-work-with-a-live-branch) | Read and write with ordinary SQL | native DuckDB SQL, `CHECKPOINT` |
| [4. Snapshot](#4-take-snapshots) | Publish immutable, named versions with commit metadata | `differential_create_snapshot`, `<db>.snapshots()` |
| [5. Read history](#5-read-history) | Query or attach older versions | `AT (VERSION/TIMESTAMP)`, `ATTACH … (SNAPSHOT_*)` |
| [6. Fork](#6-fork-branches-and-clones) | Branch inside a database, or clone into a new one | `differential_create_branch`, `differential_clone_database` |
| [7. Detach](#7-detach) | Release a writer, its pins and its AT snapshots | `DETACH` |
| [8. Restore](#8-restore-a-branch) | Reset a branch to an earlier snapshot | `differential_restore` |
| [9. Delete](#9-delete-branches-and-expire-snapshots) | Drop branches, expire snapshots | `differential_drop_branch`, `differential_expire_snapshots` |
| [10. Reclaim space](#10-reclaim-space) | Collect unreferenced data, compact the metadata store | `differential_cleanup_old_files`, `differential_compact` |
| [11. S3](#11-publish-to-s3) | Export snapshots, read them anywhere, follow branch heads, recover them | `DATA_PATH 's3://…'`, `differential_evict_snapshots`, `differential_recover_export` |
| [12. Operate](#12-operate-and-recover) | Retry safely, inspect operations and storage | `request_id`, `differential_operations()`, `differential_storage_stats()` |

## 1. Set up

```sql
INSTALL differential FROM community;   -- once per DuckDB installation
LOAD differential;                     -- every session
SET differential_repository = '/absolute/path/repository';
```

The extension targets DuckDB 1.5.6.

The repository is a local directory holding the extension's metadata store and, by default,
the data. The metadata store is an embedded SQLite database, the setup DuckLake recommends when
several local processes share a catalog ([why](architecture.md#why-sqlite-for-metadata)). It defaults to `$DIFFERENTIAL_STORE_ROOT` or `~/.duckdb/differential`. Setting it
creates nothing; the directory is created on first use. It cannot change after first use in
a DuckDB instance.

A repository is private to the OS user who creates it. For several local users, create it with
`SET differential_repository_shared = true` (before first use) in a directory owned by their
group; see [deployment.md](deployment.md#sharing-a-repository-between-local-users).

## 2. Create a database

```sql
ATTACH 'differential:sales' AS sales;
FROM differential_databases();
```

ATTACH creates the database and its `main` branch if they do not exist. Pass
`(CREATE_IF_NOT_EXISTS false)` to attach existing databases only. The name is the database's
identity in the repository; the alias (`AS sales`) is local to the session.

Payload placement is chosen once, at creation, with `DATA_PATH`:

| `DATA_PATH` | Payload stored in |
| --- | --- |
| omitted | the repository's metadata store (as SQLite blobs) |
| `'/absolute/directory'` | an immutable-file local pool in that directory |
| `'s3://bucket/prefix/'` | locally, plus S3 exports of every published snapshot ([§11](#11-publish-to-s3)) |

Metadata always stays in the repository. Attaching later with a different `DATA_PATH` is
rejected. Branches and clones inherit the data path.

An existing, cleanly closed native DuckDB file can be copied in as a new database. Its
`.wal` sidecars must not exist and the file must not change during the import:

```sql
CALL differential_import_database('/absolute/path/existing.duckdb', 'imported');
```

## 3. Work with a live branch

```sql
CREATE TABLE sales.orders (id INTEGER PRIMARY KEY, amount DECIMAL(10, 2));
INSERT INTO sales.orders VALUES (1, 10.00), (2, 20.00);
CHECKPOINT sales;
```

Everything is native DuckDB: catalog, transactions, constraints, indexes, WAL and
checkpoints. Commits are durable at DuckDB's sync points; durability matches native DuckDB,
with a full device flush. One process at a time may write a branch; a second writer is
rejected.

Checkpoints do not create public snapshots, unless you opt in for databases attached after
the setting:

```sql
SET differential_snapshot_on_checkpoint = true;   -- every changed checkpoint becomes a snapshot
SET differential_snapshot_on_checkpoint = false;
```

## 4. Take snapshots

```sql
CALL differential_create_snapshot('sales', name => 'v1', author => 'ada',
                                  message => 'initial load', request_id => 'sales-v1');
INSERT INTO sales.orders VALUES (3, 30.00);
CALL differential_create_snapshot('sales', message => 'third order');
SELECT snapshot_id, snapshot_name, author, commit_message FROM sales.snapshots();
FROM sales.current_snapshot();                     -- 2
```

A snapshot is an immutable, published version of the branch: snapshot 1 (`v1`) has two orders,
snapshot 2 has three. IDs increase per database. Names are unique per database and stay
reserved even after the snapshot expires. A snapshot of an unchanged database gets a new ID
but shares its data.

`request_id` makes the call safely retryable: repeating it returns the original result instead
of creating a second snapshot ([§12](#12-operate-and-recover)).

## 5. Read history

Query an older version inside any statement:

```sql
SELECT count(*) FROM sales.orders AT (VERSION => 1);          -- 2
SELECT count(*) FROM sales.orders AT (TIMESTAMP => now());    -- 3: latest snapshot at or before now
```

Or attach a version read-only as its own catalog:

```sql
ATTACH 'differential:sales' AS sales_v1 (SNAPSHOT_NAME 'v1');      -- or SNAPSHOT_VERSION 1
ATTACH 'differential:sales' AS sales_latest (READ_ONLY);           -- the branch's latest snapshot
SELECT count(*) FROM sales_v1.orders;                               -- 2
FROM sales_v1.current_snapshot();                                   -- 1
```

`SNAPSHOT_TIME '2026-10-01 12:00'` selects by time. History has the schema of the snapshot,
including tables, views, macros and indexes as they were. Snapshot attachments are immutable;
to see newer data, DETACH and ATTACH again.

## 6. Fork: branches and clones

A **branch** is a new writable line of history inside the same database. It shares data with
its source until it diverges:

```sql
SELECT branch_id, snapshot_id FROM differential_create_branch('sales', 'dev');     -- from main's latest snapshot (2)
ATTACH 'differential:sales' AS dev (BRANCH 'dev');
DELETE FROM dev.orders WHERE id = 1;
CALL differential_create_snapshot('dev', message => 'cleanup on dev');             -- snapshot 3
SELECT branch_name, head_snapshot_id FROM sales.branches();
```

Pass `snapshot => n` to branch from a specific snapshot, or `branch => 'name'` to start from
another branch's latest snapshot. `AT (TIMESTAMP => …)` and `READ_ONLY` resolve within the
attached branch; `AT (VERSION => n)` can read any snapshot of the database.

A **clone** is an independent new database with its own identity and snapshot numbering. It
starts from a snapshot of the source and shares data with it:

```sql
CALL differential_clone_database('sales', 'sales_copy', snapshot => 1);
ATTACH 'differential:sales_copy' AS sales_copy;
SELECT count(*) FROM sales_copy.orders;            -- 2
```

A clone survives deletion of its source's branches and snapshots. Merging branches is not
supported.

## 7. Detach

```sql
DETACH dev;
DETACH sales_copy;
```

DETACH flushes and releases the branch for other writers. It also releases the reader pins of
snapshot attachments and closes the snapshots opened by this database's `AT` queries. Pinned
snapshots cannot be garbage-collected, so detach what you no longer need. Restoring or
dropping a branch requires it to be detached everywhere.

## 8. Restore a branch

```sql
DETACH sales;
SELECT preservation_snapshot_id, snapshot_id FROM differential_restore('sales', 1);   -- 2, 4
ATTACH 'differential:sales' AS sales;
SELECT count(*) FROM sales.orders;                 -- 2
```

Restore never loses committed data. It first preserves the branch's current contents as a
snapshot, reusing the latest snapshot when nothing changed since it (here: 2) or else
publishing `preserved before restore`. Then it publishes the restored state as a new snapshot
(`restored to snapshot 1`, here 4). Both appear in `sales.snapshots()`. Pass
`branch => 'dev'` to restore another branch.

## 9. Delete: branches and expire snapshots

```sql
SELECT preservation_snapshot_id FROM differential_drop_branch('sales', 'dev');        -- 3
SELECT snapshot_id FROM differential_expire_snapshots('sales', versions => [2, 3]);
SELECT snapshot_id FROM differential_expire_snapshots('sales',
       older_than => now() - INTERVAL 30 DAY, dry_run => true);
```

- `differential_drop_branch` preserves the branch's committed contents as a snapshot (here
  its unchanged latest snapshot, 3), then removes the branch. Its name can be reused. The last branch of a database cannot be dropped.
- `differential_expire_snapshots` retires snapshots: they can no longer be attached, named or
  found by time. Already-open attachments keep reading them until detached. Select them with
  `versions => [...]`, `older_than => timestamp`, or both, and preview with `dry_run => true`.
- Expired snapshots and dropped branches release their data for collection ([§10](#10-reclaim-space)).
  Data still shared with a live branch, clone, snapshot or pin is kept.
- Deleting a whole database is not supported.

## 10. Reclaim space

```sql
SELECT deleted_objects FROM differential_cleanup_old_files(cleanup_all => true);
SELECT orphaned_files FROM differential_delete_orphaned_files(dry_run => true);
SELECT pages_reclaimed FROM differential_compact();
```

Collection runs automatically in small background steps after each checkpoint; turn it off
with `SET differential_auto_gc = false`.
- `differential_cleanup_old_files` runs it explicitly: one bounded step by default, or until
  nothing is queued with `cleanup_all => true`. `dry_run => true` counts exactly what would go.
- `differential_delete_orphaned_files` removes local-pool files that crashes left unreferenced.
- `differential_compact` returns free pages of the metadata store to the file system.

## 11. Publish to S3

A database with an S3 data path exports every snapshot to S3 as immutable, content-addressed
packs; later exports upload only changed data. The bucket must already exist.

First tell DuckDB how to reach the bucket, with the same `TYPE s3` secret httpfs uses:

```sql
CREATE SECRET (TYPE s3, KEY_ID '…', SECRET '…', REGION 'us-east-1', SCOPE 's3://my-bucket/differential/');
-- or use your AWS environment (SSO, profiles, roles):
-- CREATE SECRET (TYPE s3, PROVIDER credential_chain, REFRESH auto, SCOPE 's3://my-bucket/differential/');
```

Without a secret, the AWS SDK finds credentials in the environment, `~/.aws` or instance roles,
and you add `REGION` (and `ENDPOINT` for other services) to each ATTACH.

```sql
ATTACH 'differential:archive' AS archive (DATA_PATH 's3://my-bucket/differential/');
CREATE TABLE archive.events AS SELECT range AS id FROM range(1000);
SELECT snapshot_id, export_state, export_locator
FROM differential_create_snapshot('archive', name => 'q3');     -- complete, descriptors/<sha256>
```

A successful call means the snapshot is durable in S3. If the upload fails, the local snapshot
still exists and its export stays `pending`. Finish it later, or skip waiting entirely:

```sql
CALL differential_create_snapshot('archive', export_mode => 'deferred');   -- returns before upload
CALL differential_retry_export('archive');                                 -- uploads all pending exports
```

Once exported, a snapshot's local copy can be dropped. Reads then go through S3, and branching
or restoring from it downloads it again:

```sql
CALL differential_evict_snapshots('archive', versions => [1]);
```

Any host with read access (and the same s3 secret) can attach an export directly, without the
writer's repository. It can also copy the export into a new writable database:

```sql
ATTACH 'differential:s3://my-bucket/differential/descriptors/<sha256>' AS q3;
CALL differential_recover_export('s3://my-bucket/differential/descriptors/<sha256>', 'archive_restored');
```

To follow a branch from other hosts, attach its **branch head**, the latest exported snapshot of
that branch. Heads are updated after each export, branch creation and expiry:

```sql
ATTACH 'differential:s3://my-bucket/differential/heads/archive/main' AS archive_latest;
SELECT database_name, branch_name, snapshot_id
FROM differential_remote_heads('s3://my-bucket/differential/');
```

Re-attach to see newer snapshots ([deployment.md](deployment.md#branch-heads) has the details).
For several hosts *writing* one database through S3, see [shared databases](shared-databases.md).
Data read from S3 stays cached in memory for the session; to reuse it across sessions, set
`SET differential_remote_cache_size = '4GB'` ([caching](api.md#caching-s3-reads)).

To put every new database on S3 without repeating `DATA_PATH`, store it in a `TYPE differential`
secret:

```sql
CREATE SECRET (TYPE differential, DATA_PATH 's3://my-bucket/differential/');
ATTACH 'differential:metrics' AS metrics;                       -- created with the S3 data path
```

The export path is the `DATA_PATH` plus the snapshot's `export_locator`. Exports are never
deleted by the extension; expiring a snapshot locally does not remove its export. See
[api.md](api.md#secrets) for all secret options and rules.

## 12. Operate and recover

```sql
SELECT kind, state, snapshot_id FROM differential_operations();
SELECT metric, value FROM differential_storage_stats() WHERE metric LIKE 'local_%';
FROM differential_status();
```

- **Retries.** Give every mutation a `request_id`. After an error, timeout, cancellation or
  crash, repeat the same call: it resumes or returns the original result, and never applies
  twice. Reusing an ID with different arguments fails.
- **Crashes.** Committed data survives process death. An interrupted restore or branch drop
  leaves an intent that blocks new writers until the same request is retried.
- **Transactions.** Management functions run in autocommit mode only. Binding, `PREPARE` and
  `EXPLAIN` of a management function change nothing. Queries with `AT` open their snapshot
  while binding.
- **Errors** carry stable prefixes such as `[differential:not_found]`, `[differential:retired]`
  or `[differential:writer_busy]` ([list](api.md#development-and-limits)).
