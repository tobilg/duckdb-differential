---
name: duckdb-differential
description: Versioned DuckDB databases with the differential community extension (DuckDB 2.0) - snapshots, branches, clones, restore, branch merging, AT time travel, and replication through S3 (exports, branch heads, followers, WAL shipping, failover, shared multi-writer databases). Use when the user mentions duckdb-differential, ATTACH 'differential:...' paths or differential_* functions, or wants to version, branch, roll back, time-travel, replicate, sync or fail over DuckDB databases, including leader/follower setups on S3.
license: MIT
compatibility: DuckDB 2.0 on Linux or macOS (amd64, arm64). S3 features need network access and an existing bucket.
metadata:
  author: tobilg
  version: "1.1.0"
  repository: https://github.com/tobilg/duckdb-differential
---

# duckdb-differential

A DuckDB extension that stores ordinary DuckDB databases in a versioned repository. Everything inside
a database is native DuckDB (tables, transactions, indexes, WAL, checkpoints); the extension adds
immutable snapshots, branches, clones, restore, branch merging and time travel, and can export every snapshot to S3
for other hosts to read, follow, recover or write.

The SQL blocks below run as written in the extension's test suite. Each block is one DuckDB session;
its first line names the host, and every host has its own repository. `s3://my-bucket/lake/` stands
for a prefix the user owns.

## Concepts

| Term | Meaning |
| --- | --- |
| Repository | A local directory (`differential_repository`) holding the metadata store (SQLite) and, by default, the data. One per host; private to the OS user that created it. |
| Database | A named, versioned DuckDB database in the repository, created on first `ATTACH 'differential:<name>'`. |
| Branch | A writable line of history (`main` by default). One writer process per branch. |
| Snapshot | An immutable, public version of a branch with a database-wide number, optional name, author and message. |
| Clone | A new independent database starting from a snapshot of another, sharing unchanged data. |
| Export / branch head | With an S3 `DATA_PATH`, each snapshot is exported to S3; a branch head names a branch's latest export. |

Checkpoints and commits are **not** snapshots: snapshots are created explicitly with
`differential_create_snapshot` (or per checkpoint with `SET differential_snapshot_on_checkpoint = true`).

## Setup

```sql
-- not run: installation (the test suite loads a local build)
INSTALL differential FROM community;   -- once per DuckDB installation
LOAD differential;                     -- every session
SET differential_repository = '/absolute/path/repository';   -- optional; default $DIFFERENTIAL_STORE_ROOT or ~/.duckdb/differential
```

Check the DuckDB version first (`SELECT version();` must be 2.0). The repository setting creates
nothing and cannot change after first use in a DuckDB instance.

## Local lifecycle

```sql
-- main
ATTACH 'differential:shop' AS shop;                         -- creates the database and branch main
CREATE TABLE shop.orders AS SELECT range AS id, range * 10 AS amount FROM range(100);
CALL differential_create_snapshot('shop', name => 'loaded', author => 'etl', message => 'initial load');
DELETE FROM shop.orders WHERE id >= 50;
CALL differential_create_snapshot('shop', message => 'removed half');
SELECT snapshot_id, snapshot_name, commit_message FROM shop.snapshots();
SELECT count(*) AS n FROM shop.orders AT (VERSION => 1);   -- 100: time travel inside any query
-- expect output: "n":100
```

```sql
-- main
CALL differential_create_branch('shop', 'what_if', snapshot => 1);
ATTACH 'differential:shop' AS what_if (BRANCH 'what_if');   -- a second writable branch
UPDATE what_if.orders SET amount = amount * 2;
CALL differential_create_snapshot('what_if', message => 'doubled');
ATTACH 'differential:shop' AS loaded (SNAPSHOT_NAME 'loaded');   -- read-only, also SNAPSHOT_VERSION / SNAPSHOT_TIME / READ_ONLY
SELECT (SELECT sum(amount) FROM loaded.orders) AS before, (SELECT sum(amount) FROM what_if.orders) AS after;
DETACH what_if;
DETACH loaded;
```

```sql
-- main
SELECT preservation_snapshot_id, snapshot_id FROM differential_restore('shop', 1);   -- main must be detached
ATTACH 'differential:shop' AS shop;
SELECT count(*) AS n FROM shop.orders;
-- expect output: "n":100
```

```sql
-- main
CALL differential_clone_database('shop', 'shop_sandbox', snapshot => 1);
CALL differential_drop_branch('shop', 'what_if');            -- keeps its contents as a snapshot
SELECT snapshot_id FROM differential_expire_snapshots('shop', versions => [2], dry_run => true);
CALL differential_expire_snapshots('shop', versions => [2]);
SELECT deleted_objects FROM differential_cleanup_old_files(cleanup_all => true);
SELECT database_name FROM differential_databases() ORDER BY 1;
```

## Merging branches

A merge combines two branches' published snapshots with a three-way, cell-level comparison against
their common ancestor. The target (here `main`) must be detached; the source may stay attached.
Every table needs a primary key, and only the documented schema changes are merged
([branch merging](https://github.com/tobilg/duckdb-differential/blob/main/docs/branch-merge.md)).

```sql
-- main
ATTACH 'differential:prices' AS prices;
CREATE TABLE prices.items(id INT PRIMARY KEY, label VARCHAR, price DECIMAL(10,2));
INSERT INTO prices.items VALUES (1, 'tea', 2.50), (2, 'coffee', 3.00);
CALL differential_create_snapshot('prices');
CALL differential_create_branch('prices', 'labels', snapshot => 1);
ATTACH 'differential:prices' AS labels (BRANCH 'labels');
UPDATE labels.items SET label = 'green tea' WHERE id = 1;
CALL differential_create_snapshot('labels');
UPDATE prices.items SET price = 3.20 WHERE id = 2;
CALL differential_create_snapshot('prices');
DETACH prices;
SELECT ready, merge_action FROM differential_merge_branch_preflight('prices', 'main', 'labels', 'main');
SELECT merge_action, snapshot_id FROM differential_merge_branch('prices', 'main', 'labels', 'main',
    request_id => 'labels-into-main');
ATTACH 'differential:prices' AS prices;
SELECT label, price FROM prices.items WHERE id = 1;
-- expect output: "label":"green tea"
```

Conflicting edits of the same cell fail with `merge_conflict`; inspect them with
`differential_merge_branch_preview` and resolve them with `resolution_tables`. A destination other
than `ours` creates a new branch instead. Shared databases cannot be merged.

An existing, cleanly closed DuckDB file becomes a versioned database with
`CALL differential_import_database('/absolute/file.duckdb', 'name');` (no `.wal` file may exist).

## S3: export, read, follow, recover

Credentials come **only** from a DuckDB `TYPE s3` secret matched by `SCOPE`, a named AWS profile
(`CREDENTIAL_PROFILE`) or the AWS SDK default chain. Never pass keys to ATTACH or to functions.

```sql
-- not run: credentials (the test suite provides its own secret)
CREATE SECRET lake (TYPE s3, PROVIDER credential_chain, REFRESH auto, REGION 'us-east-1', SCOPE 's3://my-bucket/lake/');
```

`DATA_PATH` is fixed when a database is created. With an S3 data path every snapshot is exported,
and a successful `differential_create_snapshot` means the snapshot is durable in S3. `SHIP_WAL` also
ships every commit between snapshots (`'sync'`: commits wait for S3; `'async'`: within
`SHIP_INTERVAL` seconds), so a branch can be recovered elsewhere.

```sql
-- writer
ATTACH 'differential:events' AS events (DATA_PATH 's3://my-bucket/lake/', SHIP_WAL 'sync');
CREATE TABLE events.log AS SELECT range AS id FROM range(1000);
SELECT snapshot_id, export_state, export_locator FROM differential_create_snapshot('events', name => 'v1');
INSERT INTO events.log SELECT range FROM range(1000, 1500);   -- shipped, no snapshot needed
SELECT branch_name, ship_wal, ship_pending FROM differential_branches('events');
```

Any host with read access attaches a branch head (the latest exported state) read-only, without the
writer's repository. `FOLLOW true` keeps it current: each transaction sees one consistent state.

```sql
-- reader
SELECT database_name, branch_name, snapshot_id FROM differential_remote_heads('s3://my-bucket/lake/');
ATTACH 'differential:s3://my-bucket/lake/heads/events/main' AS latest;                    -- fixed until re-attached
ATTACH 'differential:s3://my-bucket/lake/heads/events/main' AS live (FOLLOW true, FOLLOW_INTERVAL 2);
SELECT count(*) AS n FROM live.log;
-- expect output: "n":1500
```

If the writer host is lost, another host recovers the branch as of its last shipped commit:

```sql
-- standby
SELECT database_name, shipped_epoch FROM differential_recover_branch('s3://my-bucket/lake/heads/events/main',
    'events', same_identity => true);   -- same_identity: take over as the writer of this database
ATTACH 'differential:events' AS events;
SELECT count(*) AS n FROM events.log;
-- expect output: "n":1500
```

`same_identity => true` is a failover: the new host continues the database and its branch heads
(a repository still shipping the branch then gets `head_conflict`); turn shipping on again there with
`SHIP_WAL`. Without it the recovered database gets a new identity. Deciding when to fail over is up
to the user; there is no automatic failover. A single exported snapshot can also be attached
(`…/descriptors/<sha256>`, the `export_locator`) or copied into a new writable database with
`differential_recover_export`.

### Several writers: shared databases

`WRITERS 'shared'` (fixed at creation, S3 only) lets several hosts write one database without a lock
service. Publishing is optimistic: a host whose base is outdated gets `branch_conflict` and keeps its
work as a snapshot. Every checkpoint is published in the background by default.

```sql
-- host a
ATTACH 'differential:team' AS team (DATA_PATH 's3://my-bucket/lake/', WRITERS 'shared');
CREATE TABLE team.tasks AS SELECT range AS id, 'open' AS state FROM range(10);
SELECT snapshot_id, export_state FROM differential_create_snapshot('team');
```

```sql
-- host b
ATTACH 'differential:team' AS team (DATA_PATH 's3://my-bucket/lake/', WRITERS 'shared');
UPDATE team.tasks SET state = 'done' WHERE id < 5;
SELECT snapshot_id FROM differential_create_snapshot('team', message => 'b finished five');
SELECT branch_name, remote_state FROM differential_branches('team');
```

On `branch_conflict`, `DETACH` the branch and call
`differential_resolve_conflict('team', keep => 'remote')` (take the S3 head; local work stays a
snapshot) or `keep => 'branch'` (keep local work on a new branch). `ON_CONFLICT 'branch'` on ATTACH
forks automatically instead. Branch merging does not apply to shared databases.

## Choosing a setup

| Need | Use |
| --- | --- |
| Versioning on one machine | default `DATA_PATH`, snapshots and branches |
| Publish versions for readers elsewhere | S3 `DATA_PATH`; readers attach `…/heads/<db>/<branch>` |
| Read replicas that stay current | writer with `SHIP_WAL`, readers with `FOLLOW true` |
| Survive loss of the writer host | `SHIP_WAL 'sync'` (no acknowledged commit lost) or `'async'`; `differential_recover_branch(…, same_identity => true)` |
| Several hosts write the same data | `WRITERS 'shared'` (snapshot-granular, conflicts resolved explicitly) |
| Reproducible experiment or test data | `differential_clone_database` or a branch from a snapshot |

## Rules that avoid most errors

- Management functions are table functions: `CALL f(…)` or `SELECT … FROM f(…)`. They run only in
  autocommit mode (not inside `BEGIN … COMMIT`). Named parameters use `=>`.
- The first argument is a database name or an attached alias; a branch alias selects that branch.
- `differential_restore`, `differential_drop_branch`, `differential_resolve_conflict` and a merge into an
  existing branch need that branch detached everywhere (`active_attachment_or_transaction` otherwise).
- One writer process per branch (`writer_busy`); open read-only attachments instead.
- Snapshot and head attachments are fixed: DETACH and ATTACH again to see newer data, or use
  `FOLLOW true`. `AT` works on branch and snapshot attachments, not on S3 exports or followers.
- `AT (VERSION => n)` reads any snapshot of the database; `AT (TIMESTAMP => t)` and `READ_ONLY`
  resolve within the attached branch. Time travel addresses snapshots, never individual commits.
- Give every mutation a `request_id => '…'` when it may be retried: repeating the call returns the
  original result and never applies twice.
- Attachments pin snapshots against garbage collection; detach what is no longer needed. Exports in S3
  are only collected by `differential_cleanup_remote_files` (two phases, a grace period apart).
- `temp_directory` must be local. Linux and macOS only.
- Errors carry stable prefixes such as `[differential:not_found]`, `[differential:writer_busy]`,
  `[differential:branch_conflict]`, `[differential:head_conflict]`, `[differential:retired]`.

## Inspecting state

```sql
-- main
SELECT branch_name, head_snapshot_id, remote_state FROM differential_branches('shop');
SELECT snapshot_id, snapshot_name, export_state FROM differential_snapshots('shop');
SELECT kind, state, snapshot_id FROM differential_operations();
SELECT metric, value FROM differential_storage_stats() WHERE metric LIKE 'local_%';
SELECT capability, status FROM differential_status();
```

`<catalog>.snapshots()`, `<catalog>.current_snapshot()` and `<catalog>.branches()` are the same,
bound to an attachment. `duckdb_functions()` lists every function with parameters and an example.

## Function index

- Introspection: `differential_snapshots`, `differential_current_snapshot`, `differential_branches`,
  `differential_databases`, `differential_operations`, `differential_providers`,
  `differential_retention_roots`, `differential_storage_stats`, `differential_status`,
  `differential_remote_heads`, `differential_writers`, `differential_followers`
- Snapshots and branches: `differential_create_snapshot`, `differential_create_branch`,
  `differential_clone_database`, `differential_restore`, `differential_drop_branch`,
  `differential_expire_snapshots`
- S3: `differential_retry_export`, `differential_evict_snapshots`, `differential_claim_branch_heads`,
  `differential_resolve_conflict`
- Import and recovery: `differential_import_database`, `differential_recover_export`,
  `differential_recover_branch`
- Merging: `differential_merge_branch_preview`, `differential_merge_branch_preflight`,
  `differential_merge_branch`, `differential_merge_snapshots_preview`, `differential_merge_snapshots`,
  `differential_merge_ancestry`, `differential_merge_requests`, `differential_merge_resolutions`,
  `differential_merge_status`, `differential_abort_merge`
- Maintenance: `differential_cleanup_old_files`, `differential_delete_orphaned_files`,
  `differential_compact`, `differential_cleanup_remote_files`, `differential_cleanup_metadata`

## References

Read these when a task needs more than the above (they are copies of the repository's docs):

- [references/sql-reference.md](references/sql-reference.md): every statement, ATTACH option,
  setting and function with parameters, result columns and an example.
- [references/lifecycle.md](references/lifecycle.md): one database from creation to cleanup, local
  and S3, step by step.
- [references/shared-databases.md](references/shared-databases.md): several writing hosts,
  conflicts, failures and housekeeping.
- [references/api.md](references/api.md): semantics, secrets and precedence, S3 request costs,
  caching, durability, error categories.
- [references/deployment.md](references/deployment.md): processes, local users and hosts; branch
  heads across hosts.
