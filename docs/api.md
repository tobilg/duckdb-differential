# SQL API

The surface follows DuckLake's conventions: a database is attached with
`ATTACH 'differential:<name>'` plus options, configuration uses `SET differential_*`,
and management is a small set of `differential_*` table functions that take the
database (or attachment alias) first and everything else as named parameters.
For a task-oriented walkthrough, see the [usage lifecycle guide](lifecycle.md) and, for several
writers, [shared databases](shared-databases.md). The [SQL reference](sql-reference.md) lists
every statement, option, setting and function with an example.

Install the community extension once, load it in each session (DuckDB 2.0), then ATTACH:

```sql
INSTALL differential FROM community;
LOAD differential;
SET differential_repository = '/absolute/repository';   -- optional
ATTACH 'differential:sales' AS sales;                      -- created on first use
CREATE TABLE sales.t AS SELECT 42 AS value;
CALL differential_create_snapshot('sales', name => 'baseline',
     author => 'Ada', message => 'initial load', request_id => 'baseline-001');
FROM differential_snapshots('sales');
ATTACH 'differential:sales' AS historical (SNAPSHOT_NAME 'baseline');
```

Named parameters accept both `=>` and `:=`.

## Settings

| Setting | Default | Meaning |
| --- | --- | --- |
| `differential_repository` | `$DIFFERENTIAL_STORE_ROOT` or `~/.duckdb/differential` | Local repository root; a relative path resolves against the working directory when set. Setting it never creates anything; the repository is created on first use. It cannot change after first use in a DuckDB instance (use a new instance). Global scope. |
| `differential_auto_gc` | `true` | Run bounded collection steps in the background after completed checkpoints. Read when a file is opened. |
| `differential_snapshot_on_checkpoint` | `false` | Publish every completed native checkpoint of a writable branch as a public snapshot (`commit_message = 'checkpoint'`). Read when a file is opened. Unchanged images publish nothing. |
| `differential_remote_cache_size` | `0` (off) | Size of the repository's disk cache for data read from S3, in `memory_limit` syntax (`'4GB'`, `'512MiB'`). Read when an S3 export, head or evicted snapshot is opened. See [caching](#caching-s3-reads). Global scope. |
| `differential_s3_max_requests` | `16` | Parallel S3 requests per process (1–64), shared by all S3 locations and DuckDB instances in the process. See [parallel S3 requests](#parallel-s3-requests). |
| `differential_group_commit` | `true` | Concurrent syncs of different databases in one process share a durable transaction (one sync); 16 MiB of new data per transaction at most. Process-wide. |
| `differential_export_verification` | `'checksum'` | How exports verify uploads: the S3 service checks each object's SHA-256 (`x-amz-checksum-sha256`), or `'read_back'` also downloads and hashes it (for S3-compatible services that ignore checksums). Process-wide. |
| `differential_export_staging` | `'disk'` | Where exports stage objects before upload: files under `<repository>/staging/` (with the quota below), or `'memory'`, within the same byte budget per process. Process-wide. |
| `differential_repository_shared` | `false` | Create the repository for several local users: setgid group-writable directories (`02770`) and files (`0660`) for the repository directory's group. Only applies when the repository is created (an existing private repository refuses it); users joining a shared repository need no setting. See [deployment.md](deployment.md#sharing-a-repository-between-local-users). |

LOAD, SET, binding, PREPARE and plain EXPLAIN do not initialize a repository.
`differential_status()` does not either.

## ATTACH

```sql
ATTACH 'differential:<database>' [AS alias] [(option value, ...)]
```

| Option | Meaning |
| --- | --- |
| `BRANCH 'name'` | Attach a writable branch (default `main`). |
| `SNAPSHOT_VERSION n` | Attach public snapshot `n` read-only. |
| `SNAPSHOT_TIME 'timestamp'` | Attach the latest snapshot of the branch published at or before the time, read-only. |
| `SNAPSHOT_NAME 'name'` | Attach a named snapshot read-only. |
| `READ_ONLY` | Without a selector: attach the branch's latest public snapshot read-only. Live files have one writer, so a live read-only attachment is not offered. |
| `CREATE_IF_NOT_EXISTS bool` | Default `true`: create the database (and its `main` branch) when missing. Never created by a read-only or snapshot attachment. |
| `DATA_PATH 'path'` | Where payload lives, fixed at creation: omitted = the repository's metadata store; an absolute directory = an immutable-file local pool; `s3://bucket/prefix/` = the S3 snapshot profile. Repeating it later must match. |
| `WRITERS 'single' \| 'shared'` | Fixed at creation (default `'single'`): `'shared'` lets several repositories (hosts) write the database through its S3 data path ([shared databases](#shared-databases-several-writers)). Repeating it later must match. |
| `PUBLISH_ON_CHECKPOINT bool` | Shared databases, writable attachments, default `true`: every completed DuckDB checkpoint becomes a snapshot that is published to S3 in the background. `false` publishes only with `differential_create_snapshot`. |
| `PUBLISH_ON_DETACH bool` | Shared databases, writable attachments, default `false`: when the branch's file closes (DETACH, or the database closing), wait until its final checkpoint is published. |
| `ON_BUSY 'warn' \| 'error' \| 'ignore'` | Shared databases, writable attachments: what ATTACH does when another live writer has a "being written" marker on the branch. `'warn'` (default) prints a warning to stderr, `'error'` refuses with `writer_busy`, `'ignore'` says nothing. Advisory only: conflicts are still detected when publishing. |
| `ON_CONFLICT 'fail' \| 'branch'` | Shared databases, writable attachments: what a snapshot does when another writer moved the branch head. `'fail'` (default) reports `branch_conflict`; `'branch'` publishes the snapshot as a branch of its own. Applies to this attachment only. |
| `SHIP_WAL 'off' \| 'async' \| 'sync'` | Single-writer databases with an S3 data path, writable attachments: ship every published state of the branch to S3 continuously ([WAL shipping](#wal-shipping-and-followers)). `'async'` ships in the background at most every `SHIP_INTERVAL`; `'sync'` makes each commit wait until it is in S3. Stored per database in the repository: later writable attachments ship until `SHIP_WAL 'off'`. Rejected for shared databases, local data paths and read-only attachments. |
| `SHIP_INTERVAL seconds` | With `SHIP_WAL` (0–3600, default 1): minimum time between two epochs of a branch in `async` mode; bounds S3 PUTs. Stored with `SHIP_WAL`. |
| S3 options | `REGION`, `ENDPOINT`, `CREDENTIAL_PROFILE`, `PATH_STYLE`, `ALLOW_HTTP`, `MAX_REQUESTS`, `RETRIES`, `CONNECT_TIMEOUT_MS`, `REQUEST_TIMEOUT_MS`, `MAX_RANGE_BYTES`, `MULTIPART_THRESHOLD` (only with an `s3://` path). A matching `TYPE s3` secret usually makes them unnecessary. |
| `SECRET 'name'` | Take unset options from a `TYPE differential` secret ([secrets](#secrets)). |

Head paths (`ATTACH 'differential:s3://…/heads/<database>[/<uuid>][/<branch>]'`) also take
`FOLLOW true` and `FOLLOW_INTERVAL seconds` (1–3600, default 10): a read-only follower that moves to
the branch's newest state on its own ([followers](#wal-shipping-and-followers)).

At most one snapshot selector may be given. `SNAPSHOT_TIME` and `READ_ONLY` resolve within
`BRANCH`. Native options (`BLOCK_SIZE`, `STORAGE_VERSION`, …) pass through to DuckDB.
Unknown options fail. Names are case-sensitive, 1–128 ASCII letters, digits, underscores
or hyphens.

Readers keep their snapshot until DETACH; refreshing means DETACH and ATTACH again. An
open native buffer manager is never remapped.

## Time travel and catalog functions

```sql
SELECT * FROM sales.t AT (VERSION => 3);
SELECT * FROM sales.t AT (TIMESTAMP => now() - INTERVAL 1 HOUR);
FROM sales.snapshots();            -- also current_snapshot() and branches()
```

`AT (VERSION => n)` reads public snapshot `n` of the attachment's database (any branch).
`AT (TIMESTAMP => t)` reads the latest snapshot of the attached branch at or before `t`. The
snapshot's own schema, views and macros apply. AT works on branch and snapshot attachments,
not on S3 exports, and is read-only. Retired, missing or pre-history snapshots are bind errors.
Shared databases also resolve other writers' snapshots
([shared databases](#shared-databases-several-writers)).

A snapshot read through AT is opened as a hidden read-only attachment and holds a reader pin
from the moment the query binds; `PREPARE` and `EXPLAIN` included. Up to 8 are
cached per DuckDB instance, least recently used first out. DETACH of the database releases the
ones it opened. While cached, a snapshot's root stays retained even if it is retired.

`<catalog>.snapshots()`, `<catalog>.current_snapshot()` and `<catalog>.branches()` are the
global introspection functions bound to that attachment; unqualified after `USE`. They
exist only in memory, never in the native file. An existing table macro with the same name
wins; new table macros cannot use these names in `main`. `current_snapshot()` returns the
attached snapshot for a snapshot attachment, else the branch's latest snapshot. Both features
need the differential catalog: `ATTACH 'differential:…'` or a `diff://` URI with `TYPE differential`.

The internal canonical URIs remain attachable with `TYPE differential`:
`diff://db/<database UUID>/branches/<branch UUID>` and
`diff://db/<database UUID>/snapshots/<ID>`. They are returned in the `uri` columns and
stay valid after a name is reused.

## Management functions

Every parameter (type, default, description) and result column of each function, with an
example, is in the [SQL reference](sql-reference.md#conventions). From SQL, `duckdb_functions()`
describes each function: description, example, categories, and parameter names and types.

Every function whose first argument is `database` accepts a database name or UUID, or the
alias of an attached differential database (`'sales'` after `ATTACH … AS sales`). Branch
and snapshot attachments resolve to their database.

Mutations accept `request_id => 'id'` and require autocommit. Request IDs are
repository-wide. Replaying the same request and arguments returns the original logical
result; reusing an ID with different arguments fails; omitting it creates a new operation.
Bulk functions derive per-item IDs (`<request_id>/<snapshot id>`, `<request_id>/<step>`).
Names and allocator metadata never rewind. Snapshot names remain reserved after expiry.

| Function | Semantics |
| --- | --- |
| `differential_create_snapshot(database, name =>, author =>, message =>, extra_info =>, export_mode => 'sync')` | Complete a native checkpoint and publish a public snapshot of the attached branch with optional commit metadata. With an S3 data path it also exports; `'deferred'` returns after local publication with `export_state = 'pending'`. An unchanged image gets a new ID sharing its payload. |
| `differential_create_branch(database, name, snapshot =>, branch =>)` | New writable branch from a snapshot. The default is the latest snapshot of `branch` (default `main`). |
| `differential_clone_database(database, name, snapshot =>, branch =>, export_mode =>)` | New independent database (UUID, allocator, snapshot 1) on the same data path. |
| `differential_restore(database, snapshot_id, branch =>, export_mode =>)` | Reset a detached branch (default: the attached branch, else `main`) to a snapshot. Its current contents are first preserved as a snapshot (`preserved before restore`). Then the restored state is published (`restored to snapshot N`). |
| `differential_drop_branch(database, branch, export_mode =>)` | Preserve the committed contents as a snapshot and tombstone a detached branch. The last active branch cannot be dropped. |
| `differential_expire_snapshots(database, versions => [..], older_than =>, dry_run => false)` | Retire snapshots: hidden from attach, name and time lookup; existing attachments survive. Exported ones are recorded as expired in S3, for remote collection. `versions` always selects; `older_than` selects non-retired snapshots published before the time. |
| `differential_evict_snapshots(database, versions =>, older_than =>, dry_run =>)` | Drop the local copy of snapshots whose S3 export completed (`older_than` only selects those). Attachments read through S3; branch/clone/restore rehydrate. |
| `differential_claim_branch_heads(database)` | Take the database's S3 branch heads over after a copy of the repository wrote them ([branch heads](#s3-snapshot-profile)); single-writer databases only. Returns `branch_name`, `snapshot_id`, `head`. |
| `differential_retry_export(database [, snapshot_id])` | Resume pending S3 exports: all of them, or one. Works for retired snapshots and deleted branches and allocates no snapshot ID. Also republishes all branch heads (shared databases: except diverged branches). |
| `differential_resolve_conflict(database, keep =>, branch =>, name =>)` | Shared databases: reconcile a detached branch (default: the attached branch, else `main`) with its S3 head. Its current contents are first preserved as a snapshot (`preserved before conflict resolution`) unless its latest snapshot holds them. `keep => 'branch'` publishes that work as a new branch (`name`, default `<branch>-conflict-<snapshot>`); `keep => 'remote'` keeps it only as a local snapshot. Then the branch becomes its S3 head again. Returns `operation_id`, `branch_name`, `remote_snapshot_id`, `kept_snapshot_id`, `kept_branch`. |
| `differential_import_database(source_path, name, data_path =>, S3 options, secret =>)` | Copy an offline, checkpoint-consistent native file into a new database. Native WAL/recovery sidecars and changing sources are rejected. |
| `differential_recover_export('s3://…/descriptors/<sha256>', name, data_path =>, S3 options, secret =>)` | Copy and verify an exported snapshot into a new writable database. |
| `differential_recover_branch('s3://…/heads/<database>[/<uuid>][/<branch>]', name, same_identity => false, S3 options, secret =>)` | Recover the newest shipped state of a branch ([WAL shipping](#wal-shipping-and-followers)) into a new database `name` on the same S3 location: the chain's base is imported without downloading, its epochs are applied, and DuckDB replays the WAL at the first ATTACH. `same_identity => true` keeps the database and branch identities (a failover). Returns `database_id`, `database_name`, `branch_id`, `branch_name`, `shipped_chain`, `shipped_epoch`, `uri`. |
| `differential_cleanup_old_files(dry_run => false, cleanup_all => false, max_objects => 1000)` | One bounded collection step, or with `cleanup_all` steps until the queues are empty. A dry run counts exactly, using a read snapshot and no locks. |
| `differential_delete_orphaned_files(dry_run => false, max_objects => 1000)` | Remove local-pool files that no mapping references (left by crashes between file write and commit). |
| `differential_compact(max_pages => 1000)` | Incrementally return free pages of the metadata store to the file system. Roots and pins are preserved. |
| `differential_cleanup_remote_files(location, older_than => a week ago, dry_run => false, S3 options)` | Two-phase garbage collection of an S3 location: condemn unreachable data, delete what stayed condemned and unreachable since before `older_than`. Exports never reference condemned data. |
| `differential_cleanup_metadata(older_than =>, dry_run => false)` | Delete completed operation records and history entries of expired snapshots older than the cutoff. |

Commit metadata (`author`, `message`, `extra_info`) is stored with the snapshot and part of
request matching. System operations label their snapshots: `checkpoint`,
`clone of <db> snapshot N`, `preserved before restore`, `restored to snapshot N`,
`preserved before branch deletion`, `imported native file`, `recovered from export`.

Lifecycle results contain `operation_id`, `database_id`, `branch_id`, `snapshot_id`,
`preservation_snapshot_id`, `uri`, `snapshot_time`, `state`, `export_state` and
`export_locator`. Zero snapshot IDs mean nothing was published. Expire/evict return one row
per selected snapshot (`snapshot_id`, `snapshot_time`, `snapshot_name`, `dry_run`,
`operation_id`, `export_locator`). Restore and drop require DETACH and exclusive branch
ownership. A crash leaves a resumable intent that fences new writers; retry the original
request. Neither whole-database deletion nor branch merge is provided.

Collection limits are 1–10000 work units: each examined queued candidate or deleted
mapping row costs one unit. A step returns `candidate_objects` (deleted plus still queued),
`candidate_bytes` for the deleted payload, and `deleted_objects`. Candidate bytes are
payload bytes, not immediately released disk bytes. Compaction limits are 1–10000 pages.
Its result has `free_pages_before`, `free_bytes_before`, `pages_reclaimed`,
`bytes_reclaimed`, `manifest_roots`, `roots_unchanged`, `payload_bytes` and
`export_retained_bytes`. WAL readers can defer physical shrinkage. 
Remote exports are never reclaimed. Pending exports retain their local graph; completed
exports do not, so their local payload becomes collectible once nothing local needs it.

## S3 snapshot profile

```sql
CREATE SECRET (TYPE s3, KEY_ID '…', SECRET '…', REGION 'us-east-1',
               SCOPE 's3://your-existing-bucket/your-owned-prefix/');
ATTACH 'differential:remote_sales' AS live (DATA_PATH 's3://your-existing-bucket/your-owned-prefix/');
CREATE TABLE live.t AS SELECT 42 AS value;
SELECT export_locator FROM differential_create_snapshot('live', request_id => 'remote-baseline');
```

The first ATTACH with a new `s3://` data path registers it as a provider. Its identity is
the location plus endpoint, and its configuration is immutable: explicit options given
later must match. Credentials come from a `TYPE s3` secret or the AWS SDK
([secrets](#secrets)). TLS verification is enabled; `ALLOW_HTTP true` (or an s3 secret with
`USE_SSL false`) is an explicit development option for compatible endpoints. No bucket is
created.

Commits are durable locally. Only successfully exported public snapshots are remotely
durable. If an export fails, the already completed local operation is reported, and the
snapshot keeps a persistent pending retention root. Finish it with
`differential_retry_export('remote_sales')` or retry the original request; neither
assigns another snapshot ID. Cancellation has the same retention rule.

Exports upload only chunks that no earlier export stored, in
content-addressed 8 MiB packs, and paged indexes map each chunk to its pack and slot.
To keep only the remote copy, call `differential_evict_snapshots('remote_sales', versions => [1])`;
`differential_snapshots()` then reports `publication_state = 'local_evicted'`.

On any host with access to the bucket, attach an export by its locator, read-only, without
copying payload:

```sql
ATTACH 'differential:s3://your-existing-bucket/your-owned-prefix/descriptors/<sha256>' AS old;
CALL differential_recover_export('s3://your-existing-bucket/your-owned-prefix/descriptors/<sha256>',
                                 'recovered', request_id => 'recover-baseline');
```

The descriptor graph works without the original writer's catalog. Provider access and any
native extension dependencies must still be available on the reader host.

To follow a branch instead of one snapshot, attach its **branch head**: the latest exported,
non-expired snapshot in the branch's history. The extension updates
`dbs/<database uuid>/branches/<branch>` after each completed export, branch creation and
expiry:

```sql
ATTACH 'differential:s3://your-existing-bucket/your-owned-prefix/heads/remote_sales/main' AS latest;
                                                         -- 'heads/remote_sales' means branch main
FROM differential_remote_heads('s3://your-existing-bucket/your-owned-prefix/');
```

`differential_remote_heads(location, S3 options, secret =>)` lists every head in a location: `database_name`,
`database_id`, `branch_name`, `branch_id`, `snapshot_id`, `snapshot_time`, `export_locator` and
`head` (the attachable path). Re-attach to pick up newer snapshots. If two databases with the same
name export to one prefix, use `heads/<database>/<database uuid>/<branch>`. A failed head update
does not undo a completed export; `differential_retry_export('<database>')` republishes the heads.
Heads are written conditionally: a repository only overwrites a head it wrote last, so a copy of
the repository elsewhere (a restored backup, a cloned VM) gets `[differential:head_conflict]`
instead of repointing them. `differential_claim_branch_heads('<database>')` takes the database's
heads over deliberately and returns `branch_name`, `snapshot_id` and `head` for each.
See [deployment.md](deployment.md#branch-heads) for the semantics.

### Shared databases (several writers)

A database created with `WRITERS 'shared'` is written by several repositories, typically one
per host, through its S3 location. There is no lock and no server: writers work locally and
publish optimistically to the branch heads.

```sql
-- on each host, in its own repository
ATTACH 'differential:team' AS team (DATA_PATH 's3://your-existing-bucket/your-owned-prefix/', WRITERS 'shared');
UPDATE team.t SET v = v + 1 WHERE id = 7;
CALL differential_create_snapshot('team');   -- publishes, or fails with branch_conflict
```

- **One identity.** The database UUID is derived from its name, so every repository that attaches
  `team` in the location writes the same database. A name already used there by a single-writer
  database is refused with `name_conflict`.
- **ATTACH starts from S3.** It reads the branch head (one GET). A repository that does not have
  the branch yet joins it: the head snapshot's map is imported, but no data is downloaded, and
  reads fetch chunks from S3 on demand.
  - Reads use DuckDB's external file cache and, when `differential_remote_cache_size` is set,
    the disk cache. The cache entries are shared with readers of the same exports.
  - Each scanning thread's sequential reads fetch ahead, in windows that grow to 4 MiB, as for
    attached exports. There is no read-ahead when neither cache would keep the data.

  A local branch with no changes since its last S3 update moves to the current head. A branch
  with local changes stays where it is.
- **Writes are local** and as fast as for any database. Publishing to S3 happens in two ways:
  - every completed DuckDB checkpoint (`CHECKPOINT`, automatic checkpoints, the checkpoint at
    DETACH) becomes a snapshot (`commit_message = 'checkpoint'`), published in the background by
    one worker per process while the session goes on (`PUBLISH_ON_CHECKPOINT false` turns this off);
  - `differential_create_snapshot` publishes synchronously and returns once the head is written
    or the conflict is reported.

  By default DETACH does not wait for a background publication. A process that exits first
  leaves the snapshot unpublished, and the next writable ATTACH of the database publishes it.
  `PUBLISH_ON_DETACH true` waits instead; its failures cannot fail DETACH, so they show as
  `remote_state = 'ahead'` and in `differential_status()` (`shared_publication`).

  Either way, publishing a snapshot works like this:
  - the snapshot takes the next free number in the database's commit index; if another writer
    took it meanwhile, this repository's unexported snapshots move above it, and the result
    reports the final number;
  - then the branch head is written only if it is still the version this repository started
    from (S3 `If-Match`).
- **Conflicts.** If another writer moved the head meanwhile, the call fails with
  `[differential:branch_conflict]`. The snapshot stays: it is local and exported, but the head
  is unchanged, and the branch is *diverged* (`differential_branches(db).remote_state`). It
  publishes nothing more until it is reconciled; after DETACH:
  ```sql
  SELECT * FROM differential_resolve_conflict('team', keep => 'branch');  -- work kept as main-conflict-<n>
  SELECT * FROM differential_resolve_conflict('team', keep => 'remote');  -- work kept as a local snapshot only
  ```
  Either way, the branch's contents are preserved first, and the branch then follows its S3
  head again.
- **Background conflicts.** A conflict found by a background publication marks the branch
  diverged as well. It shows in `differential_branches`, and the next `differential_create_snapshot`
  reports it. A diverged branch keeps taking local checkpoint snapshots but publishes none.
- **Automatic forks.** With `ATTACH … (ON_CONFLICT 'branch')`, a conflicting snapshot is
  published as `<branch>-conflict-<snapshot>` instead, also in the background. `differential_create_snapshot` then
  succeeds with `state = 'published_as_branch'` and the new branch's `branch_id`. The attachment
  keeps writing its branch (now `forked` in `remote_state`). At the next ATTACH without new changes,
  the branch follows its S3 head again. A later conflicting snapshot forks again.
- **Branches.** `differential_create_branch` creates the new head only if S3 has no branch of that
  name (`name_conflict`); of two concurrent creations, the later one gets `branch_conflict`.
  Operations publish only the heads of the branches they change, and
  `differential_retry_export` publishes all of them.
- **Readers** attach heads and exports as for any database.

- **Who is writing.** A writable attachment keeps an advisory "being written" marker in S3 for
  its branch:
  - it is renewed every 30 seconds while the session lasts;
  - it is removed when the branch's file closes;
  - it expires 2 minutes after its last renewal (after a crash), and the next ATTACH of the
    branch removes it.

  At ATTACH, other live writers of the branch produce a warning, or a refusal with
  `ON_BUSY 'error'`. `differential_writers(db)` lists the markers: `branch_name`, `writer_id`,
  `host`, `repository_id`, `process_id`, `started_at`, `renewed_at`, `live` and `this_process`.
  They assume host clocks within about a minute of each other and never affect correctness.

`differential_branches(db)` reports, without an S3 request, the branch's state as of its last
S3 update or ATTACH:
- `remote_state` is `in_sync`, `ahead` (newer local snapshots), `unpublished`, `diverged` or
  `forked` (NULL for other databases);
- `remote_snapshot_id` is the S3 head as last seen;
- `kept_branch` is a fork's name.

History and management in shared databases:
- **Other writers' snapshots.** They come from the S3 commit records and are cached locally;
  records never change, so only new ones are fetched. Their bytes stay in S3.
  - `differential_snapshots(db)` refreshes the cache and lists them with
    `publication_state = 'remote'`.
  - `SNAPSHOT_VERSION` and `SNAPSHOT_NAME` refresh it too, and attach them through their exports.
  - `AT (VERSION => n)` uses the cache as last refreshed (by a listing, a snapshot attachment, or
    in the background after each writable ATTACH), since binding never reads S3.
- **Time travel by time.** `SNAPSHOT_TIME` and `AT (TIMESTAMP => t)` follow the branch's
  ancestry, the parent links of its commit records, to the newest retained snapshot published at
  or before `t`. A branch that is not local starts at its S3 head.
- **Branches and restores** can start from, or return to, another writer's snapshot. Its map is
  imported, but not its data.
- **Expiry** (`differential_expire_snapshots`) records each published snapshot as expired in S3,
  so it is retired for every writer. A snapshot that is a branch head in S3 cannot expire. Its data
  is deleted only by `differential_cleanup_remote_files`, one grace period after nothing reaches it.
- **Dropping a branch** (`differential_drop_branch`) needs its S3 head still at this repository's
  base. It then replaces the head with a tombstone, conditionally, which drops the branch for
  every writer and reader. Another repository's unchanged copy is dropped at its next ATTACH; a
  copy with changes is reconciled like a conflict. The name can be used again.
- `differential_claim_branch_heads` is refused.

**Interruptions.** A process that dies or loses S3 partway through a publication leaves a
consistent state; the operation finishes when it is repeated, with no second snapshot number and
no false conflict.
- A snapshot publication finishes with `differential_retry_export` or the next writable ATTACH.
- Expiry, a branch drop and a conflict resolution finish when the same call is repeated (with the
  same `request_id` where one was given).
- An interrupted adoption leaves nothing behind.

**Host clocks.** Snapshot numbers, heads and conflict detection never use clocks.
- **Time travel by time** compares snapshots' publication times, taken from their writers' clocks.
  A snapshot from a host whose clock runs ahead is "in the future" for `AT (TIMESTAMP => now())`.
- **Writer markers** compare the writer's renewal time with the reader's clock. A marker from a
  host whose clock runs behind by more than the 2-minute lease looks expired, so it gives no
  warning.

The number of an unexported snapshot (for example with `export_mode => 'deferred'`) can still
change when it is exported.

### WAL shipping and followers

Snapshots are the unit of S3 durability: without WAL shipping, losing the writer's host loses the
commits since the latest exported snapshot. `SHIP_WAL` closes that gap for single-writer databases.

```sql
ATTACH 'differential:sales' AS sales (DATA_PATH 's3://my-bucket/lake/', SHIP_WAL 'sync');
INSERT INTO sales.orders VALUES (1, 'shipped before the commit returns');
SELECT branch_name, ship_wal, shipped_epoch, shipped_at, ship_pending FROM differential_branches('sales');
```

- **What is shipped.** Every publication of the branch's files (main file and WAL) is recorded in the
  same local transaction and shipped as an epoch: WAL bytes appended since the last epoch and the
  256 KiB main-file chunks that changed, inline. Epochs are create-only objects under
  `dbs/<uuid>/log/<branch>/…`, chained by hash. A chain starts from an internal base export of the
  main file after each checkpoint (and after 256 epochs or 64 MiB of epoch data).
- **`async`** (the commit returns after its local commit): epochs follow at most every
  `SHIP_INTERVAL` seconds, so a busy branch costs at most one PUT per interval (plus packs for a new
  chain's base). Losing the host loses what `ship_pending` shows as not yet shipped. Failures are
  retried in the background.
- **`sync`**: a commit also waits for one PUT. Losing the host loses no acknowledged commit; if the
  upload fails, the commit reports `[differential:ship_failed]` (it is durable locally).
- **Recovery and failover.** `differential_recover_branch` rebuilds the branch on any host with
  bucket access; with `same_identity => true` that host continues the database, turns shipping on
  with `SHIP_WAL` and ships into a newer chain. A copy of the repository that still ships the same
  branch then gets `head_conflict` (epochs are create-only).
- **Remote collection** keeps each branch's newest chain and collects superseded ones.

```sql
-- another host, after losing the writer:
SELECT database_name, shipped_epoch FROM differential_recover_branch(
    's3://my-bucket/lake/heads/sales/main', 'sales', same_identity => true);
ATTACH 'differential:sales' AS sales (SHIP_WAL 'sync');
```

**Followers** keep a read-only attachment current without re-attaching:

```sql
ATTACH 'differential:s3://my-bucket/lake/heads/sales/main' AS sales_live (FOLLOW true, FOLLOW_INTERVAL 10);
SELECT count(*) FROM sales_live.orders;          -- the newest shipped commit (or exported snapshot)
SELECT alias, follows, epoch, checked_at, error FROM differential_followers();
```

- A follower of a shipping branch follows commits (base export plus applied epochs); of any other
  exported branch, its exported snapshots.
- A background thread checks S3 once per interval: one request while nothing changed (a HEAD of
  the next epoch key, or of the branch head). New states are opened in the background as hidden
  read-only attachments, so a query never waits for S3 while binding.
- Each transaction reads one state; the next transaction reads the newest. A state caught during a
  checkpoint is skipped until the checkpoint completes. `AT` is not available on a follower, and
  catalog listings (`duckdb_tables()`) describe the state at ATTACH.
- DETACH stops the follower.

#### S3 request costs

The requests below are what the extension issues; the prices are Amazon S3 Standard list prices
in `us-east-1` at the time of writing (PUT, COPY, POST and LIST: $0.005 per 1,000; GET and HEAD:
$0.0004 per 1,000). Other regions, storage classes and S3-compatible services price differently, so
check your provider's price list. A month is 30 days.

**Requests per event**

| Event | Requests |
| --- | --- |
| `async` epoch (any number of commits within one `SHIP_INTERVAL`) | 1 PUT (multipart above `MULTIPART_THRESHOLD`, default 16 MiB: create + one PUT per part + complete) |
| `sync` commit | 1 PUT (concurrent commits of one branch can share an epoch) |
| New chain (after each completed checkpoint, 256 epochs or 64 MiB) | 1 LIST and 1 HEAD to number the chain, 2 LISTs of remote-collection records, the base export (one PUT per new 8 MiB pack of changed data, one per index page, one descriptor), epoch 0 and a redirect epoch in the old chain: typically 3 LISTs, 1 HEAD and 4–6 PUTs for small changes |
| Idle branch, shipping on | none |
| Idle follower, per `FOLLOW_INTERVAL` | 1 HEAD (next epoch key, or the branch head for a follower of snapshots) |
| Follower, per new epoch | 1 HEAD and 1 GET per 8 MiB (`MAX_RANGE_BYTES`) of the epoch (the check that finds no further epoch is the poll's HEAD) |
| Follower, new chain or new snapshot | the epochs of the chain from epoch 0 (HEAD + GET each), the base descriptor and index pages; table data is read on demand like any export |
| `differential_recover_branch` | 1 LIST, then HEAD + GET per epoch of the newest chain, the base descriptor and index pages; data on demand |

**Shipping, per busy branch (commits at least every interval), per month**

| Configuration | PUTs per month | Cost per month |
| --- | ---: | ---: |
| `async`, `SHIP_INTERVAL 0.2` | 12,960,000 | $64.80 |
| `async`, `SHIP_INTERVAL 1` (default) | 2,592,000 | $12.96 |
| `async`, `SHIP_INTERVAL 5` | 518,400 | $2.59 |
| `async`, `SHIP_INTERVAL 10` | 259,200 | $1.30 |
| `async`, `SHIP_INTERVAL 60` | 43,200 | $0.22 |
| `sync`, 1 commit per second | 2,592,000 | $12.96 |
| `sync`, 10 commits per second | 25,920,000 | $129.60 |
| `sync`, 100 commits per second | 259,200,000 | $1,296.00 |

Branches that are idle part of the time cost proportionally less; an idle branch costs nothing.
`async` cost depends on the interval, not on the commit rate (1,000 commits in one interval are
one epoch); `sync` cost grows with the commit rate. Chain starts add a few requests per checkpoint:
1,000 checkpoints a day (roughly 16 GB of WAL with DuckDB's default 16 MiB checkpoint threshold)
add about 3,000 LISTs and 5,000 PUTs a day, about $1.20 a month.

**Followers, each, per month while the branch is idle**

| `FOLLOW_INTERVAL` | HEADs per month | Cost per month |
| --- | ---: | ---: |
| 1 s | 2,592,000 | $1.04 |
| 5 s | 518,400 | $0.21 |
| 10 s (default) | 259,200 | $0.10 |
| 60 s | 43,200 | $0.02 |
| 300 s | 8,640 | $0.003 |

While the branch is busy, each follower also reads every epoch once: one HEAD and one GET per
epoch (for `async` at most one epoch per `SHIP_INTERVAL`). A follower of a branch that ships an epoch
every second therefore adds about 2.6 million HEADs and 2.6 million GETs a month, about $2.07 on top
of its polling, and 100 such followers about $207. With `FOLLOW_INTERVAL` longer than
`SHIP_INTERVAL` a follower reads several epochs per check; the requests per epoch stay the same, so
a longer follow interval saves only polling, not epoch reads.

**Other costs**

- **Storage.** A chain holds at most about 64 MiB of epochs plus its base export's new packs;
  superseded chains are deleted by `differential_cleanup_remote_files` after its grace period
  (default a week), so shipping adds roughly a week of chains per branch.
- **Data transfer.** Requests from compute in the bucket's region transfer data for free on AWS;
  followers and recovery outside the region pay egress for the epochs and data they read.
- **Measure your own.** `differential_storage_stats()` counts requests per type (`s3_put_calls`,
  `s3_get_calls`, `s3_head_calls`, `s3_list_calls`) and shipped epochs and bytes (`ship_epochs`,
  `ship_epoch_bytes`); `differential_followers()` reports each follower's `polls` and `changes`.

### Caching S3 reads

S3 export handles enable DuckDB's native block prefetch with the default
`storage_block_prefetch = 'REMOTE_ONLY'`. `SET storage_block_prefetch = 'NEVER'` disables
that layer; the extension's own cache-backed read-ahead remains separate. Local handles
retain DuckDB's local prefetch behavior.

Reads of S3 data (exports, branch heads, evicted snapshots) are cached at three levels:

| Level | Holds | Lifetime | Control |
| --- | --- | --- | --- |
| DuckDB's buffer pool | Blocks of attached databases | While attached | `memory_limit` |
| DuckDB's external file cache | Verified S3 data (index pages, pack ranges) | The DuckDB instance; survives DETACH | `memory_limit`; `SET enable_external_file_cache = false` turns it off; listed by `duckdb_external_file_cache()` |
| Repository disk cache | Verified chunks and index pages, by content hash | Across sessions, processes and users of the repository | `SET differential_remote_cache_size = '4GB'` (off by default) |

```sql
SET differential_remote_cache_size = '4GB';
ATTACH 'differential:s3://my-bucket/lake/heads/sales' AS latest;   -- later sessions read from disk
SELECT metric, value FROM differential_storage_stats() WHERE metric LIKE 'remote_%cache%';
```

- Every cached byte was verified against its content hash, and disk entries are verified again
  when read; a damaged file is dropped and downloaded again.
- The disk cache lives in `<repository>/cache/` with the repository's file modes. When it
  outgrows its size, the least recently used files are deleted. It is never needed for
  correctness: deleting the directory is always safe.
- Rehydration and `differential_recover_export` read the caches but do not fill them.
- The external file cache belongs to one DuckDB instance (all its connections share it); the
  disk cache is shared by every process of the repository.
- The external file cache is filled in the background, shortly after each S3 read (a read that
  arrives first fetches again). Reads use only cached data that is still in memory, never forcing
  other memory out while DuckDB loads a block.

**Reuse one DuckDB instance.** Open one database and share it (one connection, more
connections from it, or a connection pool). Each new instance starts with an empty memory cache;
in Python, every `duckdb.connect()` without a file (or with `':memory:'`) is a new instance, and
so is every in-memory JDBC connection. If your application opens an instance per request or job,
set `differential_remote_cache_size` so that new instances read from local disk instead of S3.

These examples assume the s3 secret above on the reader host too; otherwise add S3 options such
as `REGION` and `ENDPOINT` to each ATTACH and function call.

### Secrets

S3 settings can come from DuckDB secrets ([secrets manager](https://duckdb.org/docs/current/configuration/secrets_manager)),
so ATTACH and function calls need no S3 options.

Configure DuckDB's `secret_directory` and `allow_persistent_secrets` before using secrets
or attaching Differential. ATTACH looks up the default differential secret and initializes
DuckDB's secret storage; DuckDB rejects changes to those settings after initialization.

**`TYPE s3` secrets** are the standard S3 secrets of httpfs (which DuckDB autoloads on
`CREATE SECRET`). For every S3 location, the extension uses the secret whose `SCOPE` matches
best, exactly like httpfs:

| s3 secret parameter | Used as |
| --- | --- |
| `KEY_ID`, `SECRET`, `SESSION_TOKEN` | Credentials |
| `REGION` | `REGION` |
| `ENDPOINT` (+ `USE_SSL`) | `ENDPOINT`; `USE_SSL false` means `http://` and `ALLOW_HTTP` |
| `URL_STYLE 'path'` | `PATH_STYLE true` |

```sql
-- Keys, scoped to one prefix
CREATE SECRET lake_keys (TYPE s3, KEY_ID '…', SECRET '…', REGION 'eu-central-1',
                         SCOPE 's3://my-bucket/lake/');
-- Your AWS environment (SSO, profiles, roles; needs the aws extension, which DuckDB autoloads);
-- REFRESH keeps temporary credentials fresh
CREATE SECRET lake_aws (TYPE s3, PROVIDER credential_chain, PROFILE 'analytics', REFRESH auto, REGION 'us-east-1');
-- An S3-compatible service such as MinIO
CREATE SECRET minio (TYPE s3, KEY_ID '…', SECRET '…', ENDPOINT 'localhost:9000',
                     URL_STYLE 'path', USE_SSL false, SCOPE 's3://test-bucket/');
```

**`TYPE differential` secrets** hold the extension's own settings: `DATA_PATH` and the S3
options above. They hold no credentials and take no `SCOPE`.

```sql
CREATE SECRET lake (TYPE differential, DATA_PATH 's3://my-bucket/lake/', MAX_REQUESTS 2);
ATTACH 'differential:sales' AS sales (SECRET lake);    -- named: used when SECRET names it
CREATE SECRET (TYPE differential, DATA_PATH 's3://my-bucket/lake/');
ATTACH 'differential:orders' AS orders;                -- unnamed: used when no SECRET is given
```

Functions with S3 options (`differential_remote_heads`, `differential_import_database`,
`differential_recover_export`) accept `secret => 'name'`.

Rules:
- **Precedence.** For each option: the statement, then the differential secret, then the
  matching s3 secret, then the default. Without credentials from a secret, the AWS SDK resolves
  them (environment, `~/.aws`, instance roles); a data path registered with
  `CREDENTIAL_PROFILE` always uses that profile.
- **Creation only.** Settings from secrets apply when a database or S3 location is first
  registered. An existing database keeps its data path and S3 configuration.
- **Never stored.** The extension reads credentials at every ATTACH and function call and keeps
  them in memory only: not in the repository, descriptors, branch heads, listings or errors.
  `CREATE OR REPLACE SECRET` takes effect at the next ATTACH or function call; background
  exports and reads use the credentials looked up last.
  `differential_providers().credential_source` shows the secret in use.
- **Persistence.** DuckDB stores `CREATE PERSISTENT SECRET` secrets unencrypted in
  `~/.duckdb/stored_secrets`. Prefer temporary secrets or `PROVIDER credential_chain` for keys.
- Credentials are never accepted as ATTACH or function options.
- Without a secret or `CREDENTIAL_PROFILE`, the AWS SDK's default credential chain is built once
  per process. Some of its providers query the EC2 instance metadata service; on hosts outside
  EC2 where that endpoint answers slowly, set `AWS_EC2_METADATA_DISABLED=true` or use a profile.

S3 limits: `RETRIES` 0–10 (2), `CONNECT_TIMEOUT_MS` 1–30000 (1000), `REQUEST_TIMEOUT_MS`
1–300000 (5000), `MAX_RANGE_BYTES` 1–8 MiB (8 MiB), `MULTIPART_THRESHOLD` 5–512 MiB (16 MiB).
`MAX_REQUESTS` 1–64 optionally caps one S3 location below the process limit; without it, the
location follows `differential_s3_max_requests`. SDK retries run within the same request permit.

### Parallel S3 requests

`SET differential_s3_max_requests = 32` (1–64, default 16) sets how many S3 requests the process
runs at once, across all S3 locations and DuckDB instances in the process (the last value set
applies process-wide). More parallel requests mainly help on links with high latency, such as a
DuckDB host reading AWS S3 from outside the bucket's region or over the internet:

- Exports upload that many packs at once.
- Cold reads fetch that many ranges at once. Large fetches are split into requests of at least
  1 MiB. Each scanning thread's sequential reads get their own read-ahead, which grows to 4 MiB,
  so one round trip fetches many blocks instead of one.
- Memory outside `memory_limit`: up to `MAX_RANGE_BYTES` (8 MiB) per request in flight, so
  64 requests can hold about 512 MiB.
- Temporary disk during exports (`differential_export_staging = 'disk'`, the default): each pack
  upload is staged as a file of up to 8 MiB in `<repository>/staging/`. The repository allows
  8 MiB per parallel request (at least 64 MiB, at most 512 MiB at 64 requests); the files are
  deleted after each upload. With `'memory'`, uploads stream from memory and the process waits
  for room within the same budget instead.

To choose a value for your network, run `benchmarks/s3_throughput.py` against a bucket you own
([development](#development-and-limits)).

## Local pools

`DATA_PATH '/absolute/payload/path'` stores main-file and WAL bytes as immutable files
under `<path>/differential-pools/<repository-UUID>/<provider-UUID>`. Relative paths resolve
once to a canonical absolute path, and the same path reuses one pool. Maps, ownership and
control records stay in the repository's metadata store, so both locations must stay
available. A pool is neither a standalone backup nor a second metadata store.
Branches and clones inherit the data path.

## Introspection

| Function | Contents |
| --- | --- |
| `differential_snapshots(database)` | `snapshot_id`, `snapshot_time`, `snapshot_name`, `author`, `commit_message`, `commit_extra_info`, branch, capture time, root/size, retirement, local/export state, locator, `uri`, lineage |
| `differential_current_snapshot(database)` | `id`: the attached snapshot for a snapshot alias, else the latest public snapshot of the attached branch (alias) or `main` |
| `differential_branches(database)` | UUID/name, state, root, epoch, head snapshot, creation and lineage, canonical `uri`; shared databases: `remote_state`, `remote_snapshot_id`, `kept_branch`; WAL shipping: `ship_wal`, `shipped_chain`, `shipped_epoch`, `shipped_at`, `ship_pending` |
| `differential_databases()` | `database_id`, `database_name`, `data_path`, `last_snapshot_id`, `created_at`, `writers` (`single` or `shared`) |
| `differential_providers()` | Data paths, the S3 configuration and limits (never credentials), and `credential_source`: the DuckDB secret in use, the profile, or the AWS SDK default chain |
| `differential_operations()` | Request IDs, durable operation state and result identities |
| `differential_retention_roots()` | Current retention reasons and root identities |
| `differential_storage_stats()` | Lock-free counters: timings (`<name>_calls`, `_nanoseconds`, `_max_nanoseconds`, `_stddev_nanoseconds`, `_p50/_p95/_p99_nanoseconds`; per S3 operation type `s3_request_*`), WAL shipping (`ship_*`) and followers (`follow_epochs_applied`), payload/manifest/page counts, GC queues, active and retired segment bytes, WAL checkpoints, SDK calls/bytes/retries, remote memory and disk cache, coalesced ranges, prefetch, packs. For S3 ATTACH: `attach_resolve` (repository, secrets, provider registration, branch-head lookup), `branch_head_resolve`, `attach_repository_open`, `s3_secret_lookup`, `s3_provider_registration`, `remote_descriptor_open` (client and descriptor GET), `s3_client_create`, `s3_credentials`, and per request `s3_dns_lookup` and `s3_connect` (DNS + TCP + TLS, new connections only) and `s3_first_byte` |
| `differential_remote_heads(location, S3 options, secret =>)` | Branch heads in an S3 location: `database_name`, `database_id`, `branch_name`, `branch_id`, `snapshot_id`, `snapshot_time`, `export_locator`, `head` (attachable path) |
| `differential_followers()` | Followers of this process: `alias`, `database_name`, `branch_name`, `follows` (`commits` or `snapshots`), `follow_interval_ms`, `chain`, `epoch`, `snapshot_id`, `state_time`, `checked_at`, `polls`, `changes`, `error` |
| `differential_writers(database)` | Shared databases: "being written" markers in S3: `branch_name`, `writer_id`, `host`, `repository_id`, `process_id`, `started_at`, `renewed_at`, `live`, `this_process` |
| `differential_status()` | Implementation scope, durability profiles and unsupported guarantees |

List functions read a consistent snapshot of the metadata store. SDK counters are process-wide API calls,
not wire-level traffic. Cache memory is separate from DuckDB buffer memory.

## Execution rules

Apart from AT reader pins (above), all persistence happens during execution, once per table-function global state. Mutations
require autocommit. Plain EXPLAIN and PREPARE have no effects; EXPLAIN ANALYZE executes.
DuckDB rejects `PREPARE … AS CALL`; use `PREPARE p AS SELECT * FROM
differential_create_snapshot('sales', request_id => 'capture-001'); EXECUTE p;`.
ATTACH is an executed statement: it may create the repository, a database or an S3
provider registration. Cancellation can occur after local publication, so inspect
`differential_operations()` and reuse request IDs. A snapshot or export failure cannot roll
back an acknowledged native commit.

IDs are positive BIGINTs that fail on exhaustion. Wall-clock times can tie or move;
publication IDs stay transactionally ordered. Capture time is the time the checkpoint
returned, not a transaction's commit time. Qualified views and macros keep native DuckDB
definitions: snapshot attachments do not rewrite explicitly database-qualified references.
Attach under the original alias in a separate instance when those definitions
must resolve within the snapshot (`test/integration/qualified_catalog.py`).

## Development and limits

Test builds (`make integration`, `-DDIFFERENTIAL_TEST_HOOKS=ON`) add the Phase 0
pass-through backend (`DIFFERENTIAL_PROBE_ROOT`), diagnostic raw addressing
(`DIFFERENTIAL_ENABLE_PROBES=1`), probe tracing, failpoints and
`differential_probe_checkpoint`. Release builds compile none of them, and
`differential_status()` reports `test_hooks`.

Fixed bounds:
- 1 TiB logical files; a 12 KiB first chunk followed by 256 KiB chunks (one per native block),
  stored as 256 KiB objects; WAL sidecars stored as byte ranges. Manifest pages have 256 slots
  (the first covers 64 MiB − 244 KiB, later pages 64 MiB).
- Buffered writes per database are published in the background beyond 16 MiB and by the writer
  itself beyond 64 MiB (256 MiB for all databases together); segments are retired beyond 64 MiB;
  a 64 MiB local verified-chunk cache.
- 8 MiB S3 packs. Remote reads are cached in DuckDB's external file cache (within
  `memory_limit`) and, when `differential_remote_cache_size` is set, in the repository disk cache.
- Upload staging of 8 MiB per allowed parallel request (64 MiB up to 512 MiB);
  `differential_s3_max_requests` (default 16, up to 64) parallel S3
  requests per process.

There is no disk quota for authoritative storage. Failover and remote export expiry are not
supported; several writers per database only as [shared databases](#shared-databases-several-writers).

The local store format is 1 (header-aligned chunks) and the control format is 1. Repositories
in any other format fail with `incompatible_format` (there are no migrations). Native DuckDB
bytes are unchanged.

Stable error prefixes use `[differential:<category>]`: `not_found`, `retired`,
`name_conflict`, `read_only_attachment`, `writer_busy`, `stale_epoch`,
`active_attachment_or_transaction`, `unsupported_provider_capability`,
`incompatible_format`, `corrupt_data`, `unavailable_object`, `disk_full`,
`incomplete_export`, `head_conflict`, `branch_conflict`, `busy`, `access_denied`, `staging_full` and
`cancelled`. Native SQL errors
remain DuckDB errors.

Timing counters are process-wide totals and call counts, including failed attempts.
Native commit timings include read-only commits. Explicit checkpoint timings exclude
automatic checkpoints already counted in commit time. Export time includes cache, provider
work and retry checks. SDK request time includes permit wait and internal retries.
`local_gc_queued_*` counts candidates awaiting the next collection step; queued candidates
may still be live.

### Temporary spill files

Set DuckDB's `temp_directory` to a local path when queries may exceed `memory_limit`.
This also applies to S3-backed databases. After loading Differential, a nonlocal SET fails
with `local_temp_directory_required`; ATTACH checks paths configured earlier. Validation
creates neither the directory nor the repository. An empty string disables spilling.

```sql
SET temp_directory = '/tmp/duckdb-spill';
```
