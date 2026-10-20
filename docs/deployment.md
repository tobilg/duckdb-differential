# Deployment: processes, users and hosts

`differential` keeps one repository per machine. For what can run concurrently and how to
isolate tenants, see [concurrency-and-multitenancy.md](concurrency-and-multitenancy.md). A repository holds an embedded SQLite metadata
store ([why](architecture.md#why-sqlite-for-metadata)) and, by default, the data. Optionally it
also exports published snapshots to S3. This page describes who can share what, as currently
implemented and tested.

## Same machine

| Setup | Supported? | How |
| --- | --- | --- |
| Several DuckDB processes, one repository, one OS user | **Yes** | The metadata store's file locking and transactions coordinate them |
| Writers on different branches at once | **Yes** | One writer per branch, enforced by a lock file per branch |
| Two writers on the same branch | **No** (rejected with `writer_busy`) | Same as DuckDB's one-writer rule for a file |
| Readers in other processes while writing | **Yes** | Readers attach immutable snapshots or use `AT (VERSION …)` |
| Restore, branch drop and maintenance from another process | **Yes** | Management locks and resumable intents |
| Different OS users on one repository | **Yes, if the repository was created shared** (default: no) | `SET differential_repository_shared = true` before first use: setgid group-writable directories (`02770`) and group-writable files (`0660`) for the repository directory's group. Private repositories are `0700`/`0600` |
| Repository on a network filesystem (NFS, SMB) | **No** (untested) | SQLite and the file locks need local-filesystem locking semantics |
| Opening `engine.sqlite` with another SQLite library in a process that has the extension loaded (e.g. DuckDB's `sqlite` extension, Python's `sqlite3` beside an embedded DuckDB) | **No** (can crash or corrupt the store) | The extension links its own SQLite; a second copy in the same process ignores its locks. Inspect from a separate process; see [Inspecting a repository](storage-format.md#inspecting-a-repository) |

### Sharing a repository between local users

Sharing is chosen when a repository is created and recorded in it:

```sql
SET differential_repository = '/srv/differential';   -- a directory owned by the shared group
SET differential_repository_shared = true;            -- only needed by whoever creates it
ATTACH 'differential:sales' AS sales;
```

- **Group:** new directories and files belong to the repository directory's group (setgid
  directories), and that group can read and write everything. Create or `chgrp` the directory
  for the intended group first. Use a dedicated group: every member can read and change all
  databases in the repository.
- **Umask:** modes are set explicitly, so a restrictive umask does not break sharing. That also
  covers local payload pools (`DATA_PATH '/directory'`) created by a shared repository.
- **Joining:** users opening an existing shared repository need no setting.
- **Fixed at creation:** asking for sharing on an existing private repository is an error. To
  convert, create a new shared repository and copy the databases over.
- **Tested:** file and directory modes under umask `000` and `077`, a second session joining a
  shared repository, and both refusal paths. Actual access by a second OS account is not
  tested (it needs another user on the test machine).

## Local storage vs S3

S3 is an export target for published snapshots, not an alternative primary store:

| | Local repository | S3 (`DATA_PATH 's3://…'`) |
| --- | --- | --- |
| Role | The working store: every write, branch, snapshot and all metadata | Immutable copies of published snapshots, plus one small pointer per branch (its head) |
| When written | Every commit | When a snapshot is published, a branch is created, or a snapshot expires (heads), and by `differential_retry_export` |
| Metadata (branches, history, operations) | Yes | Branch heads only. Descriptors describe one snapshot each |
| Durability | Commits, at DuckDB's sync points | Snapshots, once their export completes |
| Mutable? | Yes | Snapshot objects never; branch heads are overwritten in place |
| Reading | Local, no network | Downloads what a query needs; cached in memory (DuckDB's external file cache) and optionally in the repository's disk cache (`differential_remote_cache_size`) |

With S3 configured, writes still need the local repository. S3 adds off-host durability, sharing
and the option to drop local copies (`differential_evict_snapshots`); reads of evicted
snapshots then come from S3.

## Several remote hosts with the same S3 data

| Scenario | Works today? |
| --- | --- |
| Many hosts reading exported snapshots at once | **Yes.** `ATTACH 'differential:s3://…/descriptors/<sha256>'` needs only bucket access (for example a `TYPE s3` secret), and the objects are immutable |
| Following a branch from another host | **Yes.** `ATTACH 'differential:s3://bucket/prefix/heads/sales/main'` attaches the latest exported snapshot of `sales/main`; re-attach to pick up newer snapshots, or add `(FOLLOW true, FOLLOW_INTERVAL 10)` to follow it automatically: every shipped commit when the database ships its WAL (`SHIP_WAL`), else every exported snapshot |
| Taking over a lost writer | **Yes, with WAL shipping.** With `SHIP_WAL 'async'` or `'sync'`, `differential_recover_branch('s3://bucket/prefix/heads/sales/main', 'sales', same_identity => true)` rebuilds the branch on another host as of the last shipped commit (none acknowledged is lost in `sync` mode) and that host continues the database. Without it, recovery starts from the latest exported snapshot |
| Discovering what a bucket offers | **Yes.** `differential_remote_heads('s3://bucket/prefix/')` lists every exported branch head |
| A host copying an export into its own writable database | **Yes.** `differential_recover_export` creates an independent copy with a new identity (a fork, not shared history) |
| Several writer hosts exporting into the same bucket and prefix | **Yes, without interfering.** Each database writes under its own UUID (`dbs/<uuid>/`). Two databases with the same name need the UUID form `heads/<name>/<uuid>/<branch>` when attaching |
| Several hosts writing the *same* database | **Only as a shared database** (`WRITERS 'shared'` at creation): each host writes locally and publishes optimistically; a host whose branch head moved meanwhile gets `branch_conflict` and keeps its snapshot ([concurrency](concurrency-and-multitenancy.md#shared-databases-several-writing-hosts)). Otherwise history and the branch lock live in one host's local metadata store |

### Branch heads

For every active branch of a database with an S3 data path, the extension maintains
`dbs/<database uuid>/branches/<branch>` under the prefix (attached as `…/heads/<database>/<branch>`). The object names the branch's
latest exported, non-expired snapshot and its descriptor (format in
[storage-format.md](storage-format.md#branch-heads)).

```sql
-- any host with read access, no repository of the writer needed
CREATE SECRET (TYPE s3, PROVIDER credential_chain, REGION 'us-east-1', SCOPE 's3://my-bucket/differential/');
ATTACH 'differential:s3://my-bucket/differential/heads/sales/main' AS sales_latest;
ATTACH 'differential:s3://my-bucket/differential/heads/sales' AS sales_main;  -- branch main
FROM differential_remote_heads('s3://my-bucket/differential/');
```

- **When heads change:**
  - after each completed export;
  - when a branch is created (it starts at its source snapshot);
  - when an expiry removes a branch's head snapshot (the head moves to the latest retained export).
- **Pending exports** (failed or `export_mode => 'deferred'`) do not move heads. A late export of
  an older snapshot never moves a head back.
- **Restore:** the head moves to the restored snapshot.
- **Dropped branches:** their last head stays readable in S3.
- **Failures:** if a head update fails, the snapshot export still counts as complete. The error
  says so, and `differential_retry_export('<database>')` republishes all heads.
- **Integrity:** readers check that the head and the descriptor it names agree on database and
  snapshot. A head attachment is read-only like any export attachment.
- **Concurrency:** a database has one repository, so head updates are serialized there with a
  lock. Heads are written conditionally (S3 `If-Match`): a repository only overwrites a head it
  wrote last. A copy of the repository on another host (a restored backup, a cloned VM) therefore
  cannot silently repoint the heads: its update fails with `head_conflict`, and the original's
  heads stay as they were. If the copy is meant to take over, run
  `differential_claim_branch_heads('<database>')` there; the original then gets `head_conflict`.
- **Requirement:** the S3 service must support conditional writes (`If-Match`, `If-None-Match`).
  AWS S3 and the tested MinIO release do.

Tested on the pinned MinIO fixture (`test/integration/s3_heads.py`): publication and attach by
name, branches, deferred and out-of-order exports, restore, expiry, two same-named databases in
one prefix, listing, errors. On real AWS, branch-head attach and listing were exercised manually.
