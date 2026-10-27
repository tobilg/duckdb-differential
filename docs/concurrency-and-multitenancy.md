# Concurrency and multitenancy

This page explains who can read and write what at the same time, and how to keep several
tenants (teams, customers, applications) apart. For the deployment tables (same machine, local
storage vs S3, remote hosts) see [deployment.md](deployment.md).

In short: **within a repository, a branch has exactly one writer**, enforced by a lock. In S3, a
database has one writing repository, or, as a **shared database**, several that publish
optimistically: a branch head moves only from the version its writer started from. **The unit
of isolation is the repository**: the extension has no users, roles or per-database permissions,
so tenants are separated by giving each its own repository, OS user (or process) and S3 prefix.

## Concurrency

### What can run at the same time

| Activity | Concurrent with | How it is coordinated |
| --- | --- | --- |
| Writing a branch | Nothing else writing **that** branch | A writer lock per branch (`locks/branch-<uuid>`). A second writer, in another process, another DuckDB instance of the same process or another OS user, gets `[differential:writer_busy]` |
| Writing a branch | Writers on **other** branches of the same database | Branches are independent; each has its own lock and live file |
| Writing a branch | Readers of its snapshots | Readers attach immutable snapshots (`SNAPSHOT_*`, `READ_ONLY`, `AT (VERSION …)`); the writer never changes them |
| Several connections of one DuckDB instance | Each other | Ordinary DuckDB transactions on the attached branch (DuckDB's own MVCC) |
| Taking a snapshot | Writes in other transactions | A snapshot checkpoints the branch first; it waits for, or asks you to retry after, concurrent write transactions (`active_attachment_or_transaction`) |
| Restore, dropping a branch | Readers | These need the branch without a live writer. An interrupted operation leaves an intent that blocks new writers until the same request is retried |
| Garbage collection and compaction | Everything | Background steps skip anything a reader or writer still references (reader pins, retention roots) |
| S3 export | Writing | Exports run from sealed snapshots; the branch can keep changing meanwhile |

Management functions take a `request_id`: after an error, timeout or crash, repeating the call
with the same ID resumes it or returns the original result, and never applies it twice.

### Readers and freshness

A reader sees exactly one snapshot until it detaches. To see newer data, DETACH and ATTACH
again (or use `AT (VERSION => …)` with a newer version). The live branch itself is only ever
attached by its writer.

### Several hosts

By default a database belongs to exactly one repository, and therefore to one host:

- Other hosts **read** its published snapshots from S3, by export locator or through branch
  heads (`ATTACH 'differential:s3://…/heads/<database>/<branch>'`).
- Other hosts **cannot write** it. To continue elsewhere, `differential_recover_export` creates an
  independent copy with a new identity (a fork, not shared history). With WAL shipping
  (`SHIP_WAL`), `differential_recover_branch(..., same_identity => true)` lets another host take the
  database over as of its last shipped commit (a failover, not a second writer: a repository that
  still ships the branch gets `head_conflict`).
- Other hosts can **follow** a branch read-only with `FOLLOW true` on its head path: every shipped
  commit, or every exported snapshot.
- In S3, every data object (packs, index pages, descriptors) is named by its content hash and
  written only if absent, so no writer can overwrite another's data. Each database writes under its
  own UUID (`dbs/<database uuid>/`), including its branch heads.

### Shared databases: several writing hosts

A database created with `ATTACH … (DATA_PATH 's3://…', WRITERS 'shared')` is written by every
repository (host) that attaches it that way. Its metadata lives in S3, with no catalog server
and no lock:

| Step | What happens | S3 requests |
| --- | --- | --- |
| ATTACH | Reads the branch head. A host without the branch, or whose branch has no local changes, starts from the head; data is read on demand, with read-ahead for scans | 1 GET (joining: plus the descriptor and index pages) |
| Writing | Local, as for any database | none |
| A DuckDB checkpoint | Becomes a snapshot, published in the background as below (`PUBLISH_ON_CHECKPOINT false` turns this off; `PUBLISH_ON_DETACH true` makes DETACH wait for it) | as below |
| `differential_create_snapshot` | Uploads new chunks, takes the next free snapshot number (create-only commit record), then writes the branch head only if it is still the version this host started from | today's upload, plus one LIST and the head PUT |

Two hosts can work on the same branch at the same time. Whichever publishes first moves the head;
the other gets `[differential:branch_conflict]`. Its snapshot is exported and kept, but it is not
the branch head, and the branch is diverged until it is reconciled:
- `differential_resolve_conflict(db, keep => 'branch')` (after DETACH) publishes the host's work as
  `<branch>-conflict-<n>` and makes the branch follow S3 again;
- `keep => 'remote'` follows S3 and keeps the work only as a local snapshot;
- with `ATTACH … (ON_CONFLICT 'branch')` the fork happens at once, and the branch follows S3 at
  its next ATTACH.

Merging is not supported, so concurrent work on one branch means one host's snapshot ends up on
another branch.

Guarantees:
- no lost updates: a head only moves from the version its writer saw;
- snapshot numbers are unique within the database and increase along each branch;
- S3 data is never overwritten, and a crash between steps leaves only unreferenced objects;
- readers always see complete snapshots.

Interruptions (process death, lost or failing S3 responses) between any two steps of a publication,
expiry, branch drop, conflict resolution or adoption leave a consistent state, which repeating the
operation finishes. Host clocks affect only time travel by time and the "being written" warning.

Not guaranteed:
- commits between snapshots are durable only on their host;
- exclusive writing: a conflict surfaces when publishing, not when writing;
- clean-up of unreferenced S3 objects.

Every host sees the whole published history: `differential_snapshots`, `SNAPSHOT_VERSION`,
`SNAPSHOT_NAME`, `SNAPSHOT_TIME` and `AT` reach other hosts' snapshots through the S3 commit
records. Branches and restores can use them, expiry retires snapshots for every host (never a
branch head), and dropping a branch drops it for every host.

**Early warning.** Each writable attachment keeps an advisory "being written" marker in S3, renewed
while the session lasts and removed at DETACH. Attaching a branch that another host is writing
prints a warning; `ON_BUSY 'error'` refuses instead. `differential_writers(db)` shows who is
writing. A crashed writer's marker expires after 2 minutes. The markers prevent nothing: two
writers can still both write, and the later one to publish gets `branch_conflict`.

### Guarded branch heads

Branch heads (`dbs/<database uuid>/branches/<branch>`) are the only S3 objects the extension
overwrites. It writes them **conditionally**: a repository remembers the version (ETag) of each
head it wrote last and only overwrites a head that is still that version.

Every exported snapshot also gets a create-only commit record keyed by its number, so a copy that
publishes a snapshot number the original already used is caught there as well, with the same
`head_conflict` error; claiming then abandons that export and numbers new snapshots above the
newest published one.

This matters when a repository is **copied**, for example a backup restored on another host, a
cloned VM or a copied directory. Both copies then have the same identities and would otherwise
both update the same heads, so remote readers would see the heads jump between two diverging
histories. With guarded heads:

1. Whichever copy updates a head first keeps it.
2. The other copy's update fails with `[differential:head_conflict]`. Its export is still complete;
   only the head is left alone.
3. If that copy is meant to take over (the original is retired), run
   `differential_claim_branch_heads('<database>')` there. From then on the original gets
   `head_conflict`.

A head write whose response was lost is recognised as the repository's own, so network errors
do not cause false conflicts. Guarded heads need an S3 service with conditional writes
(`If-Match`, `If-None-Match`); AWS S3 and the tested MinIO release support them.

What guarded heads do **not** do: they detect a split brain at the first head update after two
copies diverge; they do not stop either copy from writing its own local data, or a writer on
another host from taking over a branch. There is no writer lease in S3.

### What is never supported

- **A repository on a network filesystem** (NFS, SMB) shared by several hosts: file locks are not
  reliable there, so the per-branch writer lock can fail. Use one repository per host.
- **Several hosts writing the same database**, unless it is a shared database (above).
- **Merging into an attached target or into a shared database.** [Branch merging](branch-merge.md)
  needs a detached target and a local or single-writer S3 database.

## Multitenancy

### Isolation by layer

| Layer | Isolation between tenants |
| --- | --- |
| Repository (a local directory) | **Strong, when each tenant has its own.** Private repositories are `0700`/`0600`, readable only by the OS user that created them |
| Databases in one repository | **None.** Anyone who can open the repository can read, write, branch, restore or drop every database in it |
| A shared repository (`differential_repository_shared`) | **None between members.** Every member of the directory's group has full access; meant for trusted collaborators |
| S3 | **As strong as your IAM policies.** Give each tenant its own prefix (or bucket) and credentials limited to it. Heads and listings cover a whole prefix, so tenants must not share one |
| Quotas | **None.** There are no per-repository or per-database limits on local disk or S3 storage, and the extension never deletes S3 exports |

### Recommended: one OS user (or container) per tenant

Each tenant gets:

- its own process, running as its own OS user or in its own container;
- its own private repository (`SET differential_repository = '/srv/tenants/<tenant>'`);
- its own S3 prefix and credentials limited to it (a `TYPE s3` secret scoped to the prefix, or an
  AWS profile).

Nothing in the extension is shared between tenants then, and CPU, memory and disk can be limited
with the usual OS or container tools.

### Possible with care: one DuckDB instance per tenant in one process

Each DuckDB instance can use its own repository (set `differential_repository` before first use;
it cannot change afterwards) and its own `memory_limit`. Per instance or per repository are: the
memory cache of S3 data (DuckDB's external file cache), the disk cache, open S3 clients, recently
read descriptors and in-flight downloads.

Shared by the whole process:

- **S3 request budget.** `differential_s3_max_requests` is one budget for the process, and the last
  value set applies. One tenant's large scan or export can slow everyone else's S3 traffic.
- **S3 credentials from DuckDB secrets** are kept per S3 location (bucket, prefix and endpoint),
  not per instance. Tenants must use different prefixes; two tenants with different credentials
  for the same prefix would share whichever was looked up last.
- **Statistics.** `differential_storage_stats()` counters are process-wide.
- **No OS boundary.** All repositories belong to the same OS user. If tenants run their own SQL,
  they can address any file that user can read unless you restrict DuckDB itself (for example
  `enable_external_access = false`); running the extension under such restrictions is not
  tested.

### Not suitable: many tenants in one repository

Any session can address every database in a repository by name, and there is no access control
below the repository.

### Sharing data with consumers

Publishing per-tenant data to readers works well: give a consumer read-only access to the
tenant's S3 prefix (for example an IAM policy on `s3://bucket/tenant-a/*` and a scoped `TYPE s3`
secret). They attach exports or branch heads read-only, without the writer's repository. Exports
are immutable and contain only what was published, so the S3 policy is the access control.

### Not available

- Per-database or per-branch permissions.
- Quotas and automatic clean-up of S3 exports.
- Per-instance S3 request budgets and credential lookup.
- Per-tenant metrics.
- An S3 writer lease (exclusive writing, or handing a single-writer database from one host to
  another). Shared databases publish optimistically instead.
