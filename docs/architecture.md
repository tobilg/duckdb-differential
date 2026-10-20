# Architecture

One loadable C++ extension uses stock DuckDB 2.0's exported native `DuckCatalog` and
`DuckTransactionManager`. DuckDB constructs its native single-file storage manager. The
filesystem adapter routes main-file and WAL/recovery operations through a durable
differential byte store. Native SQL, catalog, compression, indexes and transactions stay
upstream. No engine implementation is copied and no patched host, FUSE layer or daemon
is required.

Byte-level formats (repository layout, metadata schema, identities, segments, pools and S3
objects) are specified in [storage-format.md](storage-format.md).

| Boundary | Implementation |
| --- | --- |
| DuckDB integration | `src/duckdb_adapter/`: URI routing, branch leases, sync boundaries, header-sync generations, flush on close, native recovery connections, execution-only SQL functions, DuckDB secrets (`secrets.cpp`) |
| Differential byte store | `src/storage/store.cpp`, `segment.cpp`: chunk and log-range overlay, append-only segments (write journal and payload), publication at native sync, background publication, paged manifests, pooled readers, CRC32C-verified zero-copy reads, map and chunk caches, segment collection, background WAL checkpoints |
| Metadata store (SQLite) | `src/metadata/control.cpp` (identities, lifecycle, providers, exports), `retention.cpp` (pins, incremental GC, maintenance, compaction, statistics), `resolve.cpp` (URIs) |
| Additional local pools | `src/providers/local/local_provider.cpp`: immutable fan-out files, batched durable publication, owned namespaces, process-wide payload fence |
| S3 transport | `src/providers/s3/s3_provider.cpp`: AWS SDK credentials, signing, retries, conditional PUT, multipart, HEAD/ranges, listing and deletion |
| WAL shipping and followers | `src/providers/s3/shipping.cpp`: epochs and chains, the shipping worker, recovery, follower images; `src/duckdb_adapter/follow.cpp`: follower polling and per-transaction state choice |
| Export and remote reads | `src/providers/s3/snapshot.cpp`: packed exports, verified publication, parallel coalesced range reads with in-flight dedupe, rehydration and writable import; `remote_gc.cpp` (two-phase remote garbage collection); `disk_cache.cpp` (repository disk cache); `src/duckdb_adapter/remote_cache.cpp` (DuckDB's external file cache as the memory layer) |

## Why SQLite for metadata

Your databases stay ordinary DuckDB databases: native file format, catalog, SQL and
transactions. Around them the extension keeps its own bookkeeping: databases, branches,
snapshots, operations, pins, exports, and the chunk maps that tie each version to its stored
256 KiB objects. That bookkeeping lives in an embedded SQLite database in the repository, the same
choice DuckLake makes. DuckLake's
[catalog guide](https://ducklake.select/docs/stable/duckdb/usage/choosing_a_catalog_database)
recommends SQLite "for local data warehousing using multiple local clients" and notes that a
DuckDB catalog limits you "to a single client".

That is the deciding requirement. Several DuckDB processes share one repository: a writer per
branch, readers of snapshots in other processes, and maintenance or restore running alongside.
A DuckDB database file has one read-write process at a time. The metadata workload also suits
SQLite:
- many tiny transactions, one at every DuckDB sync point;
- point lookups by hash and range lookups by offset;
- a synced commit (`fsync`, as native DuckDB);
- triggers that keep the garbage-collection queues current;
- incremental vacuum to return freed space.

DuckDB is optimised for analytics, so it would add overhead on the commit path.

SQLite is linked statically into the extension and is invisible to users: nothing to install,
configure or connect to. It holds only metadata. The bytes of your DuckDB files live in
append-only segment files in the repository (with `DATA_PATH '/directory'`, as plain files per
chunk), so payload is written once, read with `pread` straight into DuckDB's buffers, and never
copied through SQLite's page cache or WAL.

## Write path

The first logical chunk covers DuckDB's 12 KiB header region; subsequent chunks cover
256 KiB each, so each native data block is one chunk. All objects are 256 KiB, with zero
padding. WAL sidecars are log files: byte ranges, so a WAL append stores only its bytes and
truncation and recovery stay byte-granular. See the [exact geometry](storage-format.md#core-idea).

Native writes modify an in-process overlay per inode (whole chunks, or byte pieces of a log
file) and are appended to the group's unsynced segment before returning. A completed write
therefore survives process death, the same as native writes into the page cache. Files publish
in groups: a database file and its WAL sidecars share a write gate, segment and overlay bound. A
native `FileSync` publishes its group's overlay: a chunk that one write covered whole (a native
block) is referenced where that write stored it; other chunks are appended once as object
records; WAL pieces are ranges of their write records. The segment is synced, then one SQLite
`FULL` (`fsync`) transaction (group commit: concurrent syncs of different groups share it, up
to 16 MiB of new data) records object locations, chunk maps, ranges, lengths and the segment's
published watermark; other databases keep writing meanwhile. A group with more than 16 MiB
buffered is published by a background thread with `synchronous=NORMAL` (the writer itself only
beyond 64 MiB; everything beyond 256 MiB in all); handle close publishes with NORMAL too, and
the next FULL commit makes it durable. Segments are synced before any transaction that
references them. A dead process's segment is replayed as a valid prefix before its branch is
reused. Overwritten chunks with no other reference are deleted in the publishing transaction;
retired segments are collected or compacted once little of them is referenced. SQLite WAL
checkpoints run on a background connection, and the WAL file is truncated to 64 MiB after one.

## Read path

Reads take only a shared per-inode lock and use a pooled, memory-mapped read-only SQLite
connection: one read snapshot per native read. Chunk maps are cached per inode (keyed by the
file's map version) and per immutable root page; object locations are cached per repository.
A whole-chunk read is a single `pread` into DuckDB's buffer, verified by CRC32C; partial reads,
log records and remote chunks go through a process-wide 64 MiB verified cache. A read that finds
a segment moved by compaction retries on fresh metadata. The payload fence,
which protects old SQLite snapshots of external-pool mappings from physical deletion, is
taken only for external pools.

S3 export handles report themselves as remote to DuckDB, enabling native block prefetch
under its default `storage_block_prefetch = 'REMOTE_ONLY'`. DuckDB batches adjacent blocks
before the extension resolves chunks and coalesces S3 ranges. Local files and mutable
local overlays retain local classification. `NEVER` disables native prefetch; it does not
disable the extension's independent cached S3 read-ahead.

Temporary spill files use a local `temp_directory`. The planner validates explicit SETs,
and ATTACH validates a preconfigured path before opening the repository. Neither validation
creates files. The filesystem also rejects Differential directory creation with a spill
policy error, covering settings supplied outside the normal SQL path.

## Manifests and generations

A root identity is SHA-256 over the logical length and the ordered list of 256-slot
manifest pages. Each page is stored once by its own hash, so consecutive generations
share unchanged pages, and sealing re-hashes only the pages changed since the last seal.

The adapter seals the latest internal generation at the `FileSync` that follows a native
header write, i.e. at every completed checkpoint, with or without `TYPE differential`. A
public snapshot runs a native checkpoint, then seals the main file under a capture pin
and publishes its ID/name/time/branch history in one transaction. Commits,
checkpoints, public publication and remote export are separate boundaries. SQL effects
happen once at execution, never at bind or plan time. With
`differential_snapshot_on_checkpoint` enabled, the adapter also publishes a public snapshot
after each sealed generation of a writable branch whose root changed.

## SQL surface

`ATTACH 'differential:<database>' (options)` is resolved by the storage extension's attach
callback before DuckDB builds its native storage manager. The callback consumes the
differential options (`BRANCH`, `SNAPSHOT_*`, `READ_ONLY`, `CREATE_IF_NOT_EXISTS`,
`DATA_PATH`, S3 options), may create the database or register its data path, and rewrites
the path to the canonical `diff://db/<uuid>/branches/<uuid>` or `…/snapshots/<id>` URI.
`differential:s3://…/descriptors/<sha256>` maps to `diff://export/<provider>/…`. Native
options pass through unchanged. The canonical URIs remain attachable directly with
`TYPE differential`. These attachments use `DifferentialCatalog`, a `DuckCatalog` subclass that
generates in-memory catalog-scoped functions and resolves `AT (VERSION|TIMESTAMP)` through
hidden read-only snapshot attachments. Settings are DuckDB extension options; `differential_repository` selects
the root lazily and refuses to change it after first use.

Secrets come from DuckDB's secret manager at execution time. A `TYPE differential` secret
(registered by the extension, settings only) and the best-matching `TYPE s3` secret fill options
the statement did not give, in that order; only explicit options are checked against an existing
registration. For every registered S3 provider, each ATTACH and management call looks up the
matching s3 secret and stores its credentials in a process-wide registry keyed by
`s3://bucket/prefix[ @ endpoint]`. The SDK client of a provider without `CREDENTIAL_PROFILE`
reads that registry on every request and falls back to the AWS SDK default chain, so background
exports and remote reads use the latest secret without storing it.

## Ownership and management

Restore/delete persist an intent under exclusive database management and branch locks.
A separate native connection recovers and checkpoints under that lease, then one
transaction preserves committed contents and swaps or tombstones the branch. The lease
travels in DuckDB client registered state through `FileOpener`, so thread migration does
not lose ownership. Pending intents fence ordinary writers after process death; repeating
the request resumes them.

All of this coordination uses file locks and SQLite transactions inside one repository, so it
works between processes and, for repositories created with `differential_repository_shared`,
between local users. A `Permissions` value owned by the store (private `0700`/`0600`, or shared
setgid `02770`/`0660`) is applied to every directory and file the repository creates: segments,
locks, pins, staging and local pools. See [deployment.md](deployment.md).

## Retention and collection

Historical opens validate retention and pin a root in one transaction. Native file handles
pin inodes. Pin lock files distinguish live local processes from dead ones. Triggers queue
every file, root, page and object that may have been released. Collection examines queued
candidates with indexed liveness checks and cascades from files to roots, pages and objects
within its budget. Retention reasons: named or open files, latest generations of live
files, retained non-evicted snapshots, active branches, reader pins, compound targets and
pending exports. Bounded maintenance runs on a background worker after checkpoints
(`differential_auto_gc`). Incremental SQLite compaction returns free pages without changing
object identities or reader roots.

## S3 profile

Publication first persists export retention, then uploads the chunks no export has stored
yet in content-addressed 8 MiB packs (concurrently, conditional PUT, checksum verified by the service),
then the paged indexes, and the self-contained descriptor last. Verified objects are
trusted by their local record in the owned, immutable namespace. The descriptor includes
engine/format identity, database/branch/snapshot identity, time, size, root, pool namespace
and index hashes and sizes. A fresh reader maps the namespace to provider access and fetches
the native ranges it needs. Misses of one read are coalesced per pack and fetched
concurrently, and concurrent readers share in-flight fetches. Verified data is cached in DuckDB's
external file cache of the reading instance (within `memory_limit`, surviving DETACH; filled by a
worker thread, because DuckDB reads database blocks while holding their locks, so a read must never
make the buffer manager evict) and,
when `differential_remote_cache_size` is set, in the repository's content-addressed disk cache,
shared by every process of the repository. Bulk copies read but never fill these caches.
Derived upload staging has a quota of one 8 MiB pack per allowed parallel request (64–512 MiB)
and process locks. Writable remote recovery copies the sparse map, verifies its manifest
and creates a new owned identity; it never resumes the lost allocator.

Branch heads (`dbs/<database uuid>/branches/<branch>`) are the only mutable S3 objects apart from
the advisory writer markers of shared databases (`dbs/<uuid>/writers/…`). After an
export completes, a branch is created or snapshots expire, the extension recomputes each active
branch's latest retained export from the metadata store and rewrites the heads that changed,
under a per-database lock. Remote readers attach a head to follow a branch without the writer's
repository ([deployment.md](deployment.md#branch-heads)).

Pending exports retain their local graph. Completed exports do not, so a retired exported
snapshot is collectible locally, and `differential_evict_snapshots` drops the local copy of
a live one, whose reads then go through S3. Branch, clone and restore rehydrate an evicted
root from its export when it is no longer local. There is no distributed lock authority or remote reader expiry, and remote reclamation is
explicit (`differential_cleanup_remote_files`).

WAL shipping (`SHIP_WAL`, opt-in, single-writer S3 databases) makes commits durable off-host as
well: publications of a writable branch are tracked in the publishing transaction and shipped by a
background worker as create-only, hash-chained epochs. A chain starts from a base (an internal export
of the main file captured as a root that is not a generation) and carries WAL bytes and changed
main-file chunks inline, typically one PUT per epoch. `sync` makes a native sync wait for its epoch.
`differential_recover_branch` imports a chain's base lazily and applies its epochs, so failover takes
seconds; with `same_identity` the new host continues shipping in a newer chain, and a copied
repository is detected as a conflict. Followers (`FOLLOW true`) apply epochs (or follow branch heads)
in a background poller and serve each transaction one immutable state through hidden read-only
attachments, so no S3 request happens while a query binds.

Shared databases (`WRITERS 'shared'`) let several repositories write one database through S3
without a lock authority. Each keeps its own local metadata store and publishes optimistically:
- snapshot numbers come from the create-only commit index; unexported local snapshots are
  renumbered above numbers taken meanwhile;
- a branch head moves only from the version its writer last wrote or adopted (S3 `If-Match`),
  and a moved head is a `branch_conflict`;
- a joining repository adopts a head's chunk map without its bytes; those objects are
  remote-only (`remote_objects`), and the byte store reads them from S3 on demand through a
  fetch hook the S3 layer installs.

## Additional local pools

Local pools (`DATA_PATH '/absolute/path'`) store immutable 256 KiB files under
`<path>/differential-pools/<repository>/<provider>/<xx>/<hash>`; their mappings stay in the
repository's metadata store. A publication batch `fsync`s each file, links all names, syncs directories and
flushes the device once before the mapping commits. Collection commits a durable deletion
queue, then removes files under the exclusive payload fence. Identical hashes in multiple
pools keep all registered replicas while any map references the hash.

## Test hooks

Fault injection, probe tracing, raw-path probes and the diagnostic checkpoint function
are compiled only into test builds (`-DDIFFERENTIAL_TEST_HOOKS=ON`). Release packages
contain none of them.
