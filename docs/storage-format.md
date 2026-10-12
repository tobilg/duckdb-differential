# Storage mechanisms and formats

This page specifies how `differential` stores data: locally in a repository, in optional local
payload pools, and in S3 exports. Each component has exactly one format; anything else is
refused with `[differential:incompatible_format]`; there are no migrations:

| Component | Format identifier |
| --- | --- |
| Local store (`engine.sqlite`, `PRAGMA user_version`) | 1 |
| Control metadata (`control_format.version`) | 1 |
| Manifest page / root identity | `differential-page-v1` / `differential-manifest-v1` |
| Write journal | `DDJ1` |
| S3 export descriptor | `differential-export-v1` |
| S3 chunk index page | `DIX1` |
| S3 branch head | `differential-head-v1` |

For the design rationale, see the [architecture overview](architecture.md); for operations,
see the [lifecycle guide](lifecycle.md).

> The repository and the S3 layout are owned by the extension. Inspect them read-only; never
> edit, copy parts of, or delete files by hand. Use the SQL functions for every change.

## Core idea

DuckDB itself is not modified. It reads and writes an ordinary native database file, and
DuckDB 1.5.6 storage bytes are unchanged. The extension stores that file as a **logical file**
split into fixed **64 KiB chunks**. Each chunk is stored once, under its SHA-256 (an **object**).
A version of the file is therefore just a map from chunk index to object hash, so versions
share every unchanged chunk.

```
native DuckDB file (logical bytes)      chunk map (per file / per root)       objects
┌────────┬────────┬────────┬───       ┌───────┬──────────────┐             ┌──────────────┐
│chunk 0 │chunk 1 │chunk 2 │ …   ──▶  │ 0     │ sha256(c0)   │ ──────────▶ │ 64 KiB bytes │
└────────┴────────┴────────┴───       │ 1     │ sha256(c1)   │             │ (SQLite blob,│
                                      │ (2 absent = hole, reads as zeros) │  pool file,  │
                                      └───────┴──────────────┘             │  or S3 pack) │
                                                                           └──────────────┘
```

| Term | Meaning |
| --- | --- |
| **Logical file** | One native file: a branch's main database file, or its `.wal` / `.wal.checkpoint` / `.wal.recovery` sidecar. |
| **Chunk** | Bytes `[i·65536, (i+1)·65536)` of a logical file. The last chunk is zero-padded; the file length is stored separately. |
| **Object** | An immutable 64 KiB chunk, identified by the lowercase hex SHA-256 of exactly 65536 bytes. |
| **Hole** | A chunk with no mapping: never written, or truncated away. Reads return zeros; nothing is stored. |
| **Extent** | A live mapping `(file, chunk index) → object` of a writable file. |
| **Manifest page** | An immutable map of 256 consecutive chunk slots (16 MiB of logical file) to objects. |
| **Root** | An immutable version of a logical file: its length plus its ordered manifest pages. |
| **Generation** | The root sealed at a branch's latest completed native checkpoint (internal). |
| **Snapshot** | A public, numbered version of a database: a root plus ID, name, time, branch and commit metadata. |

## Repository layout

The repository is the directory selected by `SET differential_repository` (default
`$DIFFERENTIAL_STORE_ROOT` or `~/.duckdb/differential`). A private repository (the default) gets
mode `0700` directories and `0600` files. A repository created with
`SET differential_repository_shared = true` gets setgid group-writable `02770` directories and
`0660` files instead, for the repository directory's group. The mode is recorded in the
`repository` table and applies to everything the repository creates later, including local
payload pools. Modes are set explicitly, so the process umask cannot narrow them.

```
<repository>/
├── engine.sqlite (+ -wal, -shm)   metadata store and default chunk store (embedded SQLite, WAL mode)
├── journal/<uuid>                 per-process write journal of unsynced native writes
├── pins/<uuid>                    lock file of each reader pin; locked while its process lives
├── locks/
│   ├── branch-<branch uuid>       writer lock of a branch (one writer process)
│   ├── management-<database uuid> lock for restore / branch deletion
│   └── managed_<branch uuid>.wal  native sidecar file locks (mirrors DuckDB's own opens)
├── payload.lock                   payload fence: shared by publishers, exclusive for pool deletions
├── staging/<repository uuid>/     S3 upload staging, 8 MiB per parallel request (64–512 MiB), deleted after upload
└── cache/<2 hex>/<sha256>         optional disk cache of S3 chunks and index pages (never authoritative)
```

The repository UUID (`control_format.repository_id`) names this repository's namespace in local
pools and in S3.

### Logical file names

Inside the store, logical files have names, never paths on disk:

| Name | Content |
| --- | --- |
| `managed/<branch uuid>` | Live main database file of a branch |
| `managed/<branch uuid>.wal` (`.wal.checkpoint`, `.wal.recovery`) | Native WAL and recovery sidecars |
| `imports/<operation uuid>` | Staging file of a running import or export recovery |
| `rehydrate/<time>-<pin uuid>` | Temporary file while an evicted snapshot is downloaded again |

## The metadata store (`engine.sqlite`)

The metadata store is an embedded SQLite database, for the reasons in
[architecture.md](architecture.md#why-sqlite-for-metadata): several processes share a
repository, which a DuckDB catalog file does not allow. `engine.sqlite` has `application_id = 0x44494646` ("DIFF") and `user_version = 1`. It runs with
`journal_mode=WAL`, `synchronous=FULL`, `fullfsync=ON` and `auto_vacuum=INCREMENTAL`, with
foreign keys enforced. The extension refuses unrelated SQLite files and unknown versions with
`[differential:incompatible_format]`. One file holds two groups of tables.

### Byte store tables

| Table | Purpose |
| --- | --- |
| `files(id, name, size)` | Logical files. `name` is NULL once unlinked (still referenced by a handle or pin). |
| `objects(hash, data)` | Content-addressed chunks. `data` is the 65536-byte blob for the default store, NULL when the bytes live in a local pool or only in S3. |
| `remote_objects(hash)` | Objects whose durable copy is an S3 export (a shared database's adopted head, read on demand). Without local data they are fetched from the packs `remote_chunks` records. |
| `extents(file, chunk, object)` | Live chunk map of writable files. |
| `file_pages(file, page_no, page, dirty)` | Per-file cache of manifest pages; `dirty` pages are re-hashed at the next seal. |
| `manifest_pages(hash)`, `page_entries(page, slot, object)` | Immutable 256-slot manifest pages. |
| `roots(hash, size)` | Immutable roots. |
| `root_pages(root, page_no, page)` | The ordered manifest pages of a root. |
| `generations(file, root)` | Latest sealed root of each live file. |
| `pins(id, root, file)` | Reader and operation pins (with a lock file in `pins/`). |
| `local_pools`, `file_pools`, `local_objects`, `local_deletions` | Pool registry, file → pool, which pools hold which hashes, durable deletion queue. |
| `journal_state(journal, committed)` | Highest journal sequence committed per journal. |
| `allocators(name, next)` | Monotonic allocators (file IDs). |
| `repository(shared)` | One row: `1` if the repository was created for several local users, else `0`. |
| `reclaimed_roots`, `gc_files`, `gc_roots`, `gc_pages`, `gc_objects` | Collection state and queues; triggers enqueue every possibly released item. |

### Control tables

| Table | Purpose |
| --- | --- |
| `control_format(version, repository_id)` | Control version (1) and repository UUID. |
| `databases(id, name, last_snapshot, created_at, writers)` | Databases; `last_snapshot` is the per-database snapshot ID allocator; `writers` is `single` or `shared` (fixed at creation). |
| `branches(id, database_id, name, state, root, epoch, head, created_at, source_database, source_snapshot)` | Branches. `root` is the latest sealed generation, `head` the latest snapshot, `epoch` fences stale writers. Names are unique among active branches. |
| `snapshots(database_id, id, branch_id, name, root, published_at, captured_at, retired, evicted, author, commit_message, commit_extra_info)` | Public snapshots. Names are unique per database, including retired snapshots. |
| `history(sequence, branch_id, database_id, snapshot_id, effective_at)` | When each snapshot became a branch's state; drives `SNAPSHOT_TIME` and `AT (TIMESTAMP)`. |
| `operations(id, request, kind, payload, state, …)` | Durable, idempotent record of every mutation, keyed by `request_id`. |
| `compound_intents(operation_id, branch_id, expected_head, expected_epoch, target_root)` | Resumable restore / branch deletion. |
| `database_profiles`, `database_local_profiles` | A database's S3 provider or local pool (its `DATA_PATH`). |
| `providers`, `provider_limits` | Immutable S3 configuration (no credentials; secrets are never stored) and SDK limits (`max_requests` 0 = follow `differential_s3_max_requests`). |
| `export_roots(export_id, root, state)`, `exports(id, provider_id, database_id, snapshot_id, descriptor)` | Export retention (`pending` / `complete` / `abandoned`) and the descriptor key of each export. |
| `remote_packs`, `remote_chunks`, `verified_remote_objects` | What each provider already stores and in which database's pool, so exports upload only new chunks. |
| `remote_commits(database_id, id, parent, branch_id, branch_name, published_at, descriptor, name, author, commit_message, commit_extra_info, expired)` | Shared databases: a cache of the S3 commit records of other writers' snapshots (immutable, so never invalidated), and which of them expired in S3. Their snapshots resolve through their export descriptors. |
| `branch_conflicts(branch_id, snapshot, remote_snapshot, kept_branch, detected_at)` | Shared databases: branches whose head update failed because another writer moved the head (`snapshot` could not become the head; S3 was at `remote_snapshot`). With `kept_branch`, the snapshot was published as that branch (`ON_CONFLICT 'branch'`). Cleared by a successful head update or by adopting a head. |
| `branch_head_writes(provider_id, key, etag, write_id, pending, snapshot)` | This repository's last write of each S3 branch head: its ETag, write ID and snapshot, and the write ID of an attempt whose response was lost. For a shared database also the head version adopted at ATTACH: the base its next update must still find. |
| `collection_results`, `compaction_details` | Durable results of maintenance requests. |

All timestamps are integer **microseconds since the Unix epoch** (UTC).

## Identities and hashes

All hashes are SHA-256 in lowercase hex. Integers in hash preimages are decimal ASCII, and lines
end with `\n`.

- **Object** = `sha256(65536 chunk bytes)`.
- **Manifest page** (256 slots, slot = chunk index − 256·page_no), over its mapped slots in
  ascending order:
  ```
  differential-page-v1\n
  <slot>:<object hash>\n        (one line per mapped slot)
  ```
- **Root**, over its non-empty pages in ascending order:
  ```
  differential-manifest-v1\n
  <logical length>\n
  <page_no>:<page hash>\n       (one line per non-empty page)
  ```

Identical content therefore always has the same identity. An unchanged database gets the same
root, and a new snapshot of it adds no data. Opening a historical root recomputes its identity
and fails with `[differential:corrupt_data]` on mismatch. Every object read, whether a SQLite
blob, a pool file or S3, verifies the chunk hash before the chunk enters the process-wide 32 MiB
verified-chunk cache.

Databases, branches, operations and pins are identified by random UUIDv4s. A shared database
(`WRITERS 'shared'`) instead has a UUID derived from its name, so every repository attaching it
agrees on it: the first 16 bytes of `sha256("differential-shared-database-v1\n" + name)` with
version 8 and the RFC 9562 variant bits set. Snapshot IDs are positive integers allocated per
database and never reused.

## Write path

1. **Overlay.** A native write updates whole 64 KiB chunks in an in-memory overlay (one per
   store, bounded at 64 MiB).
2. **Journal.** Before `write()` returns, the change is appended to the process's journal, so a
   completed write survives process death. The journal is never synced; power loss may lose or
   tear its tail.
3. **Publication.** At DuckDB's `FileSync`, one SQLite transaction with `synchronous=FULL` and
   `F_FULLFSYNC` publishes the overlay atomically:
   - new objects (pool files are written and synced first);
   - extents, file lengths and dirty page markers;
   - the journal's committed sequence.

   This is the durability point; commits are durable as in native DuckDB. Overlay pressure and
   handle close publish with `synchronous=NORMAL` instead, made durable by the next FULL commit.
4. **Reclaim.** Chunks overwritten in the same transaction and referenced by nothing else are
   deleted immediately.

A background connection runs passive SQLite WAL checkpoints (requested at ~16 MiB of WAL; a
foreground checkpoint runs at ~256 MiB if the background lags, for example behind a long reader).
`journal_size_limit` is 64 MiB, so a WAL that grew past that is truncated when the log next
restarts. `engine.sqlite-wal` is removed when the last connection closes.

### Journal format (`DDJ1`)

`journal/<uuid>` is an append-only sequence of records. Each record is a 48-byte little-endian
header followed by `length` data bytes:

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 4 | magic `0x314a4444` (bytes `DDJ1`) |
| 4 | 4 | type: 1 = write, 2 = truncate |
| 8 | 8 | sequence (consecutive from 1) |
| 16 | 8 | logical file ID |
| 24 | 8 | write offset, or new length for truncate |
| 32 | 8 | data length (≤ 64 MiB; 0 for truncate) |
| 40 | 8 | checksum: 64-bit FNV-1a variant over the header (checksum zeroed) and data |

The owning process holds an exclusive `flock`. When a store opens, it claims journals whose
owner is dead and replays records after `journal_state.committed`, stopping at the first invalid
or non-consecutive record. Only a valid prefix of writes is ever applied. The checksum detects
torn writes, not tampering.

### Generations

DuckDB keeps its main file crash-consistent: a checkpoint writes new blocks, syncs, writes the
header and syncs again. The adapter notes a header write (bytes 4096–12287) and seals at the
next `FileSync`:
- dirty manifest pages are re-hashed;
- the root is computed and recorded as the file's generation.

Snapshots, branches and clones reference roots, never live files. Creating a snapshot runs a
native checkpoint, seals the main file and records the snapshot in one transaction.

## Local payload pools

A database created with `DATA_PATH '/directory'` keeps chunk bytes as files instead of SQLite
blobs. All metadata stays in `engine.sqlite`.

```
<directory>/differential-pools/<repository uuid>/<pool uuid>/<first two hex chars>/<sha256>
```

- Each file is exactly one 65536-byte object, named by its hash and never modified.
- Writes go to a temporary file in the fan-out directory, which is `fsync`ed and linked into
  place. Directories are synced, and the device is flushed once per publication batch before
  the mapping commits.
- Reads verify the hash.
- Deletion is two-phase: hashes enter `local_deletions` in the collecting transaction. Files
  are removed afterwards under the exclusive payload fence, so no reader of an older SQLite
  snapshot can lose a file it maps.
- `differential_delete_orphaned_files` removes files no mapping references (crash leftovers).

## Retention and collection

A root, and everything it reaches, stays while any of these holds it:
- a named file or open handle;
- the latest generation of a live file;
- a retained snapshot that is not evicted;
- an active branch;
- a reader or operation pin;
- a pending restore/deletion target;
- a pending S3 export.

Triggers enqueue every root, page, object or file whose last reference may have gone. Collection
then pops queued candidates, checks liveness with indexed lookups, and cascades from files to
roots, pages and objects within a work budget. It runs automatically in small background steps
after checkpoints (`differential_auto_gc`) and explicitly via `differential_cleanup_old_files`.
`differential_compact` returns free SQLite pages (`auto_vacuum=INCREMENTAL`).

A pin lock file that can be locked belongs to a dead process, and that pin is released. A
missing pin file is treated as live. Damaged metadata never releases data.

## S3 exports

A database created with `DATA_PATH 's3://bucket/prefix/'` exports every published snapshot.
Exports are immutable and content-addressed, and readable without the writer's repository. All
keys are relative to the prefix:

```
s3://bucket/prefix/
├── descriptors/<sha256>                    one per exported snapshot (the export locator)
├── names/<database name>/<database uuid>   names index: every database exported under this name
└── dbs/<database uuid>/
    ├── branches/<branch name>              branch head: mutable pointer to the branch's latest export
    ├── snapshots/<9999999999 - n>          commit record of snapshot n, newest first in a listing
    ├── packs/<sha256>                      the database's data pool: up to 128 chunks (8 MiB) each
    └── indexes/<sha256>                    DIX1 chunk index pages, 4096 chunks (256 MiB) each
```

Packs, index pages and descriptors are named by the SHA-256 of their exact bytes; commit
records and names-index entries by snapshot number and database. All of them are written with
`If-None-Match: *` (never overwritten) and an SHA-256 checksum, then read back and verified before
they are recorded. Branch heads are the only objects that change. Several repositories may share
a prefix: each database writes under its own UUID, and descriptors cannot collide.

### Pack

A pack is the concatenation of whole 64 KiB chunks. Chunk `slot` occupies bytes
`[slot·65536, (slot+1)·65536)`, and the pack's key is the SHA-256 of all its bytes. A pack holds
at most 128 chunks, fewer if the provider's `MULTIPART_THRESHOLD` requires it, so exports never
use multipart uploads.

An export uploads only chunks this provider has never stored, grouped in file order so
neighbouring ranges share a pack, into its database's pool (`dbs/<uuid>/packs/`). Later exports
reference earlier packs, also packs in another database's pool: a clone's export reuses the
packs of the database it was cloned from.

### Index page (`DIX1`)

Index page `p` covers logical chunks `[4096·p, 4096·(p+1))`. Layout, little-endian:

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 4 | magic `DIX1` |
| 4 | 4 | pack count `n` (u32) |
| 8 | 48·n | per referenced pack: the 16-byte UUID of the database whose pool holds it, then its raw 32-byte SHA-256; pack number = position |
| 8 + 48n | 36 · 4096 | one entry per chunk slot |

Each 36-byte entry is the chunk's raw SHA-256 (32 bytes), then the pack number (u16), then
the slot within that pack (u16). Pack number `0xFFFF` marks a hole: zeros, no data. Size is
always `8 + 32n + 147456` bytes.

### Descriptor

The descriptor is ASCII text, one field per line, followed by a copy of the first index page; at
most 1 MiB. Its key is `descriptors/<sha256 of its bytes>`, which is the `export_locator` of the
snapshot.

```
differential-export-v1
duckdb-<version>                  (writer's DuckDB version, e.g. duckdb-v1.5.6)
<database uuid>
<branch uuid>
<snapshot id>
<published_at, microseconds since epoch>
<root hash>                       (the snapshot's root identity)
<logical length in bytes>
dbs/<database uuid>/              (the database's pool: its index pages and new packs)
<index page count>                (= ceil(length / 256 MiB))
<index hash> <index byte size>    (one line per index page, in order)
first-index                       (when there is at least one index page)
<the first index page, raw bytes> (its size and hash must match the first index line)
```

The embedded first index page lets a reader resolve the native file header right after the
descriptor, without another request. Readers fetch the descriptor with one GET (a range of
1 MiB, no HEAD), verify it against the hash in its key and keep it in process memory (and in the
disk cache when enabled), so a branch-head ATTACH, which checks the descriptor first, reads it
once. Both caches are per repository: another DuckDB instance in the same process that uses a
different repository fetches the descriptor with its own S3 access.

The engine line is recorded, not enforced: readers accept any `duckdb-` line of up to 64
characters from `[A-Za-z0-9.+_-]`, and DuckDB itself decides at attach time whether it can open
the stored native file. Readers reject unknown format lines, malformed engine lines, UUIDs, hashes
or bounds, a page count that does not match the length, a missing or mismatching first index
page, and trailing bytes.

### Commit records

Every exported snapshot gets an immutable commit record at
`dbs/<database uuid>/snapshots/<9999999999 - n>` (the inverted snapshot number, zero-padded to 10
digits). S3 lists keys in ascending order, so a listing returns the newest snapshot first: one
`LIST max-keys=1` finds the latest snapshot number of a database. ASCII text, one `key value`
field per line, free text percent-encoded (`%0A` for a newline):

```
differential-commit-v1
database <uuid>
database_name <name>
snapshot <n>
parent <n>                        (the previous snapshot of the publishing branch; 0 for the first)
branch <uuid>                     (the publishing branch)
branch_name <name>
published_at <microseconds since epoch>
descriptor descriptors/<sha256>
name <text>
author <text>
message <text>
extra_info <text>
```

The record is created after the descriptor and before the export counts as complete. If a record
for that number already exists with other content, another writer with this database's identity
(for example a copy of the repository) published that number: the export fails with
`head_conflict` and stays pending. `differential_claim_branch_heads` then abandons such exports
(`export_state` `abandoned`; the snapshot stays local), numbers new snapshots above the newest
published one, and takes the branch heads over.

In a shared database the commit index allocates snapshot numbers. A repository exports its
snapshots in order, under a lock per database. Before uploading a descriptor it lists the newest
record: if that number is at or above the snapshot's own (and the record there is not this
snapshot's), every unexported snapshot of the database moves above it. This updates their
history, exports, operations, branch heads and the allocator in one transaction. A record created
between the listing and the write fails the create-only PUT, and the export renumbers and retries
(up to 8 times, then `[differential:busy]` with the export pending). Numbers are therefore unique
and increase along every branch, with gaps where a writer's conflicting snapshot stays unpublished.

In a shared database the commit records are the history all writers see.
- **Cache.** A repository caches the records it has not seen yet: one listing of the commit
  index, then one GET per new record, in parallel.
- **Time travel by time** follows a branch's ancestry: the `parent` links from its head.
- **Expiry.** A snapshot expires for every writer through a create-only record at
  `dbs/<database uuid>/expired/<9999999999 - n>`:
  ```
  differential-expired-v1
  snapshot <n>
  expired_at <microseconds since epoch>
  repository <repository uuid>
  ```
  Expiry records are listed with the commit index; snapshots that are branch heads cannot
  expire.

### Names index

`names/<database name>/<database uuid>` (`differential-name-v1`, then the UUID and the name, one
per line) is created with a database's first export. Readers resolve a database name with one
listing of `names/<name>/`.

### Publication order and retention

1. The publication transaction records the export as `pending`, which retains its whole local
   graph.
2. New packs are uploaded (in parallel), then index pages, then the descriptor last.
3. `exports.descriptor` is recorded and the export becomes `complete`.

A descriptor therefore only ever names objects that already exist. A failure leaves the export
`pending` for `differential_retry_export`. Remote objects are never deleted by the extension.

Once `complete`, the export no longer retains local data:
- expired snapshots become locally collectible;
- `differential_evict_snapshots` drops the local copy of live ones (`evicted = 1`).

Branching, cloning or restoring from an evicted snapshot downloads ("rehydrates") its chunks
again under the same root identity.

### Branch heads

For every active branch of an S3 database, `dbs/<database uuid>/branches/<branch name>`
names the latest snapshot in the branch's history that has a completed export and is not
expired. It is ASCII text, one field per line, and, apart from the advisory writer markers below,
the only object the extension overwrites (with an SHA-256 checksum, and only conditionally):

```
differential-head-v1
<database uuid>
<database name>
<branch uuid>
<branch name>
<snapshot id>
<published_at, microseconds since epoch>
descriptors/<sha256>              (the snapshot's export descriptor)
write <32 hex digits>             (random per write; identifies the writer of this version)
```

The database UUID in the key keeps databases with the same name in one prefix apart. Heads are
recomputed from the local metadata store and rewritten, if they changed, after every completed
export, branch creation and expiry, and by `differential_retry_export`. A lock in the
repository's `locks/` directory serializes this per database, so concurrent or out-of-order
exports converge on the latest. Heads of dropped branches are left in place.

Heads are guarded against other writers. The repository records the ETag and write ID of its
last write of each head (`branch_head_writes`) and overwrites a head only while it is still that
version: `If-Match: <recorded ETag>`, or `If-None-Match: *` when it creates it. A head that
exists but was not written by this repository (another ETag and write ID), for example by a copy
of the repository on another host, is not touched; the update fails with `head_conflict` while
the export stays complete. The write ID of each attempt is recorded before its PUT, so a write
whose response was lost, including an SDK retry that fails its condition against that first
write, is recognised as this repository's own. `differential_claim_branch_heads('<database>')`
takes a database's heads over deliberately (conditional on their current ETag).

A dropped branch of a shared database leaves a tombstone at its head key, written like a head
update (`If-Match` on the repository's base):
```
differential-dropped-v1
<database uuid>
<database name>
<branch uuid>
<branch name>
<dropped_at, microseconds since epoch>
write <32 hex digits>
```
Readers and listings treat it as absent. Another repository with an unchanged copy of the branch
drops it at ATTACH. A new branch of that name is written on top of the tombstone (`If-Match` on
its version).

Shared databases update heads differently:
- only a head whose branch got a newer exported snapshot from this repository is written;
- the condition is its base, the version this repository last wrote or adopted at ATTACH
  (`If-Match`, or `If-None-Match: *` for a branch new to S3);
- a failed condition means another writer moved the head: `[differential:branch_conflict]`, after
  the other heads were written, with the export complete and the head unchanged. The branch is
  recorded in `branch_conflicts` and skipped when all heads are republished;
- a lost response is recognised by the write ID, as above.

Readers (`ATTACH 'differential:s3://…/heads/<database>[/<uuid>][/<branch>]'`; this is an ATTACH
path, not an object key) find the UUID by listing `names/<database>/` when it is omitted. If several
databases share the name, only those with the branch count, and an ambiguous name is refused. They reject heads
whose fields don't match their key, then load the named descriptor and require it to describe the
same database and snapshot. The descriptor's branch is the branch that *published* the snapshot,
which differs for a branch created from or restored to it.

### Writer markers

A writable attachment of a shared database's branch keeps an advisory marker at
`dbs/<database uuid>/writers/<branch name>/<writer id>` (the writer ID is random per session):

```
differential-writer-v1
database <uuid>
branch_name <name>
writer <32 hex digits>
repository <repository uuid>
host <host name, percent-encoded>
process <process id>
started_at <microseconds since epoch>
renewed_at <microseconds since epoch>
lease <microseconds>             (120000000)
```

- **Lifecycle.** It is written when the branch's main file is opened for writing. A thread
  rewrites it with a new `renewed_at` every quarter lease, and it is deleted when the file
  closes.
- **Liveness.** A marker is live while the reader's clock is before `renewed_at + lease`.
- **Clean-up at ATTACH.** The writer lists the branch's markers and deletes expired and
  unreadable ones, and its own repository's leftovers (it holds the branch's lock, so they
  belong to ended sessions).
- **Live markers of other repositories** produce the `ON_BUSY` warning or refusal before the
  writer's own marker is written.

Markers are the only S3 objects anyone deletes, and nothing depends on them for correctness.

### Joining a shared database

ATTACH of a shared database reads the branch head with one GET (its ETag is the version). If the
repository has no such branch, or the local branch has no changes since its base, the head is
adopted:
- the descriptor and index pages are read;
- the snapshot's chunk map is imported as manifest pages and a root (the same identity as sealing
  the bytes, verified against the descriptor's root), its chunks as `objects` without data
  marked in `remote_objects`, and their locations in `remote_chunks`;
- the snapshot is recorded with a completed export and `evicted = 1`;
- the branch's live file gets the root's map, and the head version is recorded as the base.

"No changes" means: no snapshot after the base (for a branch whose conflicting snapshot was
published as another branch: none after that snapshot), an empty write-ahead log, and a main file
that seals to that snapshot's root.

`differential_resolve_conflict` takes the branch's writer lock and preserves the branch's current
contents (a recovery checkpoint, as for restore) as a snapshot unless the latest one holds them.
With `keep => 'branch'` it exports that snapshot and creates a branch of it. Then it adopts the
current S3 head, replacing the local branch. Nothing is downloaded. Reads of the live file load the objects of each
read together and fetch the remote-only ones in one batch: one range request per run of
consecutive pack slots, verified.
- **Caches:** the reading instance's memory cache (DuckDB's external file cache, as pack ranges
  named like those of export reads) and the disk cache when it is configured.
- **Read-ahead:** sequential reads, tracked as one stream per scanning thread (up to 16 per file),
  add the following remote-only chunks to the batch, as for exports. The window starts at the read
  size and doubles to 64 chunks.
- **What it costs:** chunks fetched ahead are kept only by those caches, and read-ahead is off
  when neither would keep them. A failed read-ahead batch is retried without the extra chunks. A write over part
of such a chunk reads it first; whole-chunk writes do not. Locally written chunks are stored as
usual, so the next export uploads only them. Collection forgets remote-only objects like any
others.

### Reading an export

`ATTACH 'differential:s3://…/descriptors/<sha256>'` fetches and validates the descriptor. Reads
then load the index page(s) covering the requested range, and fetch only the needed slot ranges
of packs. Adjacent slots are coalesced into one range request; misses are fetched in parallel,
up to the provider's request limit.

Every chunk is verified against its hash before use. Sequential scans read ahead per scanning
thread (one stream per thread, up to 16 per file; windows double to 4 MiB, each chunk is requested once),
and concurrent fetches of one chunk or index page in a process share one request. Verified data
is cached at two levels:

- **Memory:** DuckDB's external file cache of the reading instance, as ranges of
  `differential:s3://bucket/prefix/<object>` (one range per index page or coalesced pack read,
  version tag `immutable`). It is buffer-managed within `memory_limit` and outlives DETACH.
- **Disk (optional):** with `differential_remote_cache_size` set, `<repository>/cache/<first two
  hex digits>/<sha256>` holds chunks and index pages under their content hash. A background
  writer per process (at most 64 MiB queued) writes each to a temporary name
  (`.<sha256>.<pid>.<n>.tmp`) and renames it, without fsync. Every read
  re-verifies the hash and deletes a damaged entry. When the total exceeds the limit, one process
  at a time (`cache/.trim.lock`) deletes the least recently used files (by mtime, refreshed at
  most once a minute on hits) down to 90% of it. Nothing refers to these files; deleting the
  directory is always safe.

## Versioning and compatibility

- Native files are ordinary DuckDB storage written by the running DuckDB version (1.5.6 today).
  Descriptors record that version but readers do not require a match (see above).
- One store, control, root and export format each (table above). Repositories, roots and
  exports in any other format are refused; a future format change must add its own migration
  and tests.
- Fixed bounds:
  - 64 KiB chunks; 256-chunk (16 MiB) manifest pages;
  - 4096-chunk index pages; 8 MiB packs;
  - a 1 TiB logical file; 64 MiB journal records;
  - a 1 MiB descriptor, at most 4096 index pages.

## Inspecting a repository

Read-only inspection is safe. Open the metadata store with SQLite's read-only URI, e.g.
`sqlite3 'file:/path/repo/engine.sqlite?mode=ro'`, or better, use the SQL functions:

```sql
FROM differential_databases();
FROM sales.snapshots();
FROM differential_retention_roots();
SELECT metric, value FROM differential_storage_stats();
```

The formats on this page were checked against real data:
- **Local (2026-10-10):** page, root and chunk hashes recomputed from `engine.sqlite` matched the
  stored identities.
- **S3 (2026-10-10):** an independent decoder rebuilt each exported snapshot's native file from
  descriptor → index → packs, byte-identical to the local root, starting with DuckDB's `DUCK`
  magic, on the pinned MinIO fixture.
- **Identifiers (2026-10-11):** after the format identifiers were renamed, a new repository and
  export showed `user_version` 1, `control_format` 1, `differential-export-v1` and `DIX1`. The
  layouts themselves did not change.
