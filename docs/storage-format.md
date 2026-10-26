# Storage mechanisms and formats

This page specifies how `differential` stores data: locally in a repository, in optional local
payload pools, and in S3 exports. Each component has exactly one format; anything else is
refused with `[differential:incompatible_format]`; there are no migrations:

| Component | Format identifier |
| --- | --- |
| Local store (`engine.sqlite`, `PRAGMA user_version`) | 2 |
| Control metadata (`control_format.version`) | 1 |
| Manifest page / root identity | `differential-page-v1` / `differential-manifest-v1` |
| Segment record (write journal and local payload) | `DSG2` |
| S3 export descriptor | `differential-export-v2` |
| S3 chunk index page | `DIX2` |
| S3 branch head | `differential-head-v1` |
| S3 shipping epoch (WAL shipping) | `differential-epoch-v1` |

For the design rationale, see the [architecture overview](architecture.md); for operations,
see the [lifecycle guide](lifecycle.md).

> The repository and the S3 layout are owned by the extension. Inspect them read-only; never
> edit, copy parts of, or delete files by hand. Use the SQL functions for every change.

## Core idea

DuckDB itself is not modified. It reads and writes an ordinary native database file, and
DuckDB storage bytes are unchanged. The extension stores the main database file as a
**logical file** split into a **12 KiB first chunk** and subsequent **256 KiB chunks**: one
chunk per native data block. Every chunk is zero-padded to a 256 KiB object and stored once
under its SHA-256. A version of the file is therefore just a map from chunk index to object
hash, so versions share every unchanged chunk. Sidecars (the WAL and its recovery files) are
**log files** instead: byte ranges that point at the records that wrote them, so an append
stores only the appended bytes.

```
native DuckDB file (logical bytes)      chunk map (per file / per root)       objects
┌────────┬────────┬────────┬───       ┌───────┬──────────────┐             ┌──────────────┐
│chunk 0 │chunk 1 │chunk 2 │ …   ──▶  │ 0     │ sha256(c0)   │ ──────────▶ │256 KiB bytes │
└────────┴────────┴────────┴───       │ 1     │ sha256(c1)   │             │ (segment,    │
                                      │ (2 absent = hole, reads as zeros) │  pool file,  │
                                      └───────┴──────────────┘             │  or S3 pack) │
                                                                           └──────────────┘
```

| Term | Meaning |
| --- | --- |
| **Logical file** | One native file: a branch's main database file (chunked), or its `.wal` / `.wal.checkpoint` / `.wal.recovery` sidecar (a log file). |
| **Chunk** | Chunk 0 covers bytes `[0, 12288)`. Chunk `i ≥ 1` covers `[12288 + (i−1)·262144, 12288 + i·262144)`. Unused object bytes are zero; logical length is stored separately. |
| **Object** | An immutable 256 KiB chunk, identified by the lowercase hex SHA-256 of exactly 262144 bytes. |
| **Range** | A log file's mapping `(file, start, length) → bytes of a segment record`. |
| **Segment** | An append-only repository file of records: the write journal of a file group and the local payload that publications reference in place. |
| **Hole** | A chunk with no mapping: never written, or truncated away. Reads return zeros; nothing is stored. |
| **Extent** | A live mapping `(file, chunk index) → object` of a writable file. |
| **Manifest page** | An immutable map of 256 consecutive chunk slots to objects (the first page covers 64 MiB − 244 KiB; later pages cover 64 MiB). |
| **Root** | An immutable version of a logical file: its length plus its ordered manifest pages. |
| **Generation** | The root sealed at a branch's latest completed native checkpoint (internal). |
| **Snapshot** | A public, numbered version of a database: a root plus ID, name, time, branch and commit metadata. |

The chunk mapping applies to every chunked file and does not change native bytes. A file's kind
is fixed when it is created: a name with a `.` after its group key (a sidecar) in the default
store is a log file; every other file, and every file of a [local pool](#local-payload-pools),
is chunked. For offset `x`, the chunk index is `0` when `x < 12288`, otherwise
`1 + floor((x − 12288) / 262144)`. The number of chunks in a file of length `n` is zero for an
empty file, otherwise `chunk_index(n − 1) + 1`. The first S3 index page covers 1 GiB − 244 KiB;
subsequent pages cover 1 GiB. A file at the 1 TiB limit needs 1025 pages.

The first chunk isolates DuckDB's three 4096-byte headers. A default 256 KiB native data
block is exactly one chunk, so a full-block overwrite neither reads neighboring payload nor
copies the block: the segment record that wrote it is the object. Smaller native allocation
sizes (16–128 KiB) use ordinary partial-chunk writes. Chunk 0's unused 244 KiB suffix and the
last chunk's unused suffix are always zero, including after truncation and regrowth. Pack
offsets are `slot * 262144`; logical file offsets use the mapping above.

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
├── engine.sqlite (+ -wal, -shm)   metadata store (embedded SQLite, WAL mode)
├── journal/<16 hex>               active segments: one per file group and process (write journal and payload)
├── segments/<16 hex>              retired segments (payload only; collected when nothing references them)
├── pins/<uuid>                    lock file of each reader pin; locked while its process lives
├── locks/
│   ├── branch-<branch uuid>       writer lock of a branch (one writer process)
│   ├── management-<database uuid> lock for restore / branch deletion
│   └── managed_<branch uuid>.wal  native sidecar file locks (mirrors DuckDB's own opens)
├── payload.lock                   payload fence: shared by publishers, exclusive for pool deletions
├── staging/<repository uuid>/     S3 upload staging (differential_export_staging = 'disk'), 8 MiB per parallel request (64–512 MiB), deleted after upload
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
repository, which a DuckDB catalog file does not allow. `engine.sqlite` has `application_id = 0x44494646` ("DIFF") and `user_version = 2`. It runs with
`journal_mode=WAL`, `synchronous=FULL` (`fsync`) and `auto_vacuum=INCREMENTAL`, with
foreign keys enforced. The extension refuses unrelated SQLite files and unknown versions with
`[differential:incompatible_format]`. One file holds two groups of tables.

### Byte store tables

| Table | Purpose |
| --- | --- |
| `files(id, name, size, kind, version)` | Logical files. `name` is NULL once unlinked (still referenced by a handle or pin). `kind` 0 = chunked, 1 = log file. `version` changes with every update of the file's map (it keys in-process map caches). |
| `segments(id, committed, active)` | Segments that publications referenced: the published watermark (byte offset; a replay starts there) and whether the segment is still active. |
| `objects(hash, segment, position, crc)` | Content-addressed chunks. The default store's copy is the 262144 bytes at `position` of `segment`, with their CRC32C; all three are NULL when the bytes live in a local pool or only in S3. |
| `ranges(file, start, length, segment, record, record_length, crc, skip)` | Live byte map of log files: bytes `[start, start+length)` are bytes `[skip, skip+length)` of the record payload at `record` (`record_length` bytes, CRC32C `crc`) in `segment`. Ranges of a file never overlap. |
| `remote_objects(hash)` | Objects whose durable copy is an S3 export (a shared database's adopted head, read on demand). Without local data they are fetched from the packs `remote_chunks` records. |
| `extents(file, chunk, object)` | Live chunk map of writable files. |
| `file_pages(file, page_no, page, dirty)` | Per-file cache of manifest pages; `dirty` pages are re-hashed at the next seal. |
| `manifest_pages(hash)`, `page_entries(page, slot, object)` | Immutable 256-slot manifest pages. |
| `roots(hash, size)` | Immutable roots. |
| `root_pages(root, page_no, page)` | The ordered manifest pages of a root. |
| `generations(file, root)` | Latest sealed root of each live file. |
| `pins(id, root, file)` | Reader and operation pins (with a lock file in `pins/`). |
| `local_pools`, `file_pools`, `local_objects`, `local_deletions` | Pool registry, file → pool, which pools hold which hashes, durable deletion queue. |
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
| `remote_packs`, `remote_chunks`, `verified_remote_objects` | What each provider already stores and in which database's pool, so exports upload only new chunks. A newly uploaded pack replaces earlier locations of its chunks. |
| `remote_gc_records(provider_id, key)`, `remote_condemned(provider_id, key, condemned_at)` | [Remote collection](#remote-collection): the condemnation records this repository has learned, and every condemned key with its earliest condemnation (the record's S3 LastModified). Created on open, also in existing repositories. |
| `remote_sync_state(database_id, commits_full_at, expiries_full_at, expiries_watermark)` | Shared databases: when this repository last listed the commit records and expiry records in full, and up to which time it has read the expiry log. Created on open. |
| `remote_index_packs(provider_id, index_key, pack_key)`, `remote_descriptor_pages(provider_id, descriptor_key, index_key, size)`, `remote_commit_descriptors(provider_id, commit_key, descriptor_key)` | What immutable S3 objects reference, filled by exports and by remote collection, so a collection reads only objects it has not seen (an object that references nothing has one row with an empty reference). Removed with their objects. Created on open. |
| `remote_commits(database_id, id, parent, branch_id, branch_name, published_at, descriptor, name, author, commit_message, commit_extra_info, expired)` | Shared databases: a cache of the S3 commit records of other writers' snapshots (immutable, so never invalidated), and which of them expired in S3. Their snapshots resolve through their export descriptors. |
| `branch_conflicts(branch_id, snapshot, remote_snapshot, kept_branch, detected_at)` | Shared databases: branches whose head update failed because another writer moved the head (`snapshot` could not become the head; S3 was at `remote_snapshot`). With `kept_branch`, the snapshot was published as that branch (`ON_CONFLICT 'branch'`). Cleared by a successful head update or by adopting a head. |
| `branch_head_writes(provider_id, key, etag, write_id, pending, snapshot)` | This repository's last write of each S3 branch head: its ETag, write ID and snapshot, and the write ID of an attempt whose response was lost. For a shared database also the head version adopted at ATTACH: the base its next update must still find. |
| `collection_results`, `compaction_details` | Durable results of maintenance requests. |

`differential_cleanup_metadata` deletes completed `operations` (with their `collection_results`
and `compaction_details`) created before its cutoff, and `history` entries of retired snapshots
without a pending export. It never deletes `snapshot_ancestry` records, and deletes a merge
companion record (kinds `merge_schema`, `merge_expression_context`, `merge_expected_inputs`,
`merge_resolutions`, `merge_attempt`, `merge_publication`, `merge_destination`, `merge_aborted`,
`merge_target_result`) only together with its merge request, or once that request is gone.

All metadata timestamps are integer **microseconds since the Unix epoch** (UTC).
User column values retain their native DuckDB types and precision.

## Identities and hashes

All hashes are SHA-256 in lowercase hex. Integers in hash preimages are decimal ASCII, and lines
end with `\n`.

- **Object** = `sha256(262144 chunk bytes)`.
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
and fails with `[differential:corrupt_data]` on mismatch. Local segment reads are verified with
the CRC32C recorded at publication (hardware-accelerated; SHA-256 remains the identity and is
recomputed by `Verify`, for exports and for every pool-file or S3 read). A whole-chunk read is
verified straight into DuckDB's buffer; partial reads go through the process-wide 64 MiB
verified-chunk cache, which also holds log records and remote chunks.

Databases, branches, management operations and pins are identified by random UUIDv4s. Internal
snapshot-ancestry records use deterministic UUIDv8s as specified below. A shared database
(`WRITERS 'shared'`) instead has a UUID derived from its name, so every repository attaching it
agrees on it: the first 16 bytes of `sha256("differential-shared-database-v1\n" + name)` with
version 8 and the RFC 9562 variant bits set. Snapshot IDs are positive integers allocated per
database and never reused.

## Write path

1. **Overlay.** A native write updates whole 256 KiB chunks (chunked files) or byte pieces (log
   files) in an in-memory overlay. Files are published in groups: a file and its sidecars (`name`
   and `name.<suffix>`, i.e. a database and its WAL files) share a write gate, a segment and an
   overlay bound.
2. **Segment.** Before `write()` returns, the write is appended to its group's active segment
   (one per group and process), so a completed write survives process death. Appends are not
   synced; power loss may lose or tear the unsynced tail.
3. **Publication.** At DuckDB's `FileSync` the group is published while other groups keep
   writing. Its buffered chunks are hashed (SHA-256 and CRC32C). A chunk that one write record
   covers whole (a native block) is that record's payload; any other chunk not yet stored locally
   is appended as an object record. Log-file pieces are ranges of their write records. The segment
   is then synced (`fsync`; pool files are written and synced instead), and one SQLite transaction
   with `synchronous=FULL` (`fsync`) publishes atomically:
   - new objects with their segment locations, extents, ranges, file lengths and dirty page
     markers;
   - the segment's published watermark (`segments.committed`).

   This is the durability point; commits are durable as in native DuckDB. Payload is synced
   before any transaction that references it, NORMAL ones included. With group commit, syncs of
   different groups that arrive while a commit is running are prepared (hashed, segments and pool
   files synced) in their own threads and committed together in the next transaction, up to
   16 MiB of new payload; a larger one commits alone. A failed batch is retried one sync at a
   time.
4. **Background publication.** A group with more than 16 MiB of buffered writes is published by a
   background thread with `synchronous=NORMAL` (made durable by the next FULL commit); beyond
   64 MiB the writer publishes itself (backpressure), and beyond 256 MiB for all groups together
   everything is published. Handle close publishes with NORMAL too.
5. **Reclaim.** Chunks overwritten in the same transaction and referenced by nothing else are
   deleted immediately (their segment bytes become garbage for [segment collection](#retention-and-collection)).

A publication that finds its segment beyond 64 MiB retires it: the segment is marked inactive in
the same transaction and moved to `segments/` afterwards; the group's next write starts a new one.
A segment that no publication references (the writes of an external-pool database, whose payload
goes to pool files) holds only published journal records: it is retired beyond 4 MiB instead and
deleted right after the commit. A store that closes cleanly retires its segments the same way,
deleting the unreferenced ones.

A background connection runs passive SQLite WAL checkpoints (requested at ~4 MiB of WAL; a
foreground checkpoint runs at ~256 MiB if the background lags, for example behind a long reader).
`journal_size_limit` is 64 MiB, so a WAL that grew past that is truncated when the log next
restarts. `engine.sqlite-wal` is removed when the last connection closes.

### Segment format (`DSG2`)

A segment (`journal/<id>` while active, `segments/<id>` once retired; `<id>` is a random 63-bit
identity in 16 lowercase hex digits, never reused) is an append-only sequence of records. Each
record is a 48-byte little-endian header followed by `length` payload bytes:

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 4 | magic `0x32475344` (bytes `DSG2`) |
| 4 | 4 | type: 1 = write, 2 = truncate, 3 = object (published payload only; never replayed) |
| 8 | 8 | sequence (consecutive from 1) |
| 16 | 8 | logical file ID |
| 24 | 8 | write offset, new length for truncate, chunk index for an object |
| 32 | 8 | payload length (≤ 64 MiB; 0 for truncate) |
| 40 | 4 | CRC32C (Castagnoli) of the payload |
| 44 | 4 | CRC32C of header bytes 0–43 |

Locations in `objects` and `ranges` are byte offsets of payloads. The owning process holds an
exclusive `flock` on an active segment and syncs its directory when creating it. When a store
opens, it claims active segments whose owner is dead and replays their write and truncate records
after `segments.committed` (from the start for a segment no publication referenced), stopping at
the first invalid or non-consecutive record. Only a valid prefix of writes is ever applied; the
replay publishes them (into the replaying store's own segments) and retires the dead segment in
the same transaction. The checksums detect torn writes, not tampering.

### Generations

DuckDB keeps its main file crash-consistent: a checkpoint writes new blocks, syncs, writes the
header and syncs again. The adapter notes a header write (bytes 4096–12287) and seals at the
next `FileSync`:
- dirty manifest pages are re-hashed;
- the root is computed and recorded as the file's generation.

Snapshots, branches and clones reference roots, never live files. Creating a snapshot runs a
native checkpoint, seals the main file and records the snapshot in one transaction.

## Local payload pools

A database created with `DATA_PATH '/directory'` keeps chunk bytes as files instead of repository
segments; all of its files, sidecars included, are chunked. All metadata stays in `engine.sqlite`.
Its writes are still journaled in the repository's segments until they are published.

```
<directory>/differential-pools/<repository uuid>/<pool uuid>/<first two hex chars>/<sha256>
```

- Each file is exactly one 262144-byte object, named by its hash and never modified.
- Writes go to a temporary file in the fan-out directory, which is `fsync`ed and linked into
  place. Directories are synced, and the device is flushed once per publication batch before
  the mapping commits.
- Reads verify the SHA-256 hash.
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

**Segment collection** follows each collection step (up to 64 MiB of copying per step). A retired
segment that no object or range references is deleted: its file first, then its row (a crash in
between leaves a row without a file, deleted again later; never a file a replay could mistake for
unpublished writes). A retired segment of at least 4 MiB whose live payload is less than half its
size is compacted: the live objects and log records are copied, verified, into a new segment,
which is synced and then takes over their references in one transaction; the old one is then
deleted. Active segments are never collected. A reader that finds a segment gone retries its read
on fresh metadata; a descriptor it already holds keeps reading the unchanged bytes.

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
├── gc/condemned/<microseconds>-<sha256>    remote collection: keys condemned by one run
└── dbs/<database uuid>/
    ├── branches/<branch name>              branch head: mutable pointer to the branch's latest export
    ├── snapshots/<9999999999 - n>          commit record of snapshot n, newest first in a listing
    ├── expired/<9999999999 - n>            expiry record of snapshot n
    ├── expiries/<microseconds>-<n>         shared databases: time-ordered log of the expiry records
    ├── packs/<sha256>                      the database's data pool: up to 32 chunks (8 MiB) each
    ├── indexes/<sha256>                    DIX2 index pages, 4096 chunk slots each
    └── log/<branch name>/<branch uuid>/    WAL shipping (SHIP_WAL): one directory per chain,
        └── <9999999999 - chain>/<9999999999 - epoch>   newest first in a listing
```

Packs, index pages and descriptors are named by the SHA-256 of their exact bytes; commit
records and names-index entries by snapshot number and database. All of them are written with
`If-None-Match: *` (never overwritten) and an SHA-256 checksum that the service verifies
(`x-amz-checksum-sha256`); with `differential_export_verification = 'read_back'` they are also read
back and verified before they are recorded. Branch heads are the only objects that change. Several repositories may share
a prefix: each database writes under its own UUID, and descriptors cannot collide.

### Pack

A pack is the concatenation of whole 256 KiB chunks. Chunk `slot` occupies bytes
`[slot·262144, (slot+1)·262144)`, and the pack's key is the SHA-256 of all its bytes. A pack holds
at most 32 chunks, fewer if the provider's `MULTIPART_THRESHOLD` requires it, so exports never
use multipart uploads.

An export uploads only chunks this provider has never stored, grouped in file order so
neighbouring ranges share a pack, into its database's pool (`dbs/<uuid>/packs/`). Later exports
reference earlier packs, also packs in another database's pool: a clone's export reuses the
packs of the database it was cloned from. An export never references a pack that remote
collection condemned: its chunks go into a new pack, and if that pack's bytes would equal a
condemned pack the chunks are laid out differently (rotated, or the first chunk repeated in an
extra slot), so the new pack has a key of its own.

### Index page (`DIX2`)

Index page `p` covers logical chunks `[4096·p, 4096·(p+1))`. Layout, little-endian:

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 4 | magic `DIX2` |
| 4 | 4 | pack count `n` (u32) |
| 8 | 48·n | per referenced pack: the 16-byte UUID of the database whose pool holds it, then its raw 32-byte SHA-256; pack number = position |
| 8 + 48n | 36 · 4096 | one entry per chunk slot |

Each 36-byte entry is the chunk's raw SHA-256 (32 bytes), then the pack number (u16), then
the slot within that pack (u16). Pack number `0xFFFF` marks a hole: zeros, no data. Size is
always `8 + 48n + 147456` bytes.

### Descriptor

The descriptor is ASCII text, one field per line, followed by a copy of the first index page; at
most 1 MiB. Its key is `descriptors/<sha256 of its bytes>`, which is the `export_locator` of the
snapshot.

```
differential-export-v2
duckdb-<version>                  (writer's DuckDB version, e.g. duckdb-v2.0.0)
<database uuid>
<branch uuid>
<snapshot id>
<published_at, microseconds since epoch>
<root hash>                       (the snapshot's root identity)
<logical length in bytes>
dbs/<database uuid>/              (the database's pool: its index pages and new packs)
<index page count>                (= ceil(chunk_count(length) / 4096))
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
- **Cache.** A repository caches the records it has not seen yet: a listing of the commit index,
  then one GET per new record, in parallel. The listing is incremental: it runs newest first and
  ends at the first cached record older than 15 minutes (a number below a newer one is published
  only moments after its writer listed the index). A full listing runs at most a day after the
  last one, or with `differential_snapshots(..., refresh => 'full')`. Progress is kept in
  `remote_sync_state`.
- **Time travel by time** follows a branch's ancestry: the `parent` links from its head.
- **Expiry.** A snapshot expires for every writer through a create-only record at
  `dbs/<database uuid>/expired/<9999999999 - n>` (written for exported snapshots of every S3
  database, shared or not, so remote collection can reclaim them):
  ```
  differential-expired-v1
  snapshot <n>
  expired_at <microseconds since epoch>
  repository <repository uuid>
  ```
  Snapshots that are branch heads cannot expire. In a shared database each expiry also gets an
  entry in a time-ordered log, `dbs/<database uuid>/expiries/<20-digit microseconds>-<10-digit n>`
  (same content, create-only), written after the record. Other writers read the log from 15 minutes
  before their last read (`StartAfter`), so they need no full listing of `expired/`; a full listing
  (as for commit records) covers an expiry whose log entry is missing, for example after a crash
  between the two writes, or whose writer's clock is more than 15 minutes behind.

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
`pending` for `differential_retry_export`. Remote objects are deleted only by
[remote collection](#remote-collection).

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
exports converge on the latest. A head that still names a dropped branch becomes a tombstone
(below), under the same guard; with a deferred export, `differential_retry_export` writes it.

Heads are guarded against other writers. The repository records the ETag and write ID of its
last write of each head (`branch_head_writes`) and overwrites a head only while it is still that
version: `If-Match: <recorded ETag>`, or `If-None-Match: *` when it creates it. A head that
exists but was not written by this repository (another ETag and write ID), for example by a copy
of the repository on another host, is not touched; the update fails with `head_conflict` while
the export stays complete. The write ID of each attempt is recorded before its PUT, so a write
whose response was lost, including an SDK retry that fails its condition against that first
write, is recognised as this repository's own. `differential_claim_branch_heads('<database>')`
takes a database's heads over deliberately (conditional on their current ETag).

A dropped branch leaves a tombstone at its head key, written like a head update (`If-Match` on the
version this repository last wrote; for a shared database, its base):
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
  size and doubles to 16 chunks (4 MiB).
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

### Remote collection

`differential_cleanup_remote_files(location)` collects one S3 location, whatever repositories
export into it, because index pages reference packs of any database there.

- **Roots.** Descriptors named by branch heads (tombstones name none), by commit records without
  an expiry record, and by this repository's completed exports of retained snapshots; plus the
  packs of local objects this repository reads only from S3; plus, for [WAL shipping](#continuous-wal-shipping),
  the epochs of each shipped branch's newest chain and the base descriptor its epoch 0 names.
- **Mark.** Root descriptors, their index pages (`<pool>indexes/<page>`), and the packs those pages
  list.
- **Phase 1, condemn.** Packs, index pages, descriptors and shipping epochs that are unreachable and last modified
  before the cutoff are written to records `gc/condemned/<20-digit microseconds>-<sha256>`
  (create-only, named by their content's SHA-256, at most 8000 keys each):
  ```
  differential-condemned-v1
  <key>
  ...
  ```
- **Phase 2, delete.** A later run deletes objects condemned by a record whose S3 LastModified is
  before its cutoff and that are still unreachable: shipping epochs of superseded chains; packs;
  index pages all of whose packs are so condemned; descriptors with such a page or a page that no
  longer exists. Epochs go first, then descriptors, then index pages, then packs.

References of immutable objects are cached (`remote_index_packs`, `remote_descriptor_pages`,
`remote_commit_descriptors`), so a collection reads only branch heads and objects new since the
last one.

Every export reads the records it has not seen before it starts and again before it uploads its
descriptor, and never references a condemned pack (rebuilding its index pages if a pack was
condemned meanwhile). A pack condemned after that last check is reachable through the export's
commit record long before any collection may delete it. Index pages and descriptors are deleted
only when no export could produce them again. Learning a record also forgets upload verifications
of its keys and, for packs, the recorded locations of chunks (except those a local S3-only object
still reads; such objects are first copied locally). A record whose copies fail is read again at
the next synchronization.

## Continuous WAL shipping

A single-writer S3 database with `SHIP_WAL 'async' | 'sync'` ships the published state of each
writable branch's file group (main file and WAL sidecars) as it changes. A shipped state is a
publication of the [local store](#write-path): a durable sync boundary, or a prefix of later
completed writes, which DuckDB's recovery handles like a crash at that point.

**Change tracking.** Publications of a shipping group record what changed in the publishing
transaction itself (store tables, see below), numbered by a store-wide shipping sequence:
main-file chunks (`ship_chunks`), and per file the lowest changed byte of a log file or the
truncation floor of a chunked one (`ship_lows`, one row for every published file). A shipping round
reads the group's named files and these changes in one read snapshot, uploads an epoch and then
deletes the change rows up to the sequence it read (rows changed again meanwhile carry a larger
sequence and stay).

**Chains.** Epochs form chains. Epoch 0 of a chain names a **base**: an internal export of the main
file as published when the chain started (a descriptor with snapshot number 0, its packs and index
pages as for any export, no commit record and no head), captured as an immutable root that is not
the file's generation. Epoch 0 then carries the WAL files whole and the main file's changes since the
base; every later epoch carries the changes since the previous epoch. A new chain starts after each
completed DuckDB checkpoint (the WAL is short then), after 256 epochs, after 64 MiB of epoch data, or
when one epoch would exceed 256 MiB. Its number is above every chain of the branch in S3 (one LIST),
so a failover continues in a newer chain. The superseded chain then ends with a **redirect epoch**
(`next <chain>`, no files) so followers move on without a listing.

**Epoch format** (`differential-epoch-v1`), a text header then the payload:

```
differential-epoch-v1
database <uuid>
branch <uuid>
branch_name <name>
chain <n>
epoch <n>
previous <sha256 of the previous epoch object | - for epoch 0>
base descriptors/<sha256>
sequence <shipping sequence included>
published_at <microseconds>
next <chain | ->
files <count>
file <suffix | - for the main file> <full|delta> <chunked|log> <size> <items>
c <chunk> <sha256 | - for a hole> <payload offset>          (chunked: one line per item)
t <file offset> <length> <payload offset> <sha256 | ->     (log: the bytes from offset to the end)
removed <count>
<suffix>                                                   (one line per removed file)
payload <length>
<payload bytes>
```

A `full` file replaces whatever the name held (a new file, e.g. a WAL replaced by renaming its
recovery file); a `delta` file changes its size to `<size>` and then its listed chunks, or its bytes
from the tail offset on. Chunks carry their whole 262144-byte object (identity `sha256`); a chunk
that a truncation shortened ships with its tail zeroed, as the store keeps it. Readers verify the
object against the SHA-256 recorded at upload, each inline item against its own hash, the
`previous` link and that the numbers match the key.

**Upload.** An epoch is written create-only with its SHA-256 (multipart from a temporary file in
`staging/` when larger than `MULTIPART_THRESHOLD`). Before uploading, the shipper records the
resulting state as an intent (`ship_chains.pending`); after a crash, finding the epoch in S3 with the
same hash completes it, a missing epoch is built again. Different content under an existing key is
another writer with this branch's identity (a copied repository): `head_conflict`, and shipping
stops for the branch until the next ATTACH. In `sync` mode a durable sync returns only once an epoch
includes it (`[differential:ship_failed]` when it cannot be shipped; the commit is durable locally).

**Recovery** (`differential_recover_branch`) lists the newest epoch of the branch (`LIST` max-keys 1
under its incarnation; the newest-shipping incarnation when the name was reused), reads the chain from
epoch 0 while epochs follow consecutively, imports the base map as S3-only objects and applies the
epochs to the new database's files.

**Followers** (`FOLLOW true`) keep an in-memory image of base plus applied epochs, served read-only as
`diff://follow/<id>` (main file: changed chunks over the base export; WAL files from memory). A state
with a checkpoint in progress (`.wal.checkpoint` or `.wal.recovery`) is skipped: DuckDB's read-only
recovery would have to rewrite it. Polling reads the next epoch's key (one HEAD when it does not
exist yet).

**Local state.** Store tables (created on open): `ship_groups(group_key)`,
`ship_chunks(file, chunk, seq)`, `ship_lows(file, low, seq)`, and the `ship` allocator. Control
tables: `ship_config(database_id, mode, interval_ms)` and `ship_chains(branch_id, database_id, chain,
epoch, epoch_hash, base, known, shipped_sequence, chain_bytes, shipped_at, pending)`.

**Remote collection.** The newest chain of every shipped branch incarnation is a root: its epochs
and its base (named in epoch 0). Superseded chains and their bases are condemned and deleted under
the [two-phase rules](#remote-collection).

## Versioning and compatibility

- Native files are ordinary DuckDB storage written by the running DuckDB version (2.0 today).
  Descriptors record that version but readers do not require a match (see above).
- One store, control, root and export format each (table above). Repositories, roots and
  exports in any other format are refused; a future format change must add its own migration
  and tests.
- Fixed bounds:
  - a 12 KiB first chunk, then 256 KiB chunks; all payload objects are 256 KiB;
  - 256-slot manifest pages (the first covers 244 KiB less than 64 MiB);
  - 4096-chunk index pages; 8 MiB packs;
  - a 1 TiB logical file; 64 MiB segment records (native writes are journaled in records of at
    most 16 MiB); segments retired beyond 64 MiB;
  - a 1 MiB descriptor, at most 1025 index pages.

## Inspecting a repository

Read-only inspection is safe **from a separate process**. Open the metadata store with SQLite's
read-only URI, e.g. `sqlite3 'file:/path/repo/engine.sqlite?mode=ro'`, or better, use the SQL functions:

```sql
FROM differential_databases();
FROM sales.snapshots();
FROM differential_retention_roots();
SELECT metric, value FROM differential_storage_stats();
```

> **Warning: never open `engine.sqlite` with another SQLite library inside a process that has the
> extension loaded.** The extension links its own SQLite. A second SQLite copy in the same process
> (DuckDB's `sqlite` extension attaching the file, Python's `sqlite3` next to an embedded DuckDB,
> any other library) does not see the extension's POSIX locks or its shared-memory WAL index
> (`engine.sqlite-shm`), because POSIX locks do not conflict within one process. It can truncate
> that index while the extension has it mapped, which crashes the process (SIGBUS) or corrupts the
> store. This is SQLite's documented "multiple copies of SQLite in one application" hazard. Inspect
> the file from a separate process, or use the SQL functions above.

The formats on this page were checked against real data:
- **Local (2026-10-10):** page, root and chunk hashes recomputed from `engine.sqlite` matched the
  stored identities.
- **S3 (2026-10-10):** an independent decoder rebuilt each exported snapshot's native file from
  descriptor → index → packs, byte-identical to the local root, starting with DuckDB's `DUCK`
  magic, on the pinned MinIO fixture.
- **Header alignment (2026-10-13):** format 1 isolates the 12 KiB header region. The extension
  has not been published; only this layout is implemented, with no legacy reader or migration.
  Boundary, truncation and native allocation-size coverage is in the unit and integration suites.
  The independent decoder in `s3_layout.py` reconstructs both MinIO snapshots byte-for-byte,
  verifies descriptor/index/pack/chunk hashes and checks zero padding.

## Logical merge records

Merge uses existing control-format-1 extensible fields, without changing table layouts or native
DuckDB storage. `operations.kind = 'merge'` has the usual length-prefixed payload containing
`[database UUID, base ID, ours ID, theirs ID, destination name, logical-cell-v1]`.
The first five fields define request identity. A completed request returns its stored result
even when the sixth field names an older algorithm. Pending requests require an available
algorithm; a different version is never silently substituted. Resource limits are not part
of request identity. Progress, admission and staging-write budgets are process-local and add
no persistent fields or format version.
The result snapshot's `commit_extra_info` is JSON with `algorithm`, `operation_id`, and
`base`, `ours`, `theirs` objects, each containing `snapshot` and `root`. These provenance
references do not retain the inputs. The operation UUID links to its durable request ID.

Branch-name requests use `operations.kind='merge_branch'` and an eleven-field payload:
`[database UUID, ours name, theirs name, destination, selection version, logical-cell-v1,
ours branch UUID, theirs branch UUID, base ID, ours ID, theirs ID]`. The first four fields
are semantic request identity. The initial FULL transaction reads both published heads,
resolves the best common ancestor and records the immutable selection. Pending retries
reuse these IDs rather than resolving names again. This request record is not a retention root.
The snapshot provenance additionally contains `base_selection` (`unique-base-v1`, or the legacy
`sibling-fork-v1` proof),
`ours_branch_id` and `theirs_branch_id`. Existing branch/source fields alone are not ancestry:
imports and merges also use them, and restore does not rewrite them. The resolver requires a
parent graph; only the legacy sibling fallback uses completed fork records and rejects restores and prior target merges.
Both paths reject pending compound intents.
There are no new metadata tables or format changes; selection and merge algorithms have explicit version tags.

Guarded branch requests also reserve a completed `operations.kind='merge_expected_inputs'`
companion in that same FULL transaction. Its UUIDv8 uses the SHA-256 encoding below with
hash input `differential-s3-merge-v1\nexpected-inputs\n<merge operation UUID>` (local and single-writer S3).
Its five length-prefixed fields are `[merge-expected-inputs-v1, merge operation UUID,
expected base ID, expected ours ID, expected theirs ID]`; `request` is NULL, `database_id`
is the input database, `branch_id` is NULL, and snapshot/preservation numbers are zero. Absence denotes an unguarded
request. Presence and all three values are semantic request identity, including on completed
replay. A fresh mismatch rolls back before reservation. Retries compare against the frozen
request selection, never later heads. The completed companion freezes the guard and is not a
completed merge result. It retains no roots and does not alter control-format-1 table layouts.
Preflight reports are transient SQL values and add no operation or durable target intent.

Latest-attempt diagnostics use one mutable completed `operations.kind='merge_attempt'`
companion per merge operation. Its UUIDv8 uses hash input
`differential-s3-merge-v1\nattempt\n<merge operation UUID>`. `request` and `branch_id` are NULL,
snapshot/preservation numbers are zero, and `database_id` is the input database. The ten
length-prefixed payload fields are `[merge-attempt-v1, merge operation UUID, attempt UUID,
attempt count, started microseconds, updated microseconds, finished microseconds,
attempt state, phase, error code]`. Numbers are canonical nonnegative decimal BIGINTs;
count/start are positive, finished is zero when incomplete, and payloads are at most 1 KiB.
State is `incomplete`, `failed`, or `succeeded`; phase and error category are allowlisted
constants. Failed records have an error code; succeeded records have phase `completed`.
Finished records have `finished == updated >= started`; empty errors denote no recorded error.
The companion's `created_at` remains the first attempt's start. Its operation-level `complete`
means a valid diagnostic record, **not** that its owning merge is complete.

The request lock serializes attempts. Start increments the counter and replaces the payload
in a FULL transaction; phase/failure updates check its attempt UUID before replacing it.
Publication marks it successful in the same FULL transaction as the merge result, including
S3 completion recovery. A completed replay does not write diagnostics. Missing companions
are valid for predecessor requests or requests not admitted to execution; inspection reports
zero attempts and NULL details. This addition uses the existing extensible control-format-1 operations
layout and introduces no new table, retention root or format version.

For local and single-writer S3 branch requests, `destination == ours name` selects existing-target publication.
No formerly valid new-destination request has this identity. The target is the frozen `ours
branch UUID` and head, not a new branch or a later branch with the same name. Action is derived
from frozen ancestry: `base == theirs` means `already_up_to_date`, else `base == ours` means
`fast_forward`, otherwise `merged`. New destinations report `created_branch`.

Existing-target ownership reuses `compound_intents`: `operation_id` is the merge request,
`branch_id` is the target UUID, `expected_head` is frozen ours, `expected_epoch` fences its
exclusive lease, and `target_root` is its original public root. The existing root-retention
and release/GC rules apply. Reader pins retain inputs only for the attempt. Ordinary failure
or abort deletes this intent before durable S3 preparation; a prepared S3 result keeps it until
publication or safe abort. Death leaves it durable until retry/abort. No pending result
branch is created. Request inspection exposes unprepared ownership as `target_locked`; prepared
S3 output keeps its publication state and destination reservation flag.

Publication uses one FULL transaction: unlink old main/empty-sidecar names, install the sealed
result mapping, update the existing branch and ancestry/history as appropriate, remove the intent,
and complete the request. Fast-forward moves root/head to the source snapshot and adds a target
history event without allocating a new snapshot; no-op leaves root/head/history unchanged.
Fast-forward refuses a source snapshot retired before publication. True merge allocates one
snapshot with `merge` parents (ours, theirs). All modes increment the writer epoch to fence
obsolete owners. An ordinary no-op attempt therefore still adds management/audit records.

A completed `operations.kind='merge_target_result'` companion has a random UUID, NULL request,
and the result database/branch/snapshot. Its four length-prefixed fields are
`[local-merge-target-v1, merge operation UUID, action, provenance JSON]`, or
`[s3-merge-target-v1, merge operation UUID, action, provenance JSON]` for S3 targets. The JSON has the same
input IDs/roots, algorithm, selection and schema fields as a new merge snapshot. The companion
and original completion commit together, so retries cannot duplicate it. It records fast-forward
and no-op provenance without rewriting another snapshot's metadata. These text references are
not retention roots. This uses the control-format-1 operations/intent layout unchanged;
there is no native DuckDB format change or upstream patch.

Schema policy uses a completed `operations.kind='merge_schema'` companion, reserved in the
same FULL transaction as a new snapshot/branch merge request. Its UUIDv8 uses the SHA-256
encoding specified for S3 publication below, with hash input
`differential-s3-merge-v1\nschema\n<merge operation UUID>` (also for local merges). `request`
is NULL and `database_id` is the input database. The four length-prefixed fields are
`[merge-schema-policy-v1, merge operation UUID, schema mode, schema planner version]`.
New requests use `strict`/`strict-v1`, `additive`/`additive-tables-v2`, or
`catalog`/`catalog-v1`. Catalog version 1 includes table/plain nullable column removal proofs and whole-view
definition/presence reconciliation, including native dependency registrations and bound output
contracts. It includes the additive version-2 rules without broadening older strict/additive requests. Existing
`additive`/`nullable-columns-v1` records remain readable and executable by their original
column-only planner. Version 2 adds supported new tables and combined namespace checks;
it does not change existing-column reconciliation. Never rewrite a pending version-1 record
to broaden its scope on retry. Fresh previews use the current version and expose it.
This is a versioned companion payload in the existing control-format-1 operations table; the six-/eleven-field
merge payloads and native/export formats are unchanged. Absence means legacy strict mode.
The companion's completed state freezes policy, not a completed merge result; it contains
no retention root. Retries cannot change mode. Pending recomputation requires the recorded
planner; prepared S3 results and completed requests do not replan. Result `commit_extra_info`
also includes `schema_mode` and `schema_algorithm`, including in the remote commit metadata.
The `catalog-v1` planner includes column removal: no new catalog
version or compatibility path is retained. Pending catalog requests use the current rules;
prepared/completed results still replay without recomputation. The `schema_changes.action`
preview field is transient SQL output and does not change a persisted payload or format version.

Native temporal expression settings use a completed `operations.kind='merge_expression_context'`
companion. Its UUIDv8 uses the S3 publication SHA-256 encoding with hash input
`differential-s3-merge-v1\nexpression-context\n<merge operation UUID>`. `request` is NULL and
`database_id` is the input database. Its four length-prefixed fields are
`[native-expression-context-v1, merge operation UUID, TimeZone, Calendar]`. Each setting is
at most 1,024 bytes; the encoded record is capped at 4,096 bytes before decoding. Empty means the native setting
was unavailable; if it becomes available on retry, recomputation fails rather than inventing
a replacement context. No other session settings, credentials or retention roots are stored.
Under the request lock, the first data execution commits this companion in a FULL transaction
before catalog comparison or staging. Its completed state freezes settings, not output.
Recomputation restores these settings through native DuckDB SET operations. Unknown context
versions fail closed; preview/preflight do not create records. Older pending requests without
the companion capture settings on their next data execution. Prepared S3 outputs and completed
requests bypass context decoding and replay unchanged. Result `commit_extra_info` includes
`expression_context: "native-expression-context-v1"`; actual settings live in the companion.
This adds no table/layout change to control format 1.

Resolution choices use a companion `operations.kind='merge_resolutions'` record. Its UUIDv8
uses the same SHA-256 UUID encoding as ancestry below, with the hash input
`differential-snapshot-ancestry-v1\nmerge-resolutions:<merge operation UUID>\n0`.
`request` is NULL, `database_id` is the input database, and `branch_id` holds the parent merge
operation UUID (not a branch). Payload fields are `[typed-resolutions-duckdb-2-v1, specification,
data]`. The specification concatenates length-prefixed target/source strings in map-key order;
the data is binary, length-delimited inside SQLite TEXT, not a NUL-terminated string.

The reservation is `pending`, with empty data, committed atomically with the parent request.
First capture changes it to `complete` in a FULL transaction. The data is a DuckDB 2.0
BinarySerializer Value: a STRUCT keyed by table name, each containing `keys VARCHAR[]` and
`rows LIST<STRUCT<exact table columns, _merge_action VARCHAR, _merge_columns MAP(VARCHAR,VARCHAR)>>`,
and `base`, `ours`, `theirs` BIGINT input IDs.
No choices use an empty byte string. The version tag pins this envelope to DuckDB 2.0;
an unavailable resolution version fails explicitly. The mapping has a 64 KiB bound and data
an 8 MiB bound, with an earlier conservative capture budget and 10,000-row limit.

Exact column values may include supported UUID/binary/unsigned/floating-point/temporal
types and recursively nested containers. They use the existing native DuckDB 2.0 Value
serializer; no JSON/text conversion or new persisted envelope is introduced. Capture budgets
count nested nodes and binary/string bytes before collecting rows into process memory.
The `nested_value` trees exposed by preview/resolution-audit SQL are diagnostic output only;
they are not stored in this envelope. Completed and prepared outputs retain their existing
replay behavior. Older binaries may reject pending inputs using newly supported types.

First committed capture wins concurrent retries; later calls read it rather than mutable source
tables. Completed legacy merges without a companion accept only an empty specification.
`commit_extra_info.resolutions` records the resolution-envelope version; `operation_id` links
the audit and immutable input provenance. A completed companion means choices were frozen,
not that a merge succeeded. Companion records retain user values indefinitely with operation
history, but never act as retention roots. No metadata layout or native storage format changes.

Completed merge replay validates the reserved resolution mapping without decoding the choice
envelope, so it survives an unavailable resolution encoding. Pending execution and audit
retrieval still reject unavailable encodings explicitly.

During execution, logical files `merge_stage/<pin UUID>[.wal[.checkpoint|.recovery]]`
hold the native staging database. They have no branch or history record; only the owning
process registers their URI. On normal pin release or stale-pin reconciliation their names
are cleared in the authority transaction, making their maps/payload eligible for ordinary GC.
Local results publish their branch, snapshot and completed operation atomically. S3 results
use the private publication protocol below. Reconstructed S3 inputs use pin-owned files in
the same disposable namespace, then `diff://merge_input/<pin UUID>` opens the verified root
read-only. These reader URIs require a live process registration and cannot open mutable files.
See [branch merge](branch-merge.md) for restart and idempotency semantics.

### Private S3 merge publication (s3-merge-publication-v3)

Single-writer S3 snapshot and branch merges add companion records to existing control-format-1 tables; table
layouts, native storage and remote object formats are unchanged. A `merge_publication`
operation has no user request ID. Its UUIDv8 is the first 16 SHA-256 bytes of
`differential-s3-merge-v1\npublication\n<merge operation UUID>`, with UUID version/variant bits
set. `database_id`, `branch_id`, and `snapshot_id` reserve the future result identities.
Its length-prefixed payload is:

`[s3-merge-publication-v3, merge operation UUID, destination name, root, logical size,
publication timestamp, provenance JSON, provider UUID, descriptor or empty, head write ID,
ours snapshot ID, theirs snapshot ID, head attempted (0 or 1), action, expected ETag]`.

`action` is `created_branch`, `merged`, `fast_forward` or `already_up_to_date`. New destinations
have an empty expected ETag (conditional creation); target actions freeze a nonempty ETag
verified against the selected exported target and repository ownership (conditional update).
Target records reuse the target branch UUID. Fast-forward/no-op reuse a public snapshot and
its completed descriptor/timestamp, never alter its provenance or allocate another snapshot.
Their provenance is retained in the companion and atomic target audit. A pending publication
record prevents expiry of its result snapshot until completion or abort; other inputs may expire.
No-op never sets head-attempted or writes a remote head.

The payload version advances independently of unchanged control-format-1 table layouts. Existing
12-field `s3-merge-publication-v1` and 13-field `s3-merge-publication-v2` records remain
readable/resumable as new-destination operations. V1 conservatively means `head attempted = 1`;
v2 preserves its explicit flag. Re-encoding writes v3. New records begin at 0; a FULL transaction sets 1 before
any destination-head PUT. This flag never resets, even if an attempt fails. An absent head
after a possible PUT is insufficient evidence for safe abort: the provider may still be
processing a request sent before process death.

The 32-hex-digit random head write ID is committed before S3 I/O. The descriptor field is
set only after export verification (or copied from the already-complete reused export). `export_roots[publication UUID]` holds the result as
`pending` throughout upload and remote head publication. For new output no public `exports`/
`snapshots`/`history` row exists yet; a new destination has no `branches` row. Existing targets
keep their original public state plus `compound_intents`, and reused snapshots remain public. Ordinary/background/claim head publication skips a merge-fenced target, so it cannot roll
back the remote result during the remote/local completion window. `databases.last_snapshot` advances at reservation, so gaps are permitted.

Single-writer S3 ordinary snapshots record `snapshot-parents-v1` locally in their publication
transaction, as local-storage snapshots do. Remote descriptors/commit records are unchanged;
these local parent records are not reconstructed from remote history or inferred for old snapshots.

A `merge_destination` companion reserves the branch name. Its UUIDv8 is derived with the
same recipe, using `destination` and `<database UUID>/<branch name>` as domain/key. Its
payload is the owning merge operation UUID, `database_id` identifies the database, and
`request` is NULL. Branch creation checks this indexed reservation. Completion changes its
state to `complete`; safe abort does the same only for a reservation still owned by that
merge. Later reservations may replace that completed reservation's payload.

`abort_merge` operations have ordinary optional user request IDs and a length-prefixed
`[database UUID, merge operation UUID, merge-abort-v1]` payload. Successful abort of unpublished
work creates a `merge_aborted` companion whose UUIDv8 uses the same derivation with domain
`aborted` and key `<merge operation UUID>`. Its payload is the raw merge UUID, state is
`complete`, and database_id identifies the database. The original merge row remains
`pending` in the raw control-format-1 table because it has no successful output; the terminal marker
prevents any retry. Both public operation inspection functions report it as `aborted`.
The marker, `export_roots.state = abandoned`, reservation release, publication companion
completion and abort-action completion share one FULL transaction. Completed companions
alone never prove successful publication; only the original merge's completed result does.
An abort action that recovers or observes a completed merge records its branch/snapshot;
an aborted outcome records snapshot_id 0. No remote objects or audit rows are deleted.

`locks/merge-<operation UUID>` serializes merge attempts and abort across repository-local
processes. Lock acquisition order is request, then database head lock, then short metadata
transactions. The persistent lock file is not a lease or retention root and is never unlinked
while another process might use it. Kernel ownership disappears on process death.

After uploading the ordinary packs/indexes/descriptor/commit record, publication creates
the destination head only if absent. A head matching the precommitted write identity and
result descriptor/branch/snapshot is accepted on retry. Existing foreign heads and tombstones
are never replaced. The commit record's scalar parent is `ours`; both contributors and the
explicit base remain in `extra_info`, not a general portable ancestry graph.

After confirmed head publication, one FULL transaction inserts the reserved public branch,
snapshot/history, ancestry and completed export, completes both merge/publication operations,
and releases the pending export root to ordinary retention. Before that transaction commits,
only the remote head may expose the valid result. Repeating the original request reconciles
this window without another snapshot or head. Private records and retained result roots are
kept on failure/cancellation until successful retry or explicit safe abort; no automatic expiry is implemented.


### Local snapshot ancestry payload (snapshot-parents-v1)

Local single-writer snapshot publication inserts a complete `operations.kind='snapshot_ancestry'`
record in the **same FULL transaction** as the snapshot and head update. `request` is NULL;
`database_id`, `branch_id` and `snapshot_id` identify the new snapshot, and `preservation` is zero.
These internal records are visible through `differential_operations()`. Their length-prefixed
payload is `[snapshot-parents-v1, kind, first parent ID, second parent ID]`:

- `root`, `import`, `clone`: both parents zero, beginning a local history.
- `linear`, `restore`: one parent (the previous published head), second zero.
- `merge`: the two contributing snapshot IDs, with identical parents deduplicated into one edge.

Parent IDs are in the same database. Zero means no edge. Every nonzero parent must already
exist when the child is published, so normal publication cannot create cycles. Restore follows
any newly published preservation; reusing an existing equal-root snapshot for preservation adds
only its existing history event, not an ancestry edge to that snapshot. Restoring bytes from
another snapshot does not make that target a parent. Imported/clone source fields remain provenance.

The record's primary-key UUID is the first 16 bytes of SHA-256 over UTF-8
`differential-snapshot-ancestry-v1\n<database UUID>\n<decimal snapshot ID>`, with UUID version 8 and
RFC variant bits set. This reserved namespace gives indexed lookups in the existing operations
table without a request-ID reservation or table migration. Parent payloads are at most 256 bytes,
strictly decoded, versioned independently, and never read from user `commit_extra_info`.
Store format 2, control format 1 and export v2 layouts are unchanged. Local and single-writer S3 publication emit
these local records; shared-writer publication does not. Their operation state remains `complete`
when the snapshot export is pending: ancestry completion records local metadata durability, not
export ownership. Remote commit formats and shared renumbering are unchanged.

Parent records contain no roots/files and do not participate in `ROOT_LIVE_SQL`, GC root sets,
reader pins or export retention. Snapshot retirement and payload GC leave them intact. This keeps
logical lineage available without keeping database images indefinitely. Metadata growth follows
snapshot history; compaction is deferred. Missing old records are unknown; no migration backfills
edges. The resolver only accepts such a boundary when it is already below a proven common base,
or when the previous direct-sibling proof suffices. Unrelated or incomplete histories fail.

`unique-base-v1` walks the reachable parent graph, marks reachability from each head and removes
common ancestors dominated by another common descendant. Exactly one remaining candidate is
required. A separate topological pass rejects cycles. Missing non-common ancestry fails proof;
multiple candidates return `merge_base_ambiguous`. Retention/readability checks occur **after**
selection and never change its answer. The walk is bounded by `max_ancestry_nodes` and checks
cancellation/deadlines. Pending requests store the selected IDs once; new parent records or head
changes cannot retarget them. The logical reconciliation algorithm remains `logical-cell-v1`.
