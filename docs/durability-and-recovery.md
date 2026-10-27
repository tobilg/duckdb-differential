# Durability and recovery

## Durable local byte store

Native writes go to an in-process overlay (256 KiB chunks, or byte pieces of WAL files) and are
appended to the file group's unsynced, append-only segment before returning. A native
`FileSync` syncs the segment (`fsync`) and then publishes every buffered chunk location,
extent mapping, log range and file length in one SQLite transaction in WAL mode with
`synchronous=FULL` (`fsync`, as native DuckDB, on every OS). Background publication (beyond
16 MiB buffered per database), overlay pressure and handle close publish with
`synchronous=NORMAL`, which survives process death and becomes durable with the next FULL
commit; their segment bytes are synced before that transaction too, so metadata that becomes
durable early (a later FULL commit, a SQLite checkpoint) never references unsynced payload.
Segment and repository-directory creation is synced before use. The repository has an
application identifier and a format version (2). Unrelated SQLite files and any other version
are rejected; there are no migrations.

Durability guarantees:

- An acknowledged native commit is as durable as a native one: DuckDB syncs before
  acknowledging, and that sync is a SQLite FULL commit with `fsync`, the call native DuckDB
  makes. On macOS `fsync` does not flush the device's write cache (neither does native DuckDB).
- Group commit (`differential_group_commit`, on by default) changes only how many syncs share a
  transaction: concurrent syncs of different databases (file groups) in one process commit
  together, up to 16 MiB of new chunk data per transaction (a larger sync commits alone). Each
  sync still returns only after its segment was synced and a FULL commit includes every write
  its file group had completed when it was called. A batch commits all of its databases or none (crash tests kill
  the process just before and just after such a commit). If a batch fails, its syncs are retried
  one at a time, so each failure is reported to the sync that caused it; a failed sync keeps its
  buffered, segment-backed writes, as without group commit.
- A completed but unsynced write survives process death through segment replay.
- After power loss, unsynced writes may be lost, as with native files. Replay applies only
  a checksummed (CRC32C), sequence-consecutive prefix of the segment after its published
  watermark, so the result is always some prefix of completed writes.
- Local reads verify every chunk and log record against the CRC32C recorded at publication;
  SHA-256 remains the content identity (recomputed by verification, exports and pool reads).

The data file and all native WAL names use the same byte store. Rename/replacement and
unlink are transactional and ordered after earlier buffered writes; existing inode handles
survive unlink. Shrinking clears retained partial-chunk tails, so regrowth yields zeros.
Inode numbers are never reused. A writable handle publishes its overlay before it releases
the branch lease. If that fails, the lease is held until the process exits and the segment
replays in the next owner.

On restart SQLite recovers its transaction journal, dead processes' segments are replayed,
then unmodified DuckDB performs
native database/WAL recovery. The adapter preserves `.wal`, `.wal.checkpoint`, and
`.wal.recovery` bytes and namespace operations. Small commits, automatic checkpoint
with WAL, checkpoint instead of WAL, concurrent checkpoint commits, recovery WAL
merge/rename and selected native failure boundaries have executable evidence.

Tests terminate separate processes with SIGKILL or `_exit(86)` inside deterministic
failpoints. At payload insertion, map update, length update, pre-commit and post-commit,
the byte store recovers a complete generation: the new one when the write had completed
(segment replay), otherwise the old one. Separate cases kill after completed but never
synced writes, cut the segment inside its last record, and cut every segment back to its
published (synced) watermark to emulate loss of unsynced kernel state. Native tests also interrupt
WAL sync, block write, database sync and WAL cleanup, then inspect recovered SQL data.
These tests do **not** prove torn-sector/power-loss protection. The implementation
relies on SQLite and the supported local filesystem/device honoring their sync
contracts. Volume loss, network filesystems and multi-host ownership are not covered.

## Captures and retained history

Internal generations are sealed at the `FileSync` that follows a native database-header
write, i.e. at completed checkpoints, with or without `TYPE differential`.
A seal re-hashes only manifest pages changed since the previous seal. Reader
handles keep their fixed root. A public snapshot runs a native checkpoint and then seals
the crash-consistent main file under a capture pin; no thread-local state or checkpoint
callback is involved.

Public snapshot publication, request IDs, UUID-based ownership/epochs and compound
restore/delete now live in transactional control metadata. A completed checkpoint capture
is pinned until publication. Restore/delete persist an intent under exclusive ownership,
recover native WAL on a separate connection, and atomically preserve/replace or tombstone.
Tests kill the process at four compound points, including native acknowledged WAL
preservation, also with an additional payload pool and when a pending restore's target
is retired before retry. Its durable intent retains the target independently.
Pending branch deletions reserve a surviving unfenced branch; another deletion cannot
remove that survivor while the first is awaiting recovery.

GC is distinct from logical retirement. Open root/inode pins use metadata plus local
process locks, and the lock file is authoritative after a crash. Collection keeps named or
open files, latest live generations, retained non-evicted snapshots, active branches,
reader pins, compound targets and pending exports. Completed exports are remotely durable
and no longer retain local payload. Triggers queue every released file, root,
page and object; physical deletion is bounded, incremental and transactional, and runs
automatically after checkpoints on a background worker. Retired segments are deleted once nothing
references them (file before row), and sparse ones are compacted first: live payload is copied,
verified and synced into a new segment that takes over the references in one transaction; reads
that race with it retry on fresh metadata. SQLite pages remain reusable. Incremental
compaction uses the repository's SQLite incremental auto-vacuum mode, set at creation. Tests
crash and retry compaction at two points while a historical reader remains live.

## S3 boundary

The SDK transport is integrated with public snapshot export and remote read-only attachment. It uses an SDK credential chain/profile,
SDK signing/retries, TLS verification by default, bounded range buffers and request
permits. Uploads use explicit SHA-256 checksums; ETags are only multipart completion
tokens. Part uploads are bounded to 5 MiB buffers. Conditional retries validate
existing object size/checksum metadata. Failures attempt SDK multipart abort.

The product's immutable packs (up to 8 MiB, always below the multipart threshold), index
pages and descriptors use single PUT; they never start multipart uploads. The separately tested multipart transport still lacks
durable multipart-intent reconciliation and is not selected by the snapshot format.

S3-profile publication atomically records a snapshot and pending export retention, uploads
all dependencies, then publishes the immutable descriptor last. Descriptor success with
lost local completion leaves the pending root protective. Retirement does not remove it.
The same request retries the same local snapshot and descriptor. Success makes the remote
graph the durable copy, after which local payload can be collected once nothing local
retains it, or evicted explicitly. MinIO tests deleted the original writer directory and read the
export from a fresh repository, then explicitly recovered it into a new owned identity.
`differential_retry_export(database [, id], request_id => ...)` resumes an existing graph
even after its local snapshot is retired and its publishing branch deleted.
These tests are not real-AWS or separate-physical-host verification.

Without WAL shipping, native commits remain locally durable only. Losing the local volume can lose
commits after the latest remotely completed public snapshot.

## Continuous WAL shipping

With `SHIP_WAL` (single-writer S3 databases, opt-in) every publication of a writable branch is also
shipped to S3 as part of an epoch, and `differential_recover_branch` rebuilds the branch on another
host from the newest chain's base plus its consecutive epochs; DuckDB replays the WAL natively.

- Every shipped state is a publication of the local store, so a recovered branch is a state
  DuckDB's own recovery handles (a sync boundary, or a prefix of later completed writes), including
  WAL entries that reference optimistically written blocks of the main file.
- **`sync`:** a native sync returns only after its local FULL commit and once an epoch that includes it
  is in S3. Losing the host loses no acknowledged commit. If shipping fails, the sync reports
  `[differential:ship_failed]`; the commit is durable locally, and its outcome for remote recovery
  is uncertain, like a failed `fsync`.
- **`async`:** a commit returns after its local commit; epochs follow at least `SHIP_INTERVAL` apart.
  Losing the host loses the commits not yet shipped (`differential_branches.ship_pending`).
  Failures are retried with backoff and never fail a commit.
- Epochs are create-only and chained by hash; a copied repository shipping the same branch gets
  `head_conflict` instead of forking the history. Upload intents make a crash during an upload
  safe: the epoch found in S3 with the expected hash is adopted, otherwise rebuilt.
- MinIO tests kill a writer (no detach, no close) after 50 `sync` commits and after an `async`
  interval elapsed, recover on another repository and check every row; they cover a 1.5M-row
  optimistic insert, chain rotation at a checkpoint, a failover with the same identity, a copied
  repository and remote collection of superseded chains (`test/integration/s3_shipping.py`). They
  do not cover power loss or real AWS.

Followers (`FOLLOW true`) read immutable shipped states read-only; one transaction reads one state. Export retention is conservative: S3 data is deleted
only by an explicit `differential_cleanup_remote_files`, one grace period after it became
unreachable. A reader that attached an export descriptor directly loses it once its snapshot
expired and was collected.

## Local existing-target merge recovery

Twenty-three byte-store crash cases include rename replacement, unlink, truncate,
manifest/generation installation before and after commit, completed-unsynced writes, a
torn segment and lost-unsynced-segment emulation. SQLite VFS FULL injection at the sync boundary
leaves the durable state unchanged, keeps the accepted overlay, and the retry publishes
it. Corrupt payloads, missing references
and damaged manifests fail verification. GC and compaction each pass pre/post-commit
process death and idempotent replay. Pin/retirement/GC/compaction races exercise both legal
attachment outcomes without a read-after-reclaim gap in the tested schedules.
`differential_merge_branch` with `destination = ours` takes the target's exclusive branch lease,
replays abandoned segments and records a `compound_intents` fence before comparing the durable
live image with its selected snapshot. Nonempty native sidecars or different bytes fail explicitly;
merging never recovers and then discards unsnapshotted target changes. Other branches and immutable
snapshot readers can stay open, but transactions retaining the target attachment block ownership.

A diverged result is rebuilt, constrained and checkpointed in private staging. One FULL metadata
transaction installs its native mapping and updates the target head, ancestry, outcome/provenance
companion and completed request, then removes the intent. Fast-forward installs the source image
and reuses its snapshot; no-op only completes the audit/request and releases ownership. No export
or background publication occurs. Ancestry is not borrowed from restore.

Failure/cancellation before publication clears the intent while still owning the lease.
The final metadata transaction is not interrupted mid-commit; cancellation can race successful
completion, so inspect/replay the request to determine its durable outcome. If cleanup cannot
commit, or the process dies before publication, target writes remain fenced until the original
request retries or `differential_abort_merge` clears it. Retry requires retained original inputs,
a supported planner, and the unchanged target UUID/head. It recomputes disposable staging; expired
inputs never select another base. Abort can release the intent without those inputs. A publication
transaction either commits all mappings/history/ancestry/completion or none. Completed retries
return their original result without touching a target that may since have advanced or been replaced.
See [target scope](branch-merge.md#merge-into-an-existing-target).

Destination-aware branch preflight does not acquire a lasting reservation. Its temporary
target lease checks the same durable bytes/WAL rule without advancing epochs or writing an
intent; comparison/validation uses ordinary temporary reader pins and disposable staging.
It reports existing pending lifecycle operations and recovery actions without performing them.
The optional expected base/ours/theirs snapshot IDs are checked atomically with initial branch
request reservation; a mismatch leaves no request/output. A small `merge_expected_inputs`
companion records guard identity in that transaction. Retry requires the identical guard and
uses frozen inputs; a completed replay still needs no retained inputs. No input retention is
extended between preflight and execution. See [guarded execution](branch-merge.md#preflight-and-guarded-execution).

Each admitted merge execution has bounded durable latest-attempt diagnostics, exposed through
`differential_merge_requests`. Start and coarse milestones use short FULL transactions under
the existing request lock. Success is part of the result-publication transaction, including
S3 recovery; caught failures record an allowlisted error code without exception text. Failed
diagnostic writes never hide the original execution error. A crash or unwritable failure log
can leave `last_attempt_state='incomplete'`; this does not indicate process liveness, and
request `state`/`recovery_action` remain authoritative. Abort preserves diagnostics unless it
recovers completed S3 publication. Completed replay and preflight add no attempts or retention.
[Immutable compatibility fixtures](../test/fixtures/merge/README.md) cover merging data written
by the published v1.0.0 release, from copied repository bytes.

## S3 merge recovery

Merge requests freeze their schema mode/planner in a versioned companion operation in the
same FULL transaction as their input selection. Additive schema planning writes no user data;
supported new tables and column additions are rebuilt and validated in private native staging. Local
publication commits the new branch/snapshot atomically. Before durable S3 preparation, retry
requires retained inputs and the same planner; afterward it resumes the frozen validated
result without replanning. Policy records retain no input payload. Changing mode with the
same request fails, including after failure. Preview creates no policy operation.

Additive requests record `additive-tables-v2`; a retry never changes a request's recorded
planner. Prepared S3 output and completed results remain replayable without recomputation.
These planner records add no ancestry or indefinite input-retention root.

Opt-in `catalog` requests freeze `catalog-v1`. They also reconcile table/plain nullable column removals and whole
view definitions. A typed comparison proves that a table removed by one side is unchanged
on the other; conflicting edits prevent publication. Planned views bind against empty native
table definitions in an isolated temporary catalog. Column removal additionally proves that
the retaining side has unchanged existing-key values and no inserted keys (including NULL-valued
inserts); retaining-side deletes use ordinary remaining-column row rules. Private result staging drops the original
views in reverse dependency order, removes planned tables, rebuilds retained/added tables,
then recreates views in dependency order. It preserves selected native dependency registrations
and checks bound output names/types before checkpointing. No CASCADE or constraint disabling
is used. Failures before publication leave input branches intact; retries recompute from
retained inputs until the existing durable S3 preparation boundary. Catalog policy records
do not retain input payloads.
Column projection preserves native constraints/indexes and remaps positional constraints;
`column_drop_check`/`column_drops_checked` expose the bounded typed proof before row reconciliation.
Pending catalog recomputation uses its supported planner rules; frozen prepared/completed
outputs do not change.

Single-writer S3 merges have a separate publication boundary from ordinary snapshot/export
calls. A validated, checkpointed result is FULL-committed as a private publication record
with a retained output root and reserved destination. New output is absent from public branch
and snapshot listings; an existing target retains its old local head under a durable target fence. Upload/verification and FULL-committed head-write intent precede a
conditional create of a new S3 head, or an `If-Match` update of an existing target from its
frozen, verified ETag. Fast-forward reuses an export; no-op verifies the target and writes no head.
That head makes the remote result visible, after which one local FULL transaction exposes
the branch/snapshot and completes the request. No transaction spans SQLite and S3.

A nonempty `request_id` is required. Its durable write identity recognizes a successful
head PUT whose reply was lost. Repeating the call resumes the same prepared result, including
after input expiry, and completes local bookkeeping after a crash in the remote/local
publication window. Before preparation, retry recomputes from retained inputs. Ordinary
export retries do not publish private merge results. Background/retry-export/claim head publication
skips merge-fenced targets, including after the remote head advanced but before local completion. Pending results/reservations remain
retained until retry or safe abort. `differential_merge_requests` inspects durable states
without remote reads. `differential_abort_merge` takes the same per-request lock as merge,
then checks any prepared result's remote head: it recovers already-published output, releases
provably unpublished output, or retains uncertain state. A head absent after write intent
may still arrive from a killed process's in-flight PUT. An unchanged old target ETag likewise
cannot disprove an in-flight conditional update. Neither permits abandonment; a different remote
version fences that old condition, or observing the exact result permits local completion. Prepared
fast-forward/no-op output snapshots cannot expire until completion/abort; ancestor inputs are not
retained indefinitely. Before/after-commit abort crashes roll back/replay atomically; terminal markers
prevent retries from recreating an aborted result. See the
[merge publication contract](branch-merge.md#s3-snapshot-and-branch-merges) and
[metadata representation](storage-format.md#private-s3-merge-publication-s3-merge-publication-v3).

## Fault and import coverage

Twenty-three byte-store crash cases include rename replacement, unlink, truncate,
manifest/generation installation before and after commit, completed-unsynced writes, a
torn segment and lost-unsynced-segment emulation. SQLite VFS FULL injection at the sync boundary
leaves the durable state unchanged, keeps the accepted overlay, and the retry publishes
it. Corrupt payloads, missing references
and damaged manifests fail verification. GC and compaction each pass pre/post-commit
process death and idempotent replay. Pin/retirement/GC/compaction races exercise both legal
attachment outcomes without a read-after-reclaim gap in the tested schedules.

Offline native import rejects any WAL, checkpoint-WAL or recovery-WAL sidecar. A separate
native read-only attachment validates the file and holds the upstream read lock; two hash
passes plus size/mtime checks reject changing bytes. Keep the source offline throughout.
The import publishes a new identity; retries replay that result even after source removal.
Independent native engine instances inside one OS process have not been exhaustively
fenced against violating this offline precondition.

S3 export checks new packs, indexes and descriptors by bounded GET/checksum before
descriptor completion. Previously verified immutable objects are trusted by their local
verification record without a HEAD; an externally mutating provider namespace is outside
the ownership contract. Denied uploads, corrupt
verification responses, cancellation and staging exhaustion preserve pending roots.
Derived staging lives under `staging/<repository UUID>` with a shared 64 MiB quota;
cleanup does not touch similarly named files outside that owned namespace. It can be
reconstructed; authoritative SQLite bytes cannot be evicted.
Retirement is not remote access revocation, and there is no remote-reader expiry protocol.

Additional local pools keep immutable payload files outside SQLite. Each file and its
directory entry become durable before the transaction publishes an extent mapping: every
staged file is `fsync`ed, its name is linked, the touched fan-out directories are synced,
and the pool directory is synced once more for the whole batch before the metadata store
commits its references.
A shared/exclusive repository fence prevents physical GC from overtaking a read of an
older SQLite mapping. GC first commits a deletion queue, then removes owned files and
durably clears the queue. A crash during deletion is retried idempotently. A bounded
orphan scan also removes unpublished hashes and abandoned `write-*` files in the
registered repository/provider namespace. Unknown names and other namespaces remain
untouched. Identical hashes conservatively retain every registered local replica while
any map still references the hash. Keep both the metadata store and provider volumes; local
pool files alone do not replace the metadata store. Native compound-recovery tests cover
the four process-death points with the database and WAL payloads in an additional pool.
