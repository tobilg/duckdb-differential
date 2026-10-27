# Performance and sizing

Differential keeps native DuckDB storage and sync behavior, so query execution is native.
What Differential adds is the cost of storing the database file as versioned chunks, publishing
at sync points and reading through S3. How much that matters depends on table width, compression,
keys and indexes, retained history, the local device and S3 latency. Measure representative data
with the release artifact you intend to deploy; the programs below make that repeatable.

## Writes and commits

A commit is durable at DuckDB's native sync: the file group's segment is synced, then one
SQLite transaction commits with `synchronous=FULL`. Small commits are therefore dominated by
the device's flush latency, as in native DuckDB. With `differential_group_commit` (on by
default), concurrent syncs of different databases in one process share a transaction and a
flush. Bulk loads and checkpoints write whole 256 KiB chunks; unchanged chunks are never
rewritten.

## Branches, clones and history

Branches and clones in the same pool share every unchanged chunk: creating one copies the
chunk map, not the payload, so its metadata cost grows with database size while its payload
cost is zero until it diverges. Retained snapshots keep their chunks alive until they are
expired and collected.

## Local reads

Reads go through the chunk map of the attached version. A long-running instance pays the attach
(metadata store and native catalog) once; a new process per query pays it every time, which
shows on short queries. A separate local payload pool adds a file open and an integrity check
per chunk on first access.

## S3 reads

Exports are read with verified range requests: adjacent pack slots are coalesced into one GET,
concurrent requests for one chunk share a fetch, and `differential_s3_max_requests` caps the
parallel requests per process. Two caches help repeated reads:

- DuckDB's external file cache (in memory, within `memory_limit`) for one instance.
- The repository disk cache (`differential_remote_cache_size`, off by default) across instances
  and processes. Cached chunks are verified on every read, which costs CPU on warm scans.

Evicted snapshots, followers, recovered branches and shared databases read through the same path.
Over a real network, request latency rather than bandwidth usually dominates cold scans; measure
from where your readers run.

## WAL shipping

`SHIP_WAL 'async'` adds change tracking to each commit and ships in the background, so it costs
little at commit time. `SHIP_WAL 'sync'` makes every native sync wait for its epoch to reach S3,
one PUT per commit, so commit latency includes an S3 round trip. Use `async` for bulk loads,
and `sync` only where no acknowledged commit may be lost.

## Merge costs

Snapshot comparison reads logical table contents. A changed table is rebuilt in isolated
native staging so that the final combined state is validated under the supported constraints.
Indexes and affected views are recreated before checkpointing and publication. Sparse changes
can therefore still incur full-table comparison and copying. No changed-row-only scaling is
promised.

S3 merges may additionally rehydrate evicted inputs and upload/verify the result before moving
a branch head. Materialization, comparison, application and publication have separate timers.
Fast-forward and already-up-to-date branch outcomes have different costs from a divergent merge.

## Merge resource controls

Configure DuckDB's `memory_limit`, `threads`, local `temp_directory` and
`max_temp_directory_size`, together with the merge input/staging limits and timeout. See
[merge resource controls](branch-merge.md#resource-controls-and-live-status).

`memory_limit` is a native buffer budget, not a process RSS quota. SDK buffers, indexes and
other allocations contribute to process memory. A query can fail an allocation while temporary
disk remains available; some working sets must fit in memory. Nullable containers, wide rows,
large BLOBs and concurrent queries can materially change capacity requirements.

Native temporary-file cleanup is separate from Differential GC. After process death,
abandoned spill files can remain until DuckDB initializes its temporary-file manager again.
Use the documented [request recovery](branch-merge.md#durable-requests-and-safe-abort) behavior;
a memory or disk failure does not make a partial merge a completed result.

## Measuring a deployment

The programs in [`benchmarks/`](../benchmarks/README.md) cover local commits and scans,
branch/clone sharing, maintenance operations, S3 reads and caches, request parallelism, WAL
shipping latency, TPC-H on every backend, gigabyte-scale workloads and branch merging. They create
disposable repositories (and a disposable MinIO container for S3 runs) and write their results
to the untracked `build/evidence/`. Results from a local MinIO are not AWS latency or cost
measurements, a fresh process is not a physically cold disk, and SDK request counts are not wire
or device I/O.
