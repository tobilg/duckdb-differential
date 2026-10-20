# Benchmark observations

> **These measurements were made with DuckDB 1.5.6** and predate the move to DuckDB 2.0. For
> current numbers on 2.0, with 1.5.6 as the baseline, see [the TPC-H comparison](benchmarks-tpch.md).

Gigabyte-scale runs (concurrent commits, bulk load, cold scans, S3 export settings, remote
garbage collection, shared history) are in [benchmarks-scale.md](benchmarks-scale.md).

Measured on macOS 26.7 arm64 with stock DuckDB 1.5.6 and a release build without
test hooks. These are development measurements, not release performance promises.
The baseline sections below predate the header-alignment change unless explicitly dated.
These measurements predate 2026-10-19, when sync points still used F_FULLFSYNC on macOS
(they now use fsync, as native DuckDB). Native sync points remain durability boundaries:
every native FileSync committed the buffered writes with SQLite FULL + fullfsync, which is stronger than
stock DuckDB 1.5.6 on macOS (plain fsync). External payload files use
per-file fsync plus one full device flush per published batch.

## Local baseline

Three repetitions, 100000 rows with integer IDs and SHA-256 text. Each query starts
a fresh CLI process; timing includes extension loading. OS caches were not purged.
Fresh process is not a physically cold disk. CPU/RSS and retained file sizes are
recorded by macOS time(1) in `benchmarks/local-results.json`.

| Workload | Native median ms | Default pool median ms | External pool median ms |
| --- | ---: | ---: | ---: |
| load_100k_checkpoint | 85.375 | 296.353 | 338.023 |
| fresh_process_selective_query | 19.317 | 54.341 | 43.833 |
| repeat_fresh_process_selective_query | 18.700 | 42.079 | 43.945 |

At the time of these measurements (store format 1), writes were buffered as whole 64 KiB chunks
plus an unsynced journal and published at native sync points. The remaining small-commit cost is dominated by
the full device flush per native sync (about 3.5 ms on this host).

## Same-pool lifecycle

Eight branches and eight independent clones each added **zero payload objects
and bytes**. Full extent maps are copied; metadata work is proportional to map
size. Clone writes remained isolated. Timings exclude process startup.

- Branch median: 4.988 ms (eight samples).
- Clone median: 5.979 ms (eight samples).

Source stored payload was 3670016 bytes after collecting superseded internal generations.

## Commit and maintenance operations

One 20000-row fixture per mode, twelve single-row commits, three warm scans,
three unchanged snapshots and three maintenance batches. Operations use the
stock C API. Median/p95 below use nearest-rank p95; for twelve samples p95 is
the maximum, and is not a production tail-latency estimate.

| Workload | Native median / p95 ms | Default median / p95 ms | External median / p95 ms |
| --- | ---: | ---: | ---: |
| bulk_commit_20000 | 13.487 / 13.487 | 20.266 / 20.266 | 32.392 / 32.392 |
| small_commit_one_row | 0.177 / 0.966 | 3.845 / 4.712 | 7.670 / 9.835 |
| checkpoint | 9.008 / 9.008 | 23.946 / 23.946 | 31.258 / 31.258 |
| warm_pruned_query | 0.180 / 1.267 | 0.189 / 0.236 | 0.191 / 0.232 |
| warm_full_payload_scan | 0.637 / 0.658 | 0.645 / 0.665 | 0.642 / 0.746 |
| explicit_snapshot_unchanged | — | 7.656 / 7.921 | 5.917 / 6.331 |
| restore_detached_branch | — | 13.370 / 13.370 | 9.387 / 9.387 |
| gc_batch_1000 | — | 3.325 / 4.062 | 3.609 / 14.508 |
| compaction_batch_1000 | — | 3.640 / 5.874 | 3.192 / 3.666 |

The raw operation report records CPU time, local file growth and materialized
payload/lookup/native timing deltas. RSS is the entire Python/native-library
process high-water mark, not incremental operation memory. Restore duration is
an upper bound on its branch write exclusion interval. Reclaimed SQLite pages
can be reused or shrink after WAL checkpoint; physical sectors are not measured.

## S3 selective reads

Pinned MinIO on loopback; one repetition per history depth. A sorted 250000-row
native table permits pruning. Cold is a fresh native attachment with an empty
remote cache; it does not purge MinIO or OS caches. Warm reuses native buffers.
Cold timing includes ATTACH by s3:// locator (provider registration and the
descriptor HEAD/GET) plus index/header reads. Update counts are cumulative
operations, not unique rows.

| Depth / update operations | Logical bytes | Cold verified GET bytes | GET / HEAD | Cold ms | Warm ms / GET |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 / 0 | 9187328 | 475529 | 5 / 1 | 23.477 | 0.308 / 0 |
| 2 / 2500 | 18100224 | 541130 | 6 / 1 | 12.118 | 0.268 / 0 |
| 3 / 5000 | 18624512 | 541162 | 8 / 1 | 14.002 | 0.276 / 0 |

## S3 projected scans and concurrent readers

Full scans read the payload column and validate its total length. Each depth
starts a fresh native buffer manager. Four concurrent scans use independent
native instances at once, the sequential workload four new instances one after
another (like a new in-memory instance per request). Each instance has its own
memory cache (DuckDB's external file cache); concurrent misses share fetches.
The tables below run with the repository disk cache off. Misses of one read are fetched
concurrently within the SDK request permits; adjacent pack slots are coalesced into
one range GET and concurrent requests for one chunk share a single fetch.

| Depth / workload | Wall ms | Verified GET bytes | GET / HEAD | Cache hits / misses |
| --- | ---: | ---: | ---: | ---: |
| 1 / cold_full_projected_scan | 79.104 | 9060745 | 38 / 1 | 39 / 137 |
| 2 / cold_full_projected_scan | 77.859 | 9191882 | 41 / 1 | 64 / 139 |
| 3 / cold_full_projected_scan | 98.160 | 9322986 | 39 / 1 | 31 / 141 |
| 3 / four_concurrent_full_projected_scans | 173.434 | 14763440 | 79 / 4 | 49 / 224 |
| 3 / four_sequential_new_instance_full_scans | 421.499 | 37160872 | 150 / 4 | 207 / 562 |

### With the repository disk cache (`differential_remote_cache_size = '256MB'`)

The same workloads; every workload starts with an empty disk cache. Disk entries are
written by a background writer, so instances running in lockstep share less than
instances that start later.

| Depth / workload | Wall ms | Verified GET bytes | GET / HEAD | Memory hits / misses | Disk hits / writes |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 / cold_full_projected_scan | 135.293 | 9060745 | 38 / 1 | 39 / 137 | 0 / 137 |
| 2 / cold_full_projected_scan | 86.399 | 9191882 | 38 / 1 | 50 / 139 | 0 / 139 |
| 3 / cold_full_projected_scan | 104.077 | 9322986 | 40 / 1 | 42 / 141 | 0 / 141 |
| 3 / four_concurrent_full_projected_scans | 168.388 | 13059504 | 65 / 4 | 79 / 275 | 77 / 170 |
| 3 / four_sequential_new_instance_full_scans | 169.294 | 9323952 | 43 / 4 | 253 / 562 | 421 / 141 |

Per-instance milliseconds of the sequential workload: 102.8, 18.0, 13.5, 14.2.

Request counts are SDK API calls and bytes are successfully verified payload
bytes. HTTP/TLS overhead, retry wire traffic and underlying disk I/O are not
captured. Raw records include retry decisions, publication latency and remote
manifest lookup duration (including index fetch/wait). A separate 3000000-row
resource test reads more than memory_limit (64 MB); the external file cache
stays within it.

## Reproduce

```sh
make release && make integration   # release loadable; extension-free host CLI/libduckdb
export DIFFERENTIAL_EXTENSION="$PWD/build/release/extension/differential/differential.duckdb_extension"
python3 benchmarks/local.py
python3 benchmarks/lifecycle.py
python3 benchmarks/operations.py
DIFFERENTIAL_S3_BENCHMARK=1 python3 test/integration/minio.py
DIFFERENTIAL_S3_BENCHMARK=1 DIFFERENTIAL_BENCH_REMOTE_CACHE=256MB python3 test/integration/minio.py
python3 benchmarks/report.py
```

The fixture owns and removes its MinIO container. No cloud infrastructure is
created. Deep-history/large-metadata-store scale, WAN latency, physical-disk-cold
performance and full wire-I/O attribution remain unmeasured. There is no
claimed optimum for chunk/cache/request sizes and no asserted throughput SLO.

## Header alignment and native prefetch (2026-10-13)

The first 12 KiB of a logical file occupy their own zero-padded object. An isolated default
256 KiB native block therefore touches four data chunks. The unit test overwrites a full
block, verifies exactly four changed chunk mappings, no old-payload materialization, and
an unchanged header object. Native 16 KiB, 64 KiB and 256 KiB allocation sizes also pass
the DDL/DML, checkpoint and historical-read matrix. This does not imply a universal 20–25%
I/O reduction: adjacent writes can share boundary chunks, and small native blocks still
perform partial-chunk writes.

`benchmarks/chunk_layout.py` compares loadable builds in separate disposable repositories
using 200000 rows, single-threaded execution, three repetitions and fresh CLI processes.
It records elapsed time and payload materialization for bulk checkpoint, one-row checkpoint,
scattered updates and full scan. OS caches are not purged. Supply `--extension label=path`
more than once to compare artifacts; the script contains only the current implementation.

An interleaved before/after run on macOS arm64 (three repetitions per artifact, both
release builds) produced these medians. Native durability was enabled throughout.

| Workload | Before ms | Aligned ms | Before materialized bytes | Aligned materialized bytes |
| --- | ---: | ---: | ---: | ---: |
| Bulk checkpoint | 286.66 | 304.72 | 262144 | 131072 |
| One-row checkpoint | 154.54 | 174.85 | 4521984 | 4521984 |
| Scattered update checkpoint | 257.92 | 225.05 | 8257536 | 7208960 |
| Full scan | 59.21 | 67.69 | 7143424 | 7077888 |

Payload read amplification falls for bulk and scattered checkpoints. Latency is mixed:
scattered updates improve, while the other medians are slower. Three repetitions including
process startup do not establish a general speedup or a tail-latency guarantee. The benefit
verified independently is alignment and avoidance of neighboring-payload reads for a full
native block overwrite. Raw samples and artifact hashes are in the local evidence report.

The MinIO prefetch fixture scans 16 DOUBLE columns over 131072 rows, with 16384-row groups,
one thread and fresh processes. With `NEVER`, the native file reader issues 65 reads of
256 KiB. With default `REMOTE_ONLY`, remote exports issue 49 reads of 256 KiB and 8 reads
of 512 KiB (57 data reads total). All scan results match. A local snapshot still issues
65 reads; `ALWAYS_PREFETCH` produces the same batching on either backend. These are observed
native read calls, not S3 request counts or an AWS throughput claim. Cache, transport range
coalescing and the extension's own read-ahead are separate layers.

## TPC-H

TPC-H Q1–Q22 on native files, Differential's local backends and every S3 read path, with writes:
[benchmarks-tpch.md](benchmarks-tpch.md), produced by `make bench-tpch` (repeatable; compare two runs
with `benchmarks/tpch_report.py baseline.json after.json`).

First run (2026-10-20, SF1, 4 threads, macOS x86_64, 8 CPUs, 8 GiB RAM, release extension, MinIO in
Docker on the same machine; geometric means of per-query median ratios):

- **Local, warm:** default store 0.94× native, local pool 1.06×. Per query 0.44× (Q18) to 1.22× (Q3).
- **Local, new process per query:** default store 1.20×, local pool 1.85× (one file open and SHA-256
  check per 256 KiB chunk). Worst Q15 2.58×: short queries pay the attach (metadata store, catalog)
  more than native's single file open.
- **S3, new process per query:** export from a branch head 1.39× native through httpfs (Q1 0.78×,
  Q21 2.55×); evicted snapshot 1.16×; follower 1.27×; recovered branch 1.30×; shared database 1.28×;
  a warm repository disk cache 0.17×. About 180 S3 GET/HEAD requests per query.
- **Writes (`lineitem`, 6M rows):** default store 0.89× native rows/s. To S3 durably: write + snapshot
  export 7.5 s against native write + upload 6.0 s (0.80×). `SHIP_WAL 'async'` costs nothing at write
  time (4.7 s to drain at DETACH); `'sync'` makes the bulk load 3.4× slower, because every sync of the
  load waits for its epoch: use `async` for bulk loads.
- **Compared with the objfs article** (different machine and S3 over the internet, so ratios only):
  local reads 0.94× here against 1.08× there; S3 cold 1.39× against 1.20× (over MinIO, request
  overhead is not hidden behind network latency); writes to S3 0.80× against 0.95×; local writes 0.89×
  against 0.19×.

## WAL shipping commit latency (2026-10-20)

`benchmarks/scale.py --workloads ship_latency` (macOS x86_64, 8 CPUs, local disk-backed MinIO in
Docker, 200 single-row commits of an S3 database, `SHIP_INTERVAL 1`; evidence
`build/evidence/ship-latency-macos.json`):

| `SHIP_WAL` | p50 ms | p99 ms | PUTs per commit | Epochs | Shipped bytes per commit |
| --- | ---: | ---: | ---: | ---: | ---: |
| `off` | 1.28 | 3.62 | 0 | 0 | 0 |
| `async` | 2.03 | 7.79 | 0.03 | 2 | 1,520 |
| `sync` | 13.70 | 27.36 | 1.00 | 200 | 638 |

`async` adds change tracking to each commit and ships the 200 commits as 2 epochs (the first one,
the chain start, includes the base export's packs). `sync` costs one PUT per commit; a commit waits
for the round trip to MinIO, which dominates here. On real S3 expect that round trip to be tens of
milliseconds. A local MinIO is not an AWS latency or cost measurement.

## Disk cache gate (2026-10-20)

`benchmarks/disk_cache.py --scale-gib 1` (same host; 8.9M rows; memory cache off; every measured
scan in a fresh instance; evidence `build/evidence/disk-cache.json`):

| Read path | 1 thread ms | 8 threads ms |
| --- | ---: | ---: |
| Local segments (page cache warm) | 871 | 309 |
| S3 export, cold (fills the disk cache) | 12,974 | 10,319 |
| S3 export, warm disk cache | 2,803 | 1,649 |

Warm disk-cache reads are 3–5× slower than local segments, but not because of its one-file-per-chunk
layout: the scan read each cached chunk about twice (2,256 hits for 1,172 files: read-ahead fetches a
chunk, the read itself fetches it again, since nothing in memory keeps it with the memory cache off),
and every hit re-verifies 256 KiB with SHA-256 (about half of the single-threaded time). Opening the
files costs roughly 0.1 s of the 2.8 s. Larger cache files would therefore save under 5%, so they
were not implemented. The cheaper gains are a CRC32C check for cache entries (SHA-256 when filling)
and keeping read-ahead data for the read that follows.
