# Benchmarks

Run the scripts against `DIFFERENTIAL_BUILD_DIR` or a packaged `DIFFERENTIAL_EXTENSION`.
They create disposable local fixtures; S3 runs inside the pinned MinIO harness.

- `local.py`: native/default/external pool, fresh CLI processes, three repetitions,
  100000 rows, latency/CPU/RSS/local file sizes with sync unchanged.
- `lifecycle.py`: eight branches and eight clones, payload sharing and metadata cost.
- `operations.py`: small commits, bulk ingestion, checkpoint, scans, public snapshot,
  restore, GC and compaction, with timing/counter/file-size records.
- `s3.py`: selective cold/warm and full/concurrent scans across three history/update
  levels, verified bytes, request counts, cache behavior and publication latency.
  Run through `DIFFERENTIAL_S3_BENCHMARK=1 python3 test/integration/minio.py`.
- `s3_throughput.py`: upload and cold-scan throughput per `differential_s3_max_requests` against
  a bucket you own, with export and ATTACH step timings (writes `build/evidence/s3-throughput.json`).
- `scale.py`: gigabyte-scale workloads (concurrent commits across databases, bulk load and
  checkpoint, cold scans, S3 export per export setting, remote garbage collection, shared history
  listing, commit latency with WAL shipping off/async/sync) against a disk-backed MinIO;
  `--scale-gib` (default 2), `--workloads` to select. Manual only: dispatch
  `.github/workflows/Benchmarks.yml` to run it on Linux. Writes `build/evidence/scale.json`.
- `tpch.py` and `tpch_report.py`: TPC-H Q1–Q22 at a scale factor (default SF1) on every backend:
  native file, Differential default store and local pool, and every S3 read path (native file through
  httpfs, exports, warm disk cache, evicted snapshots, followers, shared databases, recovered
  branches) against a disposable MinIO, warm and in new processes, plus `lineitem` writes. Results are
  hashed and must match native. `make bench-tpch` runs it with the release extension (4 threads by
  default, `TPCH_ARGS` for options) and renders [docs/benchmarks-tpch.md](../docs/benchmarks-tpch.md);
  `tpch_report.py baseline.json after.json` compares two runs and flags significant changes.
- `disk_cache.py`: scans an export from the repository disk cache (memory cache off) against the
  same scan from local segments, at 1 and 8 threads (writes `build/evidence/disk-cache.json`).
- `report.py`: render the measured JSON results into [the report](../docs/benchmarks.md).
- `scale_report.py <scale.json> [<after.json>]`: render one scale run, or a baseline and a later
  run of the same scale, into [docs/benchmarks-scale.md](../docs/benchmarks-scale.md).

Each script writes its raw results to `benchmarks/<name>-results.json`. These files are
machine-specific and not tracked; `report.py` needs them locally, and `docs/benchmarks.md` is the
tracked, published report.

Cold means empty native/extension caches, not a purged OS/provider disk cache.
Verified payload bytes are not complete HTTP/TLS wire bytes. These are development
fixtures on one host, not production performance or real-AWS qualification.

## Branch merge programs

| Program | Workload |
| --- | --- |
| [logical_merge.py](logical_merge.py) | Snapshot comparison and application with sparse/dense changes |
| [merge_acceptance.py](merge_acceptance.py) | Repeated bounded merge workloads with exact result checks |
| [merge_history.py](merge_history.py) | Base selection over retained snapshot history |
| [merge_resolutions.py](merge_resolutions.py) | Explicit typed conflict choices |
| [merge_table_additions.py](merge_table_additions.py) | Added tables, overlapping inserted keys and observed spill |
| [merge_catalog.py](merge_catalog.py) | Table removal, view merging and ordinary row changes |
| [merge_column_removal.py](merge_column_removal.py) | Removed-column proof, native constraints and projected results |
| [merge_target.py](merge_target.py) | Existing targets, fast-forward and already-up-to-date outcomes |
| [merge_preflight.py](merge_preflight.py) | Comparison, full preflight and guarded target execution |
| [merge_contention.py](merge_contention.py) | Merge interaction with ordinary reads |
| [merge_types.py](merge_types.py) | Supported native scalar/container values and typed verification |
| [merge_native_scale.py](merge_native_scale.py) | 100k/1M rows, 1%/20% updated keys, nullable containers and three repeats |
| [s3_merge.py](s3_merge.py) | S3 input reconstruction, comparison, application and publication |
| [merge_native_scale_minio.py](merge_native_scale_minio.py) | The native scale matrix with a fresh 1 GiB MinIO per sample and independent export verification |

Merge programs write their reports under the untracked `build/evidence/`. For example:

```sh
DIFFERENTIAL_EXTENSION="$PWD/build/release/extension/differential/differential.duckdb_extension" \
  python3 benchmarks/merge_native_scale.py --report build/evidence/native-scale-local.json
python3 test/integration/minio.py --merge-benchmark          # also --merge-schema-benchmark, --merge-target-benchmark
DIFFERENTIAL_EXTENSION="$PWD/build/release/extension/differential/differential.duckdb_extension" \
  python3 benchmarks/merge_native_scale_minio.py --output build/evidence/native-scale-minio
```

See [merge qualification](../docs/merge-qualification.md#native-scale-and-observed-spill) for the
native scale schema, limits and the separate spill failure tests.
