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
- `report.py`: render the measured JSON results into [the report](../docs/benchmarks.md).

Each script writes its raw results to `benchmarks/<name>-results.json`. These files are
machine-specific and not tracked; `report.py` needs them locally, and `docs/benchmarks.md` is the
tracked, published report.

Cold means empty native/extension caches, not a purged OS/provider disk cache.
Verified payload bytes are not complete HTTP/TLS wire bytes. These are development
fixtures on one host, not production performance or real-AWS qualification.
