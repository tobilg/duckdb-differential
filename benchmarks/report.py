#!/usr/bin/env python3
"""Render the checked-in measurements; never invent missing benchmark samples."""
import json
import math
from pathlib import Path
import statistics

ROOT = Path(__file__).resolve().parents[1]
def load(name):
    return json.loads((ROOT / 'benchmarks' / (name + '-results.json')).read_text())

local, lifecycle, operations, s3 = [load(name) for name in ('local', 'lifecycle', 'operations', 's3')]
lines = [
    '# Benchmark observations', '',
    'Gigabyte-scale runs (concurrent commits, bulk load, cold scans, S3 export settings, remote',
    'garbage collection, shared history) are rendered by benchmarks/scale_report.py.', '',
    'Measured on macOS 26.7 arm64 with stock DuckDB 1.5.6 and a release build without',
    'test hooks. These are development measurements, not release performance promises.',
    'Native sync points remain durability boundaries: every native FileSync commits the',
    'buffered writes with SQLite FULL + fullfsync (F_FULLFSYNC), which is stronger than',
    'stock DuckDB 1.5.6 on macOS (plain fsync). External payload files use',
    'per-file fsync plus one full device flush per published batch.', '',
    '## Local baseline', '',
    'Three repetitions, 100000 rows with integer IDs and SHA-256 text. Each query starts',
    'a fresh CLI process; timing includes extension loading. OS caches were not purged.',
    'Fresh process is not a physically cold disk. CPU/RSS and retained file sizes are',
    'recorded by macOS time(1) in `benchmarks/local-results.json`.', '',
    '| Workload | Native median ms | Default pool median ms | External pool median ms |',
    '| --- | ---: | ---: | ---: |'
]
modes = ('native', 'differential', 'differential_external_pool')
for workload in ('load_100k_checkpoint', 'fresh_process_selective_query', 'repeat_fresh_process_selective_query'):
    values = [statistics.median(r['milliseconds'] for r in local['measurements'] if r['mode'] == mode and r['workload'] == workload) for mode in modes]
    lines.append('| ' + workload + ' | ' + ' | '.join(f'{v:.3f}' for v in values) + ' |')
lines += ['', 'Writes are buffered as whole 64 KiB chunks plus an unsynced journal and published',
          'at native sync points. The remaining small-commit cost is dominated by',
          'the full device flush per native sync (about 3.5 ms on this host).', '']
lines += ['## Same-pool lifecycle', '',
          'Eight branches and eight independent clones each added **zero payload objects',
          'and bytes**. Full extent maps are copied; metadata work is proportional to map',
          'size. Clone writes remained isolated. Timings exclude process startup.', '']
for kind in ('branch', 'clone'):
    values = [r['milliseconds'] for r in lifecycle['measurements'] if r['kind'] == kind]
    lines.append(f'- {kind.capitalize()} median: {statistics.median(values):.3f} ms (eight samples).')
lines += ['', 'Source stored payload was ' + str(lifecycle['source_payload_bytes']) +
          ' bytes after collecting superseded internal generations.', '',
          '## Commit and maintenance operations', '',
          'One 20000-row fixture per mode, twelve single-row commits, three warm scans,',
          'three unchanged snapshots and three maintenance batches. Operations use the',
          'stock C API. Median/p95 below use nearest-rank p95; for twelve samples p95 is',
          'the maximum, and is not a production tail-latency estimate.', '',
          '| Workload | Native median / p95 ms | Default median / p95 ms | External median / p95 ms |',
          '| --- | ---: | ---: | ---: |']
for workload in dict.fromkeys(r['workload'] for r in operations['measurements']):
    cells = []
    for mode in modes:
        values = sorted(r['milliseconds'] for r in operations['measurements'] if r['mode'] == mode and r['workload'] == workload)
        cells.append(f'{statistics.median(values):.3f} / {values[math.ceil(.95*len(values))-1]:.3f}' if values else '—')
    lines.append('| ' + workload + ' | ' + ' | '.join(cells) + ' |')
lines += ['', 'The raw operation report records CPU time, local file growth and materialized',
          'payload/lookup/native timing deltas. RSS is the entire Python/native-library',
          'process high-water mark, not incremental operation memory. Restore duration is',
          'an upper bound on its branch write exclusion interval. Reclaimed SQLite pages',
          'can be reused or shrink after WAL checkpoint; physical sectors are not measured.', '',
          '## S3 selective reads', '',
          'Pinned MinIO on loopback; one repetition per history depth. A sorted 250000-row',
          'native table permits pruning. Cold is a fresh native attachment with an empty',
          'remote cache; it does not purge MinIO or OS caches. Warm reuses native buffers.',
          'Cold timing includes ATTACH by s3:// locator (provider registration and the',
          'descriptor HEAD/GET) plus index/header reads. Update counts are cumulative',
          'operations, not unique rows.', '',
          '| Depth / update operations | Logical bytes | Cold verified GET bytes | GET / HEAD | Cold ms | Warm ms / GET |',
          '| --- | ---: | ---: | ---: | ---: | ---: |']
for r in s3['results']:
    lines.append(f"| {r['history_depth']} / {r['fragmentation_updates']} | {r['logical_size']} | {r['cold_verified_get_bytes']} | {r['cold_get_calls']} / {r['cold_head_calls']} | {r['cold_attach_query_ms']:.3f} | {r['warm_query_ms']:.3f} / {r['warm_get_calls']} |")
lines += ['', '## S3 projected scans and concurrent readers', '',
          'Full scans read the payload column and validate its total length. Each depth',
          'starts a fresh native buffer manager. Four concurrent scans use independent',
          'native instances at once, the sequential workload four new instances one after',
          'another (like a new in-memory instance per request). Each instance has its own',
          'memory cache (DuckDB\'s external file cache); concurrent misses share fetches.',
          'The tables below run with the repository disk cache off. Misses of one read are fetched',
          'concurrently within the SDK request permits; adjacent pack slots are coalesced into',
          'one range GET and concurrent requests for one chunk share a single fetch.', '',
          '| Depth / workload | Wall ms | Verified GET bytes | GET / HEAD | Cache hits / misses |',
          '| --- | ---: | ---: | ---: | ---: |']
for r in s3['scans']:
    c = r['counter_deltas']
    lines.append(f"| {r['history_depth']} / {r['workload']} | {r.get('milliseconds',r.get('wall_milliseconds')):.3f} | {c['s3_verified_get_bytes']} | {c['s3_get_calls']} / {c['s3_head_calls']} | {c['remote_cache_hits']} / {c['remote_cache_misses']} |")
disk_cache_results = ROOT / 'benchmarks/s3-results-disk-cache.json'
if disk_cache_results.exists():
    cached = json.loads(disk_cache_results.read_text())
    lines += ['', f"### With the repository disk cache (`differential_remote_cache_size = '{cached['remote_cache_size']}'`)", '',
              'The same workloads; every workload starts with an empty disk cache. Disk entries are',
              'written by a background writer, so instances running in lockstep share less than',
              'instances that start later.', '',
              '| Depth / workload | Wall ms | Verified GET bytes | GET / HEAD | Memory hits / misses | Disk hits / writes |',
              '| --- | ---: | ---: | ---: | ---: | ---: |']
    for r in cached['scans']:
        c = r['counter_deltas']
        lines.append(f"| {r['history_depth']} / {r['workload']} | {r.get('milliseconds',r.get('wall_milliseconds')):.3f} | {c['s3_verified_get_bytes']} | {c['s3_get_calls']} / {c['s3_head_calls']} | {c['remote_cache_hits']} / {c['remote_cache_misses']} | {c['remote_disk_cache_hits']} / {c['remote_disk_cache_writes']} |")
    sequential = [r for r in cached['scans'] if r['workload'] == 'four_sequential_new_instance_full_scans']
    if sequential:
        lines += ['', 'Per-instance milliseconds of the sequential workload: ' +
                  ', '.join(f'{v:.1f}' for v in sequential[0]['reader_milliseconds']) + '.']
lines += ['', 'Request counts are SDK API calls and bytes are successfully verified payload',
          'bytes. HTTP/TLS overhead, retry wire traffic and underlying disk I/O are not',
          'captured. Raw records include retry decisions, publication latency and remote',
          'manifest lookup duration (including index fetch/wait). A separate 900000-row',
          'resource test reads more than memory_limit (16 MB); the external file cache',
          'stays within it.', '',
          '## Reproduce', '', '```sh',
          'make release && make integration   # release loadable; extension-free host CLI/libduckdb',
          'export DIFFERENTIAL_EXTENSION="$PWD/build/release/extension/differential/differential.duckdb_extension"',
          'python3 benchmarks/local.py', 'python3 benchmarks/lifecycle.py',
          'python3 benchmarks/operations.py',
          'DIFFERENTIAL_S3_BENCHMARK=1 python3 test/integration/minio.py',
          'DIFFERENTIAL_S3_BENCHMARK=1 DIFFERENTIAL_BENCH_REMOTE_CACHE=256MB python3 test/integration/minio.py',
          'python3 benchmarks/report.py', '```', '',
          'The fixture owns and removes its MinIO container. No cloud infrastructure is',
          'created. Deep-history/large-metadata-store scale, WAN latency, physical-disk-cold',
          'performance and full wire-I/O attribution remain unmeasured. There is no',
          'claimed optimum for chunk/cache/request sizes and no asserted throughput SLO.', '']
output = ROOT / 'build/evidence/benchmark-report.md'
output.parent.mkdir(parents=True, exist_ok=True)
output.write_text('\n'.join(lines))
print(f'Wrote {output} from measured JSON results')
