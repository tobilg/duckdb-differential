#!/usr/bin/env python3
"""Disk cache gate: is a remote read served from the repository disk cache (one file per 256 KiB
chunk) measurably slower than the same read from local segments? Only then would larger cache files
(several chunks per file, fewer opens) pay off.

Exports a table to a disposable MinIO, then scans it with the memory cache off: from the writer's
local segments, and from a fresh repository's disk cache (cold: fills it; warm: served by it), at 1
and 8 threads. Writes build/evidence/disk-cache.json.

    python3 benchmarks/disk_cache.py [--scale-gib 1]"""
import argparse
import datetime
import json
import os
from pathlib import Path
import platform
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'test/integration'))
from paths import EXTENSION  # noqa: E402
from stock_capi import Host  # noqa: E402
from minio_server import create_bucket, minio_server  # noqa: E402

parser = argparse.ArgumentParser()
parser.add_argument('--scale-gib', type=float, default=1.0)
parser.add_argument('--output', default=str(ROOT / 'build/evidence/disk-cache.json'))
options = parser.parse_args()
ROWS = max(1000, int(options.scale_gib * 2 ** 30 / 120))
BUCKET = 'differential-cache-bench'
results = []


def instance(repository):
    host = Host()
    conn = host.connect()
    host.query(conn, f"LOAD '{EXTENSION}'")
    host.query(conn, f"SET differential_repository = '{repository}'")
    host.query(conn, "SET enable_external_file_cache = false")  # disk cache only
    return host, conn


def scan(host, conn, alias, threads):
    host.query(conn, f"SET threads = {threads}")
    start = time.perf_counter()
    host.query(conn, f"SELECT sum(length(pad)) FROM {alias}.t")
    return (time.perf_counter() - start) * 1000


with tempfile.TemporaryDirectory(prefix='differential-disk-cache-') as tmp:
    work = Path(tmp)
    data = work / 'minio'
    data.mkdir()
    with minio_server(data) as (endpoint, credentials, _):
        os.environ.update({k: credentials[k] for k in ('AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY',
                                                       'AWS_EC2_METADATA_DISABLED', 'AWS_DEFAULT_REGION')})
        create_bucket(endpoint, credentials, BUCKET)
        s3 = f"ENDPOINT '{endpoint}', PATH_STYLE true, ALLOW_HTTP true"
        location = f's3://{BUCKET}/data/'
        host, conn = instance(work / 'writer')
        host.query(conn, f"ATTACH 'differential:big' AS big (DATA_PATH '{location}', {s3})")
        host.query(conn, f"CREATE TABLE big.t AS SELECT range AS v, md5(range::VARCHAR) || md5((range * 7)::VARCHAR) "
                         f"AS pad FROM range({ROWS})")
        host.query(conn, "CALL differential_create_snapshot('big')")
        scan(host, conn, 'big', 8)  # the OS page cache holds the segments
        host.close()
        # Each measured scan runs in a new instance: DuckDB's buffer pool must not serve it.
        for threads in (1, 8):
            host, conn = instance(work / 'writer')
            host.query(conn, "ATTACH 'differential:big' AS big")
            results.append({'path': 'local_segments', 'threads': threads, 'run': 'warm',
                            'scan_ms': scan(host, conn, 'big', threads)})
            host.close()
        for threads in (1, 8):
            reader = work / f'reader-{threads}'
            host, conn = instance(reader)
            host.query(conn, "SET differential_remote_cache_size = '8GB'")
            host.query(conn, f"ATTACH 'differential:{location}heads/big' AS r ({s3})")
            results.append({'path': 'remote_cold', 'threads': threads, 'run': 'cold',
                            'scan_ms': scan(host, conn, 'r', threads)})
            time.sleep(2)  # the disk cache writer drains its queue
            host.close()
            host, conn = instance(reader)
            host.query(conn, "SET differential_remote_cache_size = '8GB'")
            host.query(conn, f"ATTACH 'differential:{location}heads/big' AS r ({s3})")
            hits_before = host.query(conn, "SELECT value FROM differential_storage_stats() WHERE metric = 'remote_disk_cache_hits'")
            warm = scan(host, conn, 'r', threads)
            hits = host.query(conn, "SELECT value FROM differential_storage_stats() WHERE metric = 'remote_disk_cache_hits'")
            files = sum(1 for p in (reader / 'cache').rglob('*') if p.is_file())
            results.append({'path': 'disk_cache', 'threads': threads, 'run': 'warm', 'scan_ms': warm,
                            'disk_cache_hits': (hits or 0) - (hits_before or 0), 'cache_files': files})
            host.close()
output = Path(options.output)
output.parent.mkdir(parents=True, exist_ok=True)
output.write_text(json.dumps({'platform': platform.platform(), 'cpus': os.cpu_count(), 'rows': ROWS,
                              'scale_gib': options.scale_gib,
                              'measured_at': datetime.datetime.now(datetime.timezone.utc).isoformat(),
                              'results': results}, indent=2) + '\n')
for r in results:
    print(json.dumps(r))
print('Results:', output)
