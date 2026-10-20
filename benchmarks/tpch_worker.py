#!/usr/bin/env python3
"""One DuckDB process of benchmarks/tpch.py: `tpch_worker.py <spec.json>`.

The spec names the repository, settings, setup statements and TPC-H queries to run. For each query
the worker runs `warmups` unmeasured and `runs` measured executions (wall time of the query, its
result materialized) and prints one JSON line: times, a hash of the result (when asked) and the
S3 requests and bytes the measured executions caused (differential_storage_stats deltas)."""
import json
from pathlib import Path
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'test/integration'))
from paths import EXTENSION  # noqa: E402
from stock_capi import Host  # noqa: E402

STATS = ('s3_get_calls', 's3_head_calls', 's3_list_calls', 's3_put_calls', 's3_verified_get_bytes',
         'remote_logical_read_bytes', 'remote_disk_cache_hits')

spec = json.loads(Path(sys.argv[1]).read_text())
host = Host()
conn = host.connect()
extension_dir = spec['extension_dir']
host.query(conn, f"SET extension_directories=['{extension_dir}']")
host.query(conn, "SET autoinstall_known_extensions = false")
for extension in spec.get('extensions', []):
    host.query(conn, f"LOAD {extension}")
host.query(conn, f"LOAD '{EXTENSION}'")
host.query(conn, f"SET differential_repository = '{spec['repository']}'")
host.query(conn, f"SET threads = {spec['threads']}")
if spec.get('memory_limit'):
    host.query(conn, f"SET memory_limit = '{spec['memory_limit']}'")
for statement in spec.get('setup', []):
    host.query(conn, statement)


def stats():
    values = {}
    for name in STATS:
        value = host.query(conn, f"SELECT value FROM differential_storage_stats() WHERE metric = '{name}'")
        values[name] = value or 0
    return values


for query in spec.get('queries', []):
    text = query['sql'].rstrip().rstrip(';')
    for _ in range(spec.get('warmups', 0)):
        host.query(conn, text)
    before = stats() if spec.get('stats') else {}
    times = []
    for _ in range(spec.get('runs', 1)):
        start = time.perf_counter()
        host.query(conn, text)
        times.append((time.perf_counter() - start) * 1000)
    after = stats() if spec.get('stats') else {}
    result = {'query': query['number'], 'times_ms': times,
              'requests': {k: after[k] - before[k] for k in after}}
    if spec.get('hash'):
        # Order-independent digest of the full result (not timed).
        result['hash'] = host.text(conn, f"SELECT md5(coalesce(string_agg(row, chr(10) ORDER BY row), '')) "
                                         f"FROM (SELECT CAST(t AS VARCHAR) AS row FROM ({text}) t)")
    print(json.dumps(result), flush=True)
for statement in spec.get('after', []):
    start = time.perf_counter()
    value = host.query(conn, statement['sql'])
    print(json.dumps({'step': statement['name'], 'ms': (time.perf_counter() - start) * 1000, 'value': value}),
          flush=True)
host.close()
