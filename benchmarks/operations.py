#!/usr/bin/env python3
"""Native/differential commit and maintenance costs in disposable repositories."""
import hashlib
import json
import os
from pathlib import Path
import platform
import resource
import sqlite3
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]
import sys as _sys
_sys.path.insert(0,str(ROOT/'test/integration'))
from paths import DUCKDB_CLI, EXTENSION, DUCKDB_LIBRARY, BUILD_DIR, TEST_BINARIES

sys.path.insert(0, str(ROOT / 'test/integration'))
from stock_capi import Host

EXT = EXTENSION
measurements = []
metrics = ('local_logical_read_bytes', 'local_materialized_payload_bytes',
           'local_pool_write_bytes', 'extent_lookup_nanoseconds',
           'native_commit_nanoseconds', 'explicit_checkpoint_nanoseconds')

for mode in ('native', 'differential', 'differential_external_pool'):
    with tempfile.TemporaryDirectory(prefix='differential-operations-bench-') as tmp:
        folder = Path(tmp)
        os.environ['DIFFERENTIAL_STORE_ROOT'] = str(folder / 'repository')
        host = Host()
        c = host.connect()
        try:
            if mode == 'native':
                host.query(c, f"ATTACH '{folder / 'native.duckdb'}' AS d;")
            else:
                host.query(c, f"LOAD '{EXT}';")
                provider = 'local'
                if mode.endswith('external_pool'):
                    provider = str(folder / 'payload')
                data_path = f" (DATA_PATH '{provider}')" if provider != 'local' else ''
                host.query(c, f"ATTACH 'differential:bench' AS d{data_path};")

            def counters():
                if mode == 'native':
                    return {}
                return {name: host.query(c, f"SELECT value FROM differential_storage_stats() WHERE metric='{name}';") for name in metrics}

            def disk():
                return sum(p.stat().st_size for p in folder.rglob('*') if p.is_file())

            def measure(workload, sql, iteration=0, expected=None):
                before_counters = counters()
                before_disk = disk()
                usage = resource.getrusage(resource.RUSAGE_SELF)
                start = time.perf_counter()
                value = host.query(c, sql)
                elapsed = (time.perf_counter() - start) * 1000
                after_usage = resource.getrusage(resource.RUSAGE_SELF)
                if expected is not None:
                    assert value == expected, (mode, workload, value, expected)
                after_counters = counters()
                measurements.append({
                    'mode': mode, 'workload': workload, 'iteration': iteration,
                    'milliseconds': elapsed,
                    'user_cpu_seconds': after_usage.ru_utime - usage.ru_utime,
                    'system_cpu_seconds': after_usage.ru_stime - usage.ru_stime,
                    'process_high_water_rss_bytes': after_usage.ru_maxrss * (1 if sys.platform == 'darwin' else 1024),
                    'local_bytes_after': disk(), 'local_byte_change': disk() - before_disk,
                    'counter_deltas': {k: after_counters[k] - v for k, v in before_counters.items()}
                })

            measure('bulk_commit_20000', "CREATE TABLE d.t AS SELECT range AS id,sha256(range::VARCHAR) AS payload FROM range(20000);")
            for i in range(12):
                measure('small_commit_one_row', f"INSERT INTO d.t VALUES({20000+i},sha256('{20000+i}'));", i)
            measure('checkpoint', 'CHECKPOINT d;')
            for i in range(3):
                measure('warm_pruned_query', 'SELECT sum(id) FROM d.t WHERE id<10;', i, 45)
                measure('warm_full_payload_scan', 'SELECT sum(length(payload)) FROM d.t;', i, 20012 * 64)
            if mode != 'native':
                for i in range(3):
                    measure('explicit_snapshot_unchanged', f"CALL differential_create_snapshot('d',request_id:='snapshot-{i}');", i)
                host.query(c, "UPDATE d.t SET payload='changed' WHERE id%2=0; DETACH d;")
                measure('restore_detached_branch', "CALL differential_restore('bench',1,branch:='main',request_id:='restore');")
                host.query(c, "ATTACH 'differential:bench' AS d;")
                assert host.query(c, 'SELECT sum(length(payload)) FROM d.t;') == 20012 * 64
                host.query(c, "CALL differential_expire_snapshots('bench',versions:=[4]);")
                for i in range(3):
                    measure('gc_batch_1000', f"CALL differential_cleanup_old_files(max_objects:=1000,request_id:='gc-{i}');", i)
                for i in range(3):
                    measure('compaction_batch_1000', f"CALL differential_compact(max_pages:=1000,request_id:='compact-{i}');", i)
                with sqlite3.connect(folder / 'repository/engine.sqlite') as metadata:
                    assert metadata.execute('PRAGMA foreign_key_check').fetchall() == []
        finally:
            host.close()

report = {
    'platform': platform.platform(), 'extension_sha256': hashlib.sha256(EXT.read_bytes()).hexdigest(),
    'command': 'python3 benchmarks/operations.py',
    'durability': 'Native sync unchanged; default FULL/fullfsync and external-pool file/directory full sync enabled.',
    'scope': 'One fixture per mode, 12 small commits, three warm-query/snapshot/maintenance repetitions. C API call timing excludes setup and metric polling. Restore duration bounds its exclusive branch-lock interval. RSS is the Python/stock-library process lifetime high-water mark, not per-call allocation. Directory sizes are logical file sizes, not physical allocated sectors. No OS cache purge. Materialized-byte counters include immutable write amplification; no native provider-byte instrumentation.',
    'measurements': measurements
}
(ROOT / 'benchmarks/operations-results.json').write_text(json.dumps(report, indent=2) + '\n')
print('PASS native/default/external pool commit, scan, snapshot, restore, GC and compaction benchmarks')
