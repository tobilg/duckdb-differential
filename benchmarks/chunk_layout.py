#!/usr/bin/env python3
"""Compare builds on native checkpoints in disposable repositories.

Pass --extension label=/path/to/differential.duckdb_extension once or several times.
Each build runs in its own processes and directories. No alternate layout implementation
is retained here. Timings include process startup; OS caches are not purged.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import statistics
import subprocess
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'test/integration'))
from paths import DUCKDB_CLI, EXTENSION
from evidence import evidence_path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--extension', action='append', help='label=artifact path; repeat to compare builds')
    parser.add_argument('--repetitions', type=int, default=3)
    args = parser.parse_args()
    if args.repetitions <= 0:
        parser.error('--repetitions must be positive')
    variants = dict(value.split('=', 1) for value in args.extension) if args.extension else {'current': str(EXTENSION)}
    if any(not re.fullmatch(r'[A-Za-z0-9_-]+', label) for label in variants):
        parser.error('build labels may contain only letters, numbers, underscore and hyphen')
    report = {'platform': platform.platform(), 'command': sys.argv, 'scope': __doc__, 'measurements': []}
    select = "SELECT metric,value FROM differential_storage_stats() WHERE metric IN ('local_materialized_payload_bytes','local_logical_read_bytes','local_write_calls','local_sync_calls');"
    with tempfile.TemporaryDirectory(prefix='differential-chunk-bench-') as tmp:
        root = Path(tmp)
        for repetition in range(args.repetitions):
            # Alternate build order so one variant is not always measured first.
            order = list(variants.items())
            for label, artifact in order if repetition % 2 == 0 else reversed(order):
                extension = Path(artifact).resolve()
                quoted_extension = str(extension).replace("'", "''")
                repository = root / label / str(repetition)
                def run(workload, sql):
                    start = time.monotonic()
                    result = subprocess.run([DUCKDB_CLI, '-init', '/dev/null', '-unsigned', '-batch', '-bail', '-json'],
                                            input=f"LOAD '{quoted_extension}'; SET threads=1; SET differential_auto_gc=false; "
                                                  "ATTACH 'differential:bench' AS d; " + sql + select,
                                            text=True, capture_output=True, timeout=120,
                                            env=dict(os.environ, DIFFERENTIAL_STORE_ROOT=str(repository)))
                    seconds = time.monotonic() - start
                    if result.returncode:
                        raise RuntimeError(result.stderr)
                    decoder, blocks, text = json.JSONDecoder(), [], result.stdout.strip()
                    while text:
                        block, end = decoder.raw_decode(text)
                        blocks.append(block)
                        text = text[end:].lstrip()
                    if workload == 'full_scan':
                        assert blocks[0] == [{'bytes': 12800000}], blocks[0]
                    item = {'build': label, 'extension_sha256': hashlib.sha256(extension.read_bytes()).hexdigest(),
                            'repetition': repetition, 'workload': workload, 'milliseconds': seconds * 1000,
                            'counters': {r['metric']: r['value'] for r in blocks[-1]}}
                    report['measurements'].append(item)
                run('bulk_checkpoint', "CREATE TABLE d.t AS SELECT i,sha256(i::VARCHAR) AS payload FROM range(200000) t(i); CHECKPOINT d;")
                run('small_checkpoint', "UPDATE d.t SET payload=sha256('changed') WHERE i=100000; CHECKPOINT d;")
                run('scattered_update_checkpoint', "UPDATE d.t SET payload=sha256((i+1)::VARCHAR) WHERE i%1000=0; CHECKPOINT d;")
                run('full_scan', "SELECT sum(length(payload))::BIGINT AS bytes FROM d.t;")
    for label in variants:
        for workload in ('bulk_checkpoint', 'small_checkpoint', 'scattered_update_checkpoint', 'full_scan'):
            entries = [r for r in report['measurements'] if r['build']==label and r['workload']==workload]
            print(label, workload, 'median ms', round(statistics.median(r['milliseconds'] for r in entries), 2),
                  'median materialized bytes', statistics.median(r['counters']['local_materialized_payload_bytes'] for r in entries))
    evidence_path('chunk-layout-benchmark.json').write_text(json.dumps(report, indent=2) + '\n')


if __name__ == '__main__':
    main()
