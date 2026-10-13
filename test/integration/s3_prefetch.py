#!/usr/bin/env python3
"""Native REMOTE_ONLY block prefetch batches S3 export reads, but not local handles.

Use fresh processes (empty DuckDB buffers and memory cache), fixed single-thread scans,
and trace actual native Read calls. Transport and chunk read-ahead are separate layers.
"""
from collections import Counter
import json
import os
from pathlib import Path
import subprocess
import sys
from evidence import evidence_path
from paths import DUCKDB_CLI, EXTENSION

endpoint, directory = sys.argv[1:]
base = Path(directory) / 'prefetch'
base.mkdir()
location = 's3://differential-sdk-test/prefetch-test/'
options = f"ENDPOINT '{endpoint}', PATH_STYLE true, ALLOW_HTTP true"


def run(name, repository, sql):
    trace = base / (name + '.trace')
    result = subprocess.run([DUCKDB_CLI, '-init', '/dev/null', '-unsigned', '-batch', '-bail', '-json'],
                            input=f"LOAD '{EXTENSION}'; SET threads=1; SET memory_limit='64MB';" + sql,
                            text=True, capture_output=True, timeout=180,
                            env=dict(os.environ, DIFFERENTIAL_STORE_ROOT=str(base / repository),
                                     DIFFERENTIAL_PROBE_TRACE=str(trace)))
    assert result.returncode == 0, (result.stdout, result.stderr)
    blocks, text, decoder = [], result.stdout.strip(), json.JSONDecoder()
    while text:
        value, end = decoder.raw_decode(text)
        blocks.append(value)
        text = text[end:].lstrip()
    sizes = Counter()
    for line in trace.read_text().splitlines():
        op, path, length, offset = line.split('\t')
        if op == 'pread' and path.startswith('diff://'):
            sizes[int(length)] += 1
    return blocks, sizes


columns = ','.join(f'sin((i + {j})::DOUBLE) AS c{j}' for j in range(16))
rows, _ = run('create', 'writer', f"""ATTACH 'differential:wide' AS d
    (DATA_PATH '{location}', {options}, ROW_GROUP_SIZE 16384);
    CREATE TABLE d.t AS SELECT {columns} FROM range(131072) t(i);
    SELECT export_locator FROM differential_create_snapshot('d');""")
locator = rows[-1][0]['export_locator']
query = 'SELECT ' + ','.join(f'sum(c{j}) AS c{j}' for j in range(16)) + ' FROM r.t;'
report = {}
expected = None
for kind in ('remote', 'local'):
    report[kind] = {}
    for mode in ('NEVER', 'REMOTE_ONLY', 'ALWAYS_PREFETCH'):
        attach = (f"ATTACH 'differential:{location}{locator}' AS r ({options});" if kind == 'remote' else
                  "ATTACH 'differential:wide' AS r (SNAPSHOT_VERSION 1);")
        rows, sizes = run(kind + '-' + mode, kind + '-' + mode if kind == 'remote' else 'writer',
                          f"SET storage_block_prefetch='{mode}';" + attach + query)
        if expected is None:
            expected = rows
        assert rows == expected, (kind, mode, rows, expected)
        report[kind][mode] = dict(sizes)
    never, remote, always = (report[kind][mode] for mode in ('NEVER', 'REMOTE_ONLY', 'ALWAYS_PREFETCH'))
    assert max(never) == 262144 and max(always) > 262144, report[kind]
    assert remote == (always if kind == 'remote' else never), report[kind]
print('PASS REMOTE_ONLY batches native reads for S3 exports; local snapshots retain local prefetch behavior')
print('PASS NEVER, REMOTE_ONLY and ALWAYS_PREFETCH produce identical scan results in fresh processes')
evidence_path('s3-prefetch.json').write_text(json.dumps(report, indent=2) + '\n')
