#!/usr/bin/env python3
"""Caching of S3 reads: DuckDB's external file cache as the memory layer (bounded by
memory_limit, reused across re-attachment, off with enable_external_file_cache) and the
repository disk cache (differential_remote_cache_size, off by default; shared by processes,
verified on read, size-limited). Needs the disposable MinIO bucket initialized by minio.py."""
import json
import os
from pathlib import Path
import stat
import subprocess
import sys
from evidence import evidence_path
from paths import DUCKDB_CLI, EXTENSION

endpoint, base = sys.argv[1:]
base = Path(base) / 'cache'
base.mkdir()
LOCATION = 's3://differential-sdk-test/cache-test/'
S3 = f"ENDPOINT '{endpoint}', PATH_STYLE true, ALLOW_HTTP true"
STATS = ("SELECT metric, value FROM differential_storage_stats() WHERE metric IN ('s3_get_calls', "
         "'remote_disk_cache_hits', 'remote_disk_cache_writes', 'remote_disk_cache_evictions', "
         "'remote_disk_cache_corrupt_entries');")
CHECK = "SELECT count(*) AS n, sum(length(payload))::BIGINT AS bytes FROM r.t WHERE payload = sha256(id::VARCHAR);"
ROWS = 300000
results = []


def passed(name):
    results.append(name)
    print('PASS', name, flush=True)


def run(repository, sql, expected=0):
    r = subprocess.run([DUCKDB_CLI, '-init', '/dev/null', '-unsigned', '-batch', '-bail', '-json'],
                       input=f"LOAD '{EXTENSION}';" + sql, text=True, capture_output=True,
                       env=dict(os.environ, DIFFERENTIAL_STORE_ROOT=str(base / repository)), timeout=300)
    assert r.returncode == expected, (sql, r.stdout, r.stderr)
    if expected:
        return r.stderr
    out, text, decoder = [], r.stdout.strip(), json.JSONDecoder()
    while text:
        value, end = decoder.raw_decode(text)
        out.append(value)
        text = text[end:].lstrip()
    return out


def stats(block):
    return {row['metric']: row['value'] for row in block}


def scan_ok(block):
    assert block == [{'n': ROWS, 'bytes': ROWS * 64}], block


rows = run('writer', f"""
    ATTACH 'differential:big' AS big (DATA_PATH '{LOCATION}', {S3});
    CREATE TABLE big.t AS SELECT range AS id, sha256(range::VARCHAR) AS payload FROM range({ROWS});
    SELECT export_locator FROM differential_create_snapshot('big');""")
EXPORT = f"ATTACH 'differential:{LOCATION}{rows[-1][0]['export_locator']}' AS r ({S3});"

# Memory layer: DuckDB's external file cache, reused after DETACH/ATTACH in the same instance.
rows = run('reader', EXPORT + CHECK + STATS + "DETACH r;" + EXPORT + CHECK + STATS + """
    SELECT count(*) > 0 AS listed, bool_and(path LIKE 'differential:s3://differential-sdk-test/cache-test/%') AS ours
    FROM duckdb_external_file_cache() WHERE path LIKE 'differential:%';""")
scan_ok(rows[0])
scan_ok(rows[2])
first, second = stats(rows[1]), stats(rows[3])
assert first['s3_get_calls'] > 3, first
assert second['s3_get_calls'] == first['s3_get_calls'], (first, second)  # descriptor and data cached
assert rows[4] == [{'listed': True, 'ours': True}], rows[4]
assert not (base / 'reader' / 'cache').exists()
passed("remote reads use DuckDB's external file cache: a re-attached export needs no GET at all")

rows = run('reader-nocache', "SET enable_external_file_cache = false;" + EXPORT + CHECK + STATS + "DETACH r;" + EXPORT
           + CHECK + STATS + "SELECT count(*) AS n FROM duckdb_external_file_cache() WHERE path LIKE 'differential:%';")
scan_ok(rows[0])
scan_ok(rows[2])
first, second = stats(rows[1]), stats(rows[3])
assert second['s3_get_calls'] - first['s3_get_calls'] > 3 and rows[-1] == [{'n': 0}], (first, second, rows[-1])
passed('SET enable_external_file_cache = false disables the memory layer')

rows = run('reader-small', "SET memory_limit = '32MB';" + EXPORT + CHECK + CHECK + """
    SELECT coalesce(sum(nr_bytes) FILTER (WHERE loaded), 0) <= 32 * 1024 * 1024 AS bounded
    FROM duckdb_external_file_cache();""")
scan_ok(rows[-3])
scan_ok(rows[-2])
assert rows[-1] == [{'bounded': True}], rows[-1]
passed('the memory layer stays within memory_limit; scans larger than memory stay correct')

# Read-ahead follows each scanning thread's sequential stream, requests every chunk at most once,
# and stays off when nothing would keep its data. DuckDB 2.0 reads remote storage ahead itself
# (read_ahead_depth): its larger, earlier reads leave ours little to do, so ours is checked with
# DuckDB's off, and the default with both for the total requests.
READAHEAD = ("SELECT metric, value FROM differential_storage_stats() WHERE metric IN ('remote_prefetched_chunks', "
             "'remote_cache_hits', 'remote_logical_read_bytes', 's3_get_calls');")
for threads in (1, 4):
    for native in (False, True):
        setup = f"SET threads = {threads};" + ("" if native else "SET read_ahead_depth = 0;")
        rows = run(f'readahead-{threads}-{native}', setup + EXPORT + CHECK + READAHEAD)
        scan_ok(rows[-2])
        m = stats(rows[-1])
        chunks = m['remote_logical_read_bytes'] // 262144 + 1
        if not native:
            assert m['remote_prefetched_chunks'] > chunks // 2, (threads, m)   # the scan is mostly read ahead
        assert m['remote_prefetched_chunks'] <= chunks + 16, (threads, m)      # ... and nothing twice
        # At most one GET per 256 KiB chunk read (ranges are coalesced up to 1 MiB, read-ahead to 4 MiB).
        assert m['remote_cache_hits'] <= 2 * chunks and m['s3_get_calls'] < chunks, (threads, native, m)
rows = run('readahead-off', "SET enable_external_file_cache = false;" + EXPORT + CHECK + READAHEAD)
scan_ok(rows[-2])
assert stats(rows[-1])['remote_prefetched_chunks'] == 0, rows[-1]
passed('read-ahead follows each thread, requests each chunk once, and is off when nothing keeps the data')

# Process-wide caches never cross repositories: a second DuckDB instance with its own repository
# fetches the descriptor itself (with its own S3 access) instead of reusing the first one's.
from stock_capi import Host
counts = []
for name in ('tenant-a', 'tenant-b'):
    host = Host()
    c = host.connect()
    try:
        host.query(c, f"LOAD '{EXTENSION}'; SET differential_repository = '{base / name}';")
        before = host.query(c, "SELECT value FROM differential_storage_stats() WHERE metric = 's3_get_calls';")
        host.query(c, EXPORT)
        counts.append(host.query(c, "SELECT value FROM differential_storage_stats() WHERE metric = 's3_get_calls';") - before)
    finally:
        host.close()
assert counts[0] == counts[1] and counts[0] > 0, counts
passed('a DuckDB instance with another repository in the same process reads its own descriptor')

# Disk layer: off by default; when set, shared by later processes of the repository.
rows = run('disk', "SET differential_remote_cache_size = '256MB';" + EXPORT + CHECK + STATS)
scan_ok(rows[-2])
# Entries are written by a background writer; everything queued is on disk when the process ends.
cache = base / 'disk' / 'cache'
files = [p for p in cache.rglob('*') if p.is_file() and not p.name.startswith('.')]
assert files and all(len(p.name) == 64 for p in files), files[:3]
assert all(stat.S_IMODE(p.stat().st_mode) == 0o600 for p in files)
assert all(stat.S_IMODE(p.stat().st_mode) == 0o700 for p in [cache, *cache.iterdir()] if p.is_dir())
rows = run('disk', "SET differential_remote_cache_size = '256MB';" + EXPORT + CHECK + STATS)
scan_ok(rows[-2])
warm = stats(rows[-1])
assert warm['s3_get_calls'] == 0 and warm['remote_disk_cache_hits'] > 0, warm
rows = run('disk', EXPORT + CHECK + STATS)  # setting off: the cache is neither read nor written
assert stats(rows[-1])['s3_get_calls'] > 3 and stats(rows[-1])['remote_disk_cache_hits'] == 0, rows[-1]
passed('the disk cache (private modes) lets a new process read an export without any GET; off by default')

# Damaged entries are detected, dropped and refetched.
victim = files[0]
victim.write_bytes(b'\0' * victim.stat().st_size)
rows = run('disk', "SET differential_remote_cache_size = '256MB';" + EXPORT + CHECK + STATS)
scan_ok(rows[-2])
assert stats(rows[-1])['remote_disk_cache_corrupt_entries'] >= 1, rows[-1]
passed('damaged disk cache entries fail verification and are refetched; results stay correct')

# The size limit evicts least-recently-used entries.
rows = run('disk-small', "SET differential_remote_cache_size = '2MB';" + EXPORT + CHECK + STATS)
scan_ok(rows[-2])
total = sum(p.stat().st_size for p in (base / 'disk-small' / 'cache').rglob('*') if p.is_file())
assert 0 < total <= 2.3 * 1024 * 1024, total  # the export is ~5x larger than the limit
passed('differential_remote_cache_size bounds the disk cache (LRU eviction)')

# Bulk copies read caches but never fill them.
rows = run('recover', f"SET differential_remote_cache_size = '256MB';" +
           f"CALL differential_recover_export('{LOCATION}{EXPORT.split(LOCATION)[1].split(chr(39))[0]}', 'copy', "
           f"endpoint => '{endpoint}', path_style => true, allow_http => true);" + STATS +
           "SELECT count(*) AS n FROM duckdb_external_file_cache() WHERE path LIKE 'differential:%';")
assert stats(rows[-2])['remote_disk_cache_writes'] == 0 and rows[-1] == [{'n': 0}], rows[-2:]
passed('writable recovery fills neither cache')

error = run('errors', "SET differential_remote_cache_size = 'lots';", 1)
assert 'Invalid' in error or 'memory' in error.lower() or 'parse' in error.lower(), error
passed('invalid cache sizes are refused at SET')
evidence_path('s3-cache.json').write_text(json.dumps({'scope': 'disposable pinned MinIO; no real AWS', 'passed': results}, indent=2) + '\n')
