#!/usr/bin/env python3
"""Remote garbage collection (differential_cleanup_remote_files) against the disposable MinIO bucket
initialized by minio.py. Covers the two phases (condemn, then delete after the grace cutoff),
dry runs, that live heads stay readable from fresh repositories, that an expired snapshot's export
is gone afterwards, that exports never reuse condemned packs (identical data written again after
condemnation stays readable after deletion), and that a shared writer whose unpublished branch reads
its base snapshot from S3 copies condemned data locally before it is deleted. Each repository
stands for one host; the grace cutoff is set in the future so the test needs no waiting."""
import datetime
import json
import os
from pathlib import Path
import subprocess
import sys
from evidence import evidence_path
from paths import DUCKDB_CLI, EXTENSION

endpoint, base = sys.argv[1:]
base = Path(base) / 'remote-gc'
base.mkdir()
LOCATION = 's3://differential-sdk-test/gc-test/'
S3 = f"ENDPOINT '{endpoint}', PATH_STYLE true, ALLOW_HTTP true"
S3_ARGS = f"endpoint => '{endpoint}', path_style => true, allow_http => true"
# Everything already written counts as older than the grace cutoff.
FUTURE = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=1)).strftime('%Y-%m-%d %H:%M:%S+00')
results = []


def passed(name):
    results.append(name)
    print('PASS', name, flush=True)


def run(repository, sql, expected=0):
    setup = (f"LOAD '{EXTENSION}'; SET extension_directories=['{base / 'empty-extensions'}'];"
             "SET autoinstall_known_extensions=false; SET autoload_known_extensions=false;")
    r = subprocess.run([DUCKDB_CLI, '-init', '/dev/null', '-unsigned', '-batch', '-bail', '-json'],
                       input=setup + sql, text=True, capture_output=True,
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


def collect(repository='collector', dry_run=False):
    return run(repository, f"SELECT * FROM differential_cleanup_remote_files('{LOCATION}', older_than => "
                           f"TIMESTAMPTZ '{FUTURE}', dry_run => {str(dry_run).lower()}, {S3_ARGS});")[-1][0]


def total(repository, attach):
    # Every column, so every pack that holds the table is read.
    return run(repository, attach + "SELECT sum(v)::BIGINT AS s, count(*) AS n, sum(length(pad))::BIGINT AS p "
                                    "FROM r.t;")[-1][0]


# DuckDB keeps freed blocks in its file: replacing a table with as much other data overwrites them,
# so snapshot 1's data is then reachable only through snapshot 1.
DATA = "SELECT range AS v, repeat(md5(range::VARCHAR), 8) AS pad FROM range(200000)"
OTHER = "SELECT range + 1000000 AS v, repeat(md5((range + 1000000)::VARCHAR), 8) AS pad FROM range(200000)"
OTHER_TOTAL = {'s': sum(range(1000000, 1200000)), 'n': 200000, 'p': 200000 * 256}
W = f"ATTACH 'differential:sales' AS sales (DATA_PATH '{LOCATION}', {S3});"
run('writer', W + f"CREATE TABLE sales.t AS {DATA}; CALL differential_create_snapshot('sales');")
first = run('writer', "SELECT export_locator FROM differential_snapshots('sales') WHERE snapshot_id = 1;")[-1][0]
run('writer', W + f"DROP TABLE sales.t; CHECKPOINT sales; CREATE TABLE sales.t AS {OTHER}; CHECKPOINT sales; "
                  "CALL differential_create_snapshot('sales');")
run('writer', "CALL differential_expire_snapshots('sales', versions => [1]);")
HEAD = f"ATTACH 'differential:{LOCATION}heads/sales' AS r ({S3});"
assert total('reader-1', HEAD) == OTHER_TOTAL

dry = collect(dry_run=True)
assert dry['unreachable_objects'] > 0 and dry['condemned_objects'] == dry['unreachable_objects'] and \
    dry['deleted_objects'] == 0, dry
assert collect(dry_run=True) == dry
passed('a dry run counts the expired snapshot\'s data and changes nothing')

phase1 = collect()
assert phase1['condemned_objects'] == dry['condemned_objects'] and phase1['deleted_objects'] == 0, phase1
assert total('reader-2', HEAD) == OTHER_TOTAL
passed('phase 1 condemns unreachable data and deletes nothing')

# The same data again: its chunks were stored in now-condemned packs, which no export may reuse.
run('writer', W + f"DROP TABLE sales.t; CREATE TABLE sales.t AS {DATA}; CHECKPOINT sales; "
                  "CALL differential_create_snapshot('sales');")
phase2 = collect()
assert phase2['deleted_objects'] > 0 and phase2['deleted_bytes'] > 0, phase2
expected = {'s': sum(range(200000)), 'n': 200000, 'p': 200000 * 256}
assert total('reader-3', HEAD) == expected
passed('phase 2 deletes condemned data; data written again after condemnation was uploaded anew')

error = run('reader-4', f"ATTACH 'differential:{LOCATION}{first['export_locator']}' AS r ({S3}); "
                        "SELECT sum(length(pad)) FROM r.t;", expected=1)
assert 'differential:' in error, error
again = collect()
assert again['deleted_objects'] == 0 and total('reader-5', HEAD) == expected, again
passed('the expired export is gone; a repeated collection keeps every live object')

# A dropped branch: its head becomes a tombstone, so once its snapshots expire its data is collected.
SCRATCH = "SELECT range + 5000000 AS v, repeat(md5((range + 5000000)::VARCHAR), 8) AS pad FROM range(200000)"
run('writer', "CALL differential_create_branch('sales', 'scratch');")
run('writer', f"ATTACH 'differential:sales' AS b (BRANCH 'scratch'); DROP TABLE b.t; CHECKPOINT b; "
              f"CREATE TABLE b.t AS {SCRATCH}; CHECKPOINT b; CALL differential_create_snapshot('b');")
run('writer', "CALL differential_drop_branch('sales', 'scratch');")
versions = run('writer', "SELECT list(snapshot_id ORDER BY snapshot_id) AS ids FROM differential_snapshots('sales') "
                         "WHERE branch_id = (SELECT branch_id FROM differential_branches('sales') "
                         "WHERE branch_name = 'scratch');")[-1][0]['ids']
before = collect(dry_run=True)
run('writer', f"CALL differential_expire_snapshots('sales', versions => {versions});")
after = collect(dry_run=True)
assert after['unreachable_bytes'] > before['unreachable_bytes'] + 10 * 1024 * 1024, (before, after)
collect()
assert collect()['deleted_bytes'] > 10 * 1024 * 1024
assert total('reader-6', HEAD) == expected
passed('a dropped branch\'s data is collected once its head is a tombstone and its snapshots expired')

# Shared database: host B's unpublished branch reads its base snapshot from S3 on demand.
TEAM = f"ATTACH 'differential:team' AS team (WRITERS 'shared', DATA_PATH '{LOCATION}', {S3}, PUBLISH_ON_CHECKPOINT false);"
run('host-a', TEAM + f"CREATE TABLE team.t AS {DATA}; CALL differential_create_snapshot('team');")
run('host-b', TEAM + "CREATE TABLE team.local AS SELECT 1 AS x;")  # base 1, unpublished change
run('host-a', TEAM + f"DROP TABLE team.t; CHECKPOINT team; CREATE TABLE team.t AS {OTHER}; CHECKPOINT team; "
                     "CALL differential_create_snapshot('team');")
run('host-a', "CALL differential_expire_snapshots('team', versions => [1]);")
collect()  # condemns snapshot 1's data, which host B still reads
run('host-b', "SELECT count(*) FROM differential_snapshots('team');")  # learns the record: copies locally
assert collect()['deleted_objects'] > 0
rows = run('host-b', TEAM + "SELECT sum(v)::BIGINT AS s, count(*) AS n, sum(length(pad))::BIGINT AS p FROM team.t; "
                             "SELECT x FROM team.local;")
assert rows[-2] == [expected] and rows[-1] == [{'x': 1}], rows
passed('a shared writer copies condemned base data locally before collection deletes it')

# Warm collections read only branch heads: index pages, descriptors and commit records are cached.
def collect_requests():
    rows = run('collector', f"SELECT deleted_objects FROM differential_cleanup_remote_files('{LOCATION}', older_than => "
                            f"TIMESTAMPTZ '{FUTURE}', {S3_ARGS}); SELECT metric, value FROM differential_storage_stats() "
                            "WHERE metric IN ('s3_get_calls', 's3_verified_get_bytes');")
    return {r['metric']: int(r['value']) for r in rows[-1]}
first, second = collect_requests(), collect_requests()
assert first == second and second['s3_verified_get_bytes'] < 64 * 1024, (first, second)
passed('warm collections read only branch heads (references of immutable objects are cached)')
evidence_path('s3-remote-gc.json').write_text(json.dumps({'scope': 'disposable pinned MinIO; grace cutoff in the future',
                                                         'results': results}, indent=2) + '\n')
