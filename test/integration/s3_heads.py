#!/usr/bin/env python3
"""S3 branch heads: remote readers attach "the latest exported snapshot of <database>/<branch>"
from S3 alone. Covers publication on export, branches, deferred and out-of-order exports,
restore, expiry, repair through differential_retry_export, two databases with one name in one
prefix, listing and errors. Requires the disposable MinIO bucket initialized by minio.py."""
import json
import os
import shutil
from pathlib import Path
import subprocess
import sys
from evidence import evidence_path
from paths import DUCKDB_CLI, EXTENSION

endpoint, base = sys.argv[1:]
base = Path(base) / 'heads'
base.mkdir()
LOCATION = 's3://differential-sdk-test/heads-test/'
S3 = f"ENDPOINT '{endpoint}', PATH_STYLE true, ALLOW_HTTP true"
S3_ARGS = f"endpoint => '{endpoint}', path_style => true, allow_http => true"
results = []


def passed(name):
    results.append(name)
    print('PASS', name, flush=True)


def run(repository, sql, expected=0):
    setup = (f"LOAD '{EXTENSION}'; SET extension_directories=['{base / 'empty-extensions'}'];"
             "SET autoinstall_known_extensions=false; SET autoload_known_extensions=false;")
    r = subprocess.run([DUCKDB_CLI, '-init', '/dev/null', '-unsigned', '-batch', '-bail', '-json'],
                       input=setup + sql, text=True, capture_output=True,
                       env=dict(os.environ, DIFFERENTIAL_STORE_ROOT=str(repository)), timeout=180)
    assert r.returncode == expected, (sql, r.stdout, r.stderr)
    if expected:
        return r.stderr
    out, text, decoder = [], r.stdout.strip(), json.JSONDecoder()
    while text:
        value, end = decoder.raw_decode(text)
        out.append(value)
        text = text[end:].lstrip()
    return out


writer = base / 'writer'
W = f"ATTACH 'differential:sales' AS sales (DATA_PATH '{LOCATION}', {S3});"


def read(head, reader='reader'):
    """A fresh repository (no writer metadata) attaches a head and returns v."""
    rows = run(base / reader, f"ATTACH 'differential:{LOCATION}heads/{head}' AS r ({S3}); SELECT v FROM r.t;")
    return rows[-1][0]['v']


def heads():
    rows = run(base / 'lister', f"SELECT database_name, branch_name, snapshot_id, head FROM differential_remote_heads('{LOCATION}', {S3_ARGS}) ORDER BY ALL;")
    return [(r['database_name'], r['branch_name'], r['snapshot_id']) for r in rows[-1]]


run(writer, W + "CREATE TABLE sales.t AS SELECT 1 AS v; CALL differential_create_snapshot('sales');")
assert read('sales/main') == 1 and read('sales') == 1
assert run(base / 'reader', f"ATTACH 'differential:{LOCATION}heads/sales' AS r ({S3}); "
           "SELECT loaded FROM duckdb_extensions() WHERE extension_name='httpfs';")[-1] == [{'loaded': False}]
run(writer, W + "UPDATE sales.t SET v = 2; CALL differential_create_snapshot('sales');")
assert read('sales/main', 'reader2') == 2 and heads() == [('sales', 'main', 2)]
passed('exports publish heads/<database>/<uuid>/<branch>; fresh readers attach the latest by name')

run(writer, "CALL differential_create_branch('sales', 'dev');")  # starts at exported snapshot 2
assert ('sales', 'dev', 2) in heads() and read('sales/dev', 'reader3') == 2
run(writer, "ATTACH 'differential:sales' AS dev (BRANCH 'dev'); UPDATE dev.t SET v = 3; CALL differential_create_snapshot('dev');")
assert heads() == [('sales', 'dev', 3), ('sales', 'main', 2)] and read('sales/dev', 'reader4') == 3
passed('a new branch publishes its head at once; heads move independently per branch')

run(writer, W + "UPDATE sales.t SET v = 4; CALL differential_create_snapshot('sales', export_mode => 'deferred');")
assert ('sales', 'main', 2) in heads()  # a pending export does not move the head
run(writer, W + "UPDATE sales.t SET v = 5; CALL differential_create_snapshot('sales');")
assert ('sales', 'main', 5) in heads()
run(writer, "CALL differential_retry_export('sales');")  # the older snapshot 4 completes last
assert ('sales', 'main', 5) in heads() and read('sales/main', 'reader5') == 5
passed('pending exports leave heads alone; a late older export never moves a head back')

restored = run(writer, "SELECT snapshot_id FROM differential_restore('sales', 1, branch => 'main');")[-1][0]['snapshot_id']
assert ('sales', 'main', restored) in heads() and read('sales/main', 'reader6') == 1
run(writer, f"CALL differential_expire_snapshots('sales', versions => [{restored}]);")
assert ('sales', 'main', restored) not in heads() and read('sales/main', 'reader7') == 5
passed('restore moves the head to the restored snapshot; expiry moves it to the latest retained export')

other = base / 'other-writer'
run(other, f"ATTACH 'differential:sales' AS sales (DATA_PATH '{LOCATION}', {S3}); CREATE TABLE sales.t AS SELECT 42 AS v; CALL differential_create_snapshot('sales');")
assert 'Several databases named sales' in run(base / 'reader8', f"ATTACH 'differential:{LOCATION}heads/sales/main' AS r ({S3});", 1)
listing = run(base / 'lister', f"SELECT database_id, snapshot_id FROM differential_remote_heads('{LOCATION}', {S3_ARGS}) WHERE branch_name='main' ORDER BY snapshot_id;")[-1]
ids = {row['database_id'] for row in listing}
assert len(ids) == 2
for row in listing:
    value = run(base / 'reader9', f"ATTACH 'differential:{LOCATION}heads/sales/{row['database_id']}/main' AS r ({S3}); SELECT v FROM r.t;")[-1][0]['v']
    assert value == (42 if row['snapshot_id'] == 1 else 5), (row, value)
passed('two databases with one name in one prefix never overwrite each other; the UUID form disambiguates')

assert 'not_found' in run(base / 'reader10', f"ATTACH 'differential:{LOCATION}heads/absent/main' AS r ({S3});", 1)
assert 'not_found' in run(base / 'reader10', f"ATTACH 'differential:{LOCATION}heads/sales/{sorted(ids)[0]}/nobranch' AS r ({S3});", 1)
run(base / 'reader10', f"ATTACH 'differential:{LOCATION}heads/sales/{sorted(ids)[0]}/main' AS r ({S3}); INSERT INTO r.t VALUES (1);", 1)
passed('missing heads are not_found; head attachments are read-only')
# Guarded heads: a copy of the writer's repository (same identities) cannot overwrite the heads
# the original wrote; it gets head_conflict until it claims them, and then the original does.
writer_db = next(row['database_id'] for row in listing if row['snapshot_id'] != 1)
copy = base / 'writer-copy'
shutil.copytree(writer, copy)
run(writer, W + "UPDATE sales.t SET v = 6; CALL differential_create_snapshot('sales');")
assert read(f'sales/{writer_db}/main', 'reader11') == 6
error = run(copy, W + "UPDATE sales.t SET v = 60; CALL differential_create_snapshot('sales');", 1)
assert 'head_conflict' in error and "differential_claim_branch_heads('sales')" in error, error
assert read(f'sales/{writer_db}/main', 'reader12') == 6  # the original's head is untouched
claimed = run(copy, "SELECT branch_name, head FROM differential_claim_branch_heads('sales') ORDER BY 1;")[-1]
assert [row['branch_name'] for row in claimed] == ['dev', 'main'], claimed
assert claimed[1]['head'] == f'{LOCATION}heads/sales/{writer_db}/main', claimed
# The copy's conflicting snapshot number was taken: that export is abandoned (the snapshot stays
# local); new snapshots are numbered above the newest published one and publish normally.
states = run(copy, "SELECT export_state, count(*) AS n FROM differential_snapshots('sales') GROUP BY ALL ORDER BY 1;")[-1]
assert {'export_state': 'abandoned', 'n': 1} in states, states
run(copy, W + "CALL differential_create_snapshot('sales');")
assert read(f'sales/{writer_db}/main', 'reader13') == 60
error = run(writer, W + "UPDATE sales.t SET v = 7; CALL differential_create_snapshot('sales');", 1)
assert 'head_conflict' in error, error
assert read(f'sales/{writer_db}/main', 'reader14') == 60
passed('a copied repository cannot overwrite heads it did not write (head_conflict) until it claims them')
# Dropped branches: their heads become tombstones, also when the drop's export was deferred (then
# differential_retry_export publishes it), and the name can be used again.
dropper = base / 'drop-writer'
D = f"ATTACH 'differential:drops' AS drops (DATA_PATH '{LOCATION}', {S3});"
run(dropper, D + "CREATE TABLE drops.t AS SELECT 1 AS v; CALL differential_create_snapshot('drops');")
for name, value in (('dev', 2), ('tmp', 3)):
    run(dropper, f"CALL differential_create_branch('drops', '{name}');")
    run(dropper, f"ATTACH 'differential:drops' AS b (BRANCH '{name}'); UPDATE b.t SET v = {value}; "
                 "CALL differential_create_snapshot('b');")
    assert read(f'drops/{name}', f'reader-drop-{name}') == value
dropped_heads = lambda: [row['branch_name'] for row in run(base / 'lister', "SELECT branch_name FROM "
                         f"differential_remote_heads('{LOCATION}', {S3_ARGS}) WHERE database_name = 'drops' ORDER BY 1;")[-1]]
run(dropper, "CALL differential_drop_branch('drops', 'dev');")
assert 'not_found' in run(base / 'reader-drop-gone', f"ATTACH 'differential:{LOCATION}heads/drops/dev' AS r ({S3});", 1)
run(dropper, "CALL differential_drop_branch('drops', 'tmp', export_mode => 'deferred');")
assert 'tmp' in dropped_heads()  # not published yet
run(dropper, "CALL differential_retry_export('drops');")
assert dropped_heads() == ['main']
run(dropper, "CALL differential_create_branch('drops', 'dev');")  # the name again, from main's snapshot
assert dropped_heads() == ['dev', 'main'] and read('drops/dev', 'reader-drop-again') == 1
passed('dropped branches leave tombstones (also after a deferred drop, through retry_export); names are reusable')
evidence_path('s3-heads.json').write_text(json.dumps({'scope': 'disposable pinned MinIO; no real AWS', 'passed': results}, indent=2) + '\n')
