#!/usr/bin/env python3
"""Continuous WAL shipping (SHIP_WAL) and followers (FOLLOW) against a disposable MinIO bucket.

A writer that dies without detaching is recovered on another repository: in sync mode every
acknowledged commit is present, in async mode every commit shipped before the death. Covers large
optimistic inserts, chain rotation, failover with the same identity, a copied repository (split brain),
remote collection of superseded chains, commit-level and snapshot-level followers and their request
counts, and option validation. Requires the bucket initialized by minio.py."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
from evidence import evidence_path
from paths import DUCKDB_CLI, EXTENSION, ROOT
from stock_capi import Host

endpoint, base = sys.argv[1:]
base = Path(base) / 'shipping'
base.mkdir()
LOCATION = 's3://differential-sdk-test/shipping-test/'
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
                       env=dict(os.environ, DIFFERENTIAL_STORE_ROOT=str(repository)), timeout=300)
    assert r.returncode == expected, (sql, r.stdout, r.stderr)
    if expected:
        return r.stderr
    out, text, decoder = [], r.stdout.strip(), json.JSONDecoder()
    while text:
        value, end = decoder.raw_decode(text)
        out.append(value)
        text = text[end:].lstrip()
    return out


def writer(repository, statements, kill=True):
    """Statements in one process; with kill the process ends without detaching (like SIGKILL)."""
    r = subprocess.run([sys.executable, str(ROOT / 'test/integration/shipping_writer.py'), str(repository),
                        '1' if kill else '0', *statements], text=True, capture_output=True, timeout=300,
                       cwd=ROOT / 'test/integration')
    lines = [json.loads(line) for line in r.stdout.splitlines() if line.startswith('{')]
    return r.returncode, lines, r.stderr


def recover(name, head, repository, extra=''):
    rows = run(repository, f"SELECT database_id, database_name, shipped_chain, shipped_epoch FROM differential_recover_branch("
                           f"'{LOCATION}heads/{head}', '{name}', {S3_ARGS}{extra});")
    return rows[-1][0]


def count(repository, database, table='t'):
    return run(repository, f"ATTACH 'differential:{database}' AS r; SELECT count(*) AS n FROM r.{table};")[-1][0]['n']


def stat(repository, metric):
    rows = run(repository, f"SELECT value FROM differential_storage_stats() WHERE metric = '{metric}';")
    return rows[-1][0]['value'] if rows and rows[-1] else 0


# Options: SHIP_WAL needs a single-writer S3 database; FOLLOW needs an S3 branch path.
local = base / 'local'
assert 'S3 DATA_PATH' in run(local, "ATTACH 'differential:plain' AS p (SHIP_WAL 'async');", 1)
assert 'FOLLOW applies' in run(local, "ATTACH 'differential:plain' AS p (FOLLOW true);", 1)
assert 'SHIP_INTERVAL needs' in run(local, f"ATTACH 'differential:x' AS x (DATA_PATH '{LOCATION}', {S3}, SHIP_INTERVAL 1);", 1)
assert 'between 0 and 3600' in run(local, f"ATTACH 'differential:y' AS y (DATA_PATH '{LOCATION}', {S3}, SHIP_WAL 'async', SHIP_INTERVAL -1);", 1)
assert 'FOLLOW_INTERVAL' in run(local, f"ATTACH 'differential:{LOCATION}heads/x' AS f ({S3}, FOLLOW true, FOLLOW_INTERVAL 0.5);", 1)
passed('option validation: SHIP_WAL needs an S3 single-writer database, FOLLOW an S3 branch path')

# Shipping turned on for an existing database (no DATA_PATH at that ATTACH): the process exits cleanly
# (regression: the shipping worker must stop before the S3 runtime is torn down at exit).
late = base / 'late'
run(late, f"ATTACH 'differential:late' AS l (DATA_PATH '{LOCATION}', {S3}); CREATE TABLE l.t AS SELECT range AS i "
          "FROM range(100000); CALL differential_create_snapshot('l');")
run(late, "ATTACH 'differential:late' AS l (SHIP_WAL 'async', SHIP_INTERVAL 0); CREATE TABLE l.u AS SELECT 1 AS v; "
          "CHECKPOINT l; INSERT INTO l.u VALUES (2); DETACH l;")
assert count(base / 'recover-late', recover('late_r', 'late/main', base / 'recover-late')['database_name'], 'u') == 2
passed('shipping turned on later for an exported database ships, and the process exits cleanly')

# sync: every acknowledged commit survives the writer's death.
w1 = base / 'writer-sync'
attach = f"ATTACH 'differential:orders' AS o (DATA_PATH '{LOCATION}', {S3}, SHIP_WAL 'sync')"
code, lines, err = writer(w1, [attach, "CREATE TABLE o.t (i INTEGER, s VARCHAR)"] +
                         [f"INSERT INTO o.t VALUES ({i}, repeat('x', {i * 7}))" for i in range(50)])
assert code == 0 and all('ok' in line for line in lines), (lines, err)
branches = run(w1, "SELECT ship_wal, shipped_chain, shipped_epoch, ship_pending FROM differential_branches('orders');")[-1]
assert branches[0]['ship_wal'] == 'sync' and branches[0]['shipped_chain'] >= 1, branches
recovered = recover('orders_r', 'orders/main', base / 'recover-sync')
assert count(base / 'recover-sync', 'orders_r') == 50
rows = run(base / 'recover-sync', "ATTACH 'differential:orders_r' AS r; SELECT i, length(s) AS n FROM r.t ORDER BY i;")[-1]
assert [(r['i'], r['n']) for r in rows] == [(i, i * 7) for i in range(50)], rows
passed('sync: a writer killed after 50 acknowledged commits is recovered with all 50 on another repository')

# The setting is stored: a later attachment without the option ships too.
code, lines, err = writer(w1, ["ATTACH 'differential:orders' AS o", "INSERT INTO o.t VALUES (1000, 'later')"])
assert code == 0, (lines, err)
assert count(base / 'recover-sync2', recover('orders_r2', 'orders/main', base / 'recover-sync2')['database_id']) == 51
passed('SHIP_WAL is stored per database: later attachments ship without the option')

# async: commits are shipped within the interval; a commit is not delayed by S3.
w2 = base / 'writer-async'
attach = f"ATTACH 'differential:events' AS e (DATA_PATH '{LOCATION}', {S3}, SHIP_WAL 'async', SHIP_INTERVAL 0.2)"
code, lines, err = writer(w2, [attach, "CREATE TABLE e.t (i INTEGER)"] + [f"INSERT INTO e.t VALUES ({i})" for i in range(30)],
                         kill=False)
assert code == 0, (lines, err)
time.sleep(1)
assert count(base / 'recover-async', recover('events_r', 'events/main', base / 'recover-async')['database_name']) == 30
passed('async: commits shipped in the background are recovered after the writer closed')
code, lines, err = writer(w2, ["ATTACH 'differential:events' AS e"] + [f"INSERT INTO e.t VALUES ({50 + i})" for i in range(10)] +
                         ["!sleep 2"])
assert code == 0, (lines, err)
assert count(base / 'recover-async-kill', recover('events_k', 'events/main', base / 'recover-async-kill')['database_name']) == 40
code, lines, err = writer(w2, ["ATTACH 'differential:events' AS e", "DELETE FROM e.t WHERE i >= 50"], kill=False)
assert code == 0, (lines, err)
passed('async: a writer killed after its shipping interval elapsed loses nothing')

# Large optimistic inserts (row groups written to the main file, referenced from the WAL) and a
# chain rotation: more than 64 MiB of epoch data starts a new chain from a fresh base.
w3 = base / 'writer-bulk'
attach = f"ATTACH 'differential:bulk' AS b (DATA_PATH '{LOCATION}', {S3}, SHIP_WAL 'sync')"
code, lines, err = writer(w3, [attach, "SET checkpoint_threshold = '10GB'",
                              "CREATE TABLE b.t AS SELECT range AS i, md5(range::VARCHAR) || md5((range*3)::VARCHAR) AS s FROM range(1500000)",
                              "CHECKPOINT b", "INSERT INTO b.t VALUES (-1, 'last')"])
assert code == 0 and all('ok' in line for line in lines), (lines, err)
chains = run(w3, "SELECT shipped_chain FROM differential_branches('bulk');")[-1][0]['shipped_chain']
assert chains >= 2, chains
recovered = recover('bulk_r', 'bulk/main', base / 'recover-bulk')
assert count(base / 'recover-bulk', 'bulk_r') == 1500001
passed(f'optimistic bulk insert recovered (1.5M rows); a completed checkpoint starts a new chain (chain {chains})')

# Failover: the recovered branch keeps the original identity and continues shipping in a newer chain.
failover = base / 'failover'
recovered = recover('orders', 'orders/main', failover, ', same_identity => true')
# Shipping settings belong to a repository: the new host turns shipping on again.
code, lines, err = writer(failover, ["ATTACH 'differential:orders' AS o (SHIP_WAL 'sync')",
                                     "INSERT INTO o.t VALUES (2000, 'after failover')"])
assert code == 0, (lines, err)
state = run(failover, "SELECT shipped_chain FROM differential_branches('orders');")[-1][0]
after_failover = count(base / 'recover-after-failover', recover('orders_f', 'orders/main', base / 'recover-after-failover')['database_name'])
assert after_failover == 52, after_failover
passed(f"failover: same_identity recovery continues shipping the branch in chain {state['shipped_chain']}")

# A copy of a shipping repository is another writer of the same identity: its epochs conflict.
copy = base / 'copy'
shutil.copytree(failover, copy)
code, lines, err = writer(failover, ["ATTACH 'differential:orders' AS o", "INSERT INTO o.t VALUES (3000, 'original')"])
assert code == 0, (lines, err)
code, lines, err = writer(copy, ["ATTACH 'differential:orders' AS o", "INSERT INTO o.t VALUES (3001, 'copy')"])
assert code != 0 and 'head_conflict' in json.dumps(lines), (code, lines, err)
passed('a copied repository that ships the same branch gets head_conflict (sync commit reports it)')

# Followers: commit level (the branch ships) and snapshot level (it only exports).
host = Host()
conn = host.connect()
host.query(conn, f"LOAD '{EXTENSION}'")
host.query(conn, f"SET differential_repository = '{base / 'follower'}'")
host.query(conn, f"ATTACH 'differential:{LOCATION}heads/events/main' AS live ({S3}, FOLLOW true, FOLLOW_INTERVAL 1)")
assert host.query(conn, "SELECT count(*) FROM live.t") == 30
code, lines, err = writer(w2, ["ATTACH 'differential:events' AS e"] + [f"INSERT INTO e.t VALUES ({100 + i})" for i in range(5)],
                         kill=False)
assert code == 0, (lines, err)
deadline = time.time() + 20
while host.query(conn, "SELECT count(*) FROM live.t") != 35 and time.time() < deadline:
    time.sleep(0.25)
assert host.query(conn, "SELECT count(*) FROM live.t") == 35
assert host.query(conn, "SELECT count(*) FROM differential_followers() WHERE follows = 'commits' AND changes >= 1") == 1
# Idle: about one request per interval.
before = host.query(conn, "SELECT value FROM differential_storage_stats() WHERE metric = 's3_get_calls'") + \
    host.query(conn, "SELECT value FROM differential_storage_stats() WHERE metric = 's3_head_calls'")
time.sleep(5)
after = host.query(conn, "SELECT value FROM differential_storage_stats() WHERE metric = 's3_get_calls'") + \
    host.query(conn, "SELECT value FROM differential_storage_stats() WHERE metric = 's3_head_calls'")
assert after - before <= 8, (before, after)
passed(f'commit-level follower sees new commits within its interval; idle polling cost {after - before} requests in 5 s')
# One transaction reads one state.
host.query(conn, "BEGIN")
first = host.query(conn, "SELECT count(*) FROM live.t")
code, lines, err = writer(w2, ["ATTACH 'differential:events' AS e", "INSERT INTO e.t VALUES (999)"], kill=False)
time.sleep(3)
assert host.query(conn, "SELECT count(*) FROM live.t") == first
host.query(conn, "COMMIT")
deadline = time.time() + 20
while host.query(conn, "SELECT count(*) FROM live.t") != first + 1 and time.time() < deadline:
    time.sleep(0.25)
assert host.query(conn, "SELECT count(*) FROM live.t") == first + 1
passed('a follower transaction keeps one state; the next transaction reads the newer one')
# Snapshot level.
w4 = base / 'writer-snapshots'
run(w4, f"ATTACH 'differential:reports' AS r (DATA_PATH '{LOCATION}', {S3}); CREATE TABLE r.t AS SELECT 1 AS v; "
        "CALL differential_create_snapshot('r');")
host.query(conn, f"ATTACH 'differential:{LOCATION}heads/reports' AS rep ({S3}, FOLLOW true, FOLLOW_INTERVAL 1)")
assert host.query(conn, "SELECT max(v) FROM rep.t") == 1
run(w4, "ATTACH 'differential:reports' AS r; UPDATE r.t SET v = 2; CALL differential_create_snapshot('r');")
deadline = time.time() + 20
while host.query(conn, "SELECT max(v) FROM rep.t") != 2 and time.time() < deadline:
    time.sleep(0.25)
assert host.query(conn, "SELECT max(v) FROM rep.t") == 2
host.query(conn, "DETACH live; DETACH rep")
assert host.query(conn, "SELECT count(*) FROM differential_followers()") == 0
host.close()
passed('snapshot-level follower moves to new exported snapshots; DETACH stops followers')

# Remote collection keeps every branch's newest chain (and its base) and collects superseded chains.
lister = base / 'gc'
collect = (f"SELECT condemned_objects, deleted_objects FROM differential_cleanup_remote_files('{LOCATION}', "
           f"{S3_ARGS}, older_than => TIMESTAMPTZ '2999-01-01');")
run(lister, collect)
second = run(lister, collect)[-1][0]
assert second['deleted_objects'] >= 1, second  # superseded chains (and their bases) went
assert count(base / 'recover-after-gc', recover('bulk_g', 'bulk/main', base / 'recover-after-gc')['database_name']) == 1500001
assert count(base / 'recover-after-gc2', recover('orders_g', 'orders/main', base / 'recover-after-gc2')['database_name']) >= 53
passed('remote collection keeps the newest chains: recovery after collecting superseded chains is complete')

evidence_path('s3-shipping.json').write_text(json.dumps({'passed': results}, indent=2) + '\n')
