#!/usr/bin/env python3
"""Shared databases (WRITERS 'shared'): several repositories write one database through S3.
Covers creating and joining (one identity derived from the name; the base snapshot is read on
demand, nothing is downloaded), optimistic head updates (a writer whose base moved gets
branch_conflict and keeps its exported snapshot), fast-forward of unchanged branches at ATTACH,
snapshot numbering through the commit index under concurrent writers (renumbering), a race of two
writers on one branch, name conflicts and readers. Each repository stands for one host. Needs the
disposable MinIO bucket initialized by minio.py and network access for httpfs.

Real AWS (run by the owner; never part of make test_minio):
    DIFFERENTIAL_AWS_LOCATION=s3://<bucket>/<prefix>/ [DIFFERENTIAL_AWS_REGION=eu-central-1] \
    [AWS_PROFILE=<profile>] PYTHONPATH=test/integration python3 test/integration/s3_shared.py aws <empty directory>
Each run writes under a new <prefix>/shared-<random>/ (nothing is deleted except writer markers;
remove the prefix afterwards). Credentials come from the AWS SDK chain or the profile; httpfs reads
them through the aws extension's credential_chain."""
import datetime
import json
import os
from pathlib import Path
import subprocess
import sys
import time
from urllib.parse import urlparse
from evidence import evidence_path
from paths import DUCKDB_CLI, EXTENSION

endpoint, base = sys.argv[1:]
base = Path(base) / 'shared'
base.mkdir()
AWS = endpoint == 'aws'
if AWS:
    import secrets
    root = os.environ['DIFFERENTIAL_AWS_LOCATION']
    assert root.startswith('s3://') and root.endswith('/'), 'DIFFERENTIAL_AWS_LOCATION must be s3://bucket/prefix/'
    LOCATION = f"{root}shared-{secrets.token_hex(6)}/"
    region = os.environ.get('DIFFERENTIAL_AWS_REGION', 'us-east-1')
    profile = os.environ.get('AWS_PROFILE', '')
    S3 = f"REGION '{region}'" + (f", CREDENTIAL_PROFILE '{profile}'" if profile else '')
    S3_ARGS = f"region => '{region}'" + (f", credential_profile => '{profile}'" if profile else '')
    print('Location:', LOCATION, flush=True)
else:
    LOCATION = 's3://differential-sdk-test/shared-test/'
    S3 = f"ENDPOINT '{endpoint}', PATH_STYLE true, ALLOW_HTTP true"
    S3_ARGS = f"endpoint => '{endpoint}', path_style => true, allow_http => true"
TEAM = f"ATTACH 'differential:team' AS team (WRITERS 'shared', DATA_PATH '{LOCATION}', {S3});"
# Changes that stay local: checkpoints of this attachment are not published.
LOCAL = f"ATTACH 'differential:team' AS team (WRITERS 'shared', DATA_PATH '{LOCATION}', {S3}, PUBLISH_ON_CHECKPOINT false);"
if AWS:
    HTTPFS = (f"SET extension_directory='{base / 'extensions'}'; LOAD httpfs; LOAD aws; CREATE SECRET (TYPE s3, "
              f"PROVIDER credential_chain, REGION '{region}'" + (f", PROFILE '{profile}'" if profile else '') + ");")
else:
    HTTPFS = (f"SET extension_directory='{base / 'extensions'}'; LOAD httpfs; CREATE SECRET (TYPE s3, KEY_ID "
              f"'{os.environ['AWS_ACCESS_KEY_ID']}', SECRET '{os.environ['AWS_SECRET_ACCESS_KEY']}', ENDPOINT "
              f"'{urlparse(endpoint).netloc}', URL_STYLE 'path', USE_SSL false, REGION 'us-east-1');")
results = []


def passed(name):
    results.append(name)
    print('PASS', name, flush=True)


def command(repository, sql, **env):
    return ([DUCKDB_CLI, '-init', '/dev/null', '-unsigned', '-batch', '-bail', '-json'],
            dict(input=f"LOAD '{EXTENSION}';" + sql, text=True, capture_output=True,
                 env=dict(os.environ, DIFFERENTIAL_STORE_ROOT=str(base / repository), **env), timeout=300))


def parse(stdout):
    out, text, decoder = [], stdout.strip(), json.JSONDecoder()
    while text:
        value, end = decoder.raw_decode(text)
        out.append(value)
        text = text[end:].lstrip()
    return out


def run(repository, sql, expected=0, with_stderr=False, **env):
    args, options = command(repository, sql, **env)
    r = subprocess.run(args, **options)
    assert r.returncode == expected, (repository, sql, r.stdout, r.stderr)
    if expected:
        return r.stderr
    return (parse(r.stdout), r.stderr) if with_stderr else parse(r.stdout)


def heads():
    rows = run('lister', f"SELECT database_name, branch_name, snapshot_id FROM differential_remote_heads('{LOCATION}', "
                         f"{S3_ARGS}) ORDER BY ALL;")
    return [(r['database_name'], r['branch_name'], r['snapshot_id']) for r in rows[-1]]


def stats(rows):
    return {row['metric']: row['value'] for row in rows}


STATS = ("SELECT metric, value FROM differential_storage_stats() WHERE metric IN ('local_payload_bytes', "
         "'local_remote_object_read_bytes', 's3_put_calls', 's3_get_calls');")

# --- Create and join -------------------------------------------------------------------------
rows = run('a', TEAM + """
    CREATE TABLE team.t AS SELECT range AS id, 'a' AS v, sha256(range::VARCHAR) AS payload FROM range(200000);
    SELECT snapshot_id, export_state FROM differential_create_snapshot('team');
    SELECT database_id, writers FROM differential_databases();""")
assert rows[-2] == [{'snapshot_id': 1, 'export_state': 'complete'}], rows
team_id = rows[-1][0]['database_id']
assert rows[-1][0]['writers'] == 'shared' and team_id[14] == '8', rows  # name-derived identity (UUID version 8)
assert heads() == [('team', 'main', 1)]
rows = run('b', TEAM + STATS + "SELECT count(*) AS n, sum(id)::BIGINT AS s, count(*) FILTER (v = 'a') AS a FROM team.t;" + STATS +
           "SELECT database_id FROM differential_databases(); SELECT snapshot_id, publication_state, export_state "
           "FROM differential_snapshots('team');")
before, after = stats(rows[0]), stats(rows[2])
assert rows[1] == [{'n': 200000, 's': 199999 * 200000 // 2, 'a': 200000}], rows[1]
assert before['local_payload_bytes'] == 0 and after['local_payload_bytes'] == 0, (before, after)
assert after['local_remote_object_read_bytes'] > 0 and after['s3_put_calls'] == 1, after  # the writer marker
assert rows[3] == [{'database_id': team_id}], rows[3]
assert rows[4] == [{'snapshot_id': 1, 'publication_state': 'local_evicted', 'export_state': 'complete'}], rows[4]
passed('a second repository joins the shared database by name (same identity) and reads its head on demand: '
       'no local payload, no data uploads')

# --- Read-ahead for a writer's base snapshot ---------------------------------------------------
SCAN_STATS = ("SELECT metric, value FROM differential_storage_stats() WHERE metric IN ('s3_get_calls', "
              "'local_remote_object_read_bytes', 'local_remote_object_ahead_bytes');")
SCAN = "SELECT sum(length(payload))::BIGINT AS n FROM team.t;"
scans = {}
for repository, setting in (('ahead', ''), ('no-ahead', 'SET enable_external_file_cache = false;')):
    rows = run(repository, setting + LOCAL + SCAN_STATS + SCAN + SCAN_STATS + SCAN + SCAN_STATS)
    first, second, third = stats(rows[0]), stats(rows[2]), stats(rows[4])
    assert rows[1] == rows[3] == [{'n': 200000 * 64}], rows
    scans[repository] = (second['s3_get_calls'] - first['s3_get_calls'], third['s3_get_calls'] - second['s3_get_calls'],
                         second['local_remote_object_ahead_bytes'] - first['local_remote_object_ahead_bytes'])
(gets, again, ahead), (plain_gets, plain_again, plain_ahead) = scans['ahead'], scans['no-ahead']
assert ahead > 0 and plain_ahead == 0, scans
assert gets * 2 <= plain_gets, scans  # read-ahead: fewer, larger range requests
assert again == 0, scans  # the second scan is served by DuckDB's external file cache
passed(f'a writer reading its base from S3 reads ahead: a cold scan takes {gets} GETs instead of {plain_gets}, '
       'and the next scan none (DuckDB external file cache); without a cache to keep it, no read-ahead')

# --- Optimistic head updates -----------------------------------------------------------------
run('a', LOCAL + "UPDATE team.t SET v = 'a2' WHERE id = 1;")  # local change, not yet a snapshot
rows = run('b', TEAM + "UPDATE team.t SET v = 'b' WHERE id < 10; SELECT snapshot_id FROM differential_create_snapshot('team');")
assert rows[-1] == [{'snapshot_id': 2}] and heads() == [('team', 'main', 2)], rows
error = run('a', TEAM + "SELECT * FROM differential_create_snapshot('team');", expected=1)
assert '[differential:branch_conflict]' in error and 'moved in S3 to snapshot 2' in error, error
assert 'last saw it at snapshot 1' in error and 'Snapshot 3 is exported' in error, error
assert heads() == [('team', 'main', 2)]
rows = run('a', "SELECT snapshot_id, publication_state, export_state FROM differential_snapshots('team') ORDER BY 1;")
assert rows[-1] == [{'snapshot_id': 1, 'publication_state': 'local_published', 'export_state': 'complete'},
                    {'snapshot_id': 2, 'publication_state': 'remote', 'export_state': 'complete'},  # b's
                    {'snapshot_id': 3, 'publication_state': 'local_published', 'export_state': 'complete'}], rows
passed("a writer whose base moved gets branch_conflict: its snapshot is renumbered above the other writer's (3), "
       'exported and kept, and the head stays')

BRANCHES = "SELECT branch_name, remote_state, remote_snapshot_id, kept_branch FROM differential_branches('team') " \
           "WHERE state = 'active' ORDER BY branch_name;"
error = run('a', TEAM + "SELECT * FROM differential_create_snapshot('team');", expected=1)
assert 'diverged from S3' in error and "differential_resolve_conflict('team'" in error, error
rows = run('a', BRANCHES + "SELECT max(snapshot_id) AS n FROM differential_snapshots('team');")
assert rows[0] == [{'branch_name': 'main', 'remote_state': 'diverged', 'remote_snapshot_id': 2, 'kept_branch': None}]
assert rows[1] == [{'n': 3}], rows
passed('a diverged branch publishes nothing more until it is reconciled; differential_branches reports it')

run('a', "CALL differential_create_branch('team', 'a-work', snapshot => 3);")
assert ('team', 'a-work', 3) in heads()
rows = run('reader', f"ATTACH 'differential:{LOCATION}heads/team/a-work' AS r ({S3}); SELECT v FROM r.t WHERE id = 1;")
assert rows[-1] == [{'v': 'a2'}], rows
error = run('a', "CALL differential_create_branch('team', 'a-work', snapshot => 1);", expected=1)
assert 'already exists' in error, error
error = run('c', TEAM + "CALL differential_create_branch('team', 'a-work', snapshot => 2);", expected=1)
assert '[differential:name_conflict]' in error and 'already exists in S3' in error, error
passed('the conflicting snapshot is kept as a new branch, whose name is then taken for every writer')

RESOLVE = "SELECT * FROM differential_resolve_conflict('team', keep => 'branch', request_id => 'resolve-a');"
run('a', LOCAL + "UPDATE team.t SET v = 'a3' WHERE id = 2;")  # not yet a snapshot
error = run('a', TEAM + RESOLVE, expected=1)
assert '[differential:active_attachment_or_transaction]' in error and 'DETACH' in error, error
rows = run('a', RESOLVE + RESOLVE + BRANCHES)
assert rows[0] == rows[1] and rows[0][0]['branch_name'] == 'main', rows
assert {k: rows[0][0][k] for k in ('remote_snapshot_id', 'kept_snapshot_id', 'kept_branch')} == \
    {'remote_snapshot_id': 2, 'kept_snapshot_id': 4, 'kept_branch': 'main-conflict-4'}, rows
assert {'branch_name': 'main', 'remote_state': 'in_sync', 'remote_snapshot_id': 2, 'kept_branch': None} in rows[2], rows
assert ('team', 'main-conflict-4', 4) in heads() and ('team', 'main', 2) in heads()
rows = run('reader3', f"ATTACH 'differential:{LOCATION}heads/team/main-conflict-4' AS r ({S3}); "
                      "SELECT id, v FROM r.t WHERE id IN (1, 2) ORDER BY id;")
assert rows[-1] == [{'id': 1, 'v': 'a2'}, {'id': 2, 'v': 'a3'}], rows
rows = run('a', TEAM + "SELECT count(*) FILTER (v = 'b') AS b, count(*) FILTER (v LIKE 'a_') AS mine FROM team.t;")
assert rows[-1] == [{'b': 10, 'mine': 0}], rows
passed("resolve keep => 'branch' preserves the unsnapshotted work, publishes it as main-conflict-4 and makes main "
       'its S3 head again; it needs the branch detached and replays by request_id')

# --- Fast-forward ----------------------------------------------------------------------------
rows = run('c', TEAM + "SELECT count(*) FILTER (v = 'b') AS b FROM team.t; UPDATE team.t SET v = 'c' WHERE id = 5; "
                       "SELECT snapshot_id FROM differential_create_snapshot('team');")
assert rows[0] == [{'b': 10}] and rows[1] == [{'snapshot_id': 5}], rows
rows = run('b', TEAM + "SELECT v FROM team.t WHERE id = 5; SELECT id FROM team.current_snapshot();")
assert rows == [[{'v': 'c'}], [{'id': 5}]], rows
passed("an unchanged branch moves to the S3 head at ATTACH (b sees c's snapshot 5)")

# --- Concurrent writers on different branches: numbering through the commit index -------------
run('b', "CALL differential_create_branch('team', 'b-dev', snapshot => 5);")
work = {'b': f"ATTACH 'differential:team' AS w (BRANCH 'b-dev');",
        'c': "ATTACH 'differential:team' AS w;"}
processes = []
for repository, attach in work.items():
    script = attach + ''.join(f"UPDATE w.t SET v = '{repository}{i}' WHERE id = {100 + i}; "
                              f"SELECT snapshot_id FROM differential_create_snapshot('w');" for i in range(5))
    args, options = command(repository, script)
    options.pop('timeout')
    processes.append(subprocess.Popen(args, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                      text=True, env=options['env']))
    processes[-1].stdin.write(options['input'])
    processes[-1].stdin.close()
numbers = []
for process in processes:
    stdout = process.stdout.read()
    stderr = process.stderr.read()
    assert process.wait(timeout=300) == 0, (stdout, stderr)
    ids = [row[0]['snapshot_id'] for row in parse(stdout)]
    assert ids == sorted(ids) and len(ids) == 5, ids
    numbers += ids
assert len(set(numbers)) == 10 and min(numbers) > 5, numbers
current = heads()
assert ('team', 'b-dev', max(n for n in numbers[:5])) in current and ('team', 'main', max(numbers[5:])) in current, current
run('inspect', f"SET extension_directory='{base / 'extensions'}'; INSTALL httpfs;" + (" INSTALL aws;" if AWS else ""))
keys = run('inspect', HTTPFS + f"SELECT file FROM glob('{LOCATION}dbs/{team_id}/snapshots/*');")[-1]
published = sorted(9999999999 - int(row['file'].rsplit('/', 1)[1]) for row in keys)
assert set(numbers) <= set(published) and published == sorted(set(published)), (numbers, published)
passed('two writers on different branches publish concurrently: distinct, increasing snapshot numbers, one '
       'commit record each')

# --- Two writers race on one branch ----------------------------------------------------------
for repository in ('b', 'c'):  # both start from main's head, with local changes
    run(repository, LOCAL + f"UPDATE team.t SET v = 'race-{repository}' WHERE id = 7;")
racers = []
for repository in ('b', 'c'):
    args, options = command(repository, TEAM + "SELECT snapshot_id FROM differential_create_snapshot('team');")
    racers.append((repository, subprocess.Popen(args, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                                stderr=subprocess.PIPE, text=True, env=options['env'])))
    racers[-1][1].stdin.write(options['input'])
    racers[-1][1].stdin.close()
outcomes = {}
for repository, process in racers:
    stdout, stderr = process.stdout.read(), process.stderr.read()
    outcomes[repository] = (process.wait(timeout=300), stdout, stderr)
winners = [r for r, o in outcomes.items() if o[0] == 0]
losers = [r for r, o in outcomes.items() if o[0] != 0]
assert len(winners) == 1 and len(losers) == 1, outcomes
assert '[differential:branch_conflict]' in outcomes[losers[0]][2], outcomes
rows = run('reader2', f"ATTACH 'differential:{LOCATION}heads/team' AS r ({S3}); SELECT v FROM r.t WHERE id = 7;")
assert rows[-1] == [{'v': f'race-{winners[0]}'}], (rows, winners)
passed('two writers updating one branch at once: exactly one moves the head, the other gets branch_conflict')

loser, winner = losers[0], winners[0]
rows = run(loser, "SELECT * FROM differential_resolve_conflict('team', keep => 'remote');" + BRANCHES + TEAM +
           "SELECT v FROM team.t WHERE id = 7;")
assert rows[0][0]['kept_branch'] is None and rows[0][0]['kept_snapshot_id'] > 0, rows
assert {'branch_name': 'main', 'remote_state': 'in_sync', 'remote_snapshot_id': rows[0][0]['remote_snapshot_id'],
        'kept_branch': None} in rows[1], rows
assert rows[2] == [{'v': f'race-{winner}'}], rows
passed("resolve keep => 'remote' makes the loser's branch its S3 head again (its snapshot stays local)")

# ON_CONFLICT 'branch': a losing snapshot becomes a branch of its own; the branch follows S3 at the next ATTACH.
run(winner, LOCAL + "UPDATE team.t SET v = 'later' WHERE id = 8;")
run(loser, LOCAL + "UPDATE team.t SET v = 'mine' WHERE id = 8;")
run(winner, TEAM + "CALL differential_create_snapshot('team');")
FORKING = f"ATTACH 'differential:team' AS team (ON_CONFLICT 'branch');"
rows = run(loser, FORKING + "SELECT snapshot_id, state, export_state, branch_id FROM differential_create_snapshot('team');"
                  + BRANCHES)
forked = rows[0][0]
assert forked['state'] == 'published_as_branch' and forked['export_state'] == 'complete', rows
fork = f"main-conflict-{forked['snapshot_id']}"
main = next(r for r in rows[1] if r['branch_name'] == 'main')
assert main['remote_state'] == 'forked' and main['kept_branch'] == fork, rows
assert main['remote_snapshot_id'] < forked['snapshot_id'], rows  # the winner's head
assert {'branch_name': fork, 'remote_state': 'in_sync', 'remote_snapshot_id': forked['snapshot_id'],
        'kept_branch': None} in rows[1], rows
assert ('team', fork, forked['snapshot_id']) in heads()
rows = run(loser, TEAM + "SELECT v FROM team.t WHERE id = 8;" + BRANCHES)
assert rows[0] == [{'v': 'later'}], rows
assert any(r['branch_name'] == 'main' and r['remote_state'] == 'in_sync' for r in rows[1]), rows
rows = run('reader4', f"ATTACH 'differential:{LOCATION}heads/team/{fork}' AS r ({S3}); SELECT v FROM r.t WHERE id = 8;")
assert rows[-1] == [{'v': 'mine'}], rows
error = run(loser, f"ATTACH 'differential:team' AS team (ON_CONFLICT 'branch', READ_ONLY);", expected=1)
assert 'apply to writable attachments' in error, error
passed("ON_CONFLICT 'branch' publishes a losing snapshot as <branch>-conflict-<n>; the unchanged branch follows S3 "
       'at its next ATTACH')

# --- Publication after checkpoints, at DETACH, and of snapshots left unpublished ------------
def main_head():
    return next(n for d, b, n in heads() if d == 'team' and b == 'main')


def publishes_in_background(repository, sql, before):
    """Run `sql`, then keep the session busy (no DETACH, no exit) until main's head moves."""
    # Killed below: a short marker lease lets its marker expire soon (test builds).
    args, options = command(repository, sql + "SELECT sum(range) FROM range(100000000000);",
                            DIFFERENTIAL_TEST_MARKER_LEASE_MS='3000')
    process = subprocess.Popen(args, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                               text=True, env=options['env'])
    process.stdin.write(options['input'])
    process.stdin.close()
    try:
        deadline = time.monotonic() + 60
        while main_head() == before:
            assert process.poll() is None and time.monotonic() < deadline, process.stderr.read()
            time.sleep(0.2)
    finally:
        process.kill()
        process.wait()
    return main_head()


before = main_head()
after = publishes_in_background('p', TEAM + "UPDATE team.t SET v = 'background' WHERE id = 9; CHECKPOINT team;", before)
assert after > before
background = after
rows = run('reader5', f"ATTACH 'differential:{LOCATION}heads/team' AS r ({S3}); SELECT v FROM r.t WHERE id = 9;")
assert rows[-1] == [{'v': 'background'}], rows
rows = run('p', f"SELECT commit_message FROM differential_snapshots('team') WHERE snapshot_id = {after};")
assert rows[-1] == [{'commit_message': 'checkpoint'}], rows
passed('a checkpoint of a shared database is a snapshot published in the background while the session goes on')

rows = run('q', LOCAL + "UPDATE team.t SET v = 'kept local' WHERE id = 10; CHECKPOINT team; "
                "SELECT count(*) AS n FROM differential_snapshots('team') WHERE publication_state != 'remote';")
assert rows[-1] == [{'n': 1}] and main_head() == after, rows  # only the joined head
passed('PUBLISH_ON_CHECKPOINT false: checkpoints stay local')

rows = run('q', f"ATTACH 'differential:team' AS team (PUBLISH_ON_DETACH true); UPDATE team.t SET v = 'at detach' "
                "WHERE id = 10; DETACH team; SELECT value FROM differential_storage_stats() WHERE metric = "
                "'shared_publications';")
assert rows[-1][0]['value'] >= 1 and main_head() > after, rows
rows = run('reader6', f"ATTACH 'differential:{LOCATION}heads/team' AS r ({S3}); SELECT v FROM r.t WHERE id = 10;")
assert rows[-1] == [{'v': 'at detach'}], rows
passed('PUBLISH_ON_DETACH publishes the final checkpoint before DETACH returns')

before = main_head()
rows = run('q', TEAM + "UPDATE team.t SET v = 'deferred' WHERE id = 11; SELECT snapshot_id, export_state FROM "
                "differential_create_snapshot('team', export_mode => 'deferred');")
assert rows[-1][0]['export_state'] == 'pending' and main_head() == before, rows
after = publishes_in_background('q', TEAM, before)
rows = run('reader7', f"ATTACH 'differential:{LOCATION}heads/team' AS r ({S3}); SELECT v FROM r.t WHERE id = 11;")
assert rows[-1] == [{'v': 'deferred'}] and after > before, rows
passed('a snapshot left unpublished is published in the background at the next ATTACH')

# --- Advisory "being written" markers -------------------------------------------------------
LEASE = {'DIFFERENTIAL_TEST_MARKER_LEASE_MS': '3000'}  # test builds: 3 s lease, renewed every 0.75 s
time.sleep(4)  # markers of the sessions killed above have expired
WRITERS = "SELECT branch_name, host, live, this_process FROM differential_writers('team') ORDER BY ALL;"
args, options = command('p', TEAM + "SELECT sum(range) FROM range(100000000000);", **LEASE)
busy = subprocess.Popen(args, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                        env=options['env'])
busy.stdin.write(options['input'])
busy.stdin.close()
try:
    deadline = time.monotonic() + 60
    while not any(w['live'] for w in run('a', WRITERS)[-1]):
        assert busy.poll() is None and time.monotonic() < deadline, busy.stderr.read()
        time.sleep(0.2)
    time.sleep(4)  # longer than the lease: only renewal keeps the marker live
    writers = [w for w in run('a', WRITERS)[-1] if w['live']]
    assert len(writers) == 1 and writers[0]['branch_name'] == 'main' and writers[0]['live'], writers
    assert not writers[0]['this_process'], writers
    error = run('q', f"ATTACH 'differential:team' AS team (ON_BUSY 'error');", expected=1, **LEASE)
    assert '[differential:writer_busy]' in error and 'being written by 1 other writer' in error, error
    rows, stderr = run('q', TEAM + WRITERS, with_stderr=True, **LEASE)
    assert 'Warning: Branch main is being written by 1 other writer' in stderr, stderr
    assert [w['this_process'] for w in rows[-1]] == [False, True] or [w['this_process'] for w in rows[-1]] == \
        [True, False], rows
    rows, stderr = run('q', f"ATTACH 'differential:team' AS team (ON_BUSY 'ignore');", with_stderr=True, **LEASE)
    assert 'Warning' not in stderr, stderr
finally:
    busy.kill()  # its marker stays behind until it expires
    busy.wait()
time.sleep(4)
rows = run('q', f"ATTACH 'differential:team' AS team (ON_BUSY 'error');" + WRITERS, **LEASE)
assert [w['this_process'] for w in rows[-1]] == [True], rows  # the expired marker was removed
assert run('a', WRITERS) == [[]], 'a closed session removes its marker'
passed("writer markers: a live writer is listed by differential_writers and warns (ON_BUSY 'warn'), refuses "
       "(ON_BUSY 'error') or is ignored; markers are renewed while a session lasts, removed when it ends, and "
       "expire after a crash")

# --- History across writers, expiry and dropped branches ----------------------------------
latest = main_head()
rows = run('a', "SELECT snapshot_id, publication_state, commit_message, snapshot_time::VARCHAR AS t FROM "
                f"differential_snapshots('team') WHERE snapshot_id IN ({background}, {latest}) ORDER BY 1;")
assert [(r['snapshot_id'], r['publication_state']) for r in rows[-1]] == [(background, 'remote'), (latest, 'remote')], rows
assert rows[-1][0]['commit_message'] == 'checkpoint', rows
background_time = rows[-1][0]['t']
moment = datetime.datetime.fromisoformat(background_time.replace('+00', '+00:00'))
before_background = (moment - datetime.timedelta(microseconds=1)).isoformat(sep=' ')
rows = run('a', f"ATTACH 'differential:team' AS old (SNAPSHOT_VERSION {background}); SELECT v FROM old.t WHERE id = 9;")
assert rows[-1] == [{'v': 'background'}], rows
rows = run('a', TEAM + f"SELECT v FROM team.t AT (VERSION => {background}) WHERE id = 9; "
                f"SELECT v FROM team.t AT (TIMESTAMP => TIMESTAMPTZ '{background_time}') WHERE id = 9; "
                f"SELECT v FROM team.t AT (TIMESTAMP => TIMESTAMPTZ '{before_background}') "
                "WHERE id = 9;")
assert rows == [[{'v': 'background'}], [{'v': 'background'}], [{'v': 'b'}]], rows  # b's update before it
rows = run('b', TEAM + "UPDATE team.t SET v = 'release' WHERE id = 12; SELECT snapshot_id FROM "
                "differential_create_snapshot('team', name => 'release-1');")
release = rows[-1][0]['snapshot_id']
rows = run('a', "ATTACH 'differential:team' AS r (SNAPSHOT_NAME 'release-1'); SELECT v FROM r.t WHERE id = 12; "
                f"SELECT snapshot_name FROM differential_snapshots('team') WHERE snapshot_id = {release};")
assert rows == [[{'v': 'release'}], [{'snapshot_name': 'release-1'}]], rows
passed("other writers' snapshots are listed (publication_state 'remote') and attachable by version, name, AT "
       '(VERSION) and AT (TIMESTAMP) along the branch ancestry')

rows = run('b', f"SELECT snapshot_id FROM differential_expire_snapshots('team', versions => [{background}]);")
assert rows[-1] == [{'snapshot_id': background}], rows
rows = run('a', f"SELECT retired FROM differential_snapshots('team') WHERE snapshot_id = {background};")
assert rows[-1] == [{'retired': True}], rows
error = run('a', f"ATTACH 'differential:team' AS old (SNAPSHOT_VERSION {background});", expected=1)
assert '[differential:retired]' in error, error
error = run('b', f"SELECT * FROM differential_expire_snapshots('team', versions => [{release}]);", expected=1)
assert 'is the head of branch main' in error, error
passed('an expired snapshot is retired for every writer; a branch head cannot expire')

run('b', f"CALL differential_create_branch('team', 'tmp', snapshot => {release});")
rows = run('c', "ATTACH 'differential:team' AS tmp (BRANCH 'tmp'); SELECT v FROM tmp.t WHERE id = 12;")
assert rows[-1] == [{'v': 'release'}] and ('team', 'tmp', release) in heads(), rows
run('b', "CALL differential_drop_branch('team', 'tmp');")
assert not any(b == 'tmp' for d, b, n in heads())
error = run('reader8', f"ATTACH 'differential:{LOCATION}heads/team/tmp' AS r ({S3});", expected=1)
assert 'not_found' in error, error
error = run('c', "ATTACH 'differential:team' AS tmp (BRANCH 'tmp');", expected=1)
assert '[differential:not_found]' in error and 'dropped' in error, error
rows = run('c', "SELECT state FROM differential_branches('team') WHERE branch_name = 'tmp';")
assert rows[-1] == [{'state': 'deleted'}], rows
run('c', f"CALL differential_create_branch('team', 'tmp', snapshot => {release});")
assert ('team', 'tmp', release) in heads()
passed('a dropped branch is dropped for every writer (an unchanged local copy too) and readers; its name can be '
       'used again')

run('a', f"CALL differential_create_branch('team', 'from-release', snapshot => {release});")  # b's snapshot
rows = run('reader9', f"ATTACH 'differential:{LOCATION}heads/team/from-release' AS r ({S3}); SELECT v FROM r.t WHERE id = 12;")
assert rows[-1] == [{'v': 'release'}], rows
run('c', TEAM + "SELECT 1;")  # c's main follows S3 (to b's release)
rows = run('c', f"SELECT snapshot_id FROM differential_restore('team', {latest});")  # not a local snapshot of c
restored = rows[-1][0]['snapshot_id']
assert main_head() == restored, (main_head(), restored)
rows = run('reader10', f"ATTACH 'differential:{LOCATION}heads/team' AS r ({S3}); SELECT v FROM r.t WHERE id = 12;")
assert rows[-1] == [{'v': 'a'}], rows
passed("branches start from, and restores return to, other writers' snapshots (imported without their data)")

# --- Names, claims and readers ---------------------------------------------------------------
run('solo', f"ATTACH 'differential:solo' AS solo (DATA_PATH '{LOCATION}', {S3}); CREATE TABLE solo.t AS SELECT 1 AS v; "
            "CALL differential_create_snapshot('solo');")
error = run('d', f"ATTACH 'differential:solo' AS solo (WRITERS 'shared', DATA_PATH '{LOCATION}', {S3});", expected=1)
assert '[differential:name_conflict]' in error and 'not shared' in error, error
error = run('a', "CALL differential_claim_branch_heads('team');", expected=1)
assert 'shared by several writers' in error, error
rows = run('e', f"ATTACH 'differential:team' AS team (WRITERS 'shared', DATA_PATH '{LOCATION}', {S3}, READ_ONLY); "
                "SELECT v FROM team.t WHERE id = 7;")
assert rows[-1] == [{'v': f'race-{winners[0]}'}], rows
error = run('f', f"ATTACH 'differential:team' AS team (WRITERS 'shared', DATA_PATH '{LOCATION}', {S3}, BRANCH 'nope');",
            expected=1)
assert '[differential:not_found]' in error, error
passed('a name taken by a single-writer database is refused; claims are refused; a fresh read-only attachment '
       'reads the latest head; unknown branches are not found')

evidence_path('s3-shared-aws.json' if AWS else 's3-shared.json').write_text(json.dumps({'scope': (f'real AWS ({LOCATION})' if AWS else 'disposable pinned MinIO') + '; one host, one repository per '
                                                                 'simulated host', 'passed': results},
                                                       indent=2) + '\n')
