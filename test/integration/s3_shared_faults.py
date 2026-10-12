#!/usr/bin/env python3
"""Shared databases under faults: process death between every step of the publication protocol
(test-build failpoints, DIFFERENTIAL_SHARED_CRASH) followed by recovery, lost and failing S3
responses (an HTTP relay in front of the disposable MinIO; all S3 calls still come from the SDK),
and skewed host clocks (DIFFERENTIAL_TEST_CLOCK_SKEW_MS). Each repository stands for one host."""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import datetime
import http.client
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import threading
from urllib.parse import unquote, urlsplit
from evidence import evidence_path
from paths import DUCKDB_CLI, EXTENSION

endpoint, base = sys.argv[1:]
target = urlsplit(endpoint)
base = Path(base) / 'shared-faults'
base.mkdir()
results = []


class Relay(BaseHTTPRequestHandler):
    """Forwards to MinIO. A rule {method, path, action, remaining} matching a request (path after
    URL decoding, so listing prefixes match too) either fails it with 503 ('fail') or forwards it
    and drops the response ('lose')."""
    protocol_version = 'HTTP/1.1'
    rules = []
    lock = threading.Lock()

    def log_message(self, *args):
        pass

    def rule(self):
        with Relay.lock:
            for rule in Relay.rules:
                if rule['remaining'] and rule['method'] == self.command and rule['path'] in unquote(self.path):
                    rule['remaining'] -= 1
                    rule['hits'] += 1
                    return rule['action']
        return None

    def handle_request(self):
        if self.headers.get('Transfer-Encoding', '').lower() == 'chunked':
            parts = []
            while True:
                line = self.rfile.readline()
                parts.append(line)
                size = int(line.split(b';', 1)[0], 16)
                if not size:
                    while True:
                        trailer = self.rfile.readline()
                        parts.append(trailer)
                        if trailer == b'\r\n':
                            break
                    break
                parts.append(self.rfile.read(size + 2))
            body = b''.join(parts)
        else:
            body = self.rfile.read(int(self.headers.get('Content-Length', '0')))
        action = self.rule()
        if action == 'fail':
            data = b'<Error><Code>SlowDown</Code><Message>Injected failure</Message></Error>'
            self.send_response(503)
            self.send_header('Content-Length', str(len(data)))
            self.end_headers()
            self.wfile.write(data)
            return
        connection = http.client.HTTPConnection(target.hostname, target.port, timeout=10)
        try:
            connection.request(self.command, self.path, body, dict(self.headers))
            response = connection.getresponse()
            data = response.read()
            if action == 'lose':
                self.connection.shutdown(socket.SHUT_RDWR)
                self.connection.close()
                return
            self.send_response(response.status)
            for key, value in response.getheaders():
                if key.lower() not in ('transfer-encoding', 'connection'):
                    self.send_header(key, value)
            self.end_headers()
            if self.command != 'HEAD':
                self.wfile.write(data)
        finally:
            connection.close()
            self.close_connection = True

    do_GET = do_HEAD = do_PUT = do_POST = do_DELETE = handle_request


server = ThreadingHTTPServer(('127.0.0.1', 0), Relay)
threading.Thread(target=server.serve_forever, daemon=True).start()
relay = f'http://127.0.0.1:{server.server_port}'
LOCATION = 's3://differential-sdk-test/shared-faults/'
S3 = f"ENDPOINT '{relay}', PATH_STYLE true, ALLOW_HTTP true"
S3_ARGS = f"endpoint => '{relay}', path_style => true, allow_http => true"
# Publication only where a test asks for it: checkpoints stay local.
TEAM = f"ATTACH 'differential:team' AS team (WRITERS 'shared', DATA_PATH '{LOCATION}', {S3}, PUBLISH_ON_CHECKPOINT false);"
OTHER = (f"ATTACH 'differential:team' AS other (WRITERS 'shared', DATA_PATH '{LOCATION}', {S3}, BRANCH 'other', "
         "PUBLISH_ON_CHECKPOINT false);")


def faults(*rules):
    with Relay.lock:
        Relay.rules = [dict(method=m, path=p, action=a, remaining=n, hits=0) for m, p, a, n in rules]


def hits():
    with Relay.lock:
        return sum(rule['hits'] for rule in Relay.rules)


def passed(name):
    results.append(name)
    print('PASS', name, flush=True)


def run(repository, sql, expected=0, **env):
    r = subprocess.run([DUCKDB_CLI, '-init', '/dev/null', '-unsigned', '-batch', '-bail', '-json'],
                       input=f"LOAD '{EXTENSION}';" + sql, text=True, capture_output=True, timeout=300,
                       env=dict(os.environ, DIFFERENTIAL_STORE_ROOT=str(base / repository), **env))
    assert r.returncode == expected, (repository, sql, expected, r.returncode, r.stdout, r.stderr)
    if expected:
        return r.stderr
    out, text, decoder = [], r.stdout.strip(), json.JSONDecoder()
    while text:
        value, end = decoder.raw_decode(text)
        out.append(value)
        text = text[end:].lstrip()
    return out


def heads():
    rows = run('lister', f"SELECT branch_name, snapshot_id FROM differential_remote_heads('{LOCATION}', {S3_ARGS});")
    return {r['branch_name']: r['snapshot_id'] for r in rows[-1]}


def head_value(branch, row_id, reader='reader'):
    rows = run(reader, f"ATTACH 'differential:{LOCATION}heads/team/{branch}' AS r ({S3}); SELECT v FROM r.t WHERE id = {row_id};")
    return rows[-1][0]['v']


def state(repository, branch='main'):
    rows = run(repository, "SELECT remote_state FROM differential_branches('team') WHERE branch_name = "
                           f"'{branch}' AND state = 'active';")
    return rows[-1][0]['remote_state']


CRASH = 86  # failpoints end the process with this status

faults()
run('w', TEAM + "CREATE TABLE team.t AS SELECT range AS id, 'start' AS v FROM range(1000); "
                "CALL differential_create_snapshot('team'); CALL differential_create_branch('team', 'other');")
run('o', OTHER + "SELECT 1;")  # a second host, writing branch 'other'

# --- Process death between the steps of a publication, then recovery ---------------------------
for point in ('renumbered', 'commit_recorded', 'head_pending', 'head_written'):
    # Another host takes the next snapshot number (on its own branch), so w's export renumbers.
    run('o', OTHER + f"UPDATE other.t SET v = 'other-{point}' WHERE id = 1; CALL differential_create_snapshot('other');")
    error = run('w', TEAM + f"UPDATE team.t SET v = '{point}' WHERE id = 1; CALL differential_create_snapshot('team');",
                expected=CRASH, DIFFERENTIAL_SHARED_CRASH=point)
    rows = run('w', "SELECT snapshot_id, export_state FROM differential_retry_export('team');")
    rows = run('w', "SELECT snapshot_id, export_state FROM differential_snapshots('team') WHERE "
                    "publication_state != 'remote' ORDER BY snapshot_id DESC LIMIT 1;")
    latest = rows[-1][0]
    assert latest['export_state'] == 'complete', (point, rows)
    assert heads()['main'] == latest['snapshot_id'], (point, heads(), latest)
    assert head_value('main', 1) == point and state('w') == 'in_sync', point
    assert run('w', "SELECT count(*) AS n FROM differential_snapshots('team') WHERE export_state = 'pending';")[-1] == \
        [{'n': 0}], point
passed('process death after renumbering, after the commit record, before and after the head write: '
       'differential_retry_export completes the publication without a conflict or a second number')

error = run('c', TEAM + "SELECT 1;", expected=CRASH, DIFFERENTIAL_SHARED_CRASH='head_adopted')
rows = run('c', "SELECT count(*) AS n FROM differential_branches('team');")
assert rows[-1] == [{'n': 0}], rows  # the adoption rolled back
rows = run('c', TEAM + "SELECT v FROM team.t WHERE id = 1;")
assert rows[-1] == [{'v': 'head_written'}] and state('c') == 'in_sync', rows
passed('process death while adopting a head leaves nothing behind; the next ATTACH adopts it')

run('w', "CALL differential_create_branch('team', 'tmp');")
assert 'tmp' in heads()
run('w', "CALL differential_drop_branch('team', 'tmp', request_id => 'drop-tmp');", expected=CRASH,
    DIFFERENTIAL_SHARED_CRASH='branch_dropped')
assert 'tmp' in heads()  # dropped here, not yet in S3
run('w', "CALL differential_drop_branch('team', 'tmp', request_id => 'drop-tmp');")
assert 'tmp' not in heads()
passed('process death between the local drop and the tombstone: repeating the drop finishes it in S3')

old = run('w', "SELECT min(snapshot_id) AS n FROM differential_snapshots('team');")[-1][0]['n']
run('w', f"CALL differential_expire_snapshots('team', versions => [{old}]);", expected=CRASH,
    DIFFERENTIAL_SHARED_CRASH='expiry_recorded')
rows = run('o', f"SELECT retired FROM differential_snapshots('team') WHERE snapshot_id = {old};")
assert rows[-1] == [{'retired': True}], rows  # recorded in S3 before the local step
run('w', f"CALL differential_expire_snapshots('team', versions => [{old}]);")
rows = run('w', f"SELECT retired FROM differential_snapshots('team') WHERE snapshot_id = {old};")
assert rows[-1] == [{'retired': True}], rows
passed('process death after the expiry record: other hosts already see it; repeating the expiry finishes it')

run('w', TEAM + "UPDATE team.t SET v = 'w-diverged' WHERE id = 2;")
run('c', TEAM + "UPDATE team.t SET v = 'c-wins' WHERE id = 2; CALL differential_create_snapshot('team');")
error = run('w', TEAM + "CALL differential_create_snapshot('team');", expected=1)
assert '[differential:branch_conflict]' in error, error
RESOLVE = "SELECT * FROM differential_resolve_conflict('team', keep => 'branch', request_id => 'resolve-w');"
run('w', RESOLVE, expected=CRASH, DIFFERENTIAL_SHARED_CRASH='conflict_forked')
rows = run('w', RESOLVE)
fork = rows[-1][0]['kept_branch']
assert fork and fork in heads() and state('w') == 'in_sync', (rows, heads())
assert head_value(fork, 2) == 'w-diverged' and head_value('main', 2) == 'c-wins'
passed('process death after the fork of a conflict resolution: repeating the request finishes it with one fork')

# --- Lost and failing S3 responses -------------------------------------------------------------
before = heads()['main']
faults(('PUT', '/branches/main', 'lose', 1))
rows = run('w', TEAM + "UPDATE team.t SET v = 'lost-head' WHERE id = 3; "
                       "SELECT snapshot_id FROM differential_create_snapshot('team');")
assert hits() == 1 and heads()['main'] == rows[-1][0]['snapshot_id'] > before, (hits(), heads(), rows)
assert state('w') == 'in_sync'
passed('a lost head-write response (SDK retry against the landed write) is recognised: no false branch_conflict')

local = run('w', "SELECT max(snapshot_id) AS n FROM differential_snapshots('team');")[-1][0]['n']
faults(('PUT', '/snapshots/', 'lose', 1))
rows = run('w', TEAM + "UPDATE team.t SET v = 'lost-record' WHERE id = 3; "
                       "SELECT snapshot_id FROM differential_create_snapshot('team');")
assert hits() == 1 and rows[-1] == [{'snapshot_id': local + 1}], (hits(), rows)  # not renumbered
assert heads()['main'] == local + 1 and head_value('main', 3) == 'lost-record'
passed('a lost commit-record response: the create-only write is recognised as identical; no renumbering')

faults(('GET', '/branches/main', 'fail', 1000))
error = run('o', TEAM + "SELECT 1;", expected=1)
assert '503' in error or 'SlowDown' in error or 'Injected' in error, error
faults()
rows = run('o', TEAM + "SELECT v FROM team.t WHERE id = 3;")
assert rows[-1] == [{'v': 'lost-record'}], rows
passed('S3 failing at ATTACH: the attachment fails with the S3 error and the next one works')

faults(('PUT', '/writers/', 'fail', 1000), ('DELETE', '/writers/', 'fail', 1000))
rows = run('o', TEAM + "SELECT v FROM team.t WHERE id = 3;")
assert rows[-1] == [{'v': 'lost-record'}] and hits() > 0, rows
faults()
passed('writer markers failing to write or delete never fail an attachment (advisory)')

faults(('GET', '/snapshots/', 'fail', 1000))  # the commit-index listing
error = run('w', TEAM + "UPDATE team.t SET v = 'after-outage' WHERE id = 3; CALL differential_create_snapshot('team');",
            expected=1)
assert '[differential:incomplete_export]' in error, error
faults()
run('w', "CALL differential_retry_export('team');")
assert head_value('main', 3) == 'after-outage' and state('w') == 'in_sync'
passed('the commit index unreachable during a publication: the snapshot stays pending and '
       'differential_retry_export publishes it')

# --- Skewed clocks -----------------------------------------------------------------------------
HOUR = str(3600 * 1000)
run('fast', TEAM + "SELECT 1;", DIFFERENTIAL_TEST_CLOCK_SKEW_MS=HOUR)
run('slow', TEAM + "SELECT 1;", DIFFERENTIAL_TEST_CLOCK_SKEW_MS='-' + HOUR)
for repository, skew in (('fast', HOUR), ('slow', '-' + HOUR)):
    run(repository, TEAM + f"UPDATE team.t SET v = 'skew-{repository}' WHERE id = 4;",
        DIFFERENTIAL_TEST_CLOCK_SKEW_MS=skew)
run('fast', TEAM + "CALL differential_create_snapshot('team');", DIFFERENTIAL_TEST_CLOCK_SKEW_MS=HOUR)
error = run('slow', TEAM + "CALL differential_create_snapshot('team');", expected=1,
            DIFFERENTIAL_TEST_CLOCK_SKEW_MS='-' + HOUR)
assert '[differential:branch_conflict]' in error, error
assert head_value('main', 4) == 'skew-fast'
passed('clocks two hours apart: the conflict is detected as with any clocks (numbers and heads use no time)')

# Time travel and writer markers do use host clocks (documented): a snapshot published by a host
# whose clock is ahead lies "in the future" for AT (TIMESTAMP), and a marker from a host whose clock
# is behind looks expired.
fast_head = heads()['main']
now = datetime.datetime.now(datetime.timezone.utc).isoformat(sep=' ')
rows = run('o', TEAM + f"SELECT v FROM team.t AT (TIMESTAMP => TIMESTAMPTZ '{now}') WHERE id = 4; "
                       f"SELECT v FROM team.t AT (VERSION => {fast_head}) WHERE id = 4;")
assert rows[0] != [{'v': 'skew-fast'}] and rows[1] == [{'v': 'skew-fast'}], rows
args = [DUCKDB_CLI, '-init', '/dev/null', '-unsigned', '-batch', '-bail', '-json']
busy = subprocess.Popen(args, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                        env=dict(os.environ, DIFFERENTIAL_STORE_ROOT=str(base / 'o'),
                                 DIFFERENTIAL_TEST_CLOCK_SKEW_MS='-' + HOUR))
busy.stdin.write(f"LOAD '{EXTENSION}';" + TEAM + "SELECT sum(range) FROM range(100000000000);")
busy.stdin.close()
try:
    import time
    deadline = time.monotonic() + 60
    while True:
        writers = run('w', "SELECT live FROM differential_writers('team') WHERE NOT this_process;")[-1]
        if writers or time.monotonic() > deadline:
            break
        time.sleep(0.2)
    assert writers == [{'live': False}], writers
    rows = run('w', TEAM + "SELECT 1;")  # no warning: the behind-clock marker looks expired
finally:
    busy.kill()
    busy.wait()
passed('host clocks affect only time travel by time and writer markers, as documented: a snapshot from a '
       'host an hour ahead is not yet visible to AT (TIMESTAMP => now()), and a marker from a host an hour '
       'behind looks expired')

server.shutdown()
evidence_path('s3-shared-faults.json').write_text(json.dumps({'scope': 'disposable pinned MinIO behind a fault '
                                                                       'relay; one host; no real AWS',
                                                              'passed': results}, indent=2) + '\n')
