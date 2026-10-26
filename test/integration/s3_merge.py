#!/usr/bin/env python3
"""Single-writer S3 snapshot merges, against the disposable MinIO fixture only.
All S3 calls originate in the extension's SDK; the relay injects transport failures.
"""
from contextlib import closing
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import http.client
import json
import os
from pathlib import Path
import shutil
import socket
import sqlite3
import subprocess
import sys
import threading
import time
from urllib.parse import urlsplit

from evidence import evidence_path
from paths import DUCKDB_CLI, EXTENSION


def parse(text):
    results, decoder = [], json.JSONDecoder()
    while text.strip():
        text = text.lstrip()
        value, end = decoder.raw_decode(text)
        results.append(value)
        text = text[end:]
    return results


class FaultRelay:
    def __init__(self, endpoint):
        self.mode, self.triggered = '', 0
        self.requests, self.received, self.sent = 0, 0, 0
        self.head_writes = [] # Conditions only, never Authorization or credential headers.
        self.head_received, self.head_release, self.head_finished = threading.Event(), threading.Event(), threading.Event()
        self.target = urlsplit(endpoint)
        owner = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = 'HTTP/1.1'
            def log_message(self, *args): pass
            def handle_request(self):
                # Packed product exports stay below the multipart threshold. The
                # AWS SDK may nevertheless stream a signed chunked PUT body.
                if self.headers.get('Transfer-Encoding', '').lower() == 'chunked':
                    parts = []
                    while True:
                        line = self.rfile.readline(); parts.append(line)
                        size = int(line.split(b';', 1)[0], 16)
                        if size: parts.append(self.rfile.read(size + 2))
                        else:
                            while True:
                                line = self.rfile.readline(); parts.append(line)
                                if line == b'\r\n': break
                            break
                    body = b''.join(parts)
                else: body = self.rfile.read(int(self.headers.get('Content-Length', '0')))
                owner.requests += 1; owner.sent += len(body)
                if self.command == 'PUT' and '/branches/' in self.path:
                    owner.head_writes.append((self.path, self.headers.get('If-Match'), self.headers.get('If-None-Match')))
                denied = ((owner.mode == 'upload-denied' and self.command == 'PUT' and '/packs/' in self.path)
                          or (owner.mode == 'descriptor-denied' and self.command == 'PUT' and '/descriptors/' in self.path)
                          or (owner.mode == 'download-denied' and self.command == 'GET' and '/packs/' in self.path)
                          or (owner.mode == 'head-read-denied' and self.command in ('HEAD','GET') and '/branches/' in self.path)
                          or (owner.mode == 'head-get-denied' and self.command == 'GET' and '/branches/' in self.path))
                if denied:
                    owner.triggered += 1
                    data = b'<Error><Code>AccessDenied</Code><Message>Injected denial</Message></Error>'
                    self.send_response(403); self.send_header('Content-Length', str(len(data)))
                    self.end_headers(); self.wfile.write(data); return
                delayed = owner.mode == 'head-delayed' and self.command == 'PUT' and '/branches/' in self.path
                if delayed:
                    owner.head_received.set()
                    if not owner.head_release.wait(30): raise AssertionError('Delayed head test timed out')
                connection = http.client.HTTPConnection(owner.target.hostname, owner.target.port, timeout=15)
                try:
                    connection.request(self.command, self.path, body, dict(self.headers))
                    response = connection.getresponse(); data = response.read(); headers = response.getheaders()
                    if delayed: owner.head_finished.set()
                    owner.received += len(data)
                    if owner.mode == 'head-response-loss' and self.command == 'PUT' and '/branches/' in self.path and response.status == 200:
                        owner.triggered += 1
                        self.connection.shutdown(socket.SHUT_RDWR); self.connection.close(); return
                    if owner.mode == 'corrupt-download' and self.command == 'GET' and '/packs/' in self.path and data:
                        owner.triggered += 1; data = bytes([data[0] ^ 1]) + data[1:]
                        headers = [(k, v) for k, v in headers if not k.lower().startswith('x-amz-checksum')]
                    self.send_response(response.status)
                    for key, value in headers:
                        if key.lower() not in ('transfer-encoding', 'connection'): self.send_header(key, value)
                    self.end_headers()
                    if self.command != 'HEAD': self.wfile.write(data)
                except (BrokenPipeError,ConnectionResetError):
                    if not delayed: raise
                finally:
                    connection.close(); self.close_connection = True
            do_GET = do_HEAD = do_PUT = do_POST = do_DELETE = handle_request
        self.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.endpoint = f'http://127.0.0.1:{self.server.server_port}'

    def close(self):
        self.head_release.set()
        self.server.shutdown(); self.server.server_close(); self.thread.join()


class Fixture:
    def __init__(self, base, label, endpoint):
        self.folder = Path(base) / label; self.folder.mkdir()
        self.repo = self.folder / 'repo'
        self.endpoint = endpoint
        self.location = f's3://differential-sdk-test/merge-{label}/'
        self.options = f"ENDPOINT '{endpoint}', PATH_STYLE true, ALLOW_HTTP true"

    def prefix(self, repo=None):
        return (f"LOAD '{EXTENSION}'; SET differential_repository='{repo or self.repo}';"
                f"SET temp_directory='{self.folder / 'spill'}'; SET max_temp_directory_size='256MB';"
                "SET threads=2; SET memory_limit='128MB'; SET differential_auto_gc=false;")

    def run(self, sql, error=None, crash=None, repo=None, raw=False):
        env = dict(os.environ)
        if crash: env['DIFFERENTIAL_MERGE_CRASH'] = crash
        result = subprocess.run([DUCKDB_CLI, '-init', '/dev/null', '-unsigned', '-batch', '-bail', '-json'],
                                input=self.prefix(repo)+sql, text=True, capture_output=True, env=env, timeout=120)
        if crash:
            assert result.returncode == 86, (crash, result.stdout, result.stderr)
            return
        if error:
            assert result.returncode != 0 and error.lower() in result.stderr.lower(), (error, result.stdout, result.stderr)
            return result.stderr
        assert result.returncode == 0, (result.stdout, result.stderr)
        return result.stdout if raw else parse(result.stdout)

    def scalar(self, sql, repo=None):
        with closing(sqlite3.connect(f'file:{repo or self.repo}/engine.sqlite?mode=ro', uri=True)) as db:
            return db.execute(sql).fetchone()[0]

    def setup(self, ours="UPDATE o.x SET a=11 WHERE id=1;", theirs="UPDATE t.x SET b='theirs' WHERE id=1;", size=8, constraints=''):
        self.run(f"""
ATTACH 'differential:merge_db' AS d (DATA_PATH '{self.location}', {self.options});
CREATE TABLE d.x(id INT PRIMARY KEY,a BIGINT NOT NULL CHECK(a>=0),b VARCHAR,amount DECIMAL(18,3),flag BOOLEAN);
INSERT INTO d.x SELECT i,i,'base',1.250,true FROM range({size}) r(i);
CREATE INDEX ix ON d.x(a,id);
USE d; CREATE VIEW visible AS SELECT id,a,b FROM x; USE memory;
{constraints}
CALL differential_create_snapshot('d');
CALL differential_create_branch('d','ours',snapshot=>1); CALL differential_create_branch('d','theirs',snapshot=>1);
ATTACH 'differential:merge_db' AS o (BRANCH 'ours'); {ours} CALL differential_create_snapshot('o');
ATTACH 'differential:merge_db' AS t (BRANCH 'theirs'); {theirs} CALL differential_create_snapshot('t');
""")

    def merge(self, dest='merged', request='merge', options=''):
        return f"SELECT * FROM differential_merge_snapshots('merge_db',1,2,3,'{dest}',request_id=>'{request}'{options});"

    def evict(self):
        # Move every live branch beyond these inputs, so GC can really reclaim them.
        self.run("""
ATTACH 'differential:merge_db' AS d; UPDATE d.x SET b='live' WHERE id=0; CALL differential_create_snapshot('d'); DETACH d;
ATTACH 'differential:merge_db' AS d (BRANCH 'ours'); UPDATE d.x SET b='live' WHERE id=0; CALL differential_create_snapshot('d'); DETACH d;
ATTACH 'differential:merge_db' AS d (BRANCH 'theirs'); UPDATE d.x SET b='live' WHERE id=0; CALL differential_create_snapshot('d'); DETACH d;
CALL differential_evict_snapshots('merge_db',versions=>[1,2,3]); CALL differential_cleanup_old_files(cleanup_all=>true);
""")
        assert self.scalar('SELECT count(*) FROM snapshots s JOIN reclaimed_roots r ON s.root=r.root WHERE s.id<=3') == 3

    def remote(self, branch='merged', error=None):
        return self.run(f"ATTACH 'differential:{self.location}heads/merge_db/{branch}' AS r ({self.options}); SELECT * FROM r.x ORDER BY id;",
                        error=error, repo=self.folder / ('reader-'+branch))

    def hidden(self):
        assert self.scalar("SELECT count(*) FROM branches WHERE name='merged'") == 0
        assert self.scalar("SELECT count(*) FROM operations WHERE kind='merge' AND state='complete'") == 0

    def verify(self):
        row = self.remote()[-1][1]
        assert row == {'id': 1, 'a': 11, 'b': 'theirs', 'amount': '1.250', 'flag': True}, row
        self.run("ATTACH 'differential:merge_db' AS m (BRANCH 'merged'); SELECT * FROM m.visible;")
        assert self.scalar("SELECT count(*) FROM operations WHERE kind='merge' AND state='complete'") == 1
        assert self.scalar("SELECT count(*) FROM branches WHERE name='merged'") == 1
        assert self.scalar("SELECT count(*) FROM operations WHERE kind='merge_publication' AND state='pending'") == 0
        assert self.scalar('PRAGMA integrity_check') == 'ok'
        self.run('CALL differential_cleanup_old_files(cleanup_all=>true);')
        assert self.scalar("SELECT count(*) FROM files WHERE name LIKE 'merge_stage/%'") == 0


def at_gate(f, phase, during):
    gate = f.folder / 'gate'
    env = dict(os.environ, DIFFERENTIAL_MERGE_PHASE_GATE=f'{phase}:{gate}')
    process = subprocess.Popen([DUCKDB_CLI, '-init', '/dev/null', '-unsigned', '-batch', '-bail', '-json'],
                               stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env)
    process.stdin.write(f.prefix()+f.merge()); process.stdin.close(); process.stdin = None
    try:
        deadline = time.monotonic()+25
        while not Path(str(gate)+'.entered').exists():
            assert process.poll() is None and time.monotonic() < deadline, process.communicate()
            time.sleep(.01)
        during(process)
    finally: Path(str(gate)+'.release').touch()
    out, err = process.communicate(timeout=60)
    return process.returncode, out, err


def main(endpoint, base):
    base = Path(base) / 's3-merges'; base.mkdir()
    passed = []
    def ok(label): passed.append(label); print('PASS S3 merge', label, flush=True)
    relay = FaultRelay(endpoint)
    try:
        f = Fixture(base, 'basic', relay.endpoint); f.setup()
        before = f.scalar('SELECT count(*) FROM operations')
        f.run(f.merge(request=''),error='merge_request_required')
        assert f.scalar('SELECT count(*) FROM operations') == before
        f.run('PREPARE p AS '+f.merge()+' EXPLAIN '+f.merge(), raw=True)
        assert f.scalar('SELECT count(*) FROM operations') == before
        requests = relay.requests
        preview = f.run("SELECT conflicts,validation_state FROM differential_merge_snapshots_preview('merge_db',1,2,3,validate=>true);")
        assert preview[-1] == [{'conflicts': 0, 'validation_state': 'valid'}], preview
        assert relay.requests == requests and f.scalar('SELECT count(*) FROM operations') == before
        heads = [f.remote(side)[-1] for side in ('main','ours','theirs')]
        result = f.run(f.merge())[-1][0]
        assert result['snapshot_id'] == 4 and result['publication_ms'] > 0
        f.verify()
        assert [f.remote(side)[-1] for side in ('main','ours','theirs')] == heads
        f.run("CALL differential_expire_snapshots('merge_db',versions=>[1,2,3]);")
        requests = relay.requests
        retry = f.run(f.merge())[-1][0]
        assert retry['snapshot_id'] == 4 and retry['operation_id'] == result['operation_id'] and relay.requests == requests
        f.run(f.merge(dest='different'), error='Request ID reused')
        f.run(f.merge(request='other'), error='name_conflict')
        ok('local inputs, preview/PREPARE isolation, new remote head, source preservation and completed replay without S3')

        f = Fixture(base, 'evicted', relay.endpoint); f.setup(); f.evict()
        counts = (f.scalar('SELECT count(*) FROM snapshots'),f.scalar('SELECT count(*) FROM operations'))
        f.run("SELECT conflicts FROM differential_merge_snapshots_preview('merge_db',1,2,3,validate=>true);")
        assert counts == (f.scalar('SELECT count(*) FROM snapshots'),f.scalar('SELECT count(*) FROM operations'))
        f.run(f.merge(options=',max_input_bytes=>1'), error='merge_limit'); f.hidden()
        f.run(f.merge(options=',max_stage_write_bytes=>1'), error='merge_limit'); f.hidden()
        result = f.run(f.merge())[-1][0]
        assert result['materialization_ms'] > 0
        f.verify(); ok('collected evicted inputs, live changes excluded, preview isolation and input/materialization budgets')

        for mode in ('download-denied','corrupt-download','upload-denied','descriptor-denied','head-response-loss'):
            f = Fixture(base, mode, relay.endpoint); f.setup()
            if mode in ('download-denied','corrupt-download'): f.evict()
            relay.mode = mode; relay.triggered = 0
            if mode == 'head-response-loss':
                f.run(f.merge())
            else:
                f.run(f.merge(), error='corrupt_data' if mode == 'corrupt-download' else 'denied')
                f.hidden()
            assert relay.triggered > 0, mode
            relay.mode = ''
            f.run(f.merge()); f.verify(); ok(mode+' and same-request retry')

        for phase in ('staged','prepared','uploaded','head_published','publication_transaction','published'):
            f = Fixture(base, 'crash-'+phase, relay.endpoint); f.setup()
            f.run(f.merge(), crash=phase)
            if phase != 'published': f.hidden()
            if phase in ('head_published','publication_transaction','published'): assert f.remote()[-1][1]['a'] == 11
            else: f.remote(error='not_found')
            if phase in ('prepared','uploaded'):
                # Background/ordinary export retry must not expose the private result.
                f.run("CALL differential_retry_export('merge_db');")
                f.hidden(); f.remote(error='not_found')
                f.run("CALL differential_create_branch('merge_db','merged',snapshot=>1);", error='name_conflict')
            if phase != 'staged': f.run("CALL differential_expire_snapshots('merge_db',versions=>[1,2,3]);")
            f.run('CALL differential_cleanup_old_files(cleanup_all=>true);')
            one = f.run(f.merge())[-1][0]; two = f.run(f.merge())[-1][0]
            assert one['snapshot_id'] == two['snapshot_id'] == 4 and one['operation_id'] == two['operation_id']
            f.verify(); ok('crash '+phase+', GC and retry (expired inputs after durable preparation)')

        f = Fixture(base, 'destination-race', relay.endpoint); f.setup()
        copied = f.folder / 'competitor'; shutil.copytree(f.repo,copied)
        def compete(_):
            f.hidden()
            f.run("CALL differential_create_branch('merge_db','merged',snapshot=>1);", repo=copied)
        code, out, err = at_gate(f,'head_creating',compete)
        assert code != 0 and 'name_conflict' in err, (code,out,err)
        f.hidden(); assert f.remote()[-1][1]['a'] == 1
        f.run(f.merge(),error='name_conflict'); f.hidden()
        ok('conditional destination race preserves the competing branch and retains private result')

        f = Fixture(base,'destination-existing',relay.endpoint); f.setup()
        copied = f.folder/'competitor'; shutil.copytree(f.repo,copied)
        f.run("CALL differential_create_branch('merge_db','merged',snapshot=>1);",repo=copied)
        f.run(f.merge(),error='name_conflict'); f.hidden()
        assert f.scalar("SELECT count(*) FROM operations WHERE kind='merge_publication'") == 0
        ok('existing remote-only destination rejected before output preparation')

        f = Fixture(base,'concurrent-retry',relay.endpoint); f.setup()
        from merge_lifecycle import start, release, result as process_result
        retries = []
        def concurrent(_):
            retry, gate = start(f,f.merge(),'request_waiting'); release(gate)
            time.sleep(.1); assert retry.poll() is None
            retries.append(retry)
        code,out,err = at_gate(f,'uploaded',concurrent)
        assert code == 0, (out,err)
        assert parse(out)[-1][0]['operation_id'] == process_result(retries[0])['operation_id']
        f.verify(); ok('concurrent identical requests publish one branch and one result')

        f = Fixture(base,'concurrent-snapshot',relay.endpoint); f.setup()
        def advance(_):
            assert f.run("ATTACH 'differential:merge_db' AS d; UPDATE d.x SET a=90 WHERE id=0; SELECT snapshot_id FROM differential_create_snapshot('d');")[-1][0]['snapshot_id'] == 5
        code,out,err = at_gate(f,'uploaded',advance)
        assert code == 0 and parse(out)[-1][0]['snapshot_id'] == 4, (out,err)
        assert f.scalar('SELECT last_snapshot FROM databases') == 5
        assert f.remote('main')[-1][0]['a'] == 90
        f.verify(); ok('ordinary snapshot skips reserved number; merge publishes only its new head')

        f = Fixture(base, 'cancel', relay.endpoint); f.setup()
        def cancel(process):
            f.hidden()
            process.send_signal(2)
            time.sleep(.1)
        code,out,err = at_gate(f,'prepared',cancel)
        assert code != 0, (code,out,err)
        f.hidden(); f.run(f.merge()); f.verify(); ok('cancellation after durable staging and retry')

        f = Fixture(base,'resolved',relay.endpoint); f.setup(theirs='UPDATE t.x SET a=12 WHERE id=1;')
        source = f"ATTACH '{f.folder/'choices.duckdb'}' AS choices;"
        f.run(source+"CREATE TABLE choices.fixes(id INT,a BIGINT,b VARCHAR,amount DECIMAL(18,3),flag BOOLEAN,_merge_action VARCHAR,_merge_columns MAP(VARCHAR,VARCHAR)); "
              "INSERT INTO choices.fixes VALUES(1,99,NULL,NULL,NULL,'cells',map{'a':'custom'});")
        statement = f.merge(options=",resolution_tables=>map{'main.x':'choices.main.fixes'}")
        f.run(source+statement,crash='prepared')
        (f.folder/'choices.duckdb').unlink()
        f.run(statement)
        assert f.remote()[-1][1]['a'] == 99
        assert f.run("SELECT count(*) AS n FROM differential_merge_resolutions('merge_db','merge');")[-1] == [{'n':1}]
        ok('typed resolution audit and prepared retry after resolution source removal')

        f = Fixture(base,'constraint',relay.endpoint)
        f.setup(ours='UPDATE o.x SET a=100 WHERE id=1;',theirs='UPDATE t.x SET a=100 WHERE id=2;',
                constraints='CREATE UNIQUE INDEX ux ON d.x(a);')
        preview = f.run("SELECT conflicts,validation_state FROM differential_merge_snapshots_preview('merge_db',1,2,3,validate=>true);")[-1]
        assert preview == [{'conflicts':0,'validation_state':'constraint_violation'}], preview
        f.run(f.merge(),error='constraint'); f.hidden(); f.remote(error='not_found')
        assert f.scalar("SELECT count(*) FROM operations WHERE kind='merge_publication'") == 0
        ok('final UNIQUE violation is distinct from data conflict and uploads no result')

        f = Fixture(base,'unique-permutation',relay.endpoint)
        f.setup(size=3000,ours="DELETE FROM o.x; INSERT INTO o.x SELECT i,(i+1)%3000,'base',1.250,true FROM range(3000) r(i);",
                constraints='CREATE UNIQUE INDEX ux ON d.x(a);')
        f.run(f.merge())
        rows = f.remote()[-1]
        assert all(row['a'] == (row['id']+1)%3000 for row in rows) and rows[1]['b'] == 'theirs'
        ok('3000-row unique permutation validates through rebuild on S3')

        f = Fixture(base, 'conflict', relay.endpoint); f.setup(theirs='UPDATE t.x SET a=12 WHERE id=1;')
        f.run(f.merge(), error='merge_conflict'); f.hidden(); f.remote(error='not_found')
        assert f.scalar("SELECT count(*) FROM operations WHERE kind='merge_publication'") == 0
        assert f.run("SELECT conflicts FROM differential_merge_branch_preview('merge_db','ours','theirs');")[-1] == [{'conflicts': 1}]
        f.run(f.merge().replace(',1,2,3,',',999,2,3,').replace("'merge')","'missing')"), error='not_found')
        f.run("CALL differential_expire_snapshots('merge_db',versions=>[1]);")
        f.run(f.merge(), error='retired')
        ok('data conflicts and expired/missing inputs rejected before output; branch preview reports conflicts')
    finally: relay.close()
    evidence_path('s3-merge.json').write_text(json.dumps({'scope':'disposable MinIO; no real AWS', 'passed':passed},indent=2)+'\n')


if __name__ == '__main__': main(*sys.argv[1:])
