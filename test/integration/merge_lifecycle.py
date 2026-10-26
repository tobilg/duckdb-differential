#!/usr/bin/env python3
"""Durable request inspection, abort, and cross-process fencing. Disposable local stores."""
import atexit
import os
from pathlib import Path
import subprocess
import tempfile
import time
import uuid

from evidence import evidence_path
from paths import DUCKDB_CLI
from s3_merge import Fixture, parse

children = []


@atexit.register
def stop_children():
    for child in children:
        if child.poll() is None:
            child.terminate()
            try: child.communicate(timeout=5)
            except subprocess.TimeoutExpired:
                child.kill(); child.communicate(timeout=5)


class LocalFixture(Fixture):
    def __init__(self, base, label):
        super().__init__(base, label, '')

    def setup(self):
        self.run("""
ATTACH 'differential:merge_db' AS d;
CREATE TABLE d.x(id INT PRIMARY KEY,a INT,b VARCHAR);
INSERT INTO d.x VALUES(1,0,NULL),(2,0,'base');
CALL differential_create_snapshot('d');
CALL differential_create_branch('d','ours',snapshot=>1);
CALL differential_create_branch('d','theirs',snapshot=>1);
ATTACH 'differential:merge_db' AS o (BRANCH 'ours'); UPDATE o.x SET a=1 WHERE id=1;
CALL differential_create_snapshot('o');
ATTACH 'differential:merge_db' AS t (BRANCH 'theirs'); UPDATE t.x SET b='theirs' WHERE id=1;
CALL differential_create_snapshot('t');
""")


def inspect(f, request='merge'):
    return f.run(f"SELECT * FROM differential_merge_requests('merge_db',request_id=>'{request}');")[-1][0]


def abort_sql(operation, request='abort', options=''):
    return f"SELECT * FROM differential_abort_merge('merge_db','{operation}',request_id=>'{request}'{options});"


def start(f, sql, phase=None):
    gate = f.folder / ('gate-'+uuid.uuid4().hex)
    env = dict(os.environ)
    if phase: env['DIFFERENTIAL_MERGE_PHASE_GATE'] = f'{phase}:{gate}'
    p = subprocess.Popen([DUCKDB_CLI,'-init','/dev/null','-unsigned','-batch','-bail','-json'],
                         stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,env=env)
    children.append(p)
    p.stdin.write(f.prefix()+sql); p.stdin.close(); p.stdin = None
    if phase:
        deadline = time.monotonic()+25
        while not Path(str(gate)+'.entered').exists():
            if p.poll() is not None or time.monotonic() > deadline:
                p.kill(); raise AssertionError((phase,p.communicate(timeout=5)))
            time.sleep(.01)
    return p, gate


def release(gate): Path(str(gate)+'.release').touch()


def result(p, error=None):
    out, err = p.communicate(timeout=45)
    if error:
        assert p.returncode != 0 and error.lower() in err.lower(), (out,err)
        return
    assert p.returncode == 0, (out,err)
    return parse(out)[-1][0]


def main():
    passed = []
    def ok(label): passed.append(label); print('PASS merge lifecycle',label,flush=True)
    with tempfile.TemporaryDirectory(prefix='differential-merge-lifecycle-') as tmp:
        f = LocalFixture(tmp,'terminal'); f.setup()
        f.run(f.merge(options=',max_input_rows=>1'),error='merge_limit')
        counts = f.scalar('SELECT count(*) FROM operations')
        r = inspect(f); op = r['operation_id']
        assert (r['state'],r['destination'],r['base_snapshot_id'],r['ours_snapshot_id'],r['theirs_snapshot_id']) == ('pending','merged',1,2,3)
        assert r['retained_logical_bytes'] == 0 and not r['destination_reserved']
        assert f.scalar('SELECT count(*) FROM operations') == counts
        f.run("ATTACH 'differential:other' AS other;")
        f.run(abort_sql(op).replace("'merge_db'","'other'"),error='another database')
        f.run(abort_sql('absent'),error='Operation missing')
        nonmerge = f.scalar("SELECT id FROM operations WHERE kind='create_database' LIMIT 1")
        f.run(abort_sql(nonmerge),error='Not a supported merge')
        first = f.run(abort_sql(op))[-1][0]
        assert first['state'] == 'aborted' and first['snapshot_id'] is None
        assert f.run(abort_sql(op))[-1][0] == first
        assert f.run(abort_sql(op,'second-abort'))[-1][0]['state'] == 'aborted'
        assert inspect(f)['state'] == 'aborted'
        assert f.run("SELECT state FROM differential_operations() WHERE request_id='merge';")[-1] == [{'state':'aborted'}]
        f.run(f.merge(),error='merge_aborted')
        f.run(f.merge(dest='changed'),error='Request ID reused')
        f.hidden()
        completed = f.run(f.merge(request='replacement'))[-1][0]
        r2 = inspect(f,'replacement')
        assert r2['state'] == 'complete' and r2['result_root'] and r2['retained_logical_bytes'] == 0
        f.run(abort_sql(r2['operation_id']),error='Request ID reused')
        finished = f.run(abort_sql(r2['operation_id'],'completed-abort'))[-1][0]
        assert finished['state'] == 'complete' and finished['snapshot_id'] == completed['snapshot_id']
        assert f.run(abort_sql(op))[-1][0] == first  # Destination has since been reused.
        f.run("ATTACH 'differential:merge_db' AS m (BRANCH 'merged'); SELECT * FROM m.x ORDER BY id;")
        ok('bounded inspection; terminal abort; exact replay; destination reuse; completed result preserved')

        f = LocalFixture(tmp,'branch'); f.setup()
        branch = "SELECT * FROM differential_merge_branch('merge_db','ours','theirs','merged',request_id=>'merge');"
        f.run(branch,crash='resolved')
        r = inspect(f); assert r['merge_kind'] == 'merge_branch' and r['base_snapshot_id'] == 1
        f.run(abort_sql(r['operation_id'])); f.run(branch,error='merge_aborted'); f.hidden()
        ok('resolved branch requests are inspectable and abortable before data work')

        for phase in ('abort_transaction','aborted'):
            f = LocalFixture(tmp,phase); f.setup(); f.run(f.merge(),crash='staged')
            op = inspect(f)['operation_id']
            f.run(abort_sql(op),crash=phase)
            assert inspect(f)['state'] == ('pending' if phase == 'abort_transaction' else 'aborted')
            one = f.run(abort_sql(op))[-1][0]
            assert f.run(abort_sql(op))[-1][0] == one
            f.run('CALL differential_cleanup_old_files(cleanup_all=>true);')
            assert f.scalar("SELECT count(*) FROM files WHERE name LIKE 'merge_stage/%'") == 0
            assert f.scalar("SELECT count(*) FROM operations WHERE kind='merge_aborted'") == 1
            f.hidden(); ok('crash '+phase+'; rollback/replay and stale staging reclamation')

        f = LocalFixture(tmp,'abort-wins'); f.setup(); f.run(f.merge(),crash='staged')
        op = inspect(f)['operation_id']
        abort, gate = start(f,abort_sql(op),'abort_ready')
        retry, waiting = start(f,f.merge(),'request_waiting')
        release(waiting); time.sleep(.15); assert retry.poll() is None
        release(gate); assert result(abort)['state'] == 'aborted'; result(retry,'merge_aborted')
        f.hidden(); ok('abort fences an already-started retry across processes')

        f = LocalFixture(tmp,'merge-wins'); f.setup()
        merge, gate = start(f,f.merge(),'staged'); op = inspect(f)['operation_id']
        f.run(abort_sql(op,options=',timeout_ms=>100'),error='merge_timeout')
        cancelled, waiting = start(f,abort_sql(op,'cancelled'),'abort_waiting'); release(waiting)
        time.sleep(.1); cancelled.send_signal(2); cancelled.communicate(timeout=10)
        # The CLI can stop with an unfinished JSON prefix on SIGINT. Assert the
        # durable outcome, not the shell's formatting or platform-specific status.
        assert inspect(f)['state'] == 'pending'
        assert f.scalar("SELECT state FROM operations WHERE request='cancelled'") == 'pending'
        abort, waiting = start(f,abort_sql(op),'abort_waiting')
        release(waiting); time.sleep(.15); assert abort.poll() is None
        release(gate); merged = result(merge); ended = result(abort)
        assert ended['state'] == 'complete' and ended['snapshot_id'] == merged['snapshot_id']
        assert f.run(abort_sql(op,'cancelled'))[-1][0]['state'] == 'complete'
        ok('abort lock timeout/cancellation is retryable; publisher wins without losing its result')

        f = LocalFixture(tmp,'two-aborts'); f.setup(); f.run(f.merge(),crash='staged')
        op = inspect(f)['operation_id']
        left, gate = start(f,abort_sql(op),'abort_ready')
        right, waiting = start(f,abort_sql(op),'abort_waiting'); release(waiting)
        release(gate); assert result(left) == result(right)
        assert f.scalar("SELECT count(*) FROM operations WHERE kind='merge_aborted'") == 1
        ok('concurrent identical aborts share one durable outcome')

        f = LocalFixture(tmp,'pagination'); f.setup()
        for i in range(5): f.run(f.merge(request=f'merge-{i}',options=',max_input_rows=>1'),error='merge_limit')
        f.run(f.merge(dest='anonymous',request='',options=',max_input_rows=>1'),error='merge_limit')
        anonymous = f.run("SELECT operation_id,request_id FROM differential_merge_requests('merge_db') WHERE destination='anonymous';")[-1][0]
        assert anonymous['request_id'] is None
        f.run(abort_sql(anonymous['operation_id']))
        page, ids = '', []
        while True:
            rows = f.run(f"SELECT operation_id FROM differential_merge_requests('merge_db',max_results=>2,after_operation_id=>'{page}');")[-1]
            if not rows: break
            ids += [r['operation_id'] for r in rows]; page = ids[-1]
        assert len(ids) == len(set(ids)) == 6 and ids == sorted(ids)
        assert f.run("SELECT count(*) AS n FROM differential_merge_requests('merge_db',request_id=>'unknown');")[-1] == [{'n':0}]
        f.run("SELECT * FROM differential_merge_requests('merge_db',max_results=>1001);",error='1..1000')
        ok('exclusive operation-id pagination and request filtering')
    import json
    evidence_path('merge-lifecycle.json').write_text(json.dumps({'passed':passed,'scope':'local disposable stores'},indent=2)+'\n')


if __name__ == '__main__': main()
