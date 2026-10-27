#!/usr/bin/env python3
"""Branch merge selection, frozen requests, retention races and publication recovery.
All repositories and subprocesses are disposable; no provider access is required.
"""
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import tempfile
import threading
import time

from logical_merge import Fixture
from merge_hardening import run_at_gate, assert_no_stage
from paths import DUCKDB_CLI, EXTENSION
from evidence import evidence_path


def preview(name, extra=''):
    return f"differential_merge_branch_preview('{name}','o','t'{extra})"


def merge(name, dest='merged', request='', extra=''):
    return f"differential_merge_branch('{name}','o','t','{dest}',request_id=>'{request}'{extra})"


def setup(f):
    return f.setup('CREATE TABLE d.x(id INT PRIMARY KEY,a INT,b VARCHAR);',
                   'INSERT INTO d.x VALUES(1,0,NULL);',
                   'UPDATE o.x SET a=1;', "UPDATE t.x SET b='new';")


def result_ok(f, name, branch='merged'):
    f.attach(name, branch)
    assert f.q("SELECT count(*) FROM m.x WHERE id=1 AND a=1 AND b='new'") == 1
    assert f.q('SELECT count(*) FROM m.x') == 1
    f.q('DETACH m')


def advance(f, name):
    f.q(f"ATTACH 'differential:{name}' AS live (BRANCH 'o'); UPDATE live.x SET a=99;"
        "CALL differential_create_snapshot('live'); DETACH live;")


def main():
    passed = []
    def ok(label): passed.append(label); print('PASS', label, flush=True)
    with tempfile.TemporaryDirectory(prefix='differential-merge-branches-') as tmp:
        root = Path(tmp); repo = root/'repo'; f = Fixture(repo)
        try:
            n = setup(f)
            before = f.q('SELECT count(*) FROM differential_operations()')
            assert f.q(f"SELECT count(*) FROM {preview(n,',validate=>true')} WHERE base_snapshot_id=1 AND ours_snapshot_id=2 AND theirs_snapshot_id=3 AND validation_state='valid'") == 1
            assert f.q('SELECT count(*) FROM differential_operations()') == before
            assert f.q(f"SELECT count(*) FROM differential_snapshots('{n}')") == 3
            assert f.q(f"SELECT count(*) FROM differential_branches('{n}')") == 3
            f.q(f"ATTACH 'differential:{n}' AS live (BRANCH 'o'); UPDATE live.x SET a=99;")
            assert f.q(f'SELECT snapshot_id FROM {merge(n,request="first")}') == 4
            result_ok(f,n)
            assert f.q('SELECT a FROM live.x') == 99
            f.q('DETACH live')
            f.q(f"SELECT * FROM differential_merge_snapshots('{n}',1,2,3,'explicit');")
            f.q(f"ATTACH 'differential:{n}' AS m (BRANCH 'merged'); ATTACH 'differential:{n}' AS e (BRANCH 'explicit');")
            assert f.q('SELECT count(*) FROM ((SELECT * FROM m.x EXCEPT ALL SELECT * FROM e.x) UNION ALL (SELECT * FROM e.x EXCEPT ALL SELECT * FROM m.x))') == 0
            f.q('DETACH m; DETACH e;')
            assert f.q(f"SELECT count(*) FROM differential_snapshots('{n}') WHERE snapshot_id=4 AND commit_extra_info LIKE '%unique-base-v1%' AND commit_extra_info LIKE '%ours_branch_id%'") == 1
            advance(f,n)
            assert f.q(f'SELECT snapshot_id FROM {merge(n,request="first")}') == 4
            assert f.q(f'SELECT ours_snapshot_id FROM {merge(n,request="first")}') == 2
            f.error(f"SELECT * FROM differential_merge_branch('{n}','t','o','merged',request_id=>'first')", 'Request ID')
            f.error(f"SELECT * FROM differential_merge_snapshots('{n}',1,2,3,'merged',request_id=>'first')", 'Request ID')
            f.error(f'SELECT * FROM {merge(n)}', 'name_conflict')
            ok('branch preview is isolated; merge equals explicit snapshots, excludes live writes and replays the original IDs after heads advance')

            frozen = setup(f)
            f.error(f'SELECT * FROM {merge(frozen,request="frozen",extra=",max_input_rows=>1")}', 'merge_limit')
            assert_no_stage(f)
            advance(f,frozen)
            # Reusing a branch name does not retarget an already resolved request.
            f.q(f"CALL differential_drop_branch('{frozen}','o'); CALL differential_create_branch('{frozen}','o',snapshot=>3);")
            assert f.q(f'SELECT ours_snapshot_id FROM {merge(frozen,request="frozen")}') == 2
            result_ok(f,frozen)
            f.q(f"CALL differential_drop_branch('{frozen}','t'); CALL differential_expire_snapshots('{frozen}',versions=>[1,2,3]);")
            assert f.q(f'SELECT ours_snapshot_id FROM {merge(frozen,request="frozen")}') == 2
            f.error(f'SELECT * FROM {merge(frozen,dest="changed",request="frozen")}', 'Request ID')
            ok('pending selection survives head movement and branch-name reuse; completed replay survives source deletion and expiry')

            expired = setup(f)
            f.error(f'SELECT * FROM {merge(expired,request="expired",extra=",max_input_rows=>1")}', 'merge_limit')
            f.q(f"CALL differential_expire_snapshots('{expired}',versions=>[1]);")
            f.error(f'SELECT * FROM {merge(expired,request="expired")}', 'retired')
            f.error(f'SELECT * FROM {preview(expired)}', 'retired')
            assert f.q(f"SELECT count(*) FROM differential_branches('{expired}') WHERE branch_name='merged'") == 0
            head_expired = setup(f)
            f.q(f"CALL differential_expire_snapshots('{head_expired}',versions=>[2]);")
            f.error(f'SELECT * FROM {preview(head_expired)}', 'retired')
            ok('retired base/head and expired pending inputs fail without substitution or completed output')

            scope = setup(f)
            assert f.q(f"SELECT base_snapshot_id FROM differential_merge_branch_preview('{scope}','main','o')") == 1
            f.error(f"SELECT * FROM differential_merge_branch_preview('{scope}','o','o')", 'merge_base_unavailable')
            f.error(f"SELECT * FROM differential_merge_branch_preview('{scope}','missing','t')", 'not_found')
            f.q(f"ATTACH 'differential:{scope}' AS source; CALL differential_create_snapshot('source'); DETACH source;")
            assert f.q(f"SELECT count(DISTINCT root) FROM differential_snapshots('{scope}') WHERE snapshot_id IN (1,4)") == 1
            f.q(f"CALL differential_create_branch('{scope}','later',snapshot=>4);")
            assert f.q(f"SELECT base_snapshot_id FROM differential_merge_branch_preview('{scope}','o','later')") == 1
            f.q(f"SELECT * FROM {merge(scope)}; CALL differential_create_branch('{scope}','from_ours',snapshot=>2);")
            assert f.q(f"SELECT base_snapshot_id FROM differential_merge_branch_preview('{scope}','merged','from_ours')") == 2
            f.q(f"CALL differential_restore('{scope}',1,branch=>'o');")
            assert f.q(f'SELECT base_snapshot_id FROM {preview(scope)}') == 1
            # An unchanged sibling's head can still be the fork snapshot, owned by main.
            fresh = setup(f)
            f.q(f"CALL differential_create_branch('{fresh}','unchanged',snapshot=>1);")
            assert f.q(f"SELECT theirs_snapshot_id FROM differential_merge_branch_preview('{fresh}','o','unchanged')") == 1
            # Fresh direct forks of a completed merge snapshot are supported.
            sid = f.q(f'SELECT snapshot_id FROM {merge(fresh)}')
            f.q(f"CALL differential_create_branch('{fresh}','next_o',snapshot=>{sid}); CALL differential_create_branch('{fresh}','next_t',snapshot=>{sid});")
            assert f.q(f"SELECT base_snapshot_id FROM differential_merge_branch('{fresh}','next_o','next_t','next')") == sid
            ok('explicit parent edges support parent/child, different forks, restores and merge results without treating equal roots as ancestry')

            conflict = f.setup('CREATE TABLE d.x(id INT PRIMARY KEY,a INT);','INSERT INTO d.x VALUES(1,0);','UPDATE o.x SET a=1;','UPDATE t.x SET a=2;')
            assert f.q(f'SELECT conflicts FROM {preview(conflict)}') == 1
            f.error(f'SELECT * FROM {merge(conflict)}','merge_conflict')
            constraint = f.setup('CREATE TABLE d.x(id INT PRIMARY KEY,a INT UNIQUE);','INSERT INTO d.x VALUES(1,1),(2,2);','UPDATE o.x SET a=3 WHERE id=1;','UPDATE t.x SET a=3 WHERE id=2;')
            assert f.q(f"SELECT count(*) FROM {preview(constraint,',validate=>true')} WHERE validation_state='constraint_violation'") == 1
            f.error(f'SELECT * FROM {merge(constraint)}','merge_constraint')
            f.q("ATTACH 'differential:remote' AS remote (DATA_PATH 's3://merge-unsupported/disposable/');")
            f.error("SELECT * FROM differential_merge_branch('remote','o','t','merged')",'merge_request_required')
            f.error("SELECT * FROM differential_merge_branch_preview('remote','o','t')",'not_found')
            f.q('DETACH remote')
            ok('branch API preserves conflict/constraint errors; S3 requires a request and valid registered branches')

            pinned = setup(f)
            def while_pinned(*_):
                advance(f,pinned)
                f.q(f"CALL differential_expire_snapshots('{pinned}',versions=>[1,2,3]); CALL differential_cleanup_old_files(max_objects=>10000);")
            answers,errors = run_at_gate(f,root,'pinned',f'SELECT ours_snapshot_id FROM {merge(pinned,request="pinned")}',while_pinned)
            assert answers == [2] and not errors, errors
            result_ok(f,pinned); assert_no_stage(f)
            ok('resolution stays fixed while heads advance and pinned inputs expire during GC')

            concurrent = setup(f)
            retry_answers, retry_errors, retries = [], [], []
            def same_request(*_):
                advance(f,concurrent)
                os.environ.pop('DIFFERENTIAL_MERGE_PHASE_GATE')
                connection = f.host.connect()
                def retry():
                    try: retry_answers.append(f.host.query(connection,f'SELECT ours_snapshot_id FROM {merge(concurrent,request="concurrent")}'))
                    except RuntimeError as error: retry_errors.append(str(error))
                thread = threading.Thread(target=retry); retries.append(thread); thread.start()
                deadline = time.monotonic()+20
                while f.q("SELECT count(*) FROM differential_merge_status() WHERE phase='request_waiting'") != 1:
                    assert thread.is_alive() and time.monotonic()<deadline, retry_errors
                    time.sleep(.01)
            answers,errors = run_at_gate(f,root,'pinned',f'SELECT ours_snapshot_id FROM {merge(concurrent,request="concurrent")}',same_request)
            for thread in retries: thread.join(timeout=30)
            assert answers == [2] and not errors, errors
            assert retry_answers == [2] and not retry_errors and not any(t.is_alive() for t in retries), retry_errors
            result_ok(f,concurrent); assert_no_stage(f)
            assert f.q("SELECT count(*) FROM differential_operations() WHERE request_id='concurrent' AND state='complete'") == 1
            assert f.q(f"SELECT count(*) FROM differential_snapshots('{concurrent}')") == 5
            ok('overlapping same-request calls converge on one result and the original selection after a head advances')
        finally:
            f.host.close()

        # Versioned branch requests reuse the existing extensible operation payload.
        with sqlite3.connect(repo/'engine.sqlite') as db:
            payload = db.execute("SELECT payload FROM operations WHERE request='first'").fetchone()[0]
            db.execute("UPDATE operations SET payload=? WHERE request='first'",(payload.replace('unique-base-v1','unique-base-v9').replace('logical-cell-v1','logical-cell-v9'),))
            payload = db.execute("SELECT payload FROM operations WHERE request='expired'").fetchone()[0]
            db.execute("UPDATE operations SET payload=? WHERE request='expired'",(payload.replace('unique-base-v1','unique-base-v9'),))
            db.execute("UPDATE databases SET writers='shared' WHERE name=?",(scope,))
        f = Fixture(repo)
        try:
            assert f.q(f'SELECT snapshot_id FROM {merge(n,request="first")}') == 4
            f.error(f'SELECT * FROM {merge(expired,request="expired")}', 'merge_unsupported')
            f.error(f'SELECT * FROM {preview(scope)}', 'merge_unsupported')
        finally: f.host.close()
        ok('completed branch requests replay across algorithm versions; unavailable pending versions and shared writers fail explicitly')

        for kind in ('restore','drop'):
            folder = root/('pending-'+kind)
            f = Fixture(folder)
            try: name = setup(f)
            finally: f.host.close()
            prefix = f"LOAD '{EXTENSION}'; SET differential_repository='{folder}'; SET threads=2;"
            action = (f"CALL differential_restore('{name}',1,branch=>'o',request_id=>'lifecycle');" if kind=='restore'
                      else f"CALL differential_drop_branch('{name}','o',request_id=>'lifecycle');")
            p = subprocess.run([DUCKDB_CLI,'-init','/dev/null','-unsigned','-batch','-bail'],input=prefix+action,
                               text=True,capture_output=True,timeout=60,
                               env=dict(os.environ,DIFFERENTIAL_CONTROL_CRASH='compound_intent'))
            assert p.returncode == 86, (kind,p.returncode,p.stdout,p.stderr)
            with sqlite3.connect(folder/'engine.sqlite') as db:
                assert db.execute('SELECT count(*) FROM compound_intents').fetchone()[0] == 1
            f = Fixture(folder)
            try:
                f.error(f'SELECT * FROM {preview(name)}','merge_base_unavailable')
                f.error(f'SELECT * FROM {merge(name,request="pending-lifecycle")}','merge_base_unavailable')
                assert f.q("SELECT count(*) FROM differential_operations() WHERE kind='merge_branch'") == 0
                # A pending branch transition does not make the immutable snapshots ambiguous.
                f.q(f"SELECT * FROM differential_merge_snapshots('{name}',1,2,3,'explicit');")
                result_ok(f,name,'explicit')
                f.q(action)
            finally: f.host.close()
            ok('pending '+kind+' intent prevents automatic selection; explicit immutable inputs remain usable')

        for point in ('resolved','staged','ancestry_recorded','publication_transaction','published'):
            folder = root/point
            prefix = f"LOAD '{EXTENSION}'; SET differential_repository='{folder}'; SET threads=2;"
            def run(sql,crash=None):
                env = dict(os.environ)
                if crash: env['DIFFERENTIAL_MERGE_CRASH'] = crash
                p = subprocess.run([DUCKDB_CLI,'-init','/dev/null','-unsigned','-batch','-bail','-json'],input=prefix+sql,
                                   text=True,capture_output=True,env=env,timeout=60)
                assert p.returncode == (86 if crash else 0),(point,p.returncode,p.stdout,p.stderr)
                return p.stdout
            run("ATTACH 'differential:crash' AS d; CREATE TABLE d.x(id INT PRIMARY KEY,a INT,b INT); INSERT INTO d.x VALUES(1,0,0);"
                "CALL differential_create_snapshot('d'); CALL differential_create_branch('d','o',snapshot=>1); CALL differential_create_branch('d','t',snapshot=>1);"
                "ATTACH 'differential:crash' AS o (BRANCH 'o'); UPDATE o.x SET a=1; CALL differential_create_snapshot('o');"
                "ATTACH 'differential:crash' AS t (BRANCH 't'); UPDATE t.x SET b=2; CALL differential_create_snapshot('t');")
            sql = f"SELECT snapshot_id,base_snapshot_id,ours_snapshot_id,theirs_snapshot_id FROM {merge('crash',request='crash')};"
            run(sql,point)
            with sqlite3.connect(folder/'engine.sqlite') as db:
                assert db.execute("SELECT count(*) FROM branches WHERE name='merged'").fetchone()[0] == (1 if point=='published' else 0)
                assert db.execute("SELECT count(*) FROM operations WHERE kind='merge_branch' AND state='complete'").fetchone()[0] == (1 if point=='published' else 0)
                assert db.execute("SELECT count(*) FROM operations WHERE kind='snapshot_ancestry'").fetchone()[0] == (4 if point=='published' else 3)
            # Retry must use the selection durably recorded before the crash.
            run("ATTACH 'differential:crash' AS o (BRANCH 'o'); UPDATE o.x SET a=99; CALL differential_create_snapshot('o');")
            result = run(sql); assert result == run(sql)
            answer = json.loads(result)[0]
            assert (answer['base_snapshot_id'],answer['ours_snapshot_id'],answer['theirs_snapshot_id']) == (1,2,3)
            assert json.loads(run("ATTACH 'differential:crash' AS m (BRANCH 'merged'); SELECT * FROM m.x;")) == [{'id':1,'a':1,'b':2}]
            run('CALL differential_cleanup_old_files(max_objects=>10000);')
            with sqlite3.connect(folder/'engine.sqlite') as db:
                assert db.execute('PRAGMA integrity_check').fetchone()[0] == 'ok'
                assert db.execute("SELECT count(*) FROM files WHERE name LIKE 'merge_stage/%'").fetchone()[0] == 0
                assert db.execute("SELECT count(*) FROM operations WHERE kind='merge_branch' AND state='complete'").fetchone()[0] == 1
                assert db.execute("SELECT count(*) FROM operations WHERE kind='snapshot_ancestry'").fetchone()[0] == 5
            ok('crash, moved head, frozen-request replay and GC at '+point)
    evidence_path('merge-branches.json').write_text(json.dumps({'passed':passed,'scope':'local ancestry selection and frozen requests; no physical power-loss qualification'},indent=2)+'\n')


if __name__=='__main__': main()
