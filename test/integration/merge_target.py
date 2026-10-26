#!/usr/bin/env python3
"""Existing-target local merges on the stock host. All repositories are disposable."""
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time

from logical_merge import Fixture
from merge_hardening import run_at_gate, assert_no_stage
from paths import DUCKDB_CLI, EXTENSION
from evidence import evidence_path


def setup(f, ours='UPDATE o.x SET a=1;', theirs="UPDATE t.x SET b='source';", ddl=None, initial=None):
    return f.setup(ddl or 'CREATE TABLE d.x(id INT PRIMARY KEY,a INT,b VARCHAR);',
                   initial or "INSERT INTO d.x VALUES(1,0,'base'),(2,0,NULL);", ours, theirs)


def merge(n, request='', target='o', source='t', extra=''):
    return f"differential_merge_branch('{n}','{target}','{source}','{target}',request_id=>'{request}'{extra})"


def head(f, n, target='o'):
    return f.q(f"SELECT head_snapshot_id FROM differential_branches('{n}') WHERE branch_name='{target}' AND state='active'")


def check(f, n, a=1, b='source', target='o'):
    f.q(f"ATTACH 'differential:{n}' AS check_target (BRANCH '{target}');")
    try:
        assert f.q(f"SELECT count(*) FROM check_target.x WHERE id=1 AND a={a} AND b='{b}'") == 1
    finally: f.q('DETACH check_target')


def main():
    passed = []
    def ok(label): passed.append(label); print('PASS', label, flush=True)
    with tempfile.TemporaryDirectory(prefix='differential-merge-target-') as tmp:
        root = Path(tmp); repo = root/'repo'; f = Fixture(repo)
        try:
            n = setup(f)
            # Arbitrary names, source still attached, immutable target reader survives.
            f.q(f"CALL differential_create_branch('{n}','release',snapshot=>2); CALL differential_create_branch('{n}','feature',snapshot=>3);")
            f.q(f"ATTACH 'differential:{n}' AS source (BRANCH 'feature'); ATTACH 'differential:{n}' AS old (SNAPSHOT_VERSION 2,READ_ONLY);")
            sql = merge(n,'first','release','feature')
            before = f.q('SELECT count(*) FROM differential_operations()')
            f.q(f'PREPARE later AS SELECT snapshot_id FROM {sql}; EXPLAIN SELECT * FROM {sql};')
            assert f.q('SELECT count(*) FROM differential_operations()') == before
            f.q(f"CREATE TEMP TABLE identity_before AS SELECT branch_id FROM differential_branches('{n}') WHERE branch_name='release';")
            f.q('SET differential_snapshot_on_checkpoint=true;')
            assert f.q('EXECUTE later') == 4
            f.q('SET differential_snapshot_on_checkpoint=false;')
            assert f.q(f"SELECT count(*) FROM {sql} WHERE merge_action='merged' AND base_snapshot_id=1 AND ours_snapshot_id=2 AND theirs_snapshot_id=3") == 1
            assert f.q(f"SELECT count(*) FROM differential_branches('{n}') JOIN identity_before USING(branch_id) WHERE branch_name='release' AND head_snapshot_id=4") == 1
            assert f.q("SELECT count(*) FROM source.x WHERE id=1 AND a=0 AND b='source'") == 1
            f.q(f"CALL differential_expire_snapshots('{n}',versions=>[2]); CALL differential_cleanup_old_files(cleanup_all=>true);")
            assert f.q("SELECT count(*) FROM old.x WHERE id=1 AND a=1 AND b='base'") == 1
            check(f,n,target='release')
            assert f.q(f"SELECT base_snapshot_id FROM differential_merge_branch_preview('{n}','release','feature')") == 3
            assert f.q(f"SELECT snapshot_id FROM {merge(n,'again','release','feature')} WHERE merge_action='already_up_to_date'") == 4
            assert f.q(f"SELECT count(*) FROM differential_snapshots('{n}')") == 4 # retired snapshots remain listed
            f.q("UPDATE source.x SET b='next'; CALL differential_create_snapshot('source');")
            assert f.q(f"SELECT snapshot_id FROM {merge(n,'next','release','feature')} WHERE base_snapshot_id=3 AND merge_action='merged'") == 6
            check(f,n,b='next',target='release')
            assert f.q(f'SELECT snapshot_id FROM {sql}') == 4
            assert head(f,n,'release') == 6
            f.error(f"SELECT * FROM {merge(n,'first','release','t')}", 'Request ID')
            f.q('DETACH source; DETACH old;')
            assert_no_stage(f)
            ok('arbitrary target keeps identity; two parents, repeated merges, no-op, source attachment, old reader/GC and completed replay')

            ff = setup(f)
            f.q(f"CALL differential_create_branch('{ff}','unchanged',snapshot=>1);")
            assert f.q(f"SELECT snapshot_id FROM {merge(ff,'ff','unchanged','t')} WHERE merge_action='fast_forward'") == 3
            assert head(f,ff,'unchanged') == 3
            check(f,ff,a=0,target='unchanged')
            assert f.q(f"SELECT count(*) FROM differential_snapshots('{ff}')") == 3
            assert f.q(f"SELECT snapshot_id FROM {merge(ff,'ff-noop','unchanged','t')} WHERE merge_action='already_up_to_date'") == 3
            assert f.q(f"SELECT count(*) FROM differential_merge_requests('{ff}',request_id=>'ff') WHERE merge_action='fast_forward' AND state='complete'") == 1
            f.error(f"SELECT * FROM differential_merge_snapshots('{ff}',1,2,3,'o')",'name_conflict')
            f.error(f"SELECT * FROM differential_merge_branch('{ff}','o','t','main')",'name_conflict')
            ok('fast-forward reuses the source snapshot; equal heads are no-op; other existing destinations remain errors')

            busy = setup(f)
            f.q(f"ATTACH 'differential:{busy}' AS live (BRANCH 'o');")
            f.error(f'SELECT * FROM {merge(busy,"busy")}', 'active_attachment_or_transaction')
            other = f.host.connect(); f.host.query(other,'BEGIN; SELECT count(*) FROM live.x;')
            f.q('DETACH live;')
            f.error(f'SELECT * FROM {merge(busy,"busy")}', 'active_attachment_or_transaction')
            assert f.host.query(other,"SELECT count(*) FROM live.x WHERE a=1") == 2
            f.host.query(other,'COMMIT;')
            assert f.q(f'SELECT snapshot_id FROM {merge(busy,"busy")}') == 4
            check(f,busy)
            ok('active target and detached-but-held transaction block replacement; releasing ownership allows the same request')

            dirty = setup(f)
            f.q(f"ATTACH 'differential:{dirty}' AS live (BRANCH 'o'); INSERT INTO live.x VALUES(9,9,'unsnapshotted'); DETACH live;")
            f.error(f'SELECT * FROM {merge(dirty,"dirty")}', 'merge_target_dirty')
            assert head(f,dirty) == 2
            assert f.q(f"SELECT count(*) FROM differential_merge_requests('{dirty}') WHERE state='pending' AND NOT destination_reserved") == 1
            f.q(f"ATTACH 'differential:{dirty}' AS live (BRANCH 'o');")
            assert f.q("SELECT count(*) FROM live.x WHERE id=9 AND a=9") == 1
            f.q("CALL differential_create_snapshot('live'); DETACH live;")
            f.error(f'SELECT * FROM {merge(dirty,"dirty")}', 'merge_target_changed')
            assert f.q(f'SELECT snapshot_id FROM {merge(dirty,"clean")}') == 5
            f.q(f"ATTACH 'differential:{dirty}' AS live (BRANCH 'o');")
            assert f.q('SELECT count(*) FROM live.x WHERE id=9') == 1
            f.q('DETACH live;')
            ok('unsnapshotted committed data survives rejection; fresh snapshot requires a new frozen request')

            for label, ours, theirs, ddl, initial, error in [
                ('data','UPDATE o.x SET a=1;','UPDATE t.x SET a=2;',None,None,'merge_conflict'),
                ('constraint','UPDATE o.x SET a=3 WHERE id=1;','UPDATE t.x SET a=3 WHERE id=2;',
                 'CREATE TABLE d.x(id INT PRIMARY KEY,a INT UNIQUE,b VARCHAR);',"INSERT INTO d.x VALUES(1,1,'base'),(2,2,NULL);",'merge_constraint'),
                ('schema','ALTER TABLE o.x ADD COLUMN c INT;','SELECT 1;',None,None,'merge_unsupported')]:
                bad = setup(f,ours,theirs,ddl,initial)
                f.error(f'SELECT * FROM {merge(bad,label)}',error)
                assert head(f,bad) == 2
                assert f.q(f"SELECT count(*) FROM differential_snapshots('{bad}')") == 3
                f.q(f"ATTACH 'differential:{bad}' AS reusable (BRANCH 'o'); DETACH reusable;")
                assert_no_stage(f)
            ok('data conflict, final constraint and schema failures preserve target and release its fence')

            order = setup(f,
                'UPDATE o.x SET b=1 WHERE id=0;',
                'DELETE FROM t.x; INSERT INTO t.x SELECT i,(i+1)%3000,0 FROM range(3000) r(i);',
                'CREATE TABLE d.x(id INT PRIMARY KEY,a INT UNIQUE,b INT);',
                'INSERT INTO d.x SELECT i,i,0 FROM range(3000) r(i);')
            assert f.q(f'SELECT snapshot_id FROM {merge(order,"order")}') == 4
            f.q(f"ATTACH 'differential:{order}' AS verify (BRANCH 'o');")
            assert f.q('SELECT count(*) FROM verify.x WHERE a=(id+1)%3000 AND b=CASE WHEN id=0 THEN 1 ELSE 0 END') == 3000
            f.q('DETACH verify;')
            ok('native constrained table rebuild handles a 3000-row UNIQUE permutation in the target')

            catalog = setup(f,
                'UPDATE o.x SET a=7;',
                'DROP VIEW t.visible; DROP TABLE t.obsolete; ALTER TABLE t.x DROP COLUMN b; USE t; CREATE VIEW visible AS SELECT id,a FROM x; USE memory;',
                'CREATE TABLE d.x(id INT PRIMARY KEY,a INT,b VARCHAR); CREATE TABLE d.obsolete(id INT PRIMARY KEY); USE d; CREATE VIEW visible AS SELECT id,a,b FROM x; USE memory;',
                "INSERT INTO d.x VALUES(1,0,'base'); INSERT INTO d.obsolete VALUES(1);")
            catalog_sql = merge(catalog,"catalog",extra=",schema_mode=>'catalog'")
            assert f.q(f'SELECT snapshot_id FROM {catalog_sql}') == 4
            f.q(f"ATTACH 'differential:{catalog}' AS verify (BRANCH 'o');")
            assert f.q('SELECT count(*) FROM verify.visible WHERE a=7') == 1
            assert f.q("SELECT count(*) FROM duckdb_columns() WHERE database_name='verify' AND table_name='x'") == 2
            assert f.q("SELECT count(*) FROM duckdb_tables() WHERE database_name='verify' AND table_name='obsolete'") == 0
            f.q('DETACH verify;')
            ok('catalog table/column removal and view replacement compose with independent data updates in target')

            cancel = setup(f)
            def interrupt(connection, *_):
                assert f.q(f"SELECT count(*) FROM differential_merge_requests('{cancel}') WHERE state='target_locked' AND destination_reserved") == 1
                f.error(f"ATTACH 'differential:{cancel}' AS blocked (BRANCH 'o')",'writer_busy')
                f.error(f"CALL differential_restore('{cancel}',1,branch=>'o')",'active_attachment_or_transaction')
                f.host.lib.duckdb_interrupt(connection)
            answers,errors = run_at_gate(f,root,'staged',f'SELECT snapshot_id FROM {merge(cancel,"cancel")}',interrupt)
            assert not answers and errors and 'interrupt' in errors[0].lower(),errors
            assert head(f,cancel) == 2
            f.q(f"ATTACH 'differential:{cancel}' AS reusable (BRANCH 'o'); DETACH reusable;")
            assert f.q(f'SELECT snapshot_id FROM {merge(cancel,"cancel")}') == 4
            assert_no_stage(f)
            ok('cancellation releases target fence; writers and restore cannot enter while result is staged; retry succeeds')

            overlap = setup(f)
            retries, answers2, errors2 = [], [], []
            def competing(*_):
                os.environ.pop('DIFFERENTIAL_MERGE_PHASE_GATE')
                conn = f.host.connect()
                def retry():
                    try: answers2.append(f.host.query(conn,f'SELECT snapshot_id FROM {merge(overlap,"same")}'))
                    except RuntimeError as exc: errors2.append(str(exc))
                thread=threading.Thread(target=retry); retries.append(thread); thread.start()
                until=time.monotonic()+20
                while f.q("SELECT count(*) FROM differential_merge_status() WHERE phase='request_waiting'") != 1:
                    assert thread.is_alive() and time.monotonic()<until,errors2
                    time.sleep(.01)
                f.error(f'SELECT * FROM {merge(overlap,"different")}', 'merge_base_unavailable')
            answers,errors=run_at_gate(f,root,'staged',f'SELECT snapshot_id FROM {merge(overlap,"same")}',competing)
            for t in retries: t.join(30)
            assert answers == [4] and answers2 == [4] and not errors and not errors2,(answers,answers2,errors,errors2)
            assert f.q(f"SELECT count(*) FROM differential_snapshots('{overlap}')") == 4
            ok('concurrent identical requests return one result; competing requests cannot bypass the target intent')

            stale = setup(f)
            def move_before_ownership(*_):
                f.q(f"ATTACH 'differential:{stale}' AS live (BRANCH 'o'); UPDATE live.x SET a=8; CALL differential_create_snapshot('live'); DETACH live;")
            answers,errors=run_at_gate(f,root,'request_locked',f'SELECT snapshot_id FROM {merge(stale,"stale")}',move_before_ownership)
            assert not answers and len(errors)==1 and 'merge_target_changed' in errors[0],errors
            assert head(f,stale) == 4
            assert_no_stage(f)
            ok('target head movement between selection and ownership is rejected without losing the new data')

            resolved = setup(f,'UPDATE o.x SET a=1;',"UPDATE t.x SET a=2,b='chosen';")
            f.q("CREATE TABLE fixes(id INT,a INT,b VARCHAR,_merge_action VARCHAR,_merge_columns MAP(VARCHAR,VARCHAR));"
                "INSERT INTO fixes VALUES(1,42,NULL,'cells',map{'a':'custom'}),(2,43,NULL,'cells',map{'a':'custom'});")
            opts=",resolution_tables=>map{'main.x':'memory.main.fixes'}"
            assert f.q(f'SELECT snapshot_id FROM {merge(resolved,"resolved",extra=opts)}') == 4
            check(f,resolved,a=42,b='chosen')
            f.q('DROP TABLE fixes;')
            assert f.q(f'SELECT snapshot_id FROM {merge(resolved,"resolved",extra=opts)}') == 4
            ok('typed conflict decisions compose with source edits; completed retry needs no resolution table')

            reused = setup(f)
            f.error(f'SELECT * FROM {merge(reused,"reused",extra=",max_input_rows=>1")}', 'merge_limit')
            f.q(f"CALL differential_drop_branch('{reused}','o'); CALL differential_create_branch('{reused}','o',snapshot=>2);")
            f.error(f'SELECT * FROM {merge(reused,"reused")}', 'merge_target_changed')
            assert f.q(f'SELECT snapshot_id FROM {merge(reused,"replacement")}') == 4
            ok('pending target request never follows a deleted-and-reused branch name')

            completed = setup(f)
            assert f.q(f'SELECT snapshot_id FROM {merge(completed,"completed")}')==4
            f.q(f"CALL differential_drop_branch('{completed}','o'); CALL differential_create_branch('{completed}','o',snapshot=>1);"
                f"CALL differential_expire_snapshots('{completed}',versions=>[2,3,4]); CALL differential_cleanup_old_files(cleanup_all=>true);")
            assert f.q(f'SELECT snapshot_id FROM {merge(completed,"completed")}')==4
            assert head(f,completed)==1
            check(f,completed,a=0,b='base')
            ok('completed retry survives result/input expiry and target name reuse without replacing the new branch')

            os.environ['DIFFERENTIAL_MERGE_LOCAL_POOL']=str(root/'local-payloads')
            try: pooled=setup(f)
            finally: os.environ.pop('DIFFERENTIAL_MERGE_LOCAL_POOL')
            assert f.q(f'SELECT snapshot_id FROM {merge(pooled,"pooled")}')==4
            check(f,pooled)
            f.q('CALL differential_cleanup_old_files(cleanup_all=>true);')
            check(f,pooled)
            ok('target namespace replacement preserves separate local payload-pool placement across GC/reopen')

            cross=setup(f)
            code=("import sys;sys.path.insert(0,'test/integration');from logical_merge import Fixture;"
                  f"f=Fixture({str(repo)!r});f.q(\"ATTACH 'differential:{cross}' AS held (BRANCH 'o');\");"
                  "print('ready',flush=True);sys.stdin.readline();f.host.close()")
            holder=subprocess.Popen([sys.executable,'-c',code],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
            try:
                assert holder.stdout.readline().strip()=='ready'
                f.error(f'SELECT * FROM {merge(cross,"cross")}', 'active_attachment_or_transaction')
            finally:
                holder.communicate('release\n',timeout=30)
            assert holder.returncode==0
            assert f.q(f'SELECT snapshot_id FROM {merge(cross,"cross")}')==4
            ok('target lease excludes a live native attachment in another process')

            retire_ff=setup(f)
            f.q(f"CALL differential_create_branch('{retire_ff}','unchanged',snapshot=>1);")
            def expire_source(*_):
                f.q(f"CALL differential_expire_snapshots('{retire_ff}',versions=>[3]); CALL differential_cleanup_old_files(cleanup_all=>true);")
            answers,errors=run_at_gate(f,root,'publishing',f'SELECT snapshot_id FROM {merge(retire_ff,"retire-ff","unchanged","t")}',expire_source)
            assert not answers and len(errors)==1 and 'retired' in errors[0],errors
            assert head(f,retire_ff,'unchanged')==1
            f.q(f"ATTACH 'differential:{retire_ff}' AS usable (BRANCH 'unchanged'); DETACH usable;")
            ok('fast-forward refuses a source retired during comparison; transaction rollback preserves old target mapping')

            remote = 'remote_scope'
            f.q(f"ATTACH 'differential:{remote}' AS remote (DATA_PATH 's3://merge-unsupported/disposable/');")
            f.error(f"SELECT * FROM {merge(remote,'remote','main','feature')}",'not_found')
            f.q('DETACH remote;')
            ok('existing-target API rejects S3 before selecting heads or creating a destination')
        finally: f.host.close()

        with sqlite3.connect(repo/'engine.sqlite') as db:
            def fields(payload):
                payload=payload.encode(); out=[]
                while payload:
                    length,rest=payload.split(b':',1); size=int(length)
                    out.append(rest[:size].decode()); payload=rest[size:]
                return out
            audits=db.execute("SELECT payload,database_id,branch_id,snapshot_id FROM operations WHERE kind='merge_target_result'").fetchall()
            assert audits
            for payload,database,branch,snapshot in audits:
                version,op_id,action,encoded=fields(payload); provenance=json.loads(encoded)
                assert version=='local-merge-target-v1' and provenance['operation_id']==op_id
                assert provenance['algorithm']=='logical-cell-v1'
                original=db.execute('SELECT state,branch_id,snapshot_id FROM operations WHERE id=?',(op_id,)).fetchone()
                assert original==('complete',branch,snapshot)
                assert action in ('merged','fast_forward','already_up_to_date')
                for name in ('base','ours','theirs'):
                    value=provenance[name]
                    assert db.execute('SELECT root FROM snapshots WHERE database_id=? AND id=?',(database,value['snapshot'])).fetchone()==(value['root'],)
            assert db.execute('SELECT count(*) FROM compound_intents').fetchone()[0]==0
        ok('atomic target audit companions preserve all input IDs/roots and request/result identities without dangling intents')

        # Inspect only closed disposable repositories. Test real process death on
        # both sides of the FULL publication transaction, including inode install.
        for point in ('resolved','target_locked','target_checked','resolutions_frozen','staged',
                      'target_installed','ancestry_recorded','publication_transaction','published'):
            folder=root/('crash-'+point); f=Fixture(folder)
            try: n=setup(f)
            finally: f.host.close()
            prefix=f"LOAD '{EXTENSION}'; SET differential_repository='{folder}'; SET threads=2;"
            sql=f'SELECT snapshot_id,merge_action FROM {merge(n,"crash")};'
            def run(statement, crash=None):
                env=dict(os.environ)
                if crash: env['DIFFERENTIAL_MERGE_CRASH']=crash
                p=subprocess.run([DUCKDB_CLI,'-init','/dev/null','-unsigned','-batch','-bail','-json'],
                    input=prefix+statement,text=True,capture_output=True,timeout=60,env=env)
                assert p.returncode==(86 if crash else 0),(point,p.returncode,p.stdout,p.stderr)
                return p.stdout
            run(sql,point)
            with sqlite3.connect(folder/'engine.sqlite') as db:
                completed=point=='published'
                assert db.execute("SELECT head FROM branches WHERE name='o'").fetchone()[0] == (4 if completed else 2)
                assert db.execute("SELECT count(*) FROM operations WHERE kind='merge_target_result'").fetchone()[0] == int(completed)
                assert db.execute("SELECT count(*) FROM operations WHERE kind='merge_branch' AND state='complete'").fetchone()[0] == int(completed)
                assert db.execute('SELECT count(*) FROM compound_intents').fetchone()[0] == int(point not in ('resolved','published'))
            assert json.loads(run(sql)) == [{'snapshot_id':4,'merge_action':'merged'}]
            assert json.loads(run(sql)) == [{'snapshot_id':4,'merge_action':'merged'}]
            run(f"ATTACH 'differential:{n}' AS m (BRANCH 'o'); SELECT CASE WHEN count(*)=2 AND min(a)=1 AND min(b)='source' THEN true ELSE error('wrong merged rows') END FROM m.x; DETACH m; CALL differential_cleanup_old_files(cleanup_all=>true);")
            with sqlite3.connect(folder/'engine.sqlite') as db:
                assert db.execute('PRAGMA integrity_check').fetchone()[0]=='ok'
                assert db.execute('SELECT count(*) FROM compound_intents').fetchone()[0]==0
                assert db.execute("SELECT count(*) FROM files WHERE name LIKE 'merge_stage/%'").fetchone()[0]==0
                assert db.execute("SELECT count(*) FROM operations WHERE kind='merge_target_result'").fetchone()[0]==1
                assert db.execute("SELECT count(*) FROM operations WHERE kind='snapshot_ancestry'").fetchone()[0]==4
            ok('process exit at '+point+' is atomic; retry installs exactly one result and preserves ancestry')

        for action in ('fast_forward','already_up_to_date'):
            for phase in ('publication_transaction','published'):
                folder=root/(action+'-'+phase); f=Fixture(folder)
                try:
                    n=setup(f)
                    fork=1 if action=='fast_forward' else 3
                    f.q(f"CALL differential_create_branch('{n}','target',snapshot=>{fork});")
                finally: f.host.close()
                prefix=f"LOAD '{EXTENSION}'; SET differential_repository='{folder}'; SET threads=2;"
                sql=f'SELECT snapshot_id,merge_action FROM {merge(n,"ancestor","target","t")};'
                def run_ancestor(crash=None):
                    env=dict(os.environ)
                    if crash: env['DIFFERENTIAL_MERGE_CRASH']=crash
                    child=subprocess.run([DUCKDB_CLI,'-init','/dev/null','-unsigned','-batch','-bail','-json'],
                        input=prefix+sql,text=True,capture_output=True,timeout=60,env=env)
                    assert child.returncode==(86 if crash else 0),(child.stdout,child.stderr)
                    return child.stdout
                run_ancestor(phase)
                with sqlite3.connect(folder/'engine.sqlite') as db:
                    assert db.execute("SELECT head FROM branches WHERE name='target'").fetchone()[0] == (3 if phase=='published' else fork)
                    assert db.execute('SELECT count(*) FROM snapshots').fetchone()[0]==3
                assert json.loads(run_ancestor())==[{'snapshot_id':3,'merge_action':action}]
                assert json.loads(run_ancestor())==[{'snapshot_id':3,'merge_action':action}]
                with sqlite3.connect(folder/'engine.sqlite') as db:
                    assert db.execute('SELECT count(*) FROM snapshots').fetchone()[0]==3
                    assert db.execute("SELECT count(*) FROM operations WHERE kind='merge_target_result'").fetchone()[0]==1
                ok(action+' publication crash at '+phase+' replays once without allocating a snapshot')

        for phase in ('target_locked','staged'):
            folder=root/('kill-'+phase); f=Fixture(folder)
            try: n=setup(f)
            finally: f.host.close()
            gate=root/('kill-gate-'+phase)
            prefix=f"LOAD '{EXTENSION}'; SET differential_repository='{folder}'; SET threads=2;"
            child=subprocess.Popen([DUCKDB_CLI,'-init','/dev/null','-unsigned','-batch','-bail'],stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,
                env=dict(os.environ,DIFFERENTIAL_MERGE_PHASE_GATE=phase+':'+str(gate)))
            child.stdin.write(prefix+f'SELECT * FROM {merge(n,"killed")};\n'); child.stdin.close()
            try:
                until=time.monotonic()+20
                while not Path(str(gate)+'.entered').exists():
                    assert child.poll() is None and time.monotonic()<until
                    time.sleep(.01)
            finally: child.kill(); child.wait(timeout=20)
            assert child.returncode == -9
            with sqlite3.connect(folder/'engine.sqlite') as db:
                op=db.execute("SELECT id FROM operations WHERE request='killed'").fetchone()[0]
            f=Fixture(folder)
            try:
                assert f.q(f"SELECT count(*) FROM differential_merge_requests('{n}',request_id=>'killed') WHERE state='target_locked'")==1
                f.error(f"ATTACH 'differential:{n}' AS fenced (BRANCH 'o')",'writer_busy')
                f.q(f"CALL differential_expire_snapshots('{n}',versions=>[1]); CALL differential_cleanup_old_files(cleanup_all=>true);")
                f.error(f'SELECT * FROM {merge(n,"killed")}', 'retired')
                f.q(f"CALL differential_abort_merge('{n}','{op}',request_id=>'abort-killed');")
                f.q(f"CALL differential_abort_merge('{n}','{op}',request_id=>'abort-killed');")
                check(f,n,b='base')
                f.error(f'SELECT * FROM {merge(n,"killed")}', 'merge_aborted')
                assert head(f,n)==2
                assert_no_stage(f)
            finally: f.host.close()
            ok('SIGKILL at '+phase+' fences target; expired inputs do not substitute a base; idempotent abort releases ownership')

        # A committed native WAL surviving process death must not be overwritten.
        folder=root/'wal'; f=Fixture(folder)
        try: n=setup(f)
        finally: f.host.close()
        child_code=("import os,sys;sys.path.insert(0,'test/integration');from logical_merge import Fixture;"
                    f"f=Fixture({str(folder)!r});f.q(\"ATTACH 'differential:{n}' AS live (BRANCH 'o'); "
                    "INSERT INTO live.x VALUES(99,99,'wal');\");os._exit(17)")
        p=subprocess.run([sys.executable,'-c',child_code],capture_output=True,text=True,timeout=30)
        assert p.returncode==17,(p.stdout,p.stderr)
        with sqlite3.connect(folder/'engine.sqlite') as db:
            assert db.execute("SELECT count(*) FROM files WHERE name LIKE '%.wal' AND size>0").fetchone()[0]>0
        f=Fixture(folder)
        try:
            f.error(f'SELECT * FROM {merge(n,"wal")}', 'merge_target_dirty')
            assert head(f,n)==2
            f.q(f"ATTACH 'differential:{n}' AS recovered (BRANCH 'o');")
            assert f.q("SELECT count(*) FROM recovered.x WHERE id=99 AND a=99 AND b='wal'")==1
            f.q("CALL differential_create_snapshot('recovered'); DETACH recovered;")
            assert f.q(f'SELECT snapshot_id FROM {merge(n,"wal-recovered")}')==5
            f.q(f"ATTACH 'differential:{n}' AS recovered (BRANCH 'o');")
            assert f.q('SELECT count(*) FROM recovered.x WHERE id=99')==1
            f.q('DETACH recovered;')
        finally: f.host.close()
        ok('committed WAL after abrupt process exit is rejected and recoverable through normal native attach/snapshot')

    evidence_path('merge-target-integration.json').write_text(json.dumps({'passed':passed},indent=2)+'\n')

if __name__ == '__main__': main()
