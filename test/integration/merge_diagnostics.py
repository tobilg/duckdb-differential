#!/usr/bin/env python3
"""Durable, bounded merge diagnostics under failure, cancellation, races and death."""
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import tempfile
import threading
import time

from logical_merge import Fixture
from merge_target import setup, head, check
from merge_preflight import merge, snapshot_state, unchanged
from merge_hardening import run_at_gate, assert_no_stage
from paths import DUCKDB_CLI, EXTENSION
from evidence import evidence_path


def request(n, rid): return f"differential_merge_requests('{n}',request_id=>'{rid}')"


def attempt(f, n, rid, count, state, phase=None, code=None):
    where = f"attempt_count={count} AND last_attempt_state='{state}'"
    if phase: where += f" AND last_attempt_phase='{phase}'"
    where += ' AND last_attempt_error_code ' + (f"='{code}'" if code else 'IS NULL')
    where += ' AND last_attempt_started_at<=last_attempt_updated_at AND length(last_attempt_id)=36'
    where += ' AND last_attempt_finished_at ' + ('IS NULL' if state == 'incomplete' else '=last_attempt_updated_at')
    assert f.q(f'SELECT count(*) FROM {request(n,rid)} WHERE {where}') == 1, (rid,count,state,phase,code)


def main():
    passed = []
    def ok(label): passed.append(label); print('PASS',label,flush=True)
    with tempfile.TemporaryDirectory(prefix='differential-merge-diagnostics-') as tmp:
        root = Path(tmp); repo = root/'repo'; f = Fixture(repo)
        try:
            n = setup(f)
            snapshot_state(f,n)
            sql = merge(n,'guarded','result')
            f.q(f"PREPARE later AS SELECT snapshot_id FROM {sql}; EXPLAIN SELECT * FROM {sql}; "
                f"SELECT * FROM differential_merge_branch_preflight('{n}','o','t','result');")
            unchanged(f,n)
            assert f.q('EXECUTE later') == 4
            attempt(f,n,'guarded',1,'succeeded','completed')
            assert f.q(f"SELECT count(*) FROM {request(n,'guarded')} r "
                       f"JOIN differential_branches('{n}') o ON r.ours_branch_id=o.branch_id "
                       f"JOIN differential_branches('{n}') t ON r.theirs_branch_id=t.branch_id "
                       "WHERE r.inputs_guarded AND r.ours_branch_name='o' AND r.theirs_branch_name='t' "
                       "AND o.branch_name='o' AND t.branch_name='t'") == 1
            f.q(f"CREATE TEMP TABLE original AS SELECT * FROM {request(n,'guarded')}")
            assert f.q(f'SELECT snapshot_id FROM {sql}') == 4
            f.error(f"SELECT * FROM {merge(n,'guarded','result',ids=None)}",'Request ID')
            f.error(f"SELECT * FROM {merge(n,'guarded','result',ids=(1,3,3))}",'Request ID')
            assert f.q(f"SELECT count(*) FROM (SELECT * FROM {request(n,'guarded')} EXCEPT ALL SELECT * FROM original)") == 0
            ok('planning/preflight create no attempts; guarded success records original identities; exact replay is unchanged')

            f.q(f"CALL differential_drop_branch('{n}','t'); CALL differential_create_branch('{n}','t',snapshot=>3)")
            assert f.q(f'SELECT snapshot_id FROM {sql}') == 4
            assert f.q(f"SELECT count(*) FROM {request(n,'guarded')} r JOIN differential_branches('{n}') t "
                       "ON t.branch_name='t' AND t.state='active' WHERE r.theirs_branch_id<>t.branch_id AND r.theirs_branch_name='t'") == 1
            assert f.q(f"SELECT count(*) FROM (SELECT * FROM {request(n,'guarded')} EXCEPT ALL SELECT * FROM original)") == 0
            ok('original branch UUIDs survive name reuse without rewriting completed diagnostics')

            n = setup(f)
            limited = f.merge(n,'limited','limits')
            for number in range(1,5):
                f.error('SELECT * FROM '+limited[:-1]+',max_input_rows=>1)', 'merge_limit')
                attempt(f,n,'limits',number,'failed',code='merge_limit')
            with sqlite3.connect(repo/'engine.sqlite') as db:
                assert db.execute("SELECT count(*),max(length(payload)) FROM operations WHERE kind='merge_attempt' "
                                  "AND database_id=(SELECT id FROM databases WHERE name=?)",(n,)).fetchone()[0] == 1
                assert db.execute("SELECT max(length(payload)) FROM operations WHERE kind='merge_attempt'").fetchone()[0] <= 1024
            f.q(f"CREATE TEMP TABLE failed AS SELECT * FROM {request(n,'limits')}")
            assert f.q(f'SELECT snapshot_id FROM {limited}') == 4
            attempt(f,n,'limits',5,'succeeded','completed')
            assert f.q(f"SELECT count(*) FROM {request(n,'limits')} r,failed f WHERE r.last_attempt_id<>f.last_attempt_id "
                       "AND r.last_attempt_started_at>=f.last_attempt_updated_at AND r.ours_branch_name IS NULL AND r.inputs_guarded IS NULL") == 1
            missing = f"differential_merge_snapshots('{n}',1,2,999,'missing',request_id=>'missing')"
            f.error(f'SELECT * FROM {missing}','not_found')
            attempt(f,n,'missing',1,'failed','context_frozen','not_found')
            ok('bounded latest-attempt replacement, monotonic count/time, reset on retry and missing-input failures')

            conflict = setup(f,ours='UPDATE o.x SET a=1;',theirs='UPDATE t.x SET a=2;')
            f.error(f"SELECT * FROM {f.merge(conflict,request='conflict')}",'merge_conflict')
            attempt(f,conflict,'conflict',1,'failed',code='merge_conflict')
            unique = f.setup('CREATE TABLE d.x(id INT PRIMARY KEY,v INT UNIQUE);','INSERT INTO d.x VALUES(1,10),(2,20);',
                             'UPDATE o.x SET v=30 WHERE id=1;','UPDATE t.x SET v=30 WHERE id=2;')
            f.error(f"SELECT * FROM {f.merge(unique,request='constraint')}",'merge_constraint')
            attempt(f,unique,'constraint',1,'failed',code='merge_constraint')
            # Constraint text includes a synthetic sensitive value. It must not be
            # copied to the companion payload (nor any arbitrary exception text).
            secret = 'synthetic-secret-never-log-this'
            sensitive = f.setup('CREATE TABLE d.x(id INT PRIMARY KEY,v VARCHAR UNIQUE);',"INSERT INTO d.x VALUES(1,'a'),(2,'b');",
                                f"UPDATE o.x SET v='{secret}' WHERE id=1;",f"UPDATE t.x SET v='{secret}' WHERE id=2;")
            f.error(f"SELECT * FROM {f.merge(sensitive,request='sensitive')}",'merge_constraint')
            with sqlite3.connect(repo/'engine.sqlite') as db:
                assert not db.execute("SELECT 1 FROM operations WHERE kind='merge_attempt' AND instr(payload,?)>0",(secret,)).fetchall()
            attempt(f,sensitive,'sensitive',1,'failed',code='merge_constraint')
            assert_no_stage(f)
            ok('data conflicts and combined constraints are distinct; durable diagnostics exclude error values')

            for phase in ('comparison','table_inserted'):
                rid = 'cancel-'+phase
                answers,errors = run_at_gate(f,root,phase,f'SELECT * FROM {f.merge(n,rid,rid)}',
                    lambda connection,*_: f.host.lib.duckdb_interrupt(connection))
                assert errors and not answers
                attempt(f,n,rid,1,'failed',phase,'cancelled')
                assert_no_stage(f)
            answers,errors = run_at_gate(f,root,'comparison',
                f"SELECT * FROM differential_merge_snapshots('{n}',1,2,3,'timeout',request_id=>'timeout',timeout_ms=>500)",
                lambda *_:time.sleep(.6))
            assert errors and 'merge_timeout' in errors[0]
            attempt(f,n,'timeout',1,'failed','comparison','merge_timeout')
            ok('cancellation and deadline record the actual failure phase and retain no staging')

            # Admission failures are not admitted executions, even if the immutable
            # request was reserved. A waiter returning an existing result adds none.
            f.q('SET differential_merge_max_concurrent=1')
            def busy(*_):
                f.error(f"SELECT * FROM {f.merge(n,'busy','busy')}",'merge_busy')
                assert f.q(f"SELECT count(*) FROM {request(n,'busy')} WHERE attempt_count=0 AND last_attempt_state IS NULL") == 1
            answers,errors = run_at_gate(f,root,'pinned',f"SELECT * FROM {f.merge(n,'holder','holder')}",busy)
            assert answers and not errors
            f.q('SET differential_merge_max_concurrent=2')
            wait_conn = f.host.connect(); waiting = []
            def waiter(): waiting.append(f.host.query(wait_conn,f"SELECT snapshot_id FROM {f.merge(n,'race','race')}"))
            threads = []
            def duplicate(*_):
                thread = threading.Thread(target=waiter); threads.append(thread); thread.start()
                deadline = time.monotonic()+10
                while f.q("SELECT count(*) FROM differential_merge_status() WHERE phase='request_waiting'") == 0:
                    assert time.monotonic()<deadline; time.sleep(.01)
                attempt(f,n,'race',1,'incomplete','pinned')
            answers,errors = run_at_gate(f,root,'pinned',f"SELECT snapshot_id FROM {f.merge(n,'race','race')}",duplicate)
            for thread in threads: thread.join(timeout=30); assert not thread.is_alive()
            assert answers == waiting and not errors
            attempt(f,n,'race',1,'succeeded','completed')
            ok('admission rejection has no attempt; concurrent same-request waiter returns one completed attempt')

            for fault in ('begin','progress','failure','complete'):
                rid = 'diagnostic-'+fault
                sql = f.merge(n,rid,rid)
                os.environ['DIFFERENTIAL_MERGE_DIAGNOSTICS_ERROR'] = fault
                try:
                    if fault == 'failure': f.error('SELECT * FROM '+sql[:-1]+',max_input_rows=>1)','merge_limit')
                    else: f.error('SELECT * FROM '+sql,'disk_full')
                finally: os.environ.pop('DIFFERENTIAL_MERGE_DIAGNOSTICS_ERROR')
                assert f.q(f"SELECT count(*) FROM differential_branches('{n}') WHERE branch_name='{rid}'") == 0
                if fault == 'begin':
                    assert f.q(f"SELECT count(*) FROM {request(n,rid)} WHERE attempt_count=0 AND last_attempt_state IS NULL") == 1
                elif fault == 'failure': attempt(f,n,rid,1,'incomplete','pinned')
                else: attempt(f,n,rid,1,'failed',code='disk_full')
                f.q('SELECT * FROM '+sql)
                attempt(f,n,rid,1 if fault == 'begin' else 2,'succeeded','completed')
                assert_no_stage(f)
            ok('diagnostic write faults never publish partial output or mask the primary error; safe retry succeeds')
        finally:
            os.environ.pop('DIFFERENTIAL_MERGE_DIAGNOSTICS_ERROR',None)
            f.host.close()

        for phase in ('resolved','request_locked','staged','publication_transaction','published'):
            folder = root/('death-'+phase); f = Fixture(folder)
            try: n = setup(f)
            finally: f.host.close()
            sql = 'SELECT snapshot_id FROM '+merge(n,'death')
            prefix = f"LOAD '{EXTENSION}'; SET differential_repository='{folder}'; SET threads=2;"
            child = subprocess.run([DUCKDB_CLI,'-init','/dev/null','-unsigned','-batch','-bail'],input=prefix+sql,
                                   text=True,capture_output=True,timeout=60,env=dict(os.environ,DIFFERENTIAL_MERGE_CRASH=phase))
            assert child.returncode == 86,(phase,child.stdout,child.stderr)
            f = Fixture(folder)
            try:
                if phase == 'resolved':
                    assert f.q(f"SELECT count(*) FROM {request(n,'death')} WHERE attempt_count=0 AND last_attempt_state IS NULL") == 1
                elif phase == 'published': attempt(f,n,'death',1,'succeeded','completed')
                else: attempt(f,n,'death',1,'incomplete','publishing' if phase == 'publication_transaction' else phase)
                assert head(f,n) == (4 if phase == 'published' else 2)
                before_count = f.q(f'SELECT attempt_count FROM {request(n,"death")}')
                assert f.q(sql) == 4
                assert f.q(sql) == 4
                attempt(f,n,'death',before_count + (phase != 'published'),'succeeded','completed')
                check(f,n)
                f.q('CALL differential_cleanup_old_files(cleanup_all=>true)')
                assert_no_stage(f)
            finally: f.host.close()
            ok(f'process death at {phase}: outcome and target publication are atomic; retry increments only when executed')
    evidence_path('merge-diagnostics.json').write_text(json.dumps({'passed':passed,
        'scope':'Stock DuckDB v2.0.0 local synthetic repositories; injected faults/process death, not physical power loss'},indent=2)+'\n')


if __name__ == '__main__': main()
