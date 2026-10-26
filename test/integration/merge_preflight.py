#!/usr/bin/env python3
"""Destination-aware preflight and atomic reviewed-input guards on the stock host.

All repositories and crash fixtures are disposable. No remote service is contacted.
"""
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
from merge_target import setup, head, check
from merge_hardening import run_at_gate, assert_no_stage
from paths import DUCKDB_CLI, EXTENSION
from evidence import evidence_path


def preflight(n, destination='o', extra='', ours='o', theirs='t'):
    return f"differential_merge_branch_preflight('{n}','{ours}','{theirs}','{destination}'{extra})"


def merge(n, request, destination='o', ids=(1, 2, 3), extra=''):
    guard = '' if ids is None else ','.join(f'expected_{side}_snapshot_id=>{value}' for side, value in zip(('base', 'ours', 'theirs'), ids))
    return f"differential_merge_branch('{n}','o','t','{destination}',request_id=>'{request}'{','+guard if guard else ''}{extra})"


def snapshot_state(f, n):
    # Compare public identities, heads, epochs and all operation rows, not just counts.
    f.q(f"CREATE OR REPLACE TEMP TABLE branches_before AS SELECT * FROM differential_branches('{n}');"
        f"CREATE OR REPLACE TEMP TABLE snapshots_before AS SELECT * FROM differential_snapshots('{n}');"
        "CREATE OR REPLACE TEMP TABLE operations_before AS SELECT * FROM differential_operations();"
        "CREATE OR REPLACE TEMP TABLE stages_before AS SELECT identity FROM differential_retention_roots() "
        "WHERE reason='native_file' AND starts_with(identity,'merge_stage/');")


def unchanged(f, n):
    for before, source in [('branches_before', f"differential_branches('{n}')"),
                           ('snapshots_before', f"differential_snapshots('{n}')"),
                           ('operations_before', 'differential_operations()')]:
        assert f.q(f'SELECT count(*) FROM ((SELECT * FROM {before} EXCEPT ALL SELECT * FROM {source}) '
                   f'UNION ALL (SELECT * FROM {source} EXCEPT ALL SELECT * FROM {before}))') == 0
    assert f.q("SELECT count(*) FROM ((SELECT identity FROM differential_retention_roots() "
               "WHERE reason='native_file' AND starts_with(identity,'merge_stage/') EXCEPT ALL SELECT * FROM stages_before) "
               "UNION ALL (SELECT * FROM stages_before EXCEPT ALL SELECT identity FROM differential_retention_roots() "
               "WHERE reason='native_file' AND starts_with(identity,'merge_stage/')))") == 0
    assert f.q('SELECT count(*) FROM differential_merge_status()') == 0


def main():
    passed = []
    def ok(label): passed.append(label); print('PASS', label, flush=True)
    with tempfile.TemporaryDirectory(prefix='differential-merge-preflight-') as tmp:
        root = Path(tmp); repo = root/'repo'; f = Fixture(repo)
        try:
            n = setup(f)
            snapshot_state(f, n)
            f.q(f'PREPARE review AS SELECT ready FROM {preflight(n)}; EXPLAIN SELECT * FROM {preflight(n)};')
            unchanged(f, n)
            assert f.q('EXECUTE review') == 1
            for dest, action, target in [('o', 'merged', 'clean'), ('new', 'created_branch', 'not_applicable')]:
                assert f.q(f"SELECT count(*) FROM {preflight(n,dest)} WHERE ready AND validation_state='valid' "
                           f"AND base_snapshot_id=1 AND ours_snapshot_id=2 AND theirs_snapshot_id=3 "
                           f"AND merge_action='{action}' AND target_state='{target}' AND len(issues)=0 "
                           "AND data_conflicts=0 AND schema_conflicts=0 AND NOT issues_truncated") == 1
            assert f.q(f"SELECT count(*) FROM {preflight(n, extra=',validate=>false')} WHERE ready AND validation_state='not_checked' AND application_ms=0") == 1
            unchanged(f, n)
            ok('preflight and planning preserve branches, epochs, operations and snapshots; default native validation and opt-out are explicit')

            f.q(f"CALL differential_create_branch('{n}','ancestor',snapshot=>1); CALL differential_create_branch('{n}','equal',snapshot=>3);")
            assert f.q(f"SELECT count(*) FROM {preflight(n,'ancestor',ours='ancestor')} WHERE ready AND merge_action='fast_forward'") == 1
            assert f.q(f"SELECT count(*) FROM {preflight(n,'equal',ours='equal')} WHERE ready AND merge_action='already_up_to_date'") == 1
            assert f.q(f"SELECT count(*) FROM {preflight(n,'t')} WHERE NOT ready AND target_state='exists' AND issues[1].code='name_conflict'") == 1
            ok('new branch, true merge, fast-forward, no-op and existing destination are distinguished')

            f.q(f"ATTACH 'differential:{n}' AS live (BRANCH 'o'); ATTACH 'differential:{n}' AS source (BRANCH 't');")
            assert f.q(f"SELECT count(*) FROM {preflight(n)} WHERE NOT ready AND target_state='busy' AND validation_state='valid' AND issues[1].code='active_attachment_or_transaction'") == 1
            other = f.host.connect(); f.host.query(other, 'BEGIN; SELECT count(*) FROM live.x;')
            f.q('DETACH live;')
            assert f.q(f"SELECT count(*) FROM {preflight(n)} WHERE NOT ready AND target_state='busy'") == 1
            f.host.query(other, 'COMMIT;')
            assert f.q(f'SELECT ready FROM {preflight(n)}') == 1
            f.q('DETACH source;')
            ok('attached target and detached but held transaction are busy; attached source remains allowed')

            code = ("import sys;sys.path.insert(0,'test/integration');from logical_merge import Fixture;"
                    f"f=Fixture({str(repo)!r});f.q(\"ATTACH 'differential:{n}' AS held (BRANCH 'o');\");"
                    "print('ready',flush=True);sys.stdin.readline();f.host.close()")
            holder = subprocess.Popen([sys.executable,'-c',code],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
            try:
                assert holder.stdout.readline().strip() == 'ready'
                assert f.q(f"SELECT count(*) FROM {preflight(n)} WHERE NOT ready AND target_state='busy'") == 1
            finally: holder.communicate('release\n',timeout=30)
            assert holder.returncode == 0
            assert f.q(f'SELECT ready FROM {preflight(n)}') == 1
            ok('preflight uses the cross-process target lease and releases it after inspection')

            dirty = setup(f)
            f.q(f"ATTACH 'differential:{dirty}' AS live (BRANCH 'o'); INSERT INTO live.x VALUES(9,9,'live'); DETACH live;")
            snapshot_state(f, dirty)
            assert f.q(f"SELECT count(*) FROM {preflight(dirty)} WHERE NOT ready AND target_state='dirty' AND issues[1].code='merge_target_dirty'") == 1
            assert f.q(f'SELECT ready FROM {preflight(dirty,"new")}') == 1
            unchanged(f, dirty)
            f.q(f"ATTACH 'differential:{dirty}' AS live (BRANCH 'o');")
            assert f.q('SELECT count(*) FROM live.x WHERE id=9') == 1
            f.q('DETACH live;')
            ok('unsnapshotted target data is diagnosed and preserved; new destinations use only immutable inputs')

            conflict = setup(f, 'UPDATE o.x SET a=1;', 'UPDATE t.x SET a=2;')
            snapshot_state(f, conflict)
            assert f.q(f"SELECT count(*) FROM {preflight(conflict)} WHERE NOT ready AND validation_state='data_conflicts' AND data_conflicts=2 "
                       "AND issues[1].code='merge_conflict' AND issues[1].object_name='x'") == 1
            f.q("CREATE TABLE fixes(id INT,a INT,b VARCHAR,_merge_action VARCHAR,_merge_columns MAP(VARCHAR,VARCHAR)); INSERT INTO fixes VALUES(1,5,NULL,'cells',map{'a':'custom'}),(2,6,NULL,'cells',map{'a':'custom'});")
            resolutions = ",resolution_tables=>map{'main.x':'memory.main.fixes'}"
            assert f.q(f"SELECT ready FROM {preflight(conflict,extra=resolutions)}") == 1
            unchanged(f, conflict)
            f.q('DROP TABLE fixes;')
            ok('aggregate data conflicts and typed resolution validation do not freeze decisions or create a request')

            constraint = setup(f, 'UPDATE o.x SET a=3 WHERE id=1;', 'UPDATE t.x SET a=3 WHERE id=2;',
                               'CREATE TABLE d.x(id INT PRIMARY KEY,a INT UNIQUE,b VARCHAR);', "INSERT INTO d.x VALUES(1,1,'base'),(2,2,NULL);")
            assert f.q(f"SELECT count(*) FROM {preflight(constraint)} WHERE NOT ready AND data_conflicts=0 AND validation_state='constraint_violation' AND issues[1].code='merge_constraint'") == 1
            assert f.q(f"SELECT count(*) FROM {preflight(constraint,extra=',validate=>false')} WHERE ready AND validation_state='not_checked'") == 1
            order = setup(f, 'UPDATE o.x SET b=1 WHERE id=0;',
                          'DELETE FROM t.x; INSERT INTO t.x SELECT i,(i+1)%3000,0 FROM range(3000) r(i);',
                          'CREATE TABLE d.x(id INT PRIMARY KEY,a INT UNIQUE,b INT);',
                          'INSERT INTO d.x SELECT i,i,0 FROM range(3000) r(i);')
            assert f.q(f'SELECT ready FROM {preflight(order)}') == 1
            ok('final UNIQUE violation is separate from data conflict; constrained 3000-row permutation validates')

            for ddl, object_type, object_name in [('CREATE TABLE d.x(id INT);','table','main.x'),
                                                   ('CREATE TABLE d.x(id INT PRIMARY KEY,a BIT[]);','column','main.x.a'),
                                                   ('CREATE SEQUENCE d.seq;','sequence','main.seq')]:
                bad = f.setup(ddl)
                assert f.q(f"SELECT count(*) FROM {preflight(bad)} WHERE NOT ready AND validation_state='unsupported' AND len(issues)=3 "
                           f"AND issues[1].code='merge_unsupported' AND issues[1].input='base' AND issues[1].object_type='{object_type}' "
                           f"AND issues[1].object_name='{object_name}' AND data_conflicts IS NULL") == 1
                assert_no_stage(f)
            schema = setup(f, 'ALTER TABLE o.x ADD COLUMN extra INT;', 'ALTER TABLE t.x ADD COLUMN extra VARCHAR;')
            assert f.q(f"SELECT count(*) FROM {preflight(schema,extra=",schema_mode=>'additive'")} WHERE NOT ready AND schema_conflicts=1 AND validation_state='schema_conflicts' AND issues[1].code='merge_schema_conflict'") == 1
            ok('unsupported objects carry input and object identity; schema conflicts remain distinct from unsupported catalogs')

            long_type = f.setup('CREATE TABLE d.x(id INT PRIMARY KEY,a STRUCT("'+'東京'*1000+'" BIT));')
            assert f.q(f"SELECT count(*) FROM {preflight(long_type)} WHERE NOT ready AND issues_truncated "
                       "AND octet_length(encode(issues[1].message))<=4096 AND length(issues[1].message)>1000") == 1
            ok('oversized Unicode diagnostics are clipped at a UTF-8 boundary with an explicit truncation flag')

            empty = f.setup('')
            assert f.q(f"SELECT count(*) FROM {preflight(empty)} WHERE ready AND data_conflicts=0 AND validation_state='valid' AND base_snapshot_id=1") == 1
            assert f.q(f"SELECT count(*) FROM {preflight(n,theirs='missing')} WHERE NOT ready AND base_snapshot_id IS NULL AND len(issues)>0") == 1
            expired = setup(f); f.q(f"CALL differential_expire_snapshots('{expired}',versions=>[1]);")
            assert f.q(f"SELECT count(*) FROM {preflight(expired)} WHERE NOT ready AND len(issues)>0") == 1
            f.q("ATTACH 'differential:remote' AS remote (DATA_PATH 's3://unused-preflight/disposable/');")
            assert f.q(f"SELECT count(*) FROM {preflight('remote')} WHERE NOT ready AND issues[1].code='not_found'") == 1
            f.q('DETACH remote;')
            ok('empty database returns a complete report; missing/expired inputs and S3 storage are blocked')

            limits = setup(f)
            assert f.q(f"SELECT count(*) FROM {preflight(limits,extra=',max_input_rows=>1')} WHERE NOT ready AND issues[1].code='merge_limit'") == 1
            def cancel(connection, *_): f.host.lib.duckdb_interrupt(connection)
            answers, errors = run_at_gate(f, root, 'table_rebuild', f'SELECT ready FROM {preflight(limits)}', cancel)
            assert not answers and len(errors)==1 and 'interrupt' in errors[0].lower(), errors
            assert_no_stage(f)
            answers, errors = run_at_gate(f, root, 'table_rebuild',
                                         f'SELECT ready FROM {preflight(limits,extra=",timeout_ms=>1000")}', lambda *_: time.sleep(1.2))
            assert not answers and len(errors)==1 and 'merge_timeout' in errors[0], errors
            assert_no_stage(f)
            ok('limits produce blocked diagnostics; cancellation raises and cleans staging and pins')

            bounded = f.setup(''.join(f'CREATE TABLE d.x{i}(id INT PRIMARY KEY,a INT); INSERT INTO d.x{i} VALUES(1,0);' for i in range(110)), '',
                              ''.join(f'UPDATE o.x{i} SET a=1;' for i in range(110)), ''.join(f'UPDATE t.x{i} SET a=2;' for i in range(110)))
            assert f.q(f"SELECT count(*) FROM {preflight(bounded)} WHERE NOT ready AND data_conflicts=110 AND len(issues)=100 AND issues_truncated") == 1
            ok('100-diagnostic bound does not truncate aggregate counts across 110 conflicting tables')

            guarded = setup(f)
            for ids in [(1,99,3),(1,2,99),(99,2,3)]:
                snapshot_state(f, guarded)
                f.error(f'SELECT * FROM {merge(guarded,"guard",ids=ids)}', 'merge_inputs_changed')
                unchanged(f, guarded)
            f.error(f"SELECT * FROM differential_merge_branch('{guarded}','o','t','o',expected_ours_snapshot_id=>2)", 'all three')
            f.error(f"SELECT * FROM differential_merge_branch('{guarded}','o','t','o',expected_ours_snapshot_id=>NULL)", 'positive')
            f.error(f"SELECT * FROM {merge(guarded,'guard',ids=(0,2,3))}", 'positive')
            assert f.q(f'SELECT snapshot_id FROM {merge(guarded,"guard")}') == 4
            for ids in [None,(1,2,4)]: f.error(f'SELECT * FROM {merge(guarded,"guard",ids=ids)}', 'different expected snapshot IDs')
            assert f.q(f'SELECT snapshot_id FROM {merge(guarded,"guard")}') == 4
            assert head(f, guarded) == 4
            ok('all three expected IDs are enforced before reservation; guard presence and values are retry identity')

            stale = setup(f)
            assert f.q(f'SELECT ready FROM {preflight(stale,"new")}') == 1
            f.q(f'PREPARE guarded_plan AS SELECT * FROM {merge(stale,"drift","new")};')
            f.q(f"ATTACH 'differential:{stale}' AS live (BRANCH 't'); UPDATE live.x SET b='newer'; CALL differential_create_snapshot('live'); DETACH live;")
            snapshot_state(f, stale)
            f.error('EXECUTE guarded_plan', 'merge_inputs_changed')
            unchanged(f, stale)
            assert f.q(f'SELECT snapshot_id FROM {merge(stale,"drift","new",ids=(1,2,4))}') == 5
            ok('prepared guarded execution checks current heads at execution; stale preflight creates no output or request')

            pending = setup(f)
            f.error(f'SELECT * FROM {merge(pending,"pending",extra=",max_input_rows=>1")}', 'merge_limit')
            f.q(f"ATTACH 'differential:{pending}' AS source (BRANCH 't'); UPDATE source.x SET b='later'; CALL differential_create_snapshot('source'); DETACH source;")
            f.error(f'SELECT * FROM {merge(pending,"pending",ids=(1,2,4))}', 'different expected snapshot IDs')
            f.error(f'SELECT * FROM {merge(pending,"pending",ids=None)}', 'different expected snapshot IDs')
            assert f.q(f'SELECT snapshot_id FROM {merge(pending,"pending")}') == 5
            check(f, pending)  # frozen source=3, not the later source=4
            f.q(f"CALL differential_drop_branch('{pending}','o'); CALL differential_create_branch('{pending}','o',snapshot=>1);"
                f"CALL differential_expire_snapshots('{pending}',versions=>[2,3,5]); CALL differential_cleanup_old_files(cleanup_all=>true);")
            assert f.q(f'SELECT snapshot_id FROM {merge(pending,"pending")}') == 5
            assert head(f, pending) == 1
            ok('pending retry uses frozen inputs despite source movement; completed replay survives expiry and branch name reuse')

            legacy = setup(f)
            assert f.q(f'SELECT snapshot_id FROM {merge(legacy,"unguarded","new",ids=None)}') == 4
            f.error(f'SELECT * FROM {merge(legacy,"unguarded","new")}', 'different expected snapshot IDs')
            live = setup(f)
            f.q(f"ATTACH 'differential:{live}' AS source (BRANCH 't'); INSERT INTO source.x VALUES(99,99,'live');")
            assert f.q(f'SELECT snapshot_id FROM {merge(live,"live")}') == 4
            f.q('DETACH source;')
            f.q(f"ATTACH 'differential:{live}' AS verify (BRANCH 'o');")
            assert f.q('SELECT count(*) FROM verify.x WHERE id=99') == 0
            f.q('DETACH verify;')
            ok('unguarded request cannot acquire a guard on retry; unsnapshotted source changes remain excluded')

            moving = setup(f)
            def move_head(*_):
                f.q(f"ATTACH 'differential:{moving}' AS source (BRANCH 't'); UPDATE source.x SET b='after-selection'; CALL differential_create_snapshot('source'); DETACH source;")
            answers, errors = run_at_gate(f, root, 'pinned', f'SELECT ready FROM {preflight(moving)}', move_head)
            assert answers == [0] and not errors, (answers, errors)
            assert head(f, moving, 't') == 4
            selected = setup(f)
            def move_selected(*_):
                f.q(f"ATTACH 'differential:{selected}' AS source (BRANCH 't'); UPDATE source.x SET b='later'; CALL differential_create_snapshot('source'); DETACH source;")
            answers, errors = run_at_gate(f, root, 'request_locked', f'SELECT snapshot_id FROM {merge(selected,"race")}', move_selected)
            assert answers == [5] and not errors, (answers, errors)
            check(f, selected)
            ok('preflight detects movement during comparison; merge freezes guarded selection before concurrent source movement')

            overlap = setup(f); retries, second, failures = [], [], []
            def compete(*_):
                os.environ.pop('DIFFERENTIAL_MERGE_PHASE_GATE')
                connection = f.host.connect()
                def retry():
                    try: second.append(f.host.query(connection, f'SELECT snapshot_id FROM {merge(overlap,"same")}'))
                    except RuntimeError as error: failures.append(str(error))
                thread = threading.Thread(target=retry); retries.append(thread); thread.start()
                until = time.monotonic()+20
                while f.q("SELECT count(*) FROM differential_merge_status() WHERE phase='request_waiting'") != 1:
                    assert thread.is_alive() and time.monotonic()<until, failures
                    time.sleep(.01)
                f.error(f'SELECT * FROM {merge(overlap,"same",ids=(1,2,4))}', 'different expected snapshot IDs')
            answers, errors = run_at_gate(f, root, 'staged', f'SELECT snapshot_id FROM {merge(overlap,"same")}', compete)
            for thread in retries: thread.join(30)
            assert answers == [4] and second == [4] and not errors and not failures, (answers, second, errors, failures)
            assert head(f, overlap) == 4
            ok('concurrent guarded retries publish once and reject different guards while the request is in flight')
            shared = setup(f)
        finally: f.host.close()

        # Guard companion and request selection are one transaction. Failed guards
        # never left an operation; no guard companion pins any root indefinitely.
        with sqlite3.connect(repo/'engine.sqlite') as db:
            assert db.execute("SELECT count(*) FROM operations WHERE kind='merge_expected_inputs'").fetchone()[0] == 6
            assert db.execute('SELECT count(*) FROM compound_intents').fetchone()[0] == 0
            # Scope-guard fixture, as in merge_branches.py: shared ATTACH itself
            # requires S3. Set the mode only in this closed disposable repository.
            db.execute("UPDATE databases SET writers='shared' WHERE name=?", (shared,))
        ok('guard identity companions are recorded only for accepted guarded selections')
        f = Fixture(repo)
        try:
            operations = f.q('SELECT count(*) FROM differential_operations()')
            assert f.q(f"SELECT count(*) FROM {preflight(shared)} WHERE NOT ready AND issues[1].code='merge_unsupported'") == 1
            assert f.q('SELECT count(*) FROM differential_operations()') == operations
            assert_no_stage(f)
        finally: f.host.close()
        ok('shared-mode scope guard rejects before selection or publication in a closed-metadata fixture')

        for phase in ('resolved','staged','publication_transaction','published'):
            folder = root/('crash-'+phase); f = Fixture(folder)
            try: n = setup(f)
            finally: f.host.close()
            prefix = f"LOAD '{EXTENSION}'; SET differential_repository='{folder}'; SET threads=2;"
            sql = f'SELECT snapshot_id FROM {merge(n,"crash")};'
            child = subprocess.run([DUCKDB_CLI,'-init','/dev/null','-unsigned','-batch','-bail'], input=prefix+sql,
                                   text=True,capture_output=True,timeout=60,env=dict(os.environ,DIFFERENTIAL_MERGE_CRASH=phase))
            assert child.returncode == 86, (phase, child.stdout, child.stderr)
            with sqlite3.connect(folder/'engine.sqlite') as db:
                assert db.execute("SELECT count(*) FROM operations WHERE kind='merge_expected_inputs'").fetchone()[0] == 1
            f = Fixture(folder)
            try:
                if phase in ('staged','publication_transaction'):
                    # Reap the dead worker's scratch files before capturing the
                    # isolation baseline. Otherwise background GC can remove them
                    # between the two observations independently of preflight.
                    f.q('CALL differential_cleanup_old_files(cleanup_all=>true);')
                    snapshot_state(f, n)
                    assert f.q(f"SELECT count(*) FROM {preflight(n)} WHERE NOT ready AND target_state='pending' AND issues[1].code='merge_pending_operation' "
                               "AND issues[1].operation_id IS NOT NULL AND contains(issues[1].recovery_action,'differential_abort_merge')") == 1
                    unchanged(f, n)
                f.error(f'SELECT * FROM {merge(n,"crash",ids=None)}', 'different expected snapshot IDs')
                assert f.q(sql) == 4
                assert f.q(sql) == 4
                check(f, n)
                f.q('CALL differential_cleanup_old_files(cleanup_all=>true);')
                assert_no_stage(f)
            finally: f.host.close()
            ok('guarded crash at '+phase+' preserves identity; preflight reports pending recovery without changing it; retry publishes once')

        for phase in ('table_rebuild','checkpointing'):
            folder = root/('preflight-death-'+phase); f = Fixture(folder)
            try: n = setup(f)
            finally: f.host.close()
            with sqlite3.connect(folder/'engine.sqlite') as db:
                before = db.execute('SELECT id,root,epoch,head FROM branches ORDER BY id').fetchall()
                operations = db.execute('SELECT count(*) FROM operations').fetchone()[0]
            prefix = f"LOAD '{EXTENSION}'; SET differential_repository='{folder}'; SET threads=2;"
            child = subprocess.run([DUCKDB_CLI,'-init','/dev/null','-unsigned','-batch','-bail'],
                                   input=prefix+f'SELECT * FROM {preflight(n)};',text=True,capture_output=True,timeout=60,
                                   env=dict(os.environ,DIFFERENTIAL_MERGE_CRASH=phase))
            assert child.returncode == 86, (phase,child.stdout,child.stderr)
            with sqlite3.connect(folder/'engine.sqlite') as db:
                assert db.execute('SELECT id,root,epoch,head FROM branches ORDER BY id').fetchall() == before
                assert db.execute('SELECT count(*) FROM operations').fetchone()[0] == operations
                assert db.execute('SELECT count(*) FROM compound_intents').fetchone()[0] == 0
            f = Fixture(folder)
            try:
                f.q('CALL differential_cleanup_old_files(cleanup_all=>true);')
                assert_no_stage(f)
                assert f.q(f'SELECT ready FROM {preflight(n)}') == 1
            finally: f.host.close()
            ok('preflight process death at '+phase+' creates no request or fence; normal GC reclaims abandoned scratch storage')

        for kind in ('restore','drop_branch'):
            folder = root/('pending-'+kind); f = Fixture(folder)
            try: n = setup(f)
            finally: f.host.close()
            sql = (f"CALL differential_restore('{n}',1,branch=>'o',request_id=>'lifecycle');" if kind=='restore'
                   else f"CALL differential_drop_branch('{n}','o',request_id=>'lifecycle');")
            prefix = f"LOAD '{EXTENSION}'; SET differential_repository='{folder}'; SET threads=2;"
            child = subprocess.run([DUCKDB_CLI,'-init','/dev/null','-unsigned','-batch','-bail'],input=prefix+sql,
                                   text=True,capture_output=True,timeout=60,env=dict(os.environ,DIFFERENTIAL_CONTROL_CRASH='compound_intent'))
            assert child.returncode == 86, (kind,child.stdout,child.stderr)
            f = Fixture(folder)
            try:
                snapshot_state(f, n)
                assert f.q(f"SELECT count(*) FROM {preflight(n)} WHERE NOT ready AND target_state='pending' "
                           f"AND issues[1].operation_id IS NOT NULL AND contains(issues[1].message,'{kind}') AND contains(issues[1].recovery_action,'lifecycle')") == 1
                unchanged(f, n)
                f.q(sql)
            finally: f.host.close()
            ok('pending '+kind+' is reported with its owner and recovery action without executing recovery')

        # A committed WAL is not implicitly recovered/checkpointed by preflight.
        folder = root/'wal'; f = Fixture(folder)
        try: n = setup(f)
        finally: f.host.close()
        code = ("import os,sys;sys.path.insert(0,'test/integration');from logical_merge import Fixture;"
                f"f=Fixture({str(folder)!r});f.q(\"ATTACH 'differential:{n}' AS live (BRANCH 'o'); INSERT INTO live.x VALUES(99,99,'wal');\");os._exit(17)")
        child = subprocess.run([sys.executable,'-c',code],capture_output=True,text=True,timeout=30)
        assert child.returncode == 17, (child.stdout,child.stderr)
        f = Fixture(folder)
        try:
            snapshot_state(f, n)
            assert f.q(f"SELECT count(*) FROM {preflight(n)} WHERE NOT ready AND target_state='dirty'") == 1
            unchanged(f, n)
            f.q(f"ATTACH 'differential:{n}' AS recovered (BRANCH 'o');")
            assert f.q('SELECT count(*) FROM recovered.x WHERE id=99') == 1
            f.q('DETACH recovered;')
        finally: f.host.close()
        ok('preflight preserves committed native WAL and reports dirty target after process death')
    evidence_path('merge-preflight-integration.json').write_text(json.dumps({'passed':passed},indent=2)+'\n')


if __name__ == '__main__': main()
