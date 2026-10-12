#!/usr/bin/env python3
"""Public local control lifecycle through the unmodified stock CLI."""
import csv
import io
import json
import os
from pathlib import Path
import subprocess
import tempfile
from evidence import evidence_path

ROOT = Path(__file__).resolve().parents[2]
import sys as _sys
_sys.path.insert(0,str(ROOT/'test/integration'))
from paths import DUCKDB_CLI, EXTENSION, DUCKDB_LIBRARY, BUILD_DIR

BINARY = DUCKDB_CLI
EXT = EXTENSION
results = []
with tempfile.TemporaryDirectory(prefix='differential-lifecycle-') as tmp:
    env = dict(os.environ, DIFFERENTIAL_STORE_ROOT=tmp)
    def run(sql, ok=True, extra=None):
        r = subprocess.run([BINARY, '-init', '/dev/null', '-unsigned', '-batch', '-csv', '-bail'],
                           input=f"LOAD '{EXT}';\n" + sql, text=True, capture_output=True,
                           env=dict(env, **(extra or {})), timeout=30)
        if ok and r.returncode: raise AssertionError((sql, r.stdout, r.stderr))
        if not ok and not r.returncode: raise AssertionError(('unexpected success', sql, r.stdout))
        return r
    def rows(sql): return list(csv.DictReader(io.StringIO(run(sql).stdout)))
    def passed(name):
        results.append({'probe': name, 'status': 'passed'})
        print('PASS', name, flush=True)
    # ATTACH creates a database on first use (CREATE_IF_NOT_EXISTS) and is idempotent by name.
    run("ATTACH 'differential:sales' AS sales; DETACH sales; ATTACH 'differential:sales' AS sales;")
    first = rows("SELECT database_id FROM differential_databases() WHERE database_name='sales';")[0]
    assert rows("SELECT count(*) AS n FROM differential_branches('sales');")[0]['n'] == '1'
    run("ATTACH 'differential:missing' AS m (CREATE_IF_NOT_EXISTS false);", False)
    attach = "ATTACH 'differential:sales' AS sales;"
    snap = "CALL differential_create_snapshot('sales', name:='baseline',request_id:='snapshot-baseline');"
    original = rows(attach + "CREATE TABLE sales.t(i INTEGER PRIMARY KEY, v VARCHAR); INSERT INTO sales.t VALUES(1,'old');" + snap)[0]
    assert original['snapshot_id'] == '1'
    assert rows(attach + snap)[0] == original
    passed('public identity and fresh-process request retry')
    unchanged = rows(attach + "CALL differential_create_snapshot('sales',request_id:='unchanged');")[0]
    assert unchanged['snapshot_id'] == '2'
    snapshots = rows("SELECT * FROM differential_snapshots('sales');")
    assert snapshots[0]['root'] == snapshots[1]['root']
    run(attach + 'CHECKPOINT sales;')
    assert len(rows("SELECT * FROM differential_snapshots('sales');")) == 2
    run(attach + "CALL differential_create_snapshot('sales',name:='different',request_id:='snapshot-baseline');",False)
    passed('unchanged publication gets new ID and shared payload; request mismatch rejected')
    branch = rows("CALL differential_create_branch('sales','experiment',snapshot:=1,request_id:='branch');")[0]
    clone = rows("CALL differential_clone_database('sales','clone',snapshot:=1,request_id:='clone');")[0]
    assert clone['snapshot_id'] == '1' and clone['database_id'] != first['database_id']
    exp = f"ATTACH '{branch['uri']}' AS experiment (TYPE differential);"
    experiment = rows(exp + "UPDATE experiment.t SET v='experiment'; CALL differential_create_snapshot('experiment');")[0]
    assert experiment['snapshot_id'] == '3'
    run(attach + "ALTER TABLE sales.t RENAME v TO value; UPDATE sales.t SET value='new';")
    historical = f"ATTACH '{original['uri']}' AS historical (TYPE differential, READ_ONLY);"
    assert rows(historical + 'SELECT * FROM historical.t;') == [{'i':'1','v':'old'}]
    assert rows("ATTACH 'differential:clone' AS c; SELECT * FROM c.t;") == [{'i':'1','v':'old'}]
    passed('branch allocator, independent clone, historical native schema and data')
    run("CALL differential_expire_snapshots('sales',versions:=[1],request_id:='retire');")
    run(historical,False)
    run(attach + "CALL differential_create_snapshot('sales',name:='baseline');",False)
    assert rows("ATTACH 'differential:clone' AS c; SELECT * FROM c.t;") == [{'i':'1','v':'old'}]
    run("ATTACH 'differential:sales' AS retired (SNAPSHOT_NAME 'baseline');",False)
    passed('retirement hides IDs/names, reserves names and preserves clone')
    before = rows("SELECT count(*) AS n FROM differential_operations();")[0]['n']
    run("EXPLAIN SELECT * FROM differential_clone_database('sales','never',snapshot:=2); PREPARE p AS SELECT * FROM differential_clone_database('sales','never',snapshot:=2);")
    after = rows("SELECT count(*) AS n FROM differential_operations();")[0]['n']
    assert before == after
    run("BEGIN; CALL differential_clone_database('sales','explicit',snapshot:=2);",False)
    run("EXPLAIN ANALYZE SELECT * FROM differential_clone_database('sales','analyzed',snapshot:=2);")
    assert rows("SELECT count(*) AS n FROM differential_branches('analyzed');")[0]['n'] == '1'
    passed('bind/plan no effects; autocommit enforcement; EXPLAIN ANALYZE executes')
    # Both timestamp fields are visible; branch creation has no earlier history.
    created = rows("SELECT created_at FROM differential_branches('sales') WHERE branch_name='experiment';")[0]['created_at']
    run("ATTACH 'differential:sales' AS t0 (BRANCH 'experiment', SNAPSHOT_TIME '1970-01-01');",False)
    assert rows("ATTACH 'differential:sales' AS t1 (BRANCH 'experiment', SNAPSHOT_TIME '2100-01-01'); SELECT * FROM t1.t;") == [{'i':'1','v':'experiment'}]
    assert rows("ATTACH 'differential:sales' AS e (BRANCH 'experiment'); SELECT * FROM differential_current_snapshot('e');") == [{'id':'3'}]
    passed('timestamp resolution remains within branch')
    restore = "CALL differential_restore('sales',2,branch:='main',request_id:='restore-main');"
    restored = rows(restore)[0]
    assert restored['preservation_snapshot_id'] == '4' and restored['snapshot_id'] == '5', restored
    assert rows(restore)[0] == restored
    assert rows(attach + 'SELECT * FROM sales.t;') == [{'i':'1','v':'old'}]
    preserved = rows("ATTACH 'differential:sales' AS p (SNAPSHOT_VERSION 4); SELECT * FROM p.t;")
    assert preserved == [{'i':'1','value':'new'}]
    run(attach + "CALL differential_restore('sales',2,branch:='main');",False)
    passed('detached restore preserves committed schema/data, advances IDs and retries once')
    drop = "CALL differential_drop_branch('sales','experiment',request_id:='drop-experiment');"
    dropped = rows(drop)[0]
    assert rows(drop)[0] == dropped
    assert dropped['preservation_snapshot_id'] == '3'
    run(exp,False)
    replacement = rows("CALL differential_create_branch('sales','experiment',snapshot:=2);")[0]
    assert replacement['branch_id'] != branch['branch_id']
    run(exp,False)
    run("CALL differential_drop_branch('clone','main');",False)
    passed('branch deletion, name reuse with new UUID, old canonical URI and last-branch rejection')
    run("CALL differential_drop_branch('sales','main',request_id:='drop-source'); CALL differential_expire_snapshots('sales',versions:=[2]); CALL differential_cleanup_old_files();")
    assert rows("ATTACH 'differential:clone' AS c; SELECT * FROM c.t;") == [{'i':'1','v':'old'}]
    run("ATTACH 'differential:clone' AS c; UPDATE c.t SET v='independent';")
    assert rows("ATTACH 'differential:sales' AS sibling (BRANCH 'experiment'); SELECT * FROM sibling.t;") == [{'i':'1','v':'old'}]
    passed('clone survives source branch deletion and retirement/GC; sibling remains isolated')
evidence_path('public-lifecycle.json').write_text(json.dumps(results,indent=2)+'\n')
