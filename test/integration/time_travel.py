#!/usr/bin/env python3
"""Catalog-scoped functions and AT (VERSION/TIMESTAMP) time travel: resolution,
views, transactions, branches, errors, reader pins, LRU eviction, prepared-statement rebind and
concurrent connections. Disposable repositories only."""
import json
import os
from pathlib import Path
import random
import subprocess
import sys
import tempfile
import threading
from evidence import evidence_path
ROOT=Path(__file__).resolve().parents[2]
import sys as _sys
_sys.path.insert(0,str(ROOT/'test/integration'))
from paths import DUCKDB_CLI, EXTENSION, DUCKDB_LIBRARY, BUILD_DIR

sys.path.insert(0,str(ROOT/'test/integration'))
from stock_capi import Host
CLI=DUCKDB_CLI
EXT=EXTENSION
results=[]
def passed(name):
    results.append(name);print('PASS',name,flush=True)
with tempfile.TemporaryDirectory(prefix='differential-time-travel-') as tmp:
    repo=Path(tmp)/'repo'
    def run(sql,expected=0):
        r=subprocess.run([CLI,'-init','/dev/null','-unsigned','-batch','-bail','-json'],
                         input=f"LOAD '{EXT}'; SET differential_repository='{repo}';"+sql,text=True,capture_output=True,timeout=60)
        assert r.returncode==expected,(sql,r.returncode,r.stdout,r.stderr)
        return r.stdout if expected==0 else r.stderr
    def blocks(sql):
        """One list of row dicts per statement that returned a result."""
        out,text,decoder=[],run(sql).strip(),json.JSONDecoder()
        while text:
            value,end=decoder.raw_decode(text);out.append(value);text=text[end:].lstrip()
        return out
    def rows(sql):return blocks(sql)[-1]
    sales="ATTACH 'differential:sales' AS sales;"
    # History: v=1 (snapshot 1), v=2 (2), v=3 (3); live v=4. A view captures the schema too.
    run(sales+"CREATE TABLE sales.t AS SELECT 1 AS v; CREATE VIEW sales.tv AS SELECT v*10 AS w FROM t;"
        "CALL differential_create_snapshot('sales',name:='one');"
        "UPDATE sales.t SET v=2; CALL differential_create_snapshot('sales');"
        "ALTER TABLE sales.t ADD COLUMN extra INT DEFAULT 7; UPDATE sales.t SET v=3; CALL differential_create_snapshot('sales');"
        "UPDATE sales.t SET v=4;")
    times={r['snapshot_id']:r['snapshot_time'] for r in rows("SELECT snapshot_id,snapshot_time FROM differential_snapshots('sales');")}
    # Catalog-scoped functions.
    assert rows(sales+"SELECT snapshot_id,snapshot_name FROM sales.snapshots();")==[{'snapshot_id':1,'snapshot_name':'one'},{'snapshot_id':2,'snapshot_name':None},{'snapshot_id':3,'snapshot_name':None}]
    assert rows(sales+"FROM sales.current_snapshot();")==[{'id':3}]
    assert rows(sales+"SELECT branch_name FROM sales.branches();")==[{'branch_name':'main'}]
    assert rows(sales+"USE sales; SELECT count(*) AS n FROM snapshots();")==[{'n':3}]
    assert rows(sales+"ATTACH 'differential:sales' AS old (SNAPSHOT_VERSION 1); FROM old.current_snapshot();")==[{'id':1}]
    assert rows(sales+"ATTACH 'differential:other' AS other; SELECT count(*) AS n FROM other.snapshots();")==[{'n':0}]
    funcs=rows(sales+"SELECT function_name,internal FROM duckdb_functions() WHERE database_name='sales' ORDER BY 1;")
    assert funcs==[{'function_name':n,'internal':True} for n in ('branches','current_snapshot','snapshots')],funcs
    uri=rows("SELECT uri FROM differential_branches('sales');")[0]['uri']
    assert rows(sales+f"CHECKPOINT sales; DETACH sales; ATTACH '{uri}' AS plain; SELECT count(*) AS n FROM duckdb_functions() WHERE database_name='plain';")==[{'n':0}]
    passed('sales.snapshots()/current_snapshot()/branches() are generated per catalog, internal, and absent from the native file')
    shadow=rows("ATTACH 'differential:shadow' AS s; SELECT uri FROM differential_branches('shadow');")[0]['uri']
    run(f"ATTACH '{shadow}' AS p; CREATE MACRO p.current_snapshot() AS TABLE SELECT 42 AS id;")
    assert rows("ATTACH 'differential:shadow' AS s; FROM s.current_snapshot();")==[{'id':42}]
    assert 'already exists' in run(sales+"CREATE MACRO sales.snapshots() AS TABLE SELECT 1;",1)
    run(sales+"CREATE TABLE sales.snapshots AS SELECT 1 AS a; DROP TABLE sales.snapshots;")
    passed('existing user objects shadow generated functions; new table macros cannot take the names; tables can')
    # AT clauses.
    got=rows(sales+"SELECT 'live' AS k,v FROM sales.t UNION ALL SELECT 'v1',v FROM sales.t AT (VERSION => 1) "
             "UNION ALL SELECT 'v2',v FROM sales.t AT (VERSION => 2) ORDER BY k;")
    assert got==[{'k':'live','v':4},{'k':'v1','v':1},{'k':'v2','v':2}],got
    assert rows(sales+f"SELECT v FROM sales.t AT (TIMESTAMP => TIMESTAMPTZ '{times[2]}');")==[{'v':2}]
    assert rows(sales+"SELECT v FROM sales.t AT (TIMESTAMP => now());")==[{'v':3}]
    assert rows(sales+"SELECT * FROM sales.t AT (VERSION => 2);")==[{'v':2}]  # schema of the snapshot
    assert rows(sales+"SELECT * FROM sales.t AT (VERSION => 3);")==[{'v':3,'extra':7}]
    assert rows(sales+"SELECT w FROM sales.tv AT (VERSION => 1);")==[{'w':10}]
    assert rows(sales+"USE sales; SELECT v FROM t AT (VERSION => 1);")==[{'v':1}]
    assert rows(sales+"ATTACH 'differential:sales' AS old (SNAPSHOT_VERSION 1); SELECT v FROM old.t AT (VERSION => 2);")==[{'v':2}]
    passed('AT (VERSION/TIMESTAMP) reads the snapshot schema, data and views; qualified, unqualified and from snapshot attachments')
    got=blocks(sales+"BEGIN; UPDATE sales.t SET v=5; SELECT v FROM sales.t AT (VERSION => 1); "
               "CREATE TABLE sales.copy AS SELECT v FROM sales.t AT (VERSION => 2); COMMIT; SELECT v FROM sales.t; SELECT v FROM sales.copy;")
    assert [b for b in got if b]==[[{'v':1}],[{'v':5}],[{'v':2}]],got
    passed('AT inside a write transaction; CTAS from a snapshot into the live database')
    run(sales+"CALL differential_create_branch('sales','dev',snapshot:=1); ATTACH 'differential:sales' AS dev (BRANCH 'dev');"
        "UPDATE dev.t SET v=100; CALL differential_create_snapshot('dev');")
    assert rows(sales+"ATTACH 'differential:sales' AS dev (BRANCH 'dev'); SELECT v FROM dev.t AT (TIMESTAMP => now());")==[{'v':100}]
    assert rows(sales+"SELECT v FROM sales.t AT (TIMESTAMP => now());")==[{'v':3}]
    assert rows(sales+"ATTACH 'differential:sales' AS dev (BRANCH 'dev'); SELECT v FROM dev.t AT (VERSION => 2);")==[{'v':2}]
    passed('TIMESTAMP resolves within the attached branch; VERSION addresses any snapshot of the database')
    for sql,error in [("SELECT * FROM sales.t AT (VERSION => 99);",'not_found'),
                      ("SELECT * FROM sales.t AT (TIMESTAMP => TIMESTAMP '1970-01-02');",'not_found'),
                      ("SELECT * FROM sales.t AT (VERSION => 1); CALL differential_expire_snapshots('sales',versions:=[1]); SELECT * FROM sales.t AT (VERSION => 1);",'retired'),
                      ("INSERT INTO sales.t AT (VERSION => 2) VALUES (1);",'syntax error')]:
        assert error in run(sales+sql,1),(sql,run(sales+sql,1))
    passed('missing, retired and pre-history snapshots are rejected; AT is read-only')
    # Reader pins: taken at bind (also by PREPARE/EXPLAIN), released on DETACH of the live database.
    pins="SELECT live_pins FROM differential_cleanup_old_files(dry_run:=true);"
    got=blocks(sales+pins+"PREPARE q AS SELECT * FROM sales.t AT (VERSION => 2);"+pins+"DETACH sales;"+pins)
    counts=[b[0]['live_pins'] for b in got if b and 'live_pins' in b[0]]
    assert counts==[1,2,0],got
    passed('AT takes a reader pin while binding (PREPARE without EXECUTE); DETACH releases it')
    # Bounded hidden attachments, prepared statements across eviction, concurrent connections.
    run("ATTACH 'differential:many' AS m; CREATE TABLE m.t(v INT); "+''.join(
        f"INSERT INTO m.t VALUES ({i}); CALL differential_create_snapshot('m');" for i in range(1,13)))
    os.environ['DIFFERENTIAL_STORE_ROOT']=str(repo)
    host=Host()
    try:
        conns=[host.connect() for _ in range(8)]
        host.query(conns[0],f"LOAD '{EXT}'; ATTACH 'differential:many' AS m;")
        live=lambda:host.query(conns[0],pins)
        host.query(conns[0],"PREPARE p AS SELECT sum(v) FROM m.t AT (VERSION => 1);")
        assert host.query(conns[0],"EXECUTE p;")==1
        for i in range(2,13):
            assert host.query(conns[0],f"SELECT sum(v) FROM m.t AT (VERSION => {i});")==i*(i+1)//2
        assert live()==1+8,live()  # live branch + capacity
        assert host.query(conns[0],"EXECUTE p;")==1  # evicted snapshot rebinds and reopens
        errors=[]
        def worker(conn,seed):
            rnd=random.Random(seed)
            try:
                for _ in range(40):
                    i=rnd.randint(1,12)
                    assert host.query(conn,f"SELECT sum(v) FROM m.t AT (VERSION => {i});")==i*(i+1)//2
            except Exception as e:errors.append(repr(e))
        threads=[threading.Thread(target=worker,args=(c,n)) for n,c in enumerate(conns)]
        for t in threads:t.start()
        for t in threads:t.join()
        assert not errors,errors[:3]
        assert live()<=1+8
        # A transaction keeps reading a snapshot another connection evicts; the evicted attachment and
        # its reader pin live until that transaction ends.
        host.query(conns[1],"BEGIN; SELECT sum(v) FROM m.t AT (VERSION => 1);")
        for i in range(2,12):
            host.query(conns[2],f"SELECT sum(v) FROM m.t AT (VERSION => {i});")
        assert host.query(conns[1],"SELECT sum(v) FROM m.t AT (VERSION => 1);")==1
        held=live()
        assert held==1+8+1,held  # live branch + capacity + the evicted one still in use
        host.query(conns[1],"COMMIT;")
        assert live()==1+8,live()
        host.query(conns[0],"DETACH m;")
        assert live()==0
    finally:host.close()
    passed('hidden snapshot attachments stay within 8 per instance; prepared statements rebind after eviction; 8 connections x 40 random versions; an evicted snapshot stays readable and pinned until its transaction ends')
evidence_path('time-travel.json').write_text(json.dumps({'passed':results},indent=2)+'\n')
