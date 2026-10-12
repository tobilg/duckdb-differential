#!/usr/bin/env python3
"""Packed exports, incremental publication, deferred export, local eviction with S3
read-through and rehydration, and coalesced remote reads.
Requires the disposable MinIO namespace initialized by minio.py's SDK fixture."""
import csv
import io
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
from evidence import evidence_path
ROOT=Path(__file__).resolve().parents[2]
import sys as _sys
_sys.path.insert(0,str(ROOT/'test/integration'))
from paths import DUCKDB_CLI, EXTENSION, DUCKDB_LIBRARY, BUILD_DIR

CLI=DUCKDB_CLI
EXT=EXTENSION
endpoint,base=sys.argv[1:];base=Path(base)/'tiering';base.mkdir(exist_ok=False)
results=[]
def runner(root,extension=EXT):
    def run(sql,expected=0):
        r=subprocess.run([CLI,'-init','/dev/null','-unsigned','-batch','-bail','-csv'],input=f"LOAD '{extension}';"+sql,text=True,capture_output=True,env=dict(os.environ,DIFFERENTIAL_STORE_ROOT=str(root)),timeout=180)
        assert r.returncode==expected,(sql,r.returncode,r.stdout,r.stderr)
        return r.stdout if expected==0 else r.stderr
    return run
HEADERS=('operation_id,','metric,value','export_locator','snapshot_id,','n,','bad,','changed_rows','payload_value','total')
def last_rows(text):
    """Rows of the final CSV result block (each statement prints its own header line)."""
    lines=text.splitlines()
    starts=[i for i,l in enumerate(lines) if l.startswith(HEADERS)]
    return list(csv.DictReader(io.StringIO('\n'.join(lines[starts[-1]:]))))
def metric(text,name):
    lines=text.splitlines();start=max(i for i,l in enumerate(lines) if l=='metric,value')
    for row in csv.DictReader(io.StringIO('\n'.join(lines[start:]))):
        if row['metric']==name:return int(row['value'])
    raise KeyError(name)
def scalar(root,sql):
    with sqlite3.connect(Path(root)/'engine.sqlite') as db:return db.execute(sql).fetchone()[0]
def passed(name):
    results.append(name);print('PASS',name,flush=True)
S3=f"ENDPOINT '{endpoint}', PATH_STYLE true, ALLOW_HTTP true"
BUCKET='s3://differential-sdk-test/'
writer=base/'writer';run=runner(writer)
attach="ATTACH 'differential:sales' AS d;"
stats="SELECT metric,value FROM differential_storage_stats();"
first=run(f"ATTACH 'differential:sales' AS d (DATA_PATH '{BUCKET}tiering/', {S3}); CREATE TABLE d.t AS SELECT range AS id, sha256(range::VARCHAR) AS payload FROM range(300000); CALL differential_create_snapshot('d',name:='base');"+stats)
packs,uploaded=metric(first,'export_packs_uploaded'),metric(first,'export_chunks_uploaded')
assert packs>0 and uploaded>packs and metric(first,'s3_put_calls')<packs+6,(packs,uploaded)
descriptor=last_rows(run("SELECT export_locator FROM differential_snapshots('sales') WHERE snapshot_id=1;"))[0]['export_locator']
passed(f'initial export packs {uploaded} chunks into {packs} immutable packs ({metric(first,"s3_put_calls")} PUTs)')
second=run(attach+"UPDATE d.t SET payload='changed' WHERE id=5; CALL differential_create_snapshot('d');"+stats)
# Exactly the chunks of snapshot 2 that no earlier export stored are uploaded.
new_chunks=scalar(writer,"""SELECT count(DISTINCT e.object) FROM snapshots s JOIN root_pages r ON r.root=s.root JOIN page_entries e ON e.page=r.page
WHERE s.id=2 AND e.object NOT IN (SELECT e1.object FROM snapshots s1 JOIN root_pages r1 ON r1.root=s1.root JOIN page_entries e1 ON e1.page=r1.page WHERE s1.id=1)""")
assert metric(second,'export_chunks_uploaded')==new_chunks and metric(second,'export_chunks_reused')>0,(metric(second,'export_chunks_uploaded'),new_chunks,metric(second,'export_chunks_reused'))
assert metric(second,'s3_head_calls')<=8,metric(second,'s3_head_calls')
passed(f'incremental export uploads {metric(second,"export_chunks_uploaded")} changed chunks and reuses {metric(second,"export_chunks_reused")} without per-object HEADs')
deferred=last_rows(run(attach+"UPDATE d.t SET payload='later' WHERE id=6; CALL differential_create_snapshot('d',export_mode:='deferred',request_id:='deferred');"))[0]
assert deferred['export_state']=='pending' and deferred['snapshot_id']=='3',deferred
assert last_rows(run("CALL differential_retry_export('sales',3,request_id:='finish-deferred');"))[0]['export_state']=='complete'
passed('deferred export returns a pending local snapshot; retry completes the same snapshot')
# Local eviction of a remotely durable snapshot; reads go through S3.
err=runner(base/'local-only')("ATTACH 'differential:l' AS l; CREATE TABLE l.t AS SELECT 1 AS i; CALL differential_create_snapshot('l'); CALL differential_evict_snapshots('l',versions:=[1]);",1)
assert 'unsupported_provider_capability' in err,err
run("CALL differential_evict_snapshots('sales',versions:=[1],request_id:='evict-1'); SELECT * FROM differential_cleanup_old_files(max_objects:=10000);")
assert run("CALL differential_evict_snapshots('sales',versions:=[1],request_id:='evict-1');").count('\n')==2
base_root=scalar(writer,"SELECT root FROM snapshots WHERE id=1")
assert scalar(writer,f"SELECT count(*) FROM reclaimed_roots WHERE root='{base_root}'")==1
state=last_rows(run("SELECT snapshot_id,publication_state FROM differential_snapshots('sales') WHERE snapshot_id=1;"))[0]
assert state['publication_state']=='local_evicted',state
read=run("ATTACH 'differential:sales' AS old (SNAPSHOT_VERSION 1); SELECT count(*) AS n, sum(id) AS total, count(*) FILTER (WHERE payload!=sha256(id::VARCHAR)) AS bad FROM old.t;"+stats)
assert last_rows(read.split('metric,value')[0])[0]=={'n':'300000','total':'44999850000','bad':'0'},read[:400]
assert metric(read,'s3_get_calls')>0 and metric(read,'remote_coalesced_requests')>0,(metric(read,'s3_get_calls'),metric(read,'remote_coalesced_requests'))
passed(f'evicted snapshot is collected locally and reads through S3 ({metric(read,"s3_get_calls")} GETs, {metric(read,"remote_coalesced_requests")} coalesced ranges, {metric(read,"remote_prefetched_chunks")} prefetched chunks)')
# Branching from and restoring to an evicted snapshot rehydrates its exact root.
run("CALL differential_create_branch('sales','from_base',snapshot:=1,request_id:='branch-evicted');")
assert last_rows(run("ATTACH 'differential:sales' AS b (BRANCH 'from_base'); SELECT count(*) FILTER (WHERE payload!=sha256(id::VARCHAR)) AS bad, count(*) AS n FROM b.t;"))[0]=={'bad':'0','n':'300000'}
run("ATTACH 'differential:sales' AS b (BRANCH 'from_base'); UPDATE b.t SET payload='branch' WHERE id=1;")
assert scalar(writer,f"SELECT count(*) FROM reclaimed_roots WHERE root='{base_root}'")==0
restored=last_rows(run("CALL differential_restore('sales',1,branch:='main',request_id:='restore-evicted');"))[0]
assert last_rows(run(attach+"SELECT count(*) FILTER (WHERE payload='changed') AS changed_rows FROM d.t;"))[0]['changed_rows']=='0',restored
passed('branch and restore from an evicted snapshot rehydrate the exact immutable root from S3')
# A retired snapshot whose export completed is locally collectible; its export stays readable.
run("CALL differential_expire_snapshots('sales',versions:=[2]); SELECT * FROM differential_cleanup_old_files(max_objects:=10000);")
retired_root=scalar(writer,"SELECT root FROM snapshots WHERE id=2")
assert scalar(writer,f"SELECT count(*) FROM reclaimed_roots WHERE root='{retired_root}'")==1
locator=last_rows(run("SELECT export_locator FROM differential_snapshots('sales') WHERE snapshot_id=2;"))[0]['export_locator']
reader=base/'reader';rrun=runner(reader)
assert last_rows(rrun(f"ATTACH 'differential:{BUCKET}tiering/{locator}' AS two ({S3}); SELECT payload AS payload_value FROM two.t WHERE id=5;"))[0]['payload_value']=='changed'
passed('retired snapshot with a completed export releases local payload; its export remains readable')
# Descriptors record the writer's DuckDB version but readers do not require a match: an export
# written by another engine version attaches and recovers if DuckDB can open the native file.
foreign=base/'foreign-writer'
def foreign_run(sql):
    r=subprocess.run([CLI,'-init','/dev/null','-unsigned','-batch','-bail','-csv'],input=f"LOAD '{EXT}';"+sql,text=True,capture_output=True,
                     env=dict(os.environ,DIFFERENTIAL_STORE_ROOT=str(foreign),DIFFERENTIAL_TEST_ENGINE_LINE='duckdb-v9.9.9'),timeout=180)
    assert r.returncode==0,(sql,r.stdout,r.stderr);return r.stdout
foreign_locator=last_rows(foreign_run(f"ATTACH 'differential:other' AS o (DATA_PATH '{BUCKET}tiering-engine/', {S3}); CREATE TABLE o.t AS SELECT range AS id FROM range(1000); SELECT export_locator FROM differential_create_snapshot('o');"))[0]['export_locator']
engine_reader=base/'engine-reader';erun=runner(engine_reader)
assert last_rows(erun(f"ATTACH 'differential:{BUCKET}tiering-engine/{foreign_locator}' AS f ({S3}); SELECT sum(id) AS total FROM f.t;"))[0]['total']=='499500'
erun(f"CALL differential_recover_export('{BUCKET}tiering-engine/{foreign_locator}','from_other_engine',endpoint:='{endpoint}',path_style:=true,allow_http:=true);")
assert last_rows(erun("ATTACH 'differential:from_other_engine' AS r; SELECT sum(id) AS total FROM r.t;"))[0]['total']=='499500'
passed('an export whose descriptor names another DuckDB version (duckdb-v9.9.9) attaches and recovers')
evidence_path('s3-tiering.json').write_text(json.dumps({'scope':'disposable pinned MinIO; no real AWS','passed':results},indent=2)+'\n')
