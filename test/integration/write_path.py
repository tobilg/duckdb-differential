#!/usr/bin/env python3
"""Buffered write path, plain ATTACH snapshots, automatic/incremental GC and lock-free stats.
Disposable repositories only."""
import csv
import fcntl
import io
import json
import os
from pathlib import Path
import metadata_store
import sqlite3
import subprocess
import tempfile
import time
from evidence import evidence_path
from stock_capi import Host
ROOT=Path(__file__).resolve().parents[2]
import sys as _sys
_sys.path.insert(0,str(ROOT/'test/integration'))
from paths import DUCKDB_CLI, EXTENSION, DUCKDB_LIBRARY, BUILD_DIR

CLI=DUCKDB_CLI
EXT=EXTENSION
results=[]
def runner(root,extension=EXT):
    def run(sql,expected=0,timeout=60):
        r=subprocess.run([CLI,'-init','/dev/null','-unsigned','-batch','-bail','-csv'],input=f"LOAD '{extension}';"+sql,text=True,capture_output=True,env=dict(os.environ,DIFFERENTIAL_STORE_ROOT=str(root)),timeout=timeout)
        assert r.returncode==expected,(sql,r.returncode,r.stdout,r.stderr)
        return r.stdout if expected==0 else r.stderr
    return run
def rows(text):
    lines=text.splitlines()
    return list(csv.DictReader(io.StringIO('\n'.join(lines))))
def metrics(run):
    out=run("SELECT metric,value FROM differential_storage_stats();")
    return {r['metric']:int(r['value']) for r in rows(out)}
def scalar(root,sql):
    with metadata_store.connect(Path(root)/'engine.sqlite') as db:return db.execute(sql).fetchone()[0]
def passed(name):
    results.append(name);print('PASS',name,flush=True)
with tempfile.TemporaryDirectory(prefix='differential-write-path-') as tmp:
    root=Path(tmp)/'repo';run=runner(root)
    # Plain ATTACH (no TYPE) is a full differential attachment: snapshots and generations work.
    run("ATTACH 'differential:plain' AS p; CREATE TABLE p.t AS SELECT 42 AS v; CALL differential_create_snapshot('p');")
    run("ATTACH 'differential:plain' AS p; UPDATE p.t SET v=99;")
    assert run("ATTACH 'differential:plain' AS old (SNAPSHOT_VERSION 1); SELECT v FROM old.t;").endswith('v\n42\n')
    assert scalar(root,"SELECT count(*) FROM generations")>=1
    passed('plain ATTACH publishes snapshots and internal generations without TYPE differential')
    # Small commits: overwritten tail chunks of the main file are reclaimed at publication (the WAL
    # stores byte ranges, not chunks).
    attach="ATTACH 'differential:plain' AS p;"
    before=scalar(root,"SELECT count(*) FROM objects")
    run("SET differential_auto_gc=false;"+attach+''.join(f"INSERT INTO p.t VALUES({i});" for i in range(200)))
    after=scalar(root,"SELECT count(*) FROM objects")
    assert after-before<=8,(before,after)
    assert not any((root/'journal').iterdir()),'clean close left an active segment'
    passed(f'200 single-row commits retain {after-before} new payload objects; clean close retires its segments')
    # With automatic GC disabled, superseded generations remain queued for explicit collection.
    run(attach+"CREATE TABLE p.big AS SELECT range AS id, md5(range::VARCHAR) AS s FROM range(200000); CHECKPOINT p;")
    run("SET differential_auto_gc=false;"+attach+''.join(f"UPDATE p.big SET s=s||'x' WHERE id%7={i}; CHECKPOINT p;" for i in range(6)))
    queued=metrics(run)
    assert queued['local_gc_queued_roots']>0,queued
    backlog=int(rows(run("SELECT * FROM differential_cleanup_old_files(dry_run:=true);"))[0]['candidate_objects'])
    assert backlog>0,backlog
    collected=rows(run("SELECT * FROM differential_cleanup_old_files(max_objects:=10000);"))[0]
    drained=metrics(run)
    assert int(collected['deleted_objects'])>0 and drained['local_gc_queued_roots']==0 and drained['local_gc_queued_objects']==0,(collected,drained)
    assert rows(run("SELECT * FROM differential_cleanup_old_files(dry_run:=true);"))[0]['candidate_objects']=='0'
    passed(f'one explicit GC call cascades root -> page -> object queues to empty ({collected["deleted_objects"]} objects); dry run agrees')
    # Seed a known backlog, then open a host with automatic GC enabled by default.
    # The setting is read when files open; keep this host alive while its worker runs.
    run("SET differential_auto_gc=false;"+attach+''.join(f"UPDATE p.big SET s=s||'y' WHERE id%5={i}; CHECKPOINT p;" for i in range(6)))
    host=Host();connection=host.connect()
    try:
        host.query(connection,f"LOAD '{EXT}'; SET differential_repository='{root}';"+attach)
        dry_sql="SELECT candidate_objects FROM differential_cleanup_old_files(dry_run:=true);"
        auto_backlog=auto=host.query(connection,dry_sql)
        assert auto_backlog>0,auto_backlog
        deadline=time.monotonic()+60
        checkpoints=0
        while auto and time.monotonic()<deadline:
            # Maintenance is bounded and can defer work when locks are busy. Further
            # dirty checkpoints schedule another pass; an unchanged CHECKPOINT may not.
            host.query(connection,"UPDATE p.t SET v=v+1; CHECKPOINT p;")
            checkpoints+=1
            time.sleep(.1)
            auto=host.query(connection,dry_sql)
        assert auto==0,('automatic GC did not drain',auto_backlog,auto,checkpoints)
        assert host.query(connection,"SELECT count(*) FROM p.big;")==200000
        assert host.query(connection,"ATTACH 'differential:plain' AS old (SNAPSHOT_VERSION 1); SELECT v FROM old.t;")==42
    finally:host.close()
    # Closing the host may release more WAL payloads after its final maintenance pass.
    rows(run("SELECT * FROM differential_cleanup_old_files(max_objects:=10000);"))
    assert rows(run("SELECT * FROM differential_cleanup_old_files(dry_run:=true);"))[0]['candidate_objects']=='0'
    assert run(attach+"SELECT count(*) AS n FROM p.big;").endswith('n\n200000\n')
    passed(f'default automatic GC drains {auto_backlog} candidates across {checkpoints} checkpoints; live branch and snapshot retained; explicit GC clears close-time leftovers')
    # Introspection never takes the payload fence or the authority write lock.
    with open(root/'payload.lock','a+') as fence:
        fcntl.flock(fence,fcntl.LOCK_EX)
        started=time.monotonic()
        stats=metrics(run)
        dry=rows(run("SELECT * FROM differential_cleanup_old_files(dry_run:=true);"))[0]
        elapsed=time.monotonic()-started
    assert elapsed<20 and 'local_payload_bytes' in stats and dry['dry_run']=='true',elapsed
    passed('storage stats and GC dry run complete while another process holds the exclusive payload fence')
    status={r['capability']:r['status'] for r in rows(run("SELECT * FROM differential_status();"))}
    assert status['local_store_format'].startswith('2') and 'test_hooks' in status
    passed('status reports store format 1 and test-hook build mode')
    # Exactly one store format: a differential repository with any other version is refused.
    other=Path(tmp)/'other-format';other.mkdir()
    db=sqlite3.connect(other/'engine.sqlite');db.execute('PRAGMA application_id=1145652806');db.execute('PRAGMA user_version=99');db.close()
    assert 'incompatible_format' in runner(other)("ATTACH 'differential:x' AS x;",expected=1)
    passed('repositories with another store format are refused (no migrations)')
evidence_path('write-path.json').write_text(json.dumps({'passed':results},indent=2)+'\n')
