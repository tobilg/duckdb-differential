#!/usr/bin/env python3
"""Native reader pins and bounded, retryable local collection."""
import json
import csv
import io
import os
from pathlib import Path
import sqlite3
import subprocess
import tempfile
from stock_capi import Host
from evidence import evidence_path
ROOT=Path(__file__).resolve().parents[2]
import sys as _sys
_sys.path.insert(0,str(ROOT/'test/integration'))
from paths import DUCKDB_CLI, EXTENSION, DUCKDB_LIBRARY, BUILD_DIR

EXT=EXTENSION
results=[]
with tempfile.TemporaryDirectory(prefix='differential-retention-') as tmp:
    os.environ['DIFFERENTIAL_STORE_ROOT']=tmp
    host=Host();c=host.connect()
    try:
        # Explicit GC semantics are measured by object counts: keep the background collector
        # (automatic maintenance after checkpoints) out of this host session.
        host.query(c,f"LOAD '{EXT}'; SET differential_auto_gc=false; ATTACH 'differential:sales' AS d;")
        host.query(c,"CREATE TABLE d.t AS SELECT range AS id, sha256(range::VARCHAR) AS payload FROM range(12000); CALL differential_create_snapshot('d',request_id:='first');")
        host.query(c,"ATTACH 'differential:sales' AS old (SNAPSHOT_VERSION 1);")
        host.query(c,"DROP TABLE d.t; CREATE TABLE d.t AS SELECT 99 AS id; CALL differential_create_snapshot('d'); CALL differential_expire_snapshots('sales',versions:=[1]);")
        with sqlite3.connect(Path(tmp)/'engine.sqlite') as m:
            initial=m.execute('SELECT count(*) FROM objects').fetchone()[0]
            root=m.execute('SELECT root FROM snapshots WHERE id=1').fetchone()[0]
        for i in range(5):host.query(c,'CALL differential_cleanup_old_files(max_objects:=1000);')
        assert host.query(c,'SELECT sum(id) FROM old.t;')==71994000
        assert host.query(c,"SELECT count(*) FROM old.t WHERE payload!=sha256(id::VARCHAR);")==0
        with sqlite3.connect(Path(tmp)/'engine.sqlite') as m:
            assert m.execute('SELECT count(*) FROM reclaimed_roots WHERE root=?',(root,)).fetchone()[0]==0
        results.append('pinned retired snapshot survives GC with uncached native payload')
        host.query(c,'DETACH old;')
        with sqlite3.connect(Path(tmp)/'engine.sqlite') as m:
            before=m.execute('SELECT count(*) FROM objects').fetchone()[0]
        host.query(c,'CALL differential_cleanup_old_files(dry_run:=true);')
        with sqlite3.connect(Path(tmp)/'engine.sqlite') as m: assert m.execute('SELECT count(*) FROM objects').fetchone()[0]==before
        for point in ('gc_deleted','gc_committed'):
            sql=f"LOAD '{EXT}'; CALL differential_cleanup_old_files(max_objects:=7,request_id:='{point}');"
            cli=DUCKDB_CLI
            with sqlite3.connect(Path(tmp)/'engine.sqlite') as m: prior=m.execute('SELECT count(*) FROM objects').fetchone()[0]
            crash=subprocess.run([cli,'-init','/dev/null','-unsigned','-batch','-bail','-csv'],input=sql,text=True,capture_output=True,env=dict(os.environ,DIFFERENTIAL_CONTROL_CRASH=point),timeout=30)
            assert crash.returncode==86,(point,crash.stdout,crash.stderr)
            with sqlite3.connect(Path(tmp)/'engine.sqlite') as m:
                if point=='gc_deleted':assert m.execute('SELECT count(*) FROM objects').fetchone()[0]==prior
                assert m.execute('PRAGMA foreign_key_check').fetchall()==[]
            first=subprocess.run([cli,'-init','/dev/null','-unsigned','-batch','-bail','-csv'],input=sql,text=True,capture_output=True,env=os.environ,timeout=30,check=True).stdout
            second=subprocess.run([cli,'-init','/dev/null','-unsigned','-batch','-bail','-csv'],input=sql,text=True,capture_output=True,env=os.environ,timeout=30,check=True).stdout
            assert first==second
            assert host.query(c,'SELECT sum(id) FROM d.t;')==99
        results.append('GC pre/post-commit process death, atomic recovery and request replay')
        for i in range(8):host.query(c,f"CALL differential_cleanup_old_files(max_objects:=7,request_id:='batch-{i}');")
        with sqlite3.connect(Path(tmp)/'engine.sqlite') as m:
            after=m.execute('SELECT count(*) FROM objects').fetchone()[0]
            assert after<before,(initial,before,after)
            assert m.execute('SELECT count(*) FROM reclaimed_roots WHERE root=?',(root,)).fetchone()[0]==1
            assert m.execute('PRAGMA foreign_key_check').fetchall()==[]
        # A repeated batch returns its stored result and performs no more physical deletion.
        host.query(c,"CALL differential_cleanup_old_files(max_objects:=7,request_id:='batch-7');")
        with sqlite3.connect(Path(tmp)/'engine.sqlite') as m:assert m.execute('SELECT count(*) FROM objects').fetchone()[0]==after
        assert host.query(c,'SELECT sum(id) FROM d.t;')==99
        results.append('dry run, bounded resumed batches, request replay and live branch retained')
        host.query(c,"ATTACH 'differential:sales' AS kept (SNAPSHOT_VERSION 2);")
        with sqlite3.connect(Path(tmp)/'engine.sqlite') as m:
            assert m.execute('PRAGMA auto_vacuum').fetchone()[0]==2
            assert m.execute('PRAGMA freelist_count').fetchone()[0]>0
        for point in ('compaction_moved','compaction_completed'):
            with sqlite3.connect(Path(tmp)/'engine.sqlite') as m:
                roots_before=m.execute('SELECT hash,size FROM roots ORDER BY hash').fetchall()
                maps_before=m.execute('SELECT r.root,r.page_no*256+e.slot,e.object FROM root_pages r JOIN page_entries e ON e.page=r.page ORDER BY 1,2').fetchall()
                assert maps_before  # every retained root's chunk map
            sql=f"LOAD '{EXT}'; CALL differential_compact(max_pages:=7,request_id:='{point}');"
            cli=DUCKDB_CLI
            crash=subprocess.run([cli,'-init','/dev/null','-unsigned','-batch','-bail','-csv'],input=sql,text=True,capture_output=True,env=dict(os.environ,DIFFERENTIAL_CONTROL_CRASH=point),timeout=30)
            assert crash.returncode==86,(point,crash.stdout,crash.stderr)
            first=subprocess.run([cli,'-init','/dev/null','-unsigned','-batch','-bail','-csv'],input=sql,text=True,capture_output=True,env=os.environ,timeout=30,check=True).stdout
            second=subprocess.run([cli,'-init','/dev/null','-unsigned','-batch','-bail','-csv'],input=sql,text=True,capture_output=True,env=os.environ,timeout=30,check=True).stdout
            assert first==second
            compact=list(csv.DictReader(io.StringIO(first)))[0]
            assert compact['roots_unchanged']=='true' and 0<=int(compact['pages_reclaimed'])<=7,compact
            assert int(compact['payload_bytes'])>0 and compact['export_retained_bytes']=='0',compact
            with sqlite3.connect(Path(tmp)/'engine.sqlite') as m:
                assert m.execute('SELECT hash,size FROM roots ORDER BY hash').fetchall()==roots_before
                assert m.execute('SELECT r.root,r.page_no*256+e.slot,e.object FROM root_pages r JOIN page_entries e ON e.page=r.page ORDER BY 1,2').fetchall()==maps_before
            assert host.query(c,'SELECT sum(id) FROM kept.t;')==99
            assert host.query(c,'SELECT sum(id) FROM d.t;')==99
        results.append('bounded incremental compaction crash recovery and retry with a live historical reader')
        host.query(c,"UPDATE d.t SET id=103; CALL differential_create_snapshot('d'); CALL differential_create_branch('sales','survivor',snapshot:=2); ATTACH 'differential:sales' AS next (BRANCH 'survivor'); UPDATE next.t SET id=101; CALL differential_create_snapshot('next'); DETACH next; DETACH d; CALL differential_expire_snapshots('sales',versions:=[2]); CALL differential_drop_branch('sales','main');")
        for _ in range(5):host.query(c,'CALL differential_cleanup_old_files(max_objects:=1000);')
        assert host.query(c,'SELECT sum(id) FROM kept.t;')==99
        with sqlite3.connect(Path(tmp)/'engine.sqlite') as m:
            kept_root=m.execute('SELECT root FROM snapshots WHERE id=2').fetchone()[0]
            assert m.execute('SELECT count(*) FROM reclaimed_roots WHERE root=?',(kept_root,)).fetchone()[0]==0
        host.query(c,'DETACH kept; CALL differential_cleanup_old_files(max_objects:=1000);')
        with sqlite3.connect(Path(tmp)/'engine.sqlite') as m:
            assert m.execute('SELECT count(*) FROM reclaimed_roots WHERE root=?',(kept_root,)).fetchone()[0]==1
        results.append('source branch deletion and retirement preserve a pinned historical reader; root reclaimed only after detach')
        # The raw diagnostic root namespace is unavailable in normal product mode.
        try:host.query(c,f"ATTACH 'diff://roots/{root}' AS bypass (TYPE differential,READ_ONLY);")
        except RuntimeError:pass
        else:raise AssertionError('raw root bypass enabled')
    finally:host.close()
evidence_path('retention.json').write_text(json.dumps({'passed':results},indent=2)+'\n')
for item in results:print('PASS',item)
