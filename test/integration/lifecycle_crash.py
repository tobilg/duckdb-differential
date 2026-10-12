#!/usr/bin/env python3
"""Recover compound intents after process death, including an acknowledged native WAL."""
import csv
import io
import json
import os
from pathlib import Path
import selectors
import sqlite3
import subprocess
import tempfile
from evidence import evidence_path
ROOT=Path(__file__).resolve().parents[2]
import sys as _sys
_sys.path.insert(0,str(ROOT/'test/integration'))
from paths import DUCKDB_CLI, EXTENSION, DUCKDB_LIBRARY, BUILD_DIR

BINARY=DUCKDB_CLI
EXT=EXTENSION
results=[]
with tempfile.TemporaryDirectory(prefix='differential-compound-crash-') as tmp:
    for point in ('compound_intent','compound_preservation','compound_replaced','compound_completed'):
        folder=Path(tmp)/point; folder.mkdir()
        env=dict(os.environ,DIFFERENTIAL_STORE_ROOT=str(folder))
        prefix=f"LOAD '{EXT}'; "
        attach="ATTACH 'differential:sales' AS d; "
        def run(sql,extra=None,expected=0):
            r=subprocess.run([BINARY,'-init','/dev/null','-unsigned','-batch','-bail','-csv'],input=prefix+sql,text=True,capture_output=True,env=dict(env,**(extra or {})),timeout=30)
            assert r.returncode==expected,(point,sql,r.returncode,r.stdout,r.stderr)
            return r.stdout
        external_pool = bool(os.environ.get('DIFFERENTIAL_TEST_LOCAL_POOL'))
        setup = f"ATTACH 'differential:sales' AS d (DATA_PATH '{folder/'payload'}'); DETACH d; " if external_pool else ""
        run(setup+attach+"CREATE TABLE d.t(i INT); INSERT INTO d.t VALUES(1); CALL differential_create_snapshot('d');")
        retire_target=bool(os.environ.get('DIFFERENTIAL_TEST_RETIRE_TARGET'))
        if retire_target:run(attach+"INSERT INTO d.t VALUES(2); CALL differential_create_snapshot('d');")
        # Keep stdin open to prevent normal-close checkpoint; observe a native commit acknowledgment.
        proc=subprocess.Popen([BINARY,'-init','/dev/null','-unsigned','-batch','-csv'],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,env=env,bufsize=1)
        try:
            proc.stdin.write(prefix+attach+f"INSERT INTO d.t VALUES({3 if retire_target else 2}); SELECT 'ACK';\n.output /dev/stdout\n");proc.stdin.flush()
            poll=selectors.DefaultSelector();poll.register(proc.stdout,selectors.EVENT_READ)
            assert poll.select(timeout=10),'ack timeout'
            assert 'ACK' in proc.stdout.readline()
            # An active writer prevents restore before any durable intent/publication.
            run("CALL differential_restore('sales',1,branch:='main',request_id:='busy');",expected=1)
            proc.kill();proc.wait(timeout=5)
        finally:
            if proc.poll() is None:proc.kill();proc.wait(timeout=5)
        with sqlite3.connect(folder/'engine.sqlite') as metadata:
            assert metadata.execute("SELECT count(*) FROM files WHERE name LIKE '%.wal' AND size>0").fetchone()[0]==1
        restore="CALL differential_restore('sales',1,branch:='main',request_id:='restore');"
        run(restore,{'DIFFERENTIAL_CONTROL_CRASH':point},86)
        if retire_target:run("CALL differential_expire_snapshots('sales',versions:=[1]); CALL differential_cleanup_old_files();")
        if point!='compound_completed':
            run(attach,expected=1) # unresolved intent fences a new writer after crash
        answer=run(restore)
        assert run(restore)==answer
        row=list(csv.DictReader(io.StringIO(answer)))[0]
        preserved_id=3 if retire_target else 2;restored_id=preserved_id+1
        assert row['preservation_snapshot_id']==str(preserved_id) and row['snapshot_id']==str(restored_id),row
        assert run(attach+'SELECT sum(i) FROM d.t;').strip().endswith('1')
        assert run(f"ATTACH 'differential:sales' AS p (SNAPSHOT_VERSION {preserved_id}); SELECT sum(i) FROM p.t;").strip().endswith('6' if retire_target else '3')
        with sqlite3.connect(folder/'engine.sqlite') as metadata:
            assert metadata.execute('SELECT count(*) FROM snapshots').fetchone()[0]==restored_id
            assert metadata.execute('SELECT count(*) FROM compound_intents').fetchone()[0]==0
            assert metadata.execute('PRAGMA integrity_check').fetchone()[0]=='ok'
        results.append({'point':point,'status':'passed','preserved_acknowledged_wal':True,'external_local_pool':external_pool,'target_retired_while_pending':retire_target})
        print('PASS',point,flush=True)
evidence_path('retired-target-crash.json' if retire_target else 'local-pool-native-crash.json' if external_pool else 'lifecycle-crash.json').write_text(json.dumps(results,indent=2)+'\n')
