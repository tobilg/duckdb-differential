#!/usr/bin/env python3
"""Public checkpoint boundary with an old reader and a concurrent WAL-sidecar commit."""
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import threading
import time
from stock_capi import Host
from evidence import evidence_path
ROOT=Path(__file__).resolve().parents[2]
import sys as _sys
_sys.path.insert(0,str(ROOT/'test/integration'))
from paths import DUCKDB_CLI, EXTENSION, DUCKDB_LIBRARY, BUILD_DIR, TEST_BINARIES

EXT=EXTENSION
with tempfile.TemporaryDirectory(prefix='differential-capture-race-') as tmp:
    folder=Path(tmp);gate=folder/'gate'
    os.environ.update(DIFFERENTIAL_STORE_ROOT=tmp,DIFFERENTIAL_PROBE_GATE=str(gate))
    host=Host();writer=host.connect();snapshot=host.connect();reader=host.connect();history=host.connect()
    try:
        host.query(writer,f"LOAD '{EXT}'; ATTACH 'differential:sales' AS d; CREATE TABLE d.t(i INT); INSERT INTO d.t VALUES(1); CHECKPOINT d;")
        host.query(reader,'BEGIN; SELECT count(*) FROM d.t;')
        host.query(writer,'INSERT INTO d.t VALUES(2);')
        with sqlite3.connect(folder/'engine.sqlite') as metadata:
            database,branch=metadata.execute('SELECT database_id,id FROM branches').fetchone()
        os.environ['DIFFERENTIAL_PROBE_GATE_URI']=f'diff://db/{database}/branches/{branch}'
        (folder/'gate.arm').touch();errors=[]
        def publish():
            try:host.query(snapshot,"CALL differential_create_snapshot('d',request_id:='concurrent');")
            except BaseException as error:errors.append(str(error))
        worker=threading.Thread(target=publish);worker.start();deadline=time.monotonic()+8
        while not (folder/'gate.entered').exists():
            if errors:raise AssertionError(errors)
            if time.monotonic()>deadline:raise AssertionError('checkpoint gate not reached')
            time.sleep(.01)
        host.query(writer,'INSERT INTO d.t VALUES(3);')
        with sqlite3.connect(folder/'engine.sqlite') as metadata:
            assert metadata.execute("SELECT count(*) FROM files WHERE name LIKE '%.wal.checkpoint' AND size>0").fetchone()[0]==1
        (folder/'gate.release').touch();worker.join(timeout=15);assert not worker.is_alive() and not errors,errors
        host.query(history,"ATTACH 'differential:sales' AS old (SNAPSHOT_VERSION 1);")
        assert host.query(history,'SELECT sum(i) FROM old.t;')==3
        assert host.query(writer,'SELECT sum(i) FROM d.t;')==6
        assert host.query(reader,'SELECT sum(i) FROM d.t;')==1
        host.query(reader,'ROLLBACK;')
        host.query(writer,"CALL differential_create_snapshot('d');")
        assert host.query(history,'SELECT sum(i) FROM old.t;')==3
    finally:
        (folder/'gate.release').touch()
        if 'worker' in locals():worker.join(timeout=15)
        host.close()
evidence_path('snapshot-concurrency.json').write_text(json.dumps({'status':'passed','old_transaction_sum':1,'checkpoint_snapshot_sum':3,'live_sum_after_concurrent_commit':6,'concurrent_wal_checkpoint_sidecar':True},indent=2)+'\n')
print('PASS public snapshot captures checkpoint boundary while a later native sidecar commit remains live-only')
