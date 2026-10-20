#!/usr/bin/env python3
"""Cancel export after local publication and prove the acknowledged native commit remains."""
import json
import os
import secrets
from pathlib import Path
import metadata_store
import tempfile
import threading
import time
from stock_capi import Host
from evidence import evidence_path
ROOT=Path(__file__).resolve().parents[2]
import sys as _sys
_sys.path.insert(0,str(ROOT/'test/integration'))
from paths import DUCKDB_CLI, EXTENSION, DUCKDB_LIBRARY, BUILD_DIR

with tempfile.TemporaryDirectory(prefix='differential-cancel-') as tmp:
    root=Path(tmp);gate=root/'export-gate'
    os.environ.update(DIFFERENTIAL_STORE_ROOT=tmp,DIFFERENTIAL_EXPORT_GATE=str(gate),AWS_EC2_METADATA_DISABLED='true',AWS_ACCESS_KEY_ID='test_'+secrets.token_hex(8),AWS_SECRET_ACCESS_KEY=secrets.token_hex(24))
    host=Host();c=host.connect();observer=host.connect();errors=[]
    try:
        host.query(c,f"LOAD '{EXTENSION}'; ATTACH 'differential:sales' AS d (DATA_PATH 's3://unused-test-bucket/test/', ENDPOINT 'http://127.0.0.1:9', ALLOW_HTTP true, PATH_STYLE true); CREATE TABLE d.t AS SELECT 42 AS value;")
        def work():
            try:host.query(c,"CALL differential_create_snapshot('d',request_id:='cancelled');")
            except RuntimeError as e:errors.append(str(e))
        thread=threading.Thread(target=work);thread.start();end=time.monotonic()+10
        while not Path(str(gate)+'.entered').exists():
            assert thread.is_alive() and time.monotonic()<end,errors
            time.sleep(.01)
        host.lib.duckdb_interrupt(c);thread.join(timeout=10);assert not thread.is_alive() and errors
        assert host.query(observer,'SELECT sum(value) FROM d.t;')==42
        assert host.query(observer,"SELECT count(*) FROM differential_operations() WHERE state='local_complete_export_pending';")==1
        assert host.query(observer,"SELECT count(*) FROM differential_snapshots('sales') WHERE export_state='pending';")==1
        assert host.query(observer,"SELECT value FROM differential_storage_stats() WHERE metric='s3_put_calls';")==0
        with metadata_store.connect(root/'engine.sqlite') as db:
            assert db.execute("SELECT count(*) FROM export_roots WHERE state='pending'").fetchone()[0]==1
        evidence_path('cancellation.json').write_text(json.dumps({'status':'passed','native_committed_value':42,'pending_export_roots':1,'network_put_calls':0},indent=2)+'\n')
    finally:
        host.close();os.environ.pop('DIFFERENTIAL_EXPORT_GATE',None)
print('PASS cancellation after local publication: committed native row and pending export retained; no network PUT')
