#!/usr/bin/env python3
"""Offline native import rejects pending WAL and a live native attachment."""
import csv
import io
import json
import os
from pathlib import Path
import selectors
import subprocess
import tempfile
from evidence import evidence_path
ROOT=Path(__file__).resolve().parents[2]
import sys as _sys
_sys.path.insert(0,str(ROOT/'test/integration'))
from paths import DUCKDB_CLI, EXTENSION, DUCKDB_LIBRARY, BUILD_DIR

CLI=DUCKDB_CLI
EXT=EXTENSION
with tempfile.TemporaryDirectory(prefix='differential-import-') as tmp:
    base=Path(tmp);source=base/'source.db';repo=base/'repository'
    env=dict(os.environ,DIFFERENTIAL_STORE_ROOT=str(repo))
    def run(sql,expected=0,extension=True):
        r=subprocess.run([CLI,'-init','/dev/null','-unsigned','-batch','-bail','-csv'],input=(f"LOAD '{EXT}';" if extension else '')+sql,text=True,capture_output=True,env=env,timeout=30)
        assert r.returncode==expected,(sql,r.returncode,r.stdout,r.stderr)
        return r.stdout
    run(f"ATTACH '{source}' AS s; CREATE TABLE s.t(i INT PRIMARY KEY,v VARCHAR); INSERT INTO s.t VALUES(1,'one');",extension=False)
    proc=subprocess.Popen([CLI,'-init','/dev/null','-batch','-csv'],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,bufsize=1)
    try:
        proc.stdin.write(f"ATTACH '{source}' AS s; INSERT INTO s.t VALUES(2,'two'); SELECT 'ACK';\n.output /dev/stdout\n");proc.stdin.flush()
        poll=selectors.DefaultSelector();poll.register(proc.stdout,selectors.EVENT_READ);assert poll.select(timeout=10)
        assert 'ACK' in proc.stdout.readline()
        run(f"CALL differential_import_database('{source}','copy',request_id:='import');",1)
        proc.kill();proc.wait(timeout=5)
    finally:
        if proc.poll() is None:proc.kill();proc.wait(timeout=5)
    assert source.with_suffix('.db.wal').exists()
    run(f"CALL differential_import_database('{source}','copy',request_id:='import');",1)
    run(f"ATTACH '{source}' AS s; CHECKPOINT s; DETACH s;",extension=False)
    # Even with no WAL, an attachment in this host is not an offline source.
    run(f"ATTACH '{source}' AS s; CALL differential_import_database('{source}','blocked');",1)
    command=f"CALL differential_import_database('{source}','copy',request_id:='import');"
    first=run(command);assert run(command)==first
    data=run("ATTACH 'differential:copy' AS d; SELECT * FROM d.t ORDER BY i;")
    assert list(csv.DictReader(io.StringIO(data)))==[{'i':'1','v':'one'},{'i':'2','v':'two'}]
    # A completed request remains replayable after the source is removed.
    source.unlink();assert run(command)==first
    evidence_path('native-import.json').write_text(json.dumps({'status':'passed','pending_wal_rejected':True,'live_attachment_rejected':True,'source_removal_replay':True},indent=2)+'\n')
    print('PASS native import: live source/WAL rejection, checkpointed copy, new identity and replay after source removal')
