#!/usr/bin/env python3
"""A pending branch deletion reserves a surviving writable branch across process death."""
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import tempfile
from evidence import evidence_path
ROOT=Path(__file__).resolve().parents[2]
import sys as _sys
_sys.path.insert(0,str(ROOT/'test/integration'))
from paths import DUCKDB_CLI, EXTENSION, DUCKDB_LIBRARY, BUILD_DIR

CLI=DUCKDB_CLI
EXT=EXTENSION
with tempfile.TemporaryDirectory(prefix='differential-pending-deletions-') as tmp:
    env=dict(os.environ,DIFFERENTIAL_STORE_ROOT=tmp)
    def run(sql,expected=0,extra=None):
        r=subprocess.run([CLI,'-init','/dev/null','-unsigned','-batch','-bail','-csv'],input=f"LOAD '{EXT}';"+sql,text=True,capture_output=True,env=dict(env,**(extra or {})),timeout=30)
        assert r.returncode==expected,(sql,r.returncode,r.stdout,r.stderr)
        return r
    run("ATTACH 'differential:sales' AS d; CREATE TABLE d.t AS SELECT 1 AS i; CALL differential_create_snapshot('d'); CALL differential_create_branch('sales','survivor',snapshot:=1);")
    drop="CALL differential_drop_branch('sales','main',request_id:='first-delete');"
    run(drop,86,{'DIFFERENTIAL_CONTROL_CRASH':'compound_intent'})
    rejected=run("CALL differential_drop_branch('sales','survivor',request_id:='second-delete');",1)
    assert 'last active' in rejected.stderr
    with sqlite3.connect(Path(tmp)/'engine.sqlite') as db:
        assert db.execute('SELECT count(*) FROM compound_intents').fetchone()[0]==1
    run("ATTACH 'differential:sales' AS d (BRANCH 'survivor'); INSERT INTO d.t VALUES(2);")
    completed=run(drop).stdout;assert run(drop).stdout==completed
    assert run("ATTACH 'differential:sales' AS d (BRANCH 'survivor'); SELECT sum(i) AS total FROM d.t;").stdout=='total\n3\n'
evidence_path('pending-deletions.json').write_text(json.dumps({'passed':True,'scope':'process death leaves first delete pending; second delete rejected before intent; surviving branch remains writable; first request resumes once'},indent=2)+'\n')
print('PASS pending deletion reserves the last surviving branch across process death and retry')
