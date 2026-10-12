#!/usr/bin/env python3
"""Compare explicitly qualified persisted view/macro behavior with a stock native file copy."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
from evidence import evidence_path
ROOT=Path(__file__).resolve().parents[2]
import sys as _sys
_sys.path.insert(0,str(ROOT/'test/integration'))
from paths import DUCKDB_CLI, EXTENSION, DUCKDB_LIBRARY, BUILD_DIR

CLI=DUCKDB_CLI
EXT=EXTENSION
fixture="CREATE TABLE d.t AS SELECT 42 AS value; CREATE VIEW d.v AS SELECT * FROM d.t; CREATE MACRO d.m() AS TABLE SELECT * FROM d.t; CHECKPOINT d;"
with tempfile.TemporaryDirectory(prefix='differential-qualified-') as tmp:
    folder=Path(tmp);native=folder/'native.db';copy=folder/'copy.db';env=dict(os.environ,DIFFERENTIAL_STORE_ROOT=str(folder/'repository'))
    def run(sql):
        r=subprocess.run([CLI,'-init','/dev/null','-unsigned','-batch','-bail','-csv'],input=sql,text=True,capture_output=True,env=env,timeout=30)
        assert r.returncode==0,(r.stdout,r.stderr,sql)
        return r.stdout
    run(f"ATTACH '{native}' AS d;"+fixture);shutil.copyfile(native,copy)
    native_result=run(f"ATTACH '{native}' AS d; UPDATE d.t SET value=99; ATTACH '{copy}' AS old(READ_ONLY); SELECT (SELECT value FROM old.t) AS table_value,(SELECT value FROM old.v) AS view_value,(SELECT value FROM old.m()) AS macro_value;")
    run(f"LOAD '{EXT}'; ATTACH 'differential:sales' AS d;"+fixture+"CALL differential_create_snapshot('d');")
    diff_result=run(f"LOAD '{EXT}'; ATTACH 'differential:sales' AS d; UPDATE d.t SET value=99; ATTACH 'differential:sales' AS old (SNAPSHOT_VERSION 1); SELECT (SELECT value FROM old.t) AS table_value,(SELECT value FROM old.v) AS view_value,(SELECT value FROM old.m()) AS macro_value;")
    assert diff_result==native_result,(native_result,diff_result)
    same_alias=run(f"LOAD '{EXT}'; ATTACH 'differential:sales' AS d (SNAPSHOT_VERSION 1); SELECT value FROM d.v;")
    assert same_alias=='value\n42\n',same_alias
evidence_path('qualified-catalog.json').write_text(json.dumps({'status':'native-equivalent','native':native_result,'differential':diff_result,'original_alias_historical_view':same_alias,'scope':'explicit catalog qualification retains native name resolution; no catalog rewriting'},indent=2)+'\n')
print('PASS explicitly qualified views/macros match native copy behavior:',native_result.strip().replace('\n',' / '))
