#!/usr/bin/env python3
"""Public local snapshots in a disposable repository on stock DuckDB."""
import os
import argparse
from pathlib import Path
import subprocess
import tempfile
ROOT=Path(__file__).resolve().parents[1]
import sys as _sys
_sys.path.insert(0,str(ROOT/'test/integration'))
from paths import DUCKDB_CLI, EXTENSION, DUCKDB_LIBRARY, BUILD_DIR

parser=argparse.ArgumentParser();parser.add_argument('--external-pool',action='store_true');args=parser.parse_args()
with tempfile.TemporaryDirectory(prefix='differential-example-') as tmp:
    env=dict(os.environ,DIFFERENTIAL_STORE_ROOT=tmp)
    setup=f"ATTACH 'differential:example' AS current_db (DATA_PATH '{Path(tmp)/'payload-pool'}'); DETACH current_db;" if args.external_pool else ""
    sql=f"""
LOAD '{EXTENSION}';
{setup}
ATTACH 'differential:example' AS current_db;
CREATE TABLE current_db.t AS SELECT 42 AS value;
CALL differential_create_snapshot('current_db',name:='baseline',request_id:='snapshot');
ATTACH 'differential:example' AS historical (SNAPSHOT_NAME 'baseline');
UPDATE current_db.t SET value=99;
SELECT 'current' AS state,value FROM current_db.t UNION ALL SELECT 'historical',value FROM historical.t;
"""
    result=subprocess.run([DUCKDB_CLI,'-init','/dev/null','-unsigned','-batch','-csv'],input=sql,text=True,capture_output=True,env=env,check=True)
    print(result.stdout,end='')
    assert 'current,99' in result.stdout and 'historical,42' in result.stdout
