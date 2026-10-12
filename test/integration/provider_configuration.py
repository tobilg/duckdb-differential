#!/usr/bin/env python3
"""Immutable SDK policy registration/replay/introspection, without any network request."""
import csv
import io
import json
import os
from pathlib import Path
import subprocess
import tempfile
from evidence import evidence_path
ROOT=Path(__file__).resolve().parents[2]
import sys as _sys
_sys.path.insert(0,str(ROOT/'test/integration'))
from paths import DUCKDB_CLI, EXTENSION, DUCKDB_LIBRARY, BUILD_DIR

CLI=DUCKDB_CLI
EXT=EXTENSION
with tempfile.TemporaryDirectory(prefix='differential-provider-configuration-') as tmp:
    root=Path(tmp)/'repo';env=dict(os.environ,DIFFERENTIAL_STORE_ROOT=str(root),AWS_EC2_METADATA_DISABLED='true')
    def run(sql, expected=0):
        r=subprocess.run([CLI,'-init','/dev/null','-unsigned','-batch','-bail','-csv'],input=f"LOAD '{EXT}';"+sql,text=True,capture_output=True,env=env,timeout=30)
        assert r.returncode==expected,(r.stdout,r.stderr)
        return r.stdout
    location="s3://example-uncontacted-bucket/isolated/"
    options=f"DATA_PATH '{location}', MAX_REQUESTS 1, RETRIES 0, CONNECT_TIMEOUT_MS 250, REQUEST_TIMEOUT_MS 2000, MAX_RANGE_BYTES 1048576, MULTIPART_THRESHOLD 5242880"
    plan=f"differential_import_database('/nonexistent.db','planned',data_path:='{location}',retries:=0)"
    run(f"EXPLAIN SELECT * FROM {plan}; PREPARE p AS SELECT * FROM {plan};")
    assert not root.exists()
    # The provider registration behind a DATA_PATH is idempotent and immutable.
    command=f"ATTACH 'differential:archive' AS a ({options}); DETACH a;"
    assert run(command)==run(command)
    run(command.replace('RETRIES 0','RETRIES 1'),1)
    run(f"ATTACH 'differential:archive' AS a (DATA_PATH 's3://example-uncontacted-bucket/other/');",1)
    for bad in ('MAX_REQUESTS 65','RETRIES 11','CONNECT_TIMEOUT_MS 0','REQUEST_TIMEOUT_MS 300001','MAX_RANGE_BYTES 65536','MULTIPART_THRESHOLD 1'):
        run(f"ATTACH 'differential:invalid' AS i (DATA_PATH 's3://example-uncontacted-bucket/invalid/', {bad});",1)
    values=list(csv.DictReader(io.StringIO(run("SELECT data_path,max_requests,retries,connect_timeout_ms,request_timeout_ms,max_range_bytes,multipart_threshold FROM differential_providers() WHERE data_path LIKE 's3://%';"))))
    assert values==[{'data_path':location,'max_requests':'1','retries':'0','connect_timeout_ms':'250','request_timeout_ms':'2000','max_range_bytes':'1048576','multipart_threshold':'5242880'}],values
    assert run("SELECT data_path FROM differential_databases();")=='data_path\n'+location+'\n'
    assert run("SELECT value FROM differential_storage_stats() WHERE metric='s3_get_calls';")=='value\n0\n'
evidence_path('provider-configuration.json').write_text(json.dumps({'passed':True,'observed':values,'scope':'persistent configuration and execution/bind behavior; no network requests'},indent=2)+'\n')
print('PASS DATA_PATH provider options persisted, safe bind/plan, idempotent re-attach, mismatch/range rejection, fresh-process introspection')
