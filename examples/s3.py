#!/usr/bin/env python3
"""Publish and read one immutable export using an existing authorized S3 namespace.

The export remains retained remotely. This script never creates a bucket or deletes
remote objects. Credentials are resolved only by the extension's AWS SDK.
"""
import argparse
import csv
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
ROOT=Path(__file__).resolve().parents[1]
import sys as _sys
_sys.path.insert(0,str(ROOT/'test/integration'))
from paths import DUCKDB_CLI, EXTENSION, DUCKDB_LIBRARY, BUILD_DIR

p=argparse.ArgumentParser(description=__doc__)
p.add_argument('--bucket',required=True);p.add_argument('--prefix',required=True)
p.add_argument('--region',default='us-east-1');p.add_argument('--profile',default='')
p.add_argument('--endpoint',default='');p.add_argument('--path-style',action='store_true')
p.add_argument('--allow-http',action='store_true');args=p.parse_args()
cli=DUCKDB_CLI
extension=EXTENSION
def quote(value):return "'"+value.replace("'","''")+"'"
location=f"s3://{args.bucket}/{args.prefix.rstrip('/')}/"
options=[f"REGION {quote(args.region)}"]
if args.profile:options.append(f"CREDENTIAL_PROFILE {quote(args.profile)}")
if args.endpoint:options.append(f"ENDPOINT {quote(args.endpoint)}")
if args.path_style:options.append('PATH_STYLE true')
if args.allow_http:options.append('ALLOW_HTTP true')
S3=', '.join(options)
def run(root,sql):
    result=subprocess.run([cli,'-init','/dev/null','-unsigned','-batch','-bail','-csv'],input=f'LOAD {quote(str(extension))};'+sql,text=True,capture_output=True,env=dict(os.environ,DIFFERENTIAL_STORE_ROOT=str(root)),check=True,timeout=180)
    return result.stdout
with tempfile.TemporaryDirectory(prefix='differential-s3-example-') as tmp:
    writer=Path(tmp)/'writer';reader=Path(tmp)/'reader'
    run(writer,f"ATTACH 'differential:sales' AS d (DATA_PATH {quote(location)}, {S3}); CREATE TABLE d.t AS SELECT 42 AS value;")
    result=run(writer,"ATTACH 'differential:sales' AS d; CALL differential_create_snapshot('d',name:='baseline',request_id:='baseline');")
    published=list(csv.DictReader(io.StringIO(result)))[0];assert published['export_state']=='complete'
    run(writer,"ATTACH 'differential:sales' AS d; UPDATE d.t SET value=99;")
    shutil.rmtree(writer) # only this script's fresh temporary authority
    # A reader needs only the export's location and provider access.
    historical=run(reader,f"ATTACH {quote('differential:'+location+published['export_locator'])} AS old ({S3}); SELECT value FROM old.t;")
    assert historical=='value\n42\n',historical
    print(json.dumps({'snapshot_id':int(published['snapshot_id']),'descriptor':published['export_locator'],'provider_namespace':{'bucket':args.bucket,'prefix':args.prefix,'region':args.region,'endpoint':args.endpoint,'path_style':args.path_style},'snapshot_time':published['snapshot_time'],'historical_value':42,'original_writer_catalog_absent':True,'durability':'local commits; remote published snapshots','remote_retention':'permanent export root; no automatic remote GC'},indent=2))
