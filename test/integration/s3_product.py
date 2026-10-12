#!/usr/bin/env python3
"""Requires the disposable MinIO namespace initialized by minio.py's SDK fixture."""
import csv
import io
import json
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
from evidence import evidence_path
ROOT=Path(__file__).resolve().parents[2]
import sys as _sys
_sys.path.insert(0,str(ROOT/'test/integration'))
from paths import DUCKDB_CLI, EXTENSION, DUCKDB_LIBRARY, BUILD_DIR

CLI=DUCKDB_CLI
EXT=EXTENSION
endpoint,base=sys.argv[1:];base=Path(base)
writer=base/'writer';reader=base/'reader'
writer.mkdir(exist_ok=False);reader.mkdir(exist_ok=False)
S3=f"ENDPOINT '{endpoint}', PATH_STYLE true, ALLOW_HTTP true"
LOCATION='s3://differential-sdk-test/product-test/'
s3_params=f"endpoint:='{endpoint}',path_style:=true,allow_http:=true"
attach="ATTACH 'differential:sales' AS d;"
def run(sql,directory=writer,extra=None,expected=0):
    env=dict(os.environ,DIFFERENTIAL_STORE_ROOT=str(directory));env.update(extra or {})
    r=subprocess.run([CLI,'-init','/dev/null','-unsigned','-batch','-bail','-csv'],input=f"LOAD '{EXT}';"+sql,text=True,capture_output=True,env=env,timeout=120)
    assert r.returncode==expected,(sql,r.returncode,r.stdout,r.stderr)
    return r.stdout
def rows(sql,directory=writer):return list(csv.DictReader(io.StringIO(run(sql,directory))))
run(f"ATTACH 'differential:sales' AS d (DATA_PATH '{LOCATION}', {S3}, MAX_REQUESTS 2, MAX_RANGE_BYTES 1048576);"+"""
CREATE TABLE d.t AS SELECT range AS id,sha256(range::VARCHAR) AS payload FROM range(10000);
USE d; CREATE SCHEMA analytics; CREATE SEQUENCE analytics.ids START 7;
CREATE TYPE analytics.mood AS ENUM ('ready','done');
CREATE TABLE analytics.meta(id BIGINT DEFAULT nextval('analytics.ids'),label analytics.mood DEFAULT 'ready',note VARCHAR);
INSERT INTO analytics.meta(note) VALUES('original'); CREATE INDEX meta_id ON analytics.meta(id);
CREATE VIEW analytics.summary AS SELECT id,label,note FROM analytics.meta;
CREATE MACRO analytics.twice(x) AS x*2;
""")
snapshot="CALL differential_create_snapshot('d',name:='published',request_id:='publish');"
run(attach+snapshot,extra={'DIFFERENTIAL_EXPORT_CRASH':'descriptor_uploaded'},expected=86)
with sqlite3.connect(writer/'engine.sqlite') as db:
    assert db.execute('SELECT state FROM export_roots').fetchall()==[('pending',)]
    assert db.execute('SELECT count(*) FROM snapshots').fetchone()[0]==1
# Advance the active branch so snapshot 1 is retained only by its uncertain export root.
run(attach+"UPDATE d.t SET payload='changed'; DROP VIEW d.analytics.summary; ALTER TABLE d.analytics.meta ADD COLUMN later INTEGER; DROP INDEX d.analytics.meta_id; CALL differential_create_snapshot('d',request_id:='advance');")
# A new unchanged publication uploads only its new descriptor and commit record and moves the
# branch head to it, reusing all verified payload/index objects.
reuse=run(attach+"CALL differential_create_snapshot('d',request_id:='unchanged'); SELECT value AS puts FROM differential_storage_stats() WHERE metric='s3_put_calls';")
assert reuse.endswith('puts\n3\n'),reuse
# Retirement and GC must not release an uncertain descriptor's graph.
run("CALL differential_expire_snapshots('sales',versions:=[1]); CALL differential_cleanup_old_files();")
compact=rows("CALL differential_compact(max_pages:=7,request_id:='compact-exports');")[0]
assert compact['roots_unchanged']=='true' and int(compact['export_retained_bytes'])>0,compact
# Deleting the publishing branch must not strand an export that only needs its immutable root.
run("CALL differential_create_branch('sales','survivor',snapshot:=2); CALL differential_drop_branch('sales','main'); CALL differential_cleanup_old_files();")
retry="CALL differential_retry_export('sales',1,request_id:='resume-published');"
result=rows(retry)[0]
assert result['snapshot_id']=='1' and result['export_state']=='complete',result
assert rows(retry)[0]==result
assert rows("SELECT state FROM differential_operations() WHERE request_id='publish';")==[{'state':'complete'}]
locator=result['export_locator']
with sqlite3.connect(writer/'engine.sqlite') as db:
    assert db.execute('SELECT state FROM export_roots').fetchall()==[('complete',),('complete',),('complete',)]
    writer_identity=db.execute('SELECT id FROM databases').fetchone()[0]
# The source authority is physically unavailable to the reader process.
shutil.rmtree(writer)
# A reader attaches the export by its location; no writer metadata or registration step.
remote=f"ATTACH 'differential:{LOCATION}{locator}' AS old ({S3});"
actual=rows(remote+"SELECT count(*) AS n,sum(id) AS total,count(*) FILTER(WHERE payload!=sha256(id::VARCHAR)) AS corrupt FROM old.t;",reader)
assert actual==[{'n':'10000','total':'49995000','corrupt':'0'}],actual
catalog=rows(remote+"SELECT id,label,note,old.analytics.twice(21) AS answer FROM old.analytics.summary;",reader)
assert catalog==[{'id':'7','label':'ready','note':'original','answer':'42'}],catalog
assert rows(remote+"SELECT count(*) AS n FROM information_schema.columns WHERE table_catalog='old' AND table_schema='analytics' AND table_name='meta';",reader)==[{'n':'3'}]
assert rows(remote+"SELECT count(*) AS n FROM duckdb_indexes() WHERE database_name='old' AND index_name='meta_id';",reader)==[{'n':'1'}]
run(remote+"INSERT INTO old.t VALUES(1,'x');",reader,expected=1)
# An export has no repository history: no AT clauses or catalog-scoped functions.
run(remote+"SELECT count(*) FROM old.t AT (VERSION => 1);",reader,expected=1)
run(remote+"SELECT * FROM old.snapshots();",reader,expected=1)
with sqlite3.connect(reader/'engine.sqlite') as db:
    assert db.execute('SELECT count(*) FROM databases').fetchone()[0]==0
    assert db.execute('SELECT count(*) FROM objects').fetchone()[0]==0
recover=f"CALL differential_recover_export('{LOCATION}{locator}','recovered',{s3_params},request_id:='recover');"
recovered=rows(recover,reader)[0]
assert recovered['database_id']!=writer_identity and recovered['snapshot_id']=='1'
assert rows(recover,reader)[0]==recovered
local="ATTACH 'differential:recovered' AS d;"
assert rows(local+"SELECT count(*) AS n,sum(id) AS total,count(*) FILTER(WHERE payload!=sha256(id::VARCHAR)) AS corrupt FROM d.t;",reader)==actual
assert rows(local+"INSERT INTO d.analytics.meta(note) VALUES('recovered'); SELECT id,label FROM d.analytics.meta WHERE note='recovered';",reader)==[{'id':'8','label':'ready'}]
run(local+"DELETE FROM d.t;",reader)
assert rows(remote+"SELECT count(*) AS n,sum(id) AS total,count(*) FILTER(WHERE payload!=sha256(id::VARCHAR)) AS corrupt FROM old.t;",reader)==actual
run(f"CALL differential_recover_export('{LOCATION}{locator}','recovered_disk',{s3_params},data_path:='{base/'local-recovery-pool'}',request_id:='recover-disk');",reader)
assert rows("ATTACH 'differential:recovered_disk' AS d; SELECT count(*) AS n,sum(id) AS total,count(*) FILTER(WHERE payload!=sha256(id::VARCHAR)) AS corrupt FROM d.t;",reader)==actual
record={'status':'passed','endpoint':'disposable pinned MinIO','descriptor_lost_response':True,'retired_pending_root_survived_gc':True,'publishing_branch_deleted_before_retry':True,'unchanged_snapshot_puts':1,'writer_catalog_deleted':True,'read_result':actual,'historical_schema_view_index_enum_macro':catalog,'recovered_sequence_next_value':8,'reader_local_payload_objects_before_explicit_recovery':0,'writable_recovery_new_identity':True,'recovery_into_additional_local_pool':True}
evidence_path('minio-snapshot-results.json').write_text(json.dumps(record,indent=2)+'\n')
print('PASS S3 publication, descriptor response loss, pending retention through retire/GC, stock remote reads and new-identity writable recovery without writer catalog',flush=True)
