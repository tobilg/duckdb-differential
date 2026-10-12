#!/usr/bin/env python3
"""Read more immutable data than the remote cache can retain, using stock native SQL."""
import csv
import io
import json
import os
from pathlib import Path
import subprocess
import sys
from stock_capi import Host
from evidence import evidence_path
ROOT=Path(__file__).resolve().parents[2]
import sys as _sys
_sys.path.insert(0,str(ROOT/'test/integration'))
from paths import DUCKDB_CLI, EXTENSION, DUCKDB_LIBRARY, BUILD_DIR, TEST_BINARIES

EXT=EXTENSION;CLI=DUCKDB_CLI
endpoint,temporary=sys.argv[1:];base=Path(temporary)/'resources';base.mkdir(exist_ok=False)
S3=f"ENDPOINT '{endpoint}', PATH_STYLE true, ALLOW_HTTP true"
LOCATION='s3://differential-sdk-test/resource-test/'
# The same service under a second endpoint spelling is a second provider identity.
SECOND=S3.replace('127.0.0.1','localhost')
env=dict(os.environ,DIFFERENTIAL_STORE_ROOT=str(base/'writer'))
sql=f"LOAD '{EXT}'; ATTACH 'differential:large' AS d (DATA_PATH '{LOCATION}', {S3}); CREATE TABLE d.t AS SELECT range AS id,sha256(range::VARCHAR) AS payload FROM range(900000); CALL differential_create_snapshot('d');"
r=subprocess.run([CLI,'-init','/dev/null','-unsigned','-batch','-bail','-csv'],input=sql,env=env,text=True,capture_output=True,check=True,timeout=120)
# Multiple CALL schemas are emitted, so parse the final lifecycle header/result only.
lines=r.stdout.splitlines();header=next(i for i,line in enumerate(lines) if line.startswith('operation_id,database_id'))
rows=list(csv.DictReader(io.StringIO('\n'.join(lines[header:]))));locator=rows[-1]['export_locator'];assert locator.startswith('descriptors/')
os.environ['DIFFERENTIAL_STORE_ROOT']=str(base/'reader');host=Host();c=host.connect()
LIMIT=16*1024*1024
try:
    host.query(c,f"LOAD '{EXT}'; SET memory_limit='16MB'; ATTACH 'differential:{LOCATION}{locator}' AS old ({S3});")
    assert host.query(c,'SELECT count(*) FROM old.t WHERE payload!=sha256(id::VARCHAR);')==0
    def metric(name):return host.query(c,f"SELECT value FROM differential_storage_stats() WHERE metric='{name}';")
    # The memory layer is DuckDB's external file cache: buffer-managed within memory_limit.
    def resident():return host.query(c,"SELECT coalesce(sum(nr_bytes) FILTER (WHERE loaded),0) FROM duckdb_external_file_cache() WHERE path LIKE 'differential:%';")
    peak=resident();transferred=metric('s3_verified_get_bytes')
    assert 0<peak<=LIMIT,peak
    assert transferred>LIMIT,transferred
    assert host.query(c,'SELECT sum(id) FROM old.t WHERE id<100;')==4950
    host.query(c,f"ATTACH 'differential:{LOCATION}{locator}' AS another ({SECOND});")
    before_second=metric('s3_verified_get_bytes')
    assert host.query(c,'SELECT count(*) FROM another.t WHERE payload!=sha256(id::VARCHAR);')==0
    second_bytes=metric('s3_verified_get_bytes')-before_second
    assert resident()<=LIMIT
    host.query(c,'DETACH old; DETACH another;')
    assert resident()<=LIMIT
finally:host.close()
evidence_path('s3-resource-results.json').write_text(json.dumps({'rows':900000,'native_memory_limit':LIMIT,'resident_external_file_cache_bytes':peak,'verified_bytes':transferred,'second_provider_verified_bytes':second_bytes,'scope':"two provider identities; remote reads cached in DuckDB's external file cache within memory_limit; MinIO loopback"},indent=2)+'\n')
print("PASS scans larger than memory_limit stay correct; DuckDB's external file cache stays within memory_limit across two providers",flush=True)
# Upload staging grows with differential_s3_max_requests: 16 parallel 8 MiB pack uploads need
# 128 MiB, above the 64 MiB floor, and must not fail with staging_full.
parallel=base/'parallel'
sql=f"""LOAD '{EXT}'; SET differential_s3_max_requests=16;
ATTACH 'differential:wide' AS w (DATA_PATH 's3://differential-sdk-test/resource-parallel/', {S3});
CREATE TABLE w.t AS SELECT range AS id, sha256(range::VARCHAR)||sha256((range*7)::VARCHAR) AS payload FROM range(1300000);
SELECT export_state FROM differential_create_snapshot('w');
SELECT value FROM differential_storage_stats() WHERE metric='export_packs_uploaded';
SELECT value FROM differential_storage_stats() WHERE metric='remote_staging_limit';"""
r=subprocess.run([CLI,'-init','/dev/null','-unsigned','-batch','-bail','-csv','-noheader'],input=sql,env=dict(os.environ,DIFFERENTIAL_STORE_ROOT=str(parallel)),text=True,capture_output=True,timeout=300)
assert r.returncode==0,(r.stdout,r.stderr)
state,packs,limit=r.stdout.split()
assert state=='complete' and int(packs)>=9 and int(limit)==128*1024*1024,(state,packs,limit)
assert not [p for p in (parallel/'staging').rglob('upload-*')],'staging files left behind'
print(f'PASS 16 parallel uploads of {packs} packs fit the 128 MiB staging quota sized by differential_s3_max_requests',flush=True)
# SDK clients never ask the EC2 instance metadata service for a region (the region is explicit).
# A fake, slow metadata endpoint counts requests: with a named profile there must be none.
import http.server, threading, time
metadata_requests = []
class SlowMetadata(http.server.BaseHTTPRequestHandler):
    def log_message(self, *a): pass
    def do_PUT(self):
        metadata_requests.append(self.path); time.sleep(1.5)
        self.send_response(404); self.send_header('Content-Length', '0'); self.end_headers()
    do_GET = do_PUT
metadata = http.server.ThreadingHTTPServer(('127.0.0.1', 0), SlowMetadata)
threading.Thread(target=metadata.serve_forever, daemon=True).start()
profiles = base / 'profiles'; profiles.mkdir()
(profiles / 'credentials').write_text(f"[bench]\naws_access_key_id = {os.environ['AWS_ACCESS_KEY_ID']}\naws_secret_access_key = {os.environ['AWS_SECRET_ACCESS_KEY']}\n")
(profiles / 'config').write_text('[profile bench]\n')  # no region anywhere
quiet = {k: v for k, v in os.environ.items() if k not in ('AWS_DEFAULT_REGION', 'AWS_REGION', 'AWS_EC2_METADATA_DISABLED', 'AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY')}
quiet.update(DIFFERENTIAL_STORE_ROOT=str(base / 'metadata'), AWS_EC2_METADATA_SERVICE_ENDPOINT=f'http://127.0.0.1:{metadata.server_port}',
             AWS_CONFIG_FILE=str(profiles / 'config'), AWS_SHARED_CREDENTIALS_FILE=str(profiles / 'credentials'))
sql = f"""LOAD '{EXT}'; ATTACH 'differential:{LOCATION}{locator}' AS old ({S3}, CREDENTIAL_PROFILE 'bench');
SELECT sum(id) FROM old.t WHERE id < 100;
SELECT value FROM differential_storage_stats() WHERE metric = 's3_client_create_max_nanoseconds';"""
r = subprocess.run([CLI, '-init', '/dev/null', '-unsigned', '-batch', '-bail', '-csv', '-noheader'], input=sql, env=quiet, text=True, capture_output=True, timeout=120)
metadata.shutdown()
assert r.returncode == 0, (r.stdout, r.stderr)
total, client_create = r.stdout.split()
assert total == '4950' and not metadata_requests and int(client_create) < 500_000_000, (total, metadata_requests, client_create)
print('PASS SDK clients make no EC2 metadata requests when no region is configured (named profile)', flush=True)
