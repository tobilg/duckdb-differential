#!/usr/bin/env python3
"""Fault-injecting HTTP relay around disposable MinIO; all S3 calls originate in the SDK."""
import csv
import fcntl
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
import http.client
import io
import json
import os
from pathlib import Path
import socket
import sqlite3
import subprocess
import sys
import threading
from urllib.parse import urlsplit
from evidence import evidence_path
ROOT=Path(__file__).resolve().parents[2]
import sys as _sys
_sys.path.insert(0,str(ROOT/'test/integration'))
from paths import DUCKDB_CLI, EXTENSION, DUCKDB_LIBRARY, BUILD_DIR, TEST_BINARIES

EXT=EXTENSION
CLI=DUCKDB_CLI
endpoint,temporary=sys.argv[1:];target=urlsplit(endpoint);base=Path(temporary)/'publication-faults';base.mkdir(exist_ok=False)
class Relay(BaseHTTPRequestHandler):
    protocol_version='HTTP/1.1'
    mode='none';triggered=0;aborts=0
    def log_message(self,*args):pass
    def handle_request(self):
        if self.headers.get('Transfer-Encoding','').lower()=='chunked':
            parts=[]
            while True:
                line=self.rfile.readline();parts.append(line);size=int(line.split(b';',1)[0],16)
                if size:
                    parts.append(self.rfile.read(size+2))
                else:
                    while True:
                        trailer=self.rfile.readline();parts.append(trailer)
                        if trailer==b'\r\n':break
                    break
            body=b''.join(parts)
        else:body=self.rfile.read(int(self.headers.get('Content-Length','0')))
        if Relay.mode.startswith('corrupt-upload') and self.command=='PUT' and '/packs/' in self.path and body:
            # One flipped payload byte: the service must reject the body against its SHA-256 checksum.
            Relay.triggered+=1;middle=len(body)//2;body=body[:middle]+bytes([body[middle]^1])+body[middle+1:]

        denied=(Relay.mode=='payload-denied' and self.command=='PUT' and '/packs/' in self.path) or (Relay.mode=='descriptor-denied' and self.command=='PUT' and '/descriptors/' in self.path)
        if Relay.mode=='descriptor-unavailable' and self.command in ('HEAD','GET') and '/descriptors/' in self.path:
            Relay.triggered+=1;self.send_response(404);self.send_header('Content-Length','0');self.end_headers();return
        complete=Relay.mode=='multipart-complete' and self.command=='POST' and 'uploadId=' in self.path
        if self.command=='DELETE' and 'uploadId=' in self.path:Relay.aborts+=1
        if denied or complete:
            Relay.triggered+=1;status=503 if complete else 403
            data=b'<Error><Code>SlowDown</Code><Message>Injected completion failure</Message></Error>' if complete else b'<Error><Code>AccessDenied</Code><Message>Injected denial</Message></Error>'
            self.send_response(status);self.send_header('Content-Length',str(len(data)));self.end_headers();self.wfile.write(data);return
        connection=http.client.HTTPConnection(target.hostname,target.port,timeout=10)
        try:
            connection.request(self.command,self.path,body,dict(self.headers))
            response=connection.getresponse();data=response.read();headers=response.getheaders()
            lost=(Relay.mode=='descriptor-response-loss' and '/descriptors/' in self.path) or (Relay.mode=='head-response-loss' and '/branches/' in self.path)
            if lost and self.command=='PUT' and response.status==200 and Relay.triggered==0:
                Relay.triggered+=1;self.connection.shutdown(socket.SHUT_RDWR);self.connection.close();return
            if Relay.mode=='corrupt-verification' and self.command=='GET' and '/packs/' in self.path and response.status==206:
                Relay.triggered+=1;data=bytes([data[0]^1])+data[1:]
                # Remove response checksum so the extension's own immutable-object check is exercised.
                headers=[(k,v) for k,v in headers if not k.lower().startswith('x-amz-checksum')]
            self.send_response(response.status)
            for key,value in headers:
                if key.lower() not in ('transfer-encoding','connection'):self.send_header(key,value)
            self.end_headers()
            if self.command!='HEAD':self.wfile.write(data)
        finally:connection.close();self.close_connection=True
    do_GET=do_HEAD=do_PUT=do_POST=do_DELETE=handle_request
server=ThreadingHTTPServer(('127.0.0.1',0),Relay);threading.Thread(target=server.serve_forever,daemon=True).start()
proxy=f'http://127.0.0.1:{server.server_port}'
results=[]
def run(sql,folder,expected=0):
    result=subprocess.run([CLI,'-init','/dev/null','-unsigned','-batch','-bail','-csv'],input=f"LOAD '{EXT}';"+sql,text=True,capture_output=True,env=dict(os.environ,DIFFERENTIAL_STORE_ROOT=str(folder)),timeout=90)
    assert result.returncode==expected,(expected,result.returncode,result.stdout,result.stderr)
    return result
try:
    # Export settings per mode: read-back verification catches a corrupted download; uploads are checked
    # by the service, staged on disk or in memory.
    settings={'corrupt-verification':"SET differential_export_verification='read_back';",
              'corrupt-upload-memory':"SET differential_export_staging='memory';"}
    for mode in ('payload-denied','descriptor-denied','corrupt-verification','corrupt-upload','corrupt-upload-memory',
                 'descriptor-response-loss','staging-full'):
        folder=base/mode;folder.mkdir();Relay.mode='none';Relay.triggered=0
        S3=f"ENDPOINT '{proxy}', PATH_STYLE true, ALLOW_HTTP true"
        attach="ATTACH 'differential:sales' AS d;"
        run(f"ATTACH 'differential:sales' AS d (DATA_PATH 's3://differential-sdk-test/fault-publication/', {S3}); CREATE TABLE d.t AS SELECT 42 AS value;",folder)
        quota=None
        if mode=='staging-full':
            with sqlite3.connect(folder/'engine.sqlite') as metadata:repository=metadata.execute('SELECT repository_id FROM control_format').fetchone()[0]
            staging=folder/'staging'/repository;staging.mkdir(parents=True)
            (staging.parent/'upload-Foregn').write_bytes(b'preserve outside owned namespace')
            quota=(staging/'upload-Quota1').open('wb');quota.truncate(512*1024*1024);fcntl.flock(quota,fcntl.LOCK_EX)  # the largest quota (sparse)
            (staging/'upload-Stale1').write_bytes(b'derived orphan');(staging/'upload-unrelated-user-file').write_bytes(b'preserve')
        Relay.mode=mode
        statement=settings.get(mode,'')+attach+"CALL differential_create_snapshot('d',request_id:='publish');"
        success=mode=='descriptor-response-loss'
        result=run(statement,folder,0 if success else 1)
        if not success:assert 'completed; S3 export remains pending' in result.stderr,result.stderr
        if mode!='staging-full':assert Relay.triggered>0,mode
        with sqlite3.connect(folder/'engine.sqlite') as metadata:
            assert metadata.execute('SELECT state FROM export_roots').fetchall()==[('complete' if success else 'pending',)]
            assert metadata.execute('SELECT count(*) FROM snapshots').fetchone()[0]==1
        assert run(attach+'SELECT value FROM d.t;',folder).stdout.endswith('value\n42\n')
        if quota:
            quota.close();assert not (staging/'upload-Stale1').exists();assert (staging/'upload-unrelated-user-file').read_bytes()==b'preserve'
            assert (staging.parent/'upload-Foregn').read_bytes()==b'preserve outside owned namespace'
        Relay.mode='none';retry=run(statement,folder)
        rows=list(csv.DictReader(io.StringIO(retry.stdout)));assert rows[0]['snapshot_id']=='1' and rows[0]['export_state']=='complete',rows
        assert run(statement,folder).stdout==retry.stdout
        with sqlite3.connect(folder/'engine.sqlite') as metadata:assert metadata.execute('SELECT count(*) FROM snapshots').fetchone()[0]==1
        if mode=='descriptor-response-loss':
            descriptor=rows[0]['export_locator']
            location=f"differential:s3://differential-sdk-test/fault-publication/{descriptor}"
            assert run(f"ATTACH '{location}' AS e ({S3}); SELECT value FROM e.t;",folder).stdout.endswith('value\n42\n')
            Relay.mode='descriptor-unavailable';Relay.triggered=0
            failed=run(f"ATTACH '{location}' AS e ({S3});",folder,1)
            assert Relay.triggered>=1 and 'unavailable_object' in failed.stderr,failed.stderr
            Relay.mode='none'
            results.append('export attached by location after a lost reply; an unavailable descriptor fails cleanly')
        results.append(mode);print('PASS publication fault',mode,flush=True)
    # A branch-head PUT that succeeded but whose response was lost: the SDK's retry fails its
    # If-Match against our own first write, which must be recognised (no false head_conflict).
    folder=base/'head-response-loss';folder.mkdir();Relay.mode='none'
    S3=f"ENDPOINT '{proxy}', PATH_STYLE true, ALLOW_HTTP true"
    attach="ATTACH 'differential:sales' AS d;"
    run(f"ATTACH 'differential:sales' AS d (DATA_PATH 's3://differential-sdk-test/fault-heads/', {S3}); CREATE TABLE d.t AS SELECT 1 AS value; CALL differential_create_snapshot('d');",folder)
    Relay.mode='head-response-loss';Relay.triggered=0
    first=run(attach+"UPDATE d.t SET value=2; CALL differential_create_snapshot('d');",folder)
    assert Relay.triggered==1 and ',complete,complete,' in first.stdout,(Relay.triggered,first.stdout)
    Relay.mode='none'
    run(attach+"UPDATE d.t SET value=3; CALL differential_create_snapshot('d');",folder)
    reader=base/'head-response-loss-reader';reader.mkdir()
    latest=run(f"ATTACH 'differential:s3://differential-sdk-test/fault-heads/heads/sales' AS h ({S3}); SELECT value FROM h.t;",reader)
    assert latest.stdout.endswith('value\n3\n'),latest.stdout
    results.append('head-response-loss');print('PASS a branch-head write whose response was lost is recognised as our own, not as a conflict',flush=True)
    Relay.mode='multipart-complete';Relay.triggered=0;Relay.aborts=0
    result=subprocess.run([str(TEST_BINARIES/'s3_test'),proxy,str(base)],env=dict(os.environ,DIFFERENTIAL_TEST_BUCKET_EXISTS='1'),text=True,capture_output=True,timeout=60)
    assert result.returncode==1 and 'multipart complete' in result.stderr,(result.returncode,result.stdout,result.stderr)
    assert Relay.triggered==3 and Relay.aborts==1,(Relay.triggered,Relay.aborts)
    results.append('multipart completion failure: three SDK attempts and abort');print('PASS multipart completion failure and abort',flush=True)
finally:server.shutdown();server.server_close()
evidence_path('s3-publication-faults.json').write_text(json.dumps({'scope':'SDK against disposable MinIO with HTTP fault relay; no real AWS','passed':results},indent=2)+'\n')
