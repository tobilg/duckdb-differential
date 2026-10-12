#!/usr/bin/env python3
"""Protocol fault harness; AWS SDK is the client. This is not real S3 coverage."""
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
import json
import os
import secrets
from pathlib import Path
import subprocess
import tempfile
import threading
import time
from evidence import evidence_path
ROOT=Path(__file__).resolve().parents[2]
import sys as _sys
_sys.path.insert(0,str(ROOT/'test/integration'))
from paths import DUCKDB_CLI, EXTENSION, DUCKDB_LIBRARY, BUILD_DIR, TEST_BINARIES

class Handler(BaseHTTPRequestHandler):
    mode='valid';requests=0;active=0;peak=0;lock=threading.Lock();canary=''
    def log_message(self,*args):pass
    def do_GET(self):
        with Handler.lock:
            Handler.active+=1;Handler.peak=max(Handler.peak,Handler.active)
        try:self.get_impl()
        finally:
            with Handler.lock:Handler.active-=1
    def get_impl(self):
        Handler.requests+=1
        mode=Handler.mode
        if mode=='parallel':time.sleep(.15);mode='valid'
        if mode=='timeout':time.sleep(6)
        status={'missing':404,'denied':403,'denied-leak':403,'throttled':503}.get(mode,206)
        body=bytes(range(3,14));headers={'Content-Range':'bytes 3-13/64'}
        if mode in ('missing','denied','denied-leak','throttled'):
            code={'missing':'NoSuchKey','denied':'AccessDenied','denied-leak':'AccessDenied','throttled':'SlowDown'}[mode]
            message=Handler.canary if mode=='denied-leak' else 'Injected failure'
            body=f'<Error><Code>{code}</Code><Message>{message}</Message></Error>'.encode();headers={}
        elif mode=='wrong-range':headers['Content-Range']='bytes 4-14/64'
        elif mode=='full-object':status=200;body=bytes(range(64));headers={}
        elif mode=='corrupt':body=b'x'*11
        headers['Content-Length']=str(len(body))
        if mode=='short':body=body[:5]
        try:
            self.send_response(status)
            for k,v in headers.items():self.send_header(k,v)
            self.end_headers();self.wfile.write(body)
        except (BrokenPipeError,ConnectionResetError):pass
        self.close_connection=True
server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
results=[]
try:
    with tempfile.TemporaryDirectory(prefix='differential-s3-fault-') as tmp:
        for mode in ('valid','parallel','wrong-range','full-object','short','corrupt','missing','denied','denied-leak','throttled','timeout'):
            Handler.mode=mode;Handler.requests=0;Handler.peak=0
            Handler.canary=secrets.token_hex(24)
            env=dict(os.environ,DIFFERENTIAL_ENABLE_PROBES='1',AWS_ACCESS_KEY_ID='test_'+secrets.token_hex(8),AWS_SECRET_ACCESS_KEY=secrets.token_hex(24),AWS_EC2_METADATA_DISABLED='true',DIFFERENTIAL_S3_FAULT_MODE=mode,DIFFERENTIAL_ERROR_CANARY=Handler.canary)
            start=time.monotonic()
            result=subprocess.run([str(TEST_BINARIES/'s3_test'),f'http://127.0.0.1:{server.server_port}',tmp],env=env,text=True,capture_output=True,timeout=30)
            if result.returncode:raise AssertionError((mode,result.stdout,result.stderr))
            for private in (Handler.canary,env['AWS_ACCESS_KEY_ID'],env['AWS_SECRET_ACCESS_KEY']):
                assert private not in result.stdout+result.stderr, 'default output exposed a canary'
            if mode in ('short','throttled','timeout'):assert Handler.requests==3,(mode,Handler.requests)
            if mode=='parallel':assert Handler.requests==12 and 2<=Handler.peak<=4,(Handler.requests,Handler.peak)
            print(result.stdout,end='',flush=True)
            results.append({'mode':mode,'requests':Handler.requests,'peak_requests':Handler.peak,'elapsed_seconds':round(time.monotonic()-start,3),'status':'passed'})
finally:
    server.shutdown();server.server_close()
evidence_path('s3-fault-results.json').write_text(json.dumps({'scope':'HTTP protocol fault harness via AWS SDK, not real S3','results':results},indent=2)+'\n')
