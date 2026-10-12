#!/usr/bin/env python3
"""Race fresh writer processes against restore/delete; every acknowledged row stays reachable."""
from concurrent.futures import ThreadPoolExecutor
import csv
import io
import json
import os
from pathlib import Path
import subprocess
import tempfile
import threading
from evidence import evidence_path
ROOT=Path(__file__).resolve().parents[2]
import sys as _sys
_sys.path.insert(0,str(ROOT/'test/integration'))
from paths import DUCKDB_CLI, EXTENSION, DUCKDB_LIBRARY, BUILD_DIR

EXT=EXTENSION
CLI=DUCKDB_CLI
results=[]
with tempfile.TemporaryDirectory(prefix='differential-management-races-') as tmp:
    env=dict(os.environ,DIFFERENTIAL_STORE_ROOT=tmp)
    def run(sql,check=True):
        r=subprocess.run([CLI,'-init','/dev/null','-unsigned','-batch','-bail','-csv'],input=f"LOAD '{EXT}';"+sql,text=True,capture_output=True,env=env,timeout=30)
        if check:assert r.returncode==0,(sql,r.returncode,r.stdout,r.stderr)
        return r
    def total(uri,read_only=True):
        r=run(f"ATTACH '{uri}' AS d(TYPE differential{',READ_ONLY' if read_only else ''}); SELECT sum(i) AS value FROM d.t;")
        return int(list(csv.DictReader(io.StringIO(r.stdout)))[0]['value'])
    run("ATTACH 'differential:sales' AS d; CREATE TABLE d.t(i INT); INSERT INTO d.t VALUES(1); CALL differential_create_snapshot('d');")
    for kind in ('drop','restore'):
        for iteration in range(8):
            name=f'{kind}{iteration}';uri=f'diff://sales/branches/{name}'
            run(f"CALL differential_create_branch('sales','{name}',snapshot:=1);")
            writer=f"ATTACH '{uri}' AS d(TYPE differential); INSERT INTO d.t VALUES(2); SELECT 'ACK' AS acknowledged;"
            management=f"CALL differential_drop_branch('sales','{name}',request_id:='{name}');" if kind=='drop' else f"CALL differential_restore('sales',1,branch:='{name}',request_id:='{name}');"
            gate=threading.Barrier(2)
            def compete(sql):gate.wait();return run(sql,False)
            with ThreadPoolExecutor(max_workers=2) as executor:
                w=executor.submit(compete,writer);m=executor.submit(compete,management)
                written,managed=w.result(),m.result()
            acknowledged=written.returncode==0
            if acknowledged:assert 'ACK' in written.stdout
            else:assert any(s in written.stderr.lower() for s in ('busy','attachment','deleted','not found','pending compound')),(written.stdout,written.stderr)
            retried=managed.returncode!=0
            if retried:
                assert any(s in managed.stderr.lower() for s in ('busy','attachment')),(managed.stdout,managed.stderr)
                managed=run(management)
            operation=list(csv.DictReader(io.StringIO(managed.stdout)))[0]
            assert run(management).stdout==managed.stdout
            preservation=operation['preservation_snapshot_id']
            preserved=total(f'diff://sales/snapshots/{preservation}')
            if kind=='drop':
                assert preserved==(3 if acknowledged else 1),(kind,acknowledged,preserved)
                assert run(f"ATTACH '{uri}' AS gone(TYPE differential);",False).returncode!=0
                live=None
            else:
                assert total(operation['uri'])==1
                live=total(uri,False)
                if acknowledged:assert 3 in (preserved,live),(kind,preserved,live)
                else:assert preserved==live==1
            assert total('diff://sales/snapshots/1')==1
            results.append({'kind':kind,'iteration':iteration,'writer_acknowledged':acknowledged,'management_retried':retried,'preserved_sum':preserved,'live_sum':live})
evidence_path('management-races.json').write_text(json.dumps({'passed':len(results),'scope':'fresh processes competing for a branch; retries after busy outcomes; not every possible schedule','results':results},indent=2)+'\n')
print('PASS 16 writer-versus-restore/delete races; acknowledged rows preserved and requests replayed once')
