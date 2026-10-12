#!/usr/bin/env python3
"""Force native concurrent-checkpoint WAL creation, then kill and replay both WALs."""
from pathlib import Path
import os
import subprocess
import sys
import tempfile
import threading
import time
from stock_capi import Host
from evidence import evidence_path
ROOT=Path(__file__).resolve().parents[2]
import sys as _sys
_sys.path.insert(0,str(ROOT/'test/integration'))
from paths import DUCKDB_CLI, EXTENSION, DUCKDB_LIBRARY, BUILD_DIR

EXT=EXTENSION

def wait(path):
    end=time.monotonic()+8
    while not path.exists():
        if time.monotonic()>end:raise RuntimeError(f'timeout: {path}')
        time.sleep(.01)

if len(sys.argv)>1 and sys.argv[1]=='child':
    folder=Path(sys.argv[2]);gate=folder/'gate'
    host=Host();a=host.connect();b=host.connect();reader=host.connect()
    host.query(a,f"LOAD '{EXT}'; ATTACH 'diff://probe' AS d (TYPE differential); CREATE TABLE d.t(i INT); INSERT INTO d.t VALUES(1); CHECKPOINT d;")
    host.query(reader,'BEGIN; SELECT count(*) FROM d.t;')
    host.query(a,'INSERT INTO d.t VALUES(2);')
    (folder/'gate.arm').touch()
    errors=[]
    def checkpoint():
        try:host.query(a,'CHECKPOINT d;')
        except BaseException as e:errors.append(str(e))
    worker=threading.Thread(target=checkpoint,daemon=True);worker.start()
    wait(folder/'gate.entered')
    host.query(b,'INSERT INTO d.t VALUES(3);')
    if not os.environ.get('DIFFERENTIAL_TEST_DURABLE'):
        assert (folder/'probe.wal.checkpoint').exists()
    (folder/'ack').touch()
    while True:time.sleep(.1)
else:
    with tempfile.TemporaryDirectory(prefix='differential-sidecars-') as tmp:
        folder=Path(tmp);trace=folder/'trace.tsv'
        env=dict(os.environ,DIFFERENTIAL_ENABLE_PROBES='1',DIFFERENTIAL_PROBE_ROOT=tmp,DIFFERENTIAL_PROBE_TRACE=str(trace),DIFFERENTIAL_PROBE_GATE=str(folder/'gate'))
        if os.environ.get('DIFFERENTIAL_TEST_DURABLE'):
            env.pop('DIFFERENTIAL_PROBE_ROOT')
            env['DIFFERENTIAL_STORE_ROOT']=tmp
        child=subprocess.Popen([sys.executable,__file__,'child',tmp],env=env,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
        try:
            wait(folder/'ack');child.kill();child.wait(timeout=5)
            assert child.returncode==-9
            env.pop('DIFFERENTIAL_PROBE_GATE')
            cli=DUCKDB_CLI
            result=subprocess.run([cli,'-init','/dev/null','-unsigned','-csv','-c',f"LOAD '{EXT}'; ATTACH 'diff://probe' AS d (TYPE differential); SELECT sum(i),count(*) FROM d.t;"],env=env,capture_output=True,text=True,timeout=20)
            assert result.returncode==0,(result.stdout,result.stderr)
            assert '6,3' in result.stdout,result.stdout
            text=trace.read_text()
            assert 'open\tdiff://probe.wal.checkpoint' in text
            assert 'open\tdiff://probe.wal.recovery' in text
            assert 'move.from\tdiff://probe.wal.recovery' in text
            evidence_path('durable-sidecar-trace.tsv' if os.environ.get('DIFFERENTIAL_TEST_DURABLE') else 'phase0-sidecar-trace.tsv').write_text(text)
            print('PASS concurrent checkpoint .wal.checkpoint, SIGKILL after secondary commit, .wal.recovery merge/rename, all 3 rows recovered')
        finally:
            if child.poll() is None:child.kill();child.wait(timeout=5)
            out,err=child.communicate()
            if err:print(err,file=sys.stderr)
