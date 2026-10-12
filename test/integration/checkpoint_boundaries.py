#!/usr/bin/env python3
from pathlib import Path
import json
import os
import subprocess
import tempfile
from evidence import evidence_path
ROOT=Path(__file__).resolve().parents[2]
import sys as _sys
_sys.path.insert(0,str(ROOT/'test/integration'))
from paths import DUCKDB_CLI, EXTENSION, DUCKDB_LIBRARY, BUILD_DIR

CLI=DUCKDB_CLI
EXT=EXTENSION
results=[]
def run(folder,sql,ok=True):
    env=dict(os.environ,DIFFERENTIAL_ENABLE_PROBES='1',DIFFERENTIAL_STORE_ROOT=str(folder),DIFFERENTIAL_PROBE_TRACE=str(folder/'trace.tsv'))
    command=f"LOAD '{EXT}'; ATTACH 'diff://probe' AS d(TYPE differential);"+sql
    r=subprocess.run([CLI,'-init','/dev/null','-unsigned','-csv','-c',command],env=env,capture_output=True,text=True,timeout=30)
    if ok and r.returncode:raise AssertionError((r.stdout,r.stderr))
    return r
with tempfile.TemporaryDirectory(prefix='differential-checkpoint-') as tmp:
    base=Path(tmp)
    for skip,expected_wal in ((1000000000,True),(0,False)):
        folder=base/str(skip);folder.mkdir()
        run(folder,'CREATE TABLE d.t(i BIGINT); CHECKPOINT d;')
        (folder/'trace.tsv').write_text('')
        r=run(folder,f"SET checkpoint_threshold='1KB'; SET auto_checkpoint_skip_wal_threshold={skip}; INSERT INTO d.t SELECT range FROM range(10000); SELECT count(*) FROM d.t;")
        assert '10000' in r.stdout
        trace=(folder/'trace.tsv').read_text()
        assert 'checkpoint.start' in trace
        wal_writes=any(line.startswith('open\tdiff://probe.wal\t') and int(line.split('\t')[2])&2 for line in trace.splitlines())
        assert wal_writes==expected_wal,(skip,trace)
        assert '10000' in run(folder,'SELECT count(*) FROM d.t;').stdout
        results.append({'probe':'automatic-checkpoint','skip_wal_threshold':skip,'wal_written':wal_writes,'status':'passed'})
        print('PASS automatic checkpoint, WAL written:',wal_writes)
    folder=base/'failure';folder.mkdir()
    run(folder,'CREATE TABLE d.t AS SELECT 42 AS i; CHECKPOINT d;')
    (folder/'trace.tsv').write_text('')
    r=run(folder,"INSERT INTO d.t VALUES(43); SET debug_checkpoint_abort='BEFORE_HEADER_NON_FATAL'; CHECKPOINT d;",ok=False)
    assert r.returncode and 'Checkpoint failed' in r.stderr,r.stderr
    trace=(folder/'trace.tsv').read_text()
    assert trace.count('checkpoint.start')>trace.count('checkpoint.end'),trace
    assert '85,2' in run(folder,'SELECT sum(i),count(*) FROM d.t;').stdout
    results.append({'probe':'checkpoint-exception','end_callback_skipped':True,'prior_commit_survives':True,'status':'passed'})
    print('PASS skipped end callback on exception; acknowledged commit recovered')
evidence_path('checkpoint-boundary-results.json').write_text(json.dumps(results,indent=2)+'\n')
