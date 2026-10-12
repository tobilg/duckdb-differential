#!/usr/bin/env python3
from pathlib import Path
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
with tempfile.TemporaryDirectory(prefix='differential-reuse-') as tmp:
    folder=Path(tmp);trace=folder/'trace.tsv'
    env=dict(os.environ,DIFFERENTIAL_ENABLE_PROBES='1',DIFFERENTIAL_STORE_ROOT=tmp,DIFFERENTIAL_PROBE_CAPTURE='1',DIFFERENTIAL_PROBE_TRACE=str(trace))
    def run(sql):
        r=subprocess.run([CLI,'-init','/dev/null','-unsigned','-csv','-c',f"LOAD '{EXT}'; ATTACH 'diff://probe' AS d(TYPE differential);"+sql],env=env,capture_output=True,text=True,timeout=60)
        assert r.returncode==0,(r.stdout,r.stderr)
        return r.stdout
    def writes():
        return {int(parts[3]) for line in trace.read_text().splitlines() if (parts:=line.split('\t'))[0]=='pwrite' and parts[1]=='diff://probe' and int(parts[3])>=12288}
    out=run("CREATE TABLE d.old AS SELECT range AS id,sha256(range::VARCHAR) AS payload FROM range(30000); CALL differential_probe_checkpoint('d');")
    root=out.splitlines()[1].split(',')[1];before=writes()
    run('DROP TABLE d.old; CHECKPOINT d;')
    trace.write_text('')
    run("CREATE TABLE d.new AS SELECT range AS id,sha256(('new'||range)::VARCHAR) AS payload FROM range(30000); CHECKPOINT d;")
    reused=before&writes();assert reused,'Fixture did not actually reuse native block offsets'
    out=run(f"ATTACH 'diff://roots/{root}' AS history(TYPE differential,READ_ONLY); SELECT count(*),sum(id) FROM history.old; SELECT count(*) FROM history.old WHERE payload<>sha256(id::VARCHAR);")
    assert '30000,449985000' in out and out.strip().endswith('0'),out
    print('PASS native block offsets reused:',len(reused),'; historical data and catalog unchanged')
    evidence_path('block-reuse.txt').write_text(f'Native data-block offsets reused: {len(reused)}\nHistorical rows: 30000; sum(id): 449985000; mismatched payload hashes: 0\n')
