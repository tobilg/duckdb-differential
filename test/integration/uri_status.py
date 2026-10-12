#!/usr/bin/env python3
"""Regression: never normalize an unconfigured/malformed managed URI into local IO."""
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
env=dict(os.environ)
for name in ('DIFFERENTIAL_STORE_ROOT','DIFFERENTIAL_PROBE_ROOT','DIFFERENTIAL_ENABLE_PROBES'):env.pop(name,None)
def run(sql):
    return subprocess.run([CLI,'-init','/dev/null','-unsigned','-csv','-c',f"LOAD '{EXT}';"+sql],env=env,capture_output=True,text=True,timeout=15)
r=run("SELECT * FROM differential_status(); ATTACH 'diff://unconfigured' AS d;")
assert r.returncode!=0 and 'Expected a managed database branch or public snapshot URI' in r.stderr,(r.stdout,r.stderr)
assert 'v1_complete,false' in r.stdout
evidence_path('status.csv').write_text(r.stdout)
print('PASS raw URI fails closed before default repository creation; status reports incomplete v1')
with tempfile.TemporaryDirectory(prefix='differential-uri-') as tmp:
    env['DIFFERENTIAL_STORE_ROOT']=str(Path(tmp)/'lazy')
    r=run("EXPLAIN SELECT * FROM differential_clone_database('planned','x'); PREPARE p AS SELECT * FROM differential_clone_database('prepared','y');")
    assert r.returncode==0 and not Path(env['DIFFERENTIAL_STORE_ROOT']).exists(),(r.stdout,r.stderr)
    print('PASS LOAD, EXPLAIN and PREPARE do not create repository')
    env['DIFFERENTIAL_STORE_ROOT']=tmp
    env['DIFFERENTIAL_ENABLE_PROBES']='1'
    for uri in ('diff:///malformed','diff://../escape'):
        r=run(f"ATTACH '{uri}' AS d;")
        assert r.returncode and 'Invalid differential URI' in r.stderr,(r.stdout,r.stderr)
    r=run("ATTACH 'diff:/malformed' AS d(TYPE differential);")
    assert r.returncode and ('Invalid differential URI' in r.stderr or 'Expected diff:// URI' in r.stderr),r.stderr
    r=run("ATTACH 'diff://live' AS d(TYPE differential); CREATE TABLE d.t AS SELECT 1; DETACH d; ATTACH 'diff://live' AS d(TYPE differential,READ_ONLY);")
    assert r.returncode and 'Independent readers must attach an immutable root' in r.stderr,r.stderr
    print('PASS malformed/traversal URIs and independent read-only live-file attachments rejected')
