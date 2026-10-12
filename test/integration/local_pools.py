#!/usr/bin/env python3
"""Native lifecycle with payloads on a second local volume/path; disposable fixtures only."""
import csv
import io
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import tempfile
from evidence import evidence_path
ROOT = Path(__file__).resolve().parents[2]
import sys as _sys
_sys.path.insert(0,str(ROOT/'test/integration'))
from paths import DUCKDB_CLI, EXTENSION, DUCKDB_LIBRARY, BUILD_DIR

CLI = DUCKDB_CLI
EXT = EXTENSION
with tempfile.TemporaryDirectory(prefix='differential-local-pools-') as tmp:
    folder = Path(tmp); repo = folder/'metadata'; pool = folder/'payload'; untouched = folder/'never-created'
    env = dict(os.environ, DIFFERENTIAL_STORE_ROOT=str(repo))
    def run(sql, check=True):
        r = subprocess.run([CLI, '-init', '/dev/null', '-unsigned', '-batch', '-bail', '-csv'], input=f"LOAD '{EXT}';"+sql, text=True, capture_output=True, env=env, timeout=90, cwd=folder)
        if check: assert r.returncode == 0, (r.stdout, r.stderr, sql)
        return r.stdout if check else r
    run(f"EXPLAIN SELECT * FROM differential_import_database('{folder/'x.db'}','unused',data_path:='{untouched}'); PREPARE p AS SELECT * FROM differential_import_database('{folder/'x.db'}','unused',data_path:='{untouched}');")
    assert not repo.exists() and not untouched.exists()
    # DATA_PATH is resolved once (relative to the working directory) and fixed per database.
    run("ATTACH 'differential:sales' AS d (DATA_PATH 'payload'); CREATE TABLE d.t AS SELECT 42 AS i; CREATE VIEW d.v AS SELECT * FROM d.t; CALL differential_create_snapshot('d',name:='baseline');")
    run(f"ATTACH 'differential:sales' AS d (DATA_PATH '{pool}'); SELECT 1;")
    assert run("ATTACH 'differential:sales' AS d (DATA_PATH 'elsewhere');",False).returncode != 0
    def counts():
        with sqlite3.connect(repo/'engine.sqlite') as con:
            return con.execute('SELECT (SELECT count(*) FROM objects WHERE data IS NOT NULL),(SELECT count(*) FROM local_objects)').fetchone()
    before = counts(); assert before[0] == 0 and before[1] > 0, before
    sentinel=pool/'keep.txt';sentinel.write_text('unowned')
    foreign=pool/'differential-pools'/'unregistered-namespace';foreign.mkdir();(foreign/'keep.txt').write_text('foreign')
    run("CALL differential_create_branch('sales','work',snapshot:=1); CALL differential_clone_database('sales','copy',snapshot:=1);")
    assert counts() == before
    run("ATTACH 'differential:sales' AS d; UPDATE d.t SET i=99; CALL differential_create_snapshot('d');")
    assert run("ATTACH 'differential:sales' AS d (SNAPSHOT_VERSION 1); SELECT i FROM d.v;") == 'i\n42\n'
    assert run("ATTACH 'differential:copy' AS d; SELECT i FROM d.t;") == 'i\n42\n'
    run("CALL differential_restore('sales',1,branch:='main',request_id:='restore'); CALL differential_drop_branch('sales','work'); CALL differential_expire_snapshots('sales',versions:=[1]);")
    for _ in range(6): run("CALL differential_cleanup_old_files(max_objects:=100);")
    assert sentinel.read_text()=='unowned' and (foreign/'keep.txt').read_text()=='foreign'
    assert run("ATTACH 'differential:sales' AS d; SELECT i FROM d.t;") == 'i\n42\n'
    assert run("ATTACH 'differential:copy' AS d; SELECT i FROM d.t;") == 'i\n42\n'
    with sqlite3.connect(repo/'engine.sqlite') as con:
        locations = con.execute('SELECT p.path,p.repository_id,p.id,o.hash FROM local_objects o JOIN local_pools p ON p.id=o.pool').fetchall()
        assert con.execute('PRAGMA foreign_key_check').fetchall() == []
        assert con.execute('SELECT count(*) FROM local_deletions').fetchone()[0] == 0
    for path, repository, provider, digest in locations:
        assert (Path(path)/'differential-pools'/repository/provider/digest[:2]/digest).stat().st_size == 65536
    providers = list(csv.DictReader(io.StringIO(run('SELECT * FROM differential_providers();'))))
    assert any(p['data_path'] == str(pool.resolve()) for p in providers), providers
    databases = list(csv.DictReader(io.StringIO(run('SELECT * FROM differential_databases();'))))
    assert all(d['data_path']==str(pool.resolve()) for d in databases), databases
    lineage=list(csv.DictReader(io.StringIO(run("SELECT source_database_id,source_snapshot_id FROM differential_snapshots('copy');"))))[0]
    assert lineage['source_database_id'] and lineage['source_snapshot_id']=='1'
    run(f"ATTACH '{folder/'offline.db'}' AS offline; CREATE TABLE offline.imported AS SELECT 7 AS i; CHECKPOINT offline; DETACH offline; CALL differential_import_database('{folder/'offline.db'}','imported',data_path:='{pool}');")
    assert run("ATTACH 'differential:imported' AS d; SELECT i FROM d.imported;")=='i\n7\n'
    report = {'passed':True,'physical_payload_locations':len(locations),'inline_payload_objects':counts()[0], 'scope':'same host, separate directories; no separate physical volume or power-loss claim'}
evidence_path('local-pools.json').write_text(json.dumps(report,indent=2)+'\n')
print('PASS separate local payload pool: bind safety, retries, native reopen/history, zero-copy branch/clone, restore/delete/GC, provider introspection')
