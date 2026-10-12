#!/usr/bin/env python3
"""Repository selection, execution-only initialization, stable errors and native timing wrapper."""
import csv
import io
import json
import os
from pathlib import Path
import subprocess
import tempfile
from evidence import evidence_path
ROOT=Path(__file__).resolve().parents[2]
import sys as _sys
_sys.path.insert(0,str(ROOT/'test/integration'))
from paths import DUCKDB_CLI, EXTENSION, DUCKDB_LIBRARY, BUILD_DIR, TEST_BINARIES

EXT=EXTENSION;CLI=DUCKDB_CLI
with tempfile.TemporaryDirectory(prefix='differential-observe-') as tmp:
    root=Path(tmp)/'selected';unused=Path(tmp)/'unused';env=dict(os.environ,DIFFERENTIAL_STORE_ROOT=str(unused))
    def run(sql,expected=0):
        r=subprocess.run([CLI,'-init','/dev/null','-unsigned','-batch','-bail','-csv'],input=f"LOAD '{EXT}';"+sql,text=True,capture_output=True,env=env,timeout=30)
        assert r.returncode==expected,(r.returncode,r.stdout,r.stderr)
        return r
    choose=f"SET differential_repository='{root}';"
    # Selecting a repository is configuration only: nothing is created until first use.
    run(choose+"SELECT current_setting('differential_repository');")
    assert not root.exists() and not unused.exists()
    assert run("SELECT status FROM differential_status() WHERE capability='repository_open';").stdout=='status\nfalse\n'
    assert not unused.exists()
    chosen=run("PREPARE s AS SELECT status FROM differential_status() WHERE capability='repository_root';"+choose+"EXECUTE s;")
    assert chosen.stdout.endswith('status\n'+str(root)+'\n'),chosen.stdout
    assert not unused.exists()
    attach="ATTACH 'differential:sales' AS d;"
    run(choose+attach+"CREATE TABLE d.t AS SELECT 42 AS value; CALL differential_create_snapshot('d'); CHECKPOINT d;")
    assert root.exists() and not unused.exists()
    result=run(choose+attach+"INSERT INTO d.t VALUES(99); CALL differential_create_snapshot('d'); SELECT * FROM differential_storage_stats();")
    lines=result.stdout.splitlines();index=lines.index('metric,value');metrics={row['metric']:int(row['value']) for row in csv.DictReader(io.StringIO('\n'.join(lines[index:])))}
    for name in ('native_commit_calls','native_commit_nanoseconds','explicit_checkpoint_calls','local_write_nanoseconds','snapshot_publish_calls','manifest_seal_calls','local_payload_bytes','local_authority_main_bytes'):
        assert metrics[name]>0,(name,metrics[name])
    assert metrics['remote_pending_exports']==0
    for name in ('local_logical_read_bytes','local_materialized_payload_bytes','local_logical_extent_runs','local_chunk_size_bytes'):
        assert metrics[name]>0,(name,metrics[name])
    for name in ('local_derived_staging_bytes','local_gc_queued_objects','local_gc_queued_roots','local_journal_bytes','writer_conflicts','pin_conflicts','remote_cache_misses'):
        assert name in metrics,name
    errors=[("CALL differential_expire_snapshots('missing',versions:=[1]);",'not_found'),
            ("CALL differential_clone_database('sales','sales');",'name_conflict'),
            ("BEGIN; CALL differential_clone_database('sales','bad');",'active_attachment_or_transaction'),
            ("CALL differential_expire_snapshots('sales',versions:=[1]); ATTACH 'differential:sales' AS old (SNAPSHOT_VERSION 1);",'retired')]
    for sql,category in errors:
        result=run(choose+sql,1);assert '[differential:'+category+']' in result.stderr,result.stderr
    rejected=run(choose+attach+f"SET differential_repository='{Path(tmp)/'second'}';",1)
    assert not (Path(tmp)/'second').exists()
evidence_path('observability.json').write_text(json.dumps({'status':'passed','checks':['SQL repository selection before first use','bind/plan no initialization','default override untouched','native commit/checkpoint instrumentation','stable not-found/name-conflict/transaction/retired categories','repository change rejected after use']},indent=2)+'\n')
print('PASS SQL repository selection, execution-only initialization, native timing counters and stable error categories')
