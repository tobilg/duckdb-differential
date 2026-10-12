#!/usr/bin/env python3
"""DuckLake-aligned SQL surface: settings, ATTACH options, catalog-first functions,
commit metadata, bulk expiry and snapshot-on-checkpoint. Disposable repositories only."""
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
from paths import DUCKDB_CLI, EXTENSION, DUCKDB_LIBRARY, BUILD_DIR

CLI=DUCKDB_CLI
EXT=EXTENSION
results=[]
def passed(name):
    results.append(name);print('PASS',name,flush=True)
with tempfile.TemporaryDirectory(prefix='differential-sql-surface-') as tmp:
    repo=Path(tmp)/'repo';unused=Path(tmp)/'unused'
    def run(sql,expected=0):
        r=subprocess.run([CLI,'-init','/dev/null','-unsigned','-batch','-bail','-csv'],
                         input=f"LOAD '{EXT}'; SET differential_repository='{repo}';"+sql,text=True,capture_output=True,
                         env=dict(os.environ,DIFFERENTIAL_STORE_ROOT=str(unused)),timeout=60)
        assert r.returncode==expected,(sql,r.returncode,r.stdout,r.stderr)
        return r.stdout if expected==0 else r.stderr
    def rows(sql):
        out=run(sql).splitlines()
        return list(csv.DictReader(io.StringIO('\n'.join(out))))
    def last(sql):
        """Rows of the final result block: each statement prints its own header line."""
        out=run(sql).splitlines()
        starts=[i for i,l in enumerate(out) if l and not l[0].isdigit() and not l[0] in '"{' and l.split(',')[0].replace('_','').isalpha()]
        return list(csv.DictReader(io.StringIO('\n'.join(out[starts[-1]:]))))
    # Settings and lazy repository creation.
    assert rows("SELECT current_setting('differential_repository') AS r;")==[{'r':str(repo)}]
    assert not repo.exists() and not unused.exists()
    passed('SET differential_repository selects the root without creating it')
    # ATTACH creates on first use; options select branches and snapshots.
    sales="ATTACH 'differential:sales' AS sales;"
    run(sales+"CREATE TABLE sales.t AS SELECT 1 AS v;")
    assert repo.exists() and not unused.exists()
    run("ATTACH 'differential:absent' AS a (CREATE_IF_NOT_EXISTS false);",1)
    first=last(sales+"SELECT snapshot_id,snapshot_time IS NOT NULL AS timed FROM differential_create_snapshot('sales',name:='v1',author:='Ada',message:='first load',extra_info:='{\"rows\": 1}',request_id:='v1');")
    assert first==[{'snapshot_id':'1','timed':'true'}],first
    replay=last(sales+"SELECT snapshot_id FROM differential_create_snapshot('sales',name:='v1',author:='Ada',message:='first load',extra_info:='{\"rows\": 1}',request_id:='v1');")
    assert replay==[{'snapshot_id':'1'}]
    run(sales+"CALL differential_create_snapshot('sales',name:='v1',author:='Bob',request_id:='v1');",1)
    meta=rows("SELECT snapshot_id,snapshot_name,author,commit_message,commit_extra_info FROM differential_snapshots('sales');")
    assert meta==[{'snapshot_id':'1','snapshot_name':'v1','author':'Ada','commit_message':'first load','commit_extra_info':'{"rows": 1}'}],meta
    passed('ATTACH creates databases; snapshots carry DuckLake-style author/message/extra_info; request replay')
    run(sales+"UPDATE sales.t SET v=2; CALL differential_create_snapshot('sales'); UPDATE sales.t SET v=3;")
    assert rows("ATTACH 'differential:sales' AS s (SNAPSHOT_VERSION 1); SELECT v FROM s.t;")==[{'v':'1'}]
    assert rows("ATTACH 'differential:sales' AS s (SNAPSHOT_NAME 'v1'); SELECT v FROM s.t;")==[{'v':'1'}]
    assert rows("ATTACH 'differential:sales' AS s (SNAPSHOT_TIME '2100-01-01'); SELECT v FROM s.t;")==[{'v':'2'}]
    assert rows("ATTACH 'differential:sales' AS s (READ_ONLY); SELECT v FROM s.t;")==[{'v':'2'}]
    assert rows("ATTACH 'differential:sales' AS s (SNAPSHOT_VERSION 1); SELECT * FROM differential_current_snapshot('sales');")==[{'id':'2'}]
    run("ATTACH 'differential:sales' AS s (SNAPSHOT_VERSION 1); INSERT INTO s.t VALUES(9);",1)
    run("ATTACH 'differential:sales' AS s (SNAPSHOT_VERSION 1, SNAPSHOT_NAME 'v1');",1)
    run("ATTACH 'differential:sales' AS s (NO_SUCH_OPTION 1);",1)
    run("ATTACH 'differential:sales' AS s (BRANCH 'missing');",1)
    passed('SNAPSHOT_VERSION/SNAPSHOT_NAME/SNAPSHOT_TIME and READ_ONLY attach immutable snapshots; selectors and unknown options validated')
    # Catalog-first lifecycle functions with named parameters.
    branch=last("SELECT snapshot_id FROM differential_create_branch('sales','dev');")
    assert branch==[{'snapshot_id':'2'}],branch  # default source: the branch's latest snapshot
    assert rows("ATTACH 'differential:sales' AS dev (BRANCH 'dev'); SELECT v FROM dev.t;")==[{'v':'2'}]
    assert rows("ATTACH 'differential:sales' AS dev (BRANCH 'dev'); SELECT * FROM differential_current_snapshot('dev');")==[{'id':'2'}]
    clone=last("SELECT snapshot_id FROM differential_clone_database('sales','copy',snapshot:=1);")
    assert clone==[{'snapshot_id':'1'}]
    assert rows("ATTACH 'differential:copy' AS c; SELECT v FROM c.t;")==[{'v':'1'}]
    restored=last("SELECT preservation_snapshot_id,snapshot_id FROM differential_restore('sales',1);")
    assert restored==[{'preservation_snapshot_id':'3','snapshot_id':'4'}],restored
    assert rows(sales+"SELECT v FROM sales.t;")==[{'v':'1'}]
    restore_msg=rows("SELECT commit_message FROM differential_snapshots('sales') WHERE snapshot_id IN (3,4) ORDER BY snapshot_id;")
    assert restore_msg==[{'commit_message':'preserved before restore'},{'commit_message':'restored to snapshot 1'}],restore_msg
    run("CALL differential_drop_branch('sales','dev');")
    passed('create_branch/clone_database/restore/drop_branch take the database first and named options; system snapshots are labelled')
    dry=rows("SELECT snapshot_id,dry_run FROM differential_expire_snapshots('sales',older_than:=TIMESTAMPTZ '2100-01-01',dry_run:=true);")
    assert [r['snapshot_id'] for r in dry]==['1','2','3','4'] and all(r['dry_run']=='true' for r in dry),dry
    expired=rows("SELECT snapshot_id FROM differential_expire_snapshots('sales',versions:=[2,3]);")
    assert expired==[{'snapshot_id':'2'},{'snapshot_id':'3'}]
    assert rows("SELECT snapshot_id FROM differential_snapshots('sales') WHERE retired;")==[{'snapshot_id':'2'},{'snapshot_id':'3'}]
    run("ATTACH 'differential:sales' AS s (SNAPSHOT_VERSION 2);",1)
    assert 'unsupported_provider_capability' in run("CALL differential_evict_snapshots('sales',versions:=[1]);",1)
    passed('expire_snapshots selects by versions/older_than with dry_run; eviction requires a completed export')
    cleaned=rows("SELECT deleted_objects FROM differential_cleanup_old_files(cleanup_all:=true,max_objects:=10);")
    assert sum(int(r['deleted_objects']) for r in cleaned)>0 and rows("SELECT candidate_objects FROM differential_cleanup_old_files(dry_run:=true);")==[{'candidate_objects':'0'}],cleaned
    assert rows("SELECT orphaned_files FROM differential_delete_orphaned_files(dry_run:=true);")==[{'orphaned_files':'0'}]
    passed('cleanup_old_files(cleanup_all) drains the queues; delete_orphaned_files dry run')
    # Snapshot on every completed checkpoint (opt-in).
    auto="SET differential_snapshot_on_checkpoint=true; ATTACH 'differential:auto' AS a;"
    run(auto+"CREATE TABLE a.t(i INT); CHECKPOINT a; INSERT INTO a.t VALUES(1); CHECKPOINT a; INSERT INTO a.t VALUES(2); CHECKPOINT a; CHECKPOINT a;")
    auto_snaps=rows("SELECT snapshot_id,commit_message FROM differential_snapshots('auto');")
    assert len(auto_snaps)>=3 and all(r['commit_message']=='checkpoint' for r in auto_snaps),auto_snaps
    newest=auto_snaps[-1]['snapshot_id']
    assert rows(f"ATTACH 'differential:auto' AS a (SNAPSHOT_VERSION {newest}); SELECT sum(i) AS s FROM a.t;")==[{'s':'3'}]
    count=len(auto_snaps)
    run(auto+"CHECKPOINT a;")
    assert len(rows("SELECT snapshot_id FROM differential_snapshots('auto');"))==count  # unchanged image: no new snapshot
    run("ATTACH 'differential:manual' AS m; CREATE TABLE m.t(i INT); CHECKPOINT m;")
    assert rows("SELECT count(*) AS n FROM differential_snapshots('manual');")==[{'n':'0'}]
    passed(f'differential_snapshot_on_checkpoint publishes each changed checkpoint ({count} snapshots); off by default')
    # Raw internal URIs remain usable for tools that hold canonical identities.
    uri=rows("SELECT uri FROM differential_branches('sales') WHERE branch_name='main';")[0]['uri']
    assert rows(f"ATTACH '{uri}' AS raw (TYPE differential); SELECT v FROM raw.t;")==[{'v':'1'}]
    passed('canonical diff:// URIs still attach with TYPE differential')
evidence_path('sql-surface.json').write_text(json.dumps({'passed':results},indent=2)+'\n')
