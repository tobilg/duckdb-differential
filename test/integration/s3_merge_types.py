#!/usr/bin/env python3
"""Native merge types through evicted inputs and S3 target publication recovery.
Only disposable MinIO namespaces; all object operations use the extension's SDK.
"""
import json
from pathlib import Path
import sys
from evidence import evidence_path
from merge_types import CASES, typed
from s3_merge import Fixture


def main(endpoint, base):
    passed=[]
    for phase in ('uploaded','head_published'):
        f=Fixture(base,'native-types-'+phase,endpoint)
        ddl=','.join(f'c{i} {case[0]}' for i,case in enumerate(CASES))
        row=lambda side: ','.join(typed(case[side],case[0]) for case in CASES)
        updates=lambda side: ','.join(f'c{i}={typed(case[side],case[0])}' for i,case in enumerate(CASES))
        choices=','.join(f"'c{i}':'custom'" for i in range(len(CASES)))
        native=f.folder/'choices.duckdb'
        f.run(f"ATTACH 'differential:merge_db' AS d (DATA_PATH '{f.location}',{f.options});"
              f'CREATE TABLE d.x(id INT PRIMARY KEY,{ddl}); INSERT INTO d.x VALUES(1,{row(1)});'
              "CALL differential_create_snapshot('d'); CALL differential_create_branch('d','o',snapshot=>1); CALL differential_create_branch('d','t',snapshot=>1);"
              f"ATTACH 'differential:merge_db' AS o (BRANCH 'o'); UPDATE o.x SET {updates(2)}; CALL differential_create_snapshot('o');"
              f"ATTACH 'differential:merge_db' AS t (BRANCH 't'); UPDATE t.x SET {updates(3)}; CALL differential_create_snapshot('t');"
              f"ATTACH '{native}' AS choices; CREATE TABLE choices.fixes AS SELECT *, 'cells' AS _merge_action,map{{{choices}}} AS _merge_columns FROM t.x;")
        f.run("CALL differential_evict_snapshots('merge_db',versions=>[1,2,3]); CALL differential_cleanup_old_files(cleanup_all=>true);")
        p=f.run("SELECT conflicts,len(conflict_details[1].columns) AS columns FROM differential_merge_branch_preview('merge_db','o','t');")[-1][0]
        assert p=={'conflicts':1,'columns':len(CASES)},p
        merge="SELECT snapshot_id FROM differential_merge_branch('merge_db','o','t','o',request_id=>'native',resolution_tables=>map{'main.x':'choices.main.fixes'});"
        f.run(f"ATTACH '{native}' AS choices;"+merge,crash=phase)
        native.unlink()
        f.run("CALL differential_expire_snapshots('merge_db',versions=>[1,2,3]);")
        assert f.run(merge)[-1][0]['snapshot_id']==4
        assert f.run(merge)[-1][0]['snapshot_id']==4
        checks=' AND '.join(f'c{i} IS NOT DISTINCT FROM {typed(case[3],case[0])}' for i,case in enumerate(CASES))
        assert f.run("ATTACH 'differential:merge_db' AS m (BRANCH 'o');"+f'SELECT {checks} AS correct FROM m.x;')[-1][0]['correct']
        # Inspect through an independent repository and its S3 head, not the local
        # mapped result; verifies exact native types survive packs and remote reads.
        remote=f"differential:{f.location}heads/merge_db/o"
        assert f.run(f"ATTACH '{remote}' AS m (READ_ONLY,{f.options}); SELECT {checks} AS correct FROM m.x;",repo=f.folder/'reader')[-1][0]['correct']
        f.run('CALL differential_cleanup_old_files(cleanup_all=>true);')
        assert f.scalar('SELECT count(*) FROM compound_intents')==0
        assert f.scalar('SELECT count(*) FROM export_roots WHERE state=\'pending\'')==0
        passed.append(phase)
        print('PASS native S3 merge types: evicted inputs, frozen resolutions, expired inputs, removed source, exact remote result and retry after '+phase,flush=True)
    evidence_path('s3-merge-types.json').write_text(json.dumps({'passed':passed,'types':[c[0] for c in CASES],'scope':'disposable MinIO'},indent=2)+'\n')


if __name__=='__main__': main(sys.argv[1],sys.argv[2])
