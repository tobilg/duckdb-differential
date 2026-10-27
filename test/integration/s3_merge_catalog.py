#!/usr/bin/env python3
"""Table removal and view publication/recovery on the disposable pinned MinIO fixture."""
import json
from pathlib import Path
import sys

from evidence import evidence_path
from merge_catalog import CatalogFixture, setup_combined, verify_combined
from merge_schema import set_policy_version
from merge_tables import no_stage
from merge_lifecycle import inspect, abort_sql, start, release
from s3_merge import FaultRelay
from s3_merge_lifecycle import no_retention


def remote(f,query='SELECT * FROM r.visible ORDER BY id'):
    return f.run(f"ATTACH 'differential:{f.location}heads/merge_db/merged' AS r ({f.options}); {query};",
                 repo=f.folder/'catalog-reader')[-1]


def check(f):
    verify_combined(f)
    assert remote(f)==f.read('SELECT * FROM m.visible ORDER BY id')
    assert remote(f,'SELECT * FROM r.total')==[{'n':3}]
    assert remote(f,"SELECT table_name FROM duckdb_tables() WHERE database_name='r' ORDER BY 1")==[{'table_name':'x'}]
    assert remote(f,"SELECT view_name FROM duckdb_views() WHERE database_name='r' ORDER BY 1")==[{'view_name':'total'},{'view_name':'visible'}]
    assert f.scalar("SELECT count(*) FROM operations WHERE kind='merge' AND state='complete'")==1
    assert f.scalar("SELECT count(*) FROM branches WHERE name='merged'")==1
    assert f.scalar("SELECT count(*) FROM operations WHERE kind='merge_publication' AND state='pending'")==0
    f.run('CALL differential_cleanup_old_files(cleanup_all=>true);'); no_stage(f)


def main(endpoint,temporary):
    base=Path(temporary)/'s3-merge-catalog'; base.mkdir()
    passed=[]
    def ok(label): passed.append(label); print('PASS S3 merge catalog',label,flush=True)
    relay=FaultRelay(endpoint)
    try:
        f=CatalogFixture(base,'catalog-evicted',relay.endpoint); setup_combined(f); f.evict()
        before=f.scalar('SELECT count(*) FROM operations')
        rows=f.run(f.preview(',validate=>true'))[-1]; assert all(r['validation_state']=='valid' for r in rows),rows
        assert f.scalar('SELECT count(*) FROM operations')==before; f.hidden(); no_stage(f); f.remote(error='not_found')
        f.run(f.catalog()); check(f)
        assert f.scalar('SELECT count(*) FROM snapshots WHERE evicted=1 AND id<=3')==3
        ok('evicted snapshots preserve table/view removals and selected view definitions in a fresh remote reader; preview remains isolated')

        for phase in ('views_dropped','table_dropped','view_created','views_created','staged','prepared','uploaded','head_published','publication_transaction','published'):
            f=CatalogFixture(base,'catalog-crash-'+phase,relay.endpoint); setup_combined(f)
            f.run(f.catalog(),crash=phase); assert inspect(f)['schema_algorithm']=='catalog-v1'
            f.run(f.additive(),error='different schema_mode')
            if phase in ('prepared','uploaded','head_published','publication_transaction','published'):
                f.run("CALL differential_expire_snapshots('merge_db',versions=>[1,2,3]);")
                set_policy_version(f,'catalog-future-v99')
            f.run('CALL differential_cleanup_old_files(cleanup_all=>true);')
            first=f.run(f.catalog())[-1][0]; replay=f.run(f.catalog())[-1][0]
            assert first['snapshot_id']==replay['snapshot_id'] and first['operation_id']==replay['operation_id']
            check(f); ok(phase+' crash/retry publishes exactly one complete native catalog')

        f=CatalogFixture(base,'catalog-abort',relay.endpoint); setup_combined(f)
        f.run(f.catalog(),crash='prepared'); f.run(abort_sql(inspect(f)['operation_id'])); no_retention(f)
        f.remote(error='not_found'); f.run(f.catalog(request='replacement')); check(f)
        ok('safe abort releases private result/name, and a fresh request can publish')

        f=CatalogFixture(base,'catalog-expired',relay.endpoint); setup_combined(f); f.evict()
        f.run(f.catalog(),crash='views_created')
        f.run("CALL differential_expire_snapshots('merge_db',versions=>[1,2,3]); CALL differential_cleanup_old_files(cleanup_all=>true);")
        f.run(f.catalog(),error='retired'); f.hidden(); no_stage(f)
        f.run(abort_sql(inspect(f)['operation_id'])); no_retention(f)
        ok('before durable preparation, expired inputs are not replaced on retry')

        for label,left,right,extra in (
            ('drop-data','DROP TABLE o.x;','UPDATE t.x SET a=1;',''),
            ('view','USE o; CREATE VIEW v AS SELECT 1 AS n; USE memory;','USE t; CREATE VIEW v AS SELECT 2 AS n; USE memory;',''),
            ('dependency','DROP TABLE o.x;','USE t; CREATE VIEW v AS SELECT id FROM x; USE memory;',''),
            ('cycle','USE o; CREATE OR REPLACE VIEW aa AS SELECT id FROM bb; USE memory;',
             'USE t; CREATE OR REPLACE VIEW bb AS SELECT id FROM aa; USE memory;',
             'USE d; CREATE VIEW aa AS SELECT id FROM x; CREATE VIEW bb AS SELECT id FROM x; USE memory;'),
        ):
            f=CatalogFixture(base,'catalog-error-'+label,relay.endpoint); f.setup(left,right,extra=extra)
            f.run(f.catalog(),error='merge_schema_conflict'); f.hidden(); f.remote(error='not_found'); no_stage(f)
            assert f.scalar("SELECT count(*) FROM operations WHERE kind='merge_publication'")==0
        ok('table/view conflicts, dangling dependencies and cycles never prepare or publish a result')

        f=CatalogFixture(base,'catalog-cancel',relay.endpoint); setup_combined(f)
        p,gate=start(f,f.catalog(),'views_created'); p.send_signal(2); p.communicate(timeout=10); release(gate)
        f.hidden(); no_stage(f); f.remote(error='not_found'); f.run(f.catalog()); check(f)
        ok('cancellation after view recreation cleans staging and permits safe retry')

        f=CatalogFixture(base,'catalog-resolution',relay.endpoint); f.setup('UPDATE o.x SET a=1 WHERE id=1;',
            'UPDATE t.x SET a=2 WHERE id=1; USE t; CREATE VIEW v AS SELECT id,a,b FROM x; USE memory;')
        p=f.run(f.preview(',validate=>true'))[-1]; assert next(r for r in p if r['object_type']=='view')['validation_state']=='blocked_by_conflicts'
        choices="CREATE TABLE choices(id INT,a INT,b VARCHAR,_merge_action VARCHAR,_merge_columns MAP(VARCHAR,VARCHAR)); INSERT INTO choices VALUES(1,NULL,NULL,'cells',map{'a':'theirs'});"
        options=",resolution_tables=>map{'main.x':'memory.main.choices'}"
        f.run(choices+f.catalog(options=options),crash='prepared')
        f.run("CALL differential_expire_snapshots('merge_db',versions=>[1,2,3]);")
        f.run(f.catalog(options=options)); assert remote(f,'SELECT a FROM r.v WHERE id=1')==[{'a':2}]
        relay.mode='head-read-denied'; f.run(f.catalog(options=options)); relay.mode=''
        ok('typed row choices and view definitions survive source removal/input expiry; completed replay needs no S3')
    finally: relay.close()
    evidence_path('s3-merge-catalog.json').write_text(json.dumps({'passed':passed,'scope':'disposable MinIO only'},indent=2)+'\n')


if __name__=='__main__': main(*sys.argv[1:])
