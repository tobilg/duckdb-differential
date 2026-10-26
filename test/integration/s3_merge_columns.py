#!/usr/bin/env python3
"""Column removal and projected native catalog publication on disposable MinIO."""
import json
from pathlib import Path
import sys

from evidence import evidence_path
from merge_columns import ColumnFixture, setup_columns, verify_columns
from merge_tables import no_stage
from merge_lifecycle import inspect, abort_sql, start, release
from s3_merge import FaultRelay
from s3_merge_catalog import remote
from s3_merge_lifecycle import no_retention


def check(f):
    verify_columns(f)
    assert remote(f)==f.read('SELECT * FROM m.visible ORDER BY id')
    assert remote(f,'SELECT * FROM r.total')==[{'n':3}]
    assert remote(f,"SELECT column_name FROM duckdb_columns() WHERE database_name='r' AND table_name='x' ORDER BY column_index")==[
        {'column_name':name} for name in ('id','a','b')]
    assert f.scalar("SELECT count(*) FROM operations WHERE kind='merge' AND state='complete'")==1
    assert f.scalar("SELECT count(*) FROM branches WHERE name='merged'")==1
    assert f.scalar("SELECT count(*) FROM operations WHERE kind='merge_publication' AND state='pending'")==0
    f.run('CALL differential_cleanup_old_files(cleanup_all=>true);'); no_stage(f)


def main(endpoint,temporary):
    base=Path(temporary)/'s3-merge-columns'; base.mkdir()
    passed=[]
    def ok(label): passed.append(label); print('PASS S3 merge columns',label,flush=True)
    relay=FaultRelay(endpoint)
    try:
        f=ColumnFixture(base,'columns-evicted',relay.endpoint); setup_columns(f); f.evict()
        before=f.scalar('SELECT count(*) FROM operations')
        rows=f.run(f.preview(',validate=>true'))[-1]; assert all(r['validation_state']=='valid' for r in rows),rows
        table=next(r for r in rows if r['object_type']=='table')
        assert table['schema_changes'][0]['action']=='drop' and table['schema_algorithm']=='catalog-v1'
        assert f.scalar('SELECT count(*) FROM operations')==before; f.hidden(); no_stage(f); f.remote(error='not_found')
        f.run(f.catalog()); check(f)
        assert f.scalar('SELECT count(*) FROM snapshots WHERE evicted=1 AND id<=3')==3
        # The completed file still carries native constraints/defaults.
        f.read("INSERT INTO m.x(id,b) VALUES(9,'default');")
        assert f.read('SELECT a FROM m.x WHERE id=9')==[{'a':0}]
        f.run("ATTACH 'differential:merge_db' AS m (BRANCH 'merged'); INSERT INTO m.x VALUES(10,-1,'bad');",error='constraint')
        ok('evicted inputs, isolated preview and fresh remote reader preserve removed columns, selected views and native constraints')

        for phase in ('column_drop_check','column_drops_checked','table_rebuild','table_inserted','views_created',
                      'staged','prepared','uploaded','head_published','publication_transaction','published'):
            f=ColumnFixture(base,'columns-crash-'+phase,relay.endpoint); setup_columns(f)
            f.run(f.catalog(),crash=phase); assert inspect(f)['schema_algorithm']=='catalog-v1'
            f.run(f.additive(),error='different schema_mode')
            if phase in ('prepared','uploaded','head_published','publication_transaction','published'):
                f.run("CALL differential_expire_snapshots('merge_db',versions=>[1,2,3]);")
            else:
                f.hidden(); f.remote(error='not_found')
            f.run('CALL differential_cleanup_old_files(cleanup_all=>true);')
            first=f.run(f.catalog())[-1][0]; replay=f.run(f.catalog())[-1][0]
            assert first['snapshot_id']==replay['snapshot_id'] and first['operation_id']==replay['operation_id']
            check(f); ok(phase+' crash/retry publishes exactly one projected catalog')

        f=ColumnFixture(base,'columns-abort',relay.endpoint); setup_columns(f)
        f.run(f.catalog(),crash='prepared'); f.run(abort_sql(inspect(f)['operation_id'])); no_retention(f)
        f.remote(error='not_found'); f.run(f.catalog(request='replacement')); check(f)
        ok('prepared abort releases private output/name and a fresh request can publish')

        f=ColumnFixture(base,'columns-expired',relay.endpoint); setup_columns(f); f.evict()
        f.run(f.catalog(),crash='table_inserted')
        f.run("CALL differential_expire_snapshots('merge_db',versions=>[1,2,3]); CALL differential_cleanup_old_files(cleanup_all=>true);")
        f.run(f.catalog(),error='retired'); f.hidden(); no_stage(f)
        f.run(abort_sql(inspect(f)['operation_id'])); no_retention(f)
        ok('before durable preparation, expired inputs fail without substituting another base')

        for label,left,right in (
            ('changed-value','ALTER TABLE o.x DROP COLUMN gone;','UPDATE t.x SET gone=9 WHERE id=1;'),
            ('insert-null','ALTER TABLE o.x DROP COLUMN gone;',"INSERT INTO t.x VALUES(9,NULL,0,'new');"),
            ('definition','ALTER TABLE o.x DROP COLUMN gone;','ALTER TABLE t.x ALTER COLUMN gone SET DEFAULT 7;'),
            ('view','ALTER TABLE o.x DROP COLUMN gone;','USE t; CREATE VIEW v AS SELECT gone FROM x; USE memory;'),
        ):
            f=ColumnFixture(base,'columns-error-'+label,relay.endpoint); f.setup(left,right)
            f.run(f.catalog(),error='merge_schema_conflict'); f.hidden(); f.remote(error='not_found'); no_stage(f)
            assert f.scalar("SELECT count(*) FROM operations WHERE kind='merge_publication'")==0
        f=ColumnFixture(base,'columns-constraint',relay.endpoint)
        f.setup('ALTER TABLE o.x DROP COLUMN gone; UPDATE o.x SET u=9 WHERE id=1;','UPDATE t.x SET u=9 WHERE id=2;',
                definition='CREATE TABLE d.x(id INT PRIMARY KEY,u INT UNIQUE,gone INT); INSERT INTO d.x VALUES(1,1,7),(2,2,8);')
        f.run(f.catalog(),error='merge_constraint'); f.hidden(); f.remote(error='not_found'); no_stage(f)
        assert f.scalar("SELECT count(*) FROM operations WHERE kind='merge_publication'")==0
        ok('column/view conflicts and final constraint violations are distinct and never prepare or publish output')

        for phase in ('column_drop_check','table_inserted'):
            f=ColumnFixture(base,'columns-cancel-'+phase,relay.endpoint); setup_columns(f)
            p,gate=start(f,f.catalog(),phase); p.send_signal(2); p.communicate(timeout=10); release(gate)
            f.hidden(); no_stage(f); f.remote(error='not_found'); f.run(f.catalog()); check(f)
        ok('cancellation during proof/application cleans staging and permits safe retry')

        f=ColumnFixture(base,'columns-resolution',relay.endpoint)
        f.setup('ALTER TABLE o.x DROP COLUMN gone; UPDATE o.x SET a=1 WHERE id=1;','UPDATE t.x SET a=2 WHERE id=1;')
        choices="CREATE TABLE choices(id INT,a INT,b VARCHAR,_merge_action VARCHAR,_merge_columns MAP(VARCHAR,VARCHAR)); INSERT INTO choices VALUES(1,NULL,NULL,'cells',map{'a':'theirs'});"
        options=",resolution_tables=>map{'main.x':'memory.main.choices'}"
        f.run(choices+f.catalog(options=options),crash='prepared')
        f.run("CALL differential_expire_snapshots('merge_db',versions=>[1,2,3]);")
        f.run(f.catalog(options=options)); assert remote(f,'SELECT a FROM r.x WHERE id=1')==[{'a':2}]
        relay.mode='head-read-denied'; f.run(f.catalog(options=options)); relay.mode=''
        ok('typed row choices use the final column set; prepared and completed replay need no source tables/inputs')
    finally: relay.close()
    evidence_path('s3-merge-columns.json').write_text(json.dumps({'passed':passed,'scope':'disposable MinIO only'},indent=2)+'\n')


if __name__=='__main__': main(*sys.argv[1:])
