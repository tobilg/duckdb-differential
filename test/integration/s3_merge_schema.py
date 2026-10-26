#!/usr/bin/env python3
"""Additive schema merges and policy-preserving recovery on disposable MinIO."""
import json
from pathlib import Path
import sys

from evidence import evidence_path
from merge_schema import SchemaFixture, OURS, THEIRS, EXTRA, verify, set_policy_version
from merge_lifecycle import inspect, abort_sql
from s3_merge import FaultRelay
from s3_merge_lifecycle import no_retention


def check(f):
    verify(f)
    assert f.remote()[-1]==f.read()
    assert f.scalar("SELECT count(*) FROM operations WHERE kind='merge' AND state='complete'")==1
    assert f.scalar("SELECT count(*) FROM branches WHERE name='merged'")==1
    assert f.scalar("SELECT count(*) FROM operations WHERE kind='merge_publication' AND state='pending'")==0
    f.run('CALL differential_cleanup_old_files(cleanup_all=>true);')
    assert f.scalar("SELECT count(*) FROM files WHERE name LIKE 'merge_stage/%'")==0


def main(endpoint,temporary):
    base=Path(temporary)/'s3-merge-schema'; base.mkdir()
    passed=[]
    def ok(label): passed.append(label); print('PASS S3 merge schema',label,flush=True)
    relay=FaultRelay(endpoint)
    try:
        f=SchemaFixture(base,'schema-evicted',relay.endpoint); f.setup(OURS,THEIRS,extra=EXTRA); f.evict()
        before=f.scalar('SELECT count(*) FROM operations')
        p=f.run(f.preview(',validate=>true'))[-1][0]
        assert p['validation_state']=='valid' and p['schema_conflicts']==0 and len(p['schema_changes'])==2,p
        assert f.scalar('SELECT count(*) FROM operations')==before
        assert f.scalar('SELECT count(*) FROM snapshots WHERE evicted=1 AND id<=3')==3
        f.hidden(); f.remote(error='not_found')
        f.run(f.additive()); check(f)
        r=inspect(f); assert r['state']=='complete' and r['schema_algorithm']=='additive-tables-v2'
        f.run(f.merge(),error='different schema_mode')
        ok('evicted snapshots compare and publish native additive result; preview does not publish or un-evict inputs')

        f=SchemaFixture(base,'schema-empty',relay.endpoint); f.setup(theirs='ALTER TABLE t.x ADD COLUMN note VARCHAR;',rows=0)
        f.run(f.additive())
        assert f.read("SELECT column_name FROM duckdb_columns() WHERE database_name='m' AND table_name='x' ORDER BY column_index")[-1]=={'column_name':'note'}
        assert f.remote()[-1]==[]
        ok('schema-only empty table survives S3 export and fresh reader')

        f=SchemaFixture(base,'schema-conflict',relay.endpoint)
        f.setup('ALTER TABLE o.x ADD COLUMN note INT;','ALTER TABLE t.x ADD COLUMN note VARCHAR;')
        p=f.run(f.preview())[-1][0]; assert p['schema_conflicts']==1 and p['conflicts'] is None
        f.run(f.additive(),error='merge_schema_conflict'); f.hidden(); f.remote(error='not_found')
        assert f.scalar("SELECT count(*) FROM operations WHERE kind='merge_publication'")==0
        f.run(abort_sql(inspect(f)['operation_id'])); no_retention(f)
        ok('schema conflicts never prepare or publish an S3 result and remain safely abortable')

        for phase in ('schema_planned','staged','prepared','uploaded','head_published','publication_transaction','published'):
            f=SchemaFixture(base,'schema-crash-'+phase,relay.endpoint); f.setup(OURS,THEIRS,extra=EXTRA)
            f.run(f.additive(),crash=phase); r=inspect(f)
            assert r['schema_mode']=='additive' and r['schema_algorithm']=='additive-tables-v2'
            f.run(f.merge(),error='different schema_mode')
            if phase in ('prepared','uploaded','head_published','publication_transaction','published'):
                f.run("CALL differential_expire_snapshots('merge_db',versions=>[1,2,3]);")
                set_policy_version(f,'future-v99')  # No replanning of an already validated frozen result.
            f.run('CALL differential_cleanup_old_files(cleanup_all=>true);')
            first=f.run(f.additive())[-1][0]; replay=f.run(f.additive())[-1][0]
            assert first['snapshot_id']==replay['snapshot_id'] and first['operation_id']==replay['operation_id']
            check(f); ok(phase+' retry preserves schema policy and publishes exactly once')

        f=SchemaFixture(base,'schema-abort',relay.endpoint); f.setup(OURS,THEIRS); f.run(f.additive(),crash='prepared')
        r=inspect(f); assert r['state']=='prepared' and not r['publication_attempted']
        f.run(abort_sql(r['operation_id'])); no_retention(f); f.remote(error='not_found')
        f.run(f.additive(request='replacement')); check(f)
        ok('aborting prepared additive result releases owned root/name; fresh request can publish')

        f=SchemaFixture(base,'schema-resolved',relay.endpoint); f.setup(
            "ALTER TABLE o.x ADD COLUMN note VARCHAR; UPDATE o.x SET note='ours' WHERE id=1;",
            "ALTER TABLE t.x ADD COLUMN note VARCHAR; UPDATE t.x SET note='theirs' WHERE id=1;")
        choices="CREATE TABLE fixes(id INT,a INT,b VARCHAR,note VARCHAR,_merge_action VARCHAR,_merge_columns MAP(VARCHAR,VARCHAR)); INSERT INTO fixes VALUES(1,NULL,NULL,NULL,'cells',map{'note':'theirs'});"
        options=",resolution_tables=>map{'main.x':'memory.main.fixes'}"
        f.run(choices+f.additive(options=options),crash='prepared')
        f.run("CALL differential_expire_snapshots('merge_db',versions=>[1,2,3]);")
        f.run(f.additive(options=options))  # The connection-local resolution source no longer exists.
        assert f.remote()[-1][0]['note']=='theirs'
        relay.mode='head-read-denied'; assert f.run(f.additive(options=options))[-1][0]['snapshot_id']==4
        relay.mode=''
        ok('typed added-column resolutions survive source removal and input expiry; completed replay needs no S3')
    finally:
        relay.close()
    evidence_path('s3-merge-schema.json').write_text(json.dumps({'passed':passed,'scope':'disposable MinIO only; no real AWS'},indent=2)+'\n')


if __name__=='__main__': main(*sys.argv[1:])
