#!/usr/bin/env python3
"""Additive table publication and recovery on disposable MinIO, never real AWS."""
import json
from pathlib import Path
import sys

from evidence import evidence_path
from merge_schema import SchemaFixture, EXTRA, set_policy_version
from merge_tables import CREATE, INSERT, SIMPLE, rows, only_table_preview, no_stage
from merge_lifecycle import inspect, abort_sql
from s3_merge import FaultRelay
from s3_merge_lifecycle import no_retention


def remote(f,query='SELECT * FROM r.new_rows ORDER BY 1,2'):
    return f.run(f"ATTACH 'differential:{f.location}heads/merge_db/merged' AS r ({f.options}); {query};",
                 repo=f.folder/'new-table-reader')[-1]


def check(f):
    assert remote(f)==rows(f)
    assert f.scalar("SELECT count(*) FROM operations WHERE kind='merge' AND state='complete'")==1
    assert f.scalar("SELECT count(*) FROM branches WHERE name='merged'")==1
    assert f.scalar("SELECT count(*) FROM operations WHERE kind='merge_publication' AND state='pending'")==0
    f.run('CALL differential_cleanup_old_files(cleanup_all=>true);'); no_stage(f)


def main(endpoint,temporary):
    base=Path(temporary)/'s3-merge-tables'; base.mkdir()
    passed=[]
    def ok(label): passed.append(label); print('PASS S3 merge tables',label,flush=True)
    relay=FaultRelay(endpoint)
    try:
        f=SchemaFixture(base,'tables-evicted',relay.endpoint)
        f.setup((CREATE+INSERT).format(c='o'),CREATE.format(c='t')+"INSERT INTO t.new_rows VALUES(1,'one',2.375,true,10),(3,'three',5.000,false,30); CREATE TABLE t.incoming(id INT PRIMARY KEY);",extra=EXTRA)
        f.evict(); before=f.scalar('SELECT count(*) FROM operations')
        p=only_table_preview(f); assert p['validation_state']=='valid' and p['ours_inserts']==p['theirs_inserts']==2
        assert f.scalar('SELECT count(*) FROM operations')==before; f.hidden(); no_stage(f)
        f.remote(error='not_found'); f.run(f.additive()); check(f)
        assert len(remote(f))==3
        assert remote(f,'SELECT count(*) AS n FROM r.incoming')==[{'n':0}]
        assert remote(f,"SELECT count(*) AS n FROM duckdb_indexes() WHERE database_name='r' AND index_name='new_idx'")==[{'n':1}]
        assert remote(f,"SELECT column_default AS d FROM duckdb_columns() WHERE database_name='r' AND table_name='new_rows' AND column_name='flag'")==f.read("SELECT column_default AS d FROM duckdb_columns() WHERE database_name='m' AND table_name='new_rows' AND column_name='flag'",'ours')
        assert f.scalar('SELECT count(*) FROM snapshots WHERE evicted=1 AND id<=3')==3
        ok('evicted inputs merge new tables/rows, preserve native schema in a fresh S3 reader and keep preview isolated')

        for phase in ('schema_planned','table_inserted','staged','prepared','uploaded','head_published','publication_transaction','published'):
            f=SchemaFixture(base,'tables-crash-'+phase,relay.endpoint)
            f.setup(theirs=(CREATE+INSERT).format(c='t'),definition='')
            f.run(f.additive(),crash=phase); r=inspect(f)
            assert r['schema_algorithm']=='additive-tables-v2'
            f.run(f.merge(),error='different schema_mode')
            if phase in ('prepared','uploaded','head_published','publication_transaction','published'):
                f.run("CALL differential_expire_snapshots('merge_db',versions=>[1,2,3]);")
                set_policy_version(f,'future-v99')
            f.run('CALL differential_cleanup_old_files(cleanup_all=>true);')
            first=f.run(f.additive())[-1][0]; replay=f.run(f.additive())[-1][0]
            assert first['snapshot_id']==replay['snapshot_id'] and first['operation_id']==replay['operation_id']
            check(f); assert len(remote(f))==2
            ok(phase+' crash/retry publishes complete new-table schema exactly once')

        f=SchemaFixture(base,'tables-abort',relay.endpoint); f.setup(theirs=(CREATE+INSERT).format(c='t'))
        f.run(f.additive(),crash='prepared'); f.run(abort_sql(inspect(f)['operation_id'])); no_retention(f)
        f.remote(error='not_found'); f.run(f.additive(request='replacement')); check(f)
        ok('safe abort releases prepared new-table result and destination for a fresh request')

        f=SchemaFixture(base,'tables-expired',relay.endpoint); f.setup(theirs=(CREATE+INSERT).format(c='t')); f.evict()
        f.run(f.additive(),crash='staged')
        f.run("CALL differential_expire_snapshots('merge_db',versions=>[1,2,3]); CALL differential_cleanup_old_files(cleanup_all=>true);")
        f.run(f.additive(),error='retired'); f.hidden(); no_stage(f)
        f.run(abort_sql(inspect(f)['operation_id'])); no_retention(f)
        ok('before durable preparation, expired inputs cannot be substituted on retry')

        for label,left,right,error in [
            ('schema',SIMPLE.format(c='o'),'CREATE TABLE t.new_rows(id INT PRIMARY KEY,v INT);','merge_schema_conflict'),
            ('collision','CREATE TABLE o.a(id INT PRIMARY KEY); CREATE INDEX idx ON o.a(id);','CREATE TABLE t.b(id INT PRIMARY KEY); CREATE INDEX idx ON t.b(id);','merge_schema_conflict'),
            ('rows',SIMPLE.format(c='o')+"INSERT INTO o.new_rows VALUES(1,'ours');",SIMPLE.format(c='t')+"INSERT INTO t.new_rows VALUES(1,'theirs');",'merge_conflict'),
            ('constraint','CREATE TABLE o.new_rows(id INT PRIMARY KEY,u INT UNIQUE); INSERT INTO o.new_rows VALUES(1,1);','CREATE TABLE t.new_rows(id INT PRIMARY KEY,u INT UNIQUE); INSERT INTO t.new_rows VALUES(2,1);','merge_constraint'),
        ]:
            f=SchemaFixture(base,'tables-error-'+label,relay.endpoint); f.setup(left,right)
            f.run(f.additive(),error=error); f.hidden(); f.remote(error='not_found'); no_stage(f)
            assert f.scalar("SELECT count(*) FROM operations WHERE kind='merge_publication'")==0
        ok('schema/name/data/constraint failures are distinct and never prepare or publish an S3 result')

        f=SchemaFixture(base,'tables-resolution',relay.endpoint); f.setup(
            SIMPLE.format(c='o')+"INSERT INTO o.new_rows VALUES(1,'ours');",
            SIMPLE.format(c='t')+"INSERT INTO t.new_rows VALUES(1,'theirs');")
        choices="CREATE TABLE choices(id INT,v VARCHAR,_merge_action VARCHAR,_merge_columns MAP(VARCHAR,VARCHAR)); INSERT INTO choices VALUES(1,NULL,'theirs',map());"
        options=",resolution_tables=>map{'main.new_rows':'memory.main.choices'}"
        f.run(choices+f.additive(options=options),crash='prepared')
        f.run("CALL differential_expire_snapshots('merge_db',versions=>[1,2,3]);")
        f.run(f.additive(options=options)); assert remote(f)==[{'id':1,'v':'theirs'}]
        relay.mode='head-read-denied'; f.run(f.additive(options=options)); relay.mode=''
        ok('new-table row resolutions and completed replay survive source removal, input expiry and unavailable S3')
    finally:
        relay.close()
    evidence_path('s3-merge-tables.json').write_text(json.dumps({'passed':passed,'scope':'disposable MinIO only'},indent=2)+'\n')


if __name__=='__main__': main(*sys.argv[1:])
