#!/usr/bin/env python3
"""Additive table planning, typed empty inputs, native catalog preservation and recovery."""
import json
import tempfile

from evidence import evidence_path
from merge_schema import SchemaFixture, EXTRA, OURS, THEIRS, verify
from merge_lifecycle import inspect, start, release


def rows(f, table='new_rows', branch='merged'):
    return f.read(f'SELECT * FROM m.{table} ORDER BY 1,2',branch)


def only_table_preview(f, name='new_rows', options=',validate=>true'):
    return next(r for r in f.run(f.preview(options))[-1] if r['table_name']==name)


def no_stage(f):
    assert f.scalar("SELECT count(*) FROM files WHERE name LIKE 'merge_stage/%'")==0
    assert f.scalar('PRAGMA integrity_check')=='ok'


CREATE = 'CREATE TABLE {c}.new_rows(id INT,label VARCHAR,amount DECIMAL(18,3) DEFAULT 1.250 CHECK(amount>=0),flag BOOLEAN DEFAULT true,u BIGINT NOT NULL UNIQUE,PRIMARY KEY(id,label)); CREATE INDEX new_idx ON {c}.new_rows(amount,id);'
INSERT = "INSERT INTO {c}.new_rows VALUES(1,'one',2.375,true,10),(2,'two',NULL,NULL,20);"
SIMPLE = 'CREATE TABLE {c}.new_rows(id INT PRIMARY KEY,v VARCHAR);'


def main():
    passed=[]
    def ok(label): passed.append(label); print('PASS merge tables',label,flush=True)
    with tempfile.TemporaryDirectory(prefix='differential-merge-tables-') as tmp:
        for side,catalog in [('ours','o'),('theirs','t')]:
            for empty in (False,True):
                f=SchemaFixture(tmp,f'{side}-{empty}')
                script=(CREATE+('' if empty else INSERT)).format(c=catalog)
                f.setup(extra=EXTRA,**{side:script})
                before=f.scalar('SELECT count(*) FROM operations'); p=only_table_preview(f)
                assert p['schema_algorithm']=='additive-tables-v2' and p['validation_state']=='valid',p
                assert p['table_change']['source']==side and 'PRIMARY KEY' in p['table_change']['definition'],p
                assert p['table_change']['indexes'][0]['index_name']=='new_idx'
                assert p['schema_changes']==[] and p['schema_conflicts']==p['conflicts']==0
                assert p[side+'_inserts']==(0 if empty else 2)
                assert f.scalar('SELECT count(*) FROM operations')==before; f.hidden(); no_stage(f)
                f.run(f.merge(),error='table additions')
                f.run(f.additive(request='additive'))
                assert len(rows(f))==(0 if empty else 2)
                assert f.read('SELECT * FROM m.totals')==[{'n':3}]
                f.read("INSERT INTO m.new_rows(id,label,u) VALUES(9,'defaults',90)")
                assert f.read('SELECT amount,flag FROM m.new_rows WHERE id=9')==[{'amount':'1.250','flag':True}]
                f.run("ATTACH 'differential:merge_db' AS m (BRANCH 'merged'); INSERT INTO m.new_rows VALUES(10,'bad',-1,true,100);",error='constraint')
                f.run("ATTACH 'differential:merge_db' AS m (BRANCH 'merged'); INSERT INTO m.new_rows(id,label,u) VALUES(10,'duplicate',90);",error='constraint')
                assert f.read("SELECT count(*) AS n FROM duckdb_indexes() WHERE database_name='m' AND index_name='new_idx'")==[{'n':1}]
                for branch in ('main','ours','theirs'):
                    exists=f.read("SELECT count(*) AS n FROM duckdb_tables() WHERE database_name='m' AND table_name='new_rows'",branch)[0]['n']
                    assert exists==(branch==side)
                no_stage(f)
        ok('one-sided empty/nonempty tables preserve composite keys, types, defaults, CHECK/UNIQUE/indexes, views and inputs')

        f=SchemaFixture(tmp,'independent'); f.setup(
            SIMPLE.format(c='o')+"INSERT INTO o.new_rows VALUES(1,'ours'); ALTER TABLE o.x ADD COLUMN note VARCHAR;",
            "CREATE TABLE t.other(id INT PRIMARY KEY,flag BOOLEAN); INSERT INTO t.other VALUES(1,true); ALTER TABLE t.x ADD COLUMN amount DECIMAL(10,2);")
        f.run(f.additive()); assert rows(f)==[{'id':1,'v':'ours'}]
        assert rows(f,'other')==[{'id':1,'flag':True}]
        assert list(f.read()[0])==['id','a','b','amount','note']
        ok('independent table and column additions combine in one result')

        f=SchemaFixture(tmp,'empty-base'); f.setup(SIMPLE.format(c='o'),SIMPLE.format(c='t'),definition='')
        p=only_table_preview(f); assert p['table_change']['source']=='both' and p['ours_inserts']==p['theirs_inserts']==0
        f.run(f.additive()); assert rows(f)==[]
        ok('a completely empty base catalog and two identical empty additions merge')

        f=SchemaFixture(tmp,'same-snapshot'); f.setup(ours=SIMPLE.format(c='o')+"INSERT INTO o.new_rows VALUES(1,'same');",definition='')
        f.run("SELECT * FROM differential_merge_snapshots('merge_db',1,2,2,'merged',schema_mode=>'additive',max_input_rows=>1);")
        assert rows(f)==[{'id':1,'v':'same'}]
        f=SchemaFixture(tmp,'internal-name'); f.setup(theirs='CREATE TABLE t.merge_result(id INT PRIMARY KEY,v VARCHAR); INSERT INTO t.merge_result VALUES(1,\'ordinary table\');',definition='')
        f.run(f.additive()); assert rows(f,'merge_result')==[{'id':1,'v':'ordinary table'}]
        ok('duplicate snapshot IDs count once and internal temporary relation names cannot capture user tables')

        f=SchemaFixture(tmp,'identical'); f.setup(
            SIMPLE.format(c='o')+"INSERT INTO o.new_rows VALUES(1,'same'),(2,NULL);",
            SIMPLE.format(c='t')+"INSERT INTO t.new_rows VALUES(1,'same'),(3,'theirs');")
        p=only_table_preview(f); assert p['ours_inserts']==p['theirs_inserts']==2 and p['conflicts']==0
        f.run(f.additive()); assert rows(f)==[{'id':1,'v':'same'},{'id':2,'v':None},{'id':3,'v':'theirs'}]
        ok('identical definitions use an empty base: same inserts deduplicate and disjoint keys combine')

        f=SchemaFixture(tmp,'case'); f.setup(
            'CREATE TABLE o."New_Rows"(id INT PRIMARY KEY,v INT); CREATE INDEX "New_Idx" ON o."New_Rows"(v); INSERT INTO o."New_Rows" VALUES(1,10);',
            'CREATE TABLE t.new_rows(id INT PRIMARY KEY,v INT); CREATE INDEX new_idx ON t.new_rows(v); INSERT INTO t.new_rows VALUES(2,20);')
        p=only_table_preview(f,'New_Rows'); assert p['schema_conflicts']==0,p
        f.run(f.additive()); assert rows(f)==[{'id':1,'v':10},{'id':2,'v':20}]
        ok('table/index identifiers match case-insensitively without inventing a rename')

        f=SchemaFixture(tmp,'insert-conflict'); f.setup(
            SIMPLE.format(c='o')+"INSERT INTO o.new_rows VALUES(1,NULL),(2,'ours');",
            SIMPLE.format(c='t')+"INSERT INTO t.new_rows VALUES(1,NULL),(2,'theirs');")
        p=only_table_preview(f); assert p['conflicts']==1 and p['validation_state']=='data_conflicts',p
        d=p['conflict_details'][0]; assert d['kind']=='insert_insert' and not d['base_present'] and d['base_values']==[],d
        f.run(f.additive(),error='merge_conflict'); f.hidden(); no_stage(f)
        choices="CREATE TABLE choices(id INT,v VARCHAR,_merge_action VARCHAR,_merge_columns MAP(VARCHAR,VARCHAR)); INSERT INTO choices VALUES(2,NULL,'theirs',map());"
        f.run(choices+f.additive(request='resolved',options=",resolution_tables=>map{'main.new_rows':'memory.main.choices'}"))
        assert rows(f)==[{'id':1,'v':None},{'id':2,'v':'theirs'}]
        assert f.run("SELECT count(*) AS n FROM differential_merge_resolutions('merge_db','resolved');")[-1]==[{'n':1}]
        ok('same-key divergent inserts conflict with absent base and accept existing typed row resolutions')

        for label,left,right in [
            ('type','id INT PRIMARY KEY,v INT','id INT PRIMARY KEY,v VARCHAR'),
            ('default','id INT PRIMARY KEY,v INT DEFAULT 1','id INT PRIMARY KEY,v INT DEFAULT 2'),
            ('constraint','id INT PRIMARY KEY,v INT','id INT PRIMARY KEY,v INT UNIQUE'),
            ('order','id INT PRIMARY KEY,v INT','v INT,id INT PRIMARY KEY'),
        ]:
            f=SchemaFixture(tmp,'definition-'+label); f.setup(f'CREATE TABLE o.new_rows({left});',f'CREATE TABLE t.new_rows({right});')
            p=only_table_preview(f); d=p['schema_conflict_details'][0]
            assert p['schema_conflicts']==1 and p['conflicts'] is None and d['kind']=='table_definition',p
            assert d['column_name'] is None and d['base_definition'] is None and d['object_name']=='new_rows'
            assert left!=right and d['ours_definition']!=d['theirs_definition']
            f.run(f.additive(),error='merge_schema_conflict'); f.hidden(); no_stage(f)
        f=SchemaFixture(tmp,'definition-index'); f.setup(SIMPLE.format(c='o')+'CREATE INDEX idx ON o.new_rows(v);',SIMPLE.format(c='t'))
        assert only_table_preview(f)['schema_conflict_details'][0]['kind']=='table_definition'
        f.run(f.additive(),error='merge_schema_conflict'); f.hidden()
        ok('unequal column/order/default/constraint/index definitions produce explicit table schema conflicts')

        for label,left,right in [
            ('index','CREATE TABLE o.left_rows(id INT PRIMARY KEY,v INT); CREATE INDEX shared_name ON o.left_rows(v);',
                     'CREATE TABLE t.right_rows(id INT PRIMARY KEY,v INT); CREATE INDEX SHARED_NAME ON t.right_rows(v);'),
            ('view','CREATE TABLE o.shared_name(id INT PRIMARY KEY);','USE t; CREATE VIEW shared_name AS SELECT id FROM x; USE memory;'),
            ('table-index','CREATE TABLE o.shared_name(id INT PRIMARY KEY);','CREATE TABLE t.other(id INT PRIMARY KEY); CREATE INDEX shared_name ON t.other(id);'),
        ]:
            f=SchemaFixture(tmp,'collision-'+label); f.setup(left,right)
            preview=f.run(f.preview())[-1]; conflicts=[d for p in preview for d in p['schema_conflict_details']]
            assert len(conflicts)==1 and conflicts[0]['kind']=='object_name_collision' and conflicts[0]['object_name']=='shared_name',conflicts
            assert all(p['conflicts'] is None for p in preview)
            f.run(f.additive(),error='merge_schema_conflict'); f.hidden(); no_stage(f)
        f=SchemaFixture(tmp,'existing-index-collision'); f.setup(theirs='CREATE TABLE t.ix(id INT PRIMARY KEY);',extra=EXTRA)
        assert only_table_preview(f,'ix')['schema_conflict_details'][0]['kind']=='object_name_collision'
        f.run(f.additive(),error='merge_schema_conflict'); f.hidden()
        ok('cross-branch index/view/table and existing-object name collisions are diagnosed before staging')

        f=SchemaFixture(tmp,'unique-combined'); f.setup(
            'CREATE TABLE o.new_rows(id INT PRIMARY KEY,u INT UNIQUE); INSERT INTO o.new_rows VALUES(1,10);',
            'CREATE TABLE t.new_rows(id INT PRIMARY KEY,u INT UNIQUE); INSERT INTO t.new_rows VALUES(2,10);')
        p=only_table_preview(f); assert p['schema_conflicts']==p['conflicts']==0 and p['validation_state']=='constraint_violation',p
        f.run(f.additive(),error='merge_constraint'); f.hidden(); no_stage(f)
        ok('combined final UNIQUE violations remain constraint errors, not data/schema conflicts')

        for label,sql,error in [
            ('keyless','CREATE TABLE t.new_rows(v INT);','declared primary key'),
            ('type','CREATE TABLE t.new_rows(id INT PRIMARY KEY,v BIT);','type BIT'),
            ('fk','CREATE TABLE t.new_rows(id INT PRIMARY KEY,xid INT REFERENCES x(id));','supported CHECK'),
            ('view','CREATE TABLE t.new_rows(id INT PRIMARY KEY); USE t; CREATE VIEW v AS SELECT * FROM new_rows; USE memory;','view definitions/output'),
            ('drop','DROP TABLE t.x;','table additions/deletions'),
            ('rename','ALTER TABLE t.x RENAME TO renamed;','table additions/deletions'),
        ]:
            f=SchemaFixture(tmp,'unsupported-'+label); f.setup(theirs=sql)
            f.run(f.additive(),error=error); f.hidden()
        ok('keyless, unsupported types, foreign keys, new views, table drops/renames still fail explicitly')

        f=SchemaFixture(tmp,'limits'); f.setup(theirs=SIMPLE.format(c='t')+"INSERT INTO t.new_rows SELECT i,'v' FROM range(20) r(i);")
        f.run(f.additive(options=',max_input_rows=>10'),error='Input row limit'); f.hidden(); no_stage(f)
        f.run(f.additive()); assert len(rows(f))==20
        for kind in ('tables','columns','objects'):
            f=SchemaFixture(tmp,'union-'+kind)
            if kind=='tables':
                left=''.join(f'CREATE TABLE o.o{i}(id INT PRIMARY KEY);' for i in range(512))
                right=''.join(f'CREATE TABLE t.t{i}(id INT PRIMARY KEY);' for i in range(512))
            elif kind=='columns':
                definition='(id INT PRIMARY KEY,'+','.join(f'c{i} INT' for i in range(255))+');'
                left=''.join(f'CREATE TABLE o.o{i}'+definition for i in range(8))
                right=''.join(f'CREATE TABLE t.t{i}'+definition for i in range(8))
            else:
                left='CREATE TABLE o.ot(id INT PRIMARY KEY);'+''.join(f'CREATE INDEX o{i} ON o.ot(id);' for i in range(513))
                right='CREATE TABLE t.tt(id INT PRIMARY KEY);'+''.join(f'CREATE INDEX t{i} ON t.tt(id);' for i in range(512))
            f.setup(left,right,rows=0)
            f.run(f.additive(),error='Combined schema exceeds'); f.hidden()
        ok('row budgets count present relations and combined table/column/index limits bound the union')

        f=SchemaFixture(tmp,'branch-repeat'); f.setup(
            SIMPLE.format(c='o')+"INSERT INTO o.new_rows VALUES(1,'ours');",
            'CREATE TABLE t.other(id INT PRIMARY KEY,v INT); INSERT INTO t.other VALUES(1,10);')
        branch="SELECT * FROM differential_merge_branch('merge_db','ours','theirs','merged',schema_mode=>'additive',request_id=>'branch');"
        f.run(branch)
        for side,table,key,base in [('ours','new_rows',"'changed'",2),('theirs','other','20',3)]:
            f.read(f'UPDATE m.{table} SET v={key}; CALL differential_create_snapshot(\'m\')',side)
            sql=f"SELECT * FROM differential_merge_branch('merge_db','merged','{side}','next_{side}',schema_mode=>'additive',request_id=>'next-{side}');"
            answer=f.run(sql)[-1][0]; assert answer['base_snapshot_id']==base
            assert rows(f,table,'next_'+side)[0]['v']==('changed' if side=='ours' else 20)
            assert len(rows(f,'new_rows','next_'+side))==len(rows(f,'other','next_'+side))==1
        assert f.run(branch)[-1][0]['snapshot_id']==4
        ok('branch selection and repeated merges from both parents preserve independently added tables')

        for phase in ('schema_planned','table_rebuild','table_inserted','staged','publication_transaction','published'):
            f=SchemaFixture(tmp,'crash-'+phase); f.setup(theirs=(CREATE+INSERT).format(c='t'),definition='')
            f.run(f.additive(),crash=phase); f.run('CALL differential_cleanup_old_files(cleanup_all=>true);')
            first=f.run(f.additive())[-1][0]; assert f.run(f.additive())[-1][0]['snapshot_id']==first['snapshot_id']
            assert len(rows(f))==2; no_stage(f)
        for phase in ('schema_planned','table_inserted'):
            f=SchemaFixture(tmp,'cancel-'+phase); f.setup(theirs=SIMPLE.format(c='t'),definition='')
            process,gate=start(f,f.additive(),phase); process.send_signal(2); process.communicate(timeout=10); release(gate)
            f.hidden(); no_stage(f); assert inspect(f)['state']=='pending'
            f.run(f.additive()); assert rows(f)==[]
        ok('crashes during new-table creation/insertion/publication and cancellation clean up and retry exactly once')

    evidence_path('merge-tables.json').write_text(json.dumps({'passed':passed,'scope':'disposable local stores'},indent=2)+'\n')


if __name__=='__main__': main()
