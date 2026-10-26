#!/usr/bin/env python3
"""Catalog column removal: typed proofs, native schema projection and recovery."""
import itertools
import json
import tempfile

from evidence import evidence_path
from merge_catalog import CatalogFixture, object_preview, conflict
from merge_lifecycle import inspect, abort_sql, start, release
from merge_tables import no_stage


def base_definition(rows=3):
    return ("CREATE TABLE d.x(id INT PRIMARY KEY,gone DECIMAL(18,3),a INT DEFAULT 0 CHECK(a>=0),b VARCHAR);"
            f"INSERT INTO d.x SELECT i,CASE WHEN i%2=1 THEN i*1.250 END,0,'base' FROM range(1,{rows+1}) r(i);")


class ColumnFixture(CatalogFixture):
    def setup(self, ours='', theirs='', rows=3, extra='', definition=None):
        super().setup(ours,theirs,rows,extra,base_definition(rows) if definition is None else definition)


def rebuild(catalog, definition, columns, indexes=''):
    # DuckDB cannot ALTER DROP a column ahead of an indexed column. Construct the
    # valid input end state through native DDL, preserving declared constraints.
    return (f'CREATE TABLE {catalog}.replacement({definition});'
            f'INSERT INTO {catalog}.replacement SELECT {columns} FROM {catalog}.x;'
            f'DROP TABLE {catalog}.x; ALTER TABLE {catalog}.replacement RENAME TO x;'+indexes)


COLUMN_VIEWS='USE d; CREATE VIEW visible AS SELECT id,gone,a,b FROM x; CREATE VIEW only_gone AS SELECT id,gone FROM x; USE memory;'
INCOMING_DROP=("DROP VIEW t.only_gone; DROP VIEW t.visible; ALTER TABLE t.x DROP COLUMN gone;"
               "UPDATE t.x SET b='edited' WHERE id=1; USE t; CREATE VIEW visible AS SELECT id,a,b FROM x;"
               "CREATE VIEW total AS SELECT count(*) AS n FROM visible; USE memory;")


def setup_columns(f, tracked=False):
    f.setup('UPDATE o.x SET a=7 WHERE id=1;',INCOMING_DROP,
            extra=('SET enable_view_dependencies=true;' if tracked else '')+COLUMN_VIEWS)


def verify_columns(f, branch='merged'):
    assert f.read('SELECT * FROM m.x ORDER BY id',branch)==[
        {'id':1,'a':7,'b':'edited'},{'id':2,'a':0,'b':'base'},{'id':3,'a':0,'b':'base'}]
    assert f.read('SELECT * FROM m.visible WHERE id=1',branch)==[{'id':1,'a':7,'b':'edited'}]
    assert f.read('SELECT * FROM m.total',branch)==[{'n':3}]
    assert f.read("SELECT count(*) AS n FROM duckdb_views() WHERE database_name='m' AND view_name='only_gone'",branch)==[{'n':0}]
    no_stage(f)


def main():
    passed=[]
    def ok(label): passed.append(label); print('PASS merge columns',label,flush=True)
    with tempfile.TemporaryDirectory(prefix='differential-merge-columns-') as tmp:
        for side in ('ours','theirs','both'):
            for rows in (0,3):
                f=ColumnFixture(tmp,f'basic-{side}-{rows}')
                f.setup('ALTER TABLE o.x DROP COLUMN gone;' if side in ('ours','both') else '',
                        'ALTER TABLE t.x DROP COLUMN gone;' if side in ('theirs','both') else '',rows=rows)
                before=f.scalar('SELECT count(*) FROM operations')
                p=object_preview(f,'x'); assert p['validation_state']=='valid' and p['conflicts']==0,p
                assert p['schema_changes']==[{'column_name':'gone','source':side,'sql_type':'DECIMAL(18,3)','action':'drop'}],p
                assert p['ours_updates']==p['theirs_updates']==0 and p['schema_algorithm']=='catalog-v1'
                assert f.scalar('SELECT count(*) FROM operations')==before; f.hidden(); no_stage(f)
                for mode in ('strict','additive'):
                    f.run(f.preview().replace("'catalog'",repr(mode)),error='schema mismatch')
                f.run(f.catalog()); assert len(f.read())==rows
                assert f.read("SELECT column_name FROM duckdb_columns() WHERE database_name='m' ORDER BY column_index")==[
                    {'column_name':n} for n in ('id','a','b')]
                assert f.read('SELECT count(*) AS n FROM m.x','main')==[{'n':rows}]
                f.read("INSERT INTO m.x(id,b) VALUES(99,'default');")
                assert f.read('SELECT a FROM m.x WHERE id=99')==[{'a':0}]
                f.run("ATTACH 'differential:merge_db' AS m (BRANCH 'merged'); INSERT INTO m.x VALUES(100,-1,'bad');",error='constraint')
        ok('either/both removals and empty tables; schema-only incoming drops rebuild, defaults/CHECK and input state remain intact')

        # Independent state oracle for an existing key: removing a column does
        # not remove a row. The retaining side can keep, change or delete it.
        cases=0
        for old,retained,deleted,reverse in itertools.product((None,5),(None,5,6),(False,True),(False,True)):
            f=ColumnFixture(tmp,f'oracle-{cases}')
            val=lambda value:'NULL' if value is None else str(value)
            drop='ALTER TABLE {c}.x DROP COLUMN gone; UPDATE {c}.x SET a=7;'
            keep='DELETE FROM {c}.x;' if deleted else 'UPDATE {c}.x SET gone='+val(retained)+';'
            f.setup((keep if reverse else drop).format(c='o'),(drop if reverse else keep).format(c='t'),
                    definition=f'CREATE TABLE d.x(id INT PRIMARY KEY,gone INT,a INT); INSERT INTO d.x VALUES(1,{val(old)},0);')
            if deleted:
                p=object_preview(f,'x'); assert p['schema_conflicts']==0 and p['conflicts']==1,p
                assert p['conflict_details'][0]['kind']=='delete_update'; f.run(f.catalog(),error='merge_conflict')
            elif retained!=old:
                p=conflict(f,'x','column_delete_modify'); assert p['schema_conflict_details'][0]['column_name']=='gone'
            else:
                f.run(f.catalog()); assert f.read()==[{'id':1,'a':7}]
            cases+=1
        assert cases==24
        ok('24-case independent column/NULL/row-existence oracle in both orientations')

        for i,(sql_type,old,new) in enumerate((('BIGINT','-9223372036854775808','9223372036854775807'),
                ('VARCHAR',"'one''s 😺'","'two'"),('BOOLEAN','true','false'),
                ('DECIMAL(38,9)','12345678901234567890123456789.123456789','12345678901234567890123456789.123456788'),
                ('DATE',"DATE '2024-02-29'","DATE '2024-03-01'"))):
            definition=f"CREATE TABLE d.x(k VARCHAR,id INT,gone {sql_type},a INT,b VARCHAR,PRIMARY KEY(id,k)); INSERT INTO d.x VALUES('a',1,{old},0,'base'),('b',1,NULL,0,'base');"
            f=ColumnFixture(tmp,f'typed-{i}'); f.setup('ALTER TABLE o.x DROP COLUMN gone;',"UPDATE t.x SET a=7 WHERE k='a';",definition=definition)
            f.run(f.catalog()); assert f.read('SELECT sum(a) AS n FROM m.x')==[{'n':'7'}]
            f=ColumnFixture(tmp,f'typed-change-{i}'); f.setup('ALTER TABLE o.x DROP COLUMN gone;',f"UPDATE t.x SET gone={new} WHERE k='a';",definition=definition)
            conflict(f,'x','column_delete_modify')
        ok('composite keys, integer extrema, Unicode strings, booleans, exact decimals and dates use typed removal proofs')

        for value in ('NULL','1.250'):
            f=ColumnFixture(tmp,'insert-'+value); f.setup('ALTER TABLE o.x DROP COLUMN gone;',f"INSERT INTO t.x VALUES(9,{value},0,'new');")
            p=conflict(f,'x','column_delete_modify'); assert '1 newly inserted rows' in p['schema_conflict_details'][0]['reason'],p
        f=ColumnFixture(tmp,'moved-key'); f.setup('ALTER TABLE o.x DROP COLUMN gone;','UPDATE t.x SET id=9 WHERE id=1;')
        conflict(f,'x','column_delete_modify')
        f=ColumnFixture(tmp,'insert-and-delete'); f.setup("ALTER TABLE o.x DROP COLUMN gone; INSERT INTO o.x VALUES(9,0,'new');",'DELETE FROM t.x WHERE id=3;')
        f.run(f.catalog()); assert [r['id'] for r in f.read()]==[1,2,9]
        f=ColumnFixture(tmp,'removing-side-key-move'); f.setup('ALTER TABLE o.x DROP COLUMN gone; UPDATE o.x SET id=9 WHERE id=1;')
        f.run(f.catalog()); assert [r['id'] for r in f.read()]==[2,3,9]
        f=ColumnFixture(tmp,'undo'); f.setup('ALTER TABLE o.x DROP COLUMN gone;','UPDATE t.x SET gone=9 WHERE id=1; UPDATE t.x SET gone=1.250 WHERE id=1;')
        f.run(f.catalog())
        ok('retaining-side insertions/PK moves conflict even with NULL; removals, other-side inserts and undone edits follow final state')

        for i,change in enumerate(('ALTER TABLE t.x ALTER COLUMN gone TYPE VARCHAR;',
                'ALTER TABLE t.x ALTER COLUMN gone SET DEFAULT 7;',
                'CREATE INDEX new_idx ON t.x(gone);')):
            f=ColumnFixture(tmp,f'definition-{i}'); f.setup('ALTER TABLE o.x DROP COLUMN gone;',change)
            p=conflict(f,'x','column_delete_modify'); assert p['schema_conflict_details'][0]['ours_definition'] is None,p
        f=ColumnFixture(tmp,'not-null-change'); f.setup('ALTER TABLE o.x DROP COLUMN gone;','ALTER TABLE t.x ALTER COLUMN gone SET NOT NULL;',rows=1)
        conflict(f,'x','column_delete_modify')
        for label,column,extra in (('default','INT DEFAULT 0',''),('notnull','INT NOT NULL',''),
                ('unique','INT UNIQUE',''),('check','INT CHECK(gone>=0)',''),('multi-check','INT CHECK(gone>=id)',''),
                ('index','INT','CREATE INDEX dep ON d.x(gone,id);')):
            f=ColumnFixture(tmp,'scope-'+label)
            replace=rebuild('{c}','id INT PRIMARY KEY,a INT,b VARCHAR','id,a,b')
            f.setup(replace.format(c='o'),replace.format(c='t'),
                    definition=f"CREATE TABLE d.x(id INT PRIMARY KEY,gone {column},a INT,b VARCHAR); INSERT INTO d.x VALUES(1,7,0,'base');",extra=extra)
            f.run(f.catalog(),error='plain nullable non-key'); f.hidden(); no_stage(f)
        f=ColumnFixture(tmp,'pk-scope'); f.setup(rebuild('o','a INT PRIMARY KEY,b VARCHAR','a,b'),
            definition="CREATE TABLE d.x(gone INT PRIMARY KEY,a INT,b VARCHAR); INSERT INTO d.x VALUES(1,2,'base');")
        f.run(f.catalog(),error='plain nullable non-key'); f.hidden()
        ok('retaining definition/dependency changes conflict; base defaults, NOT NULL, keys, CHECK/UNIQUE and index dependencies are rejected')

        original="CREATE TABLE d.x(gone INT,id INT PRIMARY KEY,u INT UNIQUE NOT NULL,a INT DEFAULT 0 CHECK(a>=0),b VARCHAR); INSERT INTO d.x VALUES(7,1,1,0,'base'),(NULL,2,2,0,'base');"
        keep='id INT PRIMARY KEY,u INT UNIQUE NOT NULL,a INT DEFAULT 0 CHECK(a>=0),b VARCHAR'
        f=ColumnFixture(tmp,'positions'); f.setup("UPDATE o.x SET b='changed' WHERE id=1;",
            rebuild('t',keep,'id,u,a,b','CREATE INDEX keep_idx ON t.x(a,id);'),definition=original,
            extra='CREATE INDEX keep_idx ON d.x(a,id); USE d; CREATE VIEW v AS SELECT id,u,a,b FROM x; USE memory;')
        f.run(f.catalog()); assert f.read('SELECT * FROM m.v WHERE id=1')==[{'id':1,'u':1,'a':0,'b':'changed'}]
        assert f.read("SELECT index_name FROM duckdb_indexes() WHERE database_name='m'")==[{'index_name':'keep_idx'}]
        for query in ("INSERT INTO m.x VALUES(3,1,0,'bad');","INSERT INTO m.x VALUES(3,NULL,0,'bad');","INSERT INTO m.x VALUES(3,3,-1,'bad');"):
            f.run("ATTACH 'differential:merge_db' AS m (BRANCH 'merged');"+query,error='constraint')
        f=ColumnFixture(tmp,'composite-positions'); f.setup('',rebuild('t','k VARCHAR,id INT,a INT,PRIMARY KEY(id,k)','k,id,a'),
            definition="CREATE TABLE d.x(gone INT,k VARCHAR,id INT,a INT,PRIMARY KEY(id,k)); INSERT INTO d.x VALUES(7,'k',1,2);")
        f.run(f.catalog()); assert f.read()==[{'k':'k','id':1,'a':2}]
        f=ColumnFixture(tmp,'keys-only'); f.setup('ALTER TABLE o.x DROP COLUMN gone;','ALTER TABLE t.x DROP COLUMN gone;',definition='CREATE TABLE d.x(id INT PRIMARY KEY,gone INT); INSERT INTO d.x VALUES(1,NULL);')
        f.run(f.catalog()); assert f.read()==[{'id':1}]
        ok('removals before indexed/key columns remap native constraints; unrelated indexes/defaults/views and key-only tables survive')

        f=ColumnFixture(tmp,'independent'); f.setup('ALTER TABLE o.x DROP COLUMN left_old; ALTER TABLE o.x ADD COLUMN note VARCHAR; UPDATE o.x SET note=\'ours\';',
            'ALTER TABLE t.x DROP COLUMN right_old; ALTER TABLE t.x ADD COLUMN flag BOOLEAN; UPDATE t.x SET flag=true;',
            definition="CREATE TABLE d.x(id INT PRIMARY KEY,left_old INT,right_old INT,a INT,b VARCHAR); INSERT INTO d.x VALUES(1,7,8,0,'base');")
        p=object_preview(f,'x'); assert [(r['column_name'],r['action']) for r in p['schema_changes']]==[('left_old','drop'),('right_old','drop'),('flag','add'),('note','add')],p
        f.run(f.catalog()); assert f.read()==[{'id':1,'a':0,'b':'base','flag':True,'note':'ours'}]
        f=ColumnFixture(tmp,'two-columns'); f.setup('ALTER TABLE o.x DROP COLUMN gone; ALTER TABLE o.x DROP COLUMN extra;',
            'UPDATE t.x SET gone=9,extra=false WHERE id=1;',extra='ALTER TABLE d.x ADD COLUMN extra BOOLEAN; UPDATE d.x SET extra=true;')
        p=conflict(f,'x','column_delete_modify'); assert p['schema_conflicts']==2,p
        f=ColumnFixture(tmp,'duplicate-input'); f.setup(theirs='ALTER TABLE t.x DROP COLUMN gone;')
        f.run("SELECT * FROM differential_merge_snapshots('merge_db',1,3,3,'merged',schema_mode=>'catalog',max_input_rows=>6);")
        assert len(f.read())==3
        ok('independent removals/additions combine, each changed removed column is reported, and duplicate inputs count once')

        for tracked in (False,True):
            f=ColumnFixture(tmp,'views-'+str(tracked)); setup_columns(f,tracked)
            before=f.scalar('SELECT count(*) FROM operations')
            f.run('EXPLAIN '+f.catalog()+'PREPARE p AS '+f.catalog()+'DEALLOCATE p;',raw=True)
            assert f.scalar('SELECT count(*) FROM operations')==before
            assert all(r['validation_state']=='valid' for r in f.run(f.preview(',validate=>true'))[-1]); f.hidden(); no_stage(f)
            f.run(f.catalog()); verify_columns(f)
            if tracked: f.run("ATTACH 'differential:merge_db' AS m (BRANCH 'merged'); DROP TABLE m.x;",error='depends on table')
        for label,select,error in [('dependency','gone','view_binding'),('wildcard','*','view_output')]:
            f=ColumnFixture(tmp,'view-'+label); f.setup('ALTER TABLE o.x DROP COLUMN gone;',f'USE t; CREATE VIEW v AS SELECT {select} FROM x; USE memory;')
            conflict(f,'v',error,'view')
        f=ColumnFixture(tmp,'invalid-view'); f.setup('ALTER TABLE o.x DROP COLUMN gone;',extra='USE d; CREATE VIEW v AS SELECT gone FROM x; USE memory;')
        f.run(f.catalog(),error='input view cannot bind'); f.hidden()
        ok('coordinated view changes/removals preserve native tracking; dangling references, wildcard changes and invalid inputs fail explicitly')

        f=ColumnFixture(tmp,'constraint'); f.setup('ALTER TABLE o.x DROP COLUMN gone; UPDATE o.x SET u=10 WHERE id=1;',
            'UPDATE t.x SET u=10 WHERE id=2;',definition='CREATE TABLE d.x(id INT PRIMARY KEY,u INT UNIQUE,gone INT); INSERT INTO d.x VALUES(1,1,7),(2,2,8);')
        p=object_preview(f,'x'); assert p['schema_conflicts']==p['conflicts']==0 and p['validation_state']=='constraint_violation',p
        f.run(f.catalog(),error='merge_constraint'); f.hidden(); no_stage(f)
        f=ColumnFixture(tmp,'swap'); f.setup('DELETE FROM o.x; INSERT INTO o.x VALUES(1,2,7),(2,1,8);','ALTER TABLE t.x DROP COLUMN gone;',
            definition='CREATE TABLE d.x(id INT PRIMARY KEY,u INT UNIQUE,gone INT); INSERT INTO d.x VALUES(1,1,7),(2,2,8);')
        f.run(f.catalog()); assert f.read()==[{'id':1,'u':2},{'id':2,'u':1}]
        choices="CREATE TABLE choices(id INT,a INT,b VARCHAR,_merge_action VARCHAR,_merge_columns MAP(VARCHAR,VARCHAR)); INSERT INTO choices VALUES(1,NULL,NULL,'cells',map{'a':'theirs'});"
        options=",resolution_tables=>map{'main.x':'memory.main.choices'}"
        f=ColumnFixture(tmp,'resolution'); f.setup('ALTER TABLE o.x DROP COLUMN gone; UPDATE o.x SET a=1 WHERE id=1;','UPDATE t.x SET a=2 WHERE id=1;')
        p=object_preview(f,'x'); assert p['conflicts']==1 and p['schema_conflicts']==0,p
        assert all(c['name']!='gone' for c in p['conflict_details'][0]['base_values'])
        f.run(choices+f.catalog(options=options)); assert f.read('SELECT a FROM m.x WHERE id=1')==[{'a':2}]
        f.run(f.catalog(options=options)); assert f.run("SELECT count(*) AS n FROM differential_merge_resolutions('merge_db','merge');")[-1]==[{'n':1}]
        f=ColumnFixture(tmp,'cannot-resolve-schema'); f.setup('ALTER TABLE o.x DROP COLUMN gone;','UPDATE t.x SET gone=9 WHERE id=1;')
        f.run(choices+f.catalog(options=options),error='merge_schema_conflict'); f.hidden()
        ok('valid UNIQUE swaps, invalid combined constraints and typed row choices use the projected schema; choices cannot override schema conflicts')

        f=ColumnFixture(tmp,'repeat'); setup_columns(f)
        first="SELECT * FROM differential_merge_branch('merge_db','ours','theirs','merged',schema_mode=>'catalog',request_id=>'branch');"
        answer=f.run(first)[-1][0]; verify_columns(f)
        for side,base in [('ours',2),('theirs',3)]:
            f.read("UPDATE m.x SET a=9 WHERE id=2; CALL differential_create_snapshot('m');",side)
            result=f.run(f"SELECT * FROM differential_merge_branch('merge_db','merged','{side}','again_{side}',schema_mode=>'catalog',request_id=>'again-{side}');")[-1][0]
            assert result['base_snapshot_id']==base
            assert f.read('SELECT a FROM m.x WHERE id=2','again_'+side)==[{'a':9}]
            assert 'gone' not in f.read(branch='again_'+side)[0]
        f.read("UPDATE m.x SET gone=9 WHERE id=1; CALL differential_create_snapshot('m');",'ours')
        f.run("SELECT * FROM differential_merge_branch('merge_db','merged','ours','bad',schema_mode=>'catalog',request_id=>'bad');",error='merge_schema_conflict')
        assert f.run(first)[-1][0]['snapshot_id']==answer['snapshot_id']
        ok('repeated branch merges from both parents retain removals; later edits to removed columns conflict and completed requests replay')

        f=ColumnFixture(tmp,'budgets'); setup_columns(f)
        f.run(f.catalog(options=',max_input_rows=>1'),error='Input row limit'); f.hidden(); no_stage(f)
        f.run(f.catalog(options=',max_stage_write_bytes=>1'),error='merge_limit'); f.hidden(); no_stage(f)
        first=f.run(f.catalog())[-1][0]; verify_columns(f)
        f.run(f.additive(),error='different schema_mode')
        f.run(f.catalog(dest='different'),error='Request ID')
        f.run(f.catalog(request='existing-destination'),error='name_conflict')
        f.run("CALL differential_expire_snapshots('merge_db',versions=>[1,2,3]);")
        assert f.run(f.catalog())[-1][0]['snapshot_id']==first['snapshot_id']
        ok('row/staging budgets, request identity, existing destinations and completed replay after expiry preserve publication boundaries')

        for phase in ('column_drop_check','column_drops_checked','table_rebuild','table_inserted','views_created','staged','publication_transaction','published'):
            f=ColumnFixture(tmp,'crash-'+phase); setup_columns(f)
            f.run(f.catalog(),crash=phase); f.run('CALL differential_cleanup_old_files(cleanup_all=>true);')
            first=f.run(f.catalog())[-1][0]; replay=f.run(f.catalog())[-1][0]
            assert first['snapshot_id']==replay['snapshot_id']; verify_columns(f)
        for phase in ('column_drop_check','column_drops_checked','table_inserted'):
            f=ColumnFixture(tmp,'cancel-'+phase); setup_columns(f)
            p,gate=start(f,f.catalog(),phase); p.send_signal(2); p.communicate(timeout=10); release(gate)
            f.hidden(); no_stage(f); f.run(f.catalog()); verify_columns(f)
        f=ColumnFixture(tmp,'abort'); setup_columns(f); f.run(f.catalog(),crash='column_drops_checked')
        f.run(abort_sql(inspect(f)['operation_id'])); f.run(f.catalog(),error='merge_aborted')
        f.run(f.catalog(request='replacement')); verify_columns(f)
        ok('proof/rebuild/publication crashes, cancellation and safe abort clean up and retry exactly once')
    evidence_path('merge-columns.json').write_text(json.dumps({'passed':passed,'state_oracle_cases':cases},indent=2)+'\n')


if __name__=='__main__': main()
