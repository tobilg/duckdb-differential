#!/usr/bin/env python3
"""Opt-in table removal and dependency-aware whole-view merge on disposable local stores."""
import itertools
import json
import tempfile

from evidence import evidence_path
from merge_schema import SchemaFixture, set_policy_version
from merge_tables import no_stage
from merge_lifecycle import inspect, abort_sql, start, release


class CatalogFixture(SchemaFixture):
    def catalog(self, dest='merged', request='merge', options=''):
        return self.merge(dest,request,",schema_mode=>'catalog'"+options)

    def preview(self, options=''):
        return "SELECT * FROM differential_merge_snapshots_preview('merge_db',1,2,3,schema_mode=>'catalog'"+options+");"


def object_preview(f, name, kind='table', options=',validate=>true'):
    return next(r for r in f.run(f.preview(options))[-1] if r['object_name']==name and r['object_type']==kind)


def conflict(f, name, expected, kind='table'):
    p=object_preview(f,name,kind)
    assert p['validation_state']=='schema_conflicts' and p['conflicts'] is None,p
    assert any(c['kind']==expected for c in p['schema_conflict_details']),p
    f.run(f.catalog(),error='merge_schema_conflict'); f.hidden(); no_stage(f)
    return p


BASE_EXTRA="""
CREATE TABLE d.obsolete(id INT PRIMARY KEY,amount DECIMAL(18,3),flag BOOLEAN,label VARCHAR);
INSERT INTO d.obsolete VALUES(1,1.250,true,NULL),(2,NULL,NULL,'base');
CREATE INDEX obsolete_idx ON d.obsolete(amount,id);
USE d;
CREATE VIEW z_leaf AS SELECT id,amount FROM obsolete;
CREATE VIEW a_top AS SELECT count(*) AS n FROM z_leaf;
CREATE VIEW visible AS SELECT id,a,b FROM x;
USE memory;
"""
DROP_OURS="DROP VIEW o.a_top; DROP VIEW o.z_leaf; DROP TABLE o.obsolete; UPDATE o.x SET a=7 WHERE id=1;"
EDIT_THEIRS="USE t; CREATE OR REPLACE VIEW visible AS SELECT id,a,b FROM x WHERE a>=0; CREATE VIEW total AS SELECT count(*) AS n FROM visible; USE memory; UPDATE t.x SET b='edited' WHERE id=1;"


def setup_combined(f):
    f.setup(DROP_OURS,EDIT_THEIRS,extra=BASE_EXTRA)


def verify_combined(f, branch='merged'):
    assert f.read("SELECT table_name FROM duckdb_tables() WHERE database_name='m' ORDER BY 1",branch)==[{'table_name':'x'}]
    assert f.read('SELECT * FROM m.total',branch)==[{'n':3}]
    assert f.read('SELECT * FROM m.visible WHERE id=1',branch)==[{'id':1,'a':7,'b':'edited'}]
    assert f.read("SELECT view_name FROM duckdb_views() WHERE database_name='m' ORDER BY 1",branch)==[{'view_name':'total'},{'view_name':'visible'}]
    no_stage(f)


def main():
    passed=[]
    def ok(label): passed.append(label); print('PASS merge catalog',label,flush=True)
    with tempfile.TemporaryDirectory(prefix='differential-merge-catalog-') as tmp:
        for side in ('ours','theirs','both'):
            for empty in (False,True):
                f=CatalogFixture(tmp,f'drop-{side}-{empty}')
                f.setup('DROP TABLE o.x;' if side in ('ours','both') else '',
                        'DROP TABLE t.x;' if side in ('theirs','both') else '',rows=0 if empty else 3,
                        extra='CREATE INDEX x_idx ON d.x(a);')
                before=f.scalar('SELECT count(*) FROM operations')
                p=object_preview(f,'x'); assert p['table_action']=='drop' and p['table_change']['source']==side,p
                assert p['ours_deletes'] is None and p['conflicts'] is None and p['validation_state']=='valid',p
                assert p['schema_mode']=='catalog' and p['schema_algorithm']=='catalog-v1'
                assert f.scalar('SELECT count(*) FROM operations')==before; f.hidden(); no_stage(f)
                for mode in ('strict','additive'):
                    f.run(f.preview().replace("'catalog'",repr(mode)),error='table additions/deletions')
                f.run(f.catalog())
                assert f.read("SELECT count(*) AS n FROM duckdb_tables() WHERE database_name='m'")==[{'n':0}]
                assert f.read("SELECT count(*) AS n FROM duckdb_indexes() WHERE database_name='m'")==[{'n':0}]
                assert f.read('SELECT count(*) AS n FROM m.x','main')==[{'n':0 if empty else 3}]
                assert inspect(f)['schema_algorithm']=='catalog-v1'; no_stage(f)
        ok('explicit opt-in; either/both removals, empty tables and last-table removal preserve inputs and native index cleanup')

        typed="CREATE TABLE d.x(k INT,label VARCHAR,amount DECIMAL(18,3),flag BOOLEAN,PRIMARY KEY(k,label)); INSERT INTO d.x VALUES(1,'a',1.250,true),(2,'b',NULL,NULL);"
        for i,change in enumerate(("UPDATE t.x SET amount=1.251 WHERE k=1;","UPDATE t.x SET flag=true WHERE k=2;",
                                   "DELETE FROM t.x WHERE k=1;","INSERT INTO t.x VALUES(3,'c',NULL,false);",
                                   "DELETE FROM t.x WHERE k=1; INSERT INTO t.x VALUES(9,'a',1.250,true);")):
            for reverse in (False,True):
                f=CatalogFixture(tmp,f'rows-{i}-{reverse}')
                f.setup(change.replace('t.','o.') if reverse else 'DROP TABLE o.x;',
                        'DROP TABLE t.x;' if reverse else change,definition=typed)
                p=conflict(f,'x','table_delete_modify')
                assert 'rows' in p['schema_conflict_details'][0]['reason']
        for i,change in enumerate(('ALTER TABLE t.x ADD COLUMN note VARCHAR;','ALTER TABLE t.x ALTER COLUMN a SET DEFAULT 10;','CREATE INDEX another ON t.x(a);')):
            f=CatalogFixture(tmp,'definition-'+str(i)); f.setup('DROP TABLE o.x;',change)
            p=conflict(f,'x','table_delete_modify'); assert 'definition' in p['schema_conflict_details'][0]['reason']
        f=CatalogFixture(tmp,'undo'); f.setup('DROP TABLE o.x;','UPDATE t.x SET a=1; UPDATE t.x SET a=0;')
        f.run(f.catalog()); assert f.read("SELECT count(*) AS n FROM duckdb_tables() WHERE database_name='m'")==[{'n':0}]
        ok('delete-versus-data/definition changes conflict in both directions; typed composite/NULL comparison and undone edits are state-based')

        # Independent state oracle, not implementation replay. Base absent/present;
        # each side may be absent, unchanged, or independently replace the definition.
        cases=0
        for base in (None,1):
            values=(None,1,2) if base is None else (None,1,2,3)
            for ours,theirs in itertools.product(values,repeat=2):
                f=CatalogFixture(tmp,f'oracle-{base}-{ours}-{theirs}')
                def sql(c,value):
                    if value is None: return f'DROP VIEW {c}.v;' if base is not None else ''
                    return f'USE {c}; CREATE OR REPLACE VIEW v AS SELECT {value} AS n; USE memory;'
                f.setup(sql('o',ours),sql('t',theirs),definition='',
                        extra='' if base is None else f'USE d; CREATE VIEW v AS SELECT {base} AS n; USE memory;')
                expected=ours if ours==theirs else theirs if ours==base else ours if theirs==base else 'conflict'
                if expected=='conflict': conflict(f,'v','view_delete_modify' if ours is None or theirs is None else 'view_definition','view')
                else:
                    f.run(f.catalog())
                    if expected is None: assert f.read("SELECT count(*) AS n FROM duckdb_views() WHERE database_name='m'")==[{'n':0}]
                    else: assert f.read('SELECT * FROM m.v')==[{'n':expected}]
                cases+=1
        assert cases==25
        ok('25-case whole-view state oracle: additions, identical/divergent replacements, removals and delete/modify conflicts')

        f=CatalogFixture(tmp,'combined'); setup_combined(f)
        before=f.scalar('SELECT count(*) FROM operations')
        f.run('EXPLAIN '+f.catalog()+"PREPARE prepared_catalog AS "+f.catalog()+"DEALLOCATE prepared_catalog;",raw=True)
        assert f.scalar('SELECT count(*) FROM operations')==before; f.hidden(); no_stage(f)
        preview=f.run(f.preview(',validate=>true'))[-1]; assert all(r['validation_state']=='valid' for r in preview),preview
        assert next(r for r in preview if r['object_name']=='obsolete')['table_action']=='drop'
        assert next(r for r in preview if r['object_name']=='visible')['view_change']['action']=='replace'
        assert next(r for r in preview if r['object_name']=='a_top')['view_change']['action']=='drop'
        f.run(f.catalog()); verify_combined(f)
        f.run(f.catalog(request='merge',options=''),raw=True)
        assert f.read("SELECT json_extract_string(commit_extra_info,'$.schema_algorithm') AS algorithm FROM differential_snapshots('m') WHERE snapshot_id=4")==[{'algorithm':'catalog-v1'}]
        ok('coordinated table/view removals, view replacement/addition and cell merging publish one valid catalog; planning stays inert')

        f=CatalogFixture(tmp,'tracked'); f.setup(
            'DROP VIEW o.a_top; DROP VIEW o.z_leaf; DROP TABLE o.obsolete;',EDIT_THEIRS,
            extra='SET enable_view_dependencies=true;'+BASE_EXTRA)
        f.run(f.catalog()); assert f.read('SELECT * FROM m.total')==[{'n':3}]; no_stage(f)
        f.run("ATTACH 'differential:merge_db' AS m (BRANCH 'merged'); DROP TABLE m.x;",error='depends on table')
        f=CatalogFixture(tmp,'incoming-chain'); f.setup(theirs="USE t; CREATE VIEW z_leaf AS SELECT id,a FROM x; CREATE VIEW a_top AS SELECT sum(a) AS n FROM z_leaf; USE memory;")
        f.run(f.catalog()); assert f.read('SELECT * FROM m.a_top')==[{'n':'0'}]
        f=CatalogFixture(tmp,'untracked'); f.setup(theirs='USE t; CREATE VIEW v AS SELECT id FROM x; USE memory;')
        f.run('SET enable_view_dependencies=true;'+f.catalog())
        f.read('DROP TABLE m.x;')  # No new native dependency registration is invented.
        ok('view chains are removed/recreated in dependency order with native dependency tracking enabled or disabled')

        for label,extra,left,right,name in (
            ('missing-table','','DROP TABLE o.x;','USE t; CREATE VIEW v AS SELECT id FROM x; USE memory;','v'),
            ('missing-view','USE d; CREATE VIEW leaf AS SELECT id FROM x; USE memory;','DROP VIEW o.leaf;',
             'USE t; CREATE VIEW v AS SELECT id FROM leaf; USE memory;','v'),
            ('cycle','USE d; CREATE VIEW aa AS SELECT id FROM x; CREATE VIEW bb AS SELECT id FROM x; USE memory;',
             'USE o; CREATE OR REPLACE VIEW aa AS SELECT id FROM bb; USE memory;',
             'USE t; CREATE OR REPLACE VIEW bb AS SELECT id FROM aa; USE memory;','aa'),
        ):
            f=CatalogFixture(tmp,label); f.setup(left,right,extra=extra); conflict(f,name,'view_dependency','view')
        ok('individually valid inputs whose combination creates missing dependencies or a cycle are schema conflicts')

        for label,select,error in [('binding','id+1 AS n','view_binding'),('shape','*','view_output')]:
            f=CatalogFixture(tmp,label); f.setup(
                'USE o; CREATE OR REPLACE VIEW leaf AS SELECT b AS id FROM x; USE memory;',
                f'USE t; CREATE VIEW parent AS SELECT {select} FROM leaf; USE memory;',
                extra='USE d; CREATE VIEW leaf AS SELECT id FROM x; USE memory;')
            conflict(f,'parent',error,'view')
        f=CatalogFixture(tmp,'wildcard'); f.setup('ALTER TABLE o.x ADD COLUMN extra INT;',
            'USE t; CREATE VIEW v AS SELECT * FROM x; USE memory;')
        conflict(f,'v','view_output','view')
        ok('combined binding errors and implicit wildcard/output changes fail before staging')

        f=CatalogFixture(tmp,'new-table-view'); f.setup(theirs="CREATE TABLE t.new_rows(id INT PRIMARY KEY,label VARCHAR); INSERT INTO t.new_rows VALUES(1,'new'); USE t; CREATE SECURE VIEW secure_rows AS SELECT id,label FROM new_rows; CREATE VIEW summary AS SELECT count(*) AS n FROM secure_rows; USE memory;")
        f.run(f.catalog()); assert f.read('SELECT * FROM m.summary')==[{'n':1}]
        assert 'SECURE VIEW' in f.read("SELECT sql FROM duckdb_views() WHERE database_name='m' AND view_name='secure_rows'")[0]['sql']
        f=CatalogFixture(tmp,'columns-view'); f.setup('ALTER TABLE o.x ADD COLUMN other BOOLEAN;',
            "ALTER TABLE t.x ADD COLUMN note VARCHAR; UPDATE t.x SET note='new'; USE t; CREATE VIEW v AS SELECT id,note FROM x; USE memory;")
        f.run(f.catalog()); assert all(r['note']=='new' for r in f.read('SELECT * FROM m.v'))
        ok('new tables/columns combine with new views, explicit projections and native secure-view definitions')

        for label,left,right,extra,name,kind in (
            ('table-view','CREATE TABLE o.taken(id INT PRIMARY KEY);','USE t; CREATE VIEW taken AS SELECT id FROM x; USE memory;','','taken','view'),
            ('index-view','CREATE TABLE o.new_rows(id INT PRIMARY KEY); CREATE INDEX taken ON o.new_rows(id);','USE t; CREATE VIEW taken AS SELECT id FROM x; USE memory;','','taken','view'),
            ('kind','DROP TABLE o.x; USE o; CREATE VIEW x AS SELECT 1 AS id; USE memory;','','','x','view'),
        ):
            f=CatalogFixture(tmp,'name-'+label); f.setup(left,right,extra=extra)
            conflict(f,name,'object_kind' if label=='kind' else 'object_name_collision',kind)
        f=CatalogFixture(tmp,'released-index'); f.setup('DROP TABLE o.obsolete;',
            'CREATE TABLE t.reuse(id INT PRIMARY KEY); INSERT INTO t.reuse VALUES(1);',
            extra='CREATE TABLE d.obsolete(id INT PRIMARY KEY); CREATE INDEX reuse ON d.obsolete(id);')
        f.run(f.catalog()); assert f.read('SELECT * FROM m.reuse')==[{'id':1}]
        assert f.read("SELECT count(*) AS n FROM duckdb_indexes() WHERE database_name='m' AND index_name='reuse'")==[{'n':0}]
        ok('table/view/index name collisions and cross-kind replacement are explicit catalog conflicts')

        f=CatalogFixture(tmp,'constraint'); f.setup('UPDATE o.x SET u=10 WHERE id=1;','UPDATE t.x SET u=10 WHERE id=2; USE t; CREATE VIEW v AS SELECT id,u FROM x; USE memory;',
            definition='CREATE TABLE d.x(id INT PRIMARY KEY,u INT UNIQUE); INSERT INTO d.x VALUES(1,1),(2,2);')
        p=object_preview(f,'x'); assert p['validation_state']=='constraint_violation' and p['schema_conflicts']==p['conflicts']==0,p
        f.run(f.catalog(),error='merge_constraint'); f.hidden(); no_stage(f)
        f=CatalogFixture(tmp,'swap'); f.setup('DELETE FROM o.x; INSERT INTO o.x VALUES(1,2),(2,1);','USE t; CREATE VIEW v AS SELECT id,u FROM x; USE memory;',
            definition='CREATE TABLE d.x(id INT PRIMARY KEY,u INT UNIQUE); INSERT INTO d.x VALUES(1,1),(2,2);',
            extra='SET enable_view_dependencies=true; USE d; CREATE VIEW old AS SELECT id,u FROM x; USE memory;')
        f.run(f.catalog()); assert f.read('SELECT * FROM m.v ORDER BY id')==[{'id':1,'u':2},{'id':2,'u':1}]
        ok('native rebuild handles valid UNIQUE permutations with views; invalid final constraints remain distinct from catalog conflicts')

        f=CatalogFixture(tmp,'row-limit'); f.setup(theirs='DROP TABLE t.x;')
        f.run(f.catalog(options=',max_input_rows=>1'),error='Input row limit'); f.hidden(); no_stage(f)
        f.run(f.catalog()); assert inspect(f)['schema_algorithm']=='catalog-v1'
        f.run(f.additive(),error='different schema_mode')
        f=CatalogFixture(tmp,'depth'); chain='USE t; CREATE VIEW v00 AS SELECT id FROM x;'+''.join(f'CREATE VIEW v{i:02} AS SELECT id FROM v{i-1:02};' for i in range(1,65))+'USE memory;'
        f.setup(theirs=chain); f.run(f.catalog(),error='depth exceeds 64'); f.hidden(); no_stage(f)
        f=CatalogFixture(tmp,'wide'); f.setup(theirs='USE t; CREATE VIEW v AS SELECT '+','.join(f'id AS c{i}' for i in range(257))+' FROM x; USE memory;')
        f.run(f.catalog(),error='output columns'); f.hidden()
        f=CatalogFixture(tmp,'combined-views'); f.setup(
            'USE o;'+''.join(f'CREATE VIEW o{i} AS SELECT id FROM x;' for i in range(513))+'USE memory;',
            'USE t;'+''.join(f'CREATE VIEW t{i} AS SELECT id FROM x;' for i in range(512))+'USE memory;')
        f.run(f.catalog(),error='Combined schema exceeds'); f.hidden(); no_stage(f)
        definition=','.join(f'1 AS c{i}' for i in range(256))
        f=CatalogFixture(tmp,'combined-view-columns'); f.setup(
            'USE o;'+''.join(f'CREATE VIEW o{i} AS SELECT {definition};' for i in range(9))+'USE memory;',
            'USE t;'+''.join(f'CREATE VIEW t{i} AS SELECT {definition};' for i in range(9))+'USE memory;')
        f.run(f.catalog(),error='Combined schema exceeds'); f.hidden(); no_stage(f)
        f=CatalogFixture(tmp,'invalid-input'); f.setup(theirs='DROP TABLE t.x;',extra='USE d; CREATE VIEW v AS SELECT id FROM x; USE memory;')
        f.run(f.catalog(),error='view must reference only local'); f.hidden()
        f=CatalogFixture(tmp,'deferred'); f.setup(theirs='USE t; CREATE VIEW v WITH (DEFER_BINDING) AS SELECT id FROM x; USE memory;')
        f.run(f.catalog(),error='unbound/deferred'); f.hidden()
        f=CatalogFixture(tmp,'rename-state'); f.setup(ours='ALTER TABLE o.x RENAME TO renamed;')
        f.run(f.catalog()); assert len(f.read('SELECT * FROM m.renamed'))==3
        ok('row budgets, view depth/output limits, invalid input views and frozen schema-mode identity fail explicitly')

        f=CatalogFixture(tmp,'repeat'); setup_combined(f)
        branch="SELECT * FROM differential_merge_branch('merge_db','ours','theirs','merged',schema_mode=>'catalog',request_id=>'branch');"
        f.run(branch); verify_combined(f)
        for side,base in [('ours',2),('theirs',3)]:
            f.read("UPDATE m.x SET a=9 WHERE id=2; CALL differential_create_snapshot('m');",side)
            answer=f.run(f"SELECT * FROM differential_merge_branch('merge_db','merged','{side}','next_{side}',schema_mode=>'catalog',request_id=>'next-{side}');")[-1][0]
            assert answer['base_snapshot_id']==base
            assert f.read('SELECT a FROM m.visible WHERE id=2','next_'+side)==[{'a':9}]
            assert f.read("SELECT count(*) AS n FROM duckdb_tables() WHERE database_name='m' AND table_name='obsolete'",'next_'+side)==[{'n':0}]
        f.read("UPDATE m.obsolete SET flag=false WHERE id=1; CALL differential_create_snapshot('m');",'theirs')
        f.run("SELECT * FROM differential_merge_branch('merge_db','merged','theirs','bad_repeat',schema_mode=>'catalog',request_id=>'bad-repeat');",error='merge_schema_conflict')
        ok('repeated branch merges preserve removals from both parents; subsequent edits to a removed table conflict')

        for phase in ('catalog_validated','table_drop_check','views_dropped','table_dropped','view_created','views_created','staged','publication_transaction','published'):
            f=CatalogFixture(tmp,'crash-'+phase); setup_combined(f)
            f.run(f.catalog(),crash=phase); f.run('CALL differential_cleanup_old_files(cleanup_all=>true);')
            first=f.run(f.catalog())[-1][0]; second=f.run(f.catalog())[-1][0]
            assert first['snapshot_id']==second['snapshot_id'] and first['operation_id']==second['operation_id']
            verify_combined(f)
        for phase in ('table_drop_check','views_dropped','view_created'):
            f=CatalogFixture(tmp,'cancel-'+phase); setup_combined(f)
            p,gate=start(f,f.catalog(),phase); p.send_signal(2); p.communicate(timeout=10); release(gate)
            f.hidden(); no_stage(f); assert inspect(f)['state']=='pending'
            f.run(f.catalog()); verify_combined(f)
        f=CatalogFixture(tmp,'abort'); setup_combined(f); f.run(f.catalog(),crash='views_dropped')
        f.run(abort_sql(inspect(f)['operation_id'])); f.run(f.catalog(),error='merge_aborted'); f.run(f.catalog(request='replacement')); verify_combined(f)
        ok('catalog/table/view crash points, cancellation and abort leave safe inputs and exactly one completed retry result')

        f=CatalogFixture(tmp,'unknown-policy'); setup_combined(f); f.run(f.catalog(),crash='catalog_validated'); set_policy_version(f,'catalog-v99')
        f.run(f.catalog(),error='algorithm is unavailable'); f.hidden(); no_stage(f)
        f.run(abort_sql(inspect(f)['operation_id'])); f.run(f.catalog(request='replacement')); verify_combined(f)
        ok('unknown pending planners are refused; a new request can use the current planner')
    evidence_path('merge-catalog.json').write_text(json.dumps({'passed':passed,'view_oracle_cases':cases,'scope':'disposable local stores'},indent=2)+'\n')


if __name__=='__main__': main()
