#!/usr/bin/env python3
"""Additive schema planning, typed comparison/application and local recovery. Disposable stores only."""
from contextlib import closing
import json
from pathlib import Path
import sqlite3
import tempfile

from s3_merge import Fixture
from merge_lifecycle import inspect, abort_sql, start, release
from evidence import evidence_path


class SchemaFixture(Fixture):
    def __init__(self, base, label, endpoint=''):
        super().__init__(base,label,endpoint)

    def setup(self, ours='', theirs='', rows=3, extra='', definition=None):
        data = f" (DATA_PATH '{self.location}', {self.options})" if self.endpoint else ''
        definition = definition if definition is not None else (
            'CREATE TABLE d.x(id INT PRIMARY KEY,a INT DEFAULT 0 CHECK(a>=0),b VARCHAR);'
            f"INSERT INTO d.x SELECT i,0,'base' FROM range(1,{rows+1}) r(i);")
        self.run(f"""
ATTACH 'differential:merge_db' AS d{data};
{definition} {extra}
CALL differential_create_snapshot('d');
CALL differential_create_branch('d','ours',snapshot=>1); CALL differential_create_branch('d','theirs',snapshot=>1);
ATTACH 'differential:merge_db' AS o (BRANCH 'ours'); {ours} CALL differential_create_snapshot('o');
ATTACH 'differential:merge_db' AS t (BRANCH 'theirs'); {theirs} CALL differential_create_snapshot('t');
""")

    def preview(self, options=''):
        return f"SELECT * FROM differential_merge_snapshots_preview('merge_db',1,2,3,schema_mode=>'additive'{options});"

    def additive(self, dest='merged', request='merge', options=''):
        return self.merge(dest,request,",schema_mode=>'additive'"+options)

    def read(self, query='SELECT * FROM m.x ORDER BY 1', branch='merged'):
        results = self.run(f"ATTACH 'differential:merge_db' AS m (BRANCH '{branch}'); {query};")
        return results[-1] if results else []

    def evict(self):
        for branch in ('main','ours','theirs'):
            self.run(f"ATTACH 'differential:merge_db' AS live (BRANCH '{branch}'); UPDATE live.x SET b='live' WHERE id=1; CALL differential_create_snapshot('live');")
        self.run("CALL differential_evict_snapshots('merge_db',versions=>[1,2,3]); CALL differential_cleanup_old_files(cleanup_all=>true);")
        assert self.scalar('SELECT count(*) FROM snapshots s JOIN reclaimed_roots r ON s.root=r.root WHERE s.id<=3')==3


def set_policy_version(f, version):
    with closing(sqlite3.connect(f.repo/'engine.sqlite')) as db:
        payload = db.execute("SELECT payload FROM operations WHERE kind='merge_schema'").fetchone()[0]
        fields = []
        while payload:
            size,rest = payload.split(':',1); n=int(size); fields.append(rest[:n]); payload=rest[n:]
        fields[3] = version
        db.execute("UPDATE operations SET payload=? WHERE kind='merge_schema'",
                   (''.join(f'{len(x.encode())}:{x}' for x in fields),)); db.commit()


OURS = "ALTER TABLE o.x ADD COLUMN zeta BOOLEAN; UPDATE o.x SET a=11,zeta=true WHERE id=1;"
THEIRS = "ALTER TABLE t.x ADD COLUMN alpha DECIMAL(10,2); UPDATE t.x SET b='edited',alpha=2.50 WHERE id=1;"
EXTRA = "CREATE INDEX ix ON d.x(a,id); USE d; CREATE VIEW visible AS SELECT id,a,b FROM x; CREATE VIEW totals AS SELECT count(*) AS n FROM visible; USE memory;"


def verify(f):
    rows=f.read()
    assert list(rows[0]) == ['id','a','b','alpha','zeta'],rows
    assert rows[0] == {'id':1,'a':11,'b':'edited','alpha':'2.50','zeta':True},rows
    assert all(r['alpha'] is None and r['zeta'] is None for r in rows[1:])
    assert f.scalar('PRAGMA integrity_check') == 'ok'


def main():
    passed=[]
    def ok(label): passed.append(label); print('PASS merge schema',label,flush=True)
    with tempfile.TemporaryDirectory(prefix='differential-merge-schema-') as tmp:
        f=SchemaFixture(tmp,'basic'); f.setup(OURS,THEIRS,extra=EXTRA)
        f.run(f.merge(),error='schema mismatch')
        before=f.scalar('SELECT count(*) FROM operations')
        p=f.run(f.preview(',validate=>true'))[-1][0]
        assert p['validation_state']=='valid' and p['schema_conflicts']==0 and p['conflicts']==0,p
        assert p['schema_changes']==[{'column_name':'alpha','source':'theirs','sql_type':'DECIMAL(10,2)','action':'add'},
                                     {'column_name':'zeta','source':'ours','sql_type':'BOOLEAN','action':'add'}],p
        assert f.scalar('SELECT count(*) FROM operations')==before
        f.hidden()
        f.run(f.additive(),error='different schema_mode')  # Strict failure already owns this request identity.
        f.run(f.additive(request='additive')); verify(f)
        policy=inspect(f,'additive'); assert policy['schema_mode']=='additive' and policy['schema_algorithm']=='additive-tables-v2'
        f.run(f.merge(request='additive'),error='different schema_mode')
        assert f.read('SELECT * FROM m.totals')==[{'n':3}]
        assert f.read("SELECT count(*) AS n FROM duckdb_indexes() WHERE database_name='m' AND index_name='ix'")==[{'n':1}]
        assert f.read("SELECT json_extract_string(commit_extra_info,'$.schema_mode') AS mode FROM differential_snapshots('m') WHERE snapshot_id=4")==[{'mode':'additive'}]
        for branch in ('main','ours','theirs'):
            names=list(f.read('SELECT * FROM m.x LIMIT 1',branch)[0])
            assert names==['id','a','b']+({'main':[],'ours':['zeta'],'theirs':['alpha']}[branch]),names
        f.read('INSERT INTO m.x(id,b) VALUES(9,\'default\')')
        assert f.read('SELECT a FROM m.x WHERE id=9')==[{'a':0}]
        f.run("ATTACH 'differential:merge_db' AS m (BRANCH 'merged'); INSERT INTO m.x VALUES(10,-1,'bad',NULL,NULL);",error='constraint')
        ok('strict opt-in, typed NULL projection, deterministic order, preview isolation, provenance and native objects')

        for rows in (0,3):
            f=SchemaFixture(tmp,f'only-schema-{rows}'); f.setup(theirs='ALTER TABLE t.x ADD COLUMN note VARCHAR;',rows=rows)
            p=f.run(f.preview(',validate=>true'))[-1][0]
            assert p['ours_updates']==p['theirs_updates']==p['conflicts']==0 and p['validation_state']=='valid'
            f.run(f.additive())
            assert f.read('SELECT count(*) AS n,count(note) AS set_values FROM m.x')==[{'n':rows,'set_values':0}]
            f.read("INSERT INTO m.x(id,note) VALUES(99,'works')")
            ok(f'schema-only incoming column applied with {rows} rows')

        f=SchemaFixture(tmp,'reorder'); f.setup(
            'ALTER TABLE o.x ADD COLUMN zz INT; ALTER TABLE o.x ADD COLUMN aa INT; UPDATE o.x SET zz=7,aa=8;',
            'ALTER TABLE t.x ADD COLUMN AA INT; UPDATE t.x SET AA=8;')
        f.run(f.additive()); row=f.read()[0]
        assert list(row)==['id','a','b','aa','zz'] and row['aa']==8 and row['zz']==7,row
        ok('independent addition order and case-folded identical names align by name')

        f=SchemaFixture(tmp,'branch-repeat'); f.setup(
            'ALTER TABLE o.x ADD COLUMN zz INT; ALTER TABLE o.x ADD COLUMN aa INT; UPDATE o.x SET zz=7,aa=8;',
            'ALTER TABLE t.x ADD COLUMN BB INT; UPDATE t.x SET BB=9;',extra=EXTRA)
        branch_preview="SELECT * FROM differential_merge_branch_preview('merge_db','ours','theirs',schema_mode=>'additive',validate=>true);"
        p=f.run(branch_preview)[-1][0]; assert p['base_snapshot_id']==1 and p['validation_state']=='valid'
        branch_merge="SELECT * FROM differential_merge_branch('merge_db','ours','theirs','merged',request_id=>'branch',schema_mode=>'additive');"
        first=f.run(branch_merge)[-1][0]; original=f.read()
        f.run(branch_merge.replace("schema_mode=>'additive'","schema_mode=>'strict'"),error='different schema_mode')
        for side,base_id,key in [('ours',2,2),('theirs',3,3)]:
            f.run(f"ATTACH 'differential:merge_db' AS live (BRANCH '{side}'); ALTER TABLE live.x ADD COLUMN next_{side} BOOLEAN; UPDATE live.x SET next_{side}=true WHERE id={key}; CALL differential_create_snapshot('live');")
            p=f.run(f"SELECT * FROM differential_merge_branch_preview('merge_db','merged','{side}',schema_mode=>'additive',validate=>true);")[-1][0]
            assert p['base_snapshot_id']==base_id and p['validation_state']=='valid' and p['conflicts']==0,p
            f.run(f"SELECT * FROM differential_merge_branch('merge_db','merged','{side}','again_{side}',schema_mode=>'additive',request_id=>'again-{side}');")
            rows=f.read(branch='again_'+side)
            assert [{k:v for k,v in r.items() if k!='next_'+side} for r in rows]==original,rows
            assert [r['next_'+side] for r in rows]==[True if i==key else None for i in range(1,4)]
        assert f.run(branch_merge)[-1][0]['snapshot_id']==first['snapshot_id']
        ok('branch APIs and repeated merges from both parents align previously reordered columns and freeze requests')

        f=SchemaFixture(tmp,'reordered-constraints'); f.setup(
            "DROP TABLE o.x; CREATE TABLE o.x(note VARCHAR,b VARCHAR,a INT NOT NULL UNIQUE,id INT PRIMARY KEY); INSERT INTO o.x VALUES('new','base',1,1);",
            "ALTER TABLE t.x ADD COLUMN flag BOOLEAN; UPDATE t.x SET flag=true;",
            definition="CREATE TABLE d.x(id INT PRIMARY KEY,a INT NOT NULL UNIQUE,b VARCHAR); INSERT INTO d.x VALUES(1,1,'base');")
        f.run(f.additive()); assert f.read()==[{'id':1,'a':1,'b':'base','flag':True,'note':'new'}]
        f.run("ATTACH 'differential:merge_db' AS m (BRANCH 'merged'); INSERT INTO m.x VALUES(2,1,'bad',NULL,NULL);",error='constraint')
        ok('name alignment remaps positional primary-key, unique and NOT NULL constraints')

        f=SchemaFixture(tmp,'quoted'); f.setup(
            'ALTER TABLE o.x ADD COLUMN "rowid" VARCHAR; UPDATE o.x SET "rowid"=\'ordinary value\';',
            'ALTER TABLE t.x ADD COLUMN "a\"\"b" INTEGER; UPDATE t.x SET "a\"\"b"=42;')
        f.run(f.additive()); row=f.read()[0]
        assert row['rowid']=='ordinary value' and row['a"b']==42,row
        ok('quoted identifiers and a declared rowid column remain ordinary typed columns')

        f=SchemaFixture(tmp,'data-conflict'); f.setup(
            "ALTER TABLE o.x ADD COLUMN note VARCHAR; UPDATE o.x SET note=CASE id WHEN 1 THEN 'ours' WHEN 2 THEN 'same' END;",
            "ALTER TABLE t.x ADD COLUMN note VARCHAR; UPDATE t.x SET note=CASE id WHEN 1 THEN 'theirs' WHEN 2 THEN 'same' ELSE 'new' END;")
        p=f.run(f.preview())[-1][0]
        assert p['schema_conflicts']==0 and p['conflicts']==1 and p['schema_changes'][0]['source']=='both',p
        detail=p['conflict_details'][0]
        assert detail['base_present'] and detail['base_values'][-1]['is_null'] and detail['columns']==['note'],detail
        f.run(f.additive(),error='merge_conflict')
        choices="CREATE TABLE fixes(id INT,a INT,b VARCHAR,note VARCHAR,_merge_action VARCHAR,_merge_columns MAP(VARCHAR,VARCHAR)); INSERT INTO fixes VALUES(1,NULL,NULL,NULL,'cells',map{'note':'theirs'});"
        f.run(choices+f.additive(request='resolved',options=",resolution_tables=>map{'main.x':'memory.main.fixes'}"))
        assert [r['note'] for r in f.read()]==['theirs','same','new']
        assert f.run("SELECT count(*) AS n FROM differential_merge_resolutions('merge_db','resolved');")[-1]==[{'n':1}]
        ok('identical column definitions use existing cell conflicts and typed resolution schema')

        f=SchemaFixture(tmp,'existence'); f.setup(
            "ALTER TABLE o.x ADD COLUMN note VARCHAR; DELETE FROM o.x WHERE id IN (1,2); INSERT INTO o.x VALUES(4,0,'base','ours');",
            "ALTER TABLE t.x ADD COLUMN note VARCHAR; UPDATE t.x SET note='updated' WHERE id=2; INSERT INTO t.x VALUES(4,0,'base',NULL);")
        p=f.run(f.preview())[-1][0]
        assert sorted(d['kind'] for d in p['conflict_details'])==['delete_update','insert_insert'],p
        assert p['ours_deletes']==2 and p['conflicts']==2
        f.run(f.additive(),error='merge_conflict'); f.hidden()
        ok('column absence normalization preserves row absence, delete/update and same-key insert rules')

        f=SchemaFixture(tmp,'composite'); f.setup(
            "ALTER TABLE o.x ADD COLUMN flag BOOLEAN; UPDATE o.x SET flag=true; UPDATE o.x SET k2='moved' WHERE k1=2;",
            "ALTER TABLE t.x ADD COLUMN day DATE; ALTER TABLE t.x ADD COLUMN instant TIMESTAMP; UPDATE t.x SET day=DATE '2026-01-02',instant=TIMESTAMP '2026-01-02 03:04:05' WHERE k1=1;",
            definition="CREATE TABLE d.x(k1 INT,k2 VARCHAR,v BIGINT,PRIMARY KEY(k1,k2)); INSERT INTO d.x VALUES(1,'a',NULL),(2,'b',42);")
        f.run(f.additive()); rows=f.read()
        assert rows[0]['flag'] and rows[0]['v'] is None and rows[0]['day']=='2026-01-02' and rows[0]['instant'].startswith('2026-01-02 03:04:05')
        assert rows[1]['k2']=='moved' and rows[1]['day'] is None
        ok('composite keys, primary-key moves, nullable values and DATE/TIMESTAMP additions')

        f=SchemaFixture(tmp,'constraint-order'); f.setup(
            'ALTER TABLE o.x ADD COLUMN note VARCHAR; DELETE FROM o.x; INSERT INTO o.x VALUES(1,2,\'ours\'),(2,1,NULL);',
            'ALTER TABLE t.x ADD COLUMN flag BOOLEAN; UPDATE t.x SET flag=true WHERE id=2;',
            definition='CREATE TABLE d.x(id INT PRIMARY KEY,u INT UNIQUE); INSERT INTO d.x VALUES(1,1),(2,2);')
        p=f.run(f.preview(',validate=>true'))[-1][0]; assert p['validation_state']=='valid'
        f.run(f.additive()); assert f.read()==[{'id':1,'u':2,'flag':None,'note':'ours'},{'id':2,'u':1,'flag':True,'note':None}]
        f=SchemaFixture(tmp,'constraint-combined'); f.setup(
            'ALTER TABLE o.x ADD COLUMN note VARCHAR; UPDATE o.x SET u=3 WHERE id=1;',
            'ALTER TABLE t.x ADD COLUMN flag BOOLEAN; UPDATE t.x SET u=3 WHERE id=2;',
            definition='CREATE TABLE d.x(id INT PRIMARY KEY,u INT UNIQUE); INSERT INTO d.x VALUES(1,1),(2,2);')
        p=f.run(f.preview(',validate=>true'))[-1][0]; assert p['conflicts']==p['schema_conflicts']==0 and p['validation_state']=='constraint_violation',p
        f.run(f.additive(),error='merge_constraint'); f.hidden()
        ok('rebuild accepts unique-value swaps and distinguishes final constraint violations from conflicts')

        f=SchemaFixture(tmp,'schema-conflict'); f.setup('ALTER TABLE o.x ADD COLUMN note INT;',
            'ALTER TABLE t.x ADD COLUMN note VARCHAR;',extra='CREATE TABLE d.y(id INT PRIMARY KEY);')
        before=f.scalar('SELECT count(*) FROM operations'); p=f.run(f.preview(',validate=>true'))[-1]
        assert p[0]['schema_conflicts']==1 and p[0]['conflicts'] is None and p[0]['validation_state']=='schema_conflicts',p
        assert p[0]['schema_conflict_details'][0]['base_definition'] is None
        assert p[1]['validation_state']=='blocked_by_schema_conflicts' and p[1]['ours_inserts'] is None
        assert f.scalar('SELECT count(*) FROM operations')==before
        f.run(f.additive(),error='merge_schema_conflict'); f.hidden()
        f.run(abort_sql(inspect(f)['operation_id'])); assert inspect(f)['state']=='aborted'
        ok('structured schema conflicts are distinct from uncomputed row conflicts and block publication')

        rejects=[
            ('default','ALTER TABLE t.x ADD COLUMN note INT DEFAULT 7;','defaults'),
            ('drop','ALTER TABLE t.x DROP COLUMN b;','column deletion'),
            ('rename','ALTER TABLE t.x RENAME COLUMN b TO renamed;','base column'),
            ('type','ALTER TABLE t.x ALTER COLUMN b TYPE BIGINT USING 0;','base column'),
            ('bit','ALTER TABLE t.x ADD COLUMN note BIT;','type BIT'),
            ('not-null',"DROP TABLE t.x; CREATE TABLE t.x(id INT PRIMARY KEY,a INT DEFAULT 0 CHECK(a>=0),b VARCHAR,note INT NOT NULL);",'nullable non-key'),
            ('unique',"DROP TABLE t.x; CREATE TABLE t.x(id INT PRIMARY KEY,a INT DEFAULT 0 CHECK(a>=0),b VARCHAR,note INT UNIQUE);",'nullable non-key'),
        ]
        for label,sql,error in rejects:
            f=SchemaFixture(tmp,'reject-'+label); f.setup(theirs=sql)
            f.run(f.additive(),error=error); f.hidden()
        ok('defaults, drops, renames, type changes, unsupported types and added constraints rejected')

        f=SchemaFixture(tmp,'star-view'); f.setup(theirs='ALTER TABLE t.x ADD COLUMN note VARCHAR;',
            extra='USE d; CREATE VIEW all_columns AS SELECT * FROM x; USE memory;')
        f.run(f.additive(),error='view definitions/output'); f.hidden()
        f=SchemaFixture(tmp,'unaffected-view'); f.setup(theirs='ALTER TABLE t.x ADD COLUMN note VARCHAR;',
            extra='CREATE TABLE d.y(id INT PRIMARY KEY); USE d; CREATE VIEW other_columns AS SELECT * FROM y; USE memory;')
        f.run(f.additive()); assert f.read('SELECT count(*) AS n FROM m.other_columns')==[{'n':0}]
        ok('dependent wildcard views rejected explicitly; unaffected wildcard views preserved')

        f=SchemaFixture(tmp,'union-limit'); f.setup(
            ''.join(f'ALTER TABLE o.x ADD COLUMN o{i} INT;' for i in range(128)),
            ''.join(f'ALTER TABLE t.x ADD COLUMN t{i} INT;' for i in range(128)),rows=0)
        f.run(f.additive(),error='Combined schema exceeds'); f.hidden()
        ok('combined schema limits enforced even when each input individually fits')

        for phase in ('schema_planned','staged','publication_transaction','published'):
            f=SchemaFixture(tmp,'crash-'+phase); f.setup(OURS,THEIRS,extra=EXTRA)
            f.run(f.additive(),crash=phase)
            assert inspect(f)['schema_mode']=='additive'
            f.run(f.merge(),error='different schema_mode')
            f.run('CALL differential_cleanup_old_files(cleanup_all=>true);')
            one=f.run(f.additive())[-1][0]; two=f.run(f.additive())[-1][0]
            assert one['snapshot_id']==two['snapshot_id'] and one['operation_id']==two['operation_id']
            verify(f); ok('crash and policy-preserving retry at '+phase)

        for phase in ('schema_planned','staged'):
            f=SchemaFixture(tmp,'cancel-'+phase); f.setup(OURS,THEIRS)
            process,gate=start(f,f.additive(),phase)
            process.send_signal(2); process.communicate(timeout=10); release(gate)
            assert inspect(f)['state']=='pending' and inspect(f)['schema_mode']=='additive'
            f.hidden()
            assert f.scalar("SELECT count(*) FROM files WHERE name LIKE 'merge_stage/%'")==0
            f.run(f.additive()); verify(f)
        ok('cancellation during schema planning and after staged checkpoint cleans up and safely retries')

        f=SchemaFixture(tmp,'missing-policy'); f.setup(); f.run(f.merge())
        with closing(sqlite3.connect(f.repo/'engine.sqlite')) as db:
            db.execute("DELETE FROM operations WHERE kind='merge_schema'"); db.commit()
        f.run(f.merge(),error='corrupt_data'); f.run(f.additive(),error='corrupt_data')
        f=SchemaFixture(tmp,'future-complete'); f.setup(OURS,THEIRS); f.run(f.additive()); set_policy_version(f,'future-v99')
        f.run(f.additive()); f.run(f.merge(),error='different schema_mode')
        f=SchemaFixture(tmp,'future-pending'); f.setup(OURS,THEIRS); f.run(f.additive(),crash='schema_planned'); set_policy_version(f,'future-v99')
        f.run(f.additive(),error='schema merge algorithm is unavailable'); f.hidden()
        ok('a missing policy record is corrupt; completed replay and pending requests never reinterpret schema algorithms')
    evidence_path('merge-schema.json').write_text(json.dumps({'passed':passed,'scope':'disposable local stores'},indent=2)+'\n')


if __name__=='__main__': main()
