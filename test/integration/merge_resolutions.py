#!/usr/bin/env python3
"""Typed explicit resolutions, preserved native objects, and inspectable ancestry.
All repositories and native source tables are disposable. No external services.
"""
import json
import os
from pathlib import Path
import subprocess
import tempfile
import sqlite3
from contextlib import closing
from logical_merge import Fixture
from merge_hardening import assert_no_stage
from paths import DUCKDB_CLI, EXTENSION
from evidence import evidence_path


def main():
    passed = []
    def ok(label):
        passed.append(label)
        print('PASS', label, flush=True)
    with tempfile.TemporaryDirectory(prefix='differential-merge-resolutions-') as tmp:
        root = Path(tmp)
        f = Fixture(root/'repo')
        try:
            # Composite keys and all supported value families, preserving independent cells.
            n = f.setup('CREATE TABLE d.x(k INT,s VARCHAR,a INT,b VARCHAR,flag BOOLEAN,amount DECIMAL(18,3),day DATE,ts TIMESTAMP,PRIMARY KEY(k,s));',
                        "INSERT INTO d.x VALUES(1,'key',0,'base',false,1.250,DATE '2020-01-01',TIMESTAMP '2020-01-01 12:00:00.123456');",
                        "UPDATE o.x SET a=1,b='ours',flag=true,amount=2.500,day='2021-01-01',ts='2021-01-01 00:00:00';",
                        "UPDATE t.x SET a=2,b='theirs',amount=3.750,day='2022-01-01',ts=NULL;")
            f.q("CREATE TABLE fixes(k INT,s VARCHAR,a INT,b VARCHAR,flag BOOLEAN,amount DECIMAL(18,3),day DATE,ts TIMESTAMP,_merge_action VARCHAR,_merge_columns MAP(VARCHAR,VARCHAR));"
                "INSERT INTO fixes VALUES(1,'key',42,NULL,NULL,9.125,DATE 'infinity',TIMESTAMP '2030-01-01 12:34:56.123456','cells',"
                "map{'a':'custom','b':'custom','amount':'custom','day':'custom','ts':'custom'});")
            typed_name = n
            opts = ",resolution_tables=>map{'main.x':'memory.main.fixes'}"
            before = f.q('SELECT count(*) FROM differential_operations()')
            assert f.q(f"SELECT conflicts FROM {f.preview(n)}") == 1
            assert f.q(f"SELECT conflict_details[1].ours_values[7].date_value=DATE '2021-01-01' FROM {f.preview(n)}") == 1
            assert f.q(f"SELECT conflict_details[1].theirs_values[8].is_null FROM {f.preview(n)}") == 1
            assert f.q(f"SELECT resolved_conflicts FROM {f.preview(n,opts+',validate=>true')}") == 1
            assert f.q(f"SELECT validation_state='valid' FROM {f.preview(n,opts+',validate=>true')}") == 1
            assert f.q('SELECT count(*) FROM differential_operations()') == before
            assert f.q(f"SELECT count(*) FROM differential_branches('{n}')") == 3
            assert_no_stage(f)
            sql = f"differential_merge_snapshots('{n}',1,2,3,'resolved',request_id=>'typed'{opts})"
            assert f.q('SELECT snapshot_id FROM '+sql) == 4
            f.attach(n,'resolved')
            assert f.q("SELECT count(*) FROM m.x WHERE k=1 AND s='key' AND a=42 AND b IS NULL AND flag=true AND amount=9.125 AND day=DATE 'infinity' AND ts=TIMESTAMP '2030-01-01 12:34:56.123456'") == 1
            f.q('DETACH m; DROP TABLE fixes;')
            assert f.q(f"SELECT base_snapshot_id FROM differential_merge_branch('{n}','resolved','t','again',request_id=>'incorporated')") == 3
            f.attach(n,'again')
            assert f.q("SELECT count(*) FROM m.x WHERE a=42 AND b IS NULL AND amount=9.125 AND day=DATE 'infinity'") == 1
            f.q('DETACH m;')
            # Completed replay never re-reads a mutable table, even after its removal.
            assert f.q('SELECT snapshot_id FROM '+sql) == 4
            assert f.q(f"SELECT values[4].is_null FROM differential_merge_resolutions('{n}','typed')") == 1
            assert f.q(f"SELECT values[8].timestamp_value=TIMESTAMP '2030-01-01 12:34:56.123456' FROM differential_merge_resolutions('{n}','typed')") == 1
            assert f.q(f"SELECT base_snapshot_id=1 AND ours_snapshot_id=2 AND theirs_snapshot_id=3 FROM differential_merge_resolutions('{n}','typed')") == 1
            f.error('SELECT * FROM '+sql.replace('memory.main.fixes','memory.main.changed'), 'different resolution_tables')
            f.error('SELECT * FROM '+sql.replace(opts,''), 'different resolution_tables')
            ok('typed composite-key custom cells, NULL/decimal/date/timestamp fidelity, independent-cell preservation, preview isolation, audited completed replay')

            n = f.setup('CREATE TABLE d.x(id INT PRIMARY KEY,a INT,b VARCHAR);',
                        "INSERT INTO d.x VALUES(1,0,'base'),(2,0,'base'),(3,0,'base'),(4,0,'base');",
                        "UPDATE o.x SET a=1 WHERE id=1; DELETE FROM o.x WHERE id IN(2,3,4); INSERT INTO o.x VALUES(5,1,'ours'),(6,1,'ours');",
                        "UPDATE t.x SET a=2 WHERE id<=4; INSERT INTO t.x VALUES(5,2,'theirs'),(6,2,'theirs');")
            f.q("CREATE TABLE fixes(id INT,a INT,b VARCHAR,_merge_action VARCHAR,_merge_columns MAP(VARCHAR,VARCHAR));"
                "INSERT INTO fixes VALUES(1,NULL,NULL,'cells',map{'a':'theirs'}),(2,NULL,NULL,'ours',NULL),"
                "(3,NULL,NULL,'theirs',NULL),(4,44,'custom','custom',NULL),(5,NULL,NULL,'delete',NULL),(6,NULL,NULL,'theirs',NULL);")
            sql = f"differential_merge_branch('{n}','o','t','resolved',request_id=>'rows'{opts})"
            assert f.q('SELECT snapshot_id FROM '+sql) == 4
            f.attach(n,'resolved')
            assert f.q("SELECT count(*) FROM ((SELECT * FROM m.x EXCEPT ALL VALUES(1,2,'base'),(3,2,'base'),(4,44,'custom'),(6,2,'theirs')) UNION ALL (SELECT * FROM (VALUES(1,2,'base'),(3,2,'base'),(4,44,'custom'),(6,2,'theirs')) EXCEPT ALL SELECT * FROM m.x))") == 0
            f.q('DETACH m; DROP TABLE fixes;')
            ok('cell theirs and row ours/theirs/delete/custom choices for insert/insert and delete/update via branch API')

            n = f.setup('CREATE TABLE d.x(id INT PRIMARY KEY,v INT);','INSERT INTO d.x VALUES(1,0),(2,0);','UPDATE o.x SET v=1;','UPDATE t.x SET v=2;')
            f.q("CREATE MACRO count_star() AS 0; CREATE MACRO strlen(v) AS 0;")
            assert f.q(f"SELECT conflicts FROM {f.preview(n)}") == 2
            f.error('SELECT * FROM '+f.merge(n,'shadowed'),'merge_conflict')
            f.q('DROP MACRO count_star; DROP MACRO strlen;')
            f.q('CREATE TABLE fixes(id INT,v INT,_merge_action VARCHAR,_merge_columns MAP(VARCHAR,VARCHAR));')
            def preview(): return f.preview(n,opts)
            cases = [
                ("(1,NULL,'cells',map{'v':'ours'}),(1,NULL,'cells',map{'v':'theirs'})", 'duplicate'),
                ("(NULL,NULL,'cells',map{'v':'ours'})", 'NULL resolution key'),
                ("(99,NULL,'cells',map{'v':'ours'})", 'not a conflicting row'),
                ("(1,NULL,'cells',map{'id':'ours'})", 'exact conflicting columns'),
                ("(1,NULL,'cells',map{'v':'delete'})", 'exact conflicting columns'),
                ("(1,NULL,'ours',NULL)", 'exact conflicting columns'),
                ("(1,NULL,'cells',NULL)", 'exact conflicting columns'),
                ("(1,NULL,'cells',map{'v':NULL})", 'exact conflicting columns'),
            ]
            for rows, reason in cases:
                f.q('DELETE FROM fixes; INSERT INTO fixes VALUES'+rows)
                f.error('SELECT * FROM '+preview(), reason)
                assert_no_stage(f)
            f.q("DELETE FROM fixes; INSERT INTO fixes VALUES(1,NULL,'cells',map{'v':'ours'});")
            assert f.q('SELECT conflicts FROM '+preview()) == 1
            assert f.q('SELECT resolved_conflicts FROM '+preview()) == 1
            f.error(f"SELECT * FROM differential_merge_snapshots('{n}',1,2,3,'partial'{opts})", 'merge_conflict')
            assert f.q(f"SELECT count(*) FROM differential_branches('{n}')") == 3
            f.q('ALTER TABLE fixes ALTER COLUMN v TYPE BIGINT;')
            f.error('SELECT * FROM '+preview(), 'exact column')
            f.q('DROP TABLE fixes;')
            f.error('SELECT * FROM '+f.preview(n,opts.replace('memory.main.fixes','fixes')), 'catalog.schema.table')
            ok('duplicate/NULL/unmatched keys, wrong types, invalid or incomplete cell choices rejected; unresolved conflicts still block publication')

            # Oversized sources are rejected before unbounded C++ materialization.
            f.q("CREATE TABLE fixes(id INT,v INT,_merge_action VARCHAR,_merge_columns MAP(VARCHAR,VARCHAR));"
                "INSERT INTO fixes SELECT i,0,'cells',map{'v':'ours'} FROM range(10001) r(i);")
            f.error('SELECT * FROM '+preview(), 'merge_limit')
            f.q("DELETE FROM fixes; INSERT INTO fixes VALUES(1,0,'cells',map{'v':repeat('x',8388609)});")
            f.error('SELECT * FROM '+preview(), 'merge_limit')
            f.q('DROP TABLE fixes;')
            # Source identity is reserved even when the source has not been readable yet.
            pending = f"differential_merge_snapshots('{n}',1,2,3,'reserved',request_id=>'reserve'{opts})"
            f.error('SELECT * FROM '+pending,'does not exist')
            f.error('SELECT * FROM '+pending.replace('memory.main.fixes','memory.main.other'),'different resolution_tables')
            f.error('SELECT * FROM '+pending.replace(opts,''),'different resolution_tables')
            f.q("ATTACH 'differential:remote_decisions' AS remote (DATA_PATH 's3://unused-test-bucket/merge-resolutions/', ENDPOINT 'http://127.0.0.1:9', ALLOW_HTTP true, PATH_STYLE true);")
            f.error('SELECT * FROM '+preview().replace('memory.main.fixes','remote.main.fixes'),'local single-writer')
            f.q('DETACH remote;')
            ok('resolution row/byte budgets, local-source scope, and request argument identity before first data capture')

            binary = f.setup('CREATE TABLE d.x(id DATE PRIMARY KEY,v VARCHAR);',
                "INSERT INTO d.x VALUES(DATE '-infinity','base');", "UPDATE o.x SET v='ours';", "UPDATE t.x SET v='theirs';")
            f.q("CREATE TABLE fixes(id DATE,v VARCHAR,_merge_action VARCHAR,_merge_columns MAP(VARCHAR,VARCHAR));"
                "INSERT INTO fixes VALUES(DATE '-infinity','quote''; '||chr(0)||' tail','cells',map{'v':'custom'});")
            assert f.q(f"SELECT snapshot_id FROM differential_merge_snapshots('{binary}',1,2,3,'resolved',request_id=>'binary'{opts})") == 4
            assert f.q(f"SELECT values[2].text_value='quote''; '||chr(0)||' tail' FROM differential_merge_resolutions('{binary}','binary')") == 1
            f.attach(binary,'resolved')
            assert f.q("SELECT count(*) FROM m.x WHERE id=DATE '-infinity' AND v='quote''; '||chr(0)||' tail'") == 1
            f.q('DETACH m; DROP TABLE fixes;')
            ok('DATE infinity primary keys and embedded NUL/quote text survive freeze, audit and application')

            # Frozen pending decisions, including restart after a native constraint error.
            n = f.setup('CREATE TABLE d.x(id INT PRIMARY KEY,v INT CHECK(v>0));',
                        'INSERT INTO d.x VALUES(1,10);','UPDATE o.x SET v=20;','UPDATE t.x SET v=30;')
            f.q("CREATE TABLE fixes(id INT,v INT,_merge_action VARCHAR,_merge_columns MAP(VARCHAR,VARCHAR));"
                "INSERT INTO fixes VALUES(1,-1,'cells',map{'v':'custom'});")
            assert f.q(f"SELECT validation_state='constraint_violation' FROM {f.preview(n,opts+',validate=>true')}") == 1
            pending_name = n
            sql = f"differential_merge_snapshots('{n}',1,2,3,'resolved',request_id=>'invalid-final'{opts})"
            f.error('SELECT * FROM '+sql, 'merge_constraint')
            f.q('UPDATE fixes SET v=25;')
            f.error('SELECT * FROM '+sql, 'merge_constraint')
            assert f.q(f"SELECT values[2].integer_value FROM differential_merge_resolutions('{n}','invalid-final')") == -1
            assert f.q('SELECT snapshot_id FROM '+sql.replace('invalid-final','valid-final')) == 4
            f.q('DROP TABLE fixes;')
            assert_no_stage(f)
            ok('native CHECK validation after resolution; pending retries reuse immutable choices; changed decisions require a new request')

            # Broader schema objects faithfully survive a complete affected-table rebuild.
            n = f.setup("CREATE TABLE d.x(id INT PRIMARY KEY,u INT UNIQUE,a INT DEFAULT (2+3),b INT DEFAULT 7 CHECK(a+b<100),day DATE DEFAULT DATE '2024-01-01',ts TIMESTAMP DEFAULT TIMESTAMP '2024-01-01 01:02:03');"
                        "CREATE INDEX ix ON d.x(a,b); CREATE UNIQUE INDEX ux ON d.x(day);"
                        "USE d; CREATE VIEW v AS SELECT id,a+b AS total FROM x WHERE a>0; CREATE VIEW vv AS SELECT count(*) AS n,sum(total) AS total FROM v; USE memory;",
                        "INSERT INTO d.x VALUES(1,10,10,10,DATE '2020-01-01',TIMESTAMP '2020-01-01'),(2,20,20,20,DATE '2020-01-02',TIMESTAMP '2020-01-02');",
                        'UPDATE o.x SET a=a+1;', 'UPDATE t.x SET b=b+2;')
            assert f.q(f"SELECT snapshot_id FROM {f.merge(n)}") == 4
            f.attach(n)
            assert f.q('SELECT total FROM m.vv') == 66
            assert f.q("SELECT count(*) FROM duckdb_indexes() WHERE database_name='m' AND index_name IN('ix','ux')") == 2
            f.q("INSERT INTO m.x(id,u,day) VALUES(3,30,DATE '2020-01-03');")
            assert f.q("SELECT count(*) FROM m.x WHERE id=3 AND a=5 AND b=7 AND ts=TIMESTAMP '2024-01-01 01:02:03'") == 1
            f.error('UPDATE m.x SET a=100 WHERE id=1;', 'CHECK')
            f.error("INSERT INTO m.x(id,u,day) VALUES(4,40,DATE '2020-01-01');", 'Duplicate')
            f.q('DETACH m;')
            ok('unchanged local views including dependent aggregate view, simple ART/unique indexes, constant-expression defaults and CHECK constraints preserved')

            n = f.setup('CREATE TABLE d.x(id INT PRIMARY KEY,a INT,b INT,CHECK(a+b<100));',
                        'INSERT INTO d.x VALUES(1,10,10);','UPDATE o.x SET a=60;','UPDATE t.x SET b=60;')
            assert f.q(f'SELECT conflicts FROM {f.preview(n)}') == 0
            assert f.q(f"SELECT validation_state='constraint_violation' FROM {f.preview(n,',validate=>true')}") == 1
            f.error('SELECT * FROM '+f.merge(n),'merge_constraint')
            n = f.setup('CREATE TABLE d.x(id INT PRIMARY KEY,u INT); CREATE UNIQUE INDEX ux ON d.x(u);',
                        'INSERT INTO d.x VALUES(1,10),(2,20);','UPDATE o.x SET u=30 WHERE id=1;','UPDATE t.x SET u=30 WHERE id=2;')
            f.error('SELECT * FROM '+f.merge(n),'merge_constraint')
            assert f.q(f"SELECT count(*) FROM differential_branches('{n}')") == 3
            n = f.setup('CREATE TABLE d.x(id INT PRIMARY KEY,u INT,a INT); CREATE UNIQUE INDEX ux ON d.x(u);',
                        'INSERT INTO d.x SELECT i,i,0 FROM range(3000) r(i);',
                        'CREATE TEMP TABLE changed AS SELECT id,(u+1)%3000 AS u,a FROM o.x; DELETE FROM o.x; INSERT INTO o.x SELECT * FROM changed; DROP TABLE changed;',
                        'UPDATE t.x SET a=1;')
            assert f.q('SELECT snapshot_id FROM '+f.merge(n)) == 4
            f.attach(n)
            assert f.q('SELECT count(*) FROM m.x WHERE u=(id+1)%3000 AND a=1') == 3000
            f.q('DETACH m;')
            ok('combined CHECK/unique-index violations distinguished from conflicts; 3000-row unique-index permutation rebuild succeeds')

            for ddl, change in [
                ('CREATE TABLE d.x(id INT PRIMARY KEY,a INT DEFAULT 1);','ALTER TABLE o.x ALTER a SET DEFAULT 2;'),
                ('CREATE TABLE d.x(id INT PRIMARY KEY,a INT); CREATE INDEX ix ON d.x(a);','DROP INDEX o.ix;'),
                ('CREATE TABLE d.x(id INT PRIMARY KEY,a INT); USE d; CREATE VIEW v AS SELECT id FROM x; USE memory;', 'USE o; CREATE OR REPLACE VIEW v AS SELECT a FROM x; USE memory;')
            ]:
                n = f.setup(ddl,'',change)
                f.error('SELECT * FROM '+f.merge(n),'schema mismatch')
            for ddl, reason in [
                ("CREATE TABLE d.x(id INT PRIMARY KEY,a INT DEFAULT random());",'expression function'),
                ("CREATE TABLE d.x(id INT PRIMARY KEY,a INT CHECK(random()>0));",'expression function'),
                ("CREATE TABLE d.x(id INT PRIMARY KEY,a INT); USE d; CREATE VIEW v AS SELECT * FROM range(3); USE memory;",'expression function'),
                ("CREATE TABLE d.x(id INT PRIMARY KEY,a INT); CREATE INDEX ix ON d.x((a+1));",'expression index'),
                ("CREATE TABLE d.x(id INT PRIMARY KEY,a BIGNUM);",'type BIGNUM'),
            ]:
                n=f.setup(ddl)
                f.error('SELECT * FROM '+f.merge(n),reason)
            ok('catalog definition mismatches and deferred volatile/external/expression-index/BIGNUM scope rejected explicitly')

            n=f.setup('CREATE TABLE d.x(id INT PRIMARY KEY,a INT,b INT);','INSERT INTO d.x VALUES(1,0,0);','UPDATE o.x SET a=1;','UPDATE t.x SET b=1;')
            before=f.q('SELECT count(*) FROM differential_operations()')
            assert f.q(f"SELECT count(*) FROM differential_merge_ancestry('{n}',2,3)") == 3
            assert f.q(f"SELECT selected_base FROM differential_merge_ancestry('{n}',2,3) WHERE candidate") == 1
            assert f.q('SELECT count(*) FROM differential_operations()') == before
            f.q('SELECT * FROM '+f.merge(n,'left'))
            f.q(f"SELECT * FROM differential_merge_snapshots('{n}',1,3,2,'right')")
            assert f.q(f"SELECT count(*) FROM differential_merge_ancestry('{n}',4,5) WHERE candidate AND selection_state='ambiguous' AND selected_base IS NULL") == 2
            assert f.q(f"SELECT parents=[2,3] FROM differential_merge_ancestry('{n}',4,5) WHERE snapshot_id=4") == 1
            f.q(f"CALL differential_expire_snapshots('{n}',versions=>[2]);")
            assert f.q(f"SELECT count(*) FROM differential_merge_ancestry('{n}',4,5) WHERE candidate AND retired AND NOT readable") == 1
            f.error(f"SELECT * FROM differential_merge_ancestry('{n}',4,5,max_ancestry_nodes=>2)",'merge_limit')
            f.error(f"SELECT * FROM differential_merge_ancestry('{n}',999,5)",'missing_snapshot')
            before=f.q('SELECT count(*) FROM differential_operations()')
            f.q("PREPARE p AS SELECT * FROM differential_merge_ancestry('absent',1,2); EXPLAIN SELECT * FROM differential_merge_ancestry('absent',1,2);")
            f.q("PREPARE r AS SELECT * FROM differential_merge_resolutions('absent','absent'); EXPLAIN SELECT * FROM differential_merge_resolutions('absent','absent');")
            f.q("PREPARE m AS SELECT * FROM differential_merge_snapshots('absent',1,2,3,'result',resolution_tables=>map{'main.x':'memory.main.absent'});")
            assert f.q('SELECT count(*) FROM differential_operations()') == before
            ok('queryable exact parents and ambiguous/expired candidates, bounded walks, missing inputs, bind/PREPARE/EXPLAIN isolation')
        finally:
            f.host.close()

        # Offline fixture: completed replay validates identity without decoding an
        # unavailable choice encoding; pending execution and audit still need it.
        with closing(sqlite3.connect(root/'repo'/'engine.sqlite')) as db, db:
            rows = db.execute("SELECT id,CAST(payload AS BLOB) FROM operations WHERE kind='merge_resolutions' AND branch_id IN (SELECT id FROM operations WHERE request IN ('typed','invalid-final'))").fetchall()
            assert len(rows) == 2
            for identity,payload in rows:
                db.execute("UPDATE operations SET payload=CAST(? AS TEXT) WHERE id=?",(payload.replace(b'typed-resolutions-duckdb-2-v1',b'typed-resolutions-duckdb-2-v9',1),identity))
        f=Fixture(root/'repo')
        try:
            assert f.q(f"SELECT snapshot_id FROM differential_merge_snapshots('{typed_name}',1,2,3,'resolved',request_id=>'typed'{opts})") == 4
            f.error(f"SELECT * FROM differential_merge_resolutions('{typed_name}','typed')",'merge_unsupported')
            f.error(f"SELECT * FROM differential_merge_snapshots('{pending_name}',1,2,3,'resolved',request_id=>'invalid-final'{opts})",'merge_unsupported')
        finally: f.host.close()
        ok('completed replay survives unavailable decision encoding; pending execution and audit reject it')

        for phase in ['resolved','resolutions_frozen','staged','publication_transaction','published']:
            repo=root/('crash-'+phase)
            native=root/(phase+'.duckdb')
            prefix=f"LOAD '{EXTENSION}'; SET differential_repository='{repo}'; SET threads=2; SET memory_limit='128MB';"
            def run(sql, crash=None):
                env=dict(os.environ)
                if crash: env['DIFFERENTIAL_MERGE_CRASH']=crash
                result=subprocess.run([DUCKDB_CLI,'-init','/dev/null','-unsigned','-batch','-bail','-json',str(native)],
                    input=prefix+sql,text=True,capture_output=True,timeout=60,env=env)
                assert result.returncode == (86 if crash else 0),(phase,result.stdout,result.stderr,result.returncode)
                return result.stdout
            run("ATTACH 'differential:crash' AS d; CREATE TABLE d.x(id INT PRIMARY KEY,v INT); INSERT INTO d.x VALUES(1,0); CALL differential_create_snapshot('d');"
                "CALL differential_create_branch('d','o',snapshot=>1); CALL differential_create_branch('d','t',snapshot=>1);"
                "ATTACH 'differential:crash' AS o (BRANCH 'o'); UPDATE o.x SET v=1; CALL differential_create_snapshot('o');"
                "ATTACH 'differential:crash' AS t (BRANCH 't'); UPDATE t.x SET v=2; CALL differential_create_snapshot('t');"
                "CREATE TABLE fixes(id INT,v INT,_merge_action VARCHAR,_merge_columns MAP(VARCHAR,VARCHAR));"
                "INSERT INTO fixes VALUES(1,42,'cells',map{'v':'custom'});")
            # Native database catalog name equals the filename stem.
            sql=f"SELECT snapshot_id FROM differential_merge_branch('crash','o','t','resolved',request_id=>'retry',resolution_tables=>map{{'main.x':'{phase}.main.fixes'}});"
            run(sql,phase)
            run('UPDATE fixes SET v=999;')
            run(sql); run(sql)
            result=run("ATTACH 'differential:crash' AS m (BRANCH 'resolved'); SELECT v FROM m.x;")
            assert json.loads(result)[0]['v']==(999 if phase=='resolved' else 42),result
            f=Fixture(repo)
            try:
                assert f.q("SELECT count(*) FROM differential_branches('crash') WHERE branch_name='resolved'")==1
                assert f.q("SELECT count(*) FROM differential_snapshots('crash')")==4
                f.q("CALL differential_cleanup_old_files(max_objects=>10000);")
                assert_no_stage(f)
            finally: f.host.close()
        ok('process death before/after choice freeze, staged checkpoint, publication transaction and committed publication; edited sources ignored and exactly one result on retry')
    evidence_path('merge-resolutions.json').write_text(json.dumps({'passed':passed},indent=2)+'\n')


if __name__=='__main__': main()
