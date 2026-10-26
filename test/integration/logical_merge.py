#!/usr/bin/env python3
"""Local logical merge semantics, scope, constraints, cancellation and crash publication.
Every repository is disposable. Uses the unmodified stock host and loadable extension.
"""
import json
import itertools
import os
from pathlib import Path
import sqlite3
import subprocess
import tempfile
import time
import threading
from paths import DUCKDB_CLI, EXTENSION
from stock_capi import Host
from evidence import evidence_path


def literal(value):
    if value is None: return 'NULL'
    if isinstance(value, bool): return str(value).lower()
    if isinstance(value, str): return "'" + value.replace("'", "''") + "'"
    return str(value)


def values(rows):
    return ','.join('(' + ','.join(map(literal, row)) + ')' for row in rows)


class Fixture:
    def __init__(self, root):
        self.host = Host()
        self.conn = self.host.connect()
        self.q(f"LOAD '{EXTENSION}'; SET differential_repository='{root}'; SET threads=2; SET memory_limit='128MB';")
        self.number = 0

    def q(self, sql): return self.host.query(self.conn, sql)
    def error(self, sql, message):
        try: self.q(sql)
        except RuntimeError as error:
            assert message.lower() in str(error).lower(), (message, str(error))
        else: raise AssertionError(('expected error', sql, message))

    def setup(self, ddl, initial='', ours='', theirs=''):
        self.number += 1
        name = f'case{self.number}'
        self.q(f"ATTACH 'differential:{name}' AS d" + (f" (DATA_PATH '{os.environ['DIFFERENTIAL_MERGE_LOCAL_POOL']}')" if os.environ.get('DIFFERENTIAL_MERGE_LOCAL_POOL') else '') + ";" + ddl + initial)
        self.q("CALL differential_create_snapshot('d')")
        for side in ('o', 't'):
            self.q(f"CALL differential_create_branch('d','{side}',snapshot=>1); ATTACH 'differential:{name}' AS {side} (BRANCH '{side}');")
        self.q(ours or 'SELECT 1')
        self.q("CALL differential_create_snapshot('o')")
        self.q(theirs or 'SELECT 1')
        self.q("CALL differential_create_snapshot('t')")
        self.q('DETACH o; DETACH t; DETACH d;')
        return name

    def preview(self, name, suffix=''): return f"differential_merge_snapshots_preview('{name}',1,2,3{suffix})"
    def merge(self, name, destination='merged', request=''):
        return f"differential_merge_snapshots('{name}',1,2,3,'{destination}',request_id=>'{request}')"
    def attach(self, name, branch='merged'):
        self.q(f"ATTACH 'differential:{name}' AS m (BRANCH '{branch}')")


def main():
    passed = []
    def ok(label): passed.append(label); print('PASS', label, flush=True)
    with tempfile.TemporaryDirectory(prefix='differential-logical-merge-') as tmp:
        root = Path(tmp)
        f = Fixture(root / 'repo')
        try:
            name = f.setup('CREATE TABLE d.x(id INTEGER PRIMARY KEY, a BIGINT, b VARCHAR, c BOOLEAN, amount DECIMAL(18,3));'
                           'CREATE TABLE d.composite(a INT,b VARCHAR,v INT,PRIMARY KEY(a,b));',
                "INSERT INTO d.x SELECT i,0,'old',false,1.250 FROM range(1,13) r(i);"
                "UPDATE d.x SET b=NULL WHERE id=9; INSERT INTO d.composite VALUES(1,'x',NULL),(1,'y',2);",
                "UPDATE o.x SET a=1 WHERE id IN (1,3,4,12); DELETE FROM o.x WHERE id IN (5,7,8);"
                "INSERT INTO o.x VALUES(80,8,'moved',true,2.500),(100,1,NULL,false,NULL),(102,2,'same',true,3.125);"
                "UPDATE o.x SET b='from null' WHERE id=9; UPDATE o.x SET b=NULL WHERE id=10;"
                "UPDATE o.composite SET v=3 WHERE b='x';",
                "UPDATE t.x SET a=1 WHERE id IN (2,4,12); UPDATE t.x SET b='theirs',amount=2.375 WHERE id=3;"
                "DELETE FROM t.x WHERE id IN (6,7); INSERT INTO t.x VALUES(101,1,'new',NULL,8.875),(102,2,'same',true,3.125);"
                "UPDATE t.composite SET v=NULL WHERE b='y';")
            # Live mutations after the selected snapshot must not be captured.
            f.q(f"ATTACH 'differential:{name}' AS live (BRANCH 'o'); INSERT INTO live.x VALUES(999,99,'live',true,9); DETACH live;")
            counts = f.q("SELECT count(*) FROM differential_operations()")
            f.q(f"PREPARE p AS SELECT * FROM {f.merge(name)}; EXPLAIN SELECT * FROM {f.merge(name)};")
            assert f.q("SELECT count(*) FROM differential_operations()") == counts
            assert f.q(f"SELECT sum(conflicts) FROM {f.preview(name)}") == 0
            assert f.q("SELECT count(*) FROM differential_operations()") == counts
            assert f.q(f"SELECT count(*) FROM differential_snapshots('{name}')") == 3
            assert f.q(f"SELECT count(*) FROM differential_branches('{name}')") == 3
            f.q("SET differential_snapshot_on_checkpoint=true; SET default_collation='nocase';")
            sid = f.q(f"SELECT snapshot_id FROM {f.merge(name, request='first')}")
            assert f.q("SELECT current_setting('default_collation')='nocase'") == 1
            f.q("SET differential_snapshot_on_checkpoint=false; SET default_collation='';")
            assert sid == 4
            assert f.q(f"SELECT snapshot_id FROM {f.merge(name, request='first')}") == sid
            f.error(f"SELECT * FROM {f.merge(name, 'different', 'first')}", 'request')
            f.error(f"SELECT * FROM {f.merge(name)}", 'name_conflict')
            f.attach(name)
            assert f.q('SELECT count(*) FROM m.x') == 12
            assert f.q("SELECT count(*) FROM m.x WHERE id IN (5,6,7,8,999)") == 0
            assert f.q("SELECT count(*) FROM m.x WHERE (id=3 AND a=1 AND b='theirs' AND amount=2.375) OR (id=9 AND b='from null') OR (id=10 AND b IS NULL) OR (id=12 AND a=1)") == 4
            assert f.q("SELECT count(*) FROM m.composite WHERE (b='x' AND v=3) OR (b='y' AND v IS NULL)") == 2
            f.q('DETACH m')
            # Correct explicit base for re-merging incorporated theirs gives unchanged merged state.
            assert f.q(f"SELECT snapshot_id FROM differential_merge_snapshots('{name}',3,4,3,'again')") == 5
            f.attach(name, 'again')
            assert f.q('SELECT count(*) FROM m.x') == 12
            f.q('DETACH m')
            assert f.q(f"SELECT count(*) FROM differential_snapshots('{name}') WHERE snapshot_id=4 AND commit_extra_info LIKE '%logical-cell-v1%' AND commit_extra_info LIKE '%theirs%'") == 1
            ok('typed scalar, composite key, NULL, cell merge, row existence, PK move, counter state, live isolation, preview/planning, idempotency, explicit-base repeat')

            conflict = f.setup('CREATE TABLE d.x(id INT PRIMARY KEY,a INT,b VARCHAR);',
                "INSERT INTO d.x VALUES(1,0,'a'),(2,0,NULL),(3,0,'b'),(4,0,NULL);",
                "UPDATE o.x SET a=1 WHERE id=1; DELETE FROM o.x WHERE id=2; UPDATE o.x SET b='ours' WHERE id=4; INSERT INTO o.x VALUES(5,1,NULL);",
                "UPDATE t.x SET a=2 WHERE id=1; UPDATE t.x SET b='changed' WHERE id=2; UPDATE t.x SET b='theirs' WHERE id=4; INSERT INTO t.x VALUES(5,2,NULL);")
            assert f.q(f"SELECT conflicts FROM {f.preview(conflict)}") == 4
            assert f.q(f"SELECT len(conflict_details) FROM {f.preview(conflict, ',conflict_limit=>2')}") == 2
            assert f.q(f"SELECT conflict_details[1].key[1].integer_value FROM {f.preview(conflict, ',conflict_offset=>3,conflict_limit=>1')}") == 5
            assert f.q(f"SELECT NOT conflict_details[2].ours_present AND conflict_details[2].base_values[3].is_null FROM {f.preview(conflict)}") == 1
            f.error(f'SELECT * FROM {f.merge(conflict)}', 'merge_conflict')
            assert f.q(f"SELECT count(*) FROM differential_branches('{conflict}')") == 3
            assert f.q(f"SELECT count(*) FROM differential_snapshots('{conflict}')") == 3
            ok('cell/insert/delete-update conflicts, typed missing-vs-NULL details and bounded deterministic paging; no publication')

            many=f.setup('CREATE TABLE d.a(id INT PRIMARY KEY,v INT); CREATE TABLE d.b(id INT PRIMARY KEY,v INT);',
                'INSERT INTO d.a VALUES(1,0); INSERT INTO d.b VALUES(1,0);',
                'UPDATE o.a SET v=1; UPDATE o.b SET v=1;',
                'UPDATE t.a SET v=2; UPDATE t.b SET v=2;')
            assert f.q(f"SELECT sum(len(conflict_details)) FROM {f.preview(many,',conflict_limit=>1')}")==1
            assert f.q(f"SELECT len(conflict_details) FROM {f.preview(many,',conflict_limit=>1,conflict_offset=>1')} WHERE table_name='b'")==1
            ok('global conflict detail budget and deterministic paging across tables')

            # Independent exhaustive state oracle, including row absence and nullable cells.
            states=[None,(0,None),(0,'a'),(1,None),(1,'a'),(2,'b')]
            def oracle(base,ours,theirs):
                if base is None:
                    if ours is None: return theirs,None
                    if theirs is None or ours==theirs: return ours,None
                    return None,'insert_insert'
                if ours is None or theirs is None:
                    surviving=theirs if ours is None else ours
                    return (None,None) if surviving is None or surviving==base else (None,'delete_update')
                merged=[]
                for b,o,t in zip(base,ours,theirs):
                    if o==t or t==b: merged.append(o)
                    elif o==b: merged.append(t)
                    else: return None,'cell'
                return tuple(merged),None
            inputs=[[],[],[]]; good=[[],[],[]]; expected=[]; expected_conflicts=[]
            for key, triple in enumerate(itertools.product(states,repeat=3)):
                final,kind=oracle(*triple)
                for side,state in enumerate(triple):
                    if state is not None:
                        inputs[side].append((key,*state))
                        if kind is None: good[side].append((key,*state))
                if kind: expected_conflicts.append((key,kind))
                elif final is not None: expected.append((key,*final))
            def image(alias,rows): return f'DELETE FROM {alias}.x; INSERT INTO {alias}.x VALUES '+values(rows)+';'
            n=f.setup('CREATE TABLE d.x(id INT PRIMARY KEY,a INT,b VARCHAR);',image('d',inputs[0]),image('o',inputs[1]),image('t',inputs[2]))
            assert f.q(f'SELECT conflicts FROM {f.preview(n)}')==len(expected_conflicts)
            detail=f.preview(n,',conflict_limit=>1000')
            assert f.q(f"WITH details AS (SELECT unnest(conflict_details) c FROM {detail}) SELECT count(*) FROM (SELECT c.key[1].integer_value,c.kind FROM details EXCEPT SELECT * FROM (VALUES {values(expected_conflicts)}))")==0
            n=f.setup('CREATE TABLE d.x(id INT PRIMARY KEY,a INT,b VARCHAR);',image('d',good[0]),image('o',good[1]),image('t',good[2]))
            f.q(f'SELECT * FROM {f.merge(n)}'); f.attach(n)
            assert f.q('SELECT count(*) FROM m.x')==len(expected)
            assert f.q(f'SELECT count(*) FROM (SELECT * FROM m.x EXCEPT SELECT * FROM (VALUES {values(expected)}))')==0
            f.q('DETACH m')
            ok('independent exhaustive oracle: 216 base/ours/theirs row-state combinations')

            # Exact decimal detail encoding including negative and 38-digit decimal.
            decimal = f.setup('CREATE TABLE d.x(id INT PRIMARY KEY,v DECIMAL(38,6));',
                'INSERT INTO d.x VALUES(1,NULL);', 'UPDATE o.x SET v=-123.456789;', 'UPDATE t.x SET v=9.123456;')
            assert f.q(f"SELECT conflict_details[1].ours_values[2].decimal_unscaled FROM {f.preview(decimal)}") == -123456789
            assert f.q(f"SELECT conflict_details[1].ours_values[2].decimal_scale FROM {f.preview(decimal)}") == 6
            ok('decimal conflict details preserve exact unscaled value and scale')

            constraint = f.setup('CREATE TABLE d.x(id INT PRIMARY KEY,v INT UNIQUE NOT NULL);',
                'INSERT INTO d.x VALUES(1,10),(2,20);', 'UPDATE o.x SET v=30 WHERE id=1;', 'UPDATE t.x SET v=30 WHERE id=2;')
            assert f.q(f'SELECT conflicts FROM {f.preview(constraint)}') == 0
            f.error(f'SELECT * FROM {f.merge(constraint)}', 'merge_constraint')
            assert f.q(f"SELECT count(*) FROM differential_branches('{constraint}')") == 3
            f.attach(constraint, 'o'); assert f.q('SELECT sum(v) FROM m.x') == 50; f.q('DETACH m')
            swap = f.setup('CREATE TABLE d.x(id INT PRIMARY KEY,v INT UNIQUE NOT NULL);',
                'INSERT INTO d.x SELECT i,i FROM range(3000) r(i);', '',
                'DELETE FROM t.x; INSERT INTO t.x SELECT i,(i+1)%3000 FROM range(3000) r(i);')
            f.q(f'SELECT * FROM {f.merge(swap)}'); f.attach(swap)
            assert f.q('SELECT count(*) FROM m.x WHERE v=(id+1)%3000') == 3000
            f.error('UPDATE m.x SET v=(v+1)%3000', 'Duplicate key')
            f.q('DETACH m')
            ok('final UNIQUE violation is separate from data conflict; full rebuild handles 3000-row unique permutation that UPDATE rejects')

            gate=root/'cancel'
            os.environ['DIFFERENTIAL_MERGE_GATE']=str(gate)
            errors=[]
            def work():
                try: f.q(f"SELECT * FROM {f.merge(swap,'cancelled','cancel')}")
                except RuntimeError as error: errors.append(str(error))
            thread=threading.Thread(target=work); thread.start()
            deadline=time.monotonic()+30
            while not Path(str(gate)+'.entered').exists():
                assert thread.is_alive() and time.monotonic()<deadline,errors
                time.sleep(.01)
            f.host.lib.duckdb_interrupt(f.conn)
            thread.join(timeout=10)
            os.environ.pop('DIFFERENTIAL_MERGE_GATE')
            assert not thread.is_alive() and errors
            assert f.q(f"SELECT count(*) FROM differential_branches('{swap}') WHERE branch_name='cancelled'")==0
            f.q(f"SELECT * FROM {f.merge(swap,'cancelled','cancel')}")
            ok('cancellation after checkpoint publishes no destination; same-request restart succeeds')

            # Two execution connections start the same durable request before either publishes.
            gate=root/'concurrent'
            os.environ['DIFFERENTIAL_MERGE_GATE']=str(gate)
            connections=[f.host.connect(),f.host.connect()]
            answers=[]; errors=[]
            def concurrent(conn):
                try: answers.append(f.host.query(conn,f"SELECT snapshot_id FROM {f.merge(swap,'concurrent','concurrent')}"))
                except RuntimeError as error: errors.append(str(error))
            workers=[threading.Thread(target=concurrent,args=(conn,)) for conn in connections]
            for worker in workers: worker.start()
            deadline=time.monotonic()+30
            while True:
                # Use the extension's SQLite instance while writers are live: opening the
                # same WAL with Python's separately linked SQLite can invalidate shm locks.
                stages=f.q("SELECT count(*) FROM differential_retention_roots() WHERE reason='native_file' AND starts_with(identity,'merge_stage/') AND NOT contains(identity,'.')")
                waiting=f.q("SELECT count(*) FROM differential_merge_status() WHERE phase='request_waiting'")
                if Path(str(gate)+'.entered').exists() and stages==1 and waiting==1: break
                assert all(w.is_alive() for w in workers) and time.monotonic()<deadline,errors
                time.sleep(.01)
            assert f.q("SELECT count(*) FROM duckdb_databases() WHERE starts_with(database_name,'_merge_')")==0
            Path(str(gate)+'.release').touch()
            for worker in workers: worker.join(timeout=30)
            os.environ.pop('DIFFERENTIAL_MERGE_GATE')
            assert not errors and len(answers)==2 and answers[0]==answers[1],(answers,errors)
            assert f.q(f"SELECT count(*) FROM differential_branches('{swap}') WHERE branch_name='concurrent'")==1
            ok('concurrent identical requests publish exactly one completed branch/snapshot')

            n=f.setup('CREATE TABLE d.x(id BIGINT PRIMARY KEY,a INT,b VARCHAR);',
                "INSERT INTO d.x SELECT i,0,repeat('x',100) FROM range(30000) r(i);",
                'UPDATE o.x SET a=1 WHERE id%10=0;',
                "UPDATE t.x SET b='changed' WHERE id%20=0;")
            f.q(f"SET temp_directory='{root/'spill'}'; SET max_temp_directory_size='256MB'; SET debug_force_external=true;")
            f.q(f'SELECT * FROM {f.merge(n)}')
            f.q('SET debug_force_external=false;')
            f.attach(n)
            assert f.q("SELECT count(*) FROM m.x WHERE a=1 AND b='changed'")==1500
            assert f.q('SELECT count(*) FROM m.x')==30000
            f.q('DETACH m')
            ok('30k-row merge through forced external execution with bounded local temporary storage')

            for ddl, reason in [
                ('CREATE TABLE d.x(i INT);', 'primary key'),
                ('CREATE TABLE d.x(i INT PRIMARY KEY,v BIT);', 'type BIT'),
                ('CREATE TABLE d.x(i INT PRIMARY KEY,v BIT[]);', 'type BIT'),
                ("CREATE TABLE d.x(i VARCHAR COLLATE nocase PRIMARY KEY);", 'collation'),
                ('CREATE TABLE d.x(i INT PRIMARY KEY,v INT DEFAULT random());', 'expression function'),
                ('CREATE TABLE d.x(i INT PRIMARY KEY,v INT GENERATED ALWAYS AS(i+1));', 'generated'),
                ('CREATE TABLE d.x(i INT PRIMARY KEY,v INT CHECK(random()>0));', 'expression function'),
                ('CREATE TABLE d.x(i INT PRIMARY KEY); CREATE TABLE memory.merge_external(i INT); CREATE VIEW d.v AS SELECT * FROM memory.merge_external;', 'view'),
                ('CREATE TABLE d.x(i INT PRIMARY KEY,v INT); CREATE INDEX ix ON d.x((v+1));', 'expression index'),
                ('CREATE TABLE d.x(i INT PRIMARY KEY); USE d; CREATE TABLE y(i INT PRIMARY KEY REFERENCES x(i)); USE memory;', 'constraints'),
                ('CREATE SEQUENCE d.seq; CREATE TABLE d.x(i INT PRIMARY KEY);', 'sequence'),
                ('CREATE TABLE d.x(i INT PRIMARY KEY); CREATE TABLE d.audit(i INT PRIMARY KEY); USE d; CREATE TRIGGER trg AFTER INSERT ON x FOR EACH ROW INSERT INTO audit VALUES(NEW.i); USE memory;', 'trigger'),
                ('CREATE TABLE d.x(i INT PRIMARY KEY); CREATE SCHEMA d.main.main;', 'only the main schema'),
            ]:
                n = f.setup(ddl)
                f.error(f'SELECT * FROM {f.merge(n)}', reason)
                assert f.q(f"SELECT count(*) FROM differential_branches('{n}')") == 3
            n = f.setup('CREATE TABLE d.x(i INT PRIMARY KEY);', '', 'ALTER TABLE o.x ADD COLUMN a INT;')
            f.error(f'SELECT * FROM {f.merge(n)}', 'schema mismatch')
            f.error(f"SELECT * FROM differential_merge_snapshots('{name}',999,2,3,'missing')", 'not_found')
            f.q(f"CALL differential_expire_snapshots('{name}',versions=>[1])")
            f.error(f"SELECT * FROM {f.merge(name,'expired')}", 'retired')
            assert f.q(f"SELECT snapshot_id FROM {f.merge(name,request='first')}") == 4
            f.error("SELECT * FROM differential_merge_snapshots_preview('absent',1,2,3,conflict_limit=>1001)", 'conflict_limit')
            f.error("ATTACH 'diff://merge_stage/00000000-0000-0000-0000-000000000000' AS bad", 'merge staging')
            f.q("ATTACH 'differential:remote' AS r (DATA_PATH 's3://unused-test-bucket/merge/', ENDPOINT 'http://127.0.0.1:9', ALLOW_HTTP true, PATH_STYLE true);")
            f.error("SELECT * FROM differential_merge_snapshots('r',1,2,3,'remote_result',request_id=>'missing-s3')", 'not_found')
            f.q('DETACH r')
            ok('schema/type/object/key/constraint scope rejection, missing S3 inputs, missing/expired inputs, completed retry after expiry')
        finally:
            f.host.close()

        # S3/shared rejection is tested offline by inserting configuration through existing SQL ATTACH
        # with an invalid provider would itself contact S3. Instead, metadata fixture uses an existing
        # database's documented writers flag and a dummy provider record in a disposable repository.
        with sqlite3.connect(root/'repo'/'engine.sqlite') as db:
            db.execute("UPDATE databases SET writers='shared' WHERE name=?", (conflict,))
        f = Fixture(root/'repo')
        try: f.error(f'SELECT * FROM {f.merge(conflict)}', 'single-writer')
        finally: f.host.close()
        ok('shared-writer configuration rejected before staging (offline metadata fixture)')

        for point in ('staged', 'publication_transaction', 'published'):
            repo = root/point
            prefix = f"LOAD '{EXTENSION}'; SET differential_repository='{repo}'; SET threads=2;"
            def run(sql, crash=None, code=0):
                env = dict(os.environ)
                if crash: env['DIFFERENTIAL_MERGE_CRASH'] = crash
                p = subprocess.run([DUCKDB_CLI,'-init','/dev/null','-unsigned','-batch','-bail','-json'],
                    input=prefix+sql, text=True, capture_output=True, env=env, timeout=60)
                assert p.returncode == code, (point, sql, p.returncode, p.stdout, p.stderr)
                return p.stdout
            run("ATTACH 'differential:crash' AS d; CREATE TABLE d.x(i INT PRIMARY KEY,a INT,b INT); INSERT INTO d.x VALUES(1,0,0); CALL differential_create_snapshot('d');"
                "CALL differential_create_branch('d','ours',snapshot=>1); CALL differential_create_branch('d','theirs',snapshot=>1);"
                "ATTACH 'differential:crash' AS o (BRANCH 'ours'); UPDATE o.x SET a=1; CALL differential_create_snapshot('o');"
                "ATTACH 'differential:crash' AS t (BRANCH 'theirs'); UPDATE t.x SET b=2; CALL differential_create_snapshot('t');")
            merge = "SELECT snapshot_id FROM differential_merge_snapshots('crash',1,2,3,'merged',request_id=>'retry');"
            run(merge, point, 86)
            with sqlite3.connect(repo/'engine.sqlite') as db:
                expected = 1 if point == 'published' else 0
                assert db.execute("SELECT count(*) FROM branches WHERE name='merged'").fetchone()[0] == expected
                assert db.execute("SELECT count(*) FROM operations WHERE kind='merge' AND state='complete'").fetchone()[0] == expected
            result = run(merge)
            assert result == run(merge)
            run("CALL differential_cleanup_old_files(cleanup_all=>true);")
            assert json.loads(run("ATTACH 'differential:crash' AS m (BRANCH 'merged'); SELECT * FROM m.x;"))==[{'i':1,'a':1,'b':2}]
            assert json.loads(run("ATTACH 'differential:crash' AS d; SELECT * FROM d.x;"))==[{'i':1,'a':0,'b':0}]
            with sqlite3.connect(repo/'engine.sqlite') as db:
                assert db.execute('PRAGMA integrity_check').fetchone()[0] == 'ok'
                assert db.execute("SELECT count(*) FROM files WHERE name LIKE 'merge_stage/%'").fetchone()[0] == 0
                assert db.execute('SELECT count(*) FROM snapshots').fetchone()[0] == 4
            ok('crash + retry + GC at '+point)
    evidence_path('logical-merge.json').write_text(json.dumps({'passed':passed},indent=2)+'\n')


if __name__ == '__main__': main()
