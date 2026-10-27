#!/usr/bin/env python3
"""Merge expression context, catalog closure and unchanged-view preservation.

Only disposable local repositories. Native SQL is the oracle for temporal casts;
process-death probes verify the actual frozen-context/rebuild boundaries.
"""
import json
import sqlite3
import tempfile
from pathlib import Path

from evidence import evidence_path
from logical_merge import Fixture
from merge_schema import SchemaFixture
from merge_hardening import assert_no_stage


def main():
    passed = []
    def ok(label):
        passed.append(label)
        print('PASS merge catalog safety', label, flush=True)

    with tempfile.TemporaryDirectory(prefix='differential-merge-catalog-safety-') as tmp:
        root = Path(tmp)
        f = Fixture(root / 'repo')
        try:
            for typ in ('TIMESTAMPTZ', 'TIMESTAMPTZ_NS'):
                f.q("SET TimeZone='America/New_York'; SET Calendar='gregorian';")
                n = f.setup(f'CREATE TABLE d.x(id INT PRIMARY KEY,a INT,b INT,ts {typ},day DATE,CHECK(CAST(ts AS DATE)=day));',
                    f"INSERT INTO d.x SELECT 1,0,0,{typ} '2020-01-02 01:00:00+00',DATE '2020-01-01';",
                    'UPDATE o.x SET a=1;', 'UPDATE t.x SET b=2;')
                before = f.q('SELECT count(*) FROM differential_operations()')
                assert f.q(f"SELECT validation_state='valid' FROM {f.preview(n, ',validate=>true')}") == 1
                assert f.q(f"SELECT ready FROM differential_merge_branch_preflight('{n}','o','t','merged')") == 1
                assert f.q('SELECT count(*) FROM differential_operations()') == before
                f.q(f'SELECT * FROM {f.merge(n)}')
                f.attach(n)
                assert f.q('SELECT a=1 AND b=2 AND CAST(ts AS DATE)=day FROM m.x') == 1
                f.q('DETACH m')
                assert f.q("SELECT current_setting('TimeZone')='America/New_York'") == 1

                n = f.setup(f'CREATE TABLE d.x(id INT PRIMARY KEY,ts {typ},day DATE,allowed BOOLEAN,CHECK(allowed OR CAST(ts AS DATE)=day));',
                    f"INSERT INTO d.x VALUES(1,{typ} '2020-01-02 01:00:00+00',DATE '2020-01-01',true);",
                    "UPDATE o.x SET day=DATE '2020-01-02';", 'UPDATE t.x SET allowed=false;')
                assert f.q(f"SELECT conflicts=0 AND validation_state='constraint_violation' FROM {f.preview(n, ',validate=>true')}") == 1
                assert f.q(f"SELECT NOT ready AND validation_state='constraint_violation' FROM differential_merge_branch_preflight('{n}','o','t','merged')") == 1
                f.error(f'SELECT * FROM {f.merge(n)}', 'merge_constraint')
                assert f.q(f"SELECT count(*) FROM differential_branches('{n}')") == 3
                assert f.q(f"SELECT count(*) FROM differential_snapshots('{n}')") == 3
                assert_no_stage(f)
            ok('native micro/nanosecond zoned CHECKs: valid results accepted, invalid combinations blocked in preview/preflight/execution, caller unchanged')

            f.q("SET Calendar='buddhist'")
            n = f.setup('CREATE TABLE d.x(id INT PRIMARY KEY,a INT,b INT,ts TIMESTAMPTZ,day DATE,CHECK(CAST(ts AS DATE)=day));',
                "INSERT INTO d.x SELECT 1,0,0,TIMESTAMPTZ '2020-01-02 01:00:00+00',CAST(TIMESTAMPTZ '2020-01-02 01:00:00+00' AS DATE);",
                'UPDATE o.x SET a=1;', 'UPDATE t.x SET b=2;')
            f.q(f'SELECT * FROM {f.merge(n)}')
            f.attach(n)
            assert f.q('SELECT a=1 AND b=2 AND CAST(ts AS DATE)=day FROM m.x') == 1
            f.q("DETACH m; SET Calendar='gregorian'; SET TimeZone='UTC';")
            ok('native Calendar setting is preserved without inventing calendar conversion rules')

            f.q('SET GLOBAL integer_division=false')
            n = f.setup('CREATE TABLE d.x(id INT PRIMARY KEY,a INT,b INT,ratio DOUBLE,CHECK(a/2=ratio));',
                        'INSERT INTO d.x VALUES(1,1,0,0.5);', 'UPDATE o.x SET a=3,ratio=1.5;', 'UPDATE t.x SET b=2;')
            f.q('SET GLOBAL integer_division=true')
            try:
                f.q(f'SELECT * FROM {f.merge(n)}')
                f.attach(n)
                assert f.q('SELECT a=3 AND b=2 AND ratio=1.5 FROM m.x') == 1
                f.q('DETACH m')
                assert f.q("SELECT current_setting('integer_division')") == 1
            finally:
                f.q('SET GLOBAL integer_division=false')
            ok('stored native division operators survive a changed host parser setting without modifying caller settings')

            f.q("CREATE TYPE memory.external_status AS ENUM ('ok','bad')")
            definitions = [
                "USE d; CREATE VIEW v AS SELECT id,'ok'::memory.external_status AS status FROM x; USE memory;",
                "USE d; CREATE VIEW v AS SELECT id,CAST(NULL AS memory.external_status[]) AS status FROM x; USE memory;",
                "USE d; CREATE VIEW v AS SELECT id,TRY_CAST(NULL AS STRUCT(n memory.external_status)) AS status FROM x; USE memory;",
                "CREATE VIEW d.v AS SELECT 'ok'::external_status AS status;",
                "ALTER TABLE d.x ADD COLUMN extra VARCHAR DEFAULT CAST(CAST('ok' AS memory.external_status) AS VARCHAR);",
            ]
            for definition in definitions:
                n = f.setup('CREATE TABLE d.x(id INT PRIMARY KEY,a INT,b INT);'+definition,
                            'INSERT INTO d.x(id,a,b) VALUES(1,0,0);', 'UPDATE o.x SET a=1;', 'UPDATE t.x SET b=2;')
                for mode in ('strict','additive','catalog'):
                    options = ",schema_mode=>'"+mode+"'"
                    f.error(f'SELECT * FROM {f.preview(n, options)}', 'merge_unsupported')
                    f.error(f"SELECT * FROM differential_merge_snapshots('{n}',1,2,3,'merged',schema_mode=>'{mode}')", 'merge_unsupported')
                assert f.q(f"SELECT count(*) FROM differential_branches('{n}')") == 3
                assert_no_stage(f)
            n = f.setup("CREATE TABLE d.x(id INT PRIMARY KEY,a INT,b INT,s VARCHAR CHECK(CAST(s AS memory.external_status)='ok'));",
                        "INSERT INTO d.x VALUES(1,0,0,'ok');", 'UPDATE o.x SET a=1;', 'UPDATE t.x SET b=2;')
            assert f.q(f"SELECT NOT ready AND validation_state='unsupported' FROM differential_merge_branch_preflight('{n}','o','t','merged')") == 1
            f.error(f'SELECT * FROM {f.merge(n)}', 'merge_unsupported')
            f.q('DROP TYPE memory.external_status')
            ok('qualified/unqualified/custom nested CAST and TRY_CAST targets are rejected in views, defaults and CHECKs in every schema mode')

            n = f.setup('CREATE TABLE d.x(id INT PRIMARY KEY,a INT,b INT); USE d; '
                'CREATE VIEW v AS SELECT id,CAST(a AS DECIMAL(10,3)) AS amount,CAST(NULL AS STRUCT(n UUID,ts TIMESTAMPTZ_NS[])) AS nested FROM x; USE memory;',
                'INSERT INTO d.x VALUES(1,0,0);', 'UPDATE o.x SET a=1;', 'UPDATE t.x SET b=2;')
            f.q(f'SELECT * FROM {f.merge(n)}')
            f.attach(n)
            assert f.q('SELECT amount=1.000 AND nested IS NULL FROM m.v') == 1
            f.q('DETACH m')
            ok('supported native decimal and nested cast targets remain usable')

            for mode in ('strict','additive','catalog'):
                for tracked in (False, True):
                    f.q('SET enable_view_dependencies='+str(tracked).lower())
                    n = f.setup('CREATE TABLE d.x(id INT PRIMARY KEY,a INT,b INT); USE d; '
                                'CREATE VIEW z_leaf AS SELECT * FROM x; CREATE VIEW a_top AS SELECT sum(a+b) AS total FROM z_leaf; USE memory;',
                                'INSERT INTO d.x VALUES(1,0,0);', 'UPDATE o.x SET a=1;', 'UPDATE t.x SET b=2;')
                    f.q('SET enable_view_dependencies='+str(not tracked).lower())
                    assert f.q(f"SELECT bool_and(validation_state='valid') FROM {f.preview(n, ',validate=>true,schema_mode=>'+repr(mode))}") == 1
                    f.q(f"SELECT * FROM differential_merge_snapshots('{n}',1,2,3,'merged',schema_mode=>'{mode}')")
                    f.attach(n)
                    assert f.q('SELECT total FROM m.a_top') == 3
                    if tracked: f.error('DROP TABLE m.x', 'depends on table')
                    else: f.q('DROP TABLE m.x')
                    f.q('DETACH m')
                    assert f.q('SELECT current_setting(\'enable_view_dependencies\')='+str(not tracked).lower()) == 1
            ok('unchanged tracked/untracked view chains survive all schema modes independently of caller tracking setting')

            for tracked in (False, True):
                f.q('SET enable_view_dependencies='+str(tracked).lower())
                n = f.setup('CREATE TABLE d.x(id INT PRIMARY KEY,a INT,b INT); USE d; CREATE VIEW v AS SELECT * FROM x; USE memory;',
                    'INSERT INTO d.x VALUES(1,0,0);', 'UPDATE o.x SET a=1;',
                    'SET enable_view_dependencies='+str(not tracked).lower()+'; DROP VIEW t.v; USE t; CREATE VIEW v AS SELECT * FROM x; USE memory; UPDATE t.x SET b=2;')
                for mode in ('strict','additive'):
                    options = ",schema_mode=>'"+mode+"'"
                    f.error(f'SELECT * FROM {f.preview(n, options)}', 'dependencies must be unchanged')
                    f.error(f"SELECT * FROM differential_merge_snapshots('{n}',1,2,3,'merged',schema_mode=>'{mode}')", 'dependencies must be unchanged')
                f.q(f"SELECT * FROM differential_merge_snapshots('{n}',1,2,3,'merged',schema_mode=>'catalog')")
                f.attach(n)
                if not tracked: f.error('DROP TABLE m.x', 'depends on table')
                else: f.q('DROP TABLE m.x')
                f.q('DETACH m')
            ok('dependency-only view changes fail strict/additive scope checks and merge explicitly in catalog mode')
        finally:
            f.host.close()

        for invalid in (False, True):
            for phase in (('context_frozen',) if invalid else ('context_frozen','staged')):
                c = SchemaFixture(root, f'context-{invalid}-{phase}')
                if invalid:
                    ddl = "SET TimeZone='America/New_York'; CREATE TABLE d.x(id INT PRIMARY KEY,ts TIMESTAMPTZ,day DATE,allowed BOOLEAN,CHECK(allowed OR CAST(ts AS DATE)=day)); INSERT INTO d.x VALUES(1,TIMESTAMPTZ '2020-01-02 01:00:00+00',DATE '2020-01-01',true);"
                    ours, theirs = "UPDATE o.x SET day=DATE '2020-01-02';", 'UPDATE t.x SET allowed=false;'
                else:
                    ddl = "SET TimeZone='America/New_York'; SET Calendar='buddhist'; CREATE TABLE d.x(id INT PRIMARY KEY,a INT,b INT,ts TIMESTAMPTZ,day DATE,CHECK(CAST(ts AS DATE)=day)); INSERT INTO d.x SELECT 1,0,0,TIMESTAMPTZ '2020-01-02 01:00:00+00',CAST(TIMESTAMPTZ '2020-01-02 01:00:00+00' AS DATE);"
                    ours, theirs = 'UPDATE o.x SET a=1;', 'UPDATE t.x SET b=2;'
                c.setup(ours, theirs, definition=ddl)
                original = "SET TimeZone='America/New_York'; SET Calendar='"+('gregorian' if invalid else 'buddhist')+"';"
                c.run(original+c.merge(), crash=phase)
                retry = "SET TimeZone='UTC'; SET Calendar='gregorian';"+c.merge()
                if invalid:
                    c.run(retry, error='merge_constraint'); c.hidden()
                else:
                    rows = c.run(retry+"SELECT current_setting('TimeZone') AS zone,current_setting('Calendar') AS calendar;")
                    assert rows[-2][0]['snapshot_id'] == 4
                    assert rows[-1] == [{'zone':'UTC','calendar':'gregorian'}]
                    assert c.run(retry)[-1][0]['snapshot_id'] == 4
                    assert c.run(original+"ATTACH 'differential:merge_db' AS m (BRANCH 'merged'); SELECT a=1 AND b=2 AND CAST(ts AS DATE)=day AS valid FROM m.x;")[-1] == [{'valid':True}]
                assert c.scalar("SELECT count(*) FROM operations WHERE kind='merge_expression_context'") == 1
        ok('process death after context capture/staging: retries use original zone/calendar, preserve invalid-state rejection and publish exactly once')

        for mode in ('strict','additive'):
            for phase in ('views_dropped','table_inserted','views_created'):
                c = SchemaFixture(root, mode+'-'+phase)
                c.setup('UPDATE o.x SET a=1 WHERE id=1;', "UPDATE t.x SET b='edited' WHERE id=1;",
                    extra='SET enable_view_dependencies=true; USE d; CREATE VIEW z_leaf AS SELECT * FROM x; CREATE VIEW a_top AS SELECT count(*) AS n FROM z_leaf; USE memory;')
                merge = c.merge(options=",schema_mode=>'"+mode+"'")
                c.run(merge, crash=phase); c.hidden()
                assert c.run(merge)[-1][0]['snapshot_id'] == 4
                assert c.read('SELECT * FROM m.a_top') == [{'n':3}]
                c.run("ATTACH 'differential:merge_db' AS m (BRANCH 'merged'); DROP TABLE m.x;",error='depends on table')
                assert c.scalar('SELECT count(*) FROM snapshots') == 4
        ok('strict/additive tracked-view rebuild crashes preserve inputs and recover one completed native catalog')

        c = SchemaFixture(root,'unknown-context')
        c.setup('UPDATE o.x SET a=1;', "UPDATE t.x SET b='edited';")
        c.run(c.merge(),crash='context_frozen')
        with sqlite3.connect(c.repo/'engine.sqlite') as db:
            db.execute("UPDATE operations SET payload=replace(payload,'native-expression-context-v1','native-expression-context-v9') WHERE kind='merge_expression_context'")
        c.run(c.merge(),error='context version is unavailable'); c.hidden()
        ok('unknown pending native expression context versions fail closed')

    evidence_path('merge-catalog-safety.json').write_text(json.dumps({'passed':passed},indent=2)+'\n')


if __name__ == '__main__': main()
