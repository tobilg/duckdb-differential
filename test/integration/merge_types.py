#!/usr/bin/env python3
"""Native scalar equality, whole-value nested merges, typed diagnostics and recovery.

Only disposable repositories. Expected values use native typed SQL, never text/JSON equality.
"""
import json
import os
from pathlib import Path
import subprocess
import tempfile
from logical_merge import Fixture
from paths import DUCKDB_CLI, EXTENSION
from evidence import evidence_path
from s3_merge import parse


# type, base, ours, theirs, native diagnostic field (None means a nested tree).
CASES = [
    ('UUID', "'00000000-0000-0000-0000-000000000000'", "'ffffffff-ffff-ffff-ffff-ffffffffffff'", "'80000000-0000-0000-0000-000000000000'", 'uuid_value'),
    ('BLOB', "'\\x00\\xFF'", "'a\\x00b\\xFE'", "'\\xFF\\x00'", 'blob_value'),
    ('UHUGEINT', '0', '340282366920938463463374607431768211455', '170141183460469231731687303715884105728', 'unsigned_integer_value'),
    ('FLOAT', '0', "'NaN'", "'Infinity'", 'float_value'),
    ('DOUBLE', '0', "'-Infinity'", "'NaN'", 'double_value'),
    ('TIME', "'00:00:00'", "'23:59:59.123456'", "'12:34:56.654321'", 'time_value'),
    ('TIME_NS', "'00:00:00'", "'23:59:59.123456789'", "'23:59:59.123456788'", 'time_ns_value'),
    ('TIME WITH TIME ZONE', "'00:00:00+00'", "'12:34:56.123456+02'", "'12:34:56.123456-03'", 'time_tz_value'),
    ('TIMESTAMP_S', "'2000-01-01'", "'2020-01-01 12:34:56'", "'2020-01-01 12:34:57'", 'timestamp_s_value'),
    ('TIMESTAMP_MS', "'2000-01-01'", "'2020-01-01 12:34:56.123'", "'2020-01-01 12:34:56.124'", 'timestamp_ms_value'),
    ('TIMESTAMP', "'2000-01-01'", "'2020-01-01 12:34:56.123456'", "'2020-01-01 12:34:56.123457'", 'timestamp_value'),
    ('TIMESTAMP_NS', "'2000-01-01'", "'2020-01-01 12:34:56.123456789'", "'2020-01-01 12:34:56.123456788'", 'timestamp_ns_value'),
    ('TIMESTAMPTZ', "'2000-01-01 00:00:00+00'", "'2020-01-01 12:34:56.123456+02'", "'2020-01-01 12:34:56.123457+02'", 'timestamp_tz_value'),
    ('TIMESTAMPTZ_NS', "'2000-01-01 00:00:00+00'", "'2020-01-01 12:34:56.123456789+02'", "'2020-01-01 12:34:56.123456788+02'", 'timestamp_tz_ns_value'),
    ('INTERVAL', "'0 days'", "'1 month 2 days 3 microseconds'", "'1 month 2 days 4 microseconds'", 'interval_value'),
    ('INTEGER[]', '[0,NULL]', '[1,NULL]', '[2,NULL]', None),
    ('DOUBLE[2]', '[0,1]', "['NaN',NULL]", "['Infinity',NULL]", None),
    ('STRUCT(theme VARCHAR, language VARCHAR)', "{'theme':'light','language':'en'}", "{'theme':'dark','language':'en'}", "{'theme':'light','language':'de'}", None),
    ('MAP(VARCHAR,INTEGER)', "MAP {'a':0,'b':NULL}", "MAP {'a':1,'b':NULL}", "MAP {'a':0,'b':2}", None),
    ('UNION(num INTEGER, text VARCHAR)', 'union_value(num:=0)', 'union_value(num:=NULL)', "union_value(text:='x')", None),
    ('TUPLE(INTEGER,VARCHAR)', "(0,'base')", "(1,'base')", "(0,'theirs')", None),
    ('STRUCT(items MAP(VARCHAR,TIMESTAMPTZ_NS[]), amount DECIMAL(38,6))',
     "{'items':MAP {'x':[TIMESTAMPTZ_NS '2000-01-01 00:00:00+00',NULL]},'amount':1.250000}",
     "{'items':MAP {'x':[TIMESTAMPTZ_NS '2020-01-01 00:00:00.123456789+00',NULL]},'amount':1.250000}",
     "{'items':MAP {'x':[TIMESTAMPTZ_NS '2000-01-01 00:00:00+00',NULL]},'amount':2.375000}", None),
    ('UNION(u UUID, l BLOB[])[]', "[union_value(u:='00000000-0000-0000-0000-000000000000'::UUID)]",
     "[union_value(l:=[]::BLOB[]),NULL]", "[union_value(l:=['\\x00'::BLOB])]", None),
]


def typed(value, typ): return f'CAST({value} AS {typ})'


def recovery(root):
    # Freeze native values, kill at real boundaries, remove their source, then
    # recover the same target request. No string representation is used for data.
    for phase in ('resolutions_frozen','staged','publication_transaction','published'):
        repo=root/('recover-'+phase); native=root/(phase+'.duckdb')
        prefix=f"LOAD '{EXTENSION}'; SET differential_repository='{repo}'; SET threads=2; SET memory_limit='128MB';"
        def run(sql, crash=None):
            env=dict(os.environ)
            if crash: env['DIFFERENTIAL_MERGE_CRASH']=crash
            result=subprocess.run([DUCKDB_CLI,'-init','/dev/null','-unsigned','-batch','-bail','-json',str(native)],
                input=prefix+sql,text=True,capture_output=True,timeout=60,env=env)
            assert result.returncode==(86 if crash else 0),(phase,result.stdout,result.stderr)
            return parse(result.stdout)[-1] if result.stdout.strip() else []
        typ='STRUCT(n UHUGEINT,ts TIMESTAMPTZ_NS,b BLOB,u UNION(n DOUBLE,s VARCHAR))'
        expected="{'n':340282366920938463463374607431768211455::UHUGEINT,'ts':TIMESTAMPTZ_NS '2020-01-01 00:00:00.123456789+00','b':'\\x00\\xFF'::BLOB,'u':union_value(n:='NaN'::DOUBLE)}"
        run(f"ATTACH 'differential:crash' AS d; CREATE TABLE d.x(id UUID PRIMARY KEY,v {typ});"
            "INSERT INTO d.x VALUES('00000000-0000-0000-0000-000000000001',NULL); CALL differential_create_snapshot('d');"
            "CALL differential_create_branch('d','o',snapshot=>1); CALL differential_create_branch('d','t',snapshot=>1);"
            "ATTACH 'differential:crash' AS o (BRANCH 'o'); UPDATE o.x SET v={'n':1,'ts':NULL,'b':NULL,'u':union_value(s:='o')}; CALL differential_create_snapshot('o');"
            "ATTACH 'differential:crash' AS t (BRANCH 't'); UPDATE t.x SET v={'n':2,'ts':NULL,'b':NULL,'u':union_value(s:='t')}; CALL differential_create_snapshot('t');"
            "CREATE TABLE fixes AS SELECT *, 'cells' AS _merge_action,map{'v':'custom'} AS _merge_columns FROM o.x;"
            f"UPDATE fixes SET v={expected};")
        sql=f"SELECT snapshot_id FROM differential_merge_branch('crash','o','t','o',request_id=>'retry',resolution_tables=>map{{'main.x':'{phase}.main.fixes'}});"
        run(sql,phase); run('DROP TABLE fixes;')
        assert run(sql)[0]['snapshot_id']==4
        assert run(sql)[0]['snapshot_id']==4
        assert run(f"ATTACH 'differential:crash' AS m (BRANCH 'o'); SELECT v IS NOT DISTINCT FROM CAST({expected} AS {typ}) AS correct FROM m.x;")[0]['correct']
        assert run("SELECT count(*) AS n FROM differential_snapshots('crash');")[0]['n']==4


def main():
    passed = []
    def ok(label): passed.append(label); print('PASS', label, flush=True)
    with tempfile.TemporaryDirectory(prefix='differential-merge-types-') as tmp:
        root = Path(tmp); f = Fixture(root/'repo')
        try:
            for typ, b, o, t, field in CASES:
                b,o,t = [typed(v,typ) for v in (b,o,t)]
                n = f.setup(f'CREATE TABLE d.x(id INT PRIMARY KEY,v {typ},other INT);',
                    f'INSERT INTO d.x VALUES(1,{b},0),(2,{b},0),(3,{b},0),(4,NULL,0);',
                    f'UPDATE o.x SET v={o} WHERE id IN (1,2,3,4); INSERT INTO o.x VALUES(5,{o},0);',
                    f'UPDATE t.x SET other=7 WHERE id=1; UPDATE t.x SET v={o} WHERE id=2; '
                    f'UPDATE t.x SET v={t} WHERE id=3; INSERT INTO t.x VALUES(5,{o},0);')
                p=f.preview(n)
                assert f.q(f'SELECT conflicts FROM {p}') == 1, typ
                assert f.q(f"SELECT conflict_details[1].kind='cell' AND conflict_details[1].columns=['v'] FROM {p}") == 1
                if field:
                    for side,value in [('base',b),('ours',o),('theirs',t)]:
                        assert f.q(f'SELECT conflict_details[1].{side}_values[2].{field} IS NOT DISTINCT FROM {value} FROM {p}') == 1, (typ,side)
                else:
                    assert f.q(f'SELECT len(conflict_details[1].ours_values[2].nested_value)>1 FROM {p}') == 1
                    assert f.q(f'SELECT conflict_details[1].ours_values[2].nested_value[1].parent=0 FROM {p}') == 1
                f.q(f'CREATE TABLE fixes(id INT,v {typ},other INT,_merge_action VARCHAR,_merge_columns MAP(VARCHAR,VARCHAR));'
                    f"INSERT INTO fixes VALUES(3,{t},NULL,'cells',map{{'v':'custom'}});")
                opts=",resolution_tables=>map{'main.x':'memory.main.fixes'}"
                assert f.q(f"SELECT conflicts=0 AND resolved_conflicts=1 AND validation_state='valid' FROM {f.preview(n,opts+',validate=>true')}") == 1
                request=f'types-{f.number}'
                merge=f.merge(n,request=request)[:-1]+opts+')'
                assert f.q(f'SELECT snapshot_id FROM {merge}') == 4
                f.attach(n)
                expected=f'SELECT * FROM (VALUES(1,{o},7),(2,{o},0),(3,{t},0),(4,{o},0),(5,{o},0)) e(id,v,other)'
                assert f.q(f'SELECT count(*) FROM ((SELECT * FROM m.x EXCEPT ALL {expected}) UNION ALL ({expected} EXCEPT ALL SELECT * FROM m.x))') == 0, typ
                f.q('DETACH m; DROP TABLE fixes;')
                assert f.q(f'SELECT snapshot_id FROM {merge}') == 4
                if field:
                    assert f.q(f"SELECT values[2].{field} IS NOT DISTINCT FROM {t} FROM differential_merge_resolutions('{n}','{request}')") == 1
                ok('native typed merge, conflict details, custom resolution and replay: '+typ)

            # Unequal input sizes and sparse keys trigger a reordered outer-join
            # plan in stock 2.0 that cannot gather some NULL ARRAY payloads. The
            # worker materializes between joins, including recursively nested arrays.
            for typ, value in [('DOUBLE[2]', '[0,1]'),
                               ('STRUCT(a DOUBLE[2])', "{'a':[0,1]}"),
                               ('DOUBLE[2][]', '[[0,1],NULL]')]:
                key = "lpad(to_hex(i),32,'0')::UUID"
                ids = ','.join('('+str(i)+')' for i in list(range(2,33))+list(range(10000,10006))+[1])
                seed = (f'SELECT {key},CASE WHEN i IN(1,32) THEN NULL ELSE {typed(value,typ)} END,0 '
                        f'FROM (VALUES {ids}) r(i)')
                n=f.setup(f'CREATE TABLE d.x(id UUID PRIMARY KEY,v {typ},other INT);',
                    'INSERT INTO d.x '+seed+';',
                    "DELETE FROM o.x WHERE id='00000000-0000-0000-0000-000000000001';"
                    "INSERT INTO o.x VALUES('00000000-0000-0000-0000-000000004e26',NULL,0);",
                    f"INSERT INTO t.x VALUES('00000000-0000-0000-0000-000000004e26',{typed(value,typ)},0),"
                    f"('00000000-0000-0000-0000-000000002716',{typed(value,typ)},0);")
                f.q(f"ATTACH 'differential:{n}' AS o (SNAPSHOT_VERSION 2);"
                    f"ATTACH 'differential:{n}' AS t (SNAPSHOT_VERSION 3);")
                assert f.q(f'SELECT conflicts=1 AND conflict_details[1].kind=\'insert_insert\' FROM {f.preview(n)}') == 1
                f.q("CREATE TABLE fixes AS SELECT *, 'ours' AS _merge_action,"
                    "map{}::MAP(VARCHAR,VARCHAR) AS _merge_columns FROM o.x "
                    "WHERE id='00000000-0000-0000-0000-000000004e26';")
                opts=",resolution_tables=>map{'main.x':'memory.main.fixes'}"
                assert f.q(f"SELECT validation_state='valid' FROM {f.preview(n,opts+',validate=>true')}") == 1
                assert f.q('SELECT snapshot_id FROM '+f.merge(n)[:-1]+opts+')') == 4
                f.attach(n)
                expected=("SELECT * FROM (SELECT * FROM o.x UNION ALL SELECT * FROM t.x "
                          "WHERE id='00000000-0000-0000-0000-000000002716')")
                assert f.q(f'SELECT count(*) FROM ((SELECT * FROM m.x EXCEPT ALL {expected}) '
                           f'UNION ALL ({expected} EXCEPT ALL SELECT * FROM m.x))') == 0
                f.q('DETACH m; DETACH o; DETACH t; DROP TABLE fixes;')
                ok('NULL array outer joins, row conflicts and constrained application: '+typ)

            # Equality decisions must exactly agree with the host, including equal
            # values with different physical representations and nested NULLs.
            equivalent = [
                ('FLOAT', '0', "'NaN'", "'NaN'"),
                ('DOUBLE', '1', '0.0', '-0.0'),
                ('TIMESTAMPTZ', "'2000-01-01 00:00:00+00'", "'2020-01-01 12:00:00+02'", "'2020-01-01 10:00:00+00'"),
                ('TIMESTAMPTZ_NS', "'2000-01-01 00:00:00+00'", "'2020-01-01 12:00:00.123456789+02'", "'2020-01-01 10:00:00.123456789+00'"),
                ('INTERVAL', "'0 days'", "'1 month'", "'30 days'"),
                ('DOUBLE[]', '[0]', "['NaN'::DOUBLE,NULL,0.0]", "['NaN'::DOUBLE,NULL,-0.0]"),
                ('STRUCT(a INT)', 'NULL', "{'a':NULL}", "{'a':NULL}"),
                ('MAP(INT,INT)', 'NULL', 'MAP {}', 'MAP {}'),
                ('MAP(INT,INT)', 'MAP {}', 'MAP {1:NULL}', 'MAP {2:NULL}'),
                ('MAP(INT,INT)', 'MAP {}', 'MAP {1:10,2:20}', 'MAP {2:20,1:10}'),
                ('UNION(a INT,b INT)', 'NULL', 'union_value(a:=NULL)', 'union_value(b:=NULL)'),
                ('INT[]', '[0]', '[]', 'NULL'),
            ]
            for typ,b,o,t in equivalent:
                b,o,t=[typed(v,typ) for v in (b,o,t)]
                expected=f.q(f'SELECT ({o} IS DISTINCT FROM {t}) AND ({o} IS DISTINCT FROM {b}) AND ({t} IS DISTINCT FROM {b})')
                n=f.setup(f'CREATE TABLE d.x(id INT PRIMARY KEY,v {typ});',f'INSERT INTO d.x VALUES(1,{b});',
                          f'UPDATE o.x SET v={o};',f'UPDATE t.x SET v={t};')
                f.q("SET TimeZone='America/New_York'")
                assert f.q(f'SELECT conflicts FROM {f.preview(n)}') == expected,(typ,o,t)
                f.q("SET TimeZone='UTC'")
            ok('native equality oracle: NaN, signed zero, instants, intervals, list order, map order/missing keys and nested NULL/tag distinctions')

            # All six native timestamp types support infinite values without a cast
            # through microseconds; native comparisons decide these cases too.
            for typ in ('TIMESTAMP_S','TIMESTAMP_MS','TIMESTAMP','TIMESTAMP_NS','TIMESTAMPTZ','TIMESTAMPTZ_NS'):
                n=f.setup(f'CREATE TABLE d.x(id INT PRIMARY KEY,v {typ});',f"INSERT INTO d.x VALUES(1,'-infinity');",
                          "UPDATE o.x SET v='infinity';", "UPDATE t.x SET v='infinity';")
                assert f.q(f'SELECT snapshot_id FROM {f.merge(n)}') == 4
                f.attach(n); assert f.q("SELECT v='infinity' FROM m.x") == 1; f.q('DETACH m')
            ok('all six native timestamp types preserve infinities')

            # Native scalar primary keys (including composites), not hidden identity.
            for typ,b,o,t,field in CASES[:5]+CASES[8:14]:
                b,o,t=[typed(v,typ) for v in (b,o,t)]
                n=f.setup(f'CREATE TABLE d.x(k {typ},part INT,v INT,PRIMARY KEY(k,part));',f'INSERT INTO d.x VALUES({b},1,0),({o},2,0);',
                          f'UPDATE o.x SET v=1 WHERE k IS NOT DISTINCT FROM {b};',f'UPDATE t.x SET v=2 WHERE part=2; INSERT INTO t.x VALUES({t},3,3);')
                assert f.q(f'SELECT snapshot_id FROM {f.merge(n)}') == 4
                f.attach(n); assert f.q('SELECT count(*)=3 AND sum(v)=6 FROM m.x') == 1; f.q('DETACH m')
            ok('new scalar types work as declared composite primary keys')

            # Typed trees retain null containers, empty containers, active union
            # tags, binary bytes and timestamp precision at every nesting depth.
            typ='STRUCT(a INT[],b BLOB,u UNION(n INT,s VARCHAR),ts TIMESTAMPTZ_NS)'
            n=f.setup(f'CREATE TABLE d.x(id INT PRIMARY KEY,v {typ});', 'INSERT INTO d.x VALUES(1,NULL);',
                "UPDATE o.x SET v={'a':[],'b':'\\x00\\xFF'::BLOB,'u':union_value(n:=NULL),'ts':TIMESTAMPTZ_NS '2020-01-01 00:00:00.123456789+00'};",
                "UPDATE t.x SET v={'a':NULL,'b':NULL,'u':union_value(s:=NULL),'ts':NULL};")
            p=f.preview(n)
            assert f.q(f'SELECT conflict_details[1].base_present AND conflict_details[1].base_values[2].is_null AND len(conflict_details[1].base_values[2].nested_value)=1 FROM {p}')==1
            assert f.q(f"SELECT count(*) FROM (SELECT unnest(conflict_details[1].ours_values[2].nested_value) n FROM {p}) WHERE (n.cell.name='a' AND NOT n.cell.is_null) OR (n.cell.name='b' AND n.cell.blob_value='\\x00\\xFF'::BLOB) OR (n.cell.name='n' AND n.cell.is_null AND n.position=1) OR (n.cell.name='ts' AND n.cell.timestamp_tz_ns_value=TIMESTAMPTZ_NS '2020-01-01 00:00:00.123456789+00')")==4
            assert f.q(f"SELECT count(*) FROM (SELECT unnest(conflict_details[1].theirs_values[2].nested_value) n FROM {p}) WHERE n.cell.name='s' AND n.position=2 AND n.cell.is_null")==1
            ok('nested detail trees preserve parent/position, union tag, NULL versus empty, binary bytes and nanoseconds')

            n=f.setup('CREATE TABLE d.x(id INT PRIMARY KEY,v INT[]);', 'INSERT INTO d.x VALUES(1,[0]),(2,NULL),(3,[0]);',
                'DELETE FROM o.x WHERE id IN (1,2,3); INSERT INTO o.x VALUES(4,[]),(5,[1]);',
                'UPDATE t.x SET v=[1] WHERE id=1; DELETE FROM t.x WHERE id=3; INSERT INTO t.x VALUES(4,[]),(5,[2]);')
            assert f.q(f'SELECT conflicts=2 AND conflict_details[1].kind=\'delete_update\' AND NOT conflict_details[1].ours_present AND conflict_details[2].kind=\'insert_insert\' FROM {f.preview(n)}')==1
            ok('whole-value nested row existence distinguishes delete/update and same-key insert conflicts')

            typ='STRUCT(a INT[],b BLOB)'
            large="{'a':range(1000)::INT[],'b':repeat('x',65536)::BLOB}"
            n=f.setup(f'CREATE TABLE d.x(id INT PRIMARY KEY,v {typ});',
                'INSERT INTO d.x VALUES(1,NULL),(2,NULL);',
                f"UPDATE o.x SET v={large} WHERE id=1; UPDATE o.x SET v={{'a':[1],'b':'a'::BLOB}} WHERE id=2;",
                "UPDATE t.x SET v={'a':[2],'b':'b'::BLOB};")
            p=f.preview(n,',conflict_bytes=>8192')
            assert f.q(f'SELECT conflicts=2 AND len(conflict_details)=0 AND details_truncated AND next_conflict_offset=0 FROM {p}')==1
            assert f.q(f'SELECT len(conflict_details)=1 AND next_conflict_offset=2 FROM {f.preview(n,",conflict_bytes=>8192,conflict_offset=>1")}')==1
            assert f.q(f'SELECT len(conflict_details[1].ours_values[2].nested_value)>1000 FROM {f.preview(n,",conflict_bytes=>4194304,conflict_limit=>1")}')==1
            f.q(f'CREATE TABLE fixes(id INT,v {typ},_merge_action VARCHAR,_merge_columns MAP(VARCHAR,VARCHAR));'
                "INSERT INTO fixes VALUES(1,{'a':range(20000)::INT[],'b':NULL},'cells',map{'v':'custom'});")
            f.error(f"SELECT * FROM {f.preview(n,",resolution_tables=>map{'main.x':'memory.main.fixes'}")}", 'merge_limit')
            f.q("UPDATE fixes SET v={'a':[],'b':repeat('x',9000000)::BLOB};")
            f.error(f"SELECT * FROM {f.preview(n,",resolution_tables=>map{'main.x':'memory.main.fixes'}")}", 'merge_limit')
            f.q('DROP TABLE fixes;')
            assert f.q(f"SELECT count(*) FROM differential_branches('{n}')")==3
            ok('nested node and BLOB byte budgets precede C++ retrieval and frozen resolution capture; pages never skip oversized rows')

            for typ,error in [('BIT[]','merge_unsupported'),('STRUCT(v JSON)','merge_unsupported'),
                              ('VARIANT','merge_unsupported'),('INT'+'[]'*34,'merge_limit')]:
                n=f.setup(f'CREATE TABLE d.x(id INT PRIMARY KEY,v {typ});')
                f.error('SELECT * FROM '+f.preview(n),error)
                assert f.q(f"SELECT count(*) FROM differential_branches('{n}')")==3
            ok('unsupported nested leaves, JSON/VARIANT and excessive type nesting fail explicitly')

            n=f.setup('CREATE TABLE d.x(id INT PRIMARY KEY,v INT);','INSERT INTO d.x VALUES(1,0);',
                'ALTER TABLE o.x ADD COLUMN a STRUCT(n UHUGEINT,ts TIMESTAMP_NS); '
                "UPDATE o.x SET a={'n':340282366920938463463374607431768211455::UHUGEINT,'ts':TIMESTAMP_NS '2020-01-01 00:00:00.123456789'};",
                "ALTER TABLE t.x ADD COLUMN b UUID; UPDATE t.x SET b='ffffffff-ffff-ffff-ffff-ffffffffffff',v=7;")
            f.error('SELECT * FROM '+f.merge(n),'schema mismatch')
            assert f.q(f"SELECT snapshot_id FROM {f.merge(n)[:-1]},schema_mode=>'additive')")==4
            f.attach(n)
            assert f.q("SELECT v=7 AND a.n=340282366920938463463374607431768211455::UHUGEINT AND a.ts=TIMESTAMP_NS '2020-01-01 00:00:00.123456789' AND b='ffffffff-ffff-ffff-ffff-ffffffffffff'::UUID FROM m.x")==1
            f.q('DETACH m;')
            ok('existing additive rules admit new supported types; strict mode still rejects changed schemas')

            n=f.setup('CREATE TABLE d.x(id INT PRIMARY KEY,v INT,a INT[]);','INSERT INTO d.x VALUES(1,0,[1,NULL]);',
                'ALTER TABLE o.x DROP COLUMN a;','UPDATE t.x SET v=7;')
            assert f.q(f"SELECT snapshot_id FROM {f.merge(n)[:-1]},schema_mode=>'catalog')")==4
            f.attach(n); assert f.q('SELECT v=7 FROM m.x')==1; f.q('DETACH m;')
            ok('existing eligible-column removal proves whole nested values unchanged while other columns merge')
        finally: f.host.close()
        recovery(root)
        ok('native nested resolution recovery after freezing, checkpoint, publication transaction and publication; source removed, one result')
    evidence_path('merge-types.json').write_text(json.dumps({'passed':passed,'scope':'stock DuckDB v2.0.0, disposable local repositories'},indent=2)+'\n')


if __name__ == '__main__': main()
