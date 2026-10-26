#!/usr/bin/env python3
"""Bounded local merge acceptance: independent oracle, validation, limits, races and recovery.
All files and repositories are disposable. No remote provider or production data is used.
"""
from decimal import Decimal
import json
import os
from pathlib import Path
import random
import sqlite3
import subprocess
import tempfile
import threading
import time

from logical_merge import Fixture, literal, values
from paths import DUCKDB_CLI, EXTENSION
from evidence import evidence_path

ABSENT = object()
TYPES = ['INTEGER', 'VARCHAR', 'HUGEINT', 'UBIGINT', 'DECIMAL(38,9)', 'BOOLEAN', 'VARCHAR']
COLUMNS = ['part', 'key', 'signed', 'unsigned', 'amount', 'flag', 'text']
DDL = '(part INTEGER,key VARCHAR,signed HUGEINT,unsigned UBIGINT,amount DECIMAL(38,9),flag BOOLEAN,text VARCHAR,PRIMARY KEY(part,key))'


def oracle(base, ours, theirs):
    """State specification independent of SQL changesets and generated changed flags."""
    result, conflicts = {}, {}
    for key in base.keys() | ours.keys() | theirs.keys():
        b, o, t = (state.get(key, ABSENT) for state in (base, ours, theirs))
        if b is ABSENT:
            if o is ABSENT: result[key] = t
            elif t is ABSENT or o == t: result[key] = o
            else: conflicts[key] = 'insert_insert'
        elif o is ABSENT or t is ABSENT:
            survivor = t if o is ABSENT else o
            if survivor is not ABSENT and survivor != b: conflicts[key] = 'delete_update'
        else:
            merged = []
            for old, left, right in zip(b, o, t):
                if left == right: merged.append(left)
                elif left == old: merged.append(right)
                elif right == old: merged.append(left)
                else: break
            if len(merged) == len(b): result[key] = tuple(merged)
            else: conflicts[key] = 'cell'
    return result, conflicts


def typed_values(state):
    rows = []
    def cell(value, typ):
        # Parse the signed minimum directly in its destination type: SQL's unary minus
        # otherwise casts the positive magnitude first, which exceeds HUGEINT_MAX.
        token = literal(str(value)) if isinstance(value, (int, Decimal)) and not isinstance(value, bool) else literal(value)
        return f'{token}::{typ}'
    for key, payload in sorted(state.items()):
        rows.append('(' + ','.join(cell(v, typ) for v, typ in zip((*key, *payload), TYPES)) + ')')
    return ','.join(rows)


def write_state(catalog, table, state):
    return f'DELETE FROM {catalog}.{table};' + (f'INSERT INTO {catalog}.{table} VALUES {typed_values(state)};' if state else '')


def generated(seed):
    rng = random.Random(seed)
    pools = [
        [None, -(2**127), 2**127-1, -1, 0, 1],
        [None, 0, 1, 2**64-1],
        [None, Decimal('-99999999999999999999999999999.999999999'), Decimal('0'), Decimal('0.000000001'), Decimal('12.345678901')],
        [None, True, False],
        [None, '', "quo'te", 'é', 'e\u0301', '東京', 'line\nbreak'],
    ]
    def row(): return tuple(rng.choice(pool) for pool in pools)
    tables = []
    for _ in range(3):
        base = {(i % 3, f'k{i}'): row() for i in range(80)}
        sides = []
        for _ in range(2):
            side = dict(base)
            for key in list(base):
                roll = rng.random()
                if roll < .12: del side[key]
                elif roll < .65:
                    item = list(side[key]); col = rng.randrange(len(pools)); item[col] = rng.choice(pools[col]); side[key] = tuple(item)
                elif roll < .69: side[(key[0], key[1]+'-moved')] = side.pop(key)
            for i in range(80, 96):
                if rng.random() < .75: side[(i % 3, f'k{i}')] = row()
            sides.append(side)
        tables.append((base, *sides))
    return tables


def setup_states(f, tables):
    ddl, scripts = '', ['', '', '']
    for number, states in enumerate(tables):
        table = f'x{number}'
        ddl += f'CREATE TABLE d.{table}{DDL};'
        for i, catalog in enumerate(('d', 'o', 't')): scripts[i] += write_state(catalog, table, states[i])
    return f.setup(ddl, *scripts)


def assert_no_stage(f):
    assert f.q("SELECT count(*) FROM differential_retention_roots() WHERE reason='native_file' AND starts_with(identity,'merge_stage/')") == 0
    assert f.q('SELECT count(*) FROM differential_merge_status()') == 0


def run_at_gate(f, root, phase, sql, during):
    gate = root / ('gate-' + phase)
    for suffix in ('.entered', '.release'): Path(str(gate)+suffix).unlink(missing_ok=True)
    os.environ['DIFFERENTIAL_MERGE_PHASE_GATE'] = phase + ':' + str(gate)
    connection = f.host.connect()
    answers, errors = [], []
    def work():
        try: answers.append(f.host.query(connection, sql))
        except RuntimeError as error: errors.append(str(error))
    thread = threading.Thread(target=work)
    thread.start()
    try:
        deadline = time.monotonic()+20
        while not Path(str(gate)+'.entered').exists():
            assert thread.is_alive() and time.monotonic()<deadline, errors
            time.sleep(.01)
        during(connection, answers, errors)
    finally:
        Path(str(gate)+'.release').touch()
        thread.join(timeout=30)
        os.environ.pop('DIFFERENTIAL_MERGE_PHASE_GATE', None)
    assert not thread.is_alive(), 'merge did not stop at gate'
    return answers, errors


def spill_process_death(root):
    folder, spill = root/'spill-death-repo', root/'spill-death-temp'
    prefix = (f"LOAD '{EXTENSION}'; SET differential_repository='{folder}'; SET threads=2;"
              f"SET temp_directory='{spill}'; SET max_temp_directory_size='256MB';")
    cli = [DUCKDB_CLI,'-init','/dev/null','-unsigned','-batch','-bail','-json']
    def run(sql):
        result = subprocess.run(cli,input=prefix+sql,text=True,capture_output=True,timeout=90)
        assert result.returncode == 0, (result.stdout,result.stderr)
        return result.stdout
    run("SET memory_limit='128MB'; ATTACH 'differential:spill' AS d;"
        "CREATE TABLE d.x(i INT PRIMARY KEY,a INT,s VARCHAR);"
        "INSERT INTO d.x SELECT i,0,md5(i::VARCHAR) FROM range(100000) r(i); CALL differential_create_snapshot('d');"
        "CALL differential_create_branch('d','o',snapshot=>1); CALL differential_create_branch('d','t',snapshot=>1);"
        "ATTACH 'differential:spill' AS o (BRANCH 'o'); UPDATE o.x SET a=1; CALL differential_create_snapshot('o');"
        "ATTACH 'differential:spill' AS t (BRANCH 't'); UPDATE t.x SET s=md5((i+1)::VARCHAR); CALL differential_create_snapshot('t');")
    merge = "SELECT snapshot_id FROM differential_merge_snapshots('spill',1,2,3,'merged',request_id=>'spill-death');"
    observed = []
    with open(root/'spill-death.stdout','w+') as out, open(root/'spill-death.stderr','w+') as err:
        process = subprocess.Popen(cli,stdin=subprocess.PIPE,stdout=out,stderr=err,text=True)
        process.stdin.write(prefix+"SET memory_limit='32MB'; SET debug_force_external=true;"+merge)
        process.stdin.close()
        try:
            deadline = time.monotonic()+30
            while process.poll() is None and time.monotonic()<deadline:
                observed = []
                for path in spill.glob('*storage*.tmp'):
                    try:
                        if path.stat().st_size > 0:
                            observed.append(path)
                    except FileNotFoundError:
                        pass  # DuckDB can retire a spill file between glob and stat.
                if observed:
                    process.kill(); break
                time.sleep(.002)
            process.wait(timeout=10)
            err.seek(0)
            assert observed and process.returncode < 0, (process.returncode,err.read())
        finally:
            if process.poll() is None: process.kill(); process.wait()
    with sqlite3.connect(folder/'engine.sqlite') as db:
        assert db.execute("SELECT count(*) FROM branches WHERE name='merged'").fetchone()[0] == 0
    result = run("SET memory_limit='128MB'; SET debug_force_external=true;"+merge)
    assert result == run(merge)
    assert json.loads(run("ATTACH 'differential:spill' AS m (BRANCH 'merged'); SELECT count(*) n FROM m.x WHERE a=1 AND s=md5((i+1)::VARCHAR);")) == [{'n':100000}]
    run('CALL differential_cleanup_old_files(max_objects=>10000);')
    # Stock DuckDB's sweep is lazy: a memory-only retry need not initialize its
    # temporary-file manager. The next actual spill in a fresh process sweeps dead
    # owners. Force that path under a tiny disk budget without publishing anything.
    if any(path.exists() for path in observed):
        probe = subprocess.run(cli,input=prefix+"SET memory_limit='32MB'; SET max_temp_directory_size='1KiB'; SET debug_force_external=true;"
            "SELECT * FROM differential_merge_snapshots_preview('spill',1,2,3,conflict_limit=>0);",text=True,capture_output=True,timeout=60)
        assert probe.returncode != 0 and 'max_temp_directory_size' in probe.stderr, (probe.stdout,probe.stderr)
    assert not [path for path in observed if path.exists()], 'native abandoned spill files remain'
    with sqlite3.connect(folder/'engine.sqlite') as db:
        assert db.execute('PRAGMA integrity_check').fetchone()[0] == 'ok'
        assert db.execute("SELECT count(*) FROM files WHERE name LIKE 'merge_stage/%'").fetchone()[0] == 0


def main():
    passed = []
    def ok(label): passed.append(label); print('PASS', label, flush=True)
    with tempfile.TemporaryDirectory(prefix='differential-merge-hardening-') as temp:
        root = Path(temp); repo = root/'repo'
        f = Fixture(repo)
        try:
            for seed in (17, 73, 1024, 65537):
                tables = generated(seed)
                name = setup_states(f, tables)
                expected = [oracle(*states) for states in tables]
                for table, (_, conflicts) in enumerate(expected):
                    source = f"{f.preview(name, ',conflict_limit=>1000')} WHERE table_name='x{table}'"
                    assert f.q(f'SELECT conflicts FROM {source}') == len(conflicts)
                    details = f'(SELECT unnest(conflict_details) AS c FROM {source})'
                    if conflicts:
                        wanted = [(key[0], key[1], kind) for key, kind in conflicts.items()]
                        assert f.q(f'SELECT count(*) FROM (SELECT c.key[1].integer_value,c.key[2].text_value,c.kind FROM {details} EXCEPT SELECT * FROM (VALUES {values(wanted)}))') == 0
                f.error(f'SELECT * FROM {f.merge(name)}', 'merge_conflict')
                assert f.q(f"SELECT count(*) FROM differential_branches('{name}')") == 3
                cleaned = []
                for states, (_, conflicts) in zip(tables, expected):
                    cleaned.append(tuple({k: v for k, v in side.items() if k not in conflicts} for side in states))
                clean = setup_states(f, cleaned)
                assert f.q(f"SELECT count(*) FROM {f.preview(clean, ',validate=>true')} WHERE validation_state!='valid'") == 0
                f.q(f'SELECT * FROM {f.merge(clean)}'); f.attach(clean)
                for table, states in enumerate(cleaned):
                    wanted, conflicts = oracle(*states); assert not conflicts
                    assert f.q(f'SELECT count(*) FROM m.x{table}') == len(wanted)
                    assert f.q(f'SELECT count(*) FROM (SELECT * FROM m.x{table} EXCEPT SELECT * FROM (VALUES {typed_values(wanted)}))') == 0
                f.q('DETACH m'); assert_no_stage(f)
            ok('four seeded three-table state oracles: composite/string keys, PK moves, integer extrema, DECIMAL(38,9), Unicode, NULL and repeated full validation')

            name = f.setup('CREATE TABLE d.x(i INT PRIMARY KEY,v INT UNIQUE);', 'INSERT INTO d.x VALUES(1,10),(2,20);',
                           'UPDATE o.x SET v=30 WHERE i=1;', 'UPDATE t.x SET v=30 WHERE i=2;')
            operations = f.q('SELECT count(*) FROM differential_operations()')
            assert f.q(f"SELECT count(*) FROM {f.preview(name, ',validate=>true')} WHERE validation_state='constraint_violation' AND validation_error LIKE '%merge_constraint%'") == 1
            assert f.q('SELECT count(*) FROM differential_operations()') == operations
            assert f.q(f"SELECT count(*) FROM differential_snapshots('{name}')") == 3
            assert_no_stage(f)
            ok('full validation distinguishes final UNIQUE violation and leaves branch/snapshot/operation history unchanged')

            big = f.setup('CREATE TABLE d.x(i INT PRIMARY KEY,s VARCHAR); CREATE TABLE d.y(i INT PRIMARY KEY,s VARCHAR);',
                          "INSERT INTO d.x VALUES(1,repeat('é',100000)); INSERT INTO d.y VALUES(1,'base');",
                          "UPDATE o.x SET s=repeat('o',200000); UPDATE o.y SET s='ours';",
                          "UPDATE t.x SET s=repeat('t',200000); UPDATE t.y SET s='theirs';")
            page = f.preview(big, ',conflict_bytes=>4096')
            assert f.q(f'SELECT sum(len(conflict_details)) FROM {page}') == 0
            assert f.q(f'SELECT min(next_conflict_offset) FROM {page}') == 0
            assert f.q(f'SELECT count(*) FROM {page} WHERE details_truncated') == 2
            page = f.preview(big, ',conflict_limit=>1,conflict_bytes=>1048576')
            assert f.q(f'SELECT sum(len(conflict_details)) FROM {page}') == 1
            assert f.q(f'SELECT min(next_conflict_offset) FROM {page}') == 1
            assert f.q(f'SELECT sum(detail_bytes) FROM {page}') < 1048576
            assert f.q(f"SELECT strlen(conflict_details[1].base_values[2].text_value) FROM {page} WHERE table_name='x'") == 200000
            assert f.q(f"SELECT sum(len(conflict_details)) FROM {f.preview(big, ',conflict_offset=>1,conflict_bytes=>4096')}") == 1
            ok('UTF-8 conflict payload byte budget is enforced before C++ detail retrieval; paging never skips oversized rows')

            valid = f.setup('CREATE TABLE d.x(i INT PRIMARY KEY,a INT,b INT);', 'INSERT INTO d.x VALUES(1,0,0);',
                            'UPDATE o.x SET a=1;', 'UPDATE t.x SET b=2;')
            for option in ('max_input_rows=>1', 'max_input_bytes=>1', 'max_stage_write_bytes=>1'):
                f.error(f"SELECT * FROM differential_merge_snapshots('{valid}',1,2,3,'limited',request_id=>'limits',{option})", 'merge_limit')
                assert f.q(f"SELECT count(*) FROM differential_branches('{valid}') WHERE branch_name='limited'") == 0
                assert_no_stage(f)
            result = f.q(f"SELECT snapshot_id FROM {f.merge(valid, 'limited', 'limits')}")
            assert result == 4
            f.q('SET differential_merge_max_concurrent=1')
            def observe(*_):
                assert f.q("SELECT count(*) FROM differential_merge_status() WHERE phase='pinned' AND input_bytes>0") == 1
                f.error(f'SELECT * FROM {f.preview(valid)}', 'merge_busy')
                assert f.q(f"SELECT snapshot_id FROM {f.merge(valid,'limited','limits')}") == 4
            answers, errors = run_at_gate(f, root, 'pinned', f'SELECT snapshot_id FROM {f.merge(valid,"observed","observed")}', observe)
            assert answers and not errors, errors
            f.q('SET differential_merge_max_concurrent=2'); assert_no_stage(f)
            ok('input/staging limits fail before publication; resource-adjusted retries, admission, live status and completed replay under contention work')

            for phase in ('comparison', 'changes_detected', 'table_rebuild', 'table_inserted', 'checkpointing'):
                answers, errors = run_at_gate(f, root, phase,
                    f'SELECT * FROM {f.merge(valid, "cancel_"+phase, "cancel_"+phase)}',
                    lambda connection, *_: f.host.lib.duckdb_interrupt(connection))
                assert errors and not answers
                assert f.q(f"SELECT count(*) FROM differential_branches('{valid}') WHERE branch_name='cancel_{phase}'") == 0
                assert_no_stage(f)
                f.q(f'SELECT * FROM {f.merge(valid,"cancel_"+phase,"cancel_"+phase)}')
            def deadline(*_): time.sleep(.6)
            answers, errors = run_at_gate(f, root, 'comparison',
                f"SELECT * FROM differential_merge_snapshots('{valid}',1,2,3,'deadline',request_id=>'deadline',timeout_ms=>500)", deadline)
            assert errors and 'merge_timeout' in errors[0], errors
            assert_no_stage(f)
            f.q(f'SELECT * FROM {f.merge(valid,"deadline","deadline")}')
            ok('cancellation at five execution phases and deadlines clean up staging and allow same-request retry')

            for fault in ('write', 'sync'):
                os.environ['DIFFERENTIAL_MERGE_IO_FAULT'] = fault
                try: f.error(f'SELECT * FROM {f.merge(valid, "io_"+fault, "io_"+fault)}', 'merge_io')
                finally: os.environ.pop('DIFFERENTIAL_MERGE_IO_FAULT')
                assert_no_stage(f)
                f.q(f'SELECT * FROM {f.merge(valid,"io_"+fault,"io_"+fault)}')
            ok('injected native staging write/sync failures leave inputs readable and retry without a completed partial destination')

            retained = f.setup('CREATE TABLE d.x(i INT PRIMARY KEY,a INT,b INT);', 'INSERT INTO d.x VALUES(1,0,0);',
                               'UPDATE o.x SET a=1;', 'UPDATE t.x SET b=2;')
            def retire(*_):
                f.q(f"CALL differential_expire_snapshots('{retained}',versions=>[1,2,3]);")
                f.q('CALL differential_cleanup_old_files(max_objects=>10000); CALL differential_compact(max_pages=>100);')
            answers, errors = run_at_gate(f, root, 'pinned', f'SELECT snapshot_id FROM {f.merge(retained,"retained","retained")}', retire)
            assert answers == [4] and not errors, errors
            f.attach(retained, 'retained'); assert f.q('SELECT a+b FROM m.x') == 3; f.q('DETACH m')
            assert f.q(f'SELECT snapshot_id FROM {f.merge(retained,"retained","retained")}') == 4
            assert_no_stage(f)
            ok('snapshot retirement, GC and compaction during a pinned merge preserve inputs through publication and completed replay')

            # A competing request can claim a destination after comparison has completed.
            def publish_competitor(*_):
                os.environ.pop('DIFFERENTIAL_MERGE_PHASE_GATE')
                f.q(f'SELECT * FROM {f.merge(valid,"race","race-winner")}')
            answers, errors = run_at_gate(f, root, 'staged',
                f'SELECT * FROM {f.merge(valid,"race","race-loser")}', publish_competitor)
            assert not answers and errors and 'name_conflict' in errors[0], errors
            assert f.q(f"SELECT count(*) FROM differential_branches('{valid}') WHERE branch_name='race'") == 1
            assert_no_stage(f)
            ok('different requests racing for one destination publish exactly one branch; the losing staged root is cleaned up')

            spill = f.setup('CREATE TABLE d.x(i INT PRIMARY KEY,a INT,s VARCHAR);',
                "INSERT INTO d.x SELECT i,0,md5(i::VARCHAR) FROM range(100000) r(i);",
                'UPDATE o.x SET a=1;', "UPDATE t.x SET s=md5((i+1)::VARCHAR);")
            f.q(f"SET temp_directory='{root/'bounded-spill'}'; SET max_temp_directory_size='1KiB'; SET memory_limit='32MB'; SET debug_force_external=true;")
            f.error(f'SELECT * FROM {f.merge(spill,"spill","spill")}', 'max_temp_directory_size')
            assert_no_stage(f)
            f.q("SET max_temp_directory_size='256MB'; SET memory_limit='128MB';")
            f.q(f'SELECT * FROM {f.merge(spill,"spill","spill")}')
            f.q("SET debug_force_external=false; SET memory_limit='128MB';")
            ok('native temporary-storage exhaustion fails safely and the same request succeeds with an adequate spill budget')

            # Preserve durable request semantics independently of the algorithm tag.
            f.error(f"SELECT * FROM differential_merge_snapshots('{valid}',1,2,3,'legacy-pending',request_id=>'legacy-pending',max_input_rows=>1)", 'merge_limit')
        finally:
            f.host.close()

        with sqlite3.connect(repo/'engine.sqlite') as db:
            def retag(request):
                payload = db.execute('SELECT payload FROM operations WHERE request=?', (request,)).fetchone()[0]
                suffix = '15:logical-cell-v1'
                assert payload.endswith(suffix), payload
                replacement = 'unavailable-algorithm'
                db.execute('UPDATE operations SET payload=? WHERE request=?',
                           (payload[:-len(suffix)]+str(len(replacement))+':'+replacement, request))
            retag('limits'); retag('legacy-pending')
        f = Fixture(repo)
        try:
            assert f.q(f"SELECT snapshot_id FROM {f.merge(valid,'limited','limits')}") == 4
            f.error(f'SELECT * FROM {f.merge(valid,"legacy-pending","legacy-pending")}', 'Pending merge algorithm')
            f.error(f'SELECT * FROM {f.merge(valid,"wrong","limits")}', 'different payload')
        finally: f.host.close()
        ok('completed requests replay across algorithm tags; unknown pending algorithms and changed arguments are refused')

        expected = root/'expected.duckdb'
        subprocess.run([DUCKDB_CLI,'-init','/dev/null','-batch','-bail',str(expected),'-c',
                        'CREATE TABLE x(i INT PRIMARY KEY,a INT,b INT); INSERT INTO x VALUES(1,1,2);'],
                       check=True,capture_output=True)
        import sys
        report = root/'pilot.json'
        command = [sys.executable,str(Path(__file__).resolve().parents[2]/'scripts/merge_pilot.py'),
            '--repository',str(repo),'--database',valid,'--base','1','--ours','2','--theirs','3',
            '--destination','pilot','--request-id','pilot','--expected',str(expected),'--report',str(report),
            '--duckdb',str(DUCKDB_CLI),'--extension',str(EXTENSION)]
        identities = []
        for _ in range(2):
            probe = subprocess.run(command,text=True,capture_output=True,timeout=60)
            assert probe.returncode == 0, (probe.stdout,probe.stderr,report.read_text())
            identities.append(json.loads(report.read_text())['merge']['snapshot_id'])
        assert identities[0] == identities[1]
        subprocess.run([DUCKDB_CLI,'-init','/dev/null','-batch','-bail',str(expected),'-c','UPDATE x SET b=999;'],check=True,capture_output=True)
        probe = subprocess.run(command,text=True,capture_output=True,timeout=60)
        failed = json.loads(report.read_text())
        assert probe.returncode == 1 and failed['status'] == 'failed' and failed['error_code'] == 'data_mismatch'
        assert not failed['verification']['complete'] and failed['verification']['relations'][0]['missing'] == 1
        ok('pilot helper checks an independent native expected result, replays one result and detects a deliberately wrong expectation')

        spill_process_death(root)
        ok('SIGKILL after observing nonempty native spill files: same-request recovery and native abandoned-spill cleanup')

        # Recover through the normal public SQL path after process death in comparison,
        # materialization and application, including forced external query execution.
        for point in ('pinned', 'changes_detected', 'table_rebuild', 'table_inserted', 'checkpointing'):
            folder = root / ('crash-'+point)
            prefix = (f"LOAD '{EXTENSION}'; SET differential_repository='{folder}'; SET threads=2; SET memory_limit='64MB'; "
                      f"SET temp_directory='{folder}-spill'; SET max_temp_directory_size='64MB';")
            def run(sql, crash=None):
                env = dict(os.environ)
                if crash: env['DIFFERENTIAL_MERGE_CRASH'] = crash
                p = subprocess.run([DUCKDB_CLI,'-init','/dev/null','-unsigned','-batch','-bail','-json'],
                                   input=prefix+sql,text=True,capture_output=True,env=env,timeout=60)
                assert p.returncode == (86 if crash else 0), (point,p.returncode,p.stdout,p.stderr)
                return p.stdout
            run("ATTACH 'differential:crash' AS d; CREATE TABLE d.x(i INT PRIMARY KEY,a INT,b VARCHAR);"
                "INSERT INTO d.x SELECT i,0,repeat('base',25) FROM range(30000) r(i); CALL differential_create_snapshot('d');"
                "CALL differential_create_branch('d','o',snapshot=>1); CALL differential_create_branch('d','t',snapshot=>1);"
                "ATTACH 'differential:crash' AS o (BRANCH 'o'); UPDATE o.x SET a=1; CALL differential_create_snapshot('o');"
                "ATTACH 'differential:crash' AS t (BRANCH 't'); UPDATE t.x SET b='new'; CALL differential_create_snapshot('t');")
            merge = "SET debug_force_external=true; SELECT snapshot_id FROM differential_merge_snapshots('crash',1,2,3,'merged',request_id=>'crash');"
            run(merge, point)
            with sqlite3.connect(folder/'engine.sqlite') as db:
                assert db.execute("SELECT count(*) FROM branches WHERE name='merged'").fetchone()[0] == 0
            result = run(merge); assert result == run(merge)
            assert json.loads(run("ATTACH 'differential:crash' AS m (BRANCH 'merged'); SELECT count(*) n FROM m.x WHERE a=1 AND b='new';")) == [{'n':30000}]
            run('CALL differential_cleanup_old_files(max_objects=>10000);')
            with sqlite3.connect(folder/'engine.sqlite') as db:
                assert db.execute('PRAGMA integrity_check').fetchone()[0] == 'ok'
                assert db.execute("SELECT count(*) FROM files WHERE name LIKE 'merge_stage/%'").fetchone()[0] == 0
            # Each disposable case is removed immediately to bound disk use.
            import shutil
            shutil.rmtree(folder)
            shutil.rmtree(Path(str(folder)+'-spill'), ignore_errors=True)
        ok('five additional process-death phases recover exactly once with forced external execution and stale-stage GC')

    evidence_path('merge-hardening.json').write_text(json.dumps({'passed':passed,'random_seeds':[17,73,1024,65537],
        'scope':'disposable local storage on this host; injected faults/process death, not physical power loss; 30k-row spill crash cases'},indent=2)+'\n')


if __name__ == '__main__': main()
