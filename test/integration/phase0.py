#!/usr/bin/env python3
"""Stock-binary integration probes; no Python DuckDB package or custom host."""
import argparse
import json
import os
from pathlib import Path
import selectors
import shutil
import signal
import subprocess
import tempfile
import time

ROOT = Path(__file__).resolve().parents[2]
import sys as _sys
_sys.path.insert(0,str(ROOT/'test/integration'))
from paths import DUCKDB_CLI, EXTENSION, DUCKDB_LIBRARY, BUILD_DIR

parser = argparse.ArgumentParser()
parser.add_argument('--duckdb', default=os.environ.get('DUCKDB_BINARY', 'duckdb'))
parser.add_argument('--output', default=str(ROOT / 'build/phase0-results.json'))
parser.add_argument('--durable', action='store_true')
args = parser.parse_args()
BINARY = shutil.which(args.duckdb) or str(Path(args.duckdb).resolve())
EXT = EXTENSION
results = []

def quote(s):
    return "'" + str(s).replace("'", "''") + "'"

def run(sql, directory, extra=None, ok=True):
    env = dict(os.environ,DIFFERENTIAL_ENABLE_PROBES='1', DIFFERENTIAL_PROBE_ROOT=str(directory), DIFFERENTIAL_PROBE_TRACE=str(directory/'trace.tsv'))
    if args.durable:
        env.pop('DIFFERENTIAL_PROBE_ROOT', None)
        env['DIFFERENTIAL_STORE_ROOT']=str(directory)
    env.update(extra or {})
    r = subprocess.run([BINARY, '-init', '/dev/null', '-unsigned', '-batch', '-csv'],
                       input=f'LOAD {quote(EXT)};\n' + sql, text=True, capture_output=True, env=env, timeout=30)
    if ok and r.returncode:
        raise AssertionError((r.returncode, r.stdout, r.stderr, sql))
    return r

def passed(name, detail):
    results.append({'probe':name, 'status':'passed', 'detail':detail})
    print('PASS', name, flush=True)

ATTACH = "ATTACH 'diff://probe' AS d (TYPE differential); USE d;"
FIXTURE = """
CREATE TABLE t(id INTEGER PRIMARY KEY, val VARCHAR);
INSERT INTO t VALUES (1,'a'),(2,'b');
CREATE TABLE ctas AS SELECT * FROM t;
BEGIN; INSERT INTO t VALUES (3,'c'); UPDATE t SET val='A' WHERE id=1; DELETE FROM t WHERE id=2; COMMIT;
BEGIN; INSERT INTO t VALUES (99,'rollback'); ALTER TABLE t ADD COLUMN uncommitted INT; ROLLBACK;
INSERT INTO t VALUES(1,'upsert') ON CONFLICT(id) DO UPDATE SET val=excluded.val;
MERGE INTO t USING (VALUES (3,'merge'),(4,'new')) AS s(id,val) ON t.id=s.id
WHEN MATCHED THEN UPDATE SET val=s.val WHEN NOT MATCHED THEN INSERT VALUES(s.id,s.val);
ALTER TABLE t ADD COLUMN flag INTEGER DEFAULT 7;
CREATE VIEW v AS SELECT id,val FROM t;
CREATE INDEX idx ON t(val);
CREATE SEQUENCE seq START 10;
CREATE TABLE defaults(id BIGINT DEFAULT nextval('seq'), value VARCHAR);
INSERT INTO defaults(value) VALUES('first');
CHECKPOINT d;
"""
REPORT = """
SELECT * FROM t ORDER BY id;
SELECT * FROM v ORDER BY id;
SELECT * FROM defaults ORDER BY id;
SELECT table_name,column_name,data_type FROM information_schema.columns
 WHERE table_catalog='d' ORDER BY table_name,ordinal_position;
SELECT index_name,is_unique,expressions FROM duckdb_indexes() WHERE database_name='d';
SELECT sequence_name,start_value,increment_by,last_value FROM duckdb_sequences() WHERE database_name='d';
"""

with tempfile.TemporaryDirectory(prefix='differential-phase0-') as tmp:
    base = Path(tmp)
    r = run('PRAGMA version; PRAGMA platform;', base)
    assert 'v1.5.6,069cc9f9b5' in r.stdout, r.stdout
    passed('stock-load', r.stdout.strip())
    # Both native routing and the native-storage alias must work.
    run("ATTACH 'diff://routing' AS d; CREATE TABLE d.t AS SELECT 1 AS i; CHECKPOINT d; DETACH d;", base)
    passed('filesystem-only-routing', 'native ATTACH without TYPE differential')
    native = base/'native.duckdb'
    a = run(f'ATTACH {quote(native)} AS d; USE d;' + FIXTURE + REPORT, base)
    b = run(ATTACH + FIXTURE + REPORT, base)
    assert a.stdout == b.stdout, (a.stdout, b.stdout)
    a = run(f'ATTACH {quote(native)} AS d; USE d;' + REPORT, base)
    b = run(ATTACH + REPORT, base)
    assert a.stdout == b.stdout
    passed('native-sql-parity-reopen', 'CTAS/DDL/DML/MERGE/UPSERT/ALTER/view/index/sequence/commit/rollback; fresh process')
    for prefix in (f'ATTACH {quote(native)} AS d; USE d;', ATTACH):
        r = run(prefix + "INSERT INTO t(id,val) VALUES(1,'duplicate');", base, ok=False)
        assert r.returncode and 'Duplicate key' in r.stderr, r.stderr
    passed('native-error-parity', 'duplicate primary key rejected on both paths')
    trace = base/'trace.tsv'
    trace.write_text('')
    r = run(ATTACH + "EXPLAIN CALL differential_probe_checkpoint('d'); PREPARE cp AS SELECT * FROM differential_probe_checkpoint('d');", base)
    assert 'control.execute' not in trace.read_text()
    trace.write_text('')
    run(ATTACH + "PREPARE cp AS SELECT * FROM differential_probe_checkpoint('d'); EXECUTE cp; EXECUTE cp; EXPLAIN ANALYZE CALL differential_probe_checkpoint('d');", base)
    assert trace.read_text().count('control.execute') == 3
    r = run(ATTACH + "BEGIN; CALL differential_probe_checkpoint('d'); ROLLBACK;", base, ok=False)
    assert r.returncode and 'Management requires autocommit' in r.stderr
    passed('control-execution', 'plain EXPLAIN/PREPARE no execution; EXECUTE twice plus EXPLAIN ANALYZE exactly three executions; explicit transaction rejected')
    # Capture is deliberately a whole-file prototype copy, not production snapshots.
    capture = run(ATTACH + "CALL differential_probe_checkpoint('d');", base, {'DIFFERENTIAL_PROBE_CAPTURE':'1'})
    snapshot_uri = 'diff://roots/' + capture.stdout.splitlines()[1].split(',')[1] if args.durable else 'diff://captured.snapshot'
    before = run(f"ATTACH '{snapshot_uri}' AS d (TYPE differential, READ_ONLY); USE d;" + REPORT, base)
    run(ATTACH + f"ATTACH '{snapshot_uri}' AS old (TYPE differential, READ_ONLY); ALTER TABLE t ADD COLUMN newer INT; DELETE FROM t; CHECKPOINT d; SELECT CASE WHEN count(*)=3 THEN 1 ELSE error('reader changed') END FROM old.t;", base)
    after = run(f"ATTACH '{snapshot_uri}' AS d (TYPE differential, READ_ONLY); USE d;" + REPORT, base)
    assert before.stdout == after.stdout
    r = run(f"ATTACH '{snapshot_uri}' AS s (TYPE differential);", base, ok=False)
    assert r.returncode and 'read-only' in r.stderr
    passed('immutable-reader-prototype', 'fixed read-only capture survives writer schema/data change and fresh process')
    # Hold a writer open across a second independent CLI and kill after acknowledgement.
    env = dict(os.environ,DIFFERENTIAL_ENABLE_PROBES='1', DIFFERENTIAL_PROBE_ROOT=str(base), DIFFERENTIAL_PROBE_TRACE=str(trace))
    if args.durable:
        env.pop('DIFFERENTIAL_PROBE_ROOT',None)
        env['DIFFERENTIAL_STORE_ROOT']=str(base)
    proc = subprocess.Popen([BINARY,'-init','/dev/null','-unsigned','-batch','-csv'], stdin=subprocess.PIPE,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env, bufsize=1)
    try:
        proc.stdin.write(f"LOAD {quote(EXT)}; {ATTACH} INSERT INTO t VALUES(77,'durable',7,NULL); SELECT 'ACK';\n")
        proc.stdin.flush()
        # CLI stdout is block-buffered on pipes. .output /dev/stdout closes/flushed output.
        proc.stdin.write('.output /dev/stdout\n'); proc.stdin.flush()
        selector = selectors.DefaultSelector(); selector.register(proc.stdout, selectors.EVENT_READ)
        if not selector.select(timeout=10): raise AssertionError('writer acknowledgement timeout')
        ack = proc.stdout.readline()
        assert 'ACK' in ack, ack
        r = run(ATTACH, base, ok=False)
        assert r.returncode and ('lock' in r.stderr.lower()), r.stderr
        proc.kill(); proc.wait(timeout=5)
    finally:
        if proc.poll() is None: proc.kill(); proc.wait(timeout=5)
    r = run(ATTACH + 'SELECT count(*) FROM t WHERE id=77;', base)
    assert r.stdout.strip().endswith('1'), r.stdout
    passed('writer-exclusion-and-kill', 'second process rejected; SIGKILL after acknowledged insert; committed row recovered')
    # Fail after WAL durability and before selected checkpoint transitions.
    for point in ['sync.end:diff://probe.wal:1', 'pwrite.end:diff://probe:1', 'sync.end:diff://probe:1', 'try_remove:diff://probe.wal:1']:
        case = base / ('crash-' + str(len(results))); case.mkdir()
        run(ATTACH + 'CREATE TABLE keep AS SELECT 42 AS i; CHECKPOINT d;', case)
        r = run(ATTACH + 'INSERT INTO keep VALUES(43); CHECKPOINT d;', case, {'DIFFERENTIAL_PROBE_CRASH':point}, ok=False)
        assert r.returncode == 86, (point,r.returncode,r.stderr)
        r = run(ATTACH + 'SELECT min(i),max(i),count(*) FROM keep;', case)
        assert '42,43,2' in r.stdout, (point,r.stdout,r.stderr)
        passed('crash-' + point, 'previous state and WAL-synced commit recovered')
    # The pass-through must not silently route malformed managed paths to local disk.
    r = run("ATTACH 'diff://../escape' AS bad;", base, ok=False)
    assert r.returncode and ('Invalid differential probe URI' in r.stderr or 'Invalid differential URI' in r.stderr)
    passed('malformed-uri', 'traversal rejected')
    evidence = ROOT / 'build/phase0-trace.tsv'; shutil.copyfile(trace, evidence)

Path(args.output).write_text(json.dumps({'binary':BINARY, 'backend':'durable_sqlite' if args.durable else 'pass_through', 'results':results}, indent=2)+'\n')
print(f'{len(results)} probes passed; results: {args.output}')
