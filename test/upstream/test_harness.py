#!/usr/bin/env python3
"""Exercise the bridge with real SQLLogicTest load/restart/connection commands."""
import importlib.util
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('upstream', ROOT / 'scripts/test_upstream.py')
upstream = importlib.util.module_from_spec(spec)
spec.loader.exec_module(upstream)
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--build-dir', type=Path, default=ROOT / 'build/integration')
parser.add_argument('--bridge', type=Path, help='test-only bridge artifact override')
args = parser.parse_args()
build = args.build_dir.resolve()
runner = build / 'test/unittest'
extension = build / 'extension/differential/differential.duckdb_extension'
bridge = (args.bridge or build / 'extension/upstream_bridge/upstream_bridge.duckdb_extension').resolve()
evidence = upstream.evidence_path('upstream-harness-' + str(time.time_ns()))
evidence.mkdir()

SCRIPT = """query I
SELECT current_setting('secret_directory') = '__TEST_DIR__/test_secret_dir'
----
true

query I
SELECT count(*) FROM duckdb_databases() WHERE NOT internal
----
1

query T
SELECT current_database()
----
memory

query I
SELECT starts_with(path, 'diff://') FROM duckdb_databases() WHERE NOT internal
----
{differential}

statement ok
CREATE TABLE initial(i INTEGER)

load __TEST_DIR__/first.db

query I
SELECT count(*) FROM duckdb_tables() WHERE table_name='initial'
----
0

statement ok
CREATE TABLE t(i INTEGER)

statement ok
INSERT INTO t VALUES (42)

query I con1
SELECT * FROM t
----
42

statement ok
CREATE TABLE unique_values(i INTEGER PRIMARY KEY)

concurrentloop thread 0 2

statement ok
BEGIN

statement error
INSERT INTO unique_values VALUES (1), (1)
----
duplicate key

statement ok
ROLLBACK

endloop

reconnect

query T
SELECT current_database()
----
first

restart

query I
SELECT * FROM t
----
42

{snapshot}

load __TEST_DIR__/first.db readonly

query I
SELECT * FROM t
----
42

statement error
INSERT INTO t VALUES (43)
----
read-only

load __TEST_DIR__/first.db

query I
SELECT count(*) FROM duckdb_tables() WHERE table_name='t'
----
0

statement ok
CREATE TABLE t(i INTEGER)

load __TEST_DIR__/second.db

statement ok
CREATE TABLE t(i INTEGER)

load

query I
SELECT count(*) FROM duckdb_tables() WHERE table_name='t'
----
0
"""

with tempfile.TemporaryDirectory(prefix='upstream-bridge-test-') as tmp:
    root = Path(tmp)
    upstream.DUCKDB = root
    tests = root / 'test/sql'
    tests.mkdir(parents=True)
    for mode in ('native', 'differential'):
        snapshot = "statement ok\nCALL differential_create_snapshot(current_database())" if mode == 'differential' else ''
        (tests / 'bridge.test').write_text(SCRIPT.format(differential='true' if mode == 'differential' else 'false',
                                                       snapshot=snapshot))
        result, log = upstream.run_test(runner, extension, bridge, 'test/sql/bridge.test', mode, None, evidence)
        assert result['status'] == 'passed', log
        print('PASS', mode, 'catalog identity, load reset, restart, readonly and named connections')
    for source, expected in [('mode skip\n\nstatement ok\nSELECT 42\n', 'not_executed'),
                             ('# Empty source is not execution\n', 'not_executed'),
                             ('load __TEST_DIR__/empty.db\n\nrestart\n\nrestart\n', 'passed'),
                             ('mode skip\n\nstatement ok\nSELECT 404\n\nmode unskip\n\n'
                              'query I\nSELECT 1 WHERE false\n----\n\n', 'passed'),
                             ('query I\nSELECT 1\n----\n2\n', 'failed'),
                             ("query I\nSELECT 1 WHERE false AND '__TEST_DIR__' IS NOT NULL\n----\n\n", 'passed')]:
        name = 'test/sql/result_' + expected + '.test'
        (root / name).write_text(source)
        result, log = upstream.run_test(runner, extension, bridge, name, 'differential', None, evidence)
        assert result['status'] == expected, log
        print('PASS result classified as', expected)
    (root / 'data').mkdir()
    fixture = root / 'data/native.db'
    fixture.write_bytes(b'original native fixture')
    wal = root / 'data/native.db.wal'
    wal.write_bytes(b'original WAL fixture')
    readonly_fixture = root / 'data/readonly.db'
    readonly_fixture.write_bytes(b'original readonly fixture')
    (tests / 'fixture.test').write_text("statement ok\nATTACH '{DATA_DIR}/native.db' AS n\n\n"
                                       "load data/${version}.db readonly\n")
    work = root / 'copy-test'
    work.mkdir()
    tree = upstream.working_tree(work, 'test/sql/fixture.test')
    (tree / 'data/native.db').write_bytes(b'modified copy')
    (tree / 'data/native.db.wal').write_bytes(b'modified WAL copy')
    (tree / 'data/readonly.db').write_bytes(b'modified copy')
    assert fixture.read_bytes() == b'original native fixture'
    assert wal.read_bytes() == b'original WAL fixture'
    assert readonly_fixture.read_bytes() == b'original readonly fixture'
    print('PASS database fixture writes stay in disposable copies')

    # Verify existing binary fixtures really use Differential after byte import,
    # while their source bytes and read-only behavior remain intact.
    native_fixture = root / 'data/binary.db'
    subprocess.run([str(build / 'duckdb'), '-init', '/dev/null', '-batch', str(native_fixture),
                    '-c', 'CREATE TABLE fixture_values AS SELECT 42 AS i; CHECKPOINT;'],
                   check=True, capture_output=True, text=True)
    original_hash = hashlib.sha256(native_fixture.read_bytes()).hexdigest()
    for mode in ('native', 'differential'):
        (tests / 'binary.test').write_text('load data/binary.db readonly\n\n'
            'query I\nSELECT i FROM fixture_values\n----\n42\n\n'
            "query I\nSELECT starts_with(path, 'diff://') FROM duckdb_databases() WHERE NOT internal\n----\n" +
            ('true' if mode == 'differential' else 'false') + '\n\n'
            'statement error\nINSERT INTO fixture_values VALUES (43)\n----\nread-only\n')
        result, log = upstream.run_test(runner, extension, bridge, 'test/sql/binary.test', mode, None, evidence)
        assert result['status'] == 'passed', log
        assert hashlib.sha256(native_fixture.read_bytes()).hexdigest() == original_hash
        print('PASS', mode, 'binary fixture preserves source bytes and read-only semantics')
        (tests / 'binary_lifecycle.test').write_text('load data/binary.db readonly\n\nrestart\n\nrestart\n')
        result, log = upstream.run_test(runner, extension, bridge, 'test/sql/binary_lifecycle.test',
                                        mode, None, evidence)
        assert result['status'] == 'passed', log
        assert hashlib.sha256(native_fixture.read_bytes()).hexdigest() == original_hash
        print('PASS', mode, 'binary fixture lifecycle without SQL assertions')
    mutable_fixture = root / 'mutable.db'
    mutable_fixture.write_bytes(native_fixture.read_bytes())
    result = subprocess.run([str(build / 'duckdb'), '-init', '/dev/null', '-unsigned', '-batch',
                             str(mutable_fixture), '-cmd', f'LOAD {upstream.quote(bridge)};', '-c',
                             'PRAGMA upstream_bridge('
                             f"'differential', {upstream.quote(root)}, 0);"], capture_output=True, text=True)
    log = result.stdout + result.stderr
    (evidence / 'mutable-fixture.log').write_text(log)
    assert result.returncode != 0 and 'fixtures to be loaded read-only' in ' '.join(log.split()), log
    print('PASS mutable native fixtures cannot silently lose writes on reimport')

    # Verify the actual command-line audit rejects a now-passing exclusion.
    suite = json.loads((ROOT / 'test/upstream/suite.json').read_text())
    suite['skips'] = [{
        'path': 'test/sql/storage/test_empty_table.test',
        'category': 'audit_test', 'reason': 'Deliberately stale exclusion for the harness regression test.',
        'expected': {'native': {'status': 'passed'},
                     'differential': {'status': 'failed', 'message': 'deliberately nonexistent failure'}},
    }]
    config = root / 'stale.json'
    config.write_text(json.dumps(suite))
    command = [sys.executable, str(ROOT / 'scripts/test_upstream.py'), '--build-dir', str(build),
               '--bridge', str(bridge), '--suite', str(config), '--audit-skips']
    result = subprocess.run(command, capture_output=True, text=True)
    (evidence / 'stale-audit.log').write_text(result.stdout + result.stderr)
    assert result.returncode == 1 and 'STALE_SKIP' in result.stdout, result.stdout + result.stderr
    print('PASS unexpected exclusion passes fail the audit')

    # A source audit of owner-excluded disk workloads must never launch DuckDB.
    policy_suite = json.loads((ROOT / 'test/upstream/suite.json').read_text())
    policy_suite['skips'] = [x for x in policy_suite['skips'] if x.get('execution') == 'never']
    assert policy_suite['skips'], 'missing reviewed disk-space exclusions'
    config.write_text(json.dumps(policy_suite))
    fake_build = root / 'fake-build'
    for relative in ('test/unittest', 'extension/differential/differential.duckdb_extension',
                     'extension/upstream_bridge/upstream_bridge.duckdb_extension'):
        artifact = fake_build / relative
        artifact.parent.mkdir(parents=True, exist_ok=True)
        artifact.write_text('The policy audit must not execute or load this artifact.\n')
    command = [sys.executable, str(ROOT / 'scripts/test_upstream.py'), '--build-dir', str(fake_build),
               '--suite', str(config), '--audit-skips']
    result = subprocess.run(command, capture_output=True, text=True)
    (evidence / 'disk-policy-audit.log').write_text(result.stdout + result.stderr)
    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout.count('CONFIRMED_POLICY_EXCLUSION') == len(policy_suite['skips']), result.stdout
    print('PASS disk-policy audits validate source without executing workloads')

    policy_suite['skips'][0]['source_sha256'] = '0' * 64
    config.write_text(json.dumps(policy_suite))
    result = subprocess.run(command, capture_output=True, text=True)
    assert result.returncode != 0 and 'review the source and disk-space justification' in result.stderr
    print('PASS changed disk-policy source requires review')

    # An interrupted runner can print a known failure before being killed; its
    # partial output must never count as a reproduced compatibility exclusion.
    interrupted_runner = fake_build / 'test/unittest'
    interrupted_runner.write_text('#!/usr/bin/env python3\nimport os, signal\n'
                                  'print("interrupted diagnostic", flush=True)\n'
                                  'os.kill(os.getpid(), signal.SIGTERM)\n')
    interrupted_runner.chmod(0o700)
    suite['skips'][0]['expected'] = {
        mode: {'status': 'failed', 'message': 'interrupted diagnostic'}
        for mode in ('native', 'differential')
    }
    config.write_text(json.dumps(suite))
    result = subprocess.run(command, capture_output=True, text=True)
    (evidence / 'interrupted-audit.log').write_text(result.stdout + result.stderr)
    assert result.returncode == 1 and 'STALE_SKIP' in result.stdout, result.stdout + result.stderr
    print('PASS interrupted runners cannot reproduce exclusions')

print('Evidence:', evidence)
