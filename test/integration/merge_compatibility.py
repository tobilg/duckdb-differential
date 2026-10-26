#!/usr/bin/env python3
"""Open immutable, checksummed repositories written by earlier releases with the current extension.

Never creates or refreshes fixture archives. Only disposable extracted copies are opened.
"""
import hashlib
import json
from pathlib import Path, PurePosixPath
import sqlite3
import tarfile
import tempfile

from logical_merge import Fixture, literal
from merge_hardening import assert_no_stage
from evidence import evidence_path

FIXTURES = Path(__file__).resolve().parents[1] / 'fixtures/merge'
MAX_BYTES = 16 * 1024 * 1024
MAX_FILES = 1000


def relative(name):
    path = PurePosixPath(name)
    if path.is_absolute() or '..' in path.parts or not path.parts or str(path) != name:
        raise ValueError('Invalid fixture path')
    return path


def extract(manifest, repository):
    info = json.loads(manifest.read_text())
    assert info['fixture_format'] == 1
    archive_name = relative(info['archive'])
    assert len(archive_name.parts) == 1
    archive = manifest.parent / archive_name
    assert archive.stat().st_size <= MAX_BYTES
    assert hashlib.sha256(archive.read_bytes()).hexdigest() == info['archive_sha256'], 'Archive checksum'
    assert len(info['files']) <= MAX_FILES
    expected = {str(relative(f['path'])): f for f in info['files']}
    assert len(expected) == len(info['files']), 'Duplicate manifest entry'
    assert all(0 <= f['size'] <= MAX_BYTES for f in expected.values())
    assert sum(f['size'] for f in expected.values()) <= MAX_BYTES
    repository.mkdir(mode=0o700)
    seen = set()
    with tarfile.open(archive, 'r|gz') as tar:
        for entry in tar:
            name = str(relative(entry.name))
            assert entry.isfile() and name in expected and name not in seen, 'Unexpected archive entry'
            record = expected[name]
            assert entry.size == record['size']
            data = tar.extractfile(entry).read(MAX_BYTES + 1)
            assert len(data) == record['size'] and hashlib.sha256(data).hexdigest() == record['sha256'], 'File checksum'
            target = repository / name
            target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            with target.open('xb') as output: output.write(data)
            target.chmod(0o600)
            seen.add(name)
    assert seen == set(expected), 'Missing archive entry'
    return info


def same(f, left, right):
    assert f.q(f'SELECT count(*) FROM (({left} EXCEPT ALL {right}) UNION ALL ({right} EXCEPT ALL {left}))') == 0


def data(f, selector, side, info):
    f.q(f"ATTACH 'differential:compat' AS old ({selector});")
    try:
        a, b, amount, flag = {'base':(0, 'base', 'NULL', 'NULL'), 'ours':(10, 'base', 'NULL', 'NULL'),
                              'theirs':(0, 'theirs', '2.500', 'true'), 'merged':(10, 'theirs', '2.500', 'true')}[side]
        expected = (f"SELECT * FROM (VALUES (1::INT,{a}::INT,'{b}'::VARCHAR,1.250::DECIMAL(18,3),false),"
                    f"(2,0,NULL,{amount}::DECIMAL(18,3),{flag}::BOOLEAN))")
        same(f, 'SELECT * FROM old.items', expected)
        same(f, 'SELECT * FROM old.visible', expected)
        value = '3.500' if side in ('ours', 'merged') else 'NULL'
        same(f, 'SELECT * FROM old.composite',
             f"SELECT * FROM (VALUES (1::INT,'key'::VARCHAR,{value}::DECIMAL(18,3)),(2,'other',2.125))")
        assert f.q("SELECT count(*) FROM duckdb_indexes() WHERE database_name='old' AND index_name='by_a'") == 1
        assert f.q("SELECT count(*) FROM duckdb_constraints() WHERE database_name='old' AND table_name='items' AND constraint_type='CHECK'") == 1
        assert f.q("SELECT count(*) FROM duckdb_columns() WHERE database_name='old' AND table_name='items' "
                   "AND column_name='flag' AND column_default IS NOT NULL") == 1
        if info.get('profile')=='native-context':
            # Expected SQL was frozen from the producer's specified inputs/rules,
            # never copied from merged output or regenerated with current test data.
            same(f,'SELECT * FROM old.native_values',info['native_expected'][side])
            same(f,'SELECT * FROM old.native_visible',info['native_expected'][side])
            for i,typ in enumerate(info['native_types']):
                assert f.q(f'SELECT typeof(c{i})=typeof(CAST(NULL AS {typ})) FROM old.native_values') == 1
            f.q('USE old')
            assert f.q('SELECT count(*)>0 FROM duckdb_dependencies()') == 1
            f.q('USE memory')
    finally: f.q('DETACH old')


def verify_release(manifest, root, ok):
    """Data written by a published release before merging existed: no parent records."""
    repo = root / 'release'
    info = extract(manifest, repo)
    with sqlite3.connect(repo/'engine.sqlite') as db:
        assert db.execute('PRAGMA integrity_check').fetchone()[0] == 'ok'
        assert [dict(zip(('snapshot_id', 'root', 'retired'), row)) for row in
                db.execute('SELECT id,root,retired FROM snapshots ORDER BY id')] == info['snapshots']
        assert db.execute("SELECT count(*) FROM operations WHERE kind='snapshot_ancestry' OR kind LIKE 'merge%'").fetchone()[0] == 0
    f = Fixture(repo)
    try:
        f.q('SET differential_auto_gc=false')
        assert f.q("SELECT count(*) FROM differential_merge_requests('compat')") == 0
        for snapshot, side in [(1, 'base'), (2, 'ours'), (3, 'theirs')]:
            data(f, f'SNAPSHOT_VERSION {snapshot},READ_ONLY', side, info)
        assert f.q("SELECT count(*) FROM differential_merge_ancestry('compat',2,3) WHERE NOT parents_known") == 2
        ok('release snapshots, typed data, views/index/default/check and unknown parents are readable')

        # Sibling branches forked from one snapshot: the fork records prove the base.
        assert f.q("SELECT DISTINCT base_snapshot_id FROM differential_merge_branch_preview('compat','ours','theirs')") == 1
        assert f.q("SELECT snapshot_id FROM differential_merge_branch('compat','ours','theirs','merged',request_id=>'release-branch')") == 4
        assert f.q("SELECT commit_extra_info LIKE '%\"base_selection\":\"sibling-fork-v1\"%' FROM differential_snapshots('compat') "
                   "WHERE snapshot_id=4") == 1
        data(f, "BRANCH 'merged'", 'merged', info)
        assert f.q("SELECT count(*) FROM differential_merge_ancestry('compat',4,3) WHERE snapshot_id=4 AND parents=[2,3]") == 1
        ok('branch merge of release history selects the fork base and records both parents')

        assert f.q("SELECT snapshot_id FROM differential_merge_snapshots('compat',1,2,3,'explicit',request_id=>'release-explicit')") == 5
        data(f, "BRANCH 'explicit'", 'merged', info)
        assert f.q("SELECT merge_action='merged' FROM differential_merge_branch('compat','ours','theirs','ours',request_id=>'release-target')") == 1
        data(f, "BRANCH 'ours'", 'merged', info)
        data(f, "BRANCH 'theirs'", 'theirs', info)
        ok('explicit snapshot merge and existing-target merge leave release inputs unchanged')

        f.q('CALL differential_cleanup_metadata(older_than => TIMESTAMPTZ \'2999-01-01\')')
        assert f.q("SELECT count(*) FROM differential_merge_ancestry('compat',4,3) WHERE snapshot_id=4 AND parents=[2,3]") == 1
        f.q("ATTACH 'differential:compat' AS t (BRANCH 'theirs'); UPDATE t.composite SET v=9.000 WHERE k=2; "
            "CALL differential_create_snapshot('t'); DETACH t")
        # Release snapshot 2 has no parent record and is reachable only from merged: its unknown
        # history could hold a better base, so automatic selection refuses rather than guess.
        f.error("SELECT * FROM differential_merge_branch_preview('compat','merged','theirs')", 'merge_base_unavailable')
        assert f.q("SELECT count(*) FROM differential_merge_snapshots_preview('compat',3,4,7) WHERE conflicts=0") == 2
        # Branches forked after the upgrade have complete recorded ancestry above the release boundary.
        f.q("CALL differential_create_branch('compat','feature',branch=>'merged'); "
            "ATTACH 'differential:compat' AS feature (BRANCH 'feature'); UPDATE feature.items SET a=20 WHERE id=2; "
            "CALL differential_create_snapshot('feature'); DETACH feature")
        assert f.q("SELECT DISTINCT base_snapshot_id FROM differential_merge_branch_preview('compat','merged','feature')") == 4
        assert f.q("SELECT merge_action='fast_forward' FROM differential_merge_branch('compat','merged','feature','merged',"
                   "request_id=>'release-forward')") == 1
        f.q('CALL differential_cleanup_old_files(cleanup_all=>true)')
        assert_no_stage(f)
        ok('ancestry survives metadata cleanup; release history needs an explicit base, later forks do not')
    finally: f.host.close()


def verify(manifest, root, ok):
    if json.loads(manifest.read_text()).get('profile') == 'release':
        return verify_release(manifest, root, ok)
    repo = root / 'retry'
    info = extract(manifest, repo)
    with sqlite3.connect(repo/'engine.sqlite') as db:
        assert db.execute('PRAGMA integrity_check').fetchone()[0] == 'ok'
        assert [dict(zip(('snapshot_id', 'root', 'retired'), row)) for row in
                db.execute('SELECT id,root,retired FROM snapshots ORDER BY id')] == info['snapshots']
        frozen_contexts = dict(db.execute("SELECT id,payload FROM operations WHERE kind='merge_expression_context'"))
        if info.get('profile')=='native-context':
            assert len(frozen_contexts)==5 and not info['resolution_source_included']
            assert all('native-expression-context-v1' in p and 'America/New_York' in p and 'gregorian' in p for p in frozen_contexts.values())
    f = Fixture(repo)
    try:
        f.q('SET differential_auto_gc=false')
        assert f.q("SELECT count(*) FROM differential_merge_requests('compat')") == 5
        for request, prior in info['request_rows'].items():
            clauses = []
            for key, value in prior.items():
                if key == 'recovery_action': continue  # Guidance prose may evolve.
                sql_value = literal(value)
                if value is not None and key.endswith('_at'): sql_value += '::TIMESTAMPTZ'
                clauses.append(f'{key} IS NOT DISTINCT FROM {sql_value}')
            assert f.q(f"SELECT count(*) FROM differential_merge_requests('compat',request_id=>'{request}') "
                       'WHERE ' + ' AND '.join(clauses)) == 1
        baseline_count = int(info['diagnostics_present'])
        assert f.q(f"SELECT count(*) FROM differential_merge_requests('compat') WHERE attempt_count={baseline_count}") == 5
        assert f.q("SELECT count(*) FROM differential_merge_requests('compat') WHERE merge_kind='merge_branch' "
                   "AND inputs_guarded AND theirs_branch_name='theirs' AND length(ours_branch_id)=36 AND length(theirs_branch_id)=36") == 3
        if not baseline_count:
            assert f.q("SELECT count(*) FROM differential_merge_requests('compat') WHERE last_attempt_state IS NULL") == 5
        for snapshot, side in [(1,'base'), (2,'ours'), (3,'theirs'), (5,'merged'), (6,'merged')]:
            data(f, f'SNAPSHOT_VERSION {snapshot},READ_ONLY', side, info)
        f.error("ATTACH 'differential:compat' AS expired (SNAPSHOT_VERSION 4,READ_ONLY)", 'retired')
        assert f.q("SELECT count(*) FROM differential_merge_ancestry('compat',5,3) WHERE snapshot_id=5 AND parents=[2,3]") == 1
        assert f.q("SELECT count(*) FROM differential_merge_ancestry('compat',4,3) WHERE snapshot_id=4 AND retired AND parents=[2,3]") == 1
        ok('immutable snapshots, typed data, views/index/default/check, ancestry and expired result')

        f.q("CREATE TEMP TABLE before AS SELECT * FROM differential_merge_requests('compat');")
        for request, result in [('fixture-snapshot',4), ('fixture-guarded',5), ('fixture-target',6)]:
            assert f.q(info['requests'][request]) == result
        guarded = info['requests']['fixture-guarded']
        f.error(guarded.replace('expected_ours_snapshot_id=>2','expected_ours_snapshot_id=>3'), 'Request ID')
        f.error(guarded[:guarded.index(',expected_base_snapshot_id')] + ')', 'Request ID')
        f.error(guarded.replace("'guarded_result'", "'changed'"), 'Request ID')
        f.error(info['requests']['fixture-aborted'], 'merge_aborted')
        same(f, 'SELECT * FROM before', "SELECT * FROM differential_merge_requests('compat')")
        ok('completed/expired/guarded replay and terminal abort preserve recorded identity and diagnostics')

        assert f.q("SELECT count(*) FROM differential_merge_branch_preflight('compat','pending','theirs','pending') "
                   "WHERE NOT ready AND target_state='pending'") == 1
        f.error("ATTACH 'differential:compat' AS blocked (BRANCH 'pending')", 'pending')
        f.q("SET TimeZone='UTC'; SET Calendar='gregorian'")
        assert f.q(info['requests']['fixture-pending']) == 7
        assert f.q(info['requests']['fixture-pending']) == 7
        assert f.q(f"SELECT count(*) FROM differential_merge_requests('compat',request_id=>'fixture-pending') "
                   f"WHERE state='complete' AND attempt_count={baseline_count+1} AND last_attempt_state='succeeded'") == 1
        data(f, "BRANCH 'pending'", 'merged', info)
        if info.get('profile')=='native-context':
            assert f.q("SELECT current_setting('TimeZone')='UTC'") == 1
            with sqlite3.connect(repo/'engine.sqlite') as db:
                assert dict(db.execute("SELECT id,payload FROM operations WHERE kind='merge_expression_context'")) == frozen_contexts
        f.q("ATTACH 'differential:compat' AS writable (BRANCH 'pending'); BEGIN; "
            "INSERT INTO writable.items(id,a) VALUES(100,0)")
        assert f.q('SELECT flag=false FROM writable.items WHERE id=100') == 1
        f.q('ROLLBACK')
        f.error('INSERT INTO writable.items(id,a) VALUES(100,-1)', 'CHECK')
        f.error("INSERT INTO writable.composite VALUES(1,'key',0)", 'Duplicate key')
        if info.get('profile')=='native-context':
            f.error('DROP TABLE writable.native_values','depends on table')
            f.q("SET TimeZone='America/New_York'")
            assert f.q('SELECT CAST(ts AS DATE)=day FROM writable.native_values') == 1
            f.q("SET TimeZone='UTC'")
        f.q('DETACH writable')
        assert f.q("SELECT count(*) FROM differential_merge_ancestry('compat',7,3) WHERE snapshot_id=7 AND parents=[2,3]") == 1
        f.q('CALL differential_cleanup_old_files(cleanup_all=>true)')
        assert_no_stage(f)
        ok('interrupted target fence retries once, publishes both parents and cleans dead staging')
    finally: f.host.close()

    # A different copy proves abort works without retaining/replacing expired inputs.
    repo = root / 'abort'
    extract(manifest, repo)
    f = Fixture(repo)
    try:
        f.q("CALL differential_expire_snapshots('compat',versions=>[1]); CALL differential_cleanup_old_files(cleanup_all=>true)")
        pending = info['request_rows']['fixture-pending']['operation_id']
        assert f.q(f"SELECT state='aborted' FROM differential_abort_merge('compat','{pending}',request_id=>'compat-abort')") == 1
        f.error(info['requests']['fixture-pending'], 'merge_aborted')
        data(f, "BRANCH 'pending'", 'ours', info)
        assert f.q(f"SELECT count(*) FROM differential_merge_requests('compat',request_id=>'fixture-pending') "
                   f"WHERE state='aborted' AND attempt_count={baseline_count}") == 1
        f.q('CALL differential_cleanup_old_files(cleanup_all=>true)')
        assert_no_stage(f)
        ok('abort after base expiry releases a historical fence without backfilling attempt history')
    finally: f.host.close()


def main():
    passed = []
    manifests = sorted(FIXTURES.glob('*.json'))
    assert manifests, 'No committed compatibility fixtures'
    originals = {p:hashlib.sha256(p.read_bytes()).hexdigest() for p in FIXTURES.iterdir() if p.is_file()}
    for manifest in manifests:
        def ok(label):
            item = manifest.stem + ': ' + label
            passed.append(item); print('PASS', item, flush=True)
        with tempfile.TemporaryDirectory(prefix='differential-compatibility-') as tmp:
            verify(manifest, Path(tmp), ok)
    assert all(hashlib.sha256(p.read_bytes()).hexdigest() == digest for p,digest in originals.items())
    evidence_path('merge-compatibility.json').write_text(json.dumps({'passed':passed,'fixtures':[p.name for p in manifests],
        'scope':'Immutable local fixtures, including data written by the published v1.0.0 release, on stock DuckDB v2.0.0; not a claim of S3 or downgrade compatibility'},indent=2)+'\n')


if __name__ == '__main__': main()
