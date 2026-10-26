#!/usr/bin/env python3
"""Single-writer S3 branch/target merges. Disposable MinIO namespaces only."""
import json
from pathlib import Path
import shutil
import sqlite3
from contextlib import closing
import sys
import time

from evidence import evidence_path
from s3_merge import Fixture, FaultRelay
from merge_lifecycle import inspect, abort_sql, start, release, result


class BranchFixture(Fixture):
    def merge(self, dest='ours', request='merge', options='', ours='ours', theirs='theirs'):
        return (f"SELECT * FROM differential_merge_branch('merge_db','{ours}','{theirs}','{dest}',"
                f"request_id=>'{request}'{options});")

    def preflight(self, destination='ours', options=''):
        return self.run(f"SELECT * FROM differential_merge_branch_preflight('merge_db','ours','theirs','{destination}'"
                        f",validate=>true{options});")[-1][0]

    def head(self, branch='ours'):
        return self.scalar(f"SELECT head FROM branches WHERE name='{branch}' AND state='active'")

    def clean(self):
        self.run('CALL differential_cleanup_old_files(cleanup_all=>true);')
        for sql in ("SELECT count(*) FROM compound_intents", "SELECT count(*) FROM export_roots WHERE state='pending'",
                    "SELECT count(*) FROM files WHERE name LIKE 'merge_stage/%'", "SELECT count(*) FROM pins"):
            assert self.scalar(sql) == 0, sql
        assert self.scalar('PRAGMA integrity_check') == 'ok'

    def verify(self, branch='ours', a=11, b='theirs'):
        row = self.remote(branch)[-1][1]
        assert row['a'] == a and row['b'] == b, row
        local = self.run(f"ATTACH 'differential:merge_db' AS v (BRANCH '{branch}'); SELECT a,b FROM v.visible WHERE id=1;")[-1]
        assert local == [{'a': a, 'b': b}], local
        self.clean()


def main(endpoint, base):
    base = Path(base)/'s3-merge-branches'; base.mkdir()
    passed = []
    def ok(label): passed.append(label); print('PASS S3 branch merge', label, flush=True)
    relay = FaultRelay(endpoint)
    try:
        f = BranchFixture(base,'branch-basic',relay.endpoint); f.setup()
        original = f.scalar("SELECT id FROM branches WHERE name='ours'")
        source = f.remote('theirs')[-1]
        counts = [f.scalar(f'SELECT count(*) FROM {t}') for t in ('operations','snapshots','history','branches')]
        network = relay.requests
        f.run('PREPARE later AS '+f.merge()+' EXPLAIN '+f.merge(),raw=True)
        assert relay.requests == network
        f.run(f.merge(request=''),error='merge_request_required')
        report = f.preflight(); assert report['ready'] and report['merge_action'] == 'merged', report
        assert [report[k+'_snapshot_id'] for k in ('base','ours','theirs')] == [1,2,3]
        assert counts == [f.scalar(f'SELECT count(*) FROM {t}') for t in ('operations','snapshots','history','branches')]
        writes = len(relay.head_writes)
        guards = ',expected_base_snapshot_id=>1,expected_ours_snapshot_id=>2,expected_theirs_snapshot_id=>3'
        # Source live writes and an old immutable target reader do not enter the result.
        first = f.run("ATTACH 'differential:merge_db' AS live (BRANCH 'theirs'); UPDATE live.x SET a=99 WHERE id=0; "
                      "ATTACH 'differential:merge_db' AS old (SNAPSHOT_VERSION 2,READ_ONLY); "+f.merge(options=guards)+
                      "SELECT a,b FROM old.x WHERE id=1;")
        row = first[-2][0]; assert row['merge_action'] == 'merged' and row['snapshot_id'] == 4
        assert first[-1] == [{'a':11,'b':'base'}]
        assert f.scalar("SELECT id FROM branches WHERE name='ours'") == original
        assert f.head('theirs') == 3 and f.remote('theirs')[-1] == source
        assert len(relay.head_writes) == writes+1 and relay.head_writes[-1][1] and relay.head_writes[-1][2] is None
        f.verify()
        relay.mode = 'head-read-denied'; network = relay.requests
        retry = f.run(f.merge(options=guards))[-1][0]
        assert retry['operation_id'] == row['operation_id'] and retry['snapshot_id'] == 4 and relay.requests == network
        f.run(f.merge(),error='different expected snapshot IDs')
        f.run(f.merge(dest='different',options=guards),error='Request ID reused')
        relay.mode = ''
        writes = len(relay.head_writes); exports = f.scalar('SELECT count(*) FROM exports')
        noop = f.run(f.merge(request='noop'))[-1][0]
        assert noop['merge_action'] == 'already_up_to_date' and noop['snapshot_id'] == 4, noop
        assert len(relay.head_writes) == writes and f.scalar('SELECT count(*) FROM exports') == exports
        f.run("ATTACH 'differential:merge_db' AS s (BRANCH 'theirs'); UPDATE s.x SET b='next' WHERE id=1; CALL differential_create_snapshot('s');")
        next_result = f.run(f.merge(request='next'))[-1][0]
        assert next_result['base_snapshot_id'] == 3 and next_result['snapshot_id'] == 6
        f.verify(b='next'); ok('preflight/guards, detached target identity, source live exclusion, old reader, conditional PUT, replay, no-op and repeated merge')

        f = BranchFixture(base,'branch-new',relay.endpoint); f.setup()
        heads = [f.remote(side)[-1] for side in ('ours','theirs')]
        row = f.run(f.merge(dest='integration'))[-1][0]
        assert row['merge_action'] == 'created_branch' and row['snapshot_id'] == 4
        assert relay.head_writes[-1][2] == '*' and relay.head_writes[-1][1] is None
        assert heads == [f.remote(side)[-1] for side in ('ours','theirs')]
        f.verify('integration'); f.run(f.merge(dest='integration',request='existing'),error='name_conflict')
        ok('automatic ancestry to a new S3 branch uses conditional creation and preserves both inputs')

        for phase in ('prepared','uploaded'):
            f = BranchFixture(base,'branch-reservation-'+phase,relay.endpoint); f.setup()
            sql = f.merge(dest='reserved')
            f.run(sql,crash=phase)
            owner = inspect(f)['operation_id']
            # Opening another process may reclaim dead reader pins from the crash.
            # User history and the durable publication reservation must not change.
            tables = ('operations','snapshots','history','branches','export_roots','compound_intents')
            counts = [f.scalar(f'SELECT count(*) FROM {t}') for t in tables]
            writes = len(relay.head_writes)
            report = f.preflight('reserved')
            assert not report['ready'] and report['target_state'] == 'reserved', report
            issue = next(i for i in report['issues'] if i['code'] == 'merge_pending_operation')
            assert issue['phase'] == 'destination' and issue['operation_id'] == owner, issue
            assert 'retry' in issue['recovery_action'].lower() and 'differential_abort_merge' in issue['recovery_action'], issue
            after = [f.scalar(f'SELECT count(*) FROM {t}') for t in tables]
            assert counts == after, (tables,counts,after)
            assert len(relay.head_writes) == writes
            f.run(f.merge(dest='reserved',request='competing'),error='reserved by a pending S3 merge')
            if phase == 'prepared':
                f.run(abort_sql(owner)); f.clean()
                assert f.preflight('reserved')['ready']
                sql = f.merge(dest='reserved',request='replacement')
            row = f.run(sql)[-1][0]
            assert f.run(sql)[-1][0]['operation_id'] == row['operation_id']
            f.verify('reserved')
            ok(phase+' new-destination reservation is reported without mutation; abort/retry releases or completes the original owner')

        f = BranchFixture(base,'branch-native-context',relay.endpoint)
        f.setup(ours="UPDATE o.x SET a=11 WHERE id=1; UPDATE o.temporal SET a=1;",
                theirs="UPDATE t.x SET b='theirs' WHERE id=1; UPDATE t.temporal SET b=2;",
                constraints="SET TimeZone='America/New_York'; CREATE TABLE d.temporal(id INT PRIMARY KEY,a INT,b INT,ts TIMESTAMPTZ,day DATE,CHECK(CAST(ts AS DATE)=day)); INSERT INTO d.temporal VALUES(1,0,0,TIMESTAMPTZ '2020-01-02 01:00:00+00',DATE '2020-01-01'); SET enable_view_dependencies=true; USE d; CREATE VIEW tracked AS SELECT * FROM temporal; USE memory;")
        assert f.run("SET TimeZone='America/New_York'; SELECT ready FROM differential_merge_branch_preflight('merge_db','ours','theirs','ours');")[-1] == [{'ready':True}]
        f.run("SET TimeZone='America/New_York';"+f.merge(),crash='context_frozen')
        f.run("SET TimeZone='UTC';"+f.merge(),crash='prepared')
        first = f.run("SET TimeZone='UTC';"+f.merge())[-1][0]
        assert f.run(f.merge())[-1][0]['snapshot_id'] == first['snapshot_id'] == 4
        rows = f.run(f"SET TimeZone='America/New_York'; ATTACH 'differential:{f.location}heads/merge_db/ours' AS r ({f.options}); SELECT a=1 AND b=2 AND CAST(ts AS DATE)=day AS valid FROM r.tracked;",repo=f.folder/'temporal-reader')
        assert rows[-1] == [{'valid':True}], rows
        f.run("ATTACH 'differential:merge_db' AS m (BRANCH 'ours'); DROP TABLE m.temporal;",error='depends on table')
        f.verify(); ok('S3 target retry freezes native time zone before staging and preserves unchanged tracked views through remote publication')

        f = BranchFixture(base,'branch-fast-forward',relay.endpoint); f.setup()
        f.run("CALL differential_create_branch('merge_db','release',snapshot=>1);")
        identity = f.scalar("SELECT id FROM branches WHERE name='release'")
        snapshots = f.scalar('SELECT count(*) FROM snapshots'); exports = f.scalar('SELECT count(*) FROM exports')
        row = f.run(f.merge(dest='release',ours='release'))[-1][0]
        assert row['merge_action'] == 'fast_forward' and row['snapshot_id'] == 3
        assert f.scalar("SELECT id FROM branches WHERE name='release'") == identity
        assert snapshots == f.scalar('SELECT count(*) FROM snapshots') and exports == f.scalar('SELECT count(*) FROM exports')
        f.verify('release',a=1); ok('arbitrary-name fast-forward reuses the source snapshot/export and preserves target UUID')

        for phase in ('prepared','head_published'):
            f = BranchFixture(base,'branch-ff-'+phase,relay.endpoint); f.setup()
            sql = f.merge(dest='main',ours='main')
            f.run(sql,crash=phase)
            f.run("CALL differential_expire_snapshots('merge_db',versions=>[3]);",error='merge_pending_operation')
            if phase == 'prepared':
                f.run(abort_sql(inspect(f)['operation_id'])); assert f.head('main') == 1
            else:
                assert f.run(abort_sql(inspect(f)['operation_id']))[-1][0]['state'] == 'complete'
                assert f.head('main') == 3 and f.scalar('SELECT count(*) FROM snapshots') == 3
            f.clean(); ok('fast-forward '+phase+' retains reused output identity until completion or abort')

        f = BranchFixture(base,'branch-noop-crash',relay.endpoint); f.setup()
        f.run(f.merge()); writes = len(relay.head_writes)
        f.run(f.merge(request='noop'),crash='prepared')
        assert f.run(abort_sql(inspect(f,'noop')['operation_id']))[-1][0]['state'] == 'aborted'
        f.run(f.merge(request='noop-next'),crash='finalizing')
        assert f.run(f.merge(request='noop-next'))[-1][0]['snapshot_id'] == 4
        assert len(relay.head_writes) == writes; f.clean()
        ok('no-op interruption can abort or retry without any remote mutation or extra snapshot')

        f = BranchFixture(base,'branch-legacy-history',relay.endpoint); f.setup()
        with closing(sqlite3.connect(f.repo/'engine.sqlite')) as db:
            db.execute("DELETE FROM operations WHERE kind='snapshot_ancestry'"); db.commit()
        first = f.run(f.merge())[-1][0]; assert first['base_snapshot_id'] == 1
        f.run(f.merge(request='again'),error='merge_base_unavailable')
        f.run("SELECT * FROM differential_merge_snapshots('merge_db',3,4,3,'explicit',request_id=>'explicit');")
        f.verify('explicit'); ok('legacy sibling proof works once; incomplete history after a target merge requires an explicit base, never the old fork')

        f = BranchFixture(base,'branch-catalog',relay.endpoint)
        f.setup(ours="DROP VIEW o.visible; DROP INDEX o.ix; ALTER TABLE o.x DROP COLUMN b; CREATE INDEX ix ON o.x(a,id); DROP TABLE o.obsolete; UPDATE o.x SET a=11 WHERE id=1; USE o; CREATE VIEW visible AS SELECT id,a FROM x; USE memory;",
                theirs="UPDATE t.x SET amount=2.500,flag=NULL WHERE id=1; CREATE TABLE t.added(k INT PRIMARY KEY,v VARCHAR); INSERT INTO t.added VALUES(7,'new');",
                constraints="CREATE TABLE d.obsolete(k INT PRIMARY KEY); INSERT INTO d.obsolete VALUES(1);")
        opts=",schema_mode=>'catalog'"; assert f.preflight(options=opts)['ready']
        f.run(f.merge(options=opts),crash='prepared'); f.run(f.merge(options=opts))
        query="SELECT a,amount,flag FROM r.x WHERE id=1; SELECT * FROM r.added; SELECT count(*) AS n FROM r.visible;"
        rows=f.run(f"ATTACH 'differential:{f.location}heads/merge_db/ours' AS r ({f.options}); "+query,repo=f.folder/'catalog-reader')
        assert rows[-3:] == [[{'a':11,'amount':'2.500','flag':None}],[{'k':7,'v':'new'}],[{'n':8}]], rows
        f.run("ATTACH 'differential:merge_db' AS r (BRANCH 'ours'); SELECT * FROM r.obsolete;",error='does not exist')
        f.clean(); ok('catalog mode target publishes table/column removal, table addition, view replacement and typed nullable edits after retry')

        f = BranchFixture(base,'branch-evicted',relay.endpoint); f.setup()
        # Base becomes remote-only payload; heads remain published immutable inputs.
        f.run("CALL differential_evict_snapshots('merge_db',versions=>[1,2,3]); CALL differential_cleanup_old_files(cleanup_all=>true);")
        assert f.scalar('SELECT count(*) FROM snapshots WHERE evicted=1') == 3
        network = relay.requests
        assert f.run("SELECT bool_and(readable) AS readable FROM differential_merge_ancestry('merge_db',2,3);")[-1] == [{'readable':True}]
        assert relay.requests == network
        assert f.preflight()['ready']
        row = f.run(f.merge())[-1][0]; assert row['materialization_ms'] > 0
        assert f.scalar('SELECT count(*) FROM snapshots WHERE evicted=1') == 3
        f.verify(); ok('evicted inputs materialize under pins without changing eviction flags or ancestry')

        for phase in ('target_locked','target_checked','staged','prepared','uploaded','head_updating','head_published',
                      'target_installed','publication_transaction','published'):
            f = BranchFixture(base,'branch-crash-'+phase,relay.endpoint); f.setup()
            f.run(f.merge(),crash=phase)
            if phase != 'published':
                assert f.head() == 2
                claimed = f.run("SELECT count(*) AS n FROM differential_claim_branch_heads('merge_db') WHERE branch_name='ours';")[-1]
                assert claimed == [{'n': 0}], claimed
                f.run("ATTACH 'differential:merge_db' AS blocked (BRANCH 'ours');",error='writer_busy')
            r = inspect(f)
            if phase in ('prepared','uploaded','head_updating','head_published','target_installed','publication_transaction'):
                assert r['destination_reserved'] and r['retained_logical_bytes'] > 0
                f.run("CALL differential_expire_snapshots('merge_db',versions=>[1,2,3]);")
                # Ordinary export/claim must not replace a remote head with old local state.
                before = f.remote('ours')[-1]
                f.run("CALL differential_retry_export('merge_db'); CALL differential_claim_branch_heads('merge_db');")
                assert f.remote('ours')[-1] == before
            first = f.run(f.merge())[-1][0]; replay = f.run(f.merge())[-1][0]
            assert first['snapshot_id'] == replay['snapshot_id'] == 4 and first['operation_id'] == replay['operation_id']
            f.verify(); ok(phase+' crash/retry retains target fence and publishes exactly one result')

        for phase in ('prepared','head_updating','head_published'):
            f = BranchFixture(base,'branch-abort-'+phase,relay.endpoint); f.setup(); f.run(f.merge(),crash=phase)
            op = inspect(f)['operation_id']
            if phase == 'head_updating':
                f.run(abort_sql(op),error='merge_publication_uncertain')
                assert f.head() == 2 and f.scalar('SELECT count(*) FROM compound_intents') == 1
                f.run(f.merge()); f.verify()
            elif phase == 'head_published':
                assert f.run(abort_sql(op))[-1][0]['state'] == 'complete'
                f.verify()
            else:
                assert f.run(abort_sql(op))[-1][0]['state'] == 'aborted'
                assert f.head() == 2; f.clean()
                f.run(f.merge(),error='merge_aborted')
            ok(phase+' abort distinguishes safe abandonment, uncertain PUT and already published result')

        f = BranchFixture(base,'branch-delayed',relay.endpoint); f.setup(); relay.mode='head-delayed'
        relay.head_received.clear(); relay.head_release.clear(); relay.head_finished.clear()
        publisher, gate = start(f,f.merge())
        assert relay.head_received.wait(20)
        publisher.kill(); publisher.communicate(timeout=10)
        op = inspect(f)['operation_id']
        f.run(abort_sql(op),error='merge_publication_uncertain')
        relay.head_release.set(); assert relay.head_finished.wait(10); relay.mode=''
        assert f.run(abort_sql(op))[-1][0]['state'] == 'complete'
        f.verify(); ok('delayed If-Match PUT after process death cannot be unsafely aborted and is recovered')

        f = BranchFixture(base,'branch-response-loss',relay.endpoint); f.setup(); relay.mode='head-response-loss'
        f.run(f.merge()); relay.mode=''; f.verify()
        ok('lost conditional PUT response is recognized by exact merge write identity')

        f = BranchFixture(base,'branch-stale-remote',relay.endpoint); f.setup()
        copy = f.folder/'other-writer'; shutil.copytree(f.repo,copy)
        publisher, gate = start(f,f.merge(),'head_updating')
        try:
            f.run("CALL differential_claim_branch_heads('merge_db'); ATTACH 'differential:merge_db' AS w (BRANCH 'ours'); UPDATE w.x SET a=88 WHERE id=1; CALL differential_create_snapshot('w');",repo=copy)
        finally: release(gate)
        out,err = publisher.communicate(timeout=60)
        assert publisher.returncode and 'head_conflict' in err, (out,err)
        assert f.head() == 2 and f.remote('ours')[-1][1]['a'] == 88
        assert inspect(f)['last_attempt_error_code'] == 'head_conflict'
        f.run(f.merge(),error='head_conflict') # Frozen ETag never silently rebases.
        f.run(abort_sql(inspect(f)['operation_id'])); f.clean()
        assert f.remote('ours')[-1][1]['a'] == 88
        ok('competing copied repository wins between read and PUT; stale conditional write fails, retry never rebases, safe abort preserves winner')

        f = BranchFixture(base,'branch-upload-abort',relay.endpoint); f.setup(); relay.mode='upload-denied'
        f.run(f.merge(),error='denied'); relay.mode=''
        assert inspect(f)['state'] == 'prepared' and f.head() == 2
        f.run("ATTACH 'differential:merge_db' AS blocked (BRANCH 'ours');",error='writer_busy')
        f.run(abort_sql(inspect(f)['operation_id'])); f.clean()
        ok('upload failure preserves durable target fence; safe abort releases it without changing the head')

        f = BranchFixture(base,'branch-cancel',relay.endpoint); f.setup()
        publisher,gate=start(f,f.merge(),'prepared'); publisher.send_signal(2)
        publisher.communicate(timeout=10); release(gate)
        assert f.head() == 2 and f.scalar('SELECT count(*) FROM compound_intents') == 1
        f.run(f.merge()); f.verify(); ok('cancellation after preparation retains the fenced result for retry')

        f = BranchFixture(base,'branch-unpublished',relay.endpoint); f.setup(); relay.mode='upload-denied'
        f.run("ATTACH 'differential:merge_db' AS w (BRANCH 'ours'); UPDATE w.x SET a=90 WHERE id=0; CALL differential_create_snapshot('w');",error='incomplete_export')
        relay.mode=''; report=f.preflight(); assert not report['ready'] and any(i['code']=='merge_target_unpublished' for i in report['issues']), report
        f.run(f.merge(),error='merge_target_unpublished')
        assert f.scalar('SELECT count(*) FROM compound_intents') == 0
        f.run("CALL differential_retry_export('merge_db');"); f.run(f.merge()); f.verify()
        ok('target export lag is explicit in preflight/execution; export retry then the same frozen request succeeds')

        f = BranchFixture(base,'branch-busy-dirty',relay.endpoint); f.setup()
        f.run("ATTACH 'differential:merge_db' AS w (BRANCH 'ours'); "+f.merge(request='busy'),error='active_attachment_or_transaction')
        f.run("ATTACH 'differential:merge_db' AS w (BRANCH 'ours'); UPDATE w.x SET a=90 WHERE id=0;")
        assert not f.preflight()['ready']; f.run(f.merge(),error='merge_target_dirty')
        assert f.head()==2 and f.scalar('SELECT count(*) FROM compound_intents')==0
        ok('active target and unsnapshotted target writes are rejected without a publication or lingering fence')

        f = BranchFixture(base,'branch-resolutions',relay.endpoint); f.setup(theirs='UPDATE t.x SET a=12 WHERE id=1;')
        choices="CREATE TABLE choices(id INT,a BIGINT,b VARCHAR,amount DECIMAL(18,3),flag BOOLEAN,_merge_action VARCHAR,_merge_columns MAP(VARCHAR,VARCHAR)); INSERT INTO choices VALUES(1,NULL,NULL,NULL,NULL,'cells',map{'a':'theirs'});"
        options=",resolution_tables=>map{'main.x':'memory.main.choices'}"
        f.run(choices+f.merge(options=options),crash='prepared')
        f.run("CALL differential_expire_snapshots('merge_db',versions=>[1,2,3]);")
        f.run(f.merge(options=options)); f.verify(a=12,b='base')
        ok('typed explicit resolutions survive prepared target recovery without decision tables or input snapshots')

        f = BranchFixture(base,'branch-guard',relay.endpoint); f.setup()
        assert f.preflight()['ready']; count = f.scalar('SELECT count(*) FROM operations')
        f.run("ATTACH 'differential:merge_db' AS s (BRANCH 'theirs'); UPDATE s.x SET b='new' WHERE id=0; CALL differential_create_snapshot('s');")
        count = f.scalar('SELECT count(*) FROM operations')
        f.run(f.merge(options=guards),error='merge_inputs_changed')
        assert f.scalar('SELECT count(*) FROM operations') == count and f.head() == 2
        ok('reviewed input guards reject branch advancement before request reservation')

        for label,left,right,constraints,code in (
            ('conflict','UPDATE o.x SET a=11 WHERE id=1;','UPDATE t.x SET a=12 WHERE id=1;','','merge_conflict'),
            ('constraint','UPDATE o.x SET a=11 WHERE id=1;','UPDATE t.x SET a=11 WHERE id=2;','CREATE UNIQUE INDEX ux ON d.x(a);','merge_constraint'),
            ('schema','ALTER TABLE o.x ADD COLUMN extra INT;','SELECT 1;','','merge_unsupported'),
        ):
            f = BranchFixture(base,'branch-invalid-'+label,relay.endpoint); f.setup(ours=left,theirs=right,constraints=constraints)
            assert not f.preflight()['ready']; before = f.remote('ours')[-1]; writes=len(relay.head_writes)
            f.run(f.merge(),error=code)
            assert f.head() == 2 and f.remote('ours')[-1] == before and len(relay.head_writes) == writes
            assert f.scalar("SELECT count(*) FROM operations WHERE kind='merge_publication'") == 0
            f.clean(); ok(label+' failure leaves target/input state intact and releases the pre-preparation fence')
    finally: relay.close()
    evidence_path('s3-merge-branches.json').write_text(json.dumps({'passed':passed,'scope':'disposable MinIO, single writer, stock DuckDB v2.0.0'},indent=2)+'\n')


if __name__ == '__main__': main(*sys.argv[1:])
