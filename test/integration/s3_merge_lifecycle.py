#!/usr/bin/env python3
"""Abort/recovery on disposable MinIO, including a PUT arriving after writer death."""
from contextlib import closing
import json
import os
from pathlib import Path
import shutil
import sqlite3
import sys
import time

from evidence import evidence_path
from s3_merge import Fixture, FaultRelay
from merge_lifecycle import inspect, abort_sql, start, release, result


def no_retention(f):
    r = inspect(f)
    assert r['state'] == 'aborted' and not r['destination_reserved'] and r['retained_logical_bytes'] == 0
    assert f.scalar("SELECT count(*) FROM export_roots WHERE state='pending'") == 0
    f.run('CALL differential_cleanup_old_files(cleanup_all=>true);')
    assert f.scalar("SELECT count(*) FROM files WHERE name LIKE 'merge_stage/%'") == 0
    assert f.scalar("SELECT count(*) FROM operations WHERE kind='merge_aborted'") == 1
    f.run(f.merge(),error='merge_aborted'); f.hidden()


def main(endpoint, base):
    base = Path(base)/'s3-merge-lifecycle'; base.mkdir()
    passed = []
    def ok(label): passed.append(label); print('PASS S3 merge lifecycle',label,flush=True)
    relay = FaultRelay(endpoint)
    try:
        for phase in ('prepared','uploaded'):
            f = Fixture(base,'abort-'+phase,relay.endpoint); f.setup(); f.run(f.merge(),crash=phase)
            counts = f.scalar('SELECT count(*) FROM operations'); requests = relay.requests
            r = inspect(f); op = r['operation_id']
            assert r['state'] == phase and r['destination_reserved'] and r['retained_logical_bytes'] > 0
            assert not r['publication_attempted']
            assert (r['remote_descriptor'] is not None) == (phase == 'uploaded')
            assert r['attempt_count'] == 1 and r['last_attempt_state'] == 'incomplete'
            assert r['last_attempt_phase'] == phase and r['last_attempt_error_code'] is None
            assert r['last_attempt_finished_at'] is None
            assert f.scalar('SELECT count(*) FROM operations') == counts and relay.requests == requests
            f.run("CALL differential_expire_snapshots('merge_db',versions=>[1,2,3]); CALL differential_cleanup_old_files(cleanup_all=>true);")
            assert f.scalar(f"SELECT count(*) FROM reclaimed_roots WHERE root='{r['result_root']}'") == 0
            first = f.run(abort_sql(op))[-1][0]
            assert first['state'] == 'aborted'
            assert {k:v for k,v in inspect(f).items() if k.startswith('last_attempt') or k == 'attempt_count'} == {
                k:v for k,v in r.items() if k.startswith('last_attempt') or k == 'attempt_count'}
            no_retention(f)
            assert f.scalar(f"SELECT count(*) FROM reclaimed_roots WHERE root='{r['result_root']}'") == 1
            relay.mode = 'head-read-denied'
            assert f.run(abort_sql(op))[-1][0] == first  # Credential/network independent replay.
            assert f.run(abort_sql(op,'new-abort'))[-1][0]['state'] == 'aborted'
            relay.mode = ''
            # Expired inputs remain excluded; the released name can be used with a retained head.
            f.run("ATTACH 'differential:merge_db' AS fresh (BRANCH 'ours'); CALL differential_create_snapshot('fresh'); "
                  "DETACH fresh; CALL differential_create_branch('merge_db','merged',branch=>'ours');")
            assert f.remote()[-1][1]['a'] == 11
            ok(phase+' inspection has no S3 I/O; abort releases root/name without inputs')

        for phase in ('abort_transaction','aborted'):
            f = Fixture(base,'crash-'+phase,relay.endpoint); f.setup(); f.run(f.merge(),crash='uploaded')
            op = inspect(f)['operation_id']
            f.run(abort_sql(op),crash=phase)
            assert inspect(f)['state'] == ('uploaded' if phase == 'abort_transaction' else 'aborted')
            first = f.run(abort_sql(op))[-1][0]
            assert f.run(abort_sql(op))[-1][0] == first
            no_retention(f); f.remote(error='not_found')
            ok(phase+' atomically releases private result and reservation')

        f = Fixture(base,'head-read-failed',relay.endpoint); f.setup(); f.run(f.merge(),crash='uploaded')
        op = inspect(f)['operation_id']; relay.mode = 'head-read-denied'
        f.run(abort_sql(op),error='denied')
        assert inspect(f)['state'] == 'uploaded' and inspect(f)['destination_reserved']
        assert f.scalar("SELECT count(*) FROM operations WHERE kind='merge_aborted'") == 0
        relay.mode = ''; f.run(abort_sql(op)); no_retention(f)
        ok('failed remote check preserves state; same abort request resumes')

        f = Fixture(base,'reuse-reservation',relay.endpoint); f.setup(); f.run(f.merge(),crash='uploaded')
        old = inspect(f)['operation_id']; f.run(abort_sql(old))
        f.run(f.merge(request='replacement'),crash='prepared')
        assert f.run(abort_sql(old,'late-abort'))[-1][0]['state'] == 'aborted'
        assert inspect(f,'replacement')['destination_reserved']
        assert f.scalar("SELECT count(*) FROM export_roots WHERE state='pending'") == 1
        f.run("CALL differential_create_branch('merge_db','merged',snapshot=>1);",error='name_conflict')
        f.run(f.merge(request='replacement')); f.verify()
        ok('late abort replay cannot release a replacement request reservation or output')

        for phase in ('head_published','publication_transaction','published'):
            f = Fixture(base,'recover-'+phase,relay.endpoint); f.setup(); f.run(f.merge(),crash=phase)
            r = inspect(f); op = r['operation_id']
            assert r['state'] == ('complete' if phase == 'published' else 'publication_uncertain')
            assert r['attempt_count'] == 1
            assert r['last_attempt_state'] == ('succeeded' if phase == 'published' else 'incomplete')
            assert r['last_attempt_phase'] == ('completed' if phase == 'published' else 'head_published')
            f.run("CALL differential_expire_snapshots('merge_db',versions=>[1,2,3]);")
            if phase == 'head_published':
                relay.mode = 'head-get-denied'
                f.run(abort_sql(op),error='merge_publication_uncertain')
                assert inspect(f)['destination_reserved']
                relay.mode = ''
                f.run(abort_sql(op),crash='published')  # Local recovery committed, abort reply not recorded yet.
                assert inspect(f)['state'] == 'complete'
            if phase == 'published': relay.mode = 'head-read-denied'
            abort = f.run(abort_sql(op))[-1][0]
            assert abort['state'] == 'complete' and abort['snapshot_id'] == r['result_snapshot_id']
            relay.mode = ''
            assert f.run(abort_sql(op))[-1][0] == abort
            assert f.scalar("SELECT count(*) FROM operations WHERE kind='merge_aborted'") == 0
            completed = inspect(f)
            assert completed['attempt_count'] == 1 and completed['last_attempt_id'] == r['last_attempt_id']
            assert completed['last_attempt_state'] == 'succeeded' and completed['last_attempt_phase'] == 'completed'
            assert completed['last_attempt_error_code'] is None and completed['last_attempt_finished_at'] is not None
            f.verify(); ok(phase+' abort recovers completion and preserves the published branch')

        f = Fixture(base,'intent-only',relay.endpoint); f.setup(); f.run(f.merge(),crash='head_creating')
        r = inspect(f); op = r['operation_id']
        assert r['publication_attempted'] and r['state'] == 'publication_uncertain'
        f.run(abort_sql(op),error='merge_publication_uncertain')
        assert inspect(f)['destination_reserved']
        f.run(f.merge()); assert f.run(abort_sql(op))[-1][0]['state'] == 'complete'; f.verify()
        assert inspect(f)['attempt_count'] == 2 and inspect(f)['last_attempt_state'] == 'succeeded'
        ok('durable write intent plus absent head is uncertain, never silently abandoned')

        f = Fixture(base,'diagnostic-completion-fault',relay.endpoint); f.setup()
        os.environ['DIFFERENTIAL_MERGE_DIAGNOSTICS_ERROR'] = 'complete'
        try: f.run(f.merge(),error='disk_full')
        finally: os.environ.pop('DIFFERENTIAL_MERGE_DIAGNOSTICS_ERROR')
        failed = inspect(f)
        assert failed['state'] == 'publication_uncertain' and failed['destination_reserved']
        assert failed['attempt_count'] == 1 and failed['last_attempt_state'] == 'failed'
        assert failed['last_attempt_phase'] == 'finalizing' and failed['last_attempt_error_code'] == 'disk_full'
        f.hidden(); assert f.remote()[-1][1]['a'] == 11
        f.run("CALL differential_expire_snapshots('merge_db',versions=>[1,2,3]);")
        assert f.run(abort_sql(failed['operation_id']))[-1][0]['state'] == 'complete'
        completed = inspect(f)
        assert completed['attempt_count'] == 1 and completed['last_attempt_id'] == failed['last_attempt_id']
        assert completed['last_attempt_state'] == 'succeeded' and completed['last_attempt_error_code'] is None
        assert completed['last_attempt_finished_at'] >= failed['last_attempt_finished_at']
        f.verify()
        ok('diagnostic completion failure rolls back local publication; abort recovers the existing head and same attempt')

        f = Fixture(base,'delayed-put',relay.endpoint); f.setup()
        relay.mode = 'head-delayed'
        publisher, _ = start(f,f.merge())
        assert relay.head_received.wait(20)
        publisher.kill(); publisher.communicate(timeout=10)
        op = inspect(f)['operation_id']
        f.run(abort_sql(op),error='merge_publication_uncertain')
        assert inspect(f)['destination_reserved']
        relay.head_release.set(); assert relay.head_finished.wait(20)
        relay.mode = ''
        assert f.run(abort_sql(op))[-1][0]['state'] == 'complete'
        f.verify(); ok('late S3 PUT after writer death cannot publish an aborted result')

        f = Fixture(base,'foreign-head',relay.endpoint); f.setup()
        copied = f.folder/'competitor'; shutil.copytree(f.repo,copied)
        f.run(f.merge(),crash='uploaded'); op = inspect(f)['operation_id']
        f.run("CALL differential_create_branch('merge_db','merged',snapshot=>1);",repo=copied)
        f.run(f.merge(),error='name_conflict')
        failed = inspect(f)
        assert failed['attempt_count'] == 2 and failed['last_attempt_state'] == 'failed'
        assert failed['last_attempt_error_code'] == 'name_conflict'
        f.run(abort_sql(op)); no_retention(f)
        assert inspect(f)['last_attempt_id'] == failed['last_attempt_id'] and inspect(f)['last_attempt_state'] == 'failed'
        assert f.remote()[-1][1]['a'] == 1
        ok('foreign head is preserved while abandoned local result is reclaimed')

        f = Fixture(base,'upload-abort-race',relay.endpoint); f.setup()
        publisher, gate = start(f,f.merge(),'uploaded'); op = inspect(f)['operation_id']
        abort, waiting = start(f,abort_sql(op),'abort_waiting'); release(waiting)
        time.sleep(.15); assert abort.poll() is None
        publisher.kill(); publisher.communicate(timeout=10); release(gate)
        assert result(abort)['state'] == 'aborted'; no_retention(f); f.remote(error='not_found')
        ok('abort waits for uploader, then safely abandons after its death')

        f = Fixture(base,'legacy',relay.endpoint); f.setup(); f.run(f.merge(),crash='uploaded')
        # Simulate a pre-write-intent record from the previous implementation.
        with closing(sqlite3.connect(f.repo/'engine.sqlite')) as db:
            payload = db.execute("SELECT payload FROM operations WHERE kind='merge_publication'").fetchone()[0]
            fields = []
            while payload:
                size, rest = payload.split(':',1); n = int(size); fields.append(rest[:n]); payload = rest[n:]
            fields[0] = 's3-merge-publication-v1'; fields = fields[:12]
            payload = ''.join(f'{len(x.encode())}:{x}' for x in fields)
            db.execute("UPDATE operations SET payload=? WHERE kind='merge_publication'",(payload,)); db.commit()
        r = inspect(f); assert r['publication_attempted']
        f.run(abort_sql(r['operation_id']),error='merge_publication_uncertain')
        f.run(f.merge()); f.verify()
        ok('legacy publication records conservatively retain uncertain state and still resume')
    finally: relay.close()
    evidence_path('s3-merge-lifecycle.json').write_text(json.dumps({'passed':passed,'scope':'disposable MinIO; no AWS'},indent=2)+'\n')


if __name__ == '__main__': main(sys.argv[1],sys.argv[2])
