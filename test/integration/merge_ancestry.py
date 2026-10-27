#!/usr/bin/env python3
"""Local parent/child and repeated merges, incomplete history, expiry and atomic lineage.
All data and deliberate metadata-fault fixtures are confined to disposable repositories.
"""
import json
from contextlib import closing
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile

from logical_merge import Fixture
from merge_branches import setup
from merge_hardening import assert_no_stage
from paths import DUCKDB_CLI, EXTENSION
from evidence import evidence_path


def preview(n, ours, theirs, extra=''):
    return f"differential_merge_branch_preview('{n}','{ours}','{theirs}'{extra})"


def merge(n, ours, theirs, dest, extra=''):
    return f"differential_merge_branch('{n}','{ours}','{theirs}','{dest}'{extra})"


def unpack(payload):
    fields = []
    while payload:
        length, payload = payload.split(':', 1)
        n = int(length)
        fields.append(payload[:n]); payload = payload[n:]
    return fields


def pack(fields):
    return ''.join(f'{len(item)}:{item}' for item in fields)


def metadata(repo, sql, parameters=()):
    # Python and the extension contain separate SQLite copies. A separate process
    # preserves OS lock ownership when independently inspecting a live repository.
    script = ("import json,sqlite3,sys; from urllib.parse import quote; "
              "path,sql,args=json.load(sys.stdin); "
              "db=sqlite3.connect('file:'+quote(path)+'?mode=ro',uri=True); "
              "print(json.dumps(db.execute(sql,args).fetchall())); db.close()")
    r = subprocess.run([sys.executable,'-c',script],input=json.dumps([str(repo/'engine.sqlite'),sql,parameters]),
                       text=True,capture_output=True,timeout=30)
    assert r.returncode == 0,r.stderr
    return json.loads(r.stdout)


def records(repo, name):
    return {row[0]: unpack(row[1]) for row in metadata(repo,
        "SELECT snapshot_id,payload FROM operations WHERE kind='snapshot_ancestry' AND "
        "database_id=(SELECT id FROM databases WHERE name=?)", (name,))}


def main():
    passed = []
    def ok(label): passed.append(label); print('PASS', label, flush=True)
    with tempfile.TemporaryDirectory(prefix='differential-merge-ancestry-') as temporary:
        root = Path(temporary); repo = root/'repo'; f = Fixture(repo)
        try:
            n = setup(f)
            # main -> feature, then continue the merge result and the feature independently.
            assert f.q(f'SELECT base_snapshot_id FROM {preview(n,"main","o")}') == 1
            assert f.q(f'SELECT snapshot_id FROM {merge(n,"o","t","integrated")}') == 4
            f.q(f"ATTACH 'differential:{n}' AS m (BRANCH 'integrated'); UPDATE m.x SET a=2; CALL differential_create_snapshot('m'); DETACH m;"
                f"ATTACH 'differential:{n}' AS t (BRANCH 't'); UPDATE t.x SET b='later'; CALL differential_create_snapshot('t'); DETACH t;")
            assert f.q(f'SELECT base_snapshot_id FROM {preview(n,"integrated","t")}') == 3
            assert f.q(f'SELECT snapshot_id FROM {merge(n,"integrated","t","again")}') == 7
            f.attach(n,'again')
            assert f.q("SELECT count(*) FROM m.x WHERE a=2 AND b='later'") == 1
            f.q('DETACH m')
            assert f.q(f'SELECT base_snapshot_id FROM {merge(n,"again","t","already_included")}') == 6
            # Main changes independently after the original fork; its original change
            # is still part of the merge result's ancestry through the first parent.
            f.q(f"ATTACH 'differential:{n}' AS d; UPDATE d.x SET a=4; CALL differential_create_snapshot('d'); DETACH d;")
            assert f.q(f'SELECT base_snapshot_id FROM {preview(n,"again","main")}') == 1
            assert f.q(f'SELECT conflicts FROM {preview(n,"again","main")}') == 1
            parents = records(repo,n)
            assert parents[1] == ['snapshot-parents-v1','root','0','0']
            assert parents[4] == ['snapshot-parents-v1','merge','2','3']
            assert parents[5] == ['snapshot-parents-v1','linear','4','0']
            assert parents[7] == ['snapshot-parents-v1','merge','5','6']
            assert parents[8] == ['snapshot-parents-v1','merge','7','6']
            ok('parent/child and repeated merges select the incorporated side; later changes merge once and later conflicting edits stay conflicts')

            nested = setup(f)
            f.q(f"CALL differential_create_branch('{nested}','child',snapshot=>2);"
                f"ATTACH 'differential:{nested}' AS c (BRANCH 'child'); UPDATE c.x SET b='child'; CALL differential_create_snapshot('c'); DETACH c;"
                f"ATTACH 'differential:{nested}' AS o (BRANCH 'o'); UPDATE o.x SET a=2; CALL differential_create_snapshot('o'); DETACH o;")
            assert f.q(f'SELECT base_snapshot_id FROM {preview(nested,"o","child")}') == 2
            f.q(f'SELECT * FROM {merge(nested,"o","child","nested")}')
            f.attach(nested,'nested'); assert f.q("SELECT count(*) FROM m.x WHERE a=2 AND b='child'") == 1; f.q('DETACH m')
            ok('nested fork after a published edit uses its fork ancestor while the parent advances')

            # Independent merges of the same pair create two incomparable best bases.
            crossed = setup(f)
            f.q(f'SELECT * FROM {merge(crossed,"o","t","left")}')
            f.q(f'SELECT * FROM {merge(crossed,"t","o","right")}')
            before = f.q('SELECT count(*) FROM differential_operations()')
            f.error(f'SELECT * FROM {preview(crossed,"left","right")}', 'merge_base_ambiguous')
            f.error(f'SELECT * FROM {merge(crossed,"left","right","ambiguous")}', 'merge_base_ambiguous')
            assert f.q('SELECT count(*) FROM differential_operations()') == before
            # Equal logical results must not choose either base or bypass ambiguity.
            f.q(f"ATTACH 'differential:{crossed}' AS l (BRANCH 'left'); ATTACH 'differential:{crossed}' AS r (BRANCH 'right');")
            assert f.q('SELECT count(*) FROM ((SELECT * FROM l.x EXCEPT ALL SELECT * FROM r.x) UNION ALL (SELECT * FROM r.x EXCEPT ALL SELECT * FROM l.x))') == 0
            f.q('DETACH l; DETACH r;')
            assert f.q(f"SELECT snapshot_id FROM differential_merge_snapshots('{crossed}',2,4,5,'explicit')") == 6
            f.q(f"CALL differential_expire_snapshots('{crossed}',versions=>[2]);")
            f.error(f'SELECT * FROM {preview(crossed,"left","right")}', 'merge_base_ambiguous')
            ok('criss-cross histories reject multiple best bases even for equal images or when one candidate expires; explicit base remains available')

            retired = f.setup('CREATE TABLE d.x(id INT PRIMARY KEY,a INT,b VARCHAR);',
                              'INSERT INTO d.x VALUES(1701,0,NULL);',
                              'UPDATE o.x SET a=1;', "UPDATE t.x SET b='new';")
            f.q(f'SELECT * FROM {merge(retired,"o","t","integrated")}')
            f.q(f"ATTACH 'differential:{retired}' AS t (BRANCH 't'); UPDATE t.x SET b='later'; CALL differential_create_snapshot('t'); DETACH t;")
            assert f.q(f'SELECT base_snapshot_id FROM {preview(retired,"integrated","t")}') == 3
            f.q(f"CALL differential_expire_snapshots('{retired}',versions=>[3]); CALL differential_cleanup_old_files(max_objects=>10000);")
            f.error(f'SELECT * FROM {preview(retired,"integrated","t")}', 'retired')
            # Snapshot 1 is still readable, but cannot be substituted for required base 3.
            assert f.q(f"SELECT count(*) FROM differential_snapshots('{retired}') WHERE snapshot_id=1") == 1
            assert 3 in records(repo,retired)
            f.q(f"CALL differential_expire_snapshots('{retired}',versions=>[1,2]);")
            kept_o = f.q(f"SELECT preservation_snapshot_id FROM differential_drop_branch('{retired}','o')")
            kept_main = f.q(f"SELECT preservation_snapshot_id FROM differential_drop_branch('{retired}','main')")
            # Dropping a branch intentionally preserves its live image. Expire those
            # additional snapshots too before testing that ancestry alone retains no bytes.
            f.q(f"CALL differential_expire_snapshots('{retired}',versions=>[{kept_o},{kept_main}]); CALL differential_cleanup_old_files(max_objects=>10000);")
            assert metadata(repo,"SELECT count(*) FROM reclaimed_roots WHERE root=(SELECT s.root FROM snapshots s JOIN databases d ON d.id=s.database_id WHERE d.name=? AND s.id=1)",(retired,)) == [[1]]
            assert 1 in records(repo,retired)
            ok('expired best base is never replaced by an older retained ancestor; ancestry metadata survives while unreferenced payload is reclaimed')

            restored = setup(f)
            # Restore to another branch's snapshot copies values, not that branch's history.
            f.q(f"CALL differential_restore('{restored}',3,branch=>'o');")
            restore_id = f.q(f"SELECT snapshot_id FROM differential_snapshots('{restored}') WHERE commit_message LIKE 'restored to%'")
            assert records(repo,restored)[restore_id] == ['snapshot-parents-v1','restore','2','0']
            assert f.q(f'SELECT base_snapshot_id FROM {preview(restored,"o","t")}') == 1
            # A new unsnapshotted state is preserved first and becomes the restore parent.
            f.q(f"ATTACH 'differential:{restored}' AS o (BRANCH 'o'); UPDATE o.x SET a=42; DETACH o;"
                f"CALL differential_restore('{restored}',1,branch=>'o');")
            chain = records(repo,restored); head = max(chain)
            assert chain[head][1:] == ['restore',str(head-1),'0']
            assert chain[head-1][1:] == ['linear',str(restore_id),'0']
            reused = setup(f)
            f.q(f"CALL differential_create_branch('{reused}','equal_root',snapshot=>2);"
                f"ATTACH 'differential:{reused}' AS e (BRANCH 'equal_root'); CALL differential_create_snapshot('e'); DETACH e;")
            assert f.q(f"SELECT count(DISTINCT root) FROM differential_snapshots('{reused}') WHERE snapshot_id IN(2,4)") == 1
            f.q(f"CALL differential_restore('{reused}',1,branch=>'o');")
            assert records(repo,reused)[5][1:] == ['restore','2','0']
            ok('restore follows the prior head or newly published preservation, never the copied target or equal-root event')

            f.q(f"CALL differential_clone_database('{nested}','cloned',snapshot=>2);")
            assert records(repo,'cloned')[1][1:] == ['clone','0','0']
            f.q("CALL differential_create_branch('cloned','child',snapshot=>1);")
            assert f.q(f'SELECT base_snapshot_id FROM {preview("cloned","main","child")}') == 1
            source = root/'native.duckdb'
            f.q(f"ATTACH '{source}' AS native; CREATE TABLE native.x(id INT PRIMARY KEY); INSERT INTO native.x VALUES(1); DETACH native;"
                f"CALL differential_import_database('{source}','imported'); CALL differential_create_branch('imported','child',snapshot=>1);")
            assert records(repo,'imported')[1][1:] == ['import','0','0']
            assert f.q(f'SELECT base_snapshot_id FROM {preview("imported","main","child")}') == 1
            ok('native imports and clones start new local ancestry roots; their subsequent forks support automatic selection')

            before = f.q('SELECT count(*) FROM differential_operations()')
            f.error(f'SELECT * FROM {merge(n,"again","t","limited",",max_ancestry_nodes=>1")}', 'merge_limit')
            assert f.q('SELECT count(*) FROM differential_operations()') == before
            assert_no_stage(f)
            ok('bounded ancestry walk fails before request creation, pins or output')
            legacy = setup(f)
            legacy_pending = setup(f)
            legacy_request = ",request_id=>'legacy-pending'"
            f.error(f'SELECT * FROM {merge(legacy_pending,"o","t","replayed",legacy_request+",max_input_rows=>1")}', 'merge_limit')
            unknown = setup(f)
            corrupt = setup(f)
            malformed = setup(f)
            unavailable = setup(f)
        finally: f.host.close()

        # Closed-host fault fixtures represent absent legacy metadata or corrupt records.
        with closing(sqlite3.connect(repo/'engine.sqlite')) as db, db:
            for name in (legacy,unknown):
                db.execute("DELETE FROM operations WHERE kind='snapshot_ancestry' AND database_id=(SELECT id FROM databases WHERE name=?)",(name,))
            payload = unpack(db.execute("SELECT payload FROM operations WHERE request='legacy-pending'").fetchone()[0])
            payload[4] = 'sibling-fork-v1'
            db.execute("UPDATE operations SET payload=? WHERE request='legacy-pending'",(pack(payload),))
            for name,fields in ((corrupt,['snapshot-parents-v1','linear','3','0']),
                                (unavailable,['snapshot-parents-v9','root','0','0'])):
                db.execute("UPDATE operations SET payload=? WHERE kind='snapshot_ancestry' AND snapshot_id=1 AND database_id=(SELECT id FROM databases WHERE name=?)",(pack(fields),name))
            db.execute("UPDATE operations SET payload='18446744073709551615:x' WHERE kind='snapshot_ancestry' AND snapshot_id=1 AND database_id=(SELECT id FROM databases WHERE name=?)",(malformed,))
        f = Fixture(repo)
        try:
            assert f.q(f'SELECT base_snapshot_id FROM {preview(legacy,"o","t")}') == 1
            assert f.q(f"SELECT count(*) FROM differential_merge_ancestry('{legacy}',2,3) WHERE selection_state='incomplete' AND NOT parents_known AND parents=[] AND selected_base IS NULL") == 2
            f.error(f'SELECT * FROM {preview(legacy,"main","o")}', 'merge_base_unavailable')
            f.q(f'SELECT * FROM {merge(legacy,"o","t","legacy")}')
            assert f.q(f"SELECT count(*) FROM differential_snapshots('{legacy}') WHERE commit_extra_info LIKE '%sibling-fork-v1%'") == 1
            assert f.q(f'SELECT snapshot_id FROM {merge(legacy_pending,"o","t","replayed",legacy_request)}') == 4
            assert f.q(f"SELECT count(*) FROM differential_operations() WHERE kind='snapshot_ancestry' AND snapshot_id=4 AND database_id=(SELECT database_id FROM differential_databases() WHERE database_name='{legacy_pending}')") == 1
            lineage = records(repo,legacy_pending)
            assert lineage.get(4) == ['snapshot-parents-v1','merge','2','3'], (legacy_pending,lineage)
            # Newly recorded edges can prove a base without knowing its older ancestry.
            f.q(f"CALL differential_create_branch('{unknown}','fresh',snapshot=>1);"
                f"ATTACH 'differential:{unknown}' AS child (BRANCH 'fresh'); UPDATE child.x SET a=7; CALL differential_create_snapshot('child'); DETACH child;")
            assert f.q(f'SELECT base_snapshot_id FROM {preview(unknown,"main","fresh")}') == 1
            f.error(f'SELECT * FROM {preview(corrupt,"o","t")}', 'corrupt_data')
            f.error(f'SELECT * FROM {preview(malformed,"o","t")}', 'corrupt_data')
            f.error(f'SELECT * FROM {preview(unavailable,"o","t")}', 'merge_unsupported')
        finally: f.host.close()
        ok('unknown legacy history has no inferred edges; proven sibling fallback and a proven common boundary work; cycles and unknown record versions fail')

        # Ordinary publication has the same atomic parent-record boundary as merge.
        folder = root/'snapshot-crash'
        prefix = f"LOAD '{EXTENSION}'; SET differential_repository='{folder}'; SET threads=2;"
        def run(sql, crash=False):
            env = dict(os.environ)
            if crash: env['DIFFERENTIAL_CONTROL_CRASH'] = 'snapshot_ancestry'
            r = subprocess.run([DUCKDB_CLI,'-init','/dev/null','-unsigned','-batch','-bail','-json'],
                               input=prefix+sql,text=True,capture_output=True,timeout=60,env=env)
            assert r.returncode == (86 if crash else 0), (r.stdout,r.stderr,r.returncode)
            return r.stdout
        run("ATTACH 'differential:crash' AS d; CREATE TABLE d.x(id INT PRIMARY KEY); INSERT INTO d.x VALUES(1); CALL differential_create_snapshot('d');")
        statement = "ATTACH 'differential:crash' AS d; CALL differential_create_snapshot('d',request_id=>'crash-snapshot');"
        run(statement,True)
        assert list(records(folder,'crash')) == [1]
        run(statement); run(statement)
        assert records(folder,'crash')[2] == ['snapshot-parents-v1','linear','1','0']
        with closing(sqlite3.connect(folder/'engine.sqlite')) as db, db:
            assert db.execute('SELECT count(*) FROM snapshots').fetchone()[0] == 2
            assert db.execute('PRAGMA integrity_check').fetchone()[0] == 'ok'
        ok('ordinary snapshot death between ancestry recording and commit rolls back both; request replay publishes exactly one parent record')
    evidence_path('merge-ancestry.json').write_text(json.dumps({'passed':passed},indent=2)+'\n')


if __name__ == '__main__': main()
