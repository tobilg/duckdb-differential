#!/usr/bin/env python3
"""Explicitly capture a small immutable compatibility fixture with a known producer.

Never invoked by normal tests. Existing fixture names cannot be overwritten.
The hook-enabled producer is required to leave a real interrupted target request.
The release profile instead records data written by a published release artifact (selected
with DIFFERENTIAL_EXTENSION): ordinary snapshots and branches, without merge records.
"""
import argparse
import gzip
import hashlib
import io
import json
import os
from pathlib import Path
import platform
import sqlite3
import subprocess
import sys
import tarfile
import tempfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT/'test/integration'))
from paths import DUCKDB_CLI, EXTENSION
from merge_types import CASES, typed


def decode(text):
    rows = []; decoder = json.JSONDecoder()
    while text.strip():
        text = text.lstrip(); row, end = decoder.raw_decode(text)
        rows.append(row); text = text[end:]
    return rows


def sha(path): return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--label', required=True, help='Unique candidate/release fixture identity')
    parser.add_argument('--output', type=Path, required=True, help='New manifest .json path beside its .tar.gz')
    parser.add_argument('--profile', choices=('classic','native-context','release'), default='classic')
    parser.add_argument('--release-source', help='release profile: where the producer artifact was installed from')
    args = parser.parse_args()
    if args.output.suffix != '.json': parser.error('--output must end in .json')
    archive = args.output.with_suffix('.tar.gz')
    if args.output.exists() or archive.exists(): parser.error('Fixtures are immutable; choose a new name')
    env = {k:v for k,v in os.environ.items() if not k.startswith('DIFFERENTIAL_')}
    with tempfile.TemporaryDirectory(prefix='differential-fixture-producer-') as tmp:
        repo = Path(tmp)/'repo'
        prefix = (f"LOAD '{EXTENSION}'; SET differential_repository='{repo}'; SET threads=2;"
                  "SET memory_limit='128MB'; SET differential_auto_gc=false;")
        native = args.profile=='native-context'
        release = args.profile=='release'
        if release and not args.release_source: parser.error('--release-source is required for the release profile')
        choices = Path(tmp)/'choices.duckdb'
        if native:
            prefix += "SET TimeZone='America/New_York'; SET Calendar='gregorian'; SET enable_view_dependencies=true;"
        native_ddl = ("CREATE TABLE d.native_values(id UUID PRIMARY KEY,"+
                      ','.join(f'c{i} {case[0]}' for i,case in enumerate(CASES))+
                      ",note VARCHAR,ts TIMESTAMPTZ,day DATE,CHECK(CAST(ts AS DATE)=day));"
                      "INSERT INTO d.native_values VALUES('00000000-0000-0000-0000-000000000001',"+
                      ','.join(typed(case[1],case[0]) for case in CASES)+
                      ",'base',TIMESTAMPTZ '2020-01-02 01:00:00+00',DATE '2020-01-01');"
                      'USE d; CREATE VIEW native_visible AS SELECT * FROM native_values; USE memory;') if native else ''
        def run(sql, *, crash=None, error=None):
            child = subprocess.run([DUCKDB_CLI,'-init','/dev/null','-unsigned','-batch','-bail','-json'],
                                   input=prefix+sql, text=True, capture_output=True, timeout=60,
                                   env=dict(env, **({'DIFFERENTIAL_MERGE_CRASH':crash} if crash else {})))
            expected = 86 if crash else 1 if error else 0
            assert child.returncode == expected, (child.returncode,child.stdout,child.stderr)
            if error: assert error in child.stderr, child.stderr
            return decode(child.stdout) if not error and not crash else []
        hooks = run("SELECT status FROM differential_status() WHERE capability='test_hooks'")[0][0]['status']
        assert hooks.startswith('compiled') if not release else hooks=='absent', hooks
        extension_version = run("SELECT extension_version FROM duckdb_extensions() WHERE extension_name='differential'")[0][0]['extension_version']
        setup = """
ATTACH 'differential:compat' AS d;
CREATE TABLE d.items(id INT PRIMARY KEY,a INT CHECK(a>=0),b VARCHAR,amount DECIMAL(18,3),flag BOOLEAN DEFAULT false);
CREATE INDEX by_a ON d.items(a);
INSERT INTO d.items VALUES(1,0,'base',1.250,false),(2,0,NULL,NULL,NULL);
CREATE TABLE d.composite(k INT,s VARCHAR,v DECIMAL(18,3),PRIMARY KEY(k,s));
INSERT INTO d.composite VALUES(1,'key',NULL),(2,'other',2.125);
USE d; CREATE VIEW visible AS SELECT id,a,b,amount,flag FROM items; USE memory;
CALL differential_create_snapshot('d');
CALL differential_create_branch('d','ours',snapshot=>1);
CALL differential_create_branch('d','theirs',snapshot=>1); DETACH d;
ATTACH 'differential:compat' AS o (BRANCH 'ours');
UPDATE o.items SET a=10 WHERE id=1; UPDATE o.composite SET v=3.500 WHERE k=1;
CALL differential_create_snapshot('o'); DETACH o;
ATTACH 'differential:compat' AS t (BRANCH 'theirs');
UPDATE t.items SET b='theirs' WHERE id=1; UPDATE t.items SET amount=2.500,flag=true WHERE id=2;
CALL differential_create_snapshot('t'); DETACH t;
"""
        if native:
            setup = setup.replace("CALL differential_create_snapshot('d');",native_ddl+"CALL differential_create_snapshot('d');",1)
            for alias,side in (('o',2),('t',3)):
                updates = ','.join(f'c{i}={typed(case[side],case[0])}' for i,case in enumerate(CASES))
                if alias=='o': updates += ",note='ours'"
                setup = setup.replace(f"CALL differential_create_snapshot('{alias}');",
                                      f'UPDATE {alias}.native_values SET {updates};'+f"CALL differential_create_snapshot('{alias}');",1)
        run(setup)
        if release:
            write_release(args, repo, run, extension_version, archive)
            return
        if native:
            mapping = ','.join(f"'c{i}':'custom'" for i in range(len(CASES)))
            run(f"ATTACH 'differential:compat' AS source (BRANCH 'theirs',READ_ONLY); ATTACH '{choices}' AS decisions; "
                f"CREATE TABLE decisions.fixes AS SELECT *,'cells' AS _merge_action,map{{{mapping}}} AS _merge_columns FROM source.native_values; CHECKPOINT decisions;")
        def guarded(target, destination, request):
            return (f"SELECT snapshot_id FROM differential_merge_branch('compat','{target}','theirs','{destination}',"
                    f"request_id=>'{request}',expected_base_snapshot_id=>1,expected_ours_snapshot_id=>2,expected_theirs_snapshot_id=>3)")
        requests = {
            'fixture-snapshot': "SELECT snapshot_id FROM differential_merge_snapshots('compat',1,2,3,'snapshot_result',request_id=>'fixture-snapshot')",
            'fixture-guarded': guarded('ours','guarded_result','fixture-guarded'),
            'fixture-target': guarded('target','target','fixture-target'),
            'fixture-pending': guarded('pending','pending','fixture-pending'),
            'fixture-aborted': "SELECT snapshot_id FROM differential_merge_snapshots('compat',1,2,3,'aborted_result',request_id=>'fixture-aborted')",
        }
        if native:
            requests = {name:sql[:-1]+",resolution_tables=>map{'main.native_values':'decisions.main.fixes'})" for name,sql in requests.items()}
            prefix += f"ATTACH '{choices}' AS decisions (READ_ONLY);"
        for request, expected in [('fixture-snapshot',4),('fixture-guarded',5)]:
            assert run(requests[request])[0][0]['snapshot_id']==expected
        run("CALL differential_create_branch('compat','target',snapshot=>2); CALL differential_create_branch('compat','pending',snapshot=>2)")
        assert run(requests['fixture-target'])[0][0]['snapshot_id']==6
        run(requests['fixture-aborted'][:-1]+',max_input_rows=>1)', error='merge_limit')
        aborted = run("SELECT operation_id FROM differential_merge_requests('compat',request_id=>'fixture-aborted')")[0][0]['operation_id']
        run(f"CALL differential_abort_merge('compat','{aborted}',request_id=>'fixture-abort'); CALL differential_expire_snapshots('compat',versions=>[4])")
        # Keep a durable fence and staging, created by the real code, not SQL edits.
        run(requests['fixture-pending'], crash='staged')
        inspection = run("SELECT * FROM differential_merge_requests('compat')")[0]
        request_rows = {r['request_id']:r for r in inspection}
        assert request_rows['fixture-pending']['state']=='target_locked'
        assert request_rows['fixture-aborted']['state']=='aborted'
        with sqlite3.connect(repo/'engine.sqlite') as db:
            assert db.execute('PRAGMA integrity_check').fetchone()[0]=='ok'
            db.execute('PRAGMA wal_checkpoint(TRUNCATE)')
            snapshots = [dict(zip(('snapshot_id','root','retired'), r)) for r in db.execute('SELECT id,root,retired FROM snapshots ORDER BY id')]
            assert len(snapshots)==6 and snapshots[3]['retired']==1
        files = []
        packed = io.BytesIO()
        with gzip.GzipFile(fileobj=packed, mode='wb', mtime=0) as compressed:
            with tarfile.open(fileobj=compressed, mode='w', format=tarfile.PAX_FORMAT) as tar:
                for path in sorted(repo.rglob('*')):
                    if not path.is_file() or path.name.endswith('-shm'): continue
                    name = path.relative_to(repo).as_posix(); data = path.read_bytes()
                    info = tarfile.TarInfo(name); info.size=len(data); info.mode=0o600; info.mtime=0
                    tar.addfile(info,io.BytesIO(data))
                    files.append({'path':name,'size':len(data),'sha256':hashlib.sha256(data).hexdigest()})
        assert sum(f['size'] for f in files)<16777216 and len(files)<1000
        args.output.parent.mkdir(parents=True,exist_ok=True)
        with archive.open('xb') as out: out.write(packed.getvalue())
        source_files = [ROOT/'CMakeLists.txt', *(p for p in (ROOT/'src').rglob('*') if p.is_file())]
        digest = hashlib.sha256()
        for path in sorted(source_files): digest.update(path.relative_to(ROOT).as_posix().encode()+b'\0'+path.read_bytes())
        report = {'fixture_format':1,'label':args.label,'publication_status':'candidate',
                  'producer':{'duckdb':subprocess.check_output([DUCKDB_CLI,'-init','/dev/null','-no-agent','--version'],text=True).strip(),
                              'extension_sha256':sha(EXTENSION),'extension_version':extension_version,
                              'test_hooks':True,'platform':platform.platform(),
                              'git_head':subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),
                              'git_dirty':bool(subprocess.check_output(['git','status','--porcelain'],cwd=ROOT,text=True).strip()),
                              'source_sha256':digest.hexdigest(),'generator_sha256':sha(Path(__file__))},
                  'archive':archive.name,'archive_sha256':sha(archive),'files':files,
                  'store_format':1,'control_format':1,'export_format':1,
                  'diagnostics_present':'attempt_count' in inspection[0],
                  'requests':requests,'request_rows':request_rows,'snapshots':snapshots}
        if native:
            report.update(profile=args.profile,native_types=[c[0] for c in CASES],
                          expression_context={'version':'native-expression-context-v1','timezone':'America/New_York','calendar':'gregorian'},
                          resolution_source_included=False,
                          native_expected={side:"SELECT '00000000-0000-0000-0000-000000000001'::UUID,"+
                              ','.join(typed(case[index],case[0]) for case in CASES)+
                              f",'{note}'::VARCHAR,TIMESTAMPTZ '2020-01-02 01:00:00+00',DATE '2020-01-01'"
                              for side,index,note in [('base',1,'base'),('ours',2,'ours'),('theirs',3,'base'),('merged',3,'ours')]})
        with args.output.open('x') as out: out.write(json.dumps(report,indent=2)+'\n')
        print(f'Created {args.output}: {len(files)} files, {archive.stat().st_size} compressed bytes')


def pack(repo, archive):
    files = []
    packed = io.BytesIO()
    with gzip.GzipFile(fileobj=packed, mode='wb', mtime=0) as compressed:
        with tarfile.open(fileobj=compressed, mode='w', format=tarfile.PAX_FORMAT) as tar:
            for path in sorted(repo.rglob('*')):
                if not path.is_file() or path.name.endswith('-shm'): continue
                name = path.relative_to(repo).as_posix(); data = path.read_bytes()
                info = tarfile.TarInfo(name); info.size=len(data); info.mode=0o600; info.mtime=0
                tar.addfile(info,io.BytesIO(data))
                files.append({'path':name,'size':len(data),'sha256':hashlib.sha256(data).hexdigest()})
    assert sum(f['size'] for f in files)<16777216 and len(files)<1000
    with archive.open('xb') as out: out.write(packed.getvalue())
    return files


def write_release(args, repo, run, extension_version, archive):
    """Ordinary 1.0-era history: three snapshots on sibling branches, no merge or parent records."""
    with sqlite3.connect(repo/'engine.sqlite') as db:
        assert db.execute('PRAGMA integrity_check').fetchone()[0]=='ok'
        db.execute('PRAGMA wal_checkpoint(TRUNCATE)')
        store_format = db.execute('PRAGMA user_version').fetchone()[0]
        control_format = db.execute('SELECT version FROM control_format').fetchone()[0]
        kinds = sorted(r[0] for r in db.execute('SELECT DISTINCT kind FROM operations'))
        assert not [k for k in kinds if k.startswith('merge') or k=='snapshot_ancestry'], kinds
        snapshots = [dict(zip(('snapshot_id','root','retired'), r)) for r in db.execute('SELECT id,root,retired FROM snapshots ORDER BY id')]
        assert len(snapshots)==3
    args.output.parent.mkdir(parents=True,exist_ok=True)
    files = pack(repo, archive)
    report = {'fixture_format':1,'label':args.label,'publication_status':'release','profile':'release',
              'producer':{'duckdb':subprocess.check_output([DUCKDB_CLI,'-init','/dev/null','-no-agent','--version'],text=True).strip(),
                          'extension_sha256':sha(EXTENSION),'extension_version':extension_version,
                          'installed_from':args.release_source,'test_hooks':False,'platform':platform.platform(),
                          'generator_sha256':sha(Path(__file__))},
              'archive':archive.name,'archive_sha256':sha(archive),'files':files,
              'store_format':store_format,'control_format':control_format,'export_format':2,
              'operation_kinds':kinds,'snapshots':snapshots}
    with args.output.open('x') as out: out.write(json.dumps(report,indent=2)+'\n')
    print(f'Created {args.output}: {len(files)} files, {archive.stat().st_size} compressed bytes')


if __name__=='__main__': main()
