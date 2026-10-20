#!/usr/bin/env python3
"""TPC-H benchmarks: native DuckDB, Differential (local) and Differential on S3 (a disposable,
disk-backed MinIO), repeatable from this script.

    python3 benchmarks/tpch.py [--scale-factor 1] [--threads 4] [--runs 5] [--cold-runs 3]
                               [--suites read_local,read_local_cold,read_s3_cold,write]
                               [--memory-limit 4GB] [--drop-os-cache] [--output build/evidence/tpch.json]

Suites (TPC-H Q1-Q22; every measured execution materializes the full result; results are hashed and
must match native):
- read_local: one process per backend, 1 warm-up and --runs measured executions per query.
  Backends: native file; Differential default store; local pool (DATA_PATH '/dir'); the local copy of
  an S3 database.
- read_local_cold: the same backends, one new process per execution (--cold-runs), so DuckDB's buffer
  pool and the extension's caches start empty. The OS page cache stays warm unless --drop-os-cache
  (needs root: `purge` on macOS, /proc/sys/vm/drop_caches on Linux).
- read_s3_cold: one new process per execution, every S3 read path: native .duckdb through httpfs;
  an export attached from its branch head (memory cache only); the same with a warm repository disk
  cache; an evicted snapshot read through its export; a follower (FOLLOW true) of the shipping
  branch; a shared database's writable branch joined from S3; a branch recovered with
  differential_recover_branch.
- write: CREATE TABLE lineitem AS ... and CHECKPOINT into a new database (--write-runs times):
  native (and its upload to S3); Differential default store; local pool; S3 database with a snapshot
  export; with SHIP_WAL 'async' and 'sync'; a shared database.

Data is generated once per scale factor with DuckDB's tpch extension into build/benchmarks/ and
copied into each backend (COPY FROM DATABASE; timed as `load`). Render with tpch_report.py."""
import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import statistics
import subprocess
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'test/integration'))
from paths import EXTENSION  # noqa: E402
from stock_capi import Host  # noqa: E402
from minio_server import create_bucket, minio_server, put_object  # noqa: E402

SUITES = ['read_local', 'read_local_cold', 'read_s3_cold', 'write']
parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
parser.add_argument('--scale-factor', type=float, default=1.0)
parser.add_argument('--threads', type=int, default=4)
parser.add_argument('--memory-limit', default='', help="DuckDB memory_limit (default: DuckDB's)")
parser.add_argument('--runs', type=int, default=5, help='read_local: measured executions after one warm-up')
parser.add_argument('--cold-runs', type=int, default=3, help='cold suites: processes per query')
parser.add_argument('--write-runs', type=int, default=3)
parser.add_argument('--queries', default='1-22', help='e.g. 1-22 or 1,6,14')
parser.add_argument('--suites', default=','.join(SUITES))
parser.add_argument('--drop-os-cache', action='store_true')
parser.add_argument('--work-dir', default='', help='scratch directory (default: a temporary one)')
parser.add_argument('--output', default=str(ROOT / 'build/evidence/tpch.json'))
options = parser.parse_args()
selected = options.suites.split(',')
assert all(s in SUITES for s in selected), selected
QUERIES = []
for part in options.queries.split(','):
    low, _, high = part.partition('-')
    QUERIES += list(range(int(low), int(high or low) + 1))
CACHE = ROOT / 'build/benchmarks'
EXTENSIONS = CACHE / 'extensions'
BUCKET = 'differential-tpch'
results, writes, loads = [], [], []


def log(*args):
    print(*args, file=sys.stderr, flush=True)


def prepare():
    """The extension directory (tpch, httpfs) and the generated data, cached between runs."""
    CACHE.mkdir(parents=True, exist_ok=True)
    data = CACHE / f'tpch-sf{options.scale_factor:g}.duckdb'
    host = Host()
    conn = host.connect()
    host.query(conn, f"SET extension_directories=['{EXTENSIONS}']")
    for extension in ('tpch', 'httpfs'):
        host.query(conn, f"INSTALL {extension}")
    host.query(conn, "LOAD tpch")
    if not data.exists():
        log(f'generating TPC-H SF{options.scale_factor:g}')
        partial = data.with_suffix('.partial')
        partial.unlink(missing_ok=True)
        host.query(conn, f"ATTACH '{partial}' AS g")
        host.query(conn, "USE g")
        host.query(conn, f"CALL dbgen(sf = {options.scale_factor})")
        host.query(conn, "USE memory")
        host.query(conn, "DETACH g")
        partial.rename(data)
    texts = {n: host.text(conn, f"SELECT query FROM tpch_queries() WHERE query_nr = {n}") for n in QUERIES}
    host.close()
    return data, texts


def worker(spec, env=None):
    """Run tpch_worker.py with `spec`; returns its JSON lines."""
    spec = dict(spec, extension_dir=str(EXTENSIONS), threads=options.threads, memory_limit=options.memory_limit)
    with tempfile.NamedTemporaryFile('w', suffix='.json', delete=False) as handle:
        json.dump(spec, handle)
    try:
        r = subprocess.run([sys.executable, str(ROOT / 'benchmarks/tpch_worker.py'), handle.name], text=True,
                           capture_output=True, env=env or os.environ, timeout=3600)
    finally:
        os.unlink(handle.name)
    if r.returncode:
        raise RuntimeError(f"worker failed ({spec.get('setup')}): {r.stderr[-2000:]}")
    return [json.loads(line) for line in r.stdout.splitlines() if line.startswith('{')]


def drop_os_cache():
    if not options.drop_os_cache:
        return
    command = ['purge'] if platform.system() == 'Darwin' else ['sh', '-c', 'sync; echo 3 > /proc/sys/vm/drop_caches']
    subprocess.run(command, check=True)


def record(suite, variant, query, times, requests=None, hashes=()):
    results.append({'suite': suite, 'variant': variant, 'query': query, 'times_ms': times,
                    'median_ms': statistics.median(times), 'stdev_ms': statistics.stdev(times) if len(times) > 1 else 0,
                    'min_ms': min(times), 'requests': requests or {}, 'hash': next((h for h in hashes if h), None)})


def measure_warm(suite, variant, repository, setup, texts, env=None, extensions=()):
    lines = worker({'repository': str(repository), 'setup': setup, 'extensions': list(extensions), 'warmups': 1,
                    'runs': options.runs, 'hash': True, 'stats': True,
                    'queries': [{'number': n, 'sql': texts[n]} for n in QUERIES]}, env)
    for line in lines:
        if 'query' in line:
            record(suite, variant, line['query'], line['times_ms'], line['requests'], [line.get('hash')])


def measure_cold(suite, variant, repository, setup, texts, env=None, extensions=(), before=None):
    for n in QUERIES:
        times, hashes, requests = [], [], []
        for run in range(options.cold_runs):
            if before:
                before(n)
            drop_os_cache()
            line = worker({'repository': str(repository), 'setup': setup, 'extensions': list(extensions),
                           'runs': 1, 'hash': run == 0, 'stats': True,
                           'queries': [{'number': n, 'sql': texts[n]}]}, env)[0]
            times += line['times_ms']
            hashes.append(line.get('hash'))
            requests.append(line['requests'])
        median_requests = {k: statistics.median(r[k] for r in requests) for k in requests[0]}
        record(suite, variant, n, times, median_requests, hashes)
        log(f'{suite} {variant} Q{n}: {statistics.median(times):.0f} ms')


def load(variant, repository, attach, source, extra=(), env=None):
    """Copy the generated database into a backend; timed."""
    steps = [{'name': 'copy', 'sql': f"COPY FROM DATABASE src TO db"},
             {'name': 'checkpoint', 'sql': "CHECKPOINT db"}] + list(extra) + [{'name': 'detach', 'sql': "DETACH db"}]
    lines = worker({'repository': str(repository),
                    'setup': [attach, f"ATTACH '{source}' AS src (READ_ONLY)"], 'after': steps}, env)
    loads.append({'variant': variant, **{line['step']: line['ms'] for line in lines if 'step' in line}})
    log(f'loaded {variant}: {loads[-1]}')
    return {line['step']: line.get('value') for line in lines if 'step' in line}


def minio_env(credentials):
    return dict(os.environ, **{k: credentials[k] for k in ('AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY',
                                                          'AWS_EC2_METADATA_DISABLED', 'AWS_DEFAULT_REGION')})


def read_local_suites(variant, repository, setup, texts, env=None):
    for suite in ('read_local', 'read_local_cold'):
        if suite in selected:
            log(f'{suite}: {variant}')
            (measure_warm if suite == 'read_local' else measure_cold)(suite, variant, repository, setup, texts, env)


def main():
    data, texts = prepare()
    local = {'read_local', 'read_local_cold'} & set(selected)
    with tempfile.TemporaryDirectory(prefix='differential-tpch-', dir=options.work_dir or None) as tmp:
        work = Path(tmp)
        # Disk is scarce: each backend is loaded, measured and deleted before the next one.
        if local:
            read_local_suites('native', work / 'repo-native', [f"ATTACH '{data}' AS db (READ_ONLY)", "USE db"], texts)
            for variant, extra in (('differential', ''), ('differential_pool', f" (DATA_PATH '{work / 'pool'}')")):
                repository = work / f'repo-{variant}'
                attach = f"ATTACH 'differential:tpch' AS db{extra}"
                load(variant, repository, attach, data)
                read_local_suites(variant, repository, [attach.replace(' AS db', ' AS db', 1), "USE db"], texts)
                shutil.rmtree(repository, ignore_errors=True)
                shutil.rmtree(work / 'pool', ignore_errors=True)
        if local or 'read_s3_cold' in selected:
            read_s3(work, data, texts)
        if 'write' in selected:
            write_suite(work, data)
    environment = {'platform': platform.platform(), 'machine': platform.machine(), 'cpus': os.cpu_count(),
                   'python': platform.python_version(), 'measured_at': datetime.datetime.now(datetime.timezone.utc).isoformat()}
    git = lambda *a: subprocess.run(['git', '-C', str(ROOT), *a], capture_output=True, text=True).stdout.strip()  # noqa: E731
    environment.update(commit=git('rev-parse', 'HEAD'),
                       uncommitted_changes=bool(git('status', '--porcelain', '--untracked-files=no')),
                       extension_sha256=hashlib.sha256(Path(EXTENSION).read_bytes()).hexdigest(),
                       memory_gib=memory_gib())
    host = Host()
    conn = host.connect()
    environment['duckdb_version'] = host.text(conn, "SELECT library_version FROM pragma_version()")
    host.query(conn, f"LOAD '{EXTENSION}'")
    environment['extension'] = str(EXTENSION)
    environment['test_hooks'] = host.text(conn, "SELECT status FROM differential_status() WHERE capability = 'test_hooks'")
    host.close()
    # Every backend must return native's results.
    mismatches = []
    for suite in ('read_local', 'read_local_cold', 'read_s3_cold'):
        reference = {r['query']: r['hash'] for r in results if r['suite'] == suite and r['variant'] in ('native', 'native_httpfs')}
        mismatches += [f"{r['suite']} {r['variant']} Q{r['query']}" for r in results
                       if r['suite'] == suite and r['hash'] and reference.get(r['query']) and r['hash'] != reference[r['query']]]
    output = Path(options.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps({'environment': environment, 'scale_factor': options.scale_factor,
                                  'threads': options.threads, 'memory_limit': options.memory_limit or 'default',
                                  'runs': options.runs, 'cold_runs': options.cold_runs, 'write_runs': options.write_runs,
                                  'drop_os_cache': options.drop_os_cache, 'queries': QUERIES, 'suites': selected,
                                  'loads': loads, 'results': results, 'writes': writes, 'mismatches': mismatches},
                                 indent=2) + '\n')
    print('Results:', output)
    if mismatches:
        sys.exit('result mismatch: ' + ', '.join(mismatches))


def memory_gib():
    try:
        if platform.system() == 'Darwin':
            return int(subprocess.run(['sysctl', '-n', 'hw.memsize'], capture_output=True, text=True).stdout) / 2 ** 30
        return os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES') / 2 ** 30
    except Exception:
        return None


def read_s3(work, data, texts):
    suite = 'read_s3_cold'
    s3_reads = suite in selected
    if s3_reads:
        # Native: the .duckdb file uploaded once, read through httpfs (its external file cache starts empty).
        with minio_server(work / mkdir(work / 'minio-native')) as (endpoint, credentials, _):
            env = minio_env(credentials)
            create_bucket(endpoint, credentials, BUCKET)
            put_object(endpoint, credentials, BUCKET, 'native/tpch.duckdb', str(data))
            secret = (f"CREATE SECRET bench (TYPE s3, KEY_ID '{credentials['AWS_ACCESS_KEY_ID']}', "
                      f"SECRET '{credentials['AWS_SECRET_ACCESS_KEY']}', ENDPOINT '{endpoint.split('://', 1)[1]}', "
                      "URL_STYLE 'path', USE_SSL false, REGION 'us-east-1')")
            log(f'{suite}: native_httpfs')
            measure_cold(suite, 'native_httpfs', work / 'repo-httpfs',
                         [secret, f"ATTACH 's3://{BUCKET}/native/tpch.duckdb' AS db (READ_ONLY)", "USE db"], texts, env,
                         extensions=['httpfs'])
        shutil.rmtree(work / 'minio-native', ignore_errors=True)
    # The S3 database: its local copy, its exported snapshot, its shipped WAL.
    with minio_server(work / mkdir(work / 'minio-s3')) as (endpoint, credentials, _):
        env = minio_env(credentials)
        create_bucket(endpoint, credentials, BUCKET)
        s3 = f"ENDPOINT '{endpoint}', PATH_STYLE true, ALLOW_HTTP true"
        location = f's3://{BUCKET}/tpch/'
        writer = work / 'repo-s3'
        values = load('differential_s3', writer, f"ATTACH 'differential:tpch' AS db (DATA_PATH '{location}', {s3})", data,
                      [{'name': 'snapshot', 'sql': "SELECT snapshot_id FROM differential_create_snapshot('db')"}], env)
        snapshot = values['snapshot']
        # Shipping turned on after the load: the chain's base reuses the export's packs, and one small
        # change ships as an epoch (followers and recovery then read base plus epochs).
        worker({'repository': str(writer), 'setup': [
            f"ATTACH 'differential:tpch' AS db (SHIP_WAL 'async', SHIP_INTERVAL 0)",
            "CREATE TABLE db.shipped AS SELECT 1 AS v", "CHECKPOINT db", "INSERT INTO db.shipped VALUES (2)",
            "DETACH db"]}, env)
        read_local_suites('differential_s3_local', writer, ["ATTACH 'differential:tpch' AS db", "USE db"], texts, env)
        if s3_reads:
            head = f"ATTACH 'differential:{location}heads/tpch/main' AS db ({s3})"
            log(f'{suite}: export (memory cache only)')
            measure_cold(suite, 'export', work / 'repo-export', [head, "USE db"], texts, env)
            cache_repo = work / 'repo-disk-cache'
            cached = ["SET differential_remote_cache_size = '8GB'", head, "USE db"]
            fill = lambda n: worker({'repository': str(cache_repo), 'setup': cached,  # noqa: E731
                                     'queries': [{'number': n, 'sql': texts[n]}]}, env)
            log(f'{suite}: export with a warm disk cache')
            measure_cold(suite, 'export_disk_cache', cache_repo, cached, texts, env, before=fill)
            shutil.rmtree(cache_repo, ignore_errors=True)
            log(f'{suite}: follower')
            measure_cold(suite, 'follower', work / 'repo-follower',
                         [f"ATTACH 'differential:{location}heads/tpch/main' AS db ({s3}, FOLLOW true)", "USE db"], texts, env)
            recovered = work / 'repo-recovered'
            worker({'repository': str(recovered),
                    'setup': [f"SELECT * FROM differential_recover_branch('{location}heads/tpch/main', 'tpch', endpoint "
                              f"=> '{endpoint}', path_style => true, allow_http => true)"]}, env)
            log(f'{suite}: recovered branch')
            measure_cold(suite, 'recovered_branch', recovered, ["ATTACH 'differential:tpch' AS db", "USE db"], texts, env)
            shutil.rmtree(recovered, ignore_errors=True)
            # The writer's own snapshot, evicted: read through its export.
            worker({'repository': str(writer),
                    'setup': [f"CALL differential_evict_snapshots('tpch', versions => [{snapshot}])"]}, env)
            log(f'{suite}: evicted snapshot')
            measure_cold(suite, 'evicted_snapshot', writer,
                         [f"ATTACH 'differential:tpch' AS db (SNAPSHOT_VERSION {snapshot})", "USE db"], texts, env)
        shutil.rmtree(writer, ignore_errors=True)
    shutil.rmtree(work / 'minio-s3', ignore_errors=True)
    if not s3_reads:
        return
    # A shared database: written by one repository, joined (writable, read from S3) by another.
    with minio_server(work / mkdir(work / 'minio-shared')) as (endpoint, credentials, _):
        env = minio_env(credentials)
        create_bucket(endpoint, credentials, BUCKET)
        s3 = f"ENDPOINT '{endpoint}', PATH_STYLE true, ALLOW_HTTP true"
        shared = f"ATTACH 'differential:tpch_shared' AS db (WRITERS 'shared', DATA_PATH 's3://{BUCKET}/shared/', {s3}"
        load('differential_shared', work / 'repo-shared-writer', shared + ")", data,
             [{'name': 'snapshot', 'sql': "SELECT snapshot_id FROM differential_create_snapshot('db')"}], env)
        shutil.rmtree(work / 'repo-shared-writer', ignore_errors=True)
        log(f'{suite}: shared database joiner')
        measure_cold(suite, 'shared_joiner', work / 'repo-shared-reader',
                     [shared + ", ON_BUSY 'ignore', PUBLISH_ON_CHECKPOINT false)", "USE db"], texts, env)
        shutil.rmtree(work / 'repo-shared-reader', ignore_errors=True)
    shutil.rmtree(work / 'minio-shared', ignore_errors=True)


def mkdir(path):
    path.mkdir(parents=True, exist_ok=True)
    return path.name


def write_suite(work, data):
    """CTAS lineitem + CHECKPOINT into a new database per run, per backend."""
    rows = None
    variants = [('native', None), ('differential', None), ('differential_pool', None), ('differential_s3', ''),
                ('differential_s3_ship_async', ", SHIP_WAL 'async'"), ('differential_s3_ship_sync', ", SHIP_WAL 'sync'"),
                ('differential_shared', ", WRITERS 'shared'")]
    for variant, s3_option in variants:
        # S3 variants get a MinIO of their own, deleted afterwards (disk space).
        minio_data = work / f'minio-write-{variant}'
        minio_data.mkdir()
        context = minio_server(minio_data) if s3_option is not None else None
        endpoint = credentials = None
        env = os.environ
        if context:
            endpoint, credentials, _ = context.__enter__()
            env = dict(os.environ, **{k: credentials[k] for k in ('AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY',
                                                                  'AWS_EC2_METADATA_DISABLED', 'AWS_DEFAULT_REGION')})
            create_bucket(endpoint, credentials, BUCKET)
        try:
            for run in range(options.write_runs):
                repository = work / f'write-{variant}-{run}'
                target = work / f'write-{variant}-{run}.duckdb'
                if variant == 'native':
                    attach = f"ATTACH '{target}' AS db"
                elif variant == 'differential':
                    attach = "ATTACH 'differential:w' AS db"
                elif variant == 'differential_pool':
                    attach = f"ATTACH 'differential:w' AS db (DATA_PATH '{work / f'write-pool-{run}'}')"
                else:
                    attach = (f"ATTACH 'differential:w' AS db (DATA_PATH 's3://{BUCKET}/write-{run}/', ENDPOINT "
                              f"'{endpoint}', PATH_STYLE true, ALLOW_HTTP true{s3_option})")
                steps = [{'name': 'create', 'sql': "CREATE TABLE db.lineitem AS FROM src.lineitem"},
                         {'name': 'checkpoint', 'sql': "CHECKPOINT db"}]
                if s3_option is not None:
                    steps.append({'name': 'export', 'sql': "SELECT snapshot_id FROM differential_create_snapshot('db')"})
                steps += [{'name': 'detach', 'sql': "DETACH db"},
                          {'name': 'rows', 'sql': f"SELECT count(*) FROM src.lineitem"},
                          {'name': 's3_put_calls', 'sql': "SELECT value FROM differential_storage_stats() WHERE metric = 's3_put_calls'"},
                          {'name': 'ship_epoch_bytes', 'sql': "SELECT value FROM differential_storage_stats() WHERE metric = 'ship_epoch_bytes'"}]
                lines = worker({'repository': str(repository), 'setup': [attach, f"ATTACH '{data}' AS src (READ_ONLY)"],
                                'after': steps}, env)
                step = {line['step']: line for line in lines if 'step' in line}
                rows = step['rows']['value']
                entry = {'variant': variant, 'run': run, 'rows': rows,
                         **{name: step[name]['ms'] for name in ('create', 'checkpoint', 'export', 'detach') if name in step},
                         's3_put_calls': step['s3_put_calls']['value'], 'ship_epoch_bytes': step['ship_epoch_bytes']['value']}
                writes.append(entry)
                log(f'write {variant} run {run}: {entry}')
                shutil.rmtree(repository, ignore_errors=True)
                shutil.rmtree(work / f'write-pool-{run}', ignore_errors=True)
                if variant != 'native':
                    target.unlink(missing_ok=True)
        finally:
            if context:
                context.__exit__(None, None, None)
            shutil.rmtree(minio_data, ignore_errors=True)
    # Native then upload: the native file written above, uploaded to a MinIO (as the objfs article's baseline).
    minio_data = work / 'minio-write-upload'
    minio_data.mkdir()
    with minio_server(minio_data) as (endpoint, credentials, _):
        create_bucket(endpoint, credentials, BUCKET)
        for run in range(options.write_runs):
            target = work / f'write-native-{run}.duckdb'
            start = time.perf_counter()
            put_object(endpoint, credentials, BUCKET, f'native-{run}.duckdb', str(target))
            writes.append({'variant': 'native_upload', 'run': run, 'rows': rows, 'upload': (time.perf_counter() - start) * 1000,
                           'bytes': target.stat().st_size})
            target.unlink()
    shutil.rmtree(minio_data, ignore_errors=True)


main()
