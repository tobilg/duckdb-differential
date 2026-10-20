#!/usr/bin/env python3
"""Scale benchmarks: native DuckDB vs Differential at gigabyte scale, local and against a disk-backed,
disposable MinIO. Manual only; .github/workflows/Benchmarks.yml runs them on a Linux runner.

Workloads (--workloads, comma-separated, default all):
  commit_latency  single-row commits on 1, 4 and 16 databases attached in one process at once, and
                  on 4 databases while a fifth loads data (one publication group per database)
  bulk_load       CREATE TABLE AS of --scale-gib of data, then CHECKPOINT (native, default pool,
                  external pool)
  cold_scan       a full scan of that table by a fresh DuckDB instance
  s3_export       initial and incremental export, per export verification/staging setting
  s3_gc           remote garbage collection after an expiry: three runs, with request counts
  history_sync    a second host listing a shared database's history of --commits snapshots, twice

Settings a build does not have yet are reported as not_available, so the same script measures a
baseline and later builds. Results: --output (default build/evidence/scale.json)."""
import argparse
import datetime
import json
import os
from pathlib import Path
import platform
import statistics
import subprocess
import sys
import tempfile
import threading
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'test/integration'))
from paths import EXTENSION  # noqa: E402
from stock_capi import Host  # noqa: E402
from minio_server import create_bucket, minio_server  # noqa: E402

parser = argparse.ArgumentParser()
parser.add_argument('--scale-gib', type=float, default=2.0)
parser.add_argument('--workloads', default='all')
parser.add_argument('--commits', type=int, default=2000, help='history_sync: snapshots of the shared database')
parser.add_argument('--small-commits', type=int, default=200, help='commit_latency: commits per database')
parser.add_argument('--work-dir', default='', help='scratch directory (default: a temporary one)')
parser.add_argument('--output', default=str(ROOT / 'build/evidence/scale.json'))
options = parser.parse_args()
ALL = ['commit_latency', 'bulk_load', 'cold_scan', 's3_export', 's3_gc', 'history_sync', 'ship_latency']
selected = ALL if options.workloads == 'all' else options.workloads.split(',')
assert all(w in ALL for w in selected), selected
results = []
BUCKET = 'differential-scale'
ROWS = max(1000, int(options.scale_gib * 2 ** 30 / 120))  # ~120 stored bytes per row


def table(rows):
    return f"SELECT range AS v, md5(range::VARCHAR) || md5((range * 7)::VARCHAR) AS pad FROM range({rows})"


TABLE = table(ROWS)


def record(workload, mode, **values):
    entry = {'workload': workload, 'mode': mode, **values}
    results.append(entry)
    print(json.dumps(entry), flush=True)


class Instance:
    """One DuckDB instance with the extension and its own repository."""

    def __init__(self, repository, settings=()):
        self.host = Host()
        self.conn = self.host.connect()
        loaded = self.query("SELECT count(*) FROM duckdb_extensions() WHERE extension_name = 'differential' AND loaded")
        if not loaded:
            self.query(f"LOAD '{EXTENSION}'")
        self.query(f"SET differential_repository = '{repository}'")
        for statement in settings:
            self.query(statement)

    def query(self, sql, conn=None):
        return self.host.query(conn or self.conn, sql)

    def timed(self, sql, conn=None):
        start = time.perf_counter()
        value = self.query(sql, conn)
        return (time.perf_counter() - start) * 1000, value

    def stat(self, metric):
        return self.query(f"SELECT value FROM differential_storage_stats() WHERE metric = '{metric}'") or 0

    def has_setting(self, name):
        return bool(self.query(f"SELECT count(*) FROM duckdb_settings() WHERE name = '{name}'"))

    def close(self):
        self.host.close()


def percentiles(samples):
    ordered = sorted(samples)
    pick = lambda q: ordered[min(len(ordered) - 1, int(q * len(ordered)))]  # noqa: E731
    return {'count': len(samples), 'p50_ms': pick(.50), 'p95_ms': pick(.95), 'p99_ms': pick(.99),
            'mean_ms': statistics.fmean(samples), 'max_ms': ordered[-1]}


def disk_bytes(path):
    return sum(p.stat().st_size for p in Path(path).rglob('*') if p.is_file())


def commit_latency(work):
    for mode in ('native', 'differential'):
        for databases, loader in ((1, False), (4, False), (16, False), (4, True)):
            folder = work / f'commits-{mode}-{databases}-{int(loader)}'
            folder.mkdir()
            instance = Instance(folder / 'repository')
            connections, samples, lock = [], [], threading.Lock()
            for i in range(databases + int(loader)):
                conn = instance.host.connect()
                target = f"'{folder / f'db{i}.duckdb'}'" if mode == 'native' else f"'differential:db{i}'"
                instance.query(f"ATTACH {target} AS d{i}", conn)
                instance.query(f"CREATE TABLE d{i}.t (i INTEGER, s VARCHAR)", conn)
                connections.append(conn)

            def commits(i):
                for k in range(options.small_commits):
                    elapsed, _ = instance.timed(f"INSERT INTO d{i}.t VALUES ({k}, md5('{k}'))", connections[i])
                    with lock:
                        samples.append(elapsed)

            def load():
                instance.query(f"CREATE TABLE d{databases}.big AS {table(min(ROWS, 4000000))}",
                               connections[databases])
                instance.query(f"CHECKPOINT d{databases}", connections[databases])

            threads = [threading.Thread(target=commits, args=(i,)) for i in range(databases)]
            loading = threading.Thread(target=load) if loader else None
            start = time.perf_counter()
            if loading:
                loading.start()
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
            wall = time.perf_counter() - start  # the committing threads only
            if loading:
                loading.join()
            instance.close()
            record('commit_latency', mode, databases=databases, concurrent_loader=loader,
                   commits_per_second=len(samples) / wall, **percentiles(samples))


def bulk_load_and_scan(work):
    for mode in ('native', 'differential', 'differential_external_pool'):
        folder = work / f'bulk-{mode}'
        folder.mkdir()
        instance = Instance(folder / 'repository')
        attach = {'native': f"ATTACH '{folder / 'native.duckdb'}' AS d",
                  'differential': "ATTACH 'differential:bulk' AS d",
                  'differential_external_pool': f"ATTACH 'differential:bulk' AS d (DATA_PATH '{folder / 'pool'}')"}[mode]
        instance.query(attach)
        if 'bulk_load' in selected:
            load, _ = instance.timed(f"CREATE TABLE d.t AS {TABLE}")
            checkpoint, _ = instance.timed("CHECKPOINT d")
            record('bulk_load', mode, rows=ROWS, load_ms=load, checkpoint_ms=checkpoint,
                   stored_bytes=disk_bytes(folder))
        else:
            instance.query(f"CREATE TABLE d.t AS {TABLE}")
        instance.close()
        if 'cold_scan' in selected:
            instance = Instance(folder / 'repository')
            instance.query(attach)
            elapsed, value = instance.timed("SELECT sum(length(pad)) FROM d.t")
            assert value == ROWS * 64, value
            record('cold_scan', mode, rows=ROWS, scan_ms=elapsed,
                   gib_per_second=disk_bytes(folder) / 2 ** 30 / (elapsed / 1000))
            instance.close()


S3_METRICS = ('s3_put_calls', 's3_get_calls', 's3_head_calls', 's3_list_calls', 's3_single_put_input_bytes',
              's3_verified_get_bytes')


def s3_workloads(work, endpoint):
    s3 = f"ENDPOINT '{endpoint}', PATH_STYLE true, ALLOW_HTTP true"
    s3_args = f"endpoint => '{endpoint}', path_style => true, allow_http => true"
    probe = Instance(work / 'probe-repository')
    verification = probe.has_setting('differential_export_verification')
    staging = probe.has_setting('differential_export_staging')
    probe.close()
    combos = [('default', [])]
    if verification and staging:
        combos = [(f'{v}+{s}', [f"SET differential_export_verification = '{v}'",
                                f"SET differential_export_staging = '{s}'"])
                  for v in ('checksum', 'read_back') for s in ('disk', 'memory')]
    elif 's3_export' in selected:
        record('s3_export', 'export_settings', status='not_available')
    future = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S+00')
    for name, settings in combos:
        location = f's3://{BUCKET}/export-{name.replace("+", "-")}/'
        instance = Instance(work / f'export-{name}', settings)
        instance.query(f"ATTACH 'differential:exp' AS d (DATA_PATH '{location}', {s3})")
        instance.query(f"CREATE TABLE d.t AS {TABLE}")
        steps = {}
        for step, sql in (('initial', None), ('incremental', f"UPDATE d.t SET pad = upper(pad) WHERE v < {ROWS // 100}")):
            if sql:
                instance.query(sql)
            before = {m: instance.stat(m) for m in S3_METRICS}
            elapsed, _ = instance.timed("SELECT snapshot_id FROM differential_create_snapshot('d')")
            steps[step] = {'export_ms': elapsed, **{m: instance.stat(m) - before[m] for m in S3_METRICS}}
        if 's3_export' in selected:
            for step, values in steps.items():
                record('s3_export', name, step=step, rows=ROWS, **values)
        if 's3_gc' in selected and name in ('default', 'checksum+disk'):
            instance.query("CALL differential_expire_snapshots('d', versions => [1])")
            for run in (1, 2, 3):
                before = {m: instance.stat(m) for m in S3_METRICS}
                elapsed, deleted = instance.timed(f"SELECT deleted_objects FROM differential_cleanup_remote_files("
                                            f"'{location}', older_than => TIMESTAMPTZ '{future}', {s3_args})")
                record('s3_gc', name, run=run, gc_ms=elapsed, deleted_objects=deleted,
                       **{m: instance.stat(m) - before[m] for m in S3_METRICS})
        instance.close()
    if 'ship_latency' in selected:
        # Single-row commits of an S3 database without WAL shipping, with async and with sync shipping:
        # commit latency, and S3 PUTs and shipped bytes per commit.
        probe = Instance(work / 'ship-probe')
        available = probe.query("SELECT count(*) FROM differential_status() WHERE capability = 'wal_shipping'")
        probe.close()
        for mode in ('off', 'async', 'sync') if available else ():
            instance = Instance(work / f'ship-{mode}')
            option = '' if mode == 'off' else f", SHIP_WAL '{mode}', SHIP_INTERVAL 1"
            instance.query(f"ATTACH 'differential:shipped' AS d (DATA_PATH 's3://{BUCKET}/ship-{mode}/', {s3}{option})")
            instance.query("CREATE TABLE d.t (i INTEGER, s VARCHAR)")
            metrics = ('s3_put_calls', 'ship_epochs', 'ship_epoch_bytes')
            before = {m: instance.stat(m) for m in metrics}
            samples = []
            for k in range(options.small_commits):
                elapsed, _ = instance.timed(f"INSERT INTO d.t VALUES ({k}, md5('{k}'))")
                samples.append(elapsed)
            # Async: until the last commit is shipped.
            start = time.perf_counter()
            while mode == 'async' and instance.query(
                    "SELECT count(*) FROM differential_branches('d') WHERE ship_pending") and time.perf_counter() - start < 30:
                time.sleep(0.05)
            drained = (time.perf_counter() - start) * 1000
            after = {m: instance.stat(m) for m in metrics}
            n = len(samples)
            record('ship_latency', mode, **percentiles(samples), drain_ms=drained if mode == 'async' else 0,
                   puts_per_commit=(after['s3_put_calls'] - before['s3_put_calls']) / n,
                   epochs=after['ship_epochs'] - before['ship_epochs'],
                   epoch_bytes_per_commit=(after['ship_epoch_bytes'] - before['ship_epoch_bytes']) / n)
            instance.close()
    if 'history_sync' in selected:
        location = f's3://{BUCKET}/history/'
        team = f"ATTACH 'differential:team' AS team (WRITERS 'shared', DATA_PATH '{location}', {s3}, PUBLISH_ON_CHECKPOINT false)"
        writer = Instance(work / 'history-writer')
        writer.query(team)
        writer.query("CREATE TABLE team.t (i INTEGER)")
        start = time.perf_counter()
        for k in range(options.commits):
            writer.query(f"INSERT INTO team.t VALUES ({k})")
            writer.query("CALL differential_create_snapshot('team')")
        publish = (time.perf_counter() - start) * 1000
        writer.query("CALL differential_expire_snapshots('team', versions => [1, 2, 3])")
        writer.close()
        reader = Instance(work / 'history-reader')
        reader.query(team)
        for run in (1, 2):
            before = {m: reader.stat(m) for m in S3_METRICS}
            elapsed, count = reader.timed("SELECT count(*) FROM differential_snapshots('team')")
            record('history_sync', 'shared', run=run, snapshots=count, publish_all_ms=publish, sync_ms=elapsed,
                   **{m: reader.stat(m) - before[m] for m in S3_METRICS})
        reader.close()


def main():
    with tempfile.TemporaryDirectory(prefix='differential-scale-', dir=options.work_dir or None) as tmp:
        work = Path(tmp)
        if 'commit_latency' in selected:
            commit_latency(work)
        if 'bulk_load' in selected or 'cold_scan' in selected:
            bulk_load_and_scan(work)
        if {'s3_export', 's3_gc', 'history_sync', 'ship_latency'} & set(selected):
            data = work / 'minio-data'
            data.mkdir()
            with minio_server(data) as (endpoint, credentials, _):
                os.environ.update({k: credentials[k] for k in ('AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY',
                                                               'AWS_EC2_METADATA_DISABLED', 'AWS_DEFAULT_REGION')})
                create_bucket(endpoint, credentials, BUCKET)
                s3_workloads(work, endpoint)
    git = lambda *a: subprocess.run(['git', '-C', str(ROOT), *a], capture_output=True, text=True).stdout.strip()  # noqa: E731
    commit = git('rev-parse', 'HEAD')
    dirty = bool(git('status', '--porcelain', '--untracked-files=no'))
    output = Path(options.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps({
        'environment': {'platform': platform.platform(), 'machine': platform.machine(), 'cpus': os.cpu_count(),
                        'python': platform.python_version(), 'commit': commit, 'uncommitted_changes': dirty,
                        'runner': os.environ.get('RUNNER_NAME', ''), 'measured_at': datetime.datetime.now(
                            datetime.timezone.utc).isoformat()},
        'scale_gib': options.scale_gib, 'rows': ROWS, 'commits': options.commits, 'workloads': selected,
        'results': results}, indent=2) + '\n')
    print('Results:', output)


main()
