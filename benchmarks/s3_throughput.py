#!/usr/bin/env python3
"""S3 throughput by parallel request limit (differential_s3_max_requests), against a bucket you own.

For every limit, the script exports a new database of the same size (upload throughput: all of
its chunks are new) and then runs cold full scans of that export, each in a new DuckDB process
with an empty memory cache and the disk cache off (download throughput). Credentials come from
the AWS SDK chain or --profile, never from arguments. Every run writes under a new random
subprefix of --prefix and leaves its objects in place; the script prints the command to delete
them. Results go to build/evidence/s3-throughput.json (or --output).

Example (real AWS):
  python3 benchmarks/s3_throughput.py --bucket my-bucket --prefix bench/ --region eu-central-1 \\
      --profile my-profile --requests 4,8,16,32 --rows 2000000
"""
import argparse
import json
import os
from pathlib import Path
import platform
import secrets
import statistics
import subprocess
import sys
import tempfile
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'test/integration'))
from paths import DUCKDB_CLI, EXTENSION
from evidence import evidence_path

p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
p.add_argument('--bucket', required=True)
p.add_argument('--prefix', required=True, help='a prefix you own; each run uses a new random subprefix')
p.add_argument('--region', default='us-east-1')
p.add_argument('--profile', default='', help='AWS profile (default: the SDK credential chain)')
p.add_argument('--endpoint', default='', help='S3-compatible endpoint URL (default: AWS)')
p.add_argument('--path-style', action='store_true')
p.add_argument('--allow-http', action='store_true')
p.add_argument('--requests', default='4,8,16,32', help='comma-separated values of differential_s3_max_requests')
p.add_argument('--rows', type=int, default=1000000, help='rows per database (about 85 bytes stored per row)')
p.add_argument('--repetitions', type=int, default=3, help='cold scans per limit (median reported)')
p.add_argument('--threads', type=int, default=0, help='DuckDB threads (default: DuckDB default)')
p.add_argument('--output', type=Path, default=None)
args = p.parse_args()
limits = [int(v) for v in args.requests.split(',') if v.strip()]
if not args.prefix.strip('/'):
    p.error('Supply a nonempty prefix you own')
run_prefix = args.prefix.strip('/') + '/throughput-' + secrets.token_hex(6) + '/'
LOCATION = f"s3://{args.bucket}/{run_prefix}"


def quote(value):
    return "'" + value.replace("'", "''") + "'"


options = [f"REGION {quote(args.region)}"]
if args.profile:
    options.append(f"CREDENTIAL_PROFILE {quote(args.profile)}")
if args.endpoint:
    options.append(f"ENDPOINT {quote(args.endpoint)}")
if args.path_style:
    options.append('PATH_STYLE true')
if args.allow_http:
    options.append('ALLOW_HTTP true')
S3 = ', '.join(options)
STATS = ("SELECT metric, value FROM differential_storage_stats() WHERE metric IN "
         "('s3_get_calls', 's3_put_calls', 's3_verified_get_bytes', 's3_single_put_input_bytes', 's3_retry_decisions');")


def run(repository, sql):
    """One DuckDB process; returns (JSON result blocks, seconds of each .timer-ed statement)."""
    prelude = f"LOAD {quote(str(EXTENSION))}; SET differential_repository = {quote(str(repository))};"
    if args.threads:
        prelude += f"SET threads = {args.threads};"
    sql = '\n'.join(line.strip() for line in sql.splitlines())  # dot commands start a line
    r = subprocess.run([DUCKDB_CLI, '-init', '/dev/null', '-unsigned', '-batch', '-bail', '-json'],
                       input=prelude + sql, text=True, capture_output=True, timeout=3600)
    if r.returncode:
        raise RuntimeError(r.stderr.strip() or r.stdout.strip())
    blocks, timings, text = [], [], []
    for line in r.stdout.splitlines():
        if line.startswith('Run Time (s): real'):
            timings.append(float(line.split()[4]))
        else:
            text.append(line)
    rest, decoder = '\n'.join(text).strip(), json.JSONDecoder()
    while rest:
        value, end = decoder.raw_decode(rest)
        blocks.append(value)
        rest = rest[end:].lstrip()
    return blocks, timings


ATTACH_STEPS = ('attach_resolve', 'attach_repository_open', 's3_secret_lookup', 's3_provider_registration',
                'remote_descriptor_open', 's3_client_create', 's3_credentials',
                's3_dns_lookup', 's3_connect', 's3_first_byte', 'sdk_request')
ATTACH_STATS = ("SELECT metric, value FROM differential_storage_stats() WHERE metric IN (" +
                ', '.join(f"'{m}_{k}'" for m in ATTACH_STEPS for k in ('calls', 'nanoseconds', 'max_nanoseconds')) + ");")


EXPORT_STEPS = ('snapshot_checkpoint', 'snapshot_publish', 'manifest_seal', 'local_sync', 'export', 'branch_head_publish', 'export_pack_build', 'export_stage',
                'export_put', 'export_verify', 's3_connect')
EXPORT_STATS = ("SELECT metric, value FROM differential_storage_stats() WHERE metric IN (" +
                ', '.join(f"'{m}_{k}'" for m in EXPORT_STEPS for k in ('calls', 'nanoseconds', 'max_nanoseconds')) + ");")


def phases(block, names):
    values = {row['metric']: row['value'] for row in block}
    return {m: (values[f'{m}_calls'], values[f'{m}_nanoseconds'] / 1e6, values[f'{m}_max_nanoseconds'] / 1e6)
            for m in names}


def steps(block):
    """{step: (calls, total ms, max ms)} from ATTACH_STATS."""
    values = {row['metric']: row['value'] for row in block}
    return {m: (values[f'{m}_calls'], values[f'{m}_nanoseconds'] / 1e6, values[f'{m}_max_nanoseconds'] / 1e6)
            for m in ATTACH_STEPS}


def stats(block):
    return {row['metric']: row['value'] for row in block}


def mb(n):
    return n / 1e6


results = []
with tempfile.TemporaryDirectory(prefix='differential-s3-throughput-') as tmp:
    for limit in limits:
        name = f"t{limit}"
        # Distinct data per limit, so every export uploads all of its chunks.
        blocks, timings = run(Path(tmp) / f'writer-{limit}', f"""
            SET differential_s3_max_requests = {limit};
            ATTACH 'differential:{name}' AS d (DATA_PATH {quote(LOCATION)}, {S3});
            CREATE TABLE d.t AS SELECT range AS id,
                sha256(range::VARCHAR || '{name}') || sha256((range * 7)::VARCHAR || '{name}') AS payload
            FROM range({args.rows});
            .timer on
            SELECT export_state, export_locator FROM differential_create_snapshot('d');
            .timer off
            {EXPORT_STATS}
            {STATS}""")
        export = blocks[-3][0]
        export_steps = phases(blocks[-2], EXPORT_STEPS)
        assert export['export_state'] == 'complete', export
        upload = stats(blocks[-1])
        export_seconds = timings[0]
        reads = []
        for repetition in range(args.repetitions):
            blocks, timings = run(Path(tmp) / f'reader-{limit}-{repetition}', f"""
                SET differential_s3_max_requests = {limit};
                .timer on
                ATTACH {quote('differential:' + LOCATION + export['export_locator'])} AS r ({S3});
                .timer off
                {ATTACH_STATS}
                .timer on
                SELECT sum(length(payload))::BIGINT AS bytes FROM r.t;
                .timer off
                {STATS}""")
            counters = stats(blocks[-1])
            reads.append({'attach_seconds': timings[0], 'scan_seconds': timings[1],
                          'attach_steps': steps(blocks[0]),
                          'verified_get_bytes': counters['s3_verified_get_bytes'],
                          'get_calls': counters['s3_get_calls'], 'retries': counters['s3_retry_decisions']})
        scan = statistics.median(r['scan_seconds'] for r in reads)
        downloaded = statistics.median(r['verified_get_bytes'] for r in reads)
        entry = {'max_requests': limit, 'export_seconds': export_seconds, 'export_steps': export_steps,
                 'uploaded_bytes': upload['s3_single_put_input_bytes'], 'put_calls': upload['s3_put_calls'],
                 'upload_mb_per_second': mb(upload['s3_single_put_input_bytes']) / export_seconds,
                 'median_scan_seconds': scan, 'median_downloaded_bytes': downloaded,
                 'download_mb_per_second': mb(downloaded) / scan, 'reads': reads}
        results.append(entry)
        print(f"max_requests {limit:>2}: export {export_seconds:6.2f} s ({entry['upload_mb_per_second']:6.1f} MB/s up), "
              f"cold scan {scan:6.2f} s ({entry['download_mb_per_second']:6.1f} MB/s down, "
              f"{statistics.median(r['get_calls'] for r in reads):.0f} GETs)", flush=True)

report = {'command': sys.argv, 'platform': platform.platform(), 'cpu_count': os.cpu_count(),
          'provider': args.endpoint or f'AWS S3 ({args.region})', 'location': LOCATION, 'rows': args.rows,
          'threads': args.threads or 'DuckDB default', 'repetitions': args.repetitions,
          'scope': 'export = differential_create_snapshot (checkpoint, seal and upload); scan = cold full scan '
                   'in a new process, memory cache empty, disk cache off; MB = 10^6 bytes of verified payload',
          'results': results}
output = args.output or evidence_path('s3-throughput.json')
output.write_text(json.dumps(report, indent=2) + '\n')
print('\n| max_requests | export s | upload MB/s | cold scan s (median) | download MB/s | GETs |')
print('| ---: | ---: | ---: | ---: | ---: | ---: |')
for e in results:
    gets = statistics.median(r['get_calls'] for r in e['reads'])
    print(f"| {e['max_requests']} | {e['export_seconds']:.2f} | {e['upload_mb_per_second']:.1f} | "
          f"{e['median_scan_seconds']:.2f} | {e['download_mb_per_second']:.1f} | {gets:.0f} |")
# Where export time goes, by step (calls / total ms / max ms; pack steps are summed over threads).
print('\nExport steps per limit (calls / total ms / max ms):')
print('| max_requests | export s | ' + ' | '.join(EXPORT_STEPS) + ' |')
print('| ---: | ---: |' + ' ---: |' * len(EXPORT_STEPS))
for e in results:
    cells = [f"{c} / {t:.0f} / {m:.0f}" for c, t, m in (e['export_steps'][s] for s in EXPORT_STEPS)]
    print(f"| {e['max_requests']} | {e['export_seconds']:.2f} | " + ' | '.join(cells) + ' |')
# Where ATTACH time goes: the slowest ATTACH of each limit, by step (calls, total ms, max ms).
print('\nATTACH steps of the slowest ATTACH per limit (calls / total ms / max ms):')
print('| max_requests | ATTACH s | ' + ' | '.join(ATTACH_STEPS) + ' |')
print('| ---: | ---: |' + ' ---: |' * len(ATTACH_STEPS))
for e in results:
    slowest = max(e['reads'], key=lambda r: r['attach_seconds'])
    cells = [f"{c} / {t:.0f} / {m:.0f}" for c, t, m in (slowest['attach_steps'][s] for s in ATTACH_STEPS)]
    print(f"| {e['max_requests']} | {slowest['attach_seconds']:.2f} | " + ' | '.join(cells) + ' |')
print(f"\nResults: {output}")
print(f"Objects were left in place. To delete them:\n  aws s3 rm --recursive {LOCATION}"
      + (f" --profile {args.profile}" if args.profile else ''))
