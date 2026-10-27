#!/usr/bin/env python3
"""Render benchmarks/tpch.py results into build/evidence/benchmarks-tpch.md.

    python3 benchmarks/tpch_report.py <tpch.json> [<after.json>] [--output build/evidence/benchmarks-tpch.md]

With two files the first is the baseline and the second the measurement after a change (same scale
factor, threads and runs); a change is flagged when the medians differ by more than 10% and by more
than twice the larger standard deviation. Values are copied from the files; nothing is estimated."""
import argparse
import json
import math
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
parser = argparse.ArgumentParser()
parser.add_argument('files', nargs='+')
parser.add_argument('--output', default=str(ROOT / 'build/evidence/benchmarks-tpch.md'))
options = parser.parse_args()
runs = [json.loads(Path(p).read_text()) for p in options.files[:2]]
if len(runs) == 2:
    keys = ('scale_factor', 'threads', 'runs', 'cold_runs', 'memory_limit')
    assert all(runs[0][k] == runs[1][k] for k in keys), 'compare runs of the same configuration'
current = runs[-1]

REFERENCE = {'read_local': 'native', 'read_local_cold': 'native', 'read_s3_cold': 'native_httpfs'}
LABELS = {
    'native': 'Native file', 'differential': 'Differential (default store)', 'differential_pool': 'Differential (local pool)',
    'differential_s3_local': 'Differential S3 database (local copy)', 'native_httpfs': 'Native file on S3 (httpfs)',
    'export': 'Export from branch head (memory cache)', 'export_disk_cache': 'Export, warm disk cache',
    'evicted_snapshot': 'Evicted snapshot (via export)', 'follower': 'Follower (FOLLOW true)',
    'shared_joiner': 'Shared database, joined', 'recovered_branch': 'Recovered branch (recover_branch)',
    'differential_s3': 'Differential S3 + snapshot export', 'differential_s3_ship_async': "Differential S3, SHIP_WAL 'async'",
    'differential_s3_ship_sync': "Differential S3, SHIP_WAL 'sync'", 'differential_shared': 'Shared database + snapshot',
    'native_upload': 'Native file upload to S3'}
TITLES = {'read_local': 'Local reads, warm (one process: 1 warm-up, then measured runs)',
          'read_local_cold': 'Local reads, cold process (a new process per run)',
          'read_s3_cold': 'S3 reads, cold process (a new process per run, empty caches unless stated)'}


def median_of(run, suite, variant, query):
    for r in run['results']:
        if r['suite'] == suite and r['variant'] == variant and r['query'] == query:
            return r
    return None


def variants(run, suite):
    return list(dict.fromkeys(r['variant'] for r in run['results'] if r['suite'] == suite))


def geomean(values):
    values = [v for v in values if v and v > 0]
    return math.exp(sum(math.log(v) for v in values) / len(values)) if values else None


def ratio(run, suite, variant):
    reference = REFERENCE[suite]
    ratios = []
    for query in run['queries']:
        a, b = median_of(run, suite, variant, query), median_of(run, suite, reference, query)
        if a and b and b['median_ms'] > 0:
            ratios.append(a['median_ms'] / b['median_ms'])
    return geomean(ratios)


def table(header, rows):
    return ['| ' + ' | '.join(header) + ' |', '|' + ' --- |' * len(header)] + ['| ' + ' | '.join(r) + ' |' for r in rows] + ['']


def fmt(value, pattern='{:.1f}'):
    return '—' if value is None else pattern.format(value)


env = current['environment']
lines = ['# TPC-H benchmarks', '',
         'Measured by `benchmarks/tpch.py` and rendered by `benchmarks/tpch_report.py` (re-run both to update this',
         'page; see [Reproduce](#reproduce)). TPC-H Q1–Q22, every execution materializing its full result; every',
         "backend's results are hashed and must match native's.", '']
for label, run in zip(['baseline', 'after'] if len(runs) == 2 else ['measured'], runs):
    e = run['environment']
    dirty = ' (with uncommitted changes)' if e.get('uncommitted_changes') else ''
    lines.append(f"- **{label}**: commit `{e['commit'][:10]}`{dirty}, DuckDB {e.get('duckdb_version', '?')}, "
                 f"{e['platform']}, {e['cpus']} CPUs, {fmt(e.get('memory_gib'), '{:.0f}')} GiB RAM, measured "
                 f"{e['measured_at'][:19]}Z; extension test hooks {e.get('test_hooks', '?')}.")
lines += ['', f"Configuration: scale factor {current['scale_factor']:g}, `threads = {current['threads']}`, "
          f"`memory_limit` {current['memory_limit']}, {current['runs']} measured warm runs after 1 warm-up, "
          f"{current['cold_runs']} cold-process runs, {current['write_runs']} write runs; OS page cache "
          f"{'dropped before every cold run' if current['drop_os_cache'] else 'not dropped (warm for local files)'}. "
          'S3 is a disk-backed MinIO in Docker on the same machine: no network latency, so S3 numbers show the '
          "extension's overhead, not AWS latency.", '']

lines += ['## Summary', '',
          'Geometric mean over the queries of each backend\'s median time divided by the reference\'s (native file; '
          'for S3 reads, the native file read through httpfs). Below 1.00 is faster than the reference.', '']
rows = []
for suite in ('read_local', 'read_local_cold', 'read_s3_cold'):
    for variant in variants(current, suite):
        total = sum(r['median_ms'] for r in current['results'] if r['suite'] == suite and r['variant'] == variant)
        gets = sum(r['requests'].get('s3_get_calls', 0) + r['requests'].get('s3_head_calls', 0)
                   for r in current['results'] if r['suite'] == suite and r['variant'] == variant)
        cells = [suite, LABELS.get(variant, variant), fmt(total, '{:,.0f}'), fmt(ratio(current, suite, variant), '{:.2f}×'),
                 '—' if variant in ('native', 'native_httpfs') or suite != 'read_s3_cold' else f'{gets:,.0f}']
        if len(runs) == 2:
            cells.insert(4, fmt(ratio(runs[0], suite, variant), '{:.2f}×'))
        rows.append(cells)
header = ['Suite', 'Backend', 'Sum of medians ms', 'Ratio']
if len(runs) == 2:
    header = ['Suite', 'Backend', 'Sum of medians ms (after)', 'Ratio (after)', 'Ratio (baseline)']
lines += table(header + ['S3 GET+HEAD requests (all queries)'], rows)

if len(runs) == 2:
    changes = []
    for r in current['results']:
        before = median_of(runs[0], r['suite'], r['variant'], r['query'])
        if not before or not before['median_ms']:
            continue
        delta = r['median_ms'] / before['median_ms'] - 1
        noise = 2 * max(r['stdev_ms'], before['stdev_ms'])
        if abs(delta) > 0.10 and abs(r['median_ms'] - before['median_ms']) > noise:
            changes.append([r['suite'], LABELS.get(r['variant'], r['variant']), f"Q{r['query']}",
                            fmt(before['median_ms']), fmt(r['median_ms']), f'{delta * 100:+.0f}%'])
    lines += ['## Significant changes', '',
              'Queries whose median changed by more than 10% and more than twice the larger standard deviation.', '']
    lines += table(['Suite', 'Backend', 'Query', 'Baseline ms', 'After ms', 'Change'], changes) if changes else ['None.', '']

for suite in ('read_local', 'read_local_cold', 'read_s3_cold'):
    names = variants(current, suite)
    if not names:
        continue
    lines += [f'## {TITLES[suite]}', '', 'Median ms (± standard deviation); ratio to the reference in parentheses.', '']
    rows = []
    for query in current['queries']:
        reference = median_of(current, suite, REFERENCE[suite], query)
        row = [f'Q{query}']
        for variant in names:
            r = median_of(current, suite, variant, query)
            if not r:
                row.append('—')
                continue
            cell = f"{r['median_ms']:.1f} ± {r['stdev_ms']:.1f}"
            if variant != REFERENCE[suite] and reference and reference['median_ms']:
                cell += f" ({r['median_ms'] / reference['median_ms']:.2f}×)"
            row.append(cell)
        rows.append(row)
    lines += table(['Query'] + [LABELS.get(v, v) for v in names], rows)
    if suite == 'read_s3_cold':
        lines += ['S3 requests per query (median of the cold runs): GET / HEAD / LIST, and MiB read through S3.', '']
        rows = []
        for query in current['queries']:
            row = [f'Q{query}']
            for variant in names:
                r = median_of(current, suite, variant, query)
                q = r['requests'] if r else {}
                row.append('—' if variant == 'native_httpfs' or not q else
                           f"{q.get('s3_get_calls', 0):.0f} / {q.get('s3_head_calls', 0):.0f} / "
                           f"{q.get('s3_list_calls', 0):.0f}, {q.get('s3_verified_get_bytes', 0) / 2**20:.1f}")
            rows.append(row)
        lines += table(['Query'] + [LABELS.get(v, v) for v in names], rows)

if current.get('loads'):
    lines += ['## Loading (COPY FROM DATABASE of the generated data)', '']
    rows = [[LABELS.get(l['variant'], l['variant'])] + [fmt(l.get(k), '{:,.0f}') for k in ('copy', 'checkpoint', 'snapshot', 'detach')]
            for l in current['loads']]
    lines += table(['Backend', 'Copy ms', 'Checkpoint ms', 'Snapshot (export) ms', 'Detach ms'], rows)

if current.get('writes'):
    lines += ['## Writes: CREATE TABLE lineitem AS … and CHECKPOINT into a new database', '',
              'Medians over the write runs. Rows per second count the create and the checkpoint; the export (a snapshot '
              'uploaded to S3) and the detach (which ships what is left with SHIP_WAL) are listed separately.', '']
    rows = []
    for variant in dict.fromkeys(w['variant'] for w in current['writes']):
        entries = [w for w in current['writes'] if w['variant'] == variant]
        median = lambda key: sorted(w[key] for w in entries if key in w)[len([w for w in entries if key in w]) // 2] \
            if any(key in w for w in entries) else None  # noqa: E731
        create, checkpoint = median('create'), median('checkpoint')
        rate = entries[0]['rows'] / ((create + checkpoint) / 1000) if create is not None and checkpoint is not None else None
        rows.append([LABELS.get(variant, variant), fmt(create, '{:,.0f}'), fmt(checkpoint, '{:,.0f}'),
                     fmt(rate, '{:,.0f}'), fmt(median('export'), '{:,.0f}'), fmt(median('detach'), '{:,.0f}'),
                     fmt(median('upload'), '{:,.0f}'), fmt(median('s3_put_calls'), '{:,.0f}')])
    lines += table(['Backend', 'Create ms', 'Checkpoint ms', 'Rows/s', 'Export ms', 'Detach ms', 'Upload ms', 'S3 PUTs'], rows)

lines += ['## What each S3 read path reads', '',
          '- **Native file on S3 (httpfs):** the `.duckdb` file uploaded once, read with DuckDB\'s httpfs and its '
          'external file cache, empty in each new process.',
          '- **Export from branch head:** a fresh repository attaches `heads/<database>/main`; DuckDB\'s external file '
          'cache is the only cache (memory, empty per process).',
          '- **Export, warm disk cache:** the same with `differential_remote_cache_size` set, after one unmeasured process '
          'read the query: the measured process reads the repository disk cache.',
          '- **Evicted snapshot:** the writer\'s own snapshot after `differential_evict_snapshots`, read through its export.',
          '- **Follower:** `FOLLOW true` on the branch head of the shipping database: its chain\'s base export plus the '
          'epochs (in memory).',
          '- **Shared database, joined:** another repository attaches the shared database writable; its branch reads its '
          'adopted base snapshot from S3 through the store.',
          '- **Recovered branch:** `differential_recover_branch` imported the base without downloading; reads go to S3 '
          'through the store, except chunks the epochs carried (written locally at recovery).', '',
          '## Compared with the objfs article', '',
          'The [object storage article](https://dentiny.github.io/posts/duckdb-on-object-storage/) reports TPC-H with the '
          'same aggregation (median, ratio to native): local disk 1.08× native at SF10, S3 cold process 1.20× native '
          'httpfs at SF1 (over the internet, Taipei to ap-east-2), writing to S3 0.95× of native plus AWS CLI upload, '
          'local writes 0.19× native. Their S3 numbers include network latency that a local MinIO does not have, and '
          'their machine (Apple M4, 10 threads) differs; compare the ratios, not the times.', '',
          '## Reproduce', '', '```sh',
          'make release integration   # the release extension, loaded into the integration build\'s DuckDB',
          'make bench-tpch            # both steps below, with the release extension',
          f"DIFFERENTIAL_EXTENSION=build/release/extension/differential/differential.duckdb_extension \\",
          f"  python3 benchmarks/tpch.py --scale-factor {current['scale_factor']:g} --threads {current['threads']}"
          + (f" --memory-limit {current['memory_limit']}" if current['memory_limit'] != 'default' else ''),
          'python3 benchmarks/tpch_report.py build/evidence/tpch.json            # this page',
          'python3 benchmarks/tpch_report.py baseline.json build/evidence/tpch.json  # before/after',
          '```', '',
          'Needs Docker (MinIO) and network access once (DuckDB\'s `tpch` and `httpfs` extensions, cached in '
          '`build/benchmarks/`). SF1 needs about 3 GB of free disk; `--queries`, `--suites`, `--runs` narrow a run.', '']
Path(options.output).parent.mkdir(parents=True, exist_ok=True)
Path(options.output).write_text('\n'.join(lines))
print('Wrote', options.output)
