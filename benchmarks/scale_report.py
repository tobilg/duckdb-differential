#!/usr/bin/env python3
"""Render scale benchmark results (benchmarks/scale.py) into build/evidence/benchmarks-scale.md.

    python3 benchmarks/scale_report.py <scale.json> [<after.json>]

With two files the first is the baseline and the second the measurement after a change; both
must use the same scale. Values are copied from the files; nothing is estimated."""
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
runs = [json.loads(Path(p).read_text()) for p in sys.argv[1:3]]
assert runs, __doc__
assert len({(r['scale_gib'], r['commits']) for r in runs}) == 1, 'compare runs of the same scale'
labels = ['baseline', 'after'] if len(runs) == 2 else ['measured']


def rows(run, workload, **match):
    return [r for r in run['results'] if r['workload'] == workload and all(r.get(k) == v for k, v in match.items())]


def cell(run, workload, field, fmt='{:.1f}', **match):
    found = rows(run, workload, **match)
    return fmt.format(found[0][field]) if found and field in found[0] else '—'


def table(title, header, entries):
    lines = [f'### {title}', '', '| ' + ' | '.join(header) + ' |', '|' + ' --- |' * len(header)]
    lines += ['| ' + ' | '.join(entry) + ' |' for entry in entries]
    return lines + ['']


first = runs[0]
lines = ['# Scale benchmarks', '',
         'Measured by `benchmarks/scale.py` (manual; `.github/workflows/Benchmarks.yml` runs it on a hosted Linux',
         'runner) and rendered by `benchmarks/scale_report.py`. Hosted runners are noisy: compare runs of the',
         'same scale, not absolute numbers.', '']
for label, run in zip(labels, runs):
    env = run['environment']
    dirty = ' (with uncommitted changes)' if env.get('uncommitted_changes') else ''
    lines.append(f"- **{label}**: commit `{env['commit'][:10]}`{dirty}, {env['platform']}, {env['cpus']} CPUs, "
                 f"{run['scale_gib']} GiB ({run['rows']} rows), measured {env['measured_at'][:19]}Z.")
lines.append('')

entries = []
for mode in ('native', 'differential'):
    for databases, loader in ((1, False), (4, False), (16, False), (4, True)):
        for label, run in zip(labels, runs):
            match = dict(mode=mode, databases=databases, concurrent_loader=loader)
            entries.append([mode, str(databases) + (' + loader' if loader else ''), label,
                            cell(run, 'commit_latency', 'p50_ms', '{:.2f}', **match),
                            cell(run, 'commit_latency', 'p99_ms', '{:.2f}', **match),
                            cell(run, 'commit_latency', 'commits_per_second', '{:.0f}', **match)])
lines += table('Single-row commits, databases attached in one process',
               ['Mode', 'Databases', 'Run', 'p50 ms', 'p99 ms', 'Commits/s'], entries)

entries = []
for mode in ('native', 'differential', 'differential_external_pool'):
    for label, run in zip(labels, runs):
        entries.append([mode, label, cell(run, 'bulk_load', 'load_ms', '{:.0f}', mode=mode),
                        cell(run, 'bulk_load', 'checkpoint_ms', '{:.0f}', mode=mode),
                        cell(run, 'bulk_load', 'stored_bytes', '{:,}', mode=mode),
                        cell(run, 'cold_scan', 'scan_ms', '{:.0f}', mode=mode),
                        cell(run, 'cold_scan', 'gib_per_second', '{:.2f}', mode=mode)])
lines += table('Bulk load, checkpoint and cold scan',
               ['Mode', 'Run', 'Load ms', 'Checkpoint ms', 'Stored bytes', 'Cold scan ms', 'Scan GiB/s'], entries)

entries = []
modes = dict.fromkeys(r['mode'] for run in runs for r in run['results'] if r['workload'] == 's3_export' and 'step' in r)
for mode in modes:
    for step in ('initial', 'incremental'):
        for label, run in zip(labels, runs):
            entries.append([mode, step, label] + [cell(run, 's3_export', f, '{:,.0f}', mode=mode, step=step)
                                                  for f in ('export_ms', 's3_put_calls', 's3_get_calls',
                                                            's3_single_put_input_bytes', 's3_verified_get_bytes')])
lines += table('S3 export (MinIO, disk-backed)',
               ['Settings', 'Step', 'Run', 'ms', 'PUTs', 'GETs', 'PUT bytes', 'Verified GET bytes'], entries)

entries = []
for label, run in zip(labels, runs):
    for r in rows(run, 's3_gc'):
        entries.append([label, str(r['run']), f"{r['gc_ms']:.0f}", str(r.get('deleted_objects', '—')),
                        str(r['s3_get_calls']), str(r['s3_list_calls']), f"{r['s3_verified_get_bytes']:,}"])
lines += table('Remote garbage collection after an expiry (three consecutive runs)',
               ['Run', 'GC run', 'ms', 'Deleted', 'GETs', 'LISTs', 'GET bytes'], entries)

entries = []
for label, run in zip(labels, runs):
    for r in rows(run, 'history_sync'):
        entries.append([label, str(r['run']), str(r['snapshots']), f"{r['sync_ms']:.0f}", str(r['s3_list_calls']),
                        str(r['s3_get_calls'])])
lines += table(f"Shared history listing ({first['commits']} snapshots, second host)",
               ['Run', 'Listing', 'Snapshots', 'ms', 'LISTs', 'GETs'], entries)

entries = []
for mode in ('off', 'async', 'sync'):
    for label, run in zip(labels, runs):
        if rows(run, 'ship_latency', mode=mode):
            entries.append([mode, label, cell(run, 'ship_latency', 'p50_ms', '{:.2f}', mode=mode),
                            cell(run, 'ship_latency', 'p99_ms', '{:.2f}', mode=mode),
                            cell(run, 'ship_latency', 'puts_per_commit', '{:.2f}', mode=mode),
                            cell(run, 'ship_latency', 'epochs', '{:.0f}', mode=mode),
                            cell(run, 'ship_latency', 'epoch_bytes_per_commit', '{:,.0f}', mode=mode)])
if entries:
    lines += table('WAL shipping: single-row commits of an S3 database (MinIO, SHIP_INTERVAL 1 s)',
                   ['SHIP_WAL', 'Run', 'p50 ms', 'p99 ms', 'PUTs per commit', 'Epochs', 'Shipped bytes per commit'],
                   entries)

output = ROOT / 'build/evidence/benchmarks-scale.md'
output.parent.mkdir(parents=True, exist_ok=True)
output.write_text('\n'.join(lines))
print('Wrote', output)
