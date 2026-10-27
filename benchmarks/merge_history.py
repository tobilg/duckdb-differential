#!/usr/bin/env python3
"""Bounded history-resolution benchmark: unchanged shared history, one edit per side.
Fresh disposable repository per size, independent CLI processes, three repetitions.
128MB native memory/temp disk, 512MiB repository guard, 1GiB host free-space reserve,
180s deadline per process. Measures selection separately from comparison/application.
"""
import argparse
import hashlib
import json
from pathlib import Path
import platform
import shutil
import statistics
import subprocess
import tempfile

from logical_merge import process, DUCKDB_CLI, EXTENSION, evidence_path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--history', type=int, nargs=2, default=[1000,10000])
    parser.add_argument('--repeats', type=int, default=3)
    args = parser.parse_args()
    if not all(2 <= n <= 20000 for n in args.history) or not 1 <= args.repeats <= 5:
        parser.error('history must be 2..20000 and repeats 1..5')
    report = {'platform':platform.platform(), 'machine':platform.machine(),
              'artifact':str(EXTENSION), 'history':args.history, 'repeats':args.repeats,
              'schema':'x(id INT PRIMARY KEY,a INT,b VARCHAR)', 'rows':1,
              'changed_rows_per_side':1, 'memory_limit':'128MB', 'threads':2,
              'temp_limit':'128MB', 'process_deadline_seconds':180, 'cases':[]}
    report['duckdb_version'] = subprocess.check_output([DUCKDB_CLI,'-init','/dev/null','-no-agent','--version'],text=True).strip()
    digest = hashlib.sha256()
    with Path(EXTENSION).open('rb') as artifact:
        for block in iter(lambda: artifact.read(1024*1024), b''):
            digest.update(block)
    report['artifact_sha256'] = digest.hexdigest()
    with tempfile.TemporaryDirectory(prefix='differential-merge-history-') as temp:
        root = Path(temp)
        for count in args.history:
            folder = root/str(count); folder.mkdir()
            prefix = (f"LOAD '{EXTENSION}'; SET differential_repository='{folder/'repo'}';"
                      f"SET temp_directory='{folder/'spill'}'; SET max_temp_directory_size='128MB';"
                      "SET memory_limit='128MB'; SET threads=2; SET differential_auto_gc=false;")
            def run(sql, label):
                return process(prefix+sql,folder,label,min_free_bytes=1024**3,max_disk_bytes=512*1024**2)
            if shutil.disk_usage(folder).free < 1536*1024**2:
                raise RuntimeError('Insufficient free disk for bounded history measurement')
            setup = ("ATTACH 'differential:history' AS d; CREATE TABLE d.x(id INT PRIMARY KEY,a INT,b VARCHAR);"
                     "INSERT INTO d.x VALUES(1,0,'base');" + "CALL differential_create_snapshot('d');"*count +
                     f"CALL differential_create_branch('d','feature',snapshot=>{count});"
                     "UPDATE d.x SET a=1; CALL differential_create_snapshot('d'); DETACH d;"
                     "ATTACH 'differential:history' AS f (BRANCH 'feature'); UPDATE f.x SET b='changed';"
                     "CALL differential_create_snapshot('f'); DETACH f;")
            _, setup_usage = run(setup,'setup')
            case = {'shared_history_snapshots':count,'reachable_nodes':count+2,'setup':setup_usage,'samples':[]}
            for repetition in range(args.repeats):
                rows, preview_usage = run("SELECT base_snapshot_id,ancestry_nodes,selection_ms,conflicts FROM "
                    "differential_merge_branch_preview('history','main','feature',conflict_limit=>0);",f'preview-{repetition}')
                preview = rows[0][0]
                assert preview['base_snapshot_id'] == count and preview['ancestry_nodes'] == count+2 and preview['conflicts'] == 0
                rows, merge_usage = run(f"SELECT * FROM differential_merge_branch('history','main','feature','merged_{repetition}');",f'merge-{repetition}')
                merged = rows[0][0]
                assert merged['base_snapshot_id'] == count and merged['ancestry_nodes'] == count+2
                rows, _ = run(f"ATTACH 'differential:history' AS m (BRANCH 'merged_{repetition}'); SELECT * FROM m.x;",f'verify-{repetition}')
                assert rows == [[{'id':1,'a':1,'b':'changed'}]], rows
                case['samples'].append({'preview':preview,'preview_usage':preview_usage,'merge':merged,'merge_usage':merge_usage})
            case['selection_median_ms'] = statistics.median(s['merge']['selection_ms'] for s in case['samples'])
            case['repository_bytes'] = sum(p.stat().st_size for p in (folder/'repo').rglob('*') if p.is_file())
            report['cases'].append(case)
            print(f"PASS {count} shared snapshots: {case['selection_median_ms']:.3f} ms median selection",flush=True)
            shutil.rmtree(folder)
    evidence_path('merge-history-benchmark.json').write_text(json.dumps(report,indent=2)+'\n')


if __name__ == '__main__': main()
