#!/usr/bin/env python3
"""Bounded local merge benchmark. Default: 10k/100k rows, 1%/20% changes per side.
Fresh disposable repository per case; 128MB DuckDB memory, two threads, 1GB spill cap,
180s deadline per process. Peak RSS and OS I/O are from wait4 (not memory_limit).
"""
import argparse
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import sys
import tempfile
import threading
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT/'test/integration'))
from paths import DUCKDB_CLI, EXTENSION
from evidence import evidence_path


class MeasurementError(RuntimeError):
    def __init__(self, details, metrics):
        super().__init__(details)
        self.metrics = metrics


def process(sql, folder, label, *, min_free_bytes=0, max_disk_bytes=0):
    start = time.perf_counter()
    with open(folder/(label+'.sql'), 'w+') as inp, open(folder/(label+'.stdout'), 'w+') as out, open(folder/(label+'.stderr'), 'w+') as err:
        inp.write(sql); inp.seek(0)
        p = subprocess.Popen([DUCKDB_CLI, '-init', '/dev/null', '-unsigned', '-batch', '-bail', '-json'],stdin=inp,stdout=out,stderr=err)
        stop = threading.Event()
        sampled = {'spill_peak_bytes':0, 'repository_peak_bytes':0, 'disk_guard':''}
        def monitor():
            while not stop.wait(.1):
                total = spill = repository = 0
                for path in folder.rglob('*'):
                    try:
                        if not path.is_file(): continue
                        size = path.stat().st_size
                    except FileNotFoundError: continue
                    total += size
                    if 'spill' in path.relative_to(folder).parts: spill += size
                    if 'repo' in path.relative_to(folder).parts: repository += size
                sampled['spill_peak_bytes'] = max(sampled['spill_peak_bytes'],spill)
                sampled['repository_peak_bytes'] = max(sampled['repository_peak_bytes'],repository)
                if (min_free_bytes and shutil.disk_usage(folder).free < min_free_bytes) or (max_disk_bytes and total > max_disk_bytes):
                    sampled['disk_guard'] = 'benchmark disk budget/reserve reached'
                    p.kill(); return
        watcher = threading.Thread(target=monitor,daemon=True) if min_free_bytes or max_disk_bytes else None
        if watcher: watcher.start()
        timer = threading.Timer(180, p.kill)
        timer.start()
        try:
            _, status, usage = os.wait4(p.pid, 0)
            p.returncode = os.waitstatus_to_exitcode(status)
        finally:
            timer.cancel(); stop.set()
            if watcher: watcher.join()
        out.seek(0); err.seek(0)
        answer, errors = out.read(), err.read()
        metrics = {'wall_ms': (time.perf_counter()-start)*1000,
            'peak_rss_bytes': usage.ru_maxrss * (1 if sys.platform == 'darwin' else 1024),
            'user_s': usage.ru_utime, 'system_s': usage.ru_stime,
            'input_blocks': usage.ru_inblock, 'output_blocks': usage.ru_oublock, **sampled}
        if p.returncode: raise MeasurementError((label,p.returncode,answer,errors,sampled['disk_guard']),metrics)
    decoder = json.JSONDecoder(); rows = []
    while answer.strip():
        value, n = decoder.raw_decode(answer.lstrip())
        rows.append(value); answer = answer.lstrip()[n:]
    return rows, metrics


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--sizes',type=int,nargs=2,default=[10000,100000])
    args=parser.parse_args()
    if not all(100<=n<=1000000 for n in args.sizes): parser.error('sizes must be 100..1000000')
    report={'platform':platform.platform(),'machine':platform.machine(),'sizes':args.sizes,
        'memory_limit':'128MB','threads':2,'max_temp_directory_size':'1GB',
        'schema':'CREATE TABLE x(id BIGINT PRIMARY KEY,a BIGINT,b VARCHAR,flag BOOLEAN,amount DECIMAL(18,3))',
        'initial_values':'a=0, b=md5(id::VARCHAR), flag=(id%2=0), amount=(id%10000)*0.001',
        'workload':'same first K keys: ours a=1; theirs b=changed and amount=2.375; no conflicts',
        'cases':[]}
    report['duckdb_version']=subprocess.check_output([DUCKDB_CLI,'-init','/dev/null','-no-agent','--version'],text=True).strip()
    with tempfile.TemporaryDirectory(prefix='differential-merge-bench-') as temporary:
        root=Path(temporary)
        for size in args.sizes:
            for fraction in (0.01,0.20):
                changed=int(size*fraction)
                folder=root/f'{size}-{changed}'; folder.mkdir()
                repo=folder/'repo'
                prefix=f"LOAD '{EXTENSION}'; SET differential_repository='{repo}'; SET memory_limit='128MB'; SET threads=2; SET temp_directory='{folder/'spill'}'; SET max_temp_directory_size='1GB'; SET differential_auto_gc=false;"
                setup=f"""
ATTACH 'differential:bench' AS d;
CREATE TABLE d.x(id BIGINT PRIMARY KEY,a BIGINT,b VARCHAR,flag BOOLEAN,amount DECIMAL(18,3));
INSERT INTO d.x SELECT i,0,md5(i::VARCHAR),i%2=0,(i%10000)*0.001 FROM range({size}) r(i);
CALL differential_create_snapshot('d');
CALL differential_create_branch('d','ours',snapshot=>1);
CALL differential_create_branch('d','theirs',snapshot=>1);
DETACH d;
ATTACH 'differential:bench' AS o (BRANCH 'ours'); UPDATE o.x SET a=1 WHERE id<{changed};
CALL differential_create_snapshot('o'); DETACH o;
ATTACH 'differential:bench' AS t (BRANCH 'theirs'); UPDATE t.x SET b='changed',amount=2.375 WHERE id<{changed};
CALL differential_create_snapshot('t'); DETACH t;
"""
                process(prefix+setup,folder,'setup')
                before=sum(p.stat().st_size for p in repo.rglob('*') if p.is_file())
                preview, preview_usage=process(prefix+"SELECT * EXCLUDE(conflict_details) FROM differential_merge_snapshots_preview('bench',1,2,3,conflict_limit=>0);",folder,'preview')
                merged, merge_usage=process(prefix+"SELECT * FROM differential_merge_snapshots('bench',1,2,3,'merged',request_id=>'bench'); SELECT metric,value FROM differential_storage_stats() WHERE metric IN ('local_logical_read_bytes','local_materialized_payload_bytes','local_pool_read_bytes','local_pool_write_bytes');",folder,'merge')
                verification,_=process(prefix+f"ATTACH 'differential:bench' AS m (BRANCH 'merged'); SELECT count(*) AS n,count(*) FILTER(WHERE a=1 AND b='changed' AND amount=2.375) AS changed FROM m.x;",folder,'verify')
                assert verification[0][0]=={'n':size,'changed':changed},verification
                assert preview[0][0]['conflicts']==0
                after=sum(p.stat().st_size for p in repo.rglob('*') if p.is_file())
                case={'rows':size,'changed_rows_per_side':changed,'distinct_changed_keys':changed,
                    'preview':preview_usage,'merge':merge_usage,'comparison_ms':merged[0][0]['comparison_ms'],
                    'application_ms':merged[0][0]['application_ms'],
                    'merge_counters':{r['metric']:r['value'] for r in merged[1]},
                    'repository_bytes_before':before,'repository_bytes_after':after}
                report['cases'].append(case)
                print(json.dumps(case),flush=True)
    path=evidence_path('logical-merge-benchmark.json'); path.write_text(json.dumps(report,indent=2)+'\n')
    print(path)


if __name__=='__main__': main()
