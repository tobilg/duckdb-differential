#!/usr/bin/env python3
"""Bounded same-instance reader/merge contention experiment, with exact result checks."""
import argparse
import ctypes as C
import hashlib
import json
import os
from pathlib import Path
import platform
import resource
import shutil
import statistics
import subprocess
import sys
import tempfile
import threading
import time

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'test/integration'))
from paths import DUCKDB_CLI, EXTENSION
from stock_capi import Host, Result
from evidence import evidence_path


def worker(folder, size):
    host=Host(); reader=host.connect(); merger=host.connect()
    host.query(reader,f"LOAD '{EXTENSION}'; SET differential_repository='{folder/'repo'}'; SET threads=2; SET memory_limit='256MB'; SET temp_directory='{folder/'spill'}'; SET max_temp_directory_size='512MB'; ATTACH 'differential:bench' AS input (SNAPSHOT_VERSION 2,READ_ONLY);")
    sql='SELECT sum(a)+sum(strlen(b)) FROM input.x'
    expected=size*32+size//10
    def scan():
        started=time.perf_counter(); assert host.query(reader,sql)==expected
        return (time.perf_counter()-started)*1000
    baseline=[scan() for _ in range(10)]
    answer, errors={},[]
    host.lib.duckdb_value_double.argtypes=[C.POINTER(Result),C.c_uint64,C.c_uint64]
    host.lib.duckdb_value_double.restype=C.c_double
    def merge():
        result=Result(); started=time.perf_counter()
        try:
            query=b"SELECT snapshot_id,comparison_ms,application_ms FROM differential_merge_snapshots('bench',1,2,3,'merged',request_id=>'contention')"
            if host.lib.duckdb_query(merger,query,C.byref(result)):
                raise RuntimeError(host.lib.duckdb_result_error(C.byref(result)).decode())
            answer.update(snapshot_id=host.lib.duckdb_value_int64(C.byref(result),0,0),
                          comparison_ms=host.lib.duckdb_value_double(C.byref(result),1,0),
                          application_ms=host.lib.duckdb_value_double(C.byref(result),2,0),
                          wall_ms=(time.perf_counter()-started)*1000)
        except Exception as error: errors.append(str(error))
        finally: host.lib.duckdb_destroy_result(C.byref(result))
    thread=threading.Thread(target=merge); thread.start()
    concurrent=[]
    while thread.is_alive() and len(concurrent)<1000:
        concurrent.append(scan())
        time.sleep(.01)
    samples_capped = thread.is_alive()
    thread.join(timeout=180)
    assert not thread.is_alive() and not errors and concurrent, errors
    host.query(reader,f"ATTACH 'differential:bench' AS result (SNAPSHOT_VERSION {answer['snapshot_id']},READ_ONLY);")
    expected_rows = (f"SELECT i::BIGINT id,(CASE WHEN i<{size//10} THEN 1 ELSE 0 END)::BIGINT a,"
                     f"CASE WHEN i<{size//10} THEN 'changed' ELSE md5(i::VARCHAR) END b FROM range({size}) r(i)")
    assert host.query(reader,'WITH expected AS ('+expected_rows+') SELECT count(*) FROM '
        '((SELECT * FROM result.x EXCEPT ALL SELECT * FROM expected) UNION ALL '
        '(SELECT * FROM expected EXCEPT ALL SELECT * FROM result.x))')==0
    usage=resource.getrusage(resource.RUSAGE_SELF)
    host.close()
    return {'rows':size,'changed_rows_per_side':size//10,'baseline_query_ms':baseline,'concurrent_query_ms':concurrent,
            'baseline_median_ms':statistics.median(baseline),'concurrent_median_ms':statistics.median(concurrent),
            'concurrent_max_ms':max(concurrent),'samples_capped':samples_capped,'merge':answer,
            'peak_rss_bytes':usage.ru_maxrss*(1 if sys.platform=='darwin' else 1024),
            'input_blocks':usage.ru_inblock,'output_blocks':usage.ru_oublock}


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--sizes',nargs='+',type=int,default=[100000,1000000]); p.add_argument('--repeats',type=int,default=3)
    p.add_argument('--worker',type=Path); p.add_argument('--output',type=Path,default=evidence_path('merge-contention.json'))
    args=p.parse_args()
    if args.worker:
        print(json.dumps(worker(args.worker,args.sizes[0]))); return
    if not 1<=args.repeats<=10 or not all(100<=n<=1000000 for n in args.sizes): p.error('sizes100..1000000, repeats1..10')
    report={'platform':platform.platform(),'memory_limit':'256MB','threads':2,'max_temp_directory_size':'512MB',
            'duckdb_version':subprocess.check_output([DUCKDB_CLI,'-init','/dev/null','-no-agent','--version'],text=True).strip(),
            'extension_sha256':hashlib.sha256(Path(EXTENSION).read_bytes()).hexdigest(),
            'schema':'CREATE TABLE x(id BIGINT PRIMARY KEY,a BIGINT,b VARCHAR)',
            'workload':'Initial a=0,b=md5(id::VARCHAR); first10%keys:ours a=1,theirs b=changed',
            'scope':'10 warm baseline scans and at most1000 scans spaced10ms during a same-instance merge; exact input/result checks; process RSS includes warmup and verification',
            'cases':[],'blocked':[]}
    with tempfile.TemporaryDirectory(prefix='differential-merge-contention-') as tmp:
        root=Path(tmp)
        for size in args.sizes:
            if shutil.disk_usage(root).free < size*512+256*1024*1024:
                report['blocked'].append({'rows':size,'reason':'insufficient disk reserve'}); continue
            for repeat in range(args.repeats):
                folder=root/f'{size}-{repeat}'; folder.mkdir()
                prefix=f"LOAD '{EXTENSION}'; SET differential_repository='{folder/'repo'}'; SET threads=2; SET memory_limit='256MB';"
                setup=("ATTACH 'differential:bench' AS d; CREATE TABLE d.x(id BIGINT PRIMARY KEY,a BIGINT,b VARCHAR);"
                       f"INSERT INTO d.x SELECT i,0,md5(i::VARCHAR) FROM range({size}) r(i); CALL differential_create_snapshot('d');"
                       "CALL differential_create_branch('d','ours',snapshot=>1); CALL differential_create_branch('d','theirs',snapshot=>1);"
                       f"ATTACH 'differential:bench' AS o (BRANCH 'ours'); UPDATE o.x SET a=1 WHERE id<{size//10}; CALL differential_create_snapshot('o');"
                       f"ATTACH 'differential:bench' AS t (BRANCH 'theirs'); UPDATE t.x SET b='changed' WHERE id<{size//10}; CALL differential_create_snapshot('t');")
                subprocess.run([DUCKDB_CLI,'-init','/dev/null','-unsigned','-batch','-bail','-json'],input=prefix+setup,text=True,capture_output=True,check=True,timeout=180)
                result=subprocess.run([sys.executable,__file__,'--worker',str(folder),'--sizes',str(size)],text=True,capture_output=True,timeout=180)
                assert result.returncode==0,(result.stdout,result.stderr)
                case=json.loads(result.stdout); case['repeat']=repeat; report['cases'].append(case)
                args.output.write_text(json.dumps(report,indent=2)+'\n')
                print(json.dumps({k:case[k] for k in ('rows','repeat','baseline_median_ms','concurrent_median_ms','concurrent_max_ms')}),flush=True)
                shutil.rmtree(folder)
    args.output.write_text(json.dumps(report,indent=2)+'\n'); print(args.output)


if __name__=='__main__': main()
