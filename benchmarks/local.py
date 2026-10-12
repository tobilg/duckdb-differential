#!/usr/bin/env python3
"""Small reproducible local baseline (not a release qualification benchmark)."""
from pathlib import Path
import json
import os
import platform
import re
import statistics
import subprocess
import tempfile
import time
ROOT=Path(__file__).resolve().parents[1]
import sys as _sys
_sys.path.insert(0,str(ROOT/'test/integration'))
from paths import DUCKDB_CLI, EXTENSION, DUCKDB_LIBRARY, BUILD_DIR

CLI=DUCKDB_CLI
EXT=EXTENSION
measurements=[]
def query(sql,env):
    start=time.perf_counter()
    command=[CLI,'-init','/dev/null','-unsigned','-csv']
    if platform.system()=='Darwin':command=['/usr/bin/time','-l',*command]
    r=subprocess.run(command,input=sql,text=True,capture_output=True,env=env,timeout=60)
    if r.returncode:raise RuntimeError(r.stderr)
    metrics={}
    if platform.system()=='Darwin':
        rss=re.search(r'(\d+)\s+maximum resident set size',r.stderr)
        cpu=re.search(r'([\d.]+) real\s+([\d.]+) user\s+([\d.]+) sys',r.stderr)
        if rss:metrics['maximum_rss_bytes']=int(rss[1])
        if cpu:metrics.update(user_cpu_seconds=float(cpu[2]),system_cpu_seconds=float(cpu[3]))
    return round((time.perf_counter()-start)*1000,3),r.stdout,metrics
with tempfile.TemporaryDirectory(prefix='differential-benchmark-') as tmp:
    base=Path(tmp)
    for mode in ('native','differential','differential_external_pool'):
        for iteration in range(3):
            folder=base/(mode+str(iteration));folder.mkdir()
            env=dict(os.environ,DIFFERENTIAL_STORE_ROOT=str(folder))
            attach=f"ATTACH '{folder/'native.db'}' AS d;" if mode=='native' else f"LOAD '{EXT}'; ATTACH 'differential:bench' AS d;"
            initialize=attach if mode=='native' else f"LOAD '{EXT}'; ATTACH 'differential:bench' AS d;"
            if mode=='differential_external_pool':
                initialize=f"LOAD '{EXT}'; ATTACH 'differential:bench' AS d (DATA_PATH '{folder/'payloads'}');"
            ms,_,metrics=query(initialize+"CREATE TABLE d.t AS SELECT range AS id,sha256(range::VARCHAR) AS payload FROM range(100000); CHECKPOINT d;",env)
            measurements.append({'mode':mode,'iteration':iteration,'workload':'load_100k_checkpoint','milliseconds':ms,**metrics})
            sql=attach+"SELECT count(*),sum(id) FROM d.t WHERE id BETWEEN 100 AND 200;"
            ms,out,metrics=query(sql,env);assert '101,15150' in out
            measurements.append({'mode':mode,'iteration':iteration,'workload':'fresh_process_selective_query','milliseconds':ms,**metrics})
            ms,out,metrics=query(sql,env)
            measurements.append({'mode':mode,'iteration':iteration,'workload':'repeat_fresh_process_selective_query','milliseconds':ms,**metrics})
            physical=sum(p.stat().st_size for p in folder.rglob('*') if p.is_file())
            measurements.append({'mode':mode,'iteration':iteration,'workload':'retained_local_files','bytes':physical})
report={'platform':platform.platform(),'binary':CLI,'rows':100000,'repetitions':3,
        'cache_note':'Each query starts a fresh process. OS caches are not purged; these are not disk-cold measurements.',
        'durability':'Native sync unchanged. Differential writes use SQLite FULL and fullfsync; external immutable payloads use fsync/F_FULLFSYNC before directory sync and map publication; no sync disabled.',
        'scope':'Local public-path baseline; separate S3 history/fragmentation benchmark. macOS time(1) CPU/RSS includes the entire CLI process. No release performance claim.',
        'measurements':measurements}
(ROOT/'benchmarks/local-results.json').write_text(json.dumps(report,indent=2)+'\n')
for mode in ('native','differential','differential_external_pool'):
    for workload in ('load_100k_checkpoint','fresh_process_selective_query','repeat_fresh_process_selective_query'):
        values=[r['milliseconds'] for r in measurements if r['mode']==mode and r['workload']==workload]
        print(mode,workload,'median_ms=',statistics.median(values))
