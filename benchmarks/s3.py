#!/usr/bin/env python3
"""SDK byte/request measurements on the disposable MinIO test fixture."""
import json
import os
import shutil
from pathlib import Path
import sqlite3
import sys
import time
from concurrent.futures import ThreadPoolExecutor
ROOT=Path(__file__).resolve().parents[1]
import sys as _sys
_sys.path.insert(0,str(ROOT/'test/integration'))
from paths import DUCKDB_CLI, EXTENSION, DUCKDB_LIBRARY, BUILD_DIR

sys.path.insert(0,str(ROOT/'test/integration'))
from stock_capi import Host
endpoint,folder=sys.argv[1:];folder=Path(folder)/'benchmark';folder.mkdir()
os.environ['DIFFERENTIAL_STORE_ROOT']=str(folder)
EXT=EXTENSION
# DIFFERENTIAL_BENCH_REMOTE_CACHE=<size> enables the repository disk cache in every reader instance.
CACHE=os.environ.get('DIFFERENTIAL_BENCH_REMOTE_CACHE','')
SETUP=f"LOAD '{EXT}';"+(f"SET differential_remote_cache_size='{CACHE}';" if CACHE else '')
def reset_disk_cache():
    # Each workload starts without cached remote data (only this benchmark's own directory).
    shutil.rmtree(folder/'cache',ignore_errors=True)
host=Host();conn=host.connect();results=[];scans=[]
try:
    S3=f"ENDPOINT '{endpoint}', PATH_STYLE true, ALLOW_HTTP true";LOCATION='s3://differential-sdk-test/benchmark/'
    host.query(conn,f"LOAD '{EXT}'; ATTACH 'differential:bench' AS d (DATA_PATH '{LOCATION}', {S3});")
    start=time.perf_counter()
    host.query(conn,"CREATE TABLE d.t AS SELECT range AS id,sha256(range::VARCHAR) AS payload FROM range(250000);")
    load_ms=(time.perf_counter()-start)*1000
    for depth in range(1,4):
        if depth>1:host.query(conn,f"UPDATE d.t SET payload=sha256((id+{depth})::VARCHAR) WHERE id%100=0;")
        start=time.perf_counter();host.query(conn,'CALL differential_create_snapshot(\'d\');');publish_ms=(time.perf_counter()-start)*1000
        with sqlite3.connect(folder/'engine.sqlite') as db:
            key=db.execute('SELECT descriptor FROM exports WHERE snapshot_id=?',(depth,)).fetchone()[0]
            size=db.execute('SELECT r.size FROM roots r JOIN snapshots s ON s.root=r.hash WHERE s.id=?',(depth,)).fetchone()[0]
        # A fresh stock library instance has an empty native buffer manager and no live remote cache.
        reset_disk_cache();reader=Host();c=reader.connect()
        try:
            reader.query(c,SETUP)
            def metric(name):return reader.query(c,f"SELECT value FROM differential_storage_stats() WHERE metric='{name}';")
            before_bytes=metric('s3_verified_get_bytes');before_get=metric('s3_get_calls');before_head=metric('s3_head_calls');before_retry=metric('s3_retry_decisions')
            start=time.perf_counter();reader.query(c,f"ATTACH 'differential:{LOCATION}{key}' AS r ({S3});")
            assert reader.query(c,'SELECT sum(id) FROM r.t WHERE id<10;')==45
            cold_ms=(time.perf_counter()-start)*1000
            cold_bytes=metric('s3_verified_get_bytes')-before_bytes;cold_get=metric('s3_get_calls')-before_get;cold_head=metric('s3_head_calls')-before_head
            before_bytes=metric('s3_verified_get_bytes');before_get=metric('s3_get_calls')
            start=time.perf_counter();assert reader.query(c,'SELECT sum(id) FROM r.t WHERE id<10;')==45
            warm_ms=(time.perf_counter()-start)*1000
            warm_bytes=metric('s3_verified_get_bytes')-before_bytes;warm_get=metric('s3_get_calls')-before_get
            assert metric('s3_retry_decisions')==before_retry
            assert cold_bytes<size/2,(cold_bytes,size)
            assert warm_bytes==0 and warm_get==0
            results.append({'history_depth':depth,'fragmentation_updates':(depth-1)*2500,'logical_size':size,'publish_ms':publish_ms,'cold_attach_query_ms':cold_ms,'cold_verified_get_bytes':cold_bytes,'cold_get_calls':cold_get,'cold_head_calls':cold_head,'sdk_retries':0,'warm_query_ms':warm_ms,'warm_get_bytes':warm_bytes,'warm_get_calls':warm_get})
        finally:reader.close()
        reset_disk_cache();reader=Host();c=reader.connect()
        try:
            reader.query(c,SETUP)
            metric_names=('s3_verified_get_bytes','s3_get_calls','s3_head_calls','s3_retry_decisions','remote_logical_read_bytes','remote_cache_hits','remote_cache_misses','remote_disk_cache_hits','remote_disk_cache_writes','remote_manifest_lookup_nanoseconds')
            def stats():return {name:reader.query(c,f"SELECT value FROM differential_storage_stats() WHERE metric='{name}';") for name in metric_names}
            before=stats();start=time.perf_counter()
            reader.query(c,f"ATTACH 'differential:{LOCATION}{key}' AS r ({S3});")
            assert reader.query(c,'SELECT sum(length(payload)) FROM r.t;')==250000*64
            elapsed=(time.perf_counter()-start)*1000;after=stats()
            scans.append({'history_depth':depth,'workload':'cold_full_projected_scan','milliseconds':elapsed,'counter_deltas':{k:after[k]-v for k,v in before.items()}})
        finally:reader.close()
    # Four independent instances in one process: each has its own memory cache (DuckDB's
    # external file cache); they share in-flight fetches and, when enabled, the disk cache.
    metric_names=('s3_verified_get_bytes','s3_get_calls','s3_head_calls','s3_retry_decisions','remote_cache_hits','remote_cache_misses','remote_disk_cache_hits','remote_disk_cache_writes')
    def stats():return {name:host.query(conn,f"SELECT value FROM differential_storage_stats() WHERE metric='{name}';") for name in metric_names}
    reset_disk_cache();readers=[]
    try:
        for _ in range(4):
            h=Host();c=h.connect();h.query(c,SETUP);readers.append((h,c))
        before=stats()
        def scan(item):
            h,c=item;start=time.perf_counter();h.query(c,f"ATTACH 'differential:{LOCATION}{key}' AS r ({S3});")
            assert h.query(c,'SELECT sum(length(payload)) FROM r.t;')==250000*64
            return (time.perf_counter()-start)*1000
        start=time.perf_counter()
        with ThreadPoolExecutor(max_workers=4) as executor:latencies=list(executor.map(scan,readers))
        elapsed=(time.perf_counter()-start)*1000;after=stats()
        scans.append({'history_depth':3,'workload':'four_concurrent_full_projected_scans','wall_milliseconds':elapsed,'reader_milliseconds':latencies,'counter_deltas':{k:after[k]-v for k,v in before.items()}})
    finally:
        for h,c in readers:h.close()
    # Four new instances one after another, as with a new in-memory instance per request.
    reset_disk_cache();before=stats();start=time.perf_counter();latencies=[]
    for _ in range(4):
        h=Host();c=h.connect()
        try:
            h.query(c,SETUP);t=time.perf_counter();h.query(c,f"ATTACH 'differential:{LOCATION}{key}' AS r ({S3});")
            assert h.query(c,'SELECT sum(length(payload)) FROM r.t;')==250000*64
            latencies.append((time.perf_counter()-t)*1000)
        finally:h.close()
    elapsed=(time.perf_counter()-start)*1000;after=stats()
    scans.append({'history_depth':3,'workload':'four_sequential_new_instance_full_scans','wall_milliseconds':elapsed,'reader_milliseconds':latencies,'counter_deltas':{k:after[k]-v for k,v in before.items()}})
finally:host.close()
report={'command':'DIFFERENTIAL_S3_BENCHMARK=1 python3 test/integration/minio.py','fixture_rows':250000,'load_commit_ms':load_ms,'provider':'pinned MinIO loopback','scope':'one repetition; global SDK API-call counters, successful verified payload bytes; HTTP/TLS wire overhead not measured; warm native buffer cache; unchanged sync guarantees. Cold timing and counters include attaching the export by its s3:// locator (provider registration, descriptor HEAD/GET) and index/header reads. Concurrent and sequential scans use four independent DuckDB instances; each workload starts with an empty disk cache.','remote_cache_size':CACHE or '0','results':results,'scans':scans}
(ROOT/('benchmarks/s3-results-disk-cache.json' if CACHE else 'benchmarks/s3-results.json')).write_text(json.dumps(report,indent=2)+'\n')
print('PASS S3 benchmark: selective cold reads below half of logical database; warm reads issue zero GETs across three history/fragmentation levels',flush=True)
