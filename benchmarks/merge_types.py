#!/usr/bin/env python3
"""Bounded native-type merges: 10k/50k rows, 1%/20% changes, three fresh runs.
Uses the stock host, 128 MB memory, two threads, 256 MB spill, a 180-second
per-process deadline, sampled 512 MiB case disk cap and 1 GiB free reserve.
"""
import hashlib
import json
from pathlib import Path
import platform
import shutil
import subprocess
import tempfile
from logical_merge import process, DUCKDB_CLI, EXTENSION, evidence_path


def main():
    schema='x(id INT PRIMARY KEY,u UUID,v DOUBLE,ts TIMESTAMP_NS,attrs STRUCT(n UHUGEINT,a DOUBLE[],b BLOB))'
    initial="i,md5(i::VARCHAR)::UUID,0,make_timestamp_ns(1577836800123456789+i),{'n':340282366920938463463374607431768211455::UHUGEINT-i::UHUGEINT,'a':['NaN'::DOUBLE,NULL,1.5],'b':md5(i::VARCHAR)::BLOB}"
    incoming="{'n':340282366920938463463374607431768211455::UHUGEINT-id::UHUGEINT,'a':[id::DOUBLE,NULL,2.5],'b':'incoming'::BLOB}"
    report={'platform':platform.platform(),'extension_sha256':hashlib.sha256(EXTENSION.read_bytes()).hexdigest(),
        'duckdb_version':subprocess.check_output([DUCKDB_CLI,'-init','/dev/null','-no-agent','--version'],text=True).strip(),
        'schema':schema,'initial_values':initial,'workload':'same first K keys: ours v=1.5; theirs attrs='+incoming,
        'memory_limit':'128MB','threads':2,'spill_limit':'256MB','process_timeout_s':180,
        'case_disk_limit_bytes':536870912,'free_reserve_bytes':1073741824,'samples':[]}
    for size in (10000,50000):
        for fraction in (.01,.20):
            k=int(size*fraction)
            for repeat in range(3):
                with tempfile.TemporaryDirectory(prefix='differential-types-bench-') as tmp:
                    folder=Path(tmp)
                    if shutil.disk_usage(folder).free<report['free_reserve_bytes']: raise RuntimeError('free reserve unavailable')
                    prefix=(f"LOAD '{EXTENSION}'; SET differential_repository='{folder/'repo'}'; SET memory_limit='128MB';"
                        f"SET threads=2; SET temp_directory='{folder/'spill'}'; SET max_temp_directory_size='256MB'; SET differential_auto_gc=false;")
                    def run(sql,label):
                        return process(prefix+sql,folder,label,min_free_bytes=report['free_reserve_bytes'],max_disk_bytes=report['case_disk_limit_bytes'])
                    run(f"ATTACH 'differential:bench' AS d; CREATE TABLE d.{schema}; INSERT INTO d.x SELECT {initial} FROM range({size}) r(i);"
                        "CALL differential_create_snapshot('d'); CALL differential_create_branch('d','o',snapshot=>1); CALL differential_create_branch('d','t',snapshot=>1);"
                        f"ATTACH 'differential:bench' AS o (BRANCH 'o'); UPDATE o.x SET v=1.5 WHERE id<{k}; CALL differential_create_snapshot('o');"
                        f"ATTACH 'differential:bench' AS t (BRANCH 't'); UPDATE t.x SET attrs={incoming} WHERE id<{k}; CALL differential_create_snapshot('t');",'setup')
                    data,usage=run("SELECT * FROM differential_merge_branch('bench','o','t','o',request_id=>'types');"
                        "SELECT metric,value FROM differential_storage_stats() WHERE metric IN ('local_logical_read_bytes','local_materialized_payload_bytes');",'merge')
                    expected=(f"SELECT id,md5(id::VARCHAR)::UUID,CASE WHEN id<{k} THEN 1.5::DOUBLE ELSE 0::DOUBLE END,"
                        f"make_timestamp_ns(1577836800123456789+id),CASE WHEN id<{k} THEN {incoming} ELSE "
                        "{'n':340282366920938463463374607431768211455::UHUGEINT-id::UHUGEINT,'a':['NaN'::DOUBLE,NULL,1.5],'b':md5(id::VARCHAR)::BLOB} END "
                        f'FROM range({size}) r(id)')
                    verified,_=run("ATTACH 'differential:bench' AS m (BRANCH 'o'); SELECT count(*) AS differences FROM "
                        f'((SELECT * FROM m.x EXCEPT ALL {expected}) UNION ALL ({expected} EXCEPT ALL SELECT * FROM m.x));','verify')
                    assert verified[0][0]['differences']==0
                    record={'rows':size,'changed_rows_per_side':k,'repeat':repeat,
                        **{key:data[0][0][key] for key in ('selection_ms','target_check_ms','comparison_ms','application_ms','publication_ms')},
                        'process':usage,'io_counters':{v['metric']:v['value'] for v in data[1]}}
                    report['samples'].append(record); print(json.dumps(record),flush=True)
    evidence_path('merge-types-benchmark.json').write_text(json.dumps(report,indent=2)+'\n')


if __name__=='__main__': main()
