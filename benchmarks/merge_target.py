#!/usr/bin/env python3
"""Bounded local existing-target benchmark, including fast-forward and no-op costs."""
import hashlib
import json
from pathlib import Path
import platform
import shutil
import subprocess
import tempfile

from logical_merge import process, DUCKDB_CLI, EXTENSION, evidence_path


def main():
    report={'platform':platform.platform(),'duckdb_version':subprocess.check_output([DUCKDB_CLI,'-init','/dev/null','-no-agent','--version'],text=True).strip(),
            'extension_sha256':hashlib.sha256(EXTENSION.read_bytes()).hexdigest(),
            'schema':'x(id BIGINT PRIMARY KEY,a BIGINT,b VARCHAR,flag BOOLEAN,amount DECIMAL(18,3))',
            'initial_values':'a=0,b=md5(id::VARCHAR),flag=(id%2=0),amount=(id%10000)*0.001',
            'workload':'ours a=1; theirs b=changed and amount=2.375 on the same first K keys. Exactly K changed rows per side.',
            'memory_limit':'128MB','threads':2,'spill_limit':'512MB','stage_write_limit_bytes':536870912,
            'case_disk_limit_bytes':2147483648,'free_reserve_bytes':2147483648,'process_timeout_s':180,
            'repeats_per_case':3,'samples':[]}
    for size in (100000,250000):
        for fraction in (.01,.20):
            k=int(size*fraction)
            for repeat in range(3):
                with tempfile.TemporaryDirectory(prefix='differential-target-bench-') as temporary:
                    folder=Path(temporary)
                    if shutil.disk_usage(folder).free<report['free_reserve_bytes']: raise RuntimeError('free disk reserve unavailable')
                    prefix=(f"LOAD '{EXTENSION}'; SET differential_repository='{folder/'repo'}'; SET memory_limit='128MB'; SET threads=2; "
                            f"SET temp_directory='{folder/'spill'}'; SET max_temp_directory_size='512MB'; SET differential_auto_gc=false;")
                    def run(sql,label):
                        return process(prefix+sql,folder,label,min_free_bytes=report['free_reserve_bytes'],max_disk_bytes=report['case_disk_limit_bytes'])
                    run(f"""
ATTACH 'differential:bench' AS d;
CREATE TABLE d.x(id BIGINT PRIMARY KEY,a BIGINT,b VARCHAR,flag BOOLEAN,amount DECIMAL(18,3));
INSERT INTO d.x SELECT i,0,md5(i::VARCHAR),i%2=0,(i%10000)*0.001 FROM range({size}) r(i);
CALL differential_create_snapshot('d');
CALL differential_create_branch('d','ours',snapshot=>1);
CALL differential_create_branch('d','theirs',snapshot=>1);
CALL differential_create_branch('d','fast',snapshot=>1); DETACH d;
ATTACH 'differential:bench' AS o (BRANCH 'ours'); UPDATE o.x SET a=1 WHERE id<{k};
CALL differential_create_snapshot('o'); DETACH o;
ATTACH 'differential:bench' AS t (BRANCH 'theirs'); UPDATE t.x SET b='changed',amount=2.375 WHERE id<{k};
CALL differential_create_snapshot('t'); DETACH t;
""",'setup')
                    for label,target in (('merged','ours'),('fast_forward','fast'),('already_up_to_date','ours')):
                        sql=f"SELECT * FROM differential_merge_branch('bench','{target}','theirs','{target}',request_id=>'{label}',max_stage_write_bytes=>536870912);"
                        data,usage=run(sql+"SELECT metric,value FROM differential_storage_stats() WHERE metric IN ('local_logical_read_bytes','local_materialized_payload_bytes');",label)
                        result=data[0][0]; assert result['merge_action']==label,result
                        a='CASE WHEN id<'+str(k)+' THEN 1 ELSE 0 END' if target=='ours' else '0'
                        verification,_=run(f"ATTACH 'differential:bench' AS m (BRANCH '{target}'); SELECT count(*) AS n,count(*) FILTER(WHERE a={a} AND "
                            f"b=CASE WHEN id<{k} THEN 'changed' ELSE md5(id::VARCHAR) END AND flag=(id%2=0) AND "
                            f"amount=CASE WHEN id<{k} THEN 2.375 ELSE (id%10000)*0.001 END) AS correct FROM m.x;",label+'-verify')
                        assert verification[0][0]=={'n':size,'correct':size},verification
                        record={'rows':size,'changed_rows_per_side':k,'sample':repeat,'action':label,
                            **{key:result[key] for key in ('comparison_ms','application_ms','target_check_ms','publication_ms','selection_ms')},
                            'process':usage,'io_counters':{r['metric']:r['value'] for r in data[1]}}
                        report['samples'].append(record); print(json.dumps(record),flush=True)
    evidence_path('merge-target-benchmark.json').write_text(json.dumps(report,indent=2)+'\n')


if __name__=='__main__': main()
