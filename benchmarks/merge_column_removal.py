#!/usr/bin/env python3
"""Bounded catalog column-removal benchmark, including its typed surviving-value proof."""
import hashlib
import json
import platform
import shutil
import subprocess
import tempfile

from logical_merge import process, DUCKDB_CLI, EXTENSION, evidence_path
from merge_columns import ColumnFixture


def main():
    report={'platform':platform.platform(),'duckdb_version':subprocess.check_output([DUCKDB_CLI,'-init','/dev/null','-no-agent','--version'],text=True).strip(),
            'extension_sha256':hashlib.sha256(EXTENSION.read_bytes()).hexdigest(),
            'schema':'x(id BIGINT PRIMARY KEY, obsolete VARCHAR, amount DECIMAL(18,3), flag BOOLEAN); visible -> x; old_view -> x.obsolete.',
            'initial_values':"id=i; obsolete=sha256(i::VARCHAR)||sha256((i+1)::VARCHAR); amount=NULL if i%7=0 else CAST((i%1000)*0.001 AS DECIMAL(18,3)); flag=i%2=0",
            'workload':'ours sets amount=2.375 on K keys and retains obsolete unchanged; theirs drops obsolete/old_view, toggles flag on the same K keys and projects visible to id,amount,flag. Each side changes exactly K rows.',
            'schema_mode':'catalog','schema_algorithm':'catalog-v1','memory_limit':'128MB','threads':2,'spill_limit':'512MB',
            'stage_write_limit_bytes':536870912,'case_disk_limit_bytes':2147483648,'free_reserve_bytes':2147483648,
            'process_timeout_s':180,'debug_force_external':True,'repeats_per_case':3,'samples':[]}
    for size in (100000,250000):
        for fraction in (.01,.20):
            changed=int(size*fraction)
            for sample in range(3):
                with tempfile.TemporaryDirectory(prefix='differential-column-removal-bench-') as temporary:
                    if shutil.disk_usage(temporary).free<report['free_reserve_bytes']: raise RuntimeError('benchmark free disk reserve not available')
                    f=ColumnFixture(temporary,'case')
                    definition=f"""CREATE TABLE d.x(id BIGINT PRIMARY KEY,obsolete VARCHAR,amount DECIMAL(18,3),flag BOOLEAN);
INSERT INTO d.x SELECT i,sha256(i::VARCHAR)||sha256((i+1)::VARCHAR),CASE WHEN i%7!=0 THEN CAST((i%1000)*0.001 AS DECIMAL(18,3)) END,i%2=0 FROM range({size}) r(i);
USE d; CREATE VIEW visible AS SELECT id,obsolete,amount,flag FROM x; CREATE VIEW old_view AS SELECT id,obsolete FROM x; USE memory;
"""
                    f.setup(f'UPDATE o.x SET amount=2.375 WHERE id<{changed};',
                            f'DROP VIEW t.old_view; DROP VIEW t.visible; ALTER TABLE t.x DROP COLUMN obsolete; UPDATE t.x SET flag=NOT flag WHERE id<{changed}; USE t; CREATE VIEW visible AS SELECT id,amount,flag FROM x; USE memory;',definition=definition)
                    settings="SET max_temp_directory_size='512MB'; SET debug_force_external=true;"
                    stats="SELECT metric,value FROM differential_storage_stats() WHERE metric IN ('local_logical_read_bytes','local_materialized_payload_bytes');"
                    data,usage=process(f.prefix()+settings+f.catalog(options=',max_stage_write_bytes=>536870912')+stats,
                        f.folder,'merge',min_free_bytes=report['free_reserve_bytes'],max_disk_bytes=report['case_disk_limit_bytes'])
                    check=(f"SELECT count(*) AS n,count(*) FILTER(WHERE id>=0 AND id<{size} AND amount IS NOT DISTINCT FROM "
                           f"CASE WHEN id<{changed} THEN 2.375 WHEN id%7!=0 THEN CAST((id%1000)*0.001 AS DECIMAL(18,3)) END AND "
                           f"flag IS NOT DISTINCT FROM CASE WHEN id<{changed} THEN NOT (id%2=0) ELSE id%2=0 END) AS correct FROM m.visible")
                    assert f.read(check)==[{'n':size,'correct':size}]
                    assert f.read("SELECT column_name FROM duckdb_columns() WHERE database_name='m' AND table_name='x' ORDER BY column_index")==[
                        {'column_name':name} for name in ('id','amount','flag')]
                    assert f.read("SELECT count(*) AS n FROM duckdb_views() WHERE database_name='m' AND view_name='old_view'")==[{'n':0}]
                    merged=data[0][0]
                    record={'rows':size,'changed_rows_per_side':changed,'sample':sample,
                            **{key:merged[key] for key in ('comparison_ms','application_ms')},'process':usage,
                            'io_counters':{r['metric']:r['value'] for r in data[1]}}
                    report['samples'].append(record); print(json.dumps(record),flush=True)
    evidence_path('merge-column-removal-benchmark.json').write_text(json.dumps(report,indent=2)+'\n')


if __name__=='__main__': main()
