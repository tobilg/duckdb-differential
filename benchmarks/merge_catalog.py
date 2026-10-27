#!/usr/bin/env python3
"""Bounded local catalog merge benchmark: typed removal proof, row changes and view recreation."""
import hashlib
import json
import platform
import shutil
import subprocess
import tempfile

from logical_merge import process, DUCKDB_CLI, EXTENSION, evidence_path
from merge_catalog import CatalogFixture


def main():
    report={'platform':platform.platform(),'duckdb_version':subprocess.check_output([DUCKDB_CLI,'-init','/dev/null','-no-agent','--version'],text=True).strip(),
            'extension_sha256':hashlib.sha256(EXTENSION.read_bytes()).hexdigest(),
            'schema':'Two tables x and obsolete: (id BIGINT PRIMARY KEY, amount DECIMAL(18,3), flag BOOLEAN, payload VARCHAR); obsolete_idx(amount,id); visible -> x; old_view -> obsolete.',
            'initial_values':"id=i; amount=NULL if i%7=0 else CAST((i%1000)*0.001 AS DECIMAL(18,3)); flag=i%2=0; payload=sha256(i::VARCHAR)||sha256((i+1)::VARCHAR)",
            'workload':'ours sets x.amount=2.375 on K keys and adds a summary view; theirs drops obsolete/old_view, toggles x.flag on the same K keys, and replaces visible with WHERE id>=0; surviving obsolete is unchanged. Each side changes exactly K retained rows.',
            'schema_mode':'catalog','memory_limit':'128MB','threads':2,'spill_limit':'512MB',
            'stage_write_limit_bytes':536870912,'case_disk_limit_bytes':2147483648,'free_reserve_bytes':2147483648,
            'process_timeout_s':180,'debug_force_external':True,'repeats_per_case':3,'samples':[]}
    for size in (100000,250000):
        for fraction in (.01,.20):
            changed=int(size*fraction)
            for sample in range(3):
                with tempfile.TemporaryDirectory(prefix='differential-catalog-bench-') as temporary:
                    if shutil.disk_usage(temporary).free<report['free_reserve_bytes']: raise RuntimeError('benchmark free disk reserve not available')
                    f=CatalogFixture(temporary,'case')
                    definition=f"""CREATE TABLE d.x(id BIGINT PRIMARY KEY,amount DECIMAL(18,3),flag BOOLEAN,payload VARCHAR);
INSERT INTO d.x SELECT i,CASE WHEN i%7!=0 THEN CAST((i%1000)*0.001 AS DECIMAL(18,3)) END,i%2=0,sha256(i::VARCHAR)||sha256((i+1)::VARCHAR) FROM range({size}) r(i);
CREATE TABLE d.obsolete(id BIGINT PRIMARY KEY,amount DECIMAL(18,3),flag BOOLEAN,payload VARCHAR);
INSERT INTO d.obsolete SELECT * FROM d.x;
CREATE INDEX obsolete_idx ON d.obsolete(amount,id);
USE d; CREATE VIEW visible AS SELECT id,amount,flag,payload FROM x; CREATE VIEW old_view AS SELECT id,amount FROM obsolete; USE memory;
"""
                    f.setup(f"UPDATE o.x SET amount=2.375 WHERE id<{changed}; USE o; CREATE VIEW summary AS SELECT count(*) AS n FROM visible; USE memory;",
                            f"DROP VIEW t.old_view; DROP TABLE t.obsolete; UPDATE t.x SET flag=NOT flag WHERE id<{changed}; USE t; CREATE OR REPLACE VIEW visible AS SELECT id,amount,flag,payload FROM x WHERE id>=0; USE memory;",definition=definition)
                    settings="SET max_temp_directory_size='512MB'; SET debug_force_external=true;"
                    stats="SELECT metric,value FROM differential_storage_stats() WHERE metric IN ('local_logical_read_bytes','local_materialized_payload_bytes');"
                    data,usage=process(f.prefix()+settings+f.catalog(options=',max_stage_write_bytes=>536870912')+stats,
                        f.folder,'merge',min_free_bytes=report['free_reserve_bytes'],max_disk_bytes=report['case_disk_limit_bytes'])
                    check=(f"SELECT count(*) AS n,count(*) FILTER(WHERE id>=0 AND id<{size} AND amount IS NOT DISTINCT FROM "
                           f"CASE WHEN id<{changed} THEN 2.375 WHEN id%7!=0 THEN CAST((id%1000)*0.001 AS DECIMAL(18,3)) END AND "
                           f"flag IS NOT DISTINCT FROM CASE WHEN id<{changed} THEN NOT (id%2=0) ELSE id%2=0 END AND "
                           "payload=sha256(id::VARCHAR)||sha256((id+1)::VARCHAR)) AS correct FROM m.visible")
                    assert f.read(check)==[{'n':size,'correct':size}]
                    assert f.read('SELECT * FROM m.summary')==[{'n':size}]
                    assert f.read("SELECT count(*) AS n FROM duckdb_tables() WHERE database_name='m' AND table_name='obsolete'")==[{'n':0}]
                    assert f.read("SELECT count(*) AS n FROM duckdb_views() WHERE database_name='m' AND view_name='old_view'")==[{'n':0}]
                    # External execution can retain partitions in memory. Report observed
                    # temporary-file size, including zero, rather than assuming it spills.
                    merged=data[0][0]
                    record={'rows_per_initial_table':size,'removed_rows':size,'changed_rows_per_side':changed,'sample':sample,
                            **{key:merged[key] for key in ('comparison_ms','application_ms')},'process':usage,
                            'io_counters':{r['metric']:r['value'] for r in data[1]}}
                    report['samples'].append(record); print(json.dumps(record),flush=True)
    evidence_path('merge-catalog-benchmark.json').write_text(json.dumps(report,indent=2)+'\n')


if __name__=='__main__': main()
