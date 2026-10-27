#!/usr/bin/env python3
"""Bounded additive-table benchmark with observed DuckDB external execution/spilling."""
import hashlib
import json
import platform
import shutil
import subprocess
import tempfile

from logical_merge import process, DUCKDB_CLI, EXTENSION, evidence_path
from merge_schema import SchemaFixture
from merge_tables_spill import DDL, VALUES, setup, validation_sql


def main():
    report={'platform':platform.platform(),
            'duckdb_version':subprocess.check_output([DUCKDB_CLI,'-init','/dev/null','-no-agent','--version'],text=True).strip(),
            'extension_sha256':hashlib.sha256(EXTENSION.read_bytes()).hexdigest(),
            'schema':'CREATE TABLE new_rows'+DDL+'; CREATE INDEX new_idx ON new_rows(flag,id);',
            'values':VALUES,'schema_mode':'additive','base':'empty catalog',
            'inputs':'ours IDs [0,N); theirs IDs [N-N/4,2N-N/4); overlapping inserts identical',
            'memory_limit':'128MB','threads':2,'spill_limit':'512MB','stage_write_limit_bytes':536870912,
            'case_disk_limit_bytes':2147483648,'free_reserve_bytes':2147483648,'process_timeout_s':180,
            'debug_force_external':True,'repeats_per_size':3,'samples':[]}
    for size in (100000,250000):
        for sample in range(3):
            with tempfile.TemporaryDirectory(prefix='differential-table-bench-') as temporary:
                if shutil.disk_usage(temporary).free<report['free_reserve_bytes']:
                    raise RuntimeError('benchmark free disk reserve not available')
                f=SchemaFixture(temporary,'case'); setup(f,size)
                prefix=f.prefix()+"SET max_temp_directory_size='512MB'; SET debug_force_external=true;"
                stats="SELECT metric,value FROM differential_storage_stats() WHERE metric IN ('local_logical_read_bytes','local_materialized_payload_bytes');"
                data,usage=process(prefix+f.additive(options=',max_stage_write_bytes=>536870912')+stats,
                    f.folder,'merge',min_free_bytes=report['free_reserve_bytes'],max_disk_bytes=report['case_disk_limit_bytes'])
                total=2*size-size//4
                assert f.read(validation_sql(size))==[{'n':total,'correct':total}]
                assert usage['spill_peak_bytes']>0,'external execution did not produce an observed spill'
                merged=data[0][0]
                record={'rows_per_side':size,'inserted_rows_per_side':size,'identical_overlapping_keys':size//4,
                        'result_rows':total,'sample':sample,**{key:merged[key] for key in ('comparison_ms','application_ms')},
                        'process':usage,'io_counters':{r['metric']:r['value'] for r in data[1]}}
                report['samples'].append(record); print(json.dumps(record),flush=True)
    evidence_path('merge-tables-benchmark.json').write_text(json.dumps(report,indent=2)+'\n')


if __name__=='__main__': main()
