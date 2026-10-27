#!/usr/bin/env python3
"""Bounded new-table spill exhaustion and cancellation/death at a phase gate."""
import json
from pathlib import Path
import shutil
import tempfile

from evidence import evidence_path
from merge_schema import SchemaFixture
from merge_tables import no_stage
from merge_lifecycle import inspect, release, start


DDL='(id BIGINT PRIMARY KEY,amount DECIMAL(18,3),flag BOOLEAN,payload VARCHAR)'
SIGNAL_PHASE='table_inserted'
VALUES="i,CASE WHEN i%7!=0 THEN CAST((i%1000)*0.001 AS DECIMAL(18,3)) END,CASE WHEN i%11!=0 THEN i%2=0 END,sha256(i::VARCHAR)||sha256((i+1)::VARCHAR)"


def setup(f,size):
    overlap=size//4
    def side(c,start,end):
        return (f'CREATE TABLE {c}.new_rows{DDL};'
                f'INSERT INTO {c}.new_rows SELECT {VALUES} FROM range({start},{end}) r(i);'
                f'CREATE INDEX new_idx ON {c}.new_rows(flag,id);')
    f.setup(side('o',0,size),side('t',size-overlap,2*size-overlap),definition='')


def validation_sql(size,catalog='m'):
    total=2*size-size//4
    return (f'SELECT count(*) AS n,count(*) FILTER(WHERE id>=0 AND id<{total} AND '
            'amount IS NOT DISTINCT FROM CASE WHEN id%7!=0 THEN CAST((id%1000)*0.001 AS DECIMAL(18,3)) END '
            'AND flag IS NOT DISTINCT FROM CASE WHEN id%11!=0 THEN id%2=0 END '
            "AND payload=sha256(id::VARCHAR)||sha256((id+1)::VARCHAR)) AS correct "
            f'FROM {catalog}.new_rows')


def main():
    passed=[]; observed=[]
    def ok(label): passed.append(label); print('PASS merge table spill',label,flush=True)
    with tempfile.TemporaryDirectory(prefix='differential-merge-table-spill-') as tmp:
        if shutil.disk_usage(tmp).free<2*1024**3: raise RuntimeError('spill qualification needs 2 GiB free disk reserve')
        f=SchemaFixture(tmp,'exhaustion'); setup(f,100000)
        settings="SET memory_limit='128MB'; SET max_temp_directory_size='512MB'; SET debug_force_external=true;"
        f.run("SET memory_limit='32MB'; SET max_temp_directory_size='1KiB'; SET debug_force_external=true;"+f.additive(),error='max_temp_directory_size')
        f.hidden(); no_stage(f); assert inspect(f)['state']=='pending'
        f.run(settings+f.additive())
        assert f.read(validation_sql(100000))==[{'n':175000,'correct':175000}]
        no_stage(f); ok('temporary-disk exhaustion leaves inputs/output safe and the same request succeeds with adequate spill budget')

        # Signals reach the merge while it waits at a phase gate, after the forced-external
        # comparison and the bulk load of the new table; nothing depends on host speed.
        for signal,label in [(2,'cancel'),(9,'death')]:
            f=SchemaFixture(tmp,label); setup(f,100000)
            p,gate=start(f,settings+f.additive(),SIGNAL_PHASE)
            p.send_signal(signal); out,err=p.communicate(timeout=10); release(gate)
            if signal==9: assert p.returncode<0,(out,err)
            observed.append({'event':label,'phase':SIGNAL_PHASE,'returncode':p.returncode})
            f.hidden(); assert inspect(f)['state']=='pending'
            one=f.run(settings+f.additive())[-1][0]; two=f.run(f.additive())[-1][0]
            assert one['snapshot_id']==two['snapshot_id'] and one['operation_id']==two['operation_id']
            assert f.read(validation_sql(100000))==[{'n':175000,'correct':175000}]
            f.run('CALL differential_cleanup_old_files(cleanup_all=>true);'); no_stage(f)
            ok(label+' at the '+SIGNAL_PHASE+' gate: one correct result, safe replay and staging cleanup')
    evidence_path('merge-tables-spill.json').write_text(json.dumps({'passed':passed,'observed':observed,
        'rows_per_side':100000,'overlap':25000,'row_payload_bytes':128,'memory_limit':'128MB','spill_limit':'512MB',
        'debug_force_external':True,'scope':'disposable local stores; SIGINT/SIGKILL, not physical power loss'},indent=2)+'\n')


if __name__=='__main__': main()
