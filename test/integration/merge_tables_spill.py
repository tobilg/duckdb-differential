#!/usr/bin/env python3
"""Bounded new-table spill exhaustion, cancellation/death and native cleanup qualification."""
import json
from pathlib import Path
import shutil
import tempfile
import time

from evidence import evidence_path
from merge_schema import SchemaFixture
from merge_tables import no_stage
from merge_lifecycle import inspect, start


DDL='(id BIGINT PRIMARY KEY,amount DECIMAL(18,3),flag BOOLEAN,payload VARCHAR)'
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


def spill_files(folder):
    result={}
    for p in folder.glob('*storage*.tmp'):
        try:
            size=p.stat().st_size
            if size: result[p]=size
        except FileNotFoundError: pass
    return result


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

        for signal,label in [(2,'cancel'),(9,'death')]:
            f=SchemaFixture(tmp,label); setup(f,100000)
            p,_=start(f,settings+f.additive())
            found={}; deadline=time.monotonic()+45
            try:
                while p.poll() is None and time.monotonic()<deadline:
                    found=spill_files(f.folder/'spill')
                    if found:
                        p.send_signal(signal); break
                    time.sleep(.002)
                out,err=p.communicate(timeout=10)
                assert found,(p.returncode,out,err)
                if signal==9: assert p.returncode<0,(out,err)
            finally:
                if p.poll() is None: p.kill(); p.communicate(timeout=5)
            observed.append({'event':label,'spill_bytes_at_signal':sum(found.values()),'files':len(found)})
            f.hidden(); assert inspect(f)['state']=='pending'
            one=f.run(settings+f.additive())[-1][0]; two=f.run(f.additive())[-1][0]
            assert one['snapshot_id']==two['snapshot_id'] and one['operation_id']==two['operation_id']
            assert f.read(validation_sql(100000))==[{'n':175000,'correct':175000}]
            f.run('CALL differential_cleanup_old_files(cleanup_all=>true);'); no_stage(f)
            assert not [path for path in found if path.exists()],'native abandoned spill files remain after a spilling retry'
            ok(label+' after observing nonempty spill: one correct result, safe replay and native abandoned-file cleanup')
    evidence_path('merge-tables-spill.json').write_text(json.dumps({'passed':passed,'observed':observed,
        'rows_per_side':100000,'overlap':25000,'row_payload_bytes':128,'memory_limit':'128MB','spill_limit':'512MB',
        'debug_force_external':True,'scope':'disposable local stores; SIGINT/SIGKILL, not physical power loss'},indent=2)+'\n')


if __name__=='__main__': main()
