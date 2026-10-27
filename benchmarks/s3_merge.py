#!/usr/bin/env python3
"""Bounded S3 merge cost, using minio.py --merge-benchmark or --merge-target-benchmark.
Three fresh-process/repository samples, 10k/50k rows, 1%/20% changed keys per side.
Snapshot mode evicts/collects all inputs; target mode evicts/collects the base only.
"""
import hashlib
import json
import platform
from pathlib import Path
import sys

from logical_merge import process, EXTENSION, evidence_path
# logical_merge adds the integration directory to sys.path. Avoid importing this
# benchmark itself under the same module name as the S3 fixture.
import importlib.util
spec = importlib.util.spec_from_file_location('s3_merge_fixture', Path(__file__).resolve().parents[1]/'test/integration/s3_merge.py')
fixture = importlib.util.module_from_spec(spec); spec.loader.exec_module(fixture)


def main(endpoint, temporary, mode=''):
    additive = mode == '--additive'
    target = mode == '--target'
    if mode and not (additive or target): raise ValueError('expected --additive or --target')
    report = {'platform': platform.platform(), 'machine': platform.machine(),
              'extension_sha256': hashlib.sha256(EXTENSION.read_bytes()).hexdigest(),
              'provider': 'disposable pinned MinIO on local Docker; no real AWS',
              'memory_limit': '128MB', 'threads': 2, 'spill_limit': '256MB',
              'process_timeout_s': 180, 'case_disk_limit_bytes': 536870912, 'free_reserve_bytes': 1073741824,
              'schema': 'x(id INT PRIMARY KEY,a BIGINT NOT NULL CHECK(a>=0),b VARCHAR,amount DECIMAL(18,3),flag BOOLEAN); INDEX(a,id); VIEW visible AS SELECT id,a,b FROM x',
              'initial': "id=i,a=i,b=base,amount=1.250,flag=true",
              'changes': 'same first K keys: ours a=id+N; theirs b=theirs,amount=2.375,flag=false',
              'inputs': 'explicit snapshots 1,2,3; all evicted and GC reclaimed before merge; live branch heads advanced to exclude them',
              'samples': []}
    report['schema_mode'] = 'additive' if additive else 'strict'
    report['destination_mode'] = 'existing target' if target else 'new branch'
    if target:
        report['inputs'] = 'branch heads ours=2/theirs=3, selected base=1 evicted and collected; target/source heads retained locally; main advanced beyond the base'
        report['changes'] += '; main later changes id=0 outside the selected inputs'
    if additive:
        report['changes'] += '; ours adds ours_note VARCHAR, sets changed keys to ours; theirs adds theirs_delta DECIMAL(18,3), sets changed keys to 3.500; unchanged keys have NULL additions'
    base = Path(temporary) / ('merge-target-benchmark' if target else 'merge-schema-benchmark' if additive else 'merge-benchmark'); base.mkdir()
    for size in (10000,50000):
        for fraction in (.01,.20):
            changed = int(size*fraction)
            for sample in range(3):
                f = fixture.Fixture(base,f'bench-{size}-{changed}-{sample}',endpoint)
                ours=f'UPDATE o.x SET a=id+{size} WHERE id<{changed};'
                theirs=f"UPDATE t.x SET b='theirs',amount=2.375,flag=false WHERE id<{changed};"
                if additive:
                    ours += f"ALTER TABLE o.x ADD COLUMN ours_note VARCHAR; UPDATE o.x SET ours_note='ours' WHERE id<{changed};"
                    theirs += f'ALTER TABLE t.x ADD COLUMN theirs_delta DECIMAL(18,3); UPDATE t.x SET theirs_delta=3.500 WHERE id<{changed};'
                f.setup(ours=ours,theirs=theirs,size=size)
                if target:
                    f.run("ATTACH 'differential:merge_db' AS d; UPDATE d.x SET b='live' WHERE id=0; "
                          "CALL differential_create_snapshot('d'); DETACH d; "
                          "CALL differential_evict_snapshots('merge_db',versions=>[1]); CALL differential_cleanup_old_files(cleanup_all=>true);")
                    assert f.scalar('SELECT count(*) FROM snapshots s JOIN reclaimed_roots r ON s.root=r.root WHERE s.id=1') == 1
                else: f.evict()
                stats = ("SELECT metric,value FROM differential_storage_stats() WHERE metric IN "
                         "('local_logical_read_bytes','local_materialized_payload_bytes','s3_verified_get_bytes',"
                         "'s3_single_put_input_bytes','s3_get_calls','s3_put_calls');")
                merge=f.merge(options=",schema_mode=>'additive'" if additive else '')
                if target:
                    merge="SELECT * FROM differential_merge_branch('merge_db','ours','theirs','ours',request_id=>'merge');"
                destination = 'ours' if target else 'merged'
                data, usage = process(f.prefix()+merge+stats,f.folder,'merge',
                                      min_free_bytes=1073741824,max_disk_bytes=536870912)
                verification = f.run(f"ATTACH 'differential:merge_db' AS m (BRANCH '{destination}'); "
                                     f"SELECT count(*) AS n,count(*) FILTER(WHERE a=id+{size} AND b='theirs' AND amount=2.375 AND NOT flag) AS changed FROM m.x;")
                assert verification[-1] == [{'n':size,'changed':changed}],verification
                if additive:
                    verification=f.run(f"ATTACH 'differential:merge_db' AS m (BRANCH '{destination}'); "
                                       "SELECT count(*) FILTER(WHERE ours_note='ours' AND theirs_delta=3.500) AS changed, "
                                       "count(*) FILTER(WHERE ours_note IS NULL AND theirs_delta IS NULL) AS unchanged FROM m.x;")
                    assert verification[-1]==[{'changed':changed,'unchanged':size-changed}],verification
                result = data[0][0]
                record = {'rows':size,'changed_rows_per_side':changed,'sample':sample,
                          **{key:result[key] for key in ('materialization_ms','comparison_ms','application_ms','publication_ms','selection_ms','target_check_ms')},
                          'process':usage,'io_counters':{r['metric']:r['value'] for r in data[1]}}
                report['samples'].append(record); print(json.dumps(record),flush=True)
    evidence_path('s3-merge-target-benchmark.json' if target else 'merge-schema-benchmark.json' if additive else 's3-merge-benchmark.json').write_text(json.dumps(report,indent=2)+'\n')


if __name__ == '__main__': main(*sys.argv[1:])
