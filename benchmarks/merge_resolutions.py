#!/usr/bin/env python3
"""Bounded typed-resolution/catalog benchmark: 2k/10k rows, 1%/10% conflicting keys.
Three fresh-process samples per shape, fresh disposable repositories, native validation.
"""
import hashlib
import json
import platform
from pathlib import Path
import tempfile
from logical_merge import process, DUCKDB_CLI, EXTENSION, evidence_path


def main():
    report = {'platform': platform.platform(), 'machine': platform.machine(),
              'extension_sha256': hashlib.sha256(EXTENSION.read_bytes()).hexdigest(),
              'memory_limit': '128MB', 'threads': 2, 'temp_limit': '256MB',
              'process_timeout_s': 180, 'disk_budget_bytes': 536870912, 'free_reserve_bytes': 1073741824,
              'schema': "x(id INT PRIMARY KEY,v BIGINT DEFAULT 0 CHECK(v>=0),note VARCHAR DEFAULT 'base',day DATE,ts TIMESTAMP); UNIQUE INDEX(v); INDEX(day,id); VIEW visible AS SELECT id,v FROM x WHERE v>=0",
              'workload': 'first K keys: ours v=id+N,day=2021-01-01; theirs v=id+2N,note=theirs,ts=2022-01-01; custom v=id+3N. All K conflicts resolved; independent cells retained.',
              'samples': []}
    with tempfile.TemporaryDirectory(prefix='differential-resolution-bench-') as temp:
        for size in (2000,10000):
            for fraction in (.01,.10):
                changed=int(size*fraction)
                for sample in range(3):
                    folder=Path(temp)/f'{size}-{changed}-{sample}'; folder.mkdir()
                    prefix=f"LOAD '{EXTENSION}'; SET differential_repository='{folder/'repo'}'; SET memory_limit='128MB'; SET threads=2; SET temp_directory='{folder/'spill'}'; SET max_temp_directory_size='256MB'; SET differential_auto_gc=false;"
                    source=f"ATTACH '{folder/'choices.duckdb'}' AS choices;"
                    setup=f"""
ATTACH 'differential:bench' AS d;
CREATE TABLE d.x(id INT PRIMARY KEY,v BIGINT DEFAULT 0 CHECK(v>=0),note VARCHAR DEFAULT 'base',day DATE,ts TIMESTAMP);
CREATE UNIQUE INDEX ux ON d.x(v); CREATE INDEX ix ON d.x(day,id);
USE d; CREATE VIEW visible AS SELECT id,v FROM x WHERE v>=0; USE memory;
INSERT INTO d.x SELECT i,i,'base',DATE '2020-01-01',NULL FROM range({size}) r(i);
CALL differential_create_snapshot('d');
CALL differential_create_branch('d','o',snapshot=>1); CALL differential_create_branch('d','t',snapshot=>1);
ATTACH 'differential:bench' AS o (BRANCH 'o'); UPDATE o.x SET v=id+{size},day=DATE '2021-01-01' WHERE id<{changed}; CALL differential_create_snapshot('o');
ATTACH 'differential:bench' AS t (BRANCH 't'); UPDATE t.x SET v=id+{2*size},note='theirs',ts=TIMESTAMP '2022-01-01' WHERE id<{changed}; CALL differential_create_snapshot('t');
{source}
CREATE TABLE choices.fixes(id INT,v BIGINT,note VARCHAR,day DATE,ts TIMESTAMP,_merge_action VARCHAR,_merge_columns MAP(VARCHAR,VARCHAR));
INSERT INTO choices.fixes SELECT i,i+{3*size},NULL,NULL,NULL,'cells',map{{'v':'custom'}} FROM range({changed}) r(i);
"""
                    def run(sql,label):
                        return process(prefix+sql,folder,label,min_free_bytes=1073741824,max_disk_bytes=536870912)
                    run(setup,'setup')
                    options=",resolution_tables=>map{'main.x':'choices.main.fixes'}"
                    preview,pu=run(source+f"SELECT conflicts,resolved_conflicts FROM differential_merge_snapshots_preview('bench',1,2,3,conflict_limit=>0{options});",'preview')
                    result,mu=run(source+f"SELECT * FROM differential_merge_snapshots('bench',1,2,3,'resolved',request_id=>'bench'{options}); SELECT metric,value FROM differential_storage_stats() WHERE metric IN ('local_logical_read_bytes','local_materialized_payload_bytes','local_pool_read_bytes','local_pool_write_bytes');",'merge')
                    verify,_=run(f"ATTACH 'differential:bench' AS m (BRANCH 'resolved'); SELECT count(*) AS n,count(*) FILTER(WHERE v=id+{3*size} AND day=DATE '2021-01-01' AND note='theirs' AND ts=TIMESTAMP '2022-01-01') AS changed FROM m.x; SELECT count(*) AS n FROM m.visible; SELECT count(*) AS n FROM differential_merge_resolutions('bench','bench');",'verify')
                    assert preview[0][0]=={'conflicts':0,'resolved_conflicts':changed},preview
                    assert verify==[[{'n':size,'changed':changed}],[{'n':size}],[{'n':changed}]],verify
                    row={'rows':size,'changed_rows_per_side':changed,'resolved_rows':changed,'sample':sample,
                         'comparison_ms':result[0][0]['comparison_ms'],'application_ms':result[0][0]['application_ms'],
                         'preview':pu,'merge':mu,'io_counters':result[1],
                         'repository_bytes':sum(p.stat().st_size for p in (folder/'repo').rglob('*') if p.is_file())}
                    report['samples'].append(row)
                    print(json.dumps(row),flush=True)
    evidence_path('merge-resolution-benchmark.json').write_text(json.dumps(report,indent=2)+'\n')


if __name__=='__main__': main()
