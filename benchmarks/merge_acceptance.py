#!/usr/bin/env python3
"""Repeated, bounded local merge acceptance measurements; all repositories are disposable.
Fresh DuckDB processes; OS caches are not flushed. No change-proportional scaling claim.
"""
import argparse
import hashlib
import json
from pathlib import Path
import platform
import shutil
import statistics
import subprocess
import sys
import tempfile
import time

from logical_merge import process as measured_process
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT/'test/integration'))
from paths import DUCKDB_CLI, EXTENSION
from evidence import evidence_path


def process(sql, folder, label):
    return measured_process(sql, folder, label, min_free_bytes=128*1024*1024, max_disk_bytes=1536*1024*1024)


def workload(kind, size, changed):
    key = 'id BIGINT PRIMARY KEY'
    payload = 'md5(i::VARCHAR)'
    predicate = f'id<{changed}'
    key_values = 'i'
    if kind == 'composite':
        key = 'tenant SMALLINT,id VARCHAR,PRIMARY KEY(tenant,id)'
        key_values = "i%16,printf('%012d',i)"
        predicate = f"id<printf('%012d',{changed})"
    if kind == 'wide': payload = "sha256(i::VARCHAR)||sha256((i+1)::VARCHAR)||sha256((i+2)::VARCHAR)||sha256((i+3)::VARCHAR)"
    ddl = f'CREATE TABLE d.x({key},a BIGINT,b VARCHAR,flag BOOLEAN,amount DECIMAL(18,3));'
    initial = f'INSERT INTO d.x SELECT {key_values},0,{payload},i%2=0,(i%10000)*0.001 FROM range({size}) r(i);'
    left = f'UPDATE o.x SET a=1 WHERE {predicate};'
    right = f"UPDATE t.x SET b='changed',amount=2.375 WHERE {predicate};"
    if kind == 'conflicts': right = f'UPDATE t.x SET a=2 WHERE {predicate};'
    return ddl, initial, left, right


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--sizes', type=int, nargs='+', default=[100000,1000000])
    parser.add_argument('--repeats', type=int, default=3)
    parser.add_argument('--workloads', nargs='+', choices=['narrow','wide','composite','conflicts'], default=['narrow','wide','composite','conflicts'])
    parser.add_argument('--output', type=Path, default=evidence_path('merge-acceptance-benchmark.json'))
    args = parser.parse_args()
    if not 1 <= args.repeats <= 10 or not all(100 <= n <= 10000000 for n in args.sizes): parser.error('repeats: 1..10; sizes: 100..10000000')
    report = {'platform':platform.platform(),'machine':platform.machine(),'memory_limit':'256MB','threads':2,
              'duckdb_version':subprocess.check_output([DUCKDB_CLI,'-init','/dev/null','-no-agent','--version'],text=True).strip(),
              'extension_sha256':hashlib.sha256(Path(EXTENSION).read_bytes()).hexdigest(),
              'max_temp_directory_size':'512MB','max_stage_write_bytes':536870912,
              'process_deadline_seconds':180,'os_cache':'not flushed; fresh DuckDB process per phase',
              'disk_sampling':'100 ms; sampled peaks are lower bounds; 1.5 GiB per-case guard and 128 MiB free-space reserve',
              'cases':[],'blocked':[],'summaries':[]}
    def save(): args.output.write_text(json.dumps(report,indent=2)+'\n')
    with tempfile.TemporaryDirectory(prefix='differential-merge-acceptance-') as tmp:
        root = Path(tmp)
        for size in args.sizes:
            for kind in args.workloads:
                # Conservative admission estimate plus a reserve; this is a benchmark
                # guard, not a storage-engine disk quota or an automatic test exclusion.
                estimate = size * (1024 if kind == 'wide' else 512) + 256*1024*1024
                if shutil.disk_usage(root).free < estimate:
                    report['blocked'].append({'rows':size,'workload':kind,'reason':'insufficient free disk for bounded case',
                                               'estimated_required_bytes':estimate,'free_bytes':shutil.disk_usage(root).free})
                    save(); continue
                for fraction in (.01,.20):
                    changed = int(size*fraction)
                    for repeat in range(args.repeats):
                        folder = root/f'{size}-{kind}-{changed}-{repeat}'; folder.mkdir()
                        repo = folder/'repo'
                        prefix = (f"LOAD '{EXTENSION}'; SET differential_repository='{repo}'; SET threads=2; SET memory_limit='256MB';"
                                  f"SET temp_directory='{folder/'spill'}'; SET max_temp_directory_size='512MB'; SET differential_auto_gc=false;")
                        ddl, initial, ours, theirs = workload(kind,size,changed)
                        setup = ("ATTACH 'differential:bench' AS d;"+ddl+initial+"CALL differential_create_snapshot('d');"
                                 "CALL differential_create_branch('d','ours',snapshot=>1); CALL differential_create_branch('d','theirs',snapshot=>1); DETACH d;"
                                 "ATTACH 'differential:bench' AS o (BRANCH 'ours');"+ours+"CALL differential_create_snapshot('o'); DETACH o;"
                                 "ATTACH 'differential:bench' AS t (BRANCH 'theirs');"+theirs+"CALL differential_create_snapshot('t'); DETACH t;")
                        case = {'rows':size,'workload':kind,'changed_rows_per_side':changed,'repeat':repeat,'ddl':ddl,'initial':initial,'ours':ours,'theirs':theirs}
                        started = time.monotonic()
                        try:
                            _, case['setup'] = process(prefix+setup,folder,'setup')
                            before = sum(p.stat().st_size for p in repo.rglob('*') if p.is_file())
                            preview, case['preview'] = process(prefix+f"SELECT conflicts FROM differential_merge_snapshots_preview('bench',1,2,3,conflict_limit=>0,max_input_rows=>{3*size});",folder,'preview')
                            assert preview[0][0]['conflicts'] == (changed if kind == 'conflicts' else 0), preview
                            if kind != 'conflicts':
                                merged, case['merge'] = process(prefix+f"SELECT * FROM differential_merge_snapshots('bench',1,2,3,'merged',request_id=>'bench',max_input_rows=>{3*size},max_stage_write_bytes=>536870912);"
                                    "SELECT metric,value FROM differential_storage_stats() WHERE metric IN ('local_logical_read_bytes','local_materialized_payload_bytes','local_pool_read_bytes','local_pool_write_bytes');",folder,'merge')
                                case.update(comparison_ms=merged[0][0]['comparison_ms'], application_ms=merged[0][0]['application_ms'], counters={r['metric']:r['value'] for r in merged[1]})
                                expected = ddl.replace('d.x','expected')+initial.replace('d.x','expected')+ours.replace('o.x','expected')+theirs.replace('t.x','expected')
                                verified, _ = process(prefix+"ATTACH 'differential:bench' AS m (BRANCH 'merged');"+expected+
                                    'SELECT count(*) differences FROM ((SELECT * FROM m.x EXCEPT ALL SELECT * FROM expected) UNION ALL (SELECT * FROM expected EXCEPT ALL SELECT * FROM m.x));',folder,'verify')
                                assert verified[0][0]['differences'] == 0, verified
                            case['repository_bytes_before'] = before
                            case['repository_bytes_after'] = sum(p.stat().st_size for p in repo.rglob('*') if p.is_file())
                            _, case['cleanup'] = process(prefix+'CALL differential_cleanup_old_files(max_objects=>10000);',folder,'cleanup')
                            case['repository_bytes_after_cleanup'] = sum(p.stat().st_size for p in repo.rglob('*') if p.is_file())
                            case['result'] = 'passed'
                        except Exception as error:
                            case['result'] = 'failed'; case['error'] = str(error)
                            if hasattr(error,'metrics'):
                                case['failed_phase'] = error.args[0][0]
                                case['failed_phase_metrics'] = error.metrics
                        finally:
                            case['total_seconds'] = time.monotonic()-started
                            shutil.rmtree(folder)
                        report['cases'].append(case); save()
                        print(json.dumps({k:case[k] for k in ('rows','workload','changed_rows_per_side','repeat','result','total_seconds')}),flush=True)
    for size in args.sizes:
        for kind in args.workloads:
            for fraction in (.01,.20):
                group = [c for c in report['cases'] if c['rows']==size and c['workload']==kind and c['changed_rows_per_side']==int(size*fraction)]
                if not group: continue
                summary = {'rows':size,'workload':kind,'changed_rows_per_side':int(size*fraction),'samples':len(group),
                           'failed':sum(c['result']!='passed' for c in group)}
                group = [c for c in group if c['result']=='passed']
                if not group:
                    report['summaries'].append(summary); continue
                for field in ('comparison_ms','application_ms'):
                    if field in group[0]:
                        samples = [c[field] for c in group]
                        summary[field] = {'median':statistics.median(samples),'max':max(samples),'min':min(samples)}
                summary['preview_wall_ms_median'] = statistics.median(c['preview']['wall_ms'] for c in group)
                summary['peak_rss_bytes_max'] = max(c.get('merge',c['preview'])['peak_rss_bytes'] for c in group)
                report['summaries'].append(summary)
    save(); print(args.output)
    return 1 if any(c['result'] != 'passed' for c in report['cases']) or report['blocked'] else 0


if __name__=='__main__': sys.exit(main())
