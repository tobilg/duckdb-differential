#!/usr/bin/env python3
"""Native ARRAY/STRUCT/LIST spill exhaustion, SIGINT/SIGKILL and exact recovery.

Temporary-directory exhaustion is proved by DuckDB's own error. Signals are sent while
the merge waits at a phase gate, so nothing depends on host speed; this needs the
integration build's test hooks. Uses only disposable local stores or fresh namespaces
below an explicitly supplied single-writer S3 provider.
"""
import argparse
import itertools
import json
from pathlib import Path
import signal
import sys
import tempfile
import time

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'scripts'))
from merge_qualification_common import REPORT_VERSION, QualificationError, require, write_report, connection_arguments
from merge_scale_common import ScaleFixture, STATS, spill_files, workload_description
from paths import DUCKDB_CLI, EXTENSION
from evidence import evidence_path

# Entered before each table's native comparison, so every attempt reaches it; at
# 64MB that comparison runs out of memory and no later phase would be reached.
SIGNAL_PHASE='comparison'


def arguments():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--rows',type=int,default=100000)
    p.add_argument('--report',type=Path,default=evidence_path('merge-native-spill.json'))
    p.add_argument('--s3-location')
    p.add_argument('--duckdb',type=Path,default=Path(DUCKDB_CLI)); p.add_argument('--extension',type=Path,default=EXTENSION)
    p.set_defaults(memory_limit='256MB',temp_limit='2GB',process_seconds=180,case_seconds=600,
                   disk_limit_mib=4096,free_space_mib=2048)
    connection_arguments(p)
    return p


def execute(args):
    start=time.monotonic()
    report={'report_version':REPORT_VERSION,'component':'native_spill','status':'incomplete',
            'storage':'s3' if args.s3_location else 'local','workload':workload_description(),
            'configuration':{'rows':args.rows,'percent':20,'fault_memory_limit':'64MB','retry_memory_limit':'256MB','temp_limit':args.temp_limit,
                'exhaustion_temp_limit':'8MB','force_external':True,'threads':2,'signal_phase':SIGNAL_PHASE,
                'process_seconds':args.process_seconds,'case_seconds':args.case_seconds,
                'disk_limit_mib':args.disk_limit_mib,'free_space_mib':args.free_space_mib,
                'destinations':['new_branch','existing_target']},'cases':[]}
    try:
        require(args.rows==100000,'spill_rows')
        require(args.timezone in (None,'UTC') and args.calendar in (None,'gregorian'),'spill_context')
        faults=[('temporary_disk',None),('cancel',signal.SIGINT),('death',signal.SIGKILL)]
        for destination,(fault,signum) in itertools.product(('merged','ours'),faults):
            case={'fault':fault,'destination':destination,'status':'running'}; report['cases'].append(case)
            with tempfile.TemporaryDirectory(prefix='differential-native-spill-') as tmp:
                f=ScaleFixture(tmp,args,rows=args.rows,percent=20,destination=destination)
                if 'identity' in report: require(report['identity']==f.identity,'candidate_changed')
                else: report['identity']=f.identity
                f.setup(); require(not spill_files(f.spill),'preexisting_spill')
                measured,observation=f.measure(f.merge_sql(),memory='64MB',
                    temp='8MB' if fault=='temporary_disk' else args.temp_limit,external=True,signum=signum,phase=SIGNAL_PHASE)
                case['interrupted']={k:v for k,v in measured.items() if k!='data'}
                # DuckDB's own temporary-directory error proves the spill; sampled spill
                # sizes are informational. Signals wait for the merge at its phase gate.
                if fault=='temporary_disk': require(measured['returncode']!=0 and measured['temporary_disk_limit'],'disk_exhaustion_not_observed')
                else:
                    require(observation.sent,'gate_not_entered')
                    if fault=='death': require(measured['returncode']==-signal.SIGKILL,'death_not_observed')
                # Process death leaves an existing target durably fenced until
                # retry/abort; handled errors release the pre-publication fence.
                f.unpublished('target_locked' if destination=='ours' and fault=='death' else 'pending')
                failed=f.request(); case['failure_phase']=failed['last_attempt_phase']; case['failure_state']=failed['state']
                if fault=='cancel': require(failed['last_attempt_error_code']=='cancelled','cancellation_not_recorded')
                if destination=='ours' and fault=='death':
                    f.cli.run("ATTACH 'differential:scale' AS fenced (BRANCH 'ours');",error='writer_busy')
                recovered=f.run(f.merge_sql()+STATS,memory='256MB',external=True)
                case.update(f.complete(recovered))
                require(f.request()['attempt_count']==2,'duplicate_attempt_or_result')
                require(not spill_files(f.spill),'live_spill_after_close')
                case['status']='passed'
            print('PASS native spill '+destination+'/'+fault+': '+('temporary-directory limit' if fault=='temporary_disk' else 'signal at the '+SIGNAL_PHASE+' gate')+
                  ', preserved inputs, exact same-request recovery and cleanup',flush=True)
            write_report(args.report,report)
        report['status']='passed'
    except QualificationError as error: report.update(status='failed',error_code=error.code)
    except Exception: report.update(status='failed',error_code='spill_test_error')
    if report['cases'] and report['cases'][-1]['status']=='running': report['cases'][-1]['status']='failed'
    report['seconds']=time.monotonic()-start; write_report(args.report,report)
    return report


if __name__=='__main__':
    args=arguments().parse_args(); r=execute(args)
    summary={'status':r['status'],'report':str(args.report),'error_code':r.get('error_code')}
    # CI keeps no evidence files: print the failing case's bounded measurements.
    if r['status']!='passed' and r['cases']: summary['case']=r['cases'][-1]
    print(json.dumps(summary,default=str))
    sys.exit(0 if r['status']=='passed' else 1)
