#!/usr/bin/env python3
"""Bounded 100k/1M native-type merges, with exact verification and separate phase costs.

Existing single-writer S3 providers are optional; real AWS invocation is owner-only.
Use merge_native_scale_minio.py for a fresh bounded provider per measured case.
"""
import argparse
import json
from pathlib import Path
import sys
import tempfile
import time

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'scripts')); sys.path.insert(0,str(ROOT/'test/integration'))
from merge_qualification_common import REPORT_VERSION, QualificationError, require, write_report, connection_arguments
from merge_scale_common import ScaleFixture, STATS, workload_description
from paths import DUCKDB_CLI, EXTENSION
from evidence import evidence_path


def arguments():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--sizes',type=int,nargs='+',default=[100000,1000000])
    p.add_argument('--percent',type=int,nargs='+',default=[1,20])
    p.add_argument('--repeats',type=int,default=3)
    p.add_argument('--memory-limit',default='1GB'); p.add_argument('--temp-limit',default='2GB')
    p.add_argument('--process-seconds',type=int,default=300); p.add_argument('--case-seconds',type=int,default=900)
    p.add_argument('--disk-limit-mib',type=int,default=6144); p.add_argument('--free-space-mib',type=int,default=2048)
    p.add_argument('--s3-location',help='fresh namespaces below this existing prefix; owner-only on real AWS')
    p.add_argument('--report',type=Path,default=evidence_path('merge-native-scale.json'))
    p.add_argument('--duckdb',type=Path,default=Path(DUCKDB_CLI)); p.add_argument('--extension',type=Path,default=EXTENSION)
    connection_arguments(p)
    return p


def execute(args):
    report={'report_version':REPORT_VERSION,'component':'native_scale','status':'incomplete',
            'storage':'s3' if args.s3_location else 'local','workload':workload_description(),
            'configuration':{name:getattr(args,name) for name in ('sizes','percent','repeats','memory_limit','temp_limit',
                'process_seconds','case_seconds','disk_limit_mib','free_space_mib')},'cases':[]}
    report['configuration'].update(threads=2,force_external=False,os_caches_flushed=False)
    started=time.monotonic()
    try:
        require(1<=len(args.sizes)<=2 and len(set(args.sizes))==len(args.sizes) and all(1000<=n<=1000000 for n in args.sizes),'scale_sizes')
        require(1<=args.repeats<=3 and 1<=len(args.percent)<=2 and set(args.percent)<={1,20} and len(set(args.percent))==len(args.percent),'scale_matrix')
        require(0<args.process_seconds<=600 and args.process_seconds<=args.case_seconds<=1800,'scale_deadline')
        require(0<args.disk_limit_mib<=8192 and 0<=args.free_space_mib<=8192,'scale_disk_budget')
        require(args.timezone in (None,'UTC') and args.calendar in (None,'gregorian'),'scale_context')
        for size in args.sizes:
            for percent in args.percent:
                for repeat in range(args.repeats):
                    case={'rows':size,'changed_rows_per_side':size*percent//100,
                          'inserts_per_side':1,'deletes_per_side':1,'distinct_changed_keys':size*percent//100+4,
                          'percent':percent,'repeat':repeat,'status':'running'}
                    report['cases'].append(case)
                    with tempfile.TemporaryDirectory(prefix='differential-native-scale-') as tmp:
                        f=ScaleFixture(tmp,args,rows=size,percent=percent)
                        if 'identity' in report: require(report['identity']==f.identity,'candidate_changed')
                        else: report['identity']=f.identity
                        case['provider_namespace']=f.location or None
                        case['setup_initial_process']=f.setup()
                        measured,_=f.measure(f.merge_sql()+STATS)
                        case['measurement']={k:v for k,v in measured.items() if k!='data'}
                        if measured['returncode']:
                            case['failed_request']=f.request()
                        require(measured['returncode']==0,measured['error_code'] or 'merge_failed')
                        case.update(f.complete(measured),status='passed')
                    write_report(args.report,report)
                    print(json.dumps({k:case[k] for k in ('rows','percent','repeat','status','timings_ms','process')}),flush=True)
        report['status']='passed'
    except QualificationError as error: report.update(status='failed',error_code=error.code)
    except Exception: report.update(status='failed',error_code='scale_error')
    if report['cases'] and report['cases'][-1]['status']=='running': report['cases'][-1]['status']='failed'
    report['seconds']=time.monotonic()-started
    write_report(args.report,report)
    return report


if __name__=='__main__':
    args=arguments().parse_args(); report=execute(args)
    print(json.dumps({'status':report['status'],'report':str(args.report),'error_code':report.get('error_code')}))
    sys.exit(0 if report['status']=='passed' else 1)
