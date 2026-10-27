#!/usr/bin/env python3
"""Run native scale samples sequentially, each in its own capped disposable MinIO.

Provider setup and all object I/O reuse minio.py and the extension SDK. No cloud
resources are used. Fresh containers prevent immutable exports accumulating
across cases or silently sharing packs between repeated measurements.
"""
import argparse
import json
from pathlib import Path
import subprocess
import sys
import time
import uuid

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'scripts'));sys.path.insert(0,str(ROOT/'test/integration'))
from merge_qualification_common import REPORT_VERSION, QualificationError, require, write_report, sha
from evidence import evidence_path


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--sizes',type=int,nargs='+',default=[100000,1000000],choices=[1000,100000,1000000])
    p.add_argument('--percent',type=int,nargs='+',default=[1,20],choices=[1,20])
    p.add_argument('--repeats',type=int,default=3)
    p.add_argument('--output',type=Path,default=evidence_path('s3-merge-native-scale'))
    args=p.parse_args(); args.output.mkdir(parents=True,exist_ok=True)
    folder=args.output/str(uuid.uuid4());folder.mkdir(mode=0o700)
    report={'report_version':REPORT_VERSION,'component':'native_scale_minio','status':'incomplete',
            'configuration':{'sizes':args.sizes,'percent':args.percent,'repeats':args.repeats},'cases':[]}
    started=time.monotonic()
    try:
        require(1<=args.repeats<=3 and 1<=len(args.sizes)<=2 and len(set(args.sizes))==len(args.sizes)
                and 1<=len(args.percent)<=2 and len(set(args.percent))==len(args.percent),'scale_matrix')
        for rows in args.sizes:
            for percent in args.percent:
                for repeat in range(args.repeats):
                    label=f'{rows}-{percent}-{repeat}'; path=folder/(label+'.json')
                    record={'rows':rows,'percent':percent,'repeat':repeat,
                            'receipt':str(path.resolve()),'status':'running'}
                    report['cases'].append(record)
                    # minio.py owns its subprocess deadlines and finally removes its
                    # container. Do not kill that owner with a shorter outer timer.
                    with (folder/(label+'.log')).open('w') as log:
                        result=subprocess.run([sys.executable,ROOT/'test/integration/minio.py','--merge-native-scale-case',
                            '--scale-rows',str(rows),'--scale-percent',str(percent),'--scale-report',path],
                            cwd=ROOT,stdout=log,stderr=subprocess.STDOUT)
                    require(result.returncode==0 and path.is_file(),'minio_case_failed')
                    receipt=json.loads(path.read_text())
                    require(receipt['status']=='passed' and receipt['storage']=='s3' and len(receipt['cases'])==1,'incomplete_scale_case')
                    require(receipt['provider']['fresh_container'],'reused_provider')
                    if 'identity' in report: require(report['identity']==receipt['identity'],'mixed_candidates')
                    else: report['identity']=receipt['identity']
                    case=receipt['cases'][0]
                    require(case['rows']==rows and case['percent']==percent and case['status']=='passed','wrong_scale_case')
                    require(case['remote_verification']['complete'] and case['remote_verification']['fresh_repository'],'missing_remote_verification')
                    record.update(sha256=sha(path),status='passed')
                    write_report(folder/'matrix.json',report)
                    print('PASS native scale MinIO '+label,flush=True)
        report['status']='passed'
    except QualificationError as error: report.update(status='failed',error_code=error.code)
    except Exception: report.update(status='failed',error_code='minio_matrix_error')
    if report['cases'] and report['cases'][-1]['status']=='running': report['cases'][-1]['status']='failed'
    report['seconds']=time.monotonic()-started
    write_report(folder/'matrix.json',report);write_report(args.output/'latest.json',report)
    print(json.dumps({'status':report['status'],'report':str(args.output/'latest.json'),'error_code':report.get('error_code')}))
    return 0 if report['status']=='passed' else 1


if __name__=='__main__':sys.exit(main())
