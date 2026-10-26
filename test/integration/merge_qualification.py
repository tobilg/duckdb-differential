#!/usr/bin/env python3
"""The local qualification gate and adversarial checks of its evidence validation."""
import copy
import json
from pathlib import Path
import sys
import tempfile

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'scripts'))
from merge_qualification_common import (Budget, QualificationError, process, require, sha, write_report)
from qualify_merge import validate_receipts
from evidence import evidence_path


def rejected(action, code):
    try: action()
    except QualificationError as error:
        require(error.code==code, 'wrong_rejection'); return
    raise QualificationError('invalid_evidence_accepted')


def check_s3_receipts(accepted):
    """Validate real S3 receipts, then prove local/incomplete evidence cannot pass."""
    with tempfile.TemporaryDirectory(prefix='differential-s3-receipts-') as tmp:
        records=copy.deepcopy(accepted['components'])
        for record in records:
            path=Path(tmp)/(record['component']+'.json')
            path.write_bytes(Path(record['path']).read_bytes()); record['path']=str(path)
        def validate(): return validate_receipts(records,accepted['identity'],accepted['run_id'],'s3')
        require(validate(),'valid_s3_evidence_rejected')
        native=next(r for r in records if r['component']=='native_soak'); path=Path(native['path'])
        original=path.read_bytes()
        changes=[
            (lambda r:r['configuration'].update(storage='local'),'wrong_soak_scope'),
            (lambda r:r['runs'][0]['trace'][0].pop('remote_verification'),'incomplete_remote_verification'),
            (lambda r:r['runs'][0]['trace'][0]['remote_verification'].update(fresh_repository=False),'incomplete_remote_verification'),
            (lambda r:next(x for x in r['runs'][0]['trace'] if x['scenario']=='publication_retry').pop('reservation_checked'),'reservation_not_verified'),
            (lambda r:r['checks'].remove('remote_head_isolation'),'missing_checks'),
        ]
        for change,code in changes:
            receipt=json.loads(original); change(receipt); write_report(path,receipt); native['sha256']=sha(path)
            rejected(validate,code)
    return len(changes)


def main():
    output=evidence_path('merge-qualification')
    rc,_,_,_=process([sys.executable,ROOT/'scripts/qualify_merge.py','--output',output],seconds=600)
    require(rc==0, 'local_gate_failed')
    accepted=json.loads((output/'latest.json').read_text())
    require(accepted['status']=='passed' and not accepted['release_ready'] and accepted['open_release_gates'], 'overstated_acceptance')
    with tempfile.TemporaryDirectory(prefix='differential-merge-receipts-') as tmp:
        root=Path(tmp)
        original=copy.deepcopy(accepted['components'])
        for record in original:
            path=root/(record['component']+'.json'); path.write_bytes(Path(record['path']).read_bytes()); record['path']=str(path)
        def validate(records): return validate_receipts(records,accepted['identity'],accepted['run_id'])
        require(validate(original),'valid_evidence_rejected')
        rejected(lambda:validate(original[:-1]),'missing_components')
        altered=copy.deepcopy(original); altered[0]['exit_code']=1
        rejected(lambda:validate(altered),'component_not_executed_successfully')
        altered=copy.deepcopy(original); altered[0]['run_id']='another-run'
        rejected(lambda:validate(altered),'component_not_executed_successfully')
        path=Path(original[0]['path']); data=path.read_bytes(); path.write_bytes(data+b' ')
        rejected(lambda:validate(original),'receipt_changed'); path.write_bytes(data)
        def modified(component, change, code):
            records=copy.deepcopy(original); item=next(r for r in records if r['component']==component)
            path=Path(item['path']); before=path.read_bytes(); receipt=json.loads(before); change(receipt)
            write_report(path,receipt); item['sha256']=sha(path)
            try: rejected(lambda:validate(records),code)
            finally: path.write_bytes(before)
        modified('pilot_rehearsal',lambda r:r['identity'].update(extension_sha256='0'*64),'mixed_candidate_receipt')
        modified('pilot_rehearsal',lambda r:r.update(status='incomplete'),'incomplete_component')
        modified('pilot_rehearsal',lambda r:r.update(not_run=['guarded_target']),'incomplete_component')
        modified('pilot_rehearsal',lambda r:r.update(checks=[]),'incomplete_component')
        modified('pilot_rehearsal',lambda r:r.update(checks=['snapshot_new_branch_replay']),'missing_checks')
        modified('soak',lambda r:r['runs'][0]['trace'].pop(),'incomplete_soak')
        modified('soak',lambda r:r['runs'][0]['trace'][0].update(status='running'),'incomplete_soak')
        modified('native_soak',lambda r:r['configuration'].update(storage='s3'),'wrong_soak_scope')
        modified('native_soak',lambda r:r['runs'][0]['trace'][0]['verification'].update(complete=False),'incomplete_native_verification')
        modified('native_soak',lambda r:r['runs'][0]['result'].update(types=[]),'native_oracle_missing')
        modified('compatibility',lambda r:r.update(fixtures=[]),'missing_fixture_checks')
        # Exercise real runner bounds, not mock timers or SQL implementation details.
        rejected(lambda:process([sys.executable,'-c','import time; time.sleep(5)'],seconds=.1),'process_timeout')
        (root/'large').write_bytes(b'x'*4096)
        rejected(lambda:Budget(root,disk_bytes=1024,free_bytes=0).check(),'disk_budget')
        rejected(lambda:process([sys.executable,'-c',"import sys; sys.stdout.write('x'*(17*1024*1024))"],seconds=10),'output_budget')
    write_report(evidence_path('merge-qualification-checks.json'),{
        'status':'passed','checks':18,'acceptance_sha256':sha(output/'latest.json'),
        'scope':'actual local gate plus fail-closed receipt, execution deadline and resource-bound checks'})
    print('PASS local merge qualification and 18 evidence/resource rejection checks',flush=True)


if __name__=='__main__': main()
