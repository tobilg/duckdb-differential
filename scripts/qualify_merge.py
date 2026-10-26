#!/usr/bin/env python3
"""Run a bounded local or single-writer S3 gate and record the tested candidate.

This invokes fixed test entry points. It never treats old transcripts, missing tests,
or a local success as platform-wide or production release approval.
"""
import argparse
import json
import os
from pathlib import Path
import sys
import tempfile
import time
import uuid

from merge_qualification_common import (ROOT, REPORT_VERSION, CLI, QualificationError, require,
    process, sha, write_report, connection_arguments)
from merge_soak import SCENARIOS, NATIVE_SCENARIOS
from merge_native_soak import TYPES as NATIVE_TYPES
sys.path.insert(0,str(ROOT/'test/integration'))
from paths import DUCKDB_CLI, DUCKDB_LIBRARY, EXTENSION
from s3_merge_pilot import connection_flags

COMPONENTS = ('pilot_rehearsal','soak','native_soak','compatibility')
S3_COMPONENTS = ('s3_pilot_rehearsal','native_soak','compatibility')
REQUIRED = {
    'pilot_rehearsal': {'snapshot_new_branch_replay','guarded_new_branch_target_advanced_replay_and_mismatch',
                      'catalog_removal_column_view_index_default_constraint_verification','typed_resolution_and_replay_without_source',
                      'wrong_expected_data_fails_without_leaking_values_or_undoing_publication','unsupported_expected_macro_rejected',
                      'interrupted_target_retry_and_abort_after_expiry'},
    'soak': set(SCENARIOS) | {'typed_model','catalog','ancestry','replay','retention','gc'},
    'native_soak':set(NATIVE_SCENARIOS) | {'typed_model','native_equality','whole_nested_values','frozen_context','tracked_views','catalog','ancestry','replay','retention','gc'},
    's3_pilot_rehearsal':{'s3_snapshot_new_branch_remote_replay','s3_guarded_target_and_original_export_after_head_movement_expiry',
        's3_wrong_expected_data_fails_without_disclosing_values','s3_shared_scope_rejected_before_merge',
        's3_frozen_context_prepared_and_published_retry','s3_safe_abort_replay_and_released_destination',
        's3_published_abort_requires_independent_verification','s3_uncertain_abort_is_not_reported_as_success',
        's3_frozen_nested_resolution_without_source'},
}
REQUIRED['pilot_rehearsal'].add('pending_frozen_resolution_without_source')
REQUIRED['pilot_rehearsal'].add('expected_collation_mismatch_rejected')
OPEN_GATES = [
    'clean committed candidate and reproducible release build',
    'release artifact SQL tests and complete integration/upstream PR checks',
    'Linux amd64/arm64 and macOS amd64/arm64 candidate results',
    'representative user-data pilot with independently prepared expected results',
    'manual full upstream suite and reviewed exclusion audit',
    'S3 candidate qualification including owner-run real AWS checks',
    'power-loss and host/filesystem qualification appropriate to the supported durability claims',
]


def validate_receipts(records, identity, run_id, storage='local'):
    """Fail closed on missing, stale, mixed-candidate or altered component evidence."""
    components=S3_COMPONENTS if storage=='s3' else COMPONENTS
    require(len(records)==len(components) and {r['component'] for r in records}==set(components), 'missing_components')
    for record in records:
        require(record['run_id']==run_id and record['exit_code']==0, 'component_not_executed_successfully')
        path=Path(record['path']); require(path.is_file() and path.stat().st_size <= 8*1024**2, 'invalid_receipt')
        require(sha(path)==record['sha256'], 'receipt_changed')
        receipt=json.loads(path.read_text())
        require(receipt.get('report_version')==REPORT_VERSION and receipt.get('component')==record['component'], 'wrong_receipt')
        require(receipt.get('identity')==identity, 'mixed_candidate_receipt')
        require(receipt.get('status')=='passed' and receipt.get('checks') and not receipt.get('not_run'), 'incomplete_component')
        require(REQUIRED.get(record['component'],set()) <= set(receipt['checks']), 'missing_checks')
        if record['component'] in ('soak','native_soak'):
            config=receipt['configuration']; runs=receipt.get('runs',[])
            require([r['seed'] for r in runs]==config['seeds'] and runs, 'missing_soak_runs')
            for run in runs:
                trace=run['trace']
                require(len(trace)==config['steps'] and all(r['status']=='passed' for r in trace), 'incomplete_soak')
                scenarios=NATIVE_SCENARIOS if record['component']=='native_soak' else SCENARIOS
                require(set(scenarios) <= {r['scenario'] for r in trace}, 'missing_soak_scenarios')
                if record['component']=='native_soak':
                    require(config.get('profile')=='native' and config.get('storage')==storage,'wrong_soak_scope')
                    require(all(r.get('verification',{}).get('complete') for r in trace),'incomplete_native_verification')
                    require(run.get('result',{}).get('types')==NATIVE_TYPES and len(run['result'].get('native_equality_sha256',''))==64,'native_oracle_missing')
                    if storage=='s3':
                        require({'remote_export','reserved_destination','remote_head_isolation'}<=set(receipt['checks']),'missing_checks')
                        require(all(r.get('remote_verification',{}).get('complete') and r['remote_verification'].get('fresh_repository') for r in trace),'incomplete_remote_verification')
                        require(all(r.get('reservation_checked') for r in trace if r['scenario'] in ('publication_retry','publication_abort')),'reservation_not_verified')
        if record['component']=='compatibility':
            require(set(receipt.get('fixtures',[]))=={p.name for p in (ROOT/'test/fixtures/merge').glob('*.json')} and
                    len(receipt['checks'])==4*len(receipt['fixtures']), 'missing_fixture_checks')
    return True


def execute(args):
    start=time.monotonic(); run_id=str(uuid.uuid4()); args.output.mkdir(parents=True,exist_ok=True)
    folder=args.output/run_id; folder.mkdir(mode=0o700)
    report={'report_version':REPORT_VERSION,'component':'local_merge_qualification','run_id':run_id,'status':'incomplete',
            'release_ready':False,'scope':'local synthetic workflow and recovery qualification on one platform',
            'open_release_gates':OPEN_GATES,'components':[]}
    storage='s3' if args.s3_location else 'local'
    report['storage']=storage
    if storage=='s3': report.update(component='s3_merge_qualification',scope='fresh single-writer S3 synthetic workflows and local compatibility on one platform')
    try:
        require(0 < args.timeout_seconds <= 3600, 'invalid_qualification_deadline')
        with tempfile.TemporaryDirectory(prefix='differential-qualification-identity-') as tmp:
            cli=CLI(args.duckdb,args.extension,Path(tmp)/'repo')
            identity=cli.identity(); report['identity']=identity
            require(identity['test_hooks'], 'test_hooks_required')
            library_hash=sha(args.library); report['duckdb_library_sha256']=library_hash
            fixture_dir=ROOT/'test/fixtures/merge'
            fixtures={p.name:sha(p) for p in sorted(fixture_dir.iterdir()) if p.suffix in ('.json','.gz')}
            require(fixtures, 'missing_fixtures')
            report['fixture_hashes']=fixtures
            env={k:v for k,v in os.environ.items() if not k.startswith('DIFFERENTIAL_')}
            env.update(DIFFERENTIAL_EXTENSION=str(args.extension.resolve()),DUCKDB_BINARY=str(args.duckdb.resolve()),
                       DUCKDB_LIBRARY=str(args.library.resolve()),DIFFERENTIAL_EVIDENCE_DIR=str(folder.resolve()))
            commands=[
                ('pilot_rehearsal',[sys.executable,ROOT/'test/integration/merge_pilot.py','--report',folder/'pilot_rehearsal.json',
                                    '--duckdb',args.duckdb,'--extension',args.extension]),
                ('soak',[sys.executable,ROOT/'scripts/merge_soak.py','--report',folder/'soak.json','--seeds',*map(str,args.seeds),
                         '--steps',str(args.steps),'--rows',str(args.rows),'--timeout-seconds',str(args.timeout_seconds),
                         '--duckdb',args.duckdb,'--extension',args.extension]),
                ('native_soak',[sys.executable,ROOT/'scripts/merge_soak.py','--profile','native',
                         '--report',folder/'native_soak.json','--seeds',*map(str,args.seeds),
                         '--steps',str(args.steps),'--rows',str(args.rows),'--timeout-seconds',str(args.timeout_seconds),
                         '--duckdb',args.duckdb,'--extension',args.extension]),
                ('compatibility',[sys.executable,ROOT/'test/integration/merge_compatibility.py'])]
            if storage=='s3':
                connection=connection_flags(args)
                commands=[
                    ('s3_pilot_rehearsal',[sys.executable,ROOT/'test/integration/s3_merge_pilot.py','--report',folder/'s3_pilot_rehearsal.json',
                        '--s3-location',args.s3_location,'--duckdb',args.duckdb,'--extension',args.extension,
                        '--timeout-seconds',str(args.timeout_seconds),*connection]),
                    ('native_soak',[sys.executable,ROOT/'scripts/merge_soak.py','--profile','native','--s3-location',args.s3_location,
                        '--report',folder/'native_soak.json','--seeds',*map(str,args.seeds),'--steps',str(args.steps),'--rows',str(args.rows),
                        '--timeout-seconds',str(args.timeout_seconds),'--duckdb',args.duckdb,'--extension',args.extension,*connection]),
                    commands[-1]]
            for component,command in commands:
                remaining=args.timeout_seconds-(time.monotonic()-start)
                require(remaining>0, 'qualification_timeout')
                rc,out,err,usage=process(command,env=env,seconds=min(remaining,3600))
                path=folder/(component+'.json')
                if component=='compatibility':
                    original=folder/'merge-compatibility.json'
                    require(original.is_file(), 'compatibility_receipt_missing')
                    result=json.loads(original.read_text())
                    receipt={'report_version':REPORT_VERSION,'component':component,'identity':identity,
                             'status':'passed' if rc==0 else 'failed','checks':result.get('passed',[]),
                             'fixtures':result.get('fixtures',[]),'raw_report_sha256':sha(original),
                             'duckdb_library_sha256':library_hash}
                    write_report(path,receipt)
                require(path.is_file(), 'component_receipt_missing')
                report['components'].append({'component':component,'run_id':run_id,'path':str(path.resolve()),
                    'sha256':sha(path),'exit_code':rc,'process':usage})
                require(rc==0, 'component_failed')
                cli.check_artifacts(identity)
                require(sha(args.library)==library_hash,'library_changed_during_run')
                require(all(sha(fixture_dir/name)==digest for name,digest in fixtures.items()),'fixtures_changed_during_run')
            validate_receipts(report['components'],identity,run_id,storage)
            report['status']='passed'
    except QualificationError as error: report.update(status='failed',error_code=error.code)
    except Exception: report.update(status='failed',error_code='qualification_error')
    report['seconds']=time.monotonic()-start
    write_report(folder/'acceptance.json',report); write_report(args.output/'latest.json',report)
    return report


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--output',type=Path,default=ROOT/'build/evidence/merge-qualification')
    p.add_argument('--seeds',type=int,nargs='+',default=[7]); p.add_argument('--steps',type=int,default=12)
    p.add_argument('--rows',type=int,default=32); p.add_argument('--timeout-seconds',type=int,default=600)
    p.add_argument('--duckdb',type=Path,default=Path(DUCKDB_CLI)); p.add_argument('--extension',type=Path,default=EXTENSION)
    p.add_argument('--library',type=Path,default=Path(DUCKDB_LIBRARY))
    p.add_argument('--s3-location',help='run the S3 gate in fresh namespaces below this prefix (owner-only on real AWS)')
    connection_arguments(p)
    args=p.parse_args(); report=execute(args)
    print(json.dumps({'status':report['status'],'release_ready':False,'report':str(args.output/'latest.json'),
                      'error_code':report.get('error_code')}))
    return 0 if report['status']=='passed' else 1


if __name__=='__main__': sys.exit(main())
