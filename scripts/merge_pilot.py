#!/usr/bin/env python3
"""Validate a local or single-writer S3 merge against an independent native expected database.

Branch mode reviews and guards selected snapshots. Destination equal to ours updates
that detached branch. Failed verification leaves a published result for inspection.
Retries use original inputs; abort is explicit and separately idempotent.
"""
import argparse
import json
from pathlib import Path
import sys
import time

from merge_qualification_common import (ROOT, REPORT_VERSION, CLI, QualificationError, require,
    literal, identifier, single_writer_database, sha, verify_database, write_report,
    connection_arguments, s3_options, remote_heads, verify_remote, operation_request)
sys.path.insert(0, str(ROOT / 'test/integration'))
from paths import DUCKDB_CLI


def parser():
    p = argparse.ArgumentParser(description=__doc__)
    for name in ('repository', 'report'): p.add_argument('--'+name, type=Path, required=True)
    for name in ('database', 'request-id'): p.add_argument('--'+name, required=True)
    p.add_argument('--mode', choices=('snapshots', 'branches'), default='snapshots')
    p.add_argument('--base', type=int)
    for name in ('ours', 'theirs', 'destination'): p.add_argument('--'+name)
    p.add_argument('--expected', type=Path)
    p.add_argument('--schema-mode', choices=('strict', 'additive', 'catalog'), default='strict')
    p.add_argument('--resolutions', type=Path, help='committed native database containing typed resolution tables')
    p.add_argument('--resolution-table', action='append', default=[], metavar='main.TABLE=TABLE')
    p.add_argument('--abort-operation', help='operation UUID to abort instead of merging')
    p.add_argument('--duckdb', type=Path, default=Path(DUCKDB_CLI))
    p.add_argument('--extension', type=Path, default=ROOT/'build/release/extension/differential/differential.duckdb_extension')
    p.add_argument('--memory-limit', default='128MB')
    p.add_argument('--temp-limit', default='512MB')
    p.add_argument('--timeout-seconds', type=int, default=120)
    connection_arguments(p)
    return p


def branch_state(cli, db):
    return cli.rows(f'SELECT branch_id,branch_name,state,root,head_snapshot_id FROM differential_branches({db}) ORDER BY branch_id')


def roots(cli, db, ids=None):
    where = ' WHERE snapshot_id IN ('+','.join(map(str, ids))+')' if ids else ''
    return cli.rows(f'SELECT snapshot_id,root FROM differential_snapshots({db}){where} ORDER BY snapshot_id')


def resolution_options(args):
    require(bool(args.resolutions) == bool(args.resolution_table), 'resolution_arguments')
    mapping = {}
    for option in args.resolution_table:
        target, separator, table = option.partition('=')
        require(separator and target.startswith('main.') and table and target not in mapping, 'resolution_arguments')
        mapping[target] = 'decisions.main.'+identifier(table)
    return (',resolution_tables=>map{'+','.join(literal(k)+':'+literal(v) for k,v in sorted(mapping.items()))+'}') if mapping else ''


def verify_result(cli, info, snapshot, args, report):
    report['verification'] = {}
    if report['storage']=='s3':
        verify_remote(cli,info,snapshot,args.expected,s3_options(args),report['verification'])
        report['checks'].append('independent_remote_export')
    else: verify_database(cli,info['database_id'],snapshot,args.expected,report['verification'])
    require(sha(args.expected)==report['expected_sha256'], 'expected_database_changed')
    report['checks'].append('independent_typed_data_and_catalog')


def check_remote_heads(before, after, destination, snapshot, may_publish):
    for head in before:
        if may_publish and head['branch_name']==destination: continue
        require(head in after, 'remote_input_head_changed')
    if may_publish:
        published = [h for h in after if h['branch_name']==destination]
        require(len(published)==1 and published[0]['snapshot_id']==snapshot, 'remote_head_not_published')
        prior = next((h for h in before if h['branch_name']==destination),None)
        if prior: require(prior['branch_id']==published[0]['branch_id'], 'remote_target_identity_changed')


def execute(args):
    start = time.monotonic(); cli = None; phase = 'setup'
    report = {'report_version':REPORT_VERSION, 'component':'pilot', 'status':'incomplete',
              'scope':'caller-supplied single-writer repository and independent expected database',
              'database':args.database, 'request_id':args.request_id, 'mode':args.mode,
              'schema_mode':args.schema_mode, 'action':'abort' if args.abort_operation else 'merge', 'checks':[]}
    try:
        require((args.repository/'engine.sqlite').is_file() and args.request_id and 0 < args.timeout_seconds <= 3600,
                'pilot_arguments')
        cli = CLI(args.duckdb, args.extension, args.repository, memory=args.memory_limit, temp=args.temp_limit,
                  seconds=args.timeout_seconds,timezone=args.timezone,calendar=args.calendar)
        if args.s3_profile: cli.env['AWS_PROFILE']=args.s3_profile
        report['identity'] = cli.identity(); info = single_writer_database(cli,args.database)
        remote = (info['data_path'] or '').startswith('s3://')
        report['storage'] = 's3' if remote else 'local'
        db = literal(args.database); before = branch_state(cli, db); original_roots = roots(cli, db)
        heads = remote_heads(cli,info,s3_options(args,named=True)) if remote else []
        if args.abort_operation:
            prior = operation_request(cli,args.database,args.abort_operation)
            if args.expected:
                require(args.expected.is_file(), 'pilot_arguments')
                report['expected_sha256']=sha(args.expected)
            phase = 'abort'
            sql = (f'SELECT * FROM differential_abort_merge({db},{literal(args.abort_operation)},'
                   f'request_id=>{literal(args.request_id)})')
            result = cli.rows(sql)
            require(cli.rows(sql) == result, 'abort_replay_changed')
            report.update(abort=result,checks=['abort_replay'])
            after=branch_state(cli,db); after_roots=roots(cli,db)
            completed = result[0]['state']=='complete'
            recovery = completed and prior['state']!='complete'
            for branch in before:
                if recovery and branch['branch_name']==prior['destination']:
                    require(any(b['branch_id']==branch['branch_id'] and b['head_snapshot_id']==result[0]['snapshot_id'] for b in after),
                            'target_identity_changed')
                else: require(branch in after,'abort_changed_inputs')
            require(all(r in after_roots for r in original_roots), 'abort_changed_inputs')
            if not recovery: require(after==before and after_roots==original_roots, 'abort_changed_inputs')
            if remote:
                check_remote_heads(heads,remote_heads(cli,info,s3_options(args,named=True)),prior['destination'],result[0]['snapshot_id'],recovery)
                report['checks'].append('remote_head_isolation')
            report['checks'].append('input_roots_and_branch_isolation')
            if completed:
                phase='verification'
                require(args.expected is not None,'expected_database_required_for_recovered_result')
                verify_result(cli,info,result[0]['snapshot_id'],args,report)
            else: report['checks'].append('unchanged_branches_and_roots')
            report['status']='passed'
        else:
            require(args.ours and args.theirs and args.destination and args.expected and args.expected.is_file(), 'pilot_arguments')
            expected_hash = sha(args.expected); report['expected_sha256'] = expected_hash
            options = f',schema_mode=>{literal(args.schema_mode)},timeout_ms=>{args.timeout_seconds*1000}' + resolution_options(args)
            inspect = f'SELECT * FROM differential_merge_requests({db},request_id=>{literal(args.request_id)})'
            previous = cli.rows(inspect); prior = previous[0] if previous else None
            report['previous_request'] = prior
            replay = bool(prior and prior['state'] == 'complete'); report['completed_request_replay'] = replay
            if prior:
                require(prior['destination'] == args.destination and prior['schema_mode'] == args.schema_mode, 'request_mismatch')
            if args.mode == 'snapshots':
                try: ids = (args.base, int(args.ours), int(args.theirs))
                except ValueError: raise QualificationError('pilot_arguments')
                require(all(isinstance(i, int) and i > 0 for i in ids), 'pilot_arguments')
                if prior: require(prior['merge_kind'] == 'merge' and tuple(prior[n+'_snapshot_id'] for n in ('base','ours','theirs')) == ids, 'request_mismatch')
                arguments = db+','+','.join(map(str, ids)); function = 'differential_merge_snapshots'
            else:
                require(args.base is None, 'branch_base_is_selected')
                if prior:
                    require(prior['merge_kind'] == 'merge_branch' and prior['ours_branch_name'] == args.ours
                            and prior['theirs_branch_name'] == args.theirs and prior['inputs_guarded'], 'request_mismatch')
                ids = tuple(prior[n+'_snapshot_id'] for n in ('base','ours','theirs')) if prior else None
                arguments = ','.join(map(literal, (args.database, args.ours, args.theirs))); function = 'differential_merge_branch'
            attach = ''
            if args.resolutions and not replay:
                require(prior or args.resolutions.is_file(), 'resolution_database_missing')
                if args.resolutions.is_file():
                    attach = f'ATTACH {literal(args.resolutions.resolve())} AS decisions (READ_ONLY);'
            if not prior:
                phase = 'preflight'
                if args.mode == 'branches':
                    review = cli.rows(attach+f'SELECT * FROM differential_merge_branch_preflight({arguments},{literal(args.destination)},validate=>true{options})')[0]
                    report['review'] = {key:review[key] for key in ('ready','validation_state','merge_action')}
                    require(review['ready'] and review['validation_state'] == 'valid', 'preflight_not_ready')
                    ids = tuple(review[n+'_snapshot_id'] for n in ('base','ours','theirs'))
                else:
                    review = cli.rows(attach+f'SELECT validation_state,conflicts FROM differential_merge_snapshots_preview({arguments},validate=>true,conflict_limit=>0{options})')
                    report['review'] = review
                    require(all(r['validation_state'] == 'valid' and r['conflicts'] == 0 for r in review), 'preflight_not_ready')
                require(branch_state(cli, db) == before and roots(cli, db) == original_roots, 'preflight_changed_inputs')
                report['checks'].append('preflight_unchanged_inputs')
            if args.mode == 'branches':
                options += ''.join(f',expected_{name}_snapshot_id=>{sid}' for name,sid in zip(('base','ours','theirs'),ids))
            report['inputs'] = dict(zip(('base','ours','theirs'), ids)); report['input_roots'] = roots(cli, db, ids)
            sql = f'SELECT * FROM {function}({arguments},{literal(args.destination)},request_id=>{literal(args.request_id)}{options})'
            phase = 'merge'; result = cli.rows(attach+sql)[0]; report['merge'] = result
            record = cli.rows(inspect)[0]; report['request'] = record
            require(record['state'] == 'complete' and record['result_snapshot_id'] == result['snapshot_id'], 'incomplete_publication')
            phase = 'replay'; repeated = cli.rows(sql)[0]
            for key in ('operation_id','snapshot_id'): require(result[key] == repeated[key], 'replay_changed_result')
            require(cli.rows(inspect)[0] == record, 'replay_changed_diagnostics')
            report['checks'].append('completed_replay_and_diagnostics')
            after = branch_state(cli, db)
            target = next((b for b in before if b['state'] == 'active' and b['branch_name'] == args.destination), None)
            for branch in before:
                if branch == target and not replay and args.mode == 'branches' and args.destination == args.ours: continue
                require(branch in after, 'input_branch_changed')
            if not replay:
                published = [b for b in after if b['state'] == 'active' and b['branch_name'] == args.destination]
                require(len(published) == 1 and published[0]['head_snapshot_id'] == result['snapshot_id'], 'destination_not_published')
                if target: require(published[0]['branch_id'] == target['branch_id'], 'target_identity_changed')
            require(roots(cli, db, ids) == report['input_roots'], 'input_roots_changed')
            report['checks'].append('input_roots_and_branch_isolation')
            if remote:
                check_remote_heads(heads,remote_heads(cli,info,s3_options(args,named=True)),args.destination,result['snapshot_id'],not replay)
                report['checks'].append('remote_head_isolation')
            phase = 'verification'
            verify_result(cli,info,result['snapshot_id'],args,report)
            report['status'] = 'passed'
    except QualificationError as error:
        report.update(status='incomplete' if phase == 'verification' and error.code in
                      ('retired','not_found','expected_database_required_for_recovered_result') else 'failed',
                      error_code=error.code, phase=phase)
    except Exception:
        report.update(status='failed', error_code='pilot_error', phase=phase)
    finally:
        if cli and 'identity' in report:
            try: cli.check_artifacts(report['identity'])
            except QualificationError as error: report.update(status='failed', error_code=error.code)
        report['seconds'] = time.monotonic()-start
        report['peak_child_rss_bytes'] = max((s['peak_rss_bytes'] for s in cli.samples), default=0) if cli else 0
        report['processes'] = len(cli.samples) if cli else 0
        write_report(args.report, report)
    return report


def main():
    args = parser().parse_args(); report = execute(args)
    print(json.dumps({'status':report['status'], 'report':str(args.report), 'error_code':report.get('error_code')}))
    return {'passed':0, 'incomplete':2, 'failed':1}[report['status']]


if __name__ == '__main__': sys.exit(main())
