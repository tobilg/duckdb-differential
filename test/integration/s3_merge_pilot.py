#!/usr/bin/env python3
"""Generated operator-pilot rehearsal in fresh single-writer S3 namespaces.

All object operations use the extension SDK. No namespace is deleted; MinIO's
outer disposable container owns cleanup. Real AWS invocation is owner-only.
"""
import argparse
import json
import os
from pathlib import Path
import sys
import tempfile
import time
import uuid

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'scripts'))
from merge_qualification_common import (CLI, REPORT_VERSION, Budget, QualificationError, require,
    literal, process, write_report, connection_arguments, s3_options)
from paths import DUCKDB_CLI, EXTENSION

DDL=("CREATE TABLE x(id INTEGER PRIMARY KEY,a INTEGER DEFAULT 0 CHECK(a>=0),b VARCHAR,"
     "n STRUCT(value UHUGEINT,items DOUBLE[],bytes BLOB),ts TIMESTAMPTZ_NS,day DATE,"
     "CHECK(CAST(ts AS DATE)=day)); CREATE INDEX by_a ON x(a);"
     "SET enable_view_dependencies=true; CREATE VIEW visible AS SELECT * FROM x;")
BASE="{'value':0::UHUGEINT,'items':[0::DOUBLE,NULL],'bytes':'\\x00'::BLOB}"
OURS="{'value':340282366920938463463374607431768211455::UHUGEINT,'items':['NaN'::DOUBLE,NULL],'bytes':'\\x00\\xFF'::BLOB}"
THEIRS="{'value':1::UHUGEINT,'items':['Infinity'::DOUBLE],'bytes':'\\xFF'::BLOB}"


def connection_flags(args):
    out=[]
    for name in ('s3_region','s3_endpoint','s3_profile','s3_path_style','s3_allow_http'):
        value=getattr(args,name)
        if value: out += ['--'+name.replace('_','-')]+([] if isinstance(value,bool) else [value])
    return out


def run_cases(args, report):
    require(args.s3_location.startswith('s3://'),'s3_location_required')
    with tempfile.TemporaryDirectory(prefix='differential-s3-pilot-') as tmp:
        root=Path(tmp); budget=Budget(root,seconds=args.timeout_seconds)
        cli=CLI(args.duckdb,args.extension,root/'repo',budget=budget,spill=root/'spill',
                timezone='America/New_York',calendar='gregorian')
        if args.s3_profile: cli.env['AWS_PROFILE']=args.s3_profile
        report['identity']=cli.identity(); hooks=report['identity']['test_hooks']; checks=report['checks']
        prefix=args.s3_location.rstrip('/')+'/pilot-'+uuid.uuid4().hex+'/'
        report['provider_namespace']=prefix; number=0
        def setup(name, conflict=False):
            cli.run(f"ATTACH {literal('differential:'+name)} AS d (DATA_PATH {literal(prefix+name+'/')},{s3_options(args)}); USE d;"+
                DDL+f"INSERT INTO x VALUES(1,0,'base',{BASE},TIMESTAMPTZ_NS '2020-01-02 01:00:00.123456789+00',DATE '2020-01-01');"
                f"CALL differential_create_snapshot('d'); USE memory; DETACH d; CALL differential_create_branch('{name}','o',snapshot=>1);"
                f"CALL differential_create_branch('{name}','t',snapshot=>1);"
                f"ATTACH 'differential:{name}' AS o (BRANCH 'o'); UPDATE o.x SET a=1,n={OURS}; CALL differential_create_snapshot('o'); DETACH o;"
                f"ATTACH 'differential:{name}' AS t (BRANCH 't'); UPDATE t.x SET b='incoming',ts=TIMESTAMPTZ_NS '2020-01-02 01:00:00.123456788+00'"+
                (f',n={THEIRS}' if conflict else '')+"; CALL differential_create_snapshot('t'); DETACH t;")
            expected=root/(name+'.duckdb')
            cli.run(f'ATTACH {literal(expected)} AS expected; USE expected;'+DDL+
                f"INSERT INTO x VALUES(1,1,'incoming',{OURS},TIMESTAMPTZ_NS '2020-01-02 01:00:00.123456788+00',DATE '2020-01-01'); CHECKPOINT;")
            return expected
        def merge(name,dest='o',options=''):
            return (f"SELECT * FROM differential_merge_branch('{name}','o','t','{dest}',request_id=>'{name}-pilot',"
                    f"expected_base_snapshot_id=>1,expected_ours_snapshot_id=>2,expected_theirs_snapshot_id=>3{options})")
        def pilot(name,expected=None,*,dest='o',mode='branches',rid='pilot',extra=(),status='passed',code=None):
            nonlocal number
            number+=1; path=root/f'receipt-{number}.json'
            command=[sys.executable,ROOT/'scripts/merge_pilot.py','--repository',cli.repository,'--database',name,
                     '--request-id',name+'-'+rid,'--report',path,'--duckdb',args.duckdb,'--extension',args.extension,
                     '--timezone','America/New_York','--calendar','gregorian',*connection_flags(args),*extra]
            if '--abort-operation' not in extra:
                command+=['--mode',mode,'--ours','o' if mode=='branches' else '2','--theirs','t' if mode=='branches' else '3',
                          '--destination',dest,'--expected',expected]
                if mode=='snapshots': command+=['--base','1']
            elif expected: command+=['--expected',expected]
            rc,_,_,sample=process(command,seconds=180,budget=budget)
            require(path.is_file(),'pilot_receipt_missing'); result=json.loads(path.read_text())
            report.setdefault('cases',[]).append({'number':number,'name':name,'status':result['status'],
                'error_code':result.get('error_code'),'phase':result.get('phase')})
            require(result['status']==status and rc=={'passed':0,'failed':1,'incomplete':2}[status],'pilot_unexpected_status')
            if code: require(result.get('error_code')==code,'pilot_unexpected_error')
            require(result['identity']==report['identity'],'pilot_candidate_mismatch')
            if status=='passed' and 'verification' in result:
                require(result['verification']['complete'] and result['verification']['fresh_repository'],'remote_verification_missing')
                require('remote_head_isolation' in result['checks'],'remote_heads_not_checked')
            text=path.read_text()
            for key in ('AWS_ACCESS_KEY_ID','AWS_SECRET_ACCESS_KEY','AWS_SESSION_TOKEN'):
                value=os.environ.get(key)
                require(not value or value not in text,'credential_in_report')
            require('synthetic-private-marker' not in text,'row_value_in_report')
            return result
        expected=setup('snapshots')
        first=pilot('snapshots',expected,dest='result',mode='snapshots')
        again=pilot('snapshots',expected,dest='result',mode='snapshots')
        require(again['completed_request_replay'] and first['merge']['snapshot_id']==again['merge']['snapshot_id'],'snapshot_replay')
        checks.append('s3_snapshot_new_branch_remote_replay')

        expected=setup('branches')
        result=pilot('branches',expected,dest='result')
        require(result['request']['inputs_guarded'],'unguarded_pilot')
        target=pilot('branches',expected,rid='target')
        require(target['request']['merge_action']=='merged','target_not_merged')
        original=target['merge']['snapshot_id']
        cli.run("ATTACH 'differential:branches' AS o (BRANCH 'o'); UPDATE o.x SET a=9; CALL differential_create_snapshot('o'); DETACH o;"+
                f"CALL differential_expire_snapshots('branches',versions=>[{original}]);")
        repeated=pilot('branches',expected,rid='target')
        require(repeated['merge']['snapshot_id']==original,'replay_verified_latest_head')
        pilot('branches',expected,dest='changed',rid='target',status='failed',code='request_mismatch')
        checks.append('s3_guarded_target_and_original_export_after_head_movement_expiry')

        expected=setup('wrong')
        cli.run(f"ATTACH {literal(expected)} AS e; UPDATE e.x SET b='synthetic-private-marker'; CHECKPOINT;")
        pilot('wrong',expected,status='failed',code='data_mismatch')
        require(cli.scalar("SELECT count(*) FROM differential_branches('wrong') WHERE branch_name='o' AND head_snapshot_id=4")==1,'verification_undid_publication')
        checks.append('s3_wrong_expected_data_fails_without_disclosing_values')

        cli.run(f"ATTACH 'differential:shared' AS d (WRITERS 'shared',DATA_PATH {literal(prefix+'shared/')},{s3_options(args)},PUBLISH_ON_CHECKPOINT false);")
        pilot('shared',expected,status='failed',code='single_writer_required')
        require(cli.scalar("SELECT count(*) FROM differential_operations() WHERE kind IN ('merge','merge_branch') AND database_id=(SELECT database_id FROM differential_databases() WHERE database_name='shared')")==0,'shared_pilot_mutated')
        checks.append('s3_shared_scope_rejected_before_merge')

        if hooks:
            for phase in ('context_frozen','prepared','head_published'):
                name='retry_'+phase; expected=setup(name); cli.run(merge(name),crash=phase)
                if phase!='context_frozen': cli.run(f"CALL differential_expire_snapshots('{name}',versions=>[1,2,3]);")
                result=pilot(name,expected,extra=['--timezone','UTC','--calendar','buddhist'])
                require(result['request']['state']=='complete' and result['request']['attempt_count']==2,'pending_request_not_recovered')
            checks.append('s3_frozen_context_prepared_and_published_retry')

            expected=setup('abandon'); cli.run(merge('abandon','reserved'),crash='prepared')
            op=cli.rows("SELECT operation_id FROM differential_merge_requests('abandon',request_id=>'abandon-pilot')")[0]['operation_id']
            outcome=pilot('abandon',rid='abort',extra=['--abort-operation',op])
            require(outcome['abort'][0]['state']=='aborted','safe_abort_failed')
            require(cli.scalar("SELECT ready FROM differential_merge_branch_preflight('abandon','o','t','reserved')"),'aborted_reservation_retained')
            pilot('abandon',expected,dest='reserved',rid='replacement')
            checks.append('s3_safe_abort_replay_and_released_destination')

            expected=setup('published_abort'); cli.run(merge('published_abort'),crash='head_published')
            op=cli.rows("SELECT operation_id FROM differential_merge_requests('published_abort',request_id=>'published_abort-pilot')")[0]['operation_id']
            pilot('published_abort',rid='abort',extra=['--abort-operation',op],status='incomplete',code='expected_database_required_for_recovered_result')
            result=pilot('published_abort',expected,rid='abort',extra=['--abort-operation',op])
            require(result['abort'][0]['state']=='complete','published_abort_not_recovered')
            checks.append('s3_published_abort_requires_independent_verification')

            expected=setup('uncertain'); cli.run(merge('uncertain'),crash='head_updating')
            op=cli.rows("SELECT operation_id FROM differential_merge_requests('uncertain',request_id=>'uncertain-pilot')")[0]['operation_id']
            pilot('uncertain',rid='abort',extra=['--abort-operation',op],status='failed',code='merge_publication_uncertain')
            pilot('uncertain',expected)
            checks.append('s3_uncertain_abort_is_not_reported_as_success')

            expected=setup('resolved',conflict=True); decisions=root/'decisions.duckdb'
            cli.run(f"ATTACH 'differential:resolved' AS o (BRANCH 'o'); ATTACH {literal(decisions)} AS decisions;"
                    "CREATE TABLE decisions.fixes AS SELECT *,'cells' AS _merge_action,map{'n':'custom'} AS _merge_columns FROM o.x; CHECKPOINT decisions;")
            mapping=",resolution_tables=>map{'main.x':'decisions.main.\"fixes\"'}"
            cli.run(f'ATTACH {literal(decisions)} AS decisions (READ_ONLY);'+merge('resolved',options=mapping),crash='prepared')
            decisions.unlink()
            pilot('resolved',expected,extra=['--resolutions',str(decisions),'--resolution-table','main.x=fixes'])
            checks.append('s3_frozen_nested_resolution_without_source')
        else: report['not_run']=['hook_injected_s3_recovery']
        cli.check_artifacts(report['identity'])
        report.update(pilot_invocations=number,peak_disk_bytes=budget.peak_disk_bytes)


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--report',type=Path,required=True); p.add_argument('--s3-location',required=True)
    p.add_argument('--duckdb',type=Path,default=Path(DUCKDB_CLI)); p.add_argument('--extension',type=Path,default=EXTENSION)
    p.add_argument('--timeout-seconds',type=int,default=600); connection_arguments(p)
    args=p.parse_args(); start=time.monotonic()
    report={'report_version':REPORT_VERSION,'component':'s3_pilot_rehearsal','status':'incomplete','checks':[],
            'scope':'generated fresh single-writer S3 namespaces; not a representative user pilot or AWS qualification'}
    try: run_cases(args,report); report['status']='passed'
    except QualificationError as error: report.update(status='failed',error_code=error.code)
    except Exception: report.update(status='failed',error_code='s3_pilot_test_error')
    report['seconds']=time.monotonic()-start; write_report(args.report,report)
    print(json.dumps({'status':report['status'],'report':str(args.report),'error_code':report.get('error_code')}))
    return 0 if report['status']=='passed' else 1


if __name__=='__main__': sys.exit(main())
