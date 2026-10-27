#!/usr/bin/env python3
"""Disposable end-to-end tests of the operator pilot, including truthful failure receipts."""
import argparse
import json
from pathlib import Path
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'scripts'))
from merge_qualification_common import (CLI, REPORT_VERSION, QualificationError, require, literal,
    process, write_report, sha)
from paths import DUCKDB_CLI, EXTENSION
from evidence import evidence_path

DDL = ('CREATE TABLE x(id INTEGER PRIMARY KEY,a INTEGER DEFAULT 0 CHECK(a>=0),b VARCHAR,'
       'flag BOOLEAN,amount DECIMAL(18,3)); CREATE INDEX by_a ON x(a);'
       'CREATE VIEW visible AS SELECT id,a,b,flag,amount FROM x WHERE a>=0;')


def setup(cli, root, name, *, catalog=False, conflict=False):
    db=literal(name)
    cli.run(f'ATTACH {literal("differential:"+name)} AS d; USE d;'+DDL+
            "INSERT INTO x VALUES(1,0,NULL,false,1.250);"+
            ("CREATE TABLE aux(id INTEGER PRIMARY KEY); INSERT INTO aux VALUES(1);" if catalog else '')+
            f"CALL differential_create_snapshot('d'); USE memory; DETACH d; CALL differential_create_branch({db},'o',snapshot=>1);"
            f"CALL differential_create_branch({db},'t',snapshot=>1);")
    cli.run(f"ATTACH {literal('differential:'+name)} AS d(BRANCH 'o'); USE d; UPDATE x SET a=1;"+
            ('DROP TABLE aux; ALTER TABLE x ADD COLUMN tag VARCHAR; DROP VIEW visible; CREATE VIEW visible AS SELECT id,a,b,flag,amount FROM x WHERE a>=1;' if catalog else '')+
            "CALL differential_create_snapshot('d');")
    cli.run(f"ATTACH {literal('differential:'+name)} AS d(BRANCH 't'); UPDATE d.x SET b='theirs',flag=true,amount=2.375"+
            (',a=2' if conflict else '')+"; CALL differential_create_snapshot('d');")
    expected=root/(name+'.duckdb')
    wanted=DDL.replace('amount DECIMAL(18,3));','amount DECIMAL(18,3),tag VARCHAR);').replace('a>=0;','a>=1;') if catalog else DDL
    cli.run(f'ATTACH {literal(expected)} AS e; USE e;'+wanted+
            f"INSERT INTO x VALUES(1,{42 if conflict else 1},'theirs',true,2.375"+(',NULL' if catalog else '')+'); CHECKPOINT;')
    return expected


def run_cases(args, report):
    with tempfile.TemporaryDirectory(prefix='differential-merge-pilot-tests-') as tmp:
        root=Path(tmp); cli=CLI(args.duckdb,args.extension,root/'repo'); report['identity']=cli.identity()
        checks=report['checks']
        number=0
        def pilot(name, expected=None, *, mode='branches', dest='o', rid='pilot', options=(), status='passed', code=None):
            nonlocal number
            number+=1; path=root/('receipt-'+str(number)+'.json')
            command=[sys.executable,ROOT/'scripts/merge_pilot.py','--repository',cli.repository,'--database',name,
                     '--request-id',name+'-'+rid,'--report',path,'--duckdb',args.duckdb,'--extension',args.extension,*options]
            if not '--abort-operation' in options:
                command += ['--mode',mode,'--ours','o' if mode=='branches' else '2','--theirs','t' if mode=='branches' else '3',
                            '--destination',dest,'--expected',expected]
                if mode=='snapshots': command += ['--base','1']
            rc,out,err,sample=process(command,seconds=120)
            require(path.is_file(), 'pilot_receipt_missing'); result=json.loads(path.read_text())
            report.setdefault('cases',[]).append({'number':number,'name':name,'mode':mode,'destination':dest,
                'expected_status':status,'actual_status':result['status'],'error_code':result.get('error_code'),
                'phase':result.get('phase')})
            require(result['status']==status and rc=={'passed':0,'failed':1,'incomplete':2}[status], 'pilot_unexpected_status')
            if code: require(result['error_code']==code,'pilot_unexpected_error')
            require(result['identity']==report['identity'], 'pilot_candidate_mismatch')
            return result

        expected=setup(cli,root,'snapshot')
        first=pilot('snapshot',expected,mode='snapshots',dest='result')
        second=pilot('snapshot',expected,mode='snapshots',dest='result')
        require(second['completed_request_replay'] and first['merge']['snapshot_id']==second['merge']['snapshot_id'], 'pilot_snapshot_replay')
        cli.run("CALL differential_drop_branch('snapshot','result'); CALL differential_expire_snapshots('snapshot',versions=>[4]); CALL differential_cleanup_old_files(cleanup_all=>true);")
        pilot('snapshot',expected,mode='snapshots',dest='result',status='incomplete',code='retired')
        checks.append('snapshot_new_branch_replay')

        expected=setup(cli,root,'branches')
        first=pilot('branches',expected,dest='result')
        require(first['request']['inputs_guarded'] and first['review']['merge_action']=='created_branch','pilot_unguarded')
        target=pilot('branches',expected,rid='target')
        require(target['request']['merge_action']=='merged','pilot_target_action')
        cli.run("ATTACH 'differential:branches' AS d(BRANCH 'o'); UPDATE d.x SET a=9; CALL differential_create_snapshot('d');")
        pilot('branches',expected,rid='target')  # verifies original snapshot, not the now advanced head
        pilot('branches',expected,dest='changed',rid='target',status='failed',code='request_mismatch')
        checks.append('guarded_new_branch_target_advanced_replay_and_mismatch')

        expected=setup(cli,root,'catalog',catalog=True)
        result=pilot('catalog',expected,options=['--schema-mode','catalog'])
        require(result['verification']['views']==1 and result['verification']['indexes']==1, 'catalog_not_checked')
        # Changed view SQL returning identical rows must still be rejected.
        cli.run(f'ATTACH {literal(expected)} AS e; USE e; DROP VIEW visible; CREATE VIEW visible AS SELECT id,a,b,flag,amount FROM x WHERE a>=0; CHECKPOINT;')
        pilot('catalog',expected,options=['--schema-mode','catalog'],status='failed',code='catalog_mismatch')
        cli.run(f'ATTACH {literal(expected)} AS e; USE e; DROP VIEW visible; SET enable_view_dependencies=true; '
                'CREATE VIEW visible AS SELECT id,a,b,flag,amount FROM x WHERE a>=1; CHECKPOINT;')
        pilot('catalog',expected,options=['--schema-mode','catalog'],status='failed',code='catalog_mismatch')
        checks.append('catalog_removal_column_view_index_default_constraint_verification')

        expected=setup(cli,root,'resolved',conflict=True); fixes=root/'fixes.duckdb'
        cli.run(f'ATTACH {literal(fixes)} AS decisions; CREATE TABLE decisions.fixes(id INTEGER,a INTEGER,b VARCHAR,flag BOOLEAN,amount DECIMAL(18,3),_merge_action VARCHAR,_merge_columns MAP(VARCHAR,VARCHAR));'
                "INSERT INTO decisions.fixes VALUES(1,42,NULL,NULL,NULL,'cells',map{'a':'custom'}); CHECKPOINT;")
        options=['--resolutions',str(fixes),'--resolution-table','main.x=fixes']
        pilot('resolved',expected,options=options); fixes.unlink(); pilot('resolved',expected,options=options)
        checks.append('typed_resolution_and_replay_without_source')

        expected=setup(cli,root,'wrong')
        cli.run(f'ATTACH {literal(expected)} AS e; UPDATE e.x SET b=\'synthetic-private-marker\'; CHECKPOINT;')
        failed=pilot('wrong',expected,status='failed',code='data_mismatch')
        require('synthetic-private-marker' not in json.dumps(failed), 'row_value_in_report')
        require(cli.scalar("SELECT count(*) FROM differential_branches('wrong') WHERE branch_name='o' AND head_snapshot_id=4")==1, 'failed_verification_hid_result')
        checks.append('wrong_expected_data_fails_without_leaking_values_or_undoing_publication')

        expected=setup(cli,root,'extra')
        cli.run(f'ATTACH {literal(expected)} AS e; CREATE MACRO e.unverified() AS 42; CHECKPOINT;')
        pilot('extra',expected,status='failed',code='expected_catalog_unsupported')
        checks.append('unsupported_expected_macro_rejected')

        expected=setup(cli,root,'collation'); expected.unlink()
        cli.run(f'ATTACH {literal(expected)} AS e; USE e;'+DDL.replace('b VARCHAR,','b VARCHAR COLLATE nocase,')+
                "INSERT INTO x VALUES(1,1,'theirs',true,2.375); CHECKPOINT;")
        pilot('collation',expected,status='failed',code='catalog_mismatch')
        checks.append('expected_collation_mismatch_rejected')

        # Recovery scenarios need hooks and are reported explicitly for release artifacts.
        if report['identity']['test_hooks']:
            expected=setup(cli,root,'resolved_pending',conflict=True)
            cli.run(f'ATTACH {literal(fixes)} AS decisions; CREATE TABLE decisions.fixes(id INTEGER,a INTEGER,b VARCHAR,flag BOOLEAN,amount DECIMAL(18,3),_merge_action VARCHAR,_merge_columns MAP(VARCHAR,VARCHAR));'
                    "INSERT INTO decisions.fixes VALUES(1,42,NULL,NULL,NULL,'cells',map{'a':'custom'}); CHECKPOINT;")
            source=literal('decisions.main."fixes"')
            cli.run(f'ATTACH {literal(fixes)} AS decisions (READ_ONLY);'
                    "SELECT * FROM differential_merge_branch('resolved_pending','o','t','o',request_id=>'resolved_pending-pilot',"
                    "expected_base_snapshot_id=>1,expected_ours_snapshot_id=>2,expected_theirs_snapshot_id=>3,"
                    f"resolution_tables=>map{{'main.x':{source}}})",crash='staged')
            fixes.unlink()
            pilot('resolved_pending',expected,options=options)
            checks.append('pending_frozen_resolution_without_source')
            for name, action in [('pending','retry'),('aborted','abort')]:
                expected=setup(cli,root,name)
                sql=f"SELECT * FROM differential_merge_branch('{name}','o','t','o',request_id=>'{name}-pilot',expected_base_snapshot_id=>1,expected_ours_snapshot_id=>2,expected_theirs_snapshot_id=>3)"
                cli.run(sql,crash='staged')
                if action=='retry':
                    result=pilot(name,expected)
                    require(result['previous_request']['state']=='target_locked' and result['request']['attempt_count']==2,'pending_not_retried')
                else:
                    record=cli.rows(f"SELECT * FROM differential_merge_requests('{name}',request_id=>'{name}-pilot')")[0]
                    cli.run(f"CALL differential_drop_branch('{name}','main'); CALL differential_expire_snapshots('{name}',versions=>[1]); CALL differential_cleanup_old_files(cleanup_all=>true);")
                    pilot(name,rid='abort',options=['--abort-operation',record['operation_id']])
                    cli.run(sql,error='merge_aborted')
            checks.append('interrupted_target_retry_and_abort_after_expiry')
        else: report['not_run']=['hook_injected_pending_retry_and_abort']
        cli.check_artifacts(report['identity'])
        report['pilot_invocations']=number


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--report',type=Path,default=evidence_path('merge-pilot.json'))
    p.add_argument('--duckdb',type=Path,default=Path(DUCKDB_CLI)); p.add_argument('--extension',type=Path,default=EXTENSION)
    args=p.parse_args(); start=time.monotonic()
    report={'report_version':REPORT_VERSION,'component':'pilot_rehearsal','status':'incomplete','checks':[],
            'scope':'generated disposable repositories, independently prepared expected native databases; not a representative user pilot'}
    try: run_cases(args,report); report['status']='passed'
    except QualificationError as error: report.update(status='failed',error_code=error.code)
    except Exception: report.update(status='failed',error_code='pilot_test_error')
    report['seconds']=time.monotonic()-start; write_report(args.report,report)
    print(json.dumps({'status':report['status'],'report':str(args.report),'error_code':report.get('error_code')}))
    return 0 if report['status']=='passed' else 1


if __name__=='__main__': sys.exit(main())
