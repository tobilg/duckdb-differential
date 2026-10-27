#!/usr/bin/env python3
"""Bounded, seeded, stateful local merge qualification with an independent Python model.

Each seed carries its result forward through new branches, target merges, failures,
restarts, expiry and GC. Only newly created temporary directories are modified.
"""
import argparse
import copy
from decimal import Decimal
import json
from pathlib import Path
import random
import sqlite3
import sys
import tempfile
import time

from merge_qualification_common import (ROOT, REPORT_VERSION, Budget, CLI, QualificationError,
    require, literal, identifier, verify_database, write_report, connection_arguments)
from merge_native_soak import NATIVE_SCENARIOS, run_native_seed
sys.path.insert(0, str(ROOT/'test/integration'))
from paths import DUCKDB_CLI, EXTENSION

ABSENT = object()
TYPES = {'a':'BIGINT', 'b':'VARCHAR', 'flag':'BOOLEAN', 'amount':'DECIMAL(18,3)', 'tag':'VARCHAR'}
SCENARIOS = ('normal', 'cell_resolution', 'crash_staged', 'crash_transaction', 'crash_published',
             'cancel', 'expire_abort', 'limit_retry', 'catalog_add', 'fast_forward', 'catalog_drop', 'row_resolution')


def cell(value, typ):
    token = 'NULL' if value is None else ('true' if value is True else 'false' if value is False else literal(value))
    return token+'::'+typ


def columns(state): return list(TYPES) if state['tag'] else list(TYPES)[:-1]


def row_sql(key, row, state):
    return '('+','.join([cell(key[0], 'INTEGER'),cell(key[1], 'INTEGER')]+[cell(row[n], TYPES[n]) for n in columns(state)])+')'


def view_sql(state):
    return f'CREATE VIEW visible AS SELECT part,id,a,b,flag,amount FROM x WHERE a>={state["view"]};'


def create_sql(state):
    sql = ('CREATE TABLE x(part INTEGER,id INTEGER,a BIGINT NOT NULL DEFAULT 0 CHECK(a>=0),'
           'b VARCHAR,flag BOOLEAN,amount DECIMAL(18,3)'+(',tag VARCHAR' if state['tag'] else '')+',PRIMARY KEY(part,id));'
           'CREATE INDEX by_a ON x(a,id);')
    if state['rows']: sql += 'INSERT INTO x VALUES '+','.join(row_sql(k,r,state) for k,r in sorted(state['rows'].items()))+';'
    if state['aux']: sql += "CREATE TABLE aux(id INTEGER PRIMARY KEY,v VARCHAR); INSERT INTO aux VALUES(1,'aux'),(2,NULL);"
    return sql+view_sql(state)


def initial(count):
    return {'tag':False, 'aux':False, 'view':0, 'rows':{
        (i%3,i):{'a':i, 'b':None if i%4 == 0 else 'base-'+str(i), 'flag':bool(i%2),
                 'amount':None if i%5 == 0 else Decimal(i)/1000} for i in range(count)}}


def oracle(base, ours, theirs):
    """The specification operates on Python typed values, never SQL change flags/output."""
    def choose(b,o,t):
        if o == t: return o
        if o == b: return t
        if t == b: return o
        raise QualificationError('oracle_catalog_conflict')
    result = {n:choose(base[n],ours[n],theirs[n]) for n in ('tag','aux','view')}; result['rows'] = {}
    conflicts = {}
    for key in sorted(base['rows'].keys() | ours['rows'].keys() | theirs['rows'].keys()):
        b,o,t = [s['rows'].get(key, ABSENT) for s in (base,ours,theirs)]
        # Missing appended columns are NULL; removed columns are projected out.
        b,o,t = [r if r is ABSENT else {n:r.get(n) for n in columns(result)} for r in (b,o,t)]
        if b is ABSENT:
            if o is ABSENT: result['rows'][key] = t
            elif t is ABSENT or o == t: result['rows'][key] = o
            else: conflicts[key] = ('insert_insert', [])
        elif o is ABSENT or t is ABSENT:
            survivor = t if o is ABSENT else o
            if survivor is not ABSENT and survivor != b: conflicts[key] = ('delete_update', [])
        else:
            row = {}; changed = []
            for name in columns(result):
                if o[name] == t[name]: row[name] = o[name]
                elif o[name] == b[name]: row[name] = t[name]
                elif t[name] == b[name]: row[name] = o[name]
                else: changed.append(name)
            if changed: conflicts[key] = ('cell', changed)
            else: result['rows'][key] = row
    return result, conflicts


def mutate(base, rng, step, scenario):
    ours, theirs = copy.deepcopy(base), copy.deepcopy(base)
    keys = sorted(base['rows']); anchor = (0,0)
    selected = rng.sample([k for k in keys if k != anchor], min(4,len(keys)-1))
    if scenario != 'fast_forward':
        for key in [anchor,*selected[:2]]: ours['rows'][key]['a'] += rng.randint(1,10)
        if len(selected)>2: del ours['rows'][selected[2]]
    for key in [anchor,*selected[:2]]:
        theirs['rows'][key]['b'] = rng.choice([None, '', "quo'te", '東京', 'line\nbreak'])
        theirs['rows'][key]['flag'] = rng.choice([None, True, False])
        theirs['rows'][key]['amount'] = rng.choice([None,Decimal('0.001'),Decimal('-23.750'),Decimal('123456789.123')])
    dropping = scenario == 'catalog_drop' and base['tag']
    if len(selected)>3 and not dropping:
        key = selected[3]; theirs['rows'][(key[0],100000+step)] = theirs['rows'].pop(key)  # PK change
    new = {'a':step+1,'b':'insert','flag':None,'amount':Decimal('1.250')}
    if base['tag']: new['tag'] = None
    (ours if dropping else theirs)['rows'][(7,200000+step)] = dict(new)
    if scenario == 'normal': ours['rows'][(7,200000+step)] = dict(new)  # identical insert
    if scenario == 'cell_resolution': theirs['rows'][anchor]['a'] = base['rows'][anchor]['a']+100
    if scenario == 'row_resolution': del theirs['rows'][anchor]
    if scenario in ('catalog_add','catalog_drop'):
        ours['tag'] = not base['tag']; ours['aux'] = not base['aux']; ours['view'] = step+1
        for row in ours['rows'].values():
            if ours['tag']: row['tag'] = None
            else: row.pop('tag',None)
    return ours, theirs


def apply(cli, branch, before, after):
    sql = f"ATTACH 'differential:soak' AS d(BRANCH {literal(branch)}); USE d;"
    if before['view'] != after['view']: sql += 'DROP VIEW visible;'
    if before['tag'] != after['tag']:
        # Stock DuckDB requires removing the dependent ART index around DROP COLUMN.
        sql += 'DROP INDEX by_a; ALTER TABLE x '+('ADD COLUMN tag VARCHAR;' if after['tag'] else 'DROP COLUMN tag;')
        sql += 'CREATE INDEX by_a ON x(a,id);'
    if before['aux'] != after['aux']:
        sql += ("CREATE TABLE aux(id INTEGER PRIMARY KEY,v VARCHAR); INSERT INTO aux VALUES(1,'aux'),(2,NULL);" if after['aux'] else 'DROP TABLE aux;')
    for key in sorted(before['rows'].keys() - after['rows'].keys()):
        sql += f'DELETE FROM x WHERE part={key[0]} AND id={key[1]};'
    for key,row in sorted(after['rows'].items()):
        if key not in before['rows']: sql += 'INSERT INTO x VALUES '+row_sql(key,row,after)+';'
        else:
            changed = [n for n in columns(after) if before['rows'][key].get(n) != row[n]]
            if changed: sql += 'UPDATE x SET '+','.join(identifier(n)+'='+cell(row[n],TYPES[n]) for n in changed)+f' WHERE part={key[0]} AND id={key[1]};'
    if before['view'] != after['view']: sql += view_sql(after)
    return cli.rows(sql+"SELECT snapshot_id FROM differential_create_snapshot('d');")[-1]['snapshot_id']


def expected_file(cli, root, state):
    path = root/'expected.duckdb'; path.unlink(missing_ok=True)
    cli.run(f'ATTACH {literal(path)} AS expected; USE expected;'+create_sql(state)+'CHECKPOINT;')
    return path


def request(cli, rid): return cli.rows(f"SELECT * FROM differential_merge_requests('soak',request_id=>{literal(rid)})")[0]


def branches(cli):
    return cli.rows("SELECT branch_id,branch_name,root,head_snapshot_id FROM differential_branches('soak') WHERE state='active' ORDER BY branch_name")


def quiescence(cli, root, live):
    cli.run("CALL differential_cleanup_old_files(cleanup_all=>true);")
    retained = cli.rows('SELECT reason,identity,root FROM differential_retention_roots()')
    require(not any(r['reason']=='native_file' and r['identity'].startswith('merge_stage/') for r in retained), 'leaked_staging')
    require(cli.scalar('SELECT count(*) FROM differential_merge_status()') == 0, 'leaked_merge_status')
    with sqlite3.connect('file:'+str(root/'repo/engine.sqlite')+'?mode=ro', uri=True) as db:
        require(db.execute('PRAGMA integrity_check').fetchone()[0] == 'ok', 'metadata_integrity')
        for table in ('pins','compound_intents'):
            require(db.execute('SELECT count(*) FROM '+table).fetchone()[0] == 0, 'leaked_'+table)
        require(db.execute("SELECT count(*) FROM files WHERE name LIKE 'merge_stage/%'").fetchone()[0] == 0, 'leaked_staging')
        live_roots = {r['root'] for r in branches(cli)}
        stored_roots = {r[0] for r in db.execute('SELECT DISTINCT root FROM root_pages')}
        require(stored_roots <= live_roots, 'unretained_root_pages')
        counts = {name:db.execute('SELECT count(*) FROM '+name).fetchone()[0] for name in ('snapshots','operations','root_pages','pins','compound_intents')}
        require(db.execute('SELECT count(*) FROM snapshots WHERE retired=0').fetchone()[0] == 2, 'unexpected_retained_snapshot')
    require({b['branch_name'] for b in branches(cli)} == {'main',live}, 'leaked_branch')
    metrics = {r['metric']:r['value'] for r in cli.rows('SELECT * FROM differential_storage_stats()')
               if r['metric'] in ('local_payload_bytes','local_derived_staging_bytes','local_journal_bytes','local_authority_main_bytes')}
    return {'counts':counts, 'storage':metrics, 'retained_roots':len(live_roots)}


def run_seed(cli, root, seed, steps, count, trace):
    rng = random.Random(seed); state = initial(count); current = 'work'
    cli.run("ATTACH 'differential:soak' AS d; USE d;"+create_sql(state)+
            "CALL differential_create_snapshot('d'); USE memory; DETACH d; CALL differential_create_branch('soak','work',snapshot=>1);")
    head = 1
    for step in range(steps):
        scenario = SCENARIOS[step % len(SCENARIOS)]; row = {'step':step,'scenario':scenario,'status':'running'}; trace.append(row)
        source = 'source'+str(step); dest = 'result'+str(step) if scenario == 'normal' else current
        base = copy.deepcopy(state); ours,theirs = mutate(base,rng,step,scenario)
        require(len(ours['rows']) <= 2048 and len(theirs['rows']) <= 2048, 'model_row_budget')
        cli.run(f"CALL differential_create_branch('soak',{literal(source)},snapshot=>{head});")
        oid = head if scenario == 'fast_forward' else apply(cli,current,base,ours)
        tid = apply(cli,source,base,theirs); ids = (head,oid,tid)
        merged,conflicts = oracle(base,ours,theirs)
        old_branches = branches(cli)
        inputs = ','.join(map(literal,('soak',current,source,dest)))
        opts = ",schema_mode=>'catalog'"+''.join(f',expected_{n}_snapshot_id=>{i}' for n,i in zip(('base','ours','theirs'),ids))
        rid = 'step-'+str(step)
        def sql(request_id=rid, extra=''):
            return f'SELECT * FROM differential_merge_branch({inputs},request_id=>{literal(request_id)}{opts}{extra})'
        preview = cli.rows(f"SELECT * FROM differential_merge_branch_preflight({inputs},schema_mode=>'catalog',validate=>true)")[0]
        require(tuple(preview[n+'_snapshot_id'] for n in ('base','ours','theirs')) == ids, 'wrong_merge_base')
        require(branches(cli) == old_branches, 'preview_changed_branches')
        require(preview['data_conflicts'] == len(conflicts), 'oracle_conflict_count')
        require(preview['schema_conflicts'] == 0, 'unexpected_schema_conflict')
        require(preview['ready'] == (not conflicts), 'preflight_readiness')
        resolution_attach = ''
        if conflicts:
            cli.run(sql('unresolved-'+str(step)), error='merge_conflict')
            require(branches(cli) == old_branches, 'conflict_published')
            failed = request(cli,'unresolved-'+str(step))
            cli.run(f"CALL differential_abort_merge('soak',{literal(failed['operation_id'])},request_id=>{literal('abort-conflict-'+str(step))});")
            require(list(conflicts) == [(0,0)], 'unexpected_generated_conflict')
            kind,names = conflicts[(0,0)]; selected = dict(ours['rows'][(0,0)])
            if kind == 'cell':
                # Preserve independently changed source cells, choosing a typed custom value only for a.
                for name in columns(merged):
                    if name not in names:
                        b,o,t = (s['rows'][(0,0)].get(name) for s in (base,ours,theirs))
                        selected[name] = t if o == b else o
                selected['a'] = 700000+step
            merged['rows'][(0,0)] = selected
            fix = root/'resolutions.duckdb'; fix.unlink(missing_ok=True)
            definition = ','.join(['part INTEGER','id INTEGER']+[identifier(n)+' '+TYPES[n] for n in columns(merged)])
            action = "'cells',map{'a':'custom'}" if kind == 'cell' else "'ours',map{}::MAP(VARCHAR,VARCHAR)"
            cli.run(f'ATTACH {literal(fix)} AS decisions; CREATE TABLE decisions.fixes({definition},_merge_action VARCHAR,_merge_columns MAP(VARCHAR,VARCHAR));'
                    +'INSERT INTO decisions.fixes VALUES '+row_sql((0,0),selected,merged)[:-1]+','+action+'); CHECKPOINT decisions;')
            resolution_attach = f'ATTACH {literal(fix)} AS decisions (READ_ONLY);'
            opts += ",resolution_tables=>map{'main.x':'decisions.main.fixes'}"
        if scenario.startswith('crash_') or scenario == 'expire_abort':
            point = {'crash_staged':'staged','crash_transaction':'publication_transaction','crash_published':'published','expire_abort':'staged'}[scenario]
            cli.run(sql(), crash=point)
            record = request(cli,rid)
            require((record['state']=='complete') == (point=='published'), 'crash_publication_boundary')
            if point != 'published': require(branches(cli) == old_branches, 'crash_published_early')
            if scenario == 'expire_abort':
                cli.run(f"CALL differential_expire_snapshots('soak',versions=>[{head}]); CALL differential_cleanup_old_files(cleanup_all=>true);")
                abort = f"SELECT * FROM differential_abort_merge('soak',{literal(record['operation_id'])},request_id=>{literal('abort-'+rid)})"
                answer = cli.rows(abort); require(cli.rows(abort)==answer, 'abort_replay')
                cli.run(sql(), error='merge_aborted')
                require(branches(cli)==old_branches, 'abort_changed_target')
                state = ours; head = oid; row['action'] = 'aborted'
        elif scenario == 'cancel':
            cli.run(sql(), error='cancelled', cancel_gate=root/'cancel', cancel_phase='staged')
            require(branches(cli)==old_branches, 'cancel_published')
            require(request(cli,rid)['last_attempt_error_code']=='cancelled', 'cancel_diagnostics')
        elif scenario == 'limit_retry':
            cli.run(sql(extra=',max_stage_write_bytes=>1'), error='merge_limit')
            require(branches(cli)==old_branches, 'limit_published')
        if scenario != 'expire_abort':
            result = cli.rows(resolution_attach+sql())[0]; record = request(cli,rid)
            if resolution_attach: fix.unlink()  # Frozen decisions must suffice on completed replay.
            again = cli.rows(sql())[0]
            require(again['snapshot_id']==result['snapshot_id'] and request(cli,rid)==record, 'replay_changed')
            head = result['snapshot_id']; state = merged; row['action'] = result['merge_action']
            row['timings_ms'] = {n:result[n] for n in ('comparison_ms','application_ms','publication_ms')}
            if scenario == 'fast_forward': require(row['action']=='fast_forward' and head==tid, 'fast_forward_result')
            else:
                parents = cli.rows(f"SELECT parents FROM differential_merge_ancestry('soak',{head},{tid}) WHERE snapshot_id={head}")[0]['parents']
                require(parents==[oid,tid], 'merge_parents')
            if dest == current:
                previous_target = next(b for b in old_branches if b['branch_name']==current)
                new_target = next(b for b in branches(cli) if b['branch_name']==current)
                require(new_target['branch_id']==previous_target['branch_id'], 'target_identity_changed')
            else:
                cli.run(f"CALL differential_drop_branch('soak',{literal(current)});"); current = dest
            if scenario == 'fast_forward':
                repeat = cli.rows(f"SELECT * FROM differential_merge_branch('soak',{literal(current)},{literal(source)},{literal(current)},request_id=>{literal('up-to-date-'+rid)},schema_mode=>'catalog')")[0]
                require(repeat['merge_action']=='already_up_to_date' and repeat['snapshot_id']==head, 'remerge_result')
        source_before = next(b for b in old_branches if b['branch_name']==source)
        require(source_before in branches(cli), 'source_changed')
        verification = verify_database(cli,'soak',head,expected_file(cli,root,state))
        row.update(snapshot_id=head, rows=len(state['rows']), verification=verification)
        cli.run(f"CALL differential_drop_branch('soak',{literal(source)});")
        expired = [r['snapshot_id'] for r in cli.rows("SELECT snapshot_id FROM differential_snapshots('soak') WHERE NOT retired") if r['snapshot_id'] not in (1,head)]
        if expired: cli.run("CALL differential_expire_snapshots('soak',versions=>["+','.join(map(str,expired))+']);')
        row['resources'] = quiescence(cli,root,current); row['status']='passed'
    return {
        'final_snapshot':head, 'final_rows':len(state['rows']), 'rounds':len(trace)}


def parser():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--report', type=Path, required=True)
    p.add_argument('--seeds', type=int, nargs='+', default=[7,29,97])
    p.add_argument('--steps', type=int, default=40)
    p.add_argument('--rows', type=int, default=32)
    p.add_argument('--timeout-seconds', type=int, default=900)
    p.add_argument('--disk-limit-mib', type=int, default=1024)
    p.add_argument('--free-space-mib', type=int, default=1024)
    p.add_argument('--duckdb', type=Path, default=Path(DUCKDB_CLI))
    p.add_argument('--extension', type=Path, default=EXTENSION)
    p.add_argument('--profile', choices=('scalars','native'), default='scalars')
    p.add_argument('--s3-location', help='native profile: create a fresh UUID namespace below this S3 prefix')
    connection_arguments(p)
    return p


def execute(args):
    native=args.profile=='native'
    report = {'report_version':REPORT_VERSION,'component':'native_soak' if native else 'soak','status':'incomplete',
              'configuration':{'seeds':args.seeds,'steps':args.steps,'rows':args.rows,'seconds_limit':args.timeout_seconds,
                               'disk_limit_mib':args.disk_limit_mib,'free_space_mib':args.free_space_mib,
                               'memory_limit':'128MB','temp_limit':'512MB','threads':2,'profile':args.profile,
                               'storage':'s3' if args.s3_location else 'local'}, 'runs':[]}
    start=time.monotonic(); samples=[]; cli=None
    try:
        require(12 <= args.steps <= 200 and 8 <= args.rows <= 1024 and 1 <= len(args.seeds) <= 10, 'soak_arguments')
        require(len(set(args.seeds)) == len(args.seeds), 'duplicate_seed')
        require(not args.s3_location or (native and args.s3_location.startswith('s3://')), 'native_s3_profile_required')
        if native:
            require(args.timezone in (None,'America/New_York') and args.calendar in (None,'gregorian'),'native_soak_context')
        with tempfile.TemporaryDirectory(prefix='differential-merge-soak-') as tmp:
            budget=Budget(tmp, args.timeout_seconds, args.disk_limit_mib*1024**2, args.free_space_mib*1024**2)
            for seed in args.seeds:
                root=Path(tmp)/str(seed); root.mkdir(); trace=[]
                report['runs'].append({'seed':seed,'trace':trace})
                cli=CLI(args.duckdb,args.extension,root/'repo',budget=budget,spill=root/'spill',
                        timezone='America/New_York' if native else args.timezone,
                        calendar='gregorian' if native else args.calendar)
                if args.s3_profile: cli.env['AWS_PROFILE']=args.s3_profile
                identity=cli.identity()
                if 'identity' in report: require(identity==report['identity'], 'candidate_changed')
                else: report['identity']=identity
                # Fault rounds are mandatory: a hook-free artifact cannot produce a passing full soak receipt.
                require(identity['test_hooks'], 'test_hooks_required')
                try:
                    report['runs'][-1]['result']=(run_native_seed(cli,root,seed,args.steps,args.rows,trace,args) if native
                                                else run_seed(cli,root,seed,args.steps,args.rows,trace))
                finally: samples.extend(cli.samples)
                cli.check_artifacts(identity)
            report['peak_disk_bytes']=budget.peak_disk_bytes
            report['status']='passed'; report['checks']=list(NATIVE_SCENARIOS if native else SCENARIOS)+['typed_model','catalog','ancestry','replay','retention','gc']
            if native: report['checks']+=['native_equality','whole_nested_values','frozen_context','tracked_views']
            if args.s3_location: report['checks']+=['remote_export','reserved_destination','remote_head_isolation']
    except QualificationError as error: report.update(status='failed',error_code=error.code)
    except Exception: report.update(status='failed',error_code='soak_error')
    report['seconds']=time.monotonic()-start; report['processes']=len(samples)
    report['peak_child_rss_bytes']=max((s['peak_rss_bytes'] for s in samples),default=0)
    report['child_input_blocks']=sum(s['input_blocks'] for s in samples)
    report['child_output_blocks']=sum(s['output_blocks'] for s in samples)
    write_report(args.report,report); return report


def main():
    args=parser().parse_args(); report=execute(args)
    print(json.dumps({'status':report['status'],'report':str(args.report),'error_code':report.get('error_code')}))
    return 0 if report['status']=='passed' else 1


if __name__=='__main__': sys.exit(main())
