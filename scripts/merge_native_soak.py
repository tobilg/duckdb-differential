"""Finite native-value model for repeated local/S3 merges, independent of changesets.

DuckDB supplies only typed equality of a bounded input alphabet. Python applies the
specified row/cell rules and constructs expected tables without reading merge output.
"""
import copy
import hashlib
import json
from pathlib import Path
import random
import sqlite3
import sys
import uuid

from merge_qualification_common import (ROOT, QualificationError, require, literal, identifier,
    verify_database, verify_remote, remote_heads, s3_options, single_writer_database)
sys.path.insert(0,str(ROOT/'test/integration'))
from merge_types import CASES, typed

NATIVE_SCENARIOS = ('native_cells','native_agreement','nested_conflict','context_retry',
                    'tracked_view_retry','native_resolution_retry','row_existence','publication_retry',
                    'publication_abort','published_retry','native_limits','evicted_inputs')
ABSENT = object()
TYPES = [c[0] for c in CASES]+['INTEGER','VARCHAR']
NAMES = [f'c{i}' for i in range(len(CASES))]+['revision','label']
PALETTES = [list(c[1:4])+['NULL'] for c in CASES]+[['0','1','2','3'],["'base'","'ours'","'theirs'",'NULL']]
PALETTES[3] += ["'NaN'",'0.0','-0.0']
PALETTES[4] += ["'NaN'",'0.0','-0.0']
PALETTES[12] += ["'2020-01-01 10:34:56.123456+00'"]
PALETTES[14] += ["'32 days 3 microseconds'"]


def key_sql(key): return literal(str(uuid.UUID(int=key)))+'::UUID'


def row_sql(key, row):
    values = [key_sql(key)]+[typed(PALETTES[i][v],TYPES[i]) for i,v in enumerate(row)]
    return '('+','.join(values)+",TIMESTAMPTZ '2020-01-02 01:00:00+00',DATE '2020-01-01')"


def create_sql(state):
    sql = ('CREATE TABLE x(id UUID PRIMARY KEY,'+
           ','.join(identifier(n)+' '+typ for n,typ in zip(NAMES,TYPES))+
           ",ts TIMESTAMPTZ,day DATE,CHECK(CAST(ts AS DATE)=day));")
    if state: sql += 'INSERT INTO x VALUES '+','.join(row_sql(k,row) for k,row in sorted(state.items()))+';'
    return sql+('SET enable_view_dependencies=true; CREATE VIEW leaf AS SELECT * FROM x;'
                'CREATE VIEW visible AS SELECT id,c3,c17,CAST(ts AS DATE) AS day FROM leaf;')


class Model:
    def __init__(self, cli):
        self.equal = []
        # Native null-safe equality is the oracle, including NaN, signed zero,
        # zone offsets, interval normalization and complete nested values.
        for typ,values in zip(TYPES,PALETTES):
            source = '(VALUES '+','.join(f'({i},{typed(v,typ)})' for i,v in enumerate(values))+')'
            rows=cli.rows(f'SELECT a.i ai,b.i bi,a.v IS NOT DISTINCT FROM b.v eq FROM {source} a(i,v),{source} b(i,v)')
            self.equal.append({(r['ai'],r['bi']):r['eq'] for r in rows})
        self.fingerprint=hashlib.sha256(json.dumps([sorted(m.items()) for m in self.equal]).encode()).hexdigest()

    def row_equal(self,a,b): return all(self.equal[i][x,y] for i,(x,y) in enumerate(zip(a,b)))

    def alternatives(self, col, *exclude):
        return [i for i in range(len(PALETTES[col])) if all(not self.equal[col][i,j] for j in exclude)]

    def merge(self, base, ours, theirs):
        result,conflicts = {},{}
        for key in sorted(base.keys() | ours.keys() | theirs.keys()):
            b,o,t=(state.get(key,ABSENT) for state in (base,ours,theirs))
            if b is ABSENT:
                if o is ABSENT: result[key]=list(t)
                elif t is ABSENT or self.row_equal(o,t): result[key]=list(o)
                else: conflicts[key]=('insert_insert',[])
            elif o is ABSENT or t is ABSENT:
                survivor=t if o is ABSENT else o
                if survivor is not ABSENT and not self.row_equal(survivor,b): conflicts[key]=('delete_update',[])
            else:
                row=[]; names=[]
                for i,(bv,ov,tv) in enumerate(zip(b,o,t)):
                    eq=self.equal[i]
                    if eq[ov,tv] or eq[tv,bv]: row.append(ov)
                    elif eq[ov,bv]: row.append(tv)
                    else: row.append(ov); names.append(i)
                result[key]=row
                if names: conflicts[key]=('cell',names)
        return result,conflicts


def mutate(model, base, rng, step, scenario):
    ours,theirs=copy.deepcopy(base),copy.deepcopy(base)
    selected=[1]+rng.sample([k for k in base if k!=1],min(2,len(base)-1))
    for key in selected:
        for col in range(len(TYPES)):
            owner=ours if col%2==0 else theirs
            owner[key][col]=rng.choice(model.alternatives(col,base[key][col]))
    # Always carry one-sided inserts through repeated history, including NULLs.
    theirs[10000+step]=[rng.randrange(len(p)) for p in PALETTES]
    if scenario=='native_agreement':
        for col in (3,4): ours[1][col]=4; theirs[1][col]=4 # native NaN agreement
        if len(selected)>1:
            ours[selected[1]][4]=5; theirs[selected[1]][4]=6 # +0 versus -0
        ours[1][12]=1; theirs[1][12]=4 # equivalent timestamp offsets
        ours[1][14]=1; theirs[1][14]=4 # native interval equality
    if scenario in ('nested_conflict','native_resolution_retry'):
        for col in ([17] if scenario=='nested_conflict' else range(len(CASES))):
            o=model.alternatives(col,base[1][col])[0]
            t=model.alternatives(col,base[1][col],o)[0]
            ours[1][col],theirs[1][col]=o,t
    if scenario=='row_existence':
        del ours[1] # present update on theirs versus deletion
        key=20000+step
        ours[key]=[3]*len(CASES)+[0,3] # present NULL cells, not an absent row
        theirs[key]=[0]*len(TYPES)
    return ours,theirs


def apply(cli, branch, before, after):
    sql=f"ATTACH 'differential:native_soak' AS d (BRANCH {literal(branch)});"
    for key in sorted(before.keys()-after.keys()): sql+=f'DELETE FROM d.x WHERE id={key_sql(key)};'
    for key,row in sorted(after.items()):
        if key not in before: sql+='INSERT INTO d.x VALUES '+row_sql(key,row)+';'
        else:
            changed=[i for i in range(len(TYPES)) if row[i]!=before[key][i]]
            if changed: sql+='UPDATE d.x SET '+','.join(identifier(NAMES[i])+'='+typed(PALETTES[i][row[i]],TYPES[i]) for i in changed)+f' WHERE id={key_sql(key)};'
    return cli.rows(sql+"SELECT snapshot_id FROM differential_create_snapshot('d');")[-1]['snapshot_id']


def branches(cli):
    return cli.rows("SELECT branch_id,branch_name,root,head_snapshot_id FROM differential_branches('native_soak') WHERE state='active' ORDER BY branch_name")


def request(cli,rid): return cli.rows(f"SELECT * FROM differential_merge_requests('native_soak',request_id=>{literal(rid)})")[0]


def counts(cli):
    with sqlite3.connect('file:'+str(cli.repository/'engine.sqlite')+'?mode=ro',uri=True) as db:
        return {t:db.execute('SELECT count(*) FROM '+t).fetchone()[0] for t in
                ('operations','snapshots','history','branches','export_roots','compound_intents')}


def clean(cli, head, current, remote):
    cli.run(f"CALL differential_drop_branch('native_soak','source');")
    old=[r['snapshot_id'] for r in cli.rows("SELECT snapshot_id FROM differential_snapshots('native_soak') WHERE NOT retired") if r['snapshot_id'] not in (1,head)]
    if old:
        versions='['+','.join(map(str,old))+']'
        if remote: cli.run(f"CALL differential_evict_snapshots('native_soak',versions=>{versions});")
        cli.run(f"CALL differential_expire_snapshots('native_soak',versions=>{versions});")
    cli.run('CALL differential_cleanup_old_files(cleanup_all=>true);')
    with sqlite3.connect('file:'+str(cli.repository/'engine.sqlite')+'?mode=ro',uri=True) as db:
        require(db.execute('PRAGMA integrity_check').fetchone()[0]=='ok','metadata_integrity')
        for table in ('pins','compound_intents'):
            require(db.execute('SELECT count(*) FROM '+table).fetchone()[0]==0,'leaked_'+table)
        require(db.execute("SELECT count(*) FROM files WHERE name LIKE 'merge_stage/%'").fetchone()[0]==0,'leaked_staging')
        require(db.execute("SELECT count(*) FROM export_roots WHERE state='pending'").fetchone()[0]==0,'leaked_pending_export')
        require(db.execute("SELECT count(*) FROM operations WHERE kind='merge_destination' AND state='pending'").fetchone()[0]==0,'leaked_reservation')
        require(db.execute('SELECT count(*) FROM snapshots WHERE retired=0').fetchone()[0]==2,'unexpected_retained_snapshot')
        retained={r[0] for r in db.execute('SELECT DISTINCT root FROM root_pages')}
    live=branches(cli)
    require({b['branch_name'] for b in live}=={'main',current},'leaked_branch')
    require(retained<={b['root'] for b in live},'unretained_root_pages')
    return counts(cli)


def run_native_seed(cli, root, seed, steps, count, trace, args):
    rng=random.Random(seed); model=Model(cli); remote=bool(args.s3_location)
    state={i:[0]*len(TYPES) for i in range(1,count+1)}; current='work'; head=1
    location=args.s3_location.rstrip('/')+'/qualification-'+uuid.uuid4().hex+'/' if remote else ''
    options=f'(DATA_PATH {literal(location)},{s3_options(args)})' if remote else ''
    cli.run(f"ATTACH 'differential:native_soak' AS d {options}; USE d;"+create_sql(state)+
            "CALL differential_create_snapshot('d'); USE memory; DETACH d; CALL differential_create_branch('native_soak','work',snapshot=>1);")
    info=single_writer_database(cli,'native_soak')
    for step in range(steps):
        scenario=NATIVE_SCENARIOS[step%len(NATIVE_SCENARIOS)]
        row={'step':step,'scenario':scenario,'status':'running'}; trace.append(row)
        # A row-existence round may delete the anchor: restore it with a real
        # published edit before the next fork; model and base advance together.
        if 1 not in state:
            restored=copy.deepcopy(state); restored[1]=[0]*len(TYPES)
            head=apply(cli,current,state,restored); state=restored
        base=copy.deepcopy(state); ours,theirs=mutate(model,base,rng,step,scenario)
        require(max(len(ours),len(theirs))<=2048,'model_row_budget')
        cli.run(f"CALL differential_create_branch('native_soak','source',snapshot=>{head});")
        oid=apply(cli,current,base,ours); tid=apply(cli,'source',base,theirs); ids=(head,oid,tid)
        dest='result'+str(step) if scenario in ('native_cells','publication_retry','publication_abort') else current
        mode='strict' if step%2==0 else 'additive'
        options=f',schema_mode=>{literal(mode)}'+''.join(f',expected_{n}_snapshot_id=>{v}' for n,v in zip(('base','ours','theirs'),ids))
        args_sql=','.join(map(literal,('native_soak',current,'source',dest)))
        rid='native-'+str(step)
        def sql(extra=''): return f'SELECT * FROM differential_merge_branch({args_sql},request_id=>{literal(rid)}{options}{extra})'
        def preview(): return cli.rows(f'SELECT * FROM differential_merge_branch_preflight({args_sql},schema_mode=>{literal(mode)})')[0]
        expected,conflicts=model.merge(base,ours,theirs)
        old=branches(cli); old_heads=remote_heads(cli,info,s3_options(args,named=True)) if remote else []
        if remote and scenario=='evicted_inputs':
            cli.run("CALL differential_evict_snapshots('native_soak',versions=>["+','.join(map(str,ids))+']); CALL differential_cleanup_old_files(cleanup_all=>true);')
        before=counts(cli); review=preview()
        row['preflight']={name:review[name] for name in ('ready','data_conflicts','schema_conflicts','validation_state')}
        row['preflight']['issue_codes']=[issue['code'] for issue in review['issues']]
        row['expected_conflicts']=len(conflicts)
        require(counts(cli)==before and branches(cli)==old,'preview_changed_inputs')
        require(tuple(review[n+'_snapshot_id'] for n in ('base','ours','theirs'))==ids,'wrong_merge_base')
        require(review['ready']==(not conflicts) and review['data_conflicts']==len(conflicts),'native_oracle_conflicts')
        attach=''; choices=None
        if conflicts:
            cli.run(sql(),error='merge_conflict')
            failed=request(cli,rid)
            cli.run(f"CALL differential_abort_merge('native_soak',{literal(failed['operation_id'])},request_id=>{literal('abort-'+rid)});")
            rid+='-resolved'; choices=root/'decisions.duckdb'; choices.unlink(missing_ok=True)
            setup=f"ATTACH 'differential:native_soak' AS o (BRANCH {literal(current)},READ_ONLY); ATTACH {literal(choices)} AS choices;"
            setup+="CREATE TABLE choices.fixes AS SELECT *,'custom'::VARCHAR AS _merge_action,map{}::MAP(VARCHAR,VARCHAR) AS _merge_columns FROM o.x LIMIT 0;"
            for key,(kind,cols) in conflicts.items():
                if kind=='cell':
                    selected=expected[key]
                    for col in cols: selected[col]=rng.randrange(len(PALETTES[col]))
                    action="'cells',map{"+','.join(literal(NAMES[i])+":'custom'" for i in cols)+'}'
                else:
                    selected=ours.get(key,[3]*len(CASES)+[0,3])
                    if key in ours: expected[key]=list(selected)
                    else: expected.pop(key,None)
                    action=literal('ours' if key in ours else 'delete')+',map{}::MAP(VARCHAR,VARCHAR)'
                setup+='INSERT INTO choices.fixes VALUES '+row_sql(key,selected)[:-1]+','+action+');'
            cli.run(setup+'CHECKPOINT choices;')
            attach=f'ATTACH {literal(choices)} AS choices (READ_ONLY);'
            options+=",resolution_tables=>map{'main.x':'choices.main.fixes'}"
        phases={'context_retry':'context_frozen','tracked_view_retry':'views_dropped',
                'native_resolution_retry':'resolutions_frozen','publication_retry':'prepared' if remote else 'publication_transaction',
                'publication_abort':'prepared' if remote else 'staged','published_retry':'head_published' if remote else 'published'}
        if scenario in phases:
            cli.run(attach+sql(),crash=phases[scenario]); record=request(cli,rid)
            if remote and scenario in ('publication_retry','publication_abort'):
                frozen=counts(cli); blocked=preview()
                require(not blocked['ready'] and blocked['target_state']=='reserved','reservation_preflight')
                require(any(i['code']=='merge_pending_operation' and i['operation_id']==record['operation_id'] for i in blocked['issues']),'reservation_owner')
                require(counts(cli)==frozen,'reservation_preview_changed_metadata')
                row['reservation_checked']=True
            if scenario=='publication_abort':
                abort=f"SELECT * FROM differential_abort_merge('native_soak',{literal(record['operation_id'])},request_id=>{literal('abandon-'+rid)})"
                result=cli.rows(abort); require(result[0]['state']=='aborted' and cli.rows(abort)==result,'abort_replay')
                cli.run(sql(),error='merge_aborted'); require(branches(cli)==old,'abort_changed_inputs')
                require(preview()['ready'],'reservation_not_released')
                rid+='-replacement'
            if scenario=='native_resolution_retry': choices.unlink(); attach=''
        if scenario=='native_limits':
            cli.run(sql(',max_stage_write_bytes=>1'),error='merge_limit')
            require(branches(cli)==old,'limit_published')
        retry_context="SET TimeZone='UTC'; SET Calendar='buddhist';" if scenario in ('context_retry','native_resolution_retry') else ''
        result=cli.rows(retry_context+attach+sql())[0]; record=request(cli,rid)
        if choices: choices.unlink(missing_ok=True)
        require(cli.rows(sql())[0]['snapshot_id']==result['snapshot_id'] and request(cli,rid)==record,'replay_changed')
        head=result['snapshot_id']; state=expected
        require(next(b for b in old if b['branch_name']=='source') in branches(cli),'source_changed')
        if dest!=current:
            require(next(b for b in old if b['branch_name']==current) in branches(cli),'ours_changed')
            cli.run(f"CALL differential_drop_branch('native_soak',{literal(current)});"); current=dest
        else:
            require(next(b for b in old if b['branch_name']==current)['branch_id']==next(b for b in branches(cli) if b['branch_name']==current)['branch_id'],'target_identity_changed')
        parents=cli.rows(f"SELECT parents FROM differential_merge_ancestry('native_soak',{head},{tid}) WHERE snapshot_id={head}")[0]['parents']
        require(parents==[oid,tid],'merge_parents')
        expected_path=root/'expected.duckdb'; expected_path.unlink(missing_ok=True)
        cli.run(f'ATTACH {literal(expected_path)} AS expected; USE expected;'+create_sql(state)+'CHECKPOINT;')
        row['verification']=verify_database(cli,'native_soak',head,expected_path)
        if remote:
            row['remote_verification']=verify_remote(cli,info,head,expected_path,s3_options(args))
            heads=remote_heads(cli,info,s3_options(args,named=True))
            require(next(h for h in old_heads if h['branch_name']=='source') in heads,'remote_source_changed')
            require(next(h for h in heads if h['branch_name']==dest)['snapshot_id']==head,'remote_head_not_published')
        row.update(snapshot_id=head,rows=len(state),conflicts=len(conflicts),
                   timings_ms={n:result[n] for n in ('comparison_ms','application_ms','publication_ms')},
                   resources=clean(cli,head,current,remote),status='passed')
    return {'final_snapshot':head,'final_rows':len(state),'rounds':len(trace),
            'native_equality_sha256':model.fingerprint,'types':TYPES,
            'provider_namespace':location or None}
