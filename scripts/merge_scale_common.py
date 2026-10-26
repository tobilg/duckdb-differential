"""Shared, bounded native workload for scale measurements and spill fault tests.

Expected rows come from the key and the specified inputs, never merge output.
Object operations are SQL calls through the extension's pinned AWS SDK transport.
"""
from pathlib import Path
import os
import re
import sqlite3
import tempfile
import uuid

from merge_qualification_common import (CLI, Budget, require, literal,
    identifier, process, decode, error_code, catalog_queries, remote_heads,
    single_writer_database, s3_options)

COLUMNS = [('id','BIGINT'),('u','UUID'),('score','BIGINT'),('v','DOUBLE'),
           ('ts','TIMESTAMPTZ_NS'),('fixed','DOUBLE[4]'),
           ('attrs','STRUCT(n UHUGEINT,a DOUBLE[2],items INTEGER[],b BLOB)'),
           ('arrays','DOUBLE[2][]')]
DDL = ('CREATE TABLE x(id BIGINT PRIMARY KEY,u UUID,score BIGINT DEFAULT 0 CHECK(score>=0),'
       + ','.join(identifier(n)+' '+t for n,t in COLUMNS[3:])+');'
       'CREATE INDEX by_score ON x(score,id); SET enable_view_dependencies=true;'
       'CREATE VIEW leaf AS SELECT * FROM x; CREATE VIEW visible AS SELECT id,score,fixed,attrs FROM leaf;')
STATS = ("SELECT metric,value FROM differential_storage_stats() WHERE metric LIKE 's3_%' OR metric IN "
         "('local_logical_read_bytes','local_materialized_payload_bytes','sdk_request_calls');")
TIMERS = ('selection_ms','target_check_ms','materialization_ms','comparison_ms','application_ms','publication_ms')


def values(key, mode=0):
    """Mode 1 changes entire nested cells, including NULL/empty/NULL-child transitions."""
    k=f'({key})'; n=f'({k}+{mode})'
    fixed=f"CASE WHEN {n}%11=0 THEN NULL ELSE [{n}::DOUBLE,'NaN'::DOUBLE,NULL,-0.0] END::DOUBLE[4]"
    attrs=(f"CASE WHEN {n}%17=0 THEN NULL ELSE {{'n':340282366920938463463374607431768211455::UHUGEINT-{n}::UHUGEINT,"
           f"'a':CASE WHEN {n}%5=0 THEN NULL ELSE [{n}::DOUBLE,'Infinity'::DOUBLE] END::DOUBLE[2],"
           f"'items':CASE WHEN {n}%3=0 THEN []::INTEGER[] ELSE [({n}%7)::INTEGER,NULL] END,"
           f"'b':unhex(sha256({n}::VARCHAR))}} END::{COLUMNS[6][1]}")
    arrays=(f"CASE WHEN {n}%13=0 THEN NULL ELSE [[NULL,'NaN'::DOUBLE]::DOUBLE[2],"
            f"CASE WHEN {n}%7=0 THEN NULL ELSE [{n}::DOUBLE,-0.0] END::DOUBLE[2]] END::DOUBLE[2][]")
    return [k, f'md5({k}::VARCHAR)::UUID', '0::BIGINT', '0::DOUBLE',
            f'make_timestamp_ns(1577836800123456789+{n})::TIMESTAMPTZ_NS',fixed,attrs,arrays]


def expected_values(key, changed):
    base, incoming=values(key),values(key,1)
    base[2]=f'CASE WHEN {key}<{changed} THEN 1::BIGINT ELSE 0::BIGINT END'
    base[3]=f"CASE WHEN {key}<{changed} THEN 'NaN'::DOUBLE ELSE 0::DOUBLE END"
    for i in range(4,len(base)):
        base[i]=f'CASE WHEN {key}<{changed} THEN {incoming[i]} ELSE {base[i]} END'
    return base


def spill_files(directory):
    found={}
    for path in Path(directory).glob('*storage*.tmp'):
        try:
            size=path.stat().st_size
            if size: found[path]=size
        except FileNotFoundError: pass
    return found


class Observation:
    def __init__(self, directory, signum=None):
        self.directory=directory; self.signum=signum
        self.peak=0; self.sent=False; self.at_signal={}; self.samples=0

    def __call__(self, child):
        found=spill_files(self.directory); size=sum(found.values()); self.samples+=1
        self.peak=max(self.peak,size)
        if found and self.signum and not self.sent:
            # Popen.send_signal() polls/reaps; process() owns wait4 and rusage.
            try: os.kill(child.pid,self.signum)
            except ProcessLookupError: return
            self.sent=True; self.at_signal=found


class ScaleFixture:
    def __init__(self, root, args, *, rows, percent, destination='ours'):
        self.root=Path(root); self.root.mkdir(parents=True,exist_ok=True)
        self.rows=rows; self.changed=rows*percent//100; self.percent=percent; self.destination=destination
        self.spill=self.root/'spill'; self.args=args
        self.budget=Budget(self.root,args.case_seconds,args.disk_limit_mib*1024**2,args.free_space_mib*1024**2)
        self.cli=CLI(args.duckdb,args.extension,self.root/'repo',memory=args.memory_limit,temp=args.temp_limit,
                     seconds=args.process_seconds,budget=self.budget,spill=self.spill,timezone='UTC',calendar='gregorian')
        if args.s3_profile: self.cli.env['AWS_PROFILE']=args.s3_profile
        self.remote=bool(args.s3_location)
        self.location=(args.s3_location.rstrip('/')+'/scale-'+uuid.uuid4().hex+'/') if self.remote else ''
        self.identity=self.cli.identity()
        self.info=None; self.before=None; self.heads_before=None

    def measure(self, sql, *, memory=None, temp=None, external=False, signum=None, cli=None):
        cli=cli or self.cli
        spill=self.spill if cli is self.cli else cli.repository.parent/'spill'
        observation=Observation(spill,signum)
        settings=f'SET debug_force_external={str(external).lower()};'
        if memory: settings+=f'SET memory_limit={literal(memory)};'
        if temp: settings+=f'SET max_temp_directory_size={literal(temp)};'
        rc,out,err,usage=process([cli.duckdb,'-init','/dev/null','-unsigned','-no-agent','-batch','-bail','-json'],
            input_text=cli.prefix+settings+sql,env=cli.env,seconds=cli.seconds,budget=self.budget,observer=observation)
        usage.update(spill_peak_bytes=observation.peak,spill_samples=observation.samples,
                     disk_peak_bytes=self.budget.peak_disk_bytes,signal_sent=observation.sent,
                     spill_bytes_at_signal=sum(observation.at_signal.values()))
        return {'returncode':rc,'error_code':error_code(err) if rc else None,
                'temporary_disk_limit':'max_temp_directory_size' in err,
                'data':decode(out) if not rc and out.strip() else [],'process':usage},observation

    def run(self, sql, **kwargs):
        result,_=self.measure(sql,**kwargs)
        require(result['returncode']==0,result['error_code'] or 'sql_error')
        return result

    def branches(self):
        return self.cli.rows("SELECT branch_id,branch_name,root,head_snapshot_id FROM differential_branches('scale') WHERE state='active' ORDER BY branch_name")

    def setup(self):
        options=f'(DATA_PATH {literal(self.location)},{s3_options(self.args)})' if self.remote else ''
        first=self.run(f"ATTACH 'differential:scale' AS d {options}; USE d;"+DDL+
            f"INSERT INTO x SELECT {','.join(values('i'))} FROM range({self.rows}) r(i);"
            "CALL differential_create_snapshot('d'); USE memory; DETACH d;"
            "CALL differential_create_branch('scale','ours',snapshot=>1); CALL differential_create_branch('scale','theirs',snapshot=>1);")
        incoming=values('id',1)
        self.run("ATTACH 'differential:scale' AS o (BRANCH 'ours');"
            f"UPDATE o.x SET score=1,v='NaN' WHERE id<{self.changed}; DELETE FROM o.x WHERE id={self.rows-2};"
            f"INSERT INTO o.x SELECT {','.join(values(str(self.rows)))}; CALL differential_create_snapshot('o');")
        self.run("ATTACH 'differential:scale' AS t (BRANCH 'theirs'); UPDATE t.x SET v='NaN',"+
            ','.join(identifier(COLUMNS[i][0])+'='+incoming[i] for i in range(4,len(COLUMNS)))+
            f" WHERE id<{self.changed}; DELETE FROM t.x WHERE id={self.rows-1};"
            f"INSERT INTO t.x SELECT {','.join(values(str(self.rows+1)))}; CALL differential_create_snapshot('t');")
        self.info=single_writer_database(self.cli,'scale')
        # A branch merge retains its heads; evict the explicit common base and move
        # main away from it so GC proves comparison really rehydrates that input.
        if self.remote:
            self.run("ATTACH 'differential:scale' AS d; UPDATE d.x SET score=99 WHERE id=0;"
                "CALL differential_create_snapshot('d'); DETACH d;"
                "CALL differential_evict_snapshots('scale',versions=>[1]); CALL differential_cleanup_old_files(cleanup_all=>true);")
            with sqlite3.connect(self.cli.repository/'engine.sqlite') as db:
                require(db.execute('SELECT count(*) FROM snapshots s JOIN reclaimed_roots r ON s.root=r.root WHERE s.id=1').fetchone()[0]==1,'base_not_reclaimed')
        self.before=self.branches()
        self.roots=self.cli.rows("SELECT snapshot_id,root FROM differential_snapshots('scale') ORDER BY snapshot_id")
        self.heads_before=remote_heads(self.cli,self.info,s3_options(self.args,named=True)) if self.remote else []
        return first['process']

    def merge_sql(self, request='scale'):
        return (f"SELECT * FROM differential_merge_branch('scale','ours','theirs',{literal(self.destination)},request_id=>{literal(request)},"
                'expected_base_snapshot_id=>1,expected_ours_snapshot_id=>2,expected_theirs_snapshot_id=>3,'
                'max_input_rows=>4000000,max_input_bytes=>2147483648,max_stage_write_bytes=>2147483648,'
                f'timeout_ms=>{self.args.process_seconds*1000});')

    def request(self):
        return self.cli.rows("SELECT * FROM differential_merge_requests('scale',request_id=>'scale')")[0]

    def unpublished(self, expected_state='pending'):
        require(self.branches()==self.before,'failure_changed_branches')
        require(self.cli.rows("SELECT snapshot_id,root FROM differential_snapshots('scale') ORDER BY snapshot_id")==self.roots,'failure_published_snapshot')
        request=self.request()
        require(request['state']==expected_state and request['result_snapshot_id'] is None and
                not request['publication_attempted'],'failure_published_or_wrong_state')
        if self.remote: require(remote_heads(self.cli,self.info,s3_options(self.args,named=True))==self.heads_before,'failure_changed_remote_heads')

    def verify_tables(self, cli, attach):
        # Key domain + native primary-key constraint + cardinality + typed equality
        # proves the complete multiset without materializing a second million-row DB.
        checks=[f'a.{identifier(name)} IS NOT DISTINCT FROM {expr}' for (name,_),expr in
                zip(COLUMNS,expected_values('a.id',self.changed))]
        valid=f'a.id>=0 AND a.id<{self.rows+2} AND a.id NOT IN ({self.rows-2},{self.rows-1}) AND '+ ' AND '.join(checks)
        result=self.run(attach+f'SELECT count(*) n,count(*) FILTER(WHERE {valid}) correct FROM actual.x a;'+STATS,cli=cli)
        require(result['data'][0]==[{'n':self.rows,'correct':self.rows}],'typed_result_mismatch')
        # Native expected catalog is independently defined, with no copied DDL.
        sql=attach+"ATTACH ':memory:' AS expected; USE expected;"+DDL
        queries=catalog_queries('actual')+catalog_queries('expected')
        catalogs=cli.run(sql+''.join(queries))[-len(queries):]
        require(catalogs[:len(queries)//2]==catalogs[len(queries)//2:],'catalog_mismatch')
        return {'complete':True,'rows':self.rows,'process':result['process'],
                'io_counters':{v['metric']:v['value'] for v in result['data'][1]}}

    def verify(self, snapshot, *, remote=False):
        if not remote:
            return self.verify_tables(self.cli,f"ATTACH 'differential:scale' AS actual (SNAPSHOT_VERSION {snapshot},READ_ONLY);")
        exports=self.cli.rows(f"SELECT export_state,export_locator FROM differential_snapshots('scale') WHERE snapshot_id={snapshot}")
        require(len(exports)==1 and exports[0]['export_state']=='complete','incomplete_export')
        locator=exports[0]['export_locator']; require(re.fullmatch(r'descriptors/[0-9a-f]{64}',locator) is not None,'invalid_export_locator')
        with tempfile.TemporaryDirectory(prefix='remote-reader-',dir=self.root) as tmp:
            reader=self.cli.fork(Path(tmp)/'repo')
            result=self.verify_tables(reader,f'ATTACH {literal("differential:"+self.location+locator)} AS actual (READ_ONLY,{s3_options(self.args)});')
            result.update(fresh_repository=True,export_locator=locator)
            return result

    def complete(self, result):
        row=result['data'][0][0]; sid=row['snapshot_id']
        before=self.request(); replay=self.cli.rows(self.merge_sql())[0]
        require(before['state']=='complete','request_not_complete')
        require(replay['snapshot_id']==sid and replay['operation_id']==row['operation_id'] and self.request()==before,'replay_changed')
        after=self.branches()
        destination=[b for b in after if b['branch_name']==self.destination]
        require(len(destination)==1 and destination[0]['head_snapshot_id']==sid,'destination_not_completed')
        expected_branches=len(self.before)+(0 if any(b['branch_name']==self.destination for b in self.before) else 1)
        require(len(after)==expected_branches,'unexpected_branch')
        for branch in self.before:
            if branch['branch_name']==self.destination:
                require(any(b['branch_id']==branch['branch_id'] and b['head_snapshot_id']==sid for b in after),'target_identity_changed')
            else: require(branch in after,'input_branch_changed')
        snapshots=self.cli.rows("SELECT snapshot_id,root FROM differential_snapshots('scale') ORDER BY snapshot_id")
        require(len(snapshots)==len(self.roots)+1 and snapshots[-1]['snapshot_id']==sid,'duplicate_result_snapshot')
        require(snapshots[:-1]==self.roots,'input_roots_changed')
        verification=self.verify(sid)
        remote_verification=self.verify(sid,remote=True) if self.remote else None
        if self.remote:
            heads=remote_heads(self.cli,self.info,s3_options(self.args,named=True))
            for head in self.heads_before:
                if head['branch_name']!=self.destination: require(head in heads,'remote_input_changed')
            require(any(h['branch_name']==self.destination and h['snapshot_id']==sid for h in heads),'remote_head_not_published')
        self.run('CALL differential_cleanup_old_files(cleanup_all=>true);')
        with sqlite3.connect(self.cli.repository/'engine.sqlite') as db:
            require(db.execute('PRAGMA integrity_check').fetchone()[0]=='ok','metadata_integrity')
            for table in ('pins','compound_intents'):
                require(db.execute('SELECT count(*) FROM '+table).fetchone()[0]==0,'leaked_'+table)
            require(db.execute("SELECT count(*) FROM files WHERE name LIKE 'merge_stage/%'").fetchone()[0]==0,'leaked_staging')
            require(db.execute("SELECT count(*) FROM export_roots WHERE state='pending'").fetchone()[0]==0,'leaked_export')
            require(db.execute("SELECT count(*) FROM operations WHERE kind='merge_destination' AND state='pending'").fetchone()[0]==0,'leaked_reservation')
        self.cli.check_artifacts(self.identity)
        return {'snapshot_id':sid,'timings_ms':{n:row[n] for n in TIMERS},'process':result['process'],
                'io_counters':{v['metric']:v['value'] for v in result['data'][1]},
                'verification':verification,'remote_verification':remote_verification}


def workload_description():
    return {'ddl':DDL,'initial_values':values('id'),'incoming_values':values('id',1),
            'changes':'Same first K keys: ours score=1 and v=NaN; theirs v=NaN, ts +1ns and whole fixed/attrs/arrays values. '
                      'Ours deletes N-2 and inserts N; theirs deletes N-1 and inserts N+1. Inserts use base expressions.',
            'identity':'declared BIGINT primary key; u is a native UUID value',
            'verification':'exact typed equality for every column, complete key domain and cardinality under the preserved native primary key; independent catalog definitions',
            's3_inputs':'single-writer registered database; common base 1 evicted/reclaimed; branch heads 2/3 local',
            'counters':'process-local extension counters; verified GET bytes and single PUT input bytes, not physical device or wire I/O'}
