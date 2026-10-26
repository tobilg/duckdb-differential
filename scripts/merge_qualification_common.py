"""Bounded stock-host execution and evidence shared by merge qualification tools."""
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shutil
import signal
import subprocess
import tempfile
import threading
import time

ROOT = Path(__file__).resolve().parents[1]
DUCKDB_COMMIT = 'd54cbbe3df17e8e5ce68312f483b4155836112aa'
REPORT_VERSION = 1
MAX_OUTPUT = 16 * 1024 * 1024


class QualificationError(Exception):
    def __init__(self, code):
        self.code = code
        super().__init__(code)


def require(condition, code):
    if not condition: raise QualificationError(code)


def literal(value): return "'" + str(value).replace("'", "''") + "'"
def identifier(value): return '"' + str(value).replace('"', '""') + '"'


def sha(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b''): digest.update(chunk)
    return digest.hexdigest()


def write_report(path, report):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(mode='w', dir=path.parent, prefix='.merge-report-', delete=False) as output:
        temporary = Path(output.name)
        try:
            json.dump(report, output, indent=2, ensure_ascii=True)
            output.write('\n'); output.flush(); os.fsync(output.fileno())
        except BaseException:
            temporary.unlink(missing_ok=True)
            raise
    os.replace(temporary, path)


def source_identity():
    def git(*args): return subprocess.check_output(['git', *args], cwd=ROOT, timeout=30)
    names = git('ls-files', '-z', '--cached', '--others', '--exclude-standard', '--',
                'src', 'scripts', 'test', 'benchmarks', 'CMakeLists.txt', 'Makefile', 'vcpkg.json',
                'extension_config.cmake', 'extension_config_integration.cmake', '.github/workflows')
    digest = hashlib.sha256()
    for name in sorted(set(names.decode().split('\0')) - {''}):
        path = ROOT / name
        digest.update(name.encode() + b'\0')
        digest.update((sha(path) if path.is_file() else 'missing').encode() + b'\0')
    return {'git_commit':git('rev-parse', 'HEAD').decode().strip(),
            'git_dirty':bool(git('status', '--porcelain').strip()), 'source_sha256':digest.hexdigest(),
            'duckdb_commit':git('-C', 'duckdb', 'rev-parse', 'HEAD').decode().strip(),
            'duckdb_dirty':bool(git('-C', 'duckdb', 'status', '--porcelain').strip())}


def decode(text):
    outputs = []; decoder = json.JSONDecoder()
    while text.strip():
        text = text.lstrip(); value, end = decoder.raw_decode(text)
        outputs.append(value); text = text[end:]
    return outputs


def error_code(text):
    # Reports never retain raw SQL/errors, provider responses or row values.
    found = re.search(r'\[differential:([a-z_]{1,64})\]', text)
    known = {'merge_limit', 'merge_timeout', 'merge_conflict', 'merge_schema_conflict', 'merge_constraint',
             'merge_application', 'merge_resolution', 'merge_unsupported', 'merge_aborted', 'merge_inputs_changed',
             'merge_target_dirty', 'merge_target_changed', 'active_attachment_or_transaction', 'not_found',
             'retired', 'disk_full', 'corrupt_data', 'writer_busy', 'name_conflict', 'head_conflict',
             'merge_publication_uncertain', 'merge_pending_operation', 'incomplete_export'}
    if found and found[1] in known: return found[1]
    if 'Interrupt' in text or 'interrupt' in text: return 'cancelled'
    if 'Constraint Error' in text: return 'constraint_violation'
    if 'Out of Memory' in text: return 'out_of_memory'
    if 'Request ID reused' in text: return 'request_mismatch'
    return 'sql_error'


class Budget:
    def __init__(self, directory, seconds=900, disk_bytes=1024**3, free_bytes=1024**3):
        require(seconds > 0 and disk_bytes > 0 and free_bytes >= 0, 'invalid_budget')
        self.directory = Path(directory)
        self.deadline = time.monotonic() + seconds
        self.disk_bytes, self.free_bytes = disk_bytes, free_bytes
        self.peak_disk_bytes = 0

    def check(self):
        require(time.monotonic() < self.deadline, 'qualification_timeout')
        total = 0
        for path in self.directory.rglob('*'):
            try:
                if path.is_file(): total += path.stat().st_size
            except FileNotFoundError: pass
        self.peak_disk_bytes = max(self.peak_disk_bytes, total)
        require(total <= self.disk_bytes, 'disk_budget')
        require(shutil.disk_usage(self.directory).free >= self.free_bytes, 'free_space_reserve')


def process(command, *, input_text='', env=None, seconds=60, budget=None, cancel_gate=None, observer=None):
    """Output goes to bounded temporary files; wait4 owns child reaping and RSS/I/O."""
    if budget: budget.check()
    require(0 < seconds <= 3600, 'invalid_process_deadline')
    require(len(input_text.encode()) <= MAX_OUTPUT, 'input_budget')
    with tempfile.TemporaryFile() as source, tempfile.TemporaryFile() as output, tempfile.TemporaryFile() as errors:
        source.write(input_text.encode()); source.seek(0)
        start = time.monotonic(); stop = threading.Event(); failure = []; sent = []
        child = subprocess.Popen([str(x) for x in command], stdin=source, stdout=output, stderr=errors,
                                 env=env, cwd=ROOT)
        def monitor():
            last_budget = 0
            while not stop.wait(.01 if observer else .05):
                try:
                    require(time.monotonic() - start < seconds, 'process_timeout')
                    require(os.fstat(output.fileno()).st_size + os.fstat(errors.fileno()).st_size <= MAX_OUTPUT, 'output_budget')
                    if budget and (not observer or time.monotonic()-last_budget >= .1):
                        budget.check(); last_budget = time.monotonic()
                    if observer: observer(child)
                    if cancel_gate and not sent and Path(str(cancel_gate)+'.entered').exists():
                        os.kill(child.pid, signal.SIGINT)
                        Path(str(cancel_gate)+'.release').touch(); sent.append(True)
                except Exception as error:
                    failure.append(error.code if isinstance(error, QualificationError) else 'monitor_error')
                    try: os.kill(child.pid, signal.SIGKILL)
                    except ProcessLookupError: pass
                    return
        watcher = threading.Thread(target=monitor, daemon=True); watcher.start()
        try:
            _, status, usage = os.wait4(child.pid, 0)
            child.returncode = os.waitstatus_to_exitcode(status)
        except BaseException:
            try: os.kill(child.pid, signal.SIGKILL)
            except ProcessLookupError: pass
            child.wait()
            raise
        finally:
            stop.set(); watcher.join(timeout=2)
        require(not failure, failure[0] if failure else 'monitor_error')
        output.seek(0); errors.seek(0)
        out = output.read(MAX_OUTPUT+1); err = errors.read(MAX_OUTPUT+1)
        require(len(out)+len(err) <= MAX_OUTPUT, 'output_budget')
        if budget: budget.check()
        return child.returncode, out.decode(), err.decode(errors='replace'), {
            'seconds':time.monotonic()-start,
            'peak_rss_bytes':usage.ru_maxrss * (1 if platform.system() == 'Darwin' else 1024),
            'input_blocks':usage.ru_inblock, 'output_blocks':usage.ru_oublock,
            'interrupt_sent':bool(sent)}


class CLI:
    def __init__(self, duckdb, extension, repository, *, memory='128MB', temp='512MB',
                 seconds=60, budget=None, spill=None, timezone=None, calendar=None):
        self.duckdb, self.extension = Path(duckdb).resolve(), Path(extension).resolve()
        self.repository = Path(repository).resolve()
        self.seconds, self.budget = seconds, budget
        self.memory, self.temp, self.timezone, self.calendar = memory, temp, timezone, calendar
        self.samples = []
        self.env = {k:v for k,v in os.environ.items() if not k.startswith('DIFFERENTIAL_')}
        self.prefix = (f'LOAD {literal(self.extension)}; SET differential_repository={literal(self.repository)};'
                       f'SET threads=2; SET memory_limit={literal(memory)}; SET max_temp_directory_size={literal(temp)};'
                       'SET differential_auto_gc=false;')
        if spill: self.prefix += f'SET temp_directory={literal(Path(spill).resolve())};'
        for name, value in (('TimeZone',timezone),('Calendar',calendar)):
            if value is not None: self.prefix += f'SET {identifier(name)}={literal(value)};'

    def fork(self, repository):
        child = CLI(self.duckdb,self.extension,repository,memory=self.memory,temp=self.temp,
                    seconds=self.seconds,budget=self.budget,spill=Path(repository).parent/'spill',
                    timezone=self.timezone,calendar=self.calendar)
        child.env = dict(self.env)
        return child

    def run(self, sql, *, error=None, crash=None, cancel_gate=None, cancel_phase='staging'):
        env = dict(self.env)
        if crash: env['DIFFERENTIAL_MERGE_CRASH'] = crash
        if cancel_gate:
            for suffix in ('.entered', '.release'): Path(str(cancel_gate)+suffix).unlink(missing_ok=True)
            env['DIFFERENTIAL_MERGE_PHASE_GATE'] = cancel_phase+':'+str(cancel_gate)
        rc, out, err, sample = process([self.duckdb, '-init', '/dev/null', '-unsigned', '-no-agent', '-batch', '-bail', '-json'],
            input_text=self.prefix+sql, env=env, seconds=self.seconds, budget=self.budget, cancel_gate=cancel_gate)
        self.samples.append(sample)
        if crash:
            require(rc == 86, 'crash_hook_not_observed'); return []
        if error:
            # The stock shell suppresses errors/output after SIGINT. The soak also
            # verifies the durable cancellation diagnostic and unchanged target.
            cancelled_shell = error == 'cancelled' and cancel_gate and sample['interrupt_sent']
            require(rc != 0 and (cancelled_shell or error_code(err) == error), 'expected_error_not_observed')
            if cancel_gate: require(sample['interrupt_sent'], 'cancellation_not_observed')
            return []
        if rc: raise QualificationError(error_code(err))
        try: return decode(out)
        except (ValueError, UnicodeError): raise QualificationError('invalid_cli_output')

    def rows(self, sql, **kwargs): return self.run(sql, **kwargs)[-1]
    def scalar(self, sql): return next(iter(self.rows(sql)[0].values()))

    def identity(self):
        info = source_identity()
        version, hooks, extension = self.run("SELECT library_version,source_id FROM pragma_version();"
            "SELECT status FROM differential_status() WHERE capability='test_hooks';"
            "SELECT extension_version FROM duckdb_extensions() WHERE extension_name='differential';")[-3:]
        require(version[0]['library_version'] == 'v2.0.0' and version[0]['source_id'] == DUCKDB_COMMIT[:10], 'wrong_duckdb')
        require(info['duckdb_commit'] == DUCKDB_COMMIT and not info['duckdb_dirty'], 'duckdb_source_changed')
        info.update(platform=platform.system(), machine=platform.machine(), duckdb_version='v2.0.0',
                    extension_version=extension[0]['extension_version'], test_hooks=hooks[0]['status'].startswith('compiled'),
                    extension_sha256=sha(self.extension), duckdb_sha256=sha(self.duckdb))
        return info

    def check_artifacts(self, identity):
        require(sha(self.extension) == identity['extension_sha256'] and sha(self.duckdb) == identity['duckdb_sha256'],
                'artifacts_changed_during_run')
        current = source_identity()
        require(all(identity[key] == value for key,value in current.items()), 'source_changed_during_run')


def single_writer_database(cli, database):
    rows = cli.rows('SELECT * FROM differential_databases()')
    found = [r for r in rows if database in (r['database_name'], r['database_id'])]
    require(len(found) == 1, 'database_not_found')
    require(found[0]['writers'] == 'single', 'single_writer_required')
    return found[0]


def connection_arguments(parser):
    parser.add_argument('--timezone', help='native DuckDB TimeZone for execution and verification')
    parser.add_argument('--calendar', help='native DuckDB Calendar for execution and verification')
    parser.add_argument('--s3-region')
    parser.add_argument('--s3-endpoint', help='endpoint used by the independent S3 reader')
    parser.add_argument('--s3-profile', help='named AWS SDK credential profile; never credentials')
    parser.add_argument('--s3-path-style', action='store_true')
    parser.add_argument('--s3-allow-http', action='store_true')


def s3_options(args, *, named=False):
    # Credentials remain in the SDK chain/profile or native persistent secrets.
    values = {'region':args.s3_region,'endpoint':args.s3_endpoint,'credential_profile':args.s3_profile,
              'path_style':args.s3_path_style,'allow_http':args.s3_allow_http}
    return ','.join(key+('=>' if named else ' ')+(str(value).lower() if isinstance(value,bool) else literal(value))
                    for key,value in values.items() if value is not None)


def catalog_queries(alias):
    where = f'database_name={literal(alias)} AND NOT internal'
    return [
        f'SELECT schema_name,table_name,sql FROM duckdb_tables() WHERE {where} ORDER BY ALL;',
        f'SELECT schema_name,table_name,column_name,column_index,data_type,is_nullable,column_default FROM duckdb_columns() WHERE {where} ORDER BY ALL;',
        f'SELECT schema_name,table_name,constraint_type,constraint_text,constraint_column_names FROM duckdb_constraints() WHERE database_name={literal(alias)} ORDER BY ALL;',
        f'SELECT schema_name,index_name,table_name,is_unique,is_primary,expressions,sql FROM duckdb_indexes() WHERE database_name={literal(alias)} ORDER BY ALL;',
        f'SELECT schema_name,view_name,column_count,sql,is_bound FROM duckdb_views() WHERE {where} ORDER BY ALL;',
        # duckdb_dependencies() scans the current catalog. Normalize transient OIDs
        # to object identities; equal SQL alone does not prove equal native dependencies.
        f'USE {identifier(alias)}; WITH objects AS ('
        f"SELECT table_oid oid,'table' kind,schema_name,table_name name FROM duckdb_tables() WHERE {where} UNION ALL "
        f"SELECT view_oid,'view',schema_name,view_name FROM duckdb_views() WHERE {where} UNION ALL "
        f"SELECT index_oid,'index',schema_name,index_name FROM duckdb_indexes() WHERE database_name={literal(alias)} UNION ALL "
        f"SELECT oid,'schema',schema_name,schema_name FROM duckdb_schemas() WHERE database_name={literal(alias)}) "
        'SELECT o.kind object_kind,o.schema_name object_schema,o.name object_name,'
        'r.kind dependent_kind,r.schema_name dependent_schema,r.name dependent_name,d.deptype '
        'FROM duckdb_dependencies() d LEFT JOIN objects o ON o.oid=d.objid LEFT JOIN objects r ON r.oid=d.refobjid '
        "WHERE o.kind='view' OR r.kind='view' ORDER BY ALL;"]


def verify_database(cli, database, snapshot, expected, verification=None, *, actual_attach=None):
    """Compare native catalog metadata and rows with SQL typed multiset equality."""
    attach = (actual_attach or f'ATTACH {literal("differential:"+database)} AS actual (SNAPSHOT_VERSION {snapshot},READ_ONLY);')
    attach += f'ATTACH {literal(Path(expected).resolve())} AS expected (READ_ONLY);'
    queries = catalog_queries('actual') + catalog_queries('expected')
    results = cli.run(attach + ''.join(queries))[-len(queries):]
    actual, wanted = results[:len(queries)//2], results[len(queries)//2:]
    require(actual == wanted, 'catalog_mismatch')
    require(all(r['schema_name'] == 'main' for r in wanted[0] + wanted[4]), 'expected_schema_unsupported')
    # Ensure expected objects outside the supported catalog cannot be silently omitted.
    for function in ('duckdb_sequences', 'duckdb_types', 'duckdb_functions'):
        condition = '' if function == 'duckdb_sequences' else ' AND NOT internal'
        require(cli.scalar(attach+f"SELECT count(*) FROM {function}() WHERE database_name='expected'{condition}") == 0,
                'expected_catalog_unsupported')
    checks = []
    verification = {} if verification is None else verification
    verification.update(catalog_sha256=hashlib.sha256(json.dumps(actual,sort_keys=True).encode()).hexdigest(),
                        tables=len(wanted[0]), views=len(wanted[4]), indexes=len(wanted[3]), relations=checks, complete=False)
    for kind, rows, column in [('table', wanted[0], 'table_name'), ('view', wanted[4], 'view_name')]:
        for row in rows:
            name = identifier(row['schema_name'])+'.'+identifier(row[column])
            check = cli.rows(attach+f'SELECT (SELECT count(*) FROM actual.{name}) row_count,'
                f'(SELECT count(*) FROM (SELECT * FROM actual.{name} EXCEPT ALL SELECT * FROM expected.{name})) unexpected,'
                f'(SELECT count(*) FROM (SELECT * FROM expected.{name} EXCEPT ALL SELECT * FROM actual.{name})) missing;')[0]
            checks.append({'kind':kind, 'schema':row['schema_name'], 'name':row[column], **check})
            require(check['unexpected'] == check['missing'] == 0, 'data_mismatch')
    verification['complete'] = True
    return verification


def remote_heads(cli, info, options):
    return cli.rows(f'SELECT database_id,branch_id,branch_name,snapshot_id,export_locator FROM '
                    f'differential_remote_heads({literal(info["data_path"])},{options}) '
                    f'WHERE database_id={literal(info["database_id"])} ORDER BY branch_name')


def verify_remote(cli, info, snapshot, expected, options, verification=None):
    """Verify the exact immutable export, never a possibly advanced/reused branch head.

    No object operations in Python: the fresh reader uses the extension's SDK.
    Its repository is removed even on mismatch, cancellation or transport failure.
    """
    rows = cli.rows(f'SELECT export_state,export_locator FROM differential_snapshots({literal(info["database_id"])}) '
                    f'WHERE snapshot_id={int(snapshot)}')
    require(len(rows)==1 and rows[0]['export_state']=='complete', 'incomplete_export')
    locator = rows[0]['export_locator']
    require(isinstance(locator,str) and re.fullmatch(r'descriptors/[0-9a-f]{64}',locator), 'invalid_export_locator')
    verification = {} if verification is None else verification
    # Keep scratch inside a bounded soak directory when a budget is active.
    with tempfile.TemporaryDirectory(prefix='merge-remote-reader-',dir=cli.budget.directory if cli.budget else None) as tmp:
        reader = cli.fork(Path(tmp)/'repo')
        try:
            attach = f'ATTACH {literal("differential:"+info["data_path"].rstrip("/")+"/"+locator)} AS actual (READ_ONLY,{options});'
            verify_database(reader,info['database_id'],snapshot,expected,verification,actual_attach=attach)
            verification.update(fresh_repository=True,export_locator=locator)
        finally: cli.samples.extend(reader.samples)
    return verification


def operation_request(cli, database, operation):
    after = ''
    for _ in range(64):
        rows = cli.rows(f'SELECT * FROM differential_merge_requests({literal(database)},max_results=>1000,after_operation_id=>{literal(after)})')
        for row in rows:
            if row['operation_id']==operation: return row
        if len(rows)<1000: raise QualificationError('not_found')
        after = rows[-1]['operation_id']
    raise QualificationError('request_lookup_limit')
