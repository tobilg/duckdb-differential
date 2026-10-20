#!/usr/bin/env python3
"""Run unmodified upstream SQLLogicTests against native and Differential attachments.

Every test gets fresh disposable storage and a separate runner process. Skips are exact
paths with observed failure signatures, not broad globs; --audit-skips reruns both sides
and rejects unexpected passes or changed failures. --survey records failures without
changing the skip list. Required official extensions are cached under build/; tests
that reference public remote fixtures need network access.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import fnmatch
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]
DUCKDB = ROOT / 'duckdb'
# Memory limit of every test (both backends) unless its configuration sets one. Two concurrent pairs
# at 3GB fit an 8 GB machine; DuckDB's own runner uses its default (80% of RAM).
MEMORY_LIMIT = '3GB'
OFFICIAL_EXTENSIONS = {'encodings', 'httpfs', 'inet', 'quack', 'sqlite_scanner'}
sys.path.insert(0, str(ROOT / 'test/integration'))
from evidence import evidence_path


def quote(value):
    return "'" + str(value).replace("'", "''") + "'"


def required_extensions(test):
    return set(re.findall(r'^require (\w+)\s*$', (DUCKDB / test).read_text(errors='surrogateescape'), re.MULTILINE)) & OFFICIAL_EXTENSIONS


def prepare_extensions(cli, cache, required, output):
    if not required:
        return {}
    cache.mkdir(parents=True, exist_ok=True)
    sql = f'SET extension_directories=[{quote(cache)}]; '
    # INSTALL is idempotent and uses the CLI's exact version/platform directory.
    sql += ''.join(f'INSTALL {name}; LOAD {name}; ' for name in sorted(required))
    result = subprocess.run([str(cli), '-init', '/dev/null', '-batch', '-c', sql],
                            cwd=ROOT, text=True, capture_output=True)
    (output / 'extension-dependencies.log').write_text(result.stdout + result.stderr)
    if result.returncode:
        raise RuntimeError(f'official test dependency installation failed; see {output / "extension-dependencies.log"}')
    return {str(path.relative_to(cache)): hashlib.sha256(path.read_bytes()).hexdigest()
            for name in required for path in cache.rglob(name + '.duckdb_extension')}


def working_tree(work, test, fixtures=()):
    tree = work / 'tree'
    tree.mkdir()
    for entry in DUCKDB.iterdir():
        if entry.name not in ('.git', 'duckdb_unittest_tempdir'):
            (tree / entry.name).symlink_to(entry.resolve(), target_is_directory=entry.is_dir())
    # Some upstream ATTACH tests open checked-in database fixtures read/write.
    # Give each backend its own copies, while keeping large read-only CSV/Parquet
    # inputs shared. Materialize just the directory ancestors of copied files.
    source = (DUCKDB / test).read_text(errors='surrogateescape')
    references = set(re.findall(r"['\"]((?:data|test|\{DATA_DIR\})/[^'\"\n]+\.(?:db|duckdb))['\"]", source))
    references.update(re.findall(r'^load ((?:data|test|\{DATA_DIR\})/\S+\.(?:db|duckdb))(?:\s|$)',
                                 source, re.MULTILINE))
    copies = set(fixtures)
    for reference in references:
        pattern = re.sub(r'\$?\{[^}]+\}', '*', reference.replace('{DATA_DIR}', 'data'))
        if '..' in Path(pattern).parts:
            raise ValueError(f'fixture outside the working tree: {reference}')
        copies.update(str(path.relative_to(DUCKDB)) for path in DUCKDB.glob(pattern) if path.is_file())
    for relative in list(copies):
        if Path(relative).suffix in ('.db', '.duckdb'):
            copies.update(relative + suffix for suffix in ('.wal', '.wal.checkpoint', '.wal.recovery')
                          if (DUCKDB / (relative + suffix)).is_file())
    for relative in sorted(copies):
        original = DUCKDB / relative
        if not original.exists():
            continue
        if '..' in Path(relative).parts or Path(relative).is_absolute():
            raise ValueError(f'fixture outside the working tree: {relative}')
        destination = tree
        for part in Path(relative).parts[:-1]:
            destination /= part
            if destination.is_symlink():
                source = destination.resolve()
                destination.unlink()
                destination.mkdir()
                for entry in source.iterdir():
                    (destination / entry.name).symlink_to(entry, target_is_directory=entry.is_dir())
        destination /= original.name
        destination.unlink(missing_ok=True)
        if original.is_dir():
            shutil.copytree(original, destination, symlinks=True)
        else:
            shutil.copy2(original, destination)
    return tree


def run_test(runner, extension, bridge, test, mode, timeout, output, trace_empty=False, test_config=None,
             extension_cache=None):
    with tempfile.TemporaryDirectory(prefix='differential-upstream-') as tmp:
        work = Path(tmp)
        # Keep DuckDB's default cwd-relative test directory ({TEST_DIR}) inside a
        # disposable working tree, so repeated `load` commands still delete and
        # recreate their databases (fresh-database semantics).
        tree = working_tree(work, test, (test_config or {}).get('fixtures', ()))
        if re.search(r'^require-env TEST_PERSISTENT_SECRETS_AVAILABLE\s*$',
                     (DUCKDB / test).read_text(errors='surrogateescape'), re.MULTILINE):
            # These are public upstream fixtures, never the user's secret files.
            for fixture in (tree / 'data/secrets/http').glob('*.duckdb_secret'):
                if fixture.is_symlink() or not fixture.resolve().is_relative_to(work.resolve()):
                    raise RuntimeError('persistent secret fixtures need disposable copies')
                fixture.chmod(0o600)
        if extension_cache:
            # Tests may INSTALL/UPDATE extensions: never share their writable
            # directory with another test or with the prepared download cache.
            for name in required_extensions(test) | set((test_config or {}).get('preload', ())):
                for source in extension_cache.rglob(name + '.duckdb_extension*'):
                    target = work / 'extensions' / source.relative_to(extension_cache)
                    target.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(source, target)
        attach = (f"LOAD {quote(extension)}; SET differential_repository='{{TEST_DIR}}/repository'; "
                  if mode == 'differential' else '')
        attach += ''.join(f'LOAD {name}; ' for name in (test_config or {}).get('preload', ()))
        query_trace = work / 'queries.sql'
        if trace_empty:
            attach = f'SET log_query_path={quote(query_trace)}; ' + attach
        setup = "".join(f"PRAGMA upstream_bridge({quote(mode)}, '{{TEST_DIR}}', {phase});"
                        for phase in range(8))
        config = {
            'description': f'Differential upstream comparison ({mode})',
            'initial_db': '{TEST_DIR}/memory',
            # DuckDB runs on_new_connection before *every* concurrent-loop
            # statement, including COMMIT/ROLLBACK in an aborted transaction.
            # The bridge sets the instance default; subsequent connections need
            # no startup SQL and retain the upstream transaction behavior.
            'on_init': attach + setup,
            'skip_compiled': True,
            # AVAILABLE makes LoadExtension accept the bridge's explicit local path
            # before parsing on_init pragmas. SQL autoload/install stay disabled.
            'statically_loaded_extensions': ['core_functions', quote(bridge)],
            'autoloading': 'available',
            'max_threads': 2,
            'settings': [{'name': 'extension_directory', 'value': str(work / 'extensions')},
                         {'name': 'memory_limit', 'value': MEMORY_LIMIT},
                         {'name': 'autoload_known_extensions', 'value': 'false'},
                         {'name': 'autoinstall_known_extensions', 'value': 'false'}],
        }
        if re.search(r'^require-env LOCAL_EXTENSION_REPO\s*$', (DUCKDB / test).read_text(errors='surrogateescape'), re.MULTILINE):
            repository = runner.parents[1] / 'repository'
            if not repository.is_dir():
                raise RuntimeError(f'missing built extension repository fixture: {repository}')
            config['test_env'] = [{'env_name': 'LOCAL_EXTENSION_REPO', 'env_value': str(repository)}]
        for name in ('VALIDATE_TAGS', 'TEST_PERSISTENT_SECRETS_AVAILABLE'):
            if re.search(r'^require-env ' + name + r'\s*$', (DUCKDB / test).read_text(errors='surrogateescape'), re.MULTILINE):
                config.setdefault('test_env', []).append({'env_name': name, 'env_value': '1'})
        if test_config:
            config.update(test_config.get('options', {}))
            if config.get('max_threads') is None:
                config.pop('max_threads', None)
            settings = {item['name']: item['value'] for item in config['settings']}
            settings.update(test_config.get('settings', {}))
            config['settings'] = [{'name': key, 'value': value} for key, value in settings.items()]
        config_path = work / 'config.json'
        config_path.write_text(json.dumps(config))
        command = [str(runner), '--test-dir', str(tree),
                   '--test-config', str(config_path), test]
        start = time.monotonic()
        # Prevent inherited test configuration or repository settings from contaminating a run.
        env = {k: v for k, v in os.environ.items()
               if not k.startswith(('DUCKDB_TEST_', 'DIFFERENTIAL_', 'LOCAL_EXTENSION_REPO'))}
        try:
            result = subprocess.run(command, cwd=ROOT, env=env, text=True, errors='replace',
                                    capture_output=True, timeout=timeout)
            text = result.stdout + result.stderr
            code = result.returncode
        except subprocess.TimeoutExpired as error:
            text = 'TIMEOUT\n' + (error.stdout or b'').decode(errors='replace') + (error.stderr or b'').decode(errors='replace')
            code = 124
        log = output / (test.replace('/', '__') + '.' + mode + '.log')
        log.write_text(text)
        # Catch returns zero for skipped tests too: never count them as executed coverage.
        summary = re.search(r'All tests passed \((\d+) assertions? in (\d+) test cases?\)', text)
        assertions = int(summary[1]) if summary else 0
        executed = assertions > 0 and int(summary[2]) == 1
        # A successful zero-row query can perform no Catch assertions. Distinguish
        # that from `mode skip` by rerunning only these ambiguous cases with SQL
        # tracing, then checking that a statement from the source actually ran.
        no_assertions = 'assertions: - none -' in text and '1 passed' in text
        if code == 0 and no_assertions:
            if not trace_empty:
                return run_test(runner, extension, bridge, test, mode, timeout, output,
                                trace_empty=True, test_config=test_config, extension_cache=extension_cache)
            trace = query_trace.read_text(errors='replace') if query_trace.exists() else ''
            normalize = lambda sql: ' '.join(sql.split()).rstrip(';')
            source = (DUCKDB / test).read_text(errors='surrogateescape')
            queries = []
            skipped = False
            for command in re.finditer(r'^mode (skip|unskip)\s*$|'
                                       r'^(?:query|statement) [^\n]*\n((?:[^\n]+\n)*)',
                                       source, re.MULTILINE):
                if command[1]:
                    skipped = command[1] == 'skip'
                elif not skipped:
                    queries.append(command[2])
            placeholder = r'(\$?\{[^}]+\}|__\w+__)'
            patterns = [''.join('.+?' if re.fullmatch(placeholder, part) else re.escape(part)
                                for part in re.split(placeholder, normalize(q.split('\n----')[0])))
                        for q in queries if q.strip()]
            executed = any(re.search(pattern, normalize(trace)) for pattern in patterns)
            # A lifecycle-only script can test empty-database restart without any
            # SQL assertions. Verify the final initialization reached completion;
            # an empty file or a skipped script must still not count as coverage.
            commands = [line.strip() for line in source.splitlines()
                        if line.strip() and not line.lstrip().startswith('#')]
            lifecycle = commands and all(re.fullmatch(r'(?:load(?:\s+.*)?|restart)', line)
                                         for line in commands)
            if lifecycle:
                executed = ('DETACH __upstream_switch;' in trace or
                            "SELECT '__upstream_native_fixture'" in trace)
            text += '\nZero-assertion execution trace:\n' + trace
            log.write_text(text)
        status = 'passed' if code == 0 and executed else 'not_executed' if code == 0 else 'failed'
        return {'status': status, 'exit_code': code, 'assertions': assertions,
                'seconds': round(time.monotonic() - start, 3),
                'log': str(log)}, text


def main():
    global MEMORY_LIMIT
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('patterns', nargs='*', help='upstream paths/globs, overriding the default selection')
    parser.add_argument('--build-dir', type=Path, default=ROOT / 'build/integration')
    parser.add_argument('--extension', type=Path, help='override the Differential artifact (e.g. the release build)')
    parser.add_argument('--bridge', type=Path, help='override the test-only bridge artifact for adapter validation')
    parser.add_argument('--suite', type=Path, default=ROOT / 'test/upstream/suite.json')
    actions = parser.add_mutually_exclusive_group()
    actions.add_argument('--audit-skips', action='store_true')
    actions.add_argument('--survey', action='store_true')
    parser.add_argument('--timeout', type=int, help='optional diagnostic timeout; normal runs have no per-test limit')
    parser.add_argument('--jobs', type=int, default=2, help='concurrent test pairs (default: 2)')
    parser.add_argument('--memory-limit', default=MEMORY_LIMIT,
                        help=f'memory_limit of each test unless configured (default: {MEMORY_LIMIT})')
    parser.add_argument('--profile', choices=['full', 'pr'], default='full')
    parser.add_argument('--shard', help='stable shard INDEX/COUNT, zero-based')
    parser.add_argument('--list', action='store_true', help='list selection without executing')
    parser.add_argument('--retry-from', type=Path, help='rerun unsuccessful tests in a results.json')
    args = parser.parse_args()
    MEMORY_LIMIT = args.memory_limit
    if args.timeout is not None and args.timeout <= 0:
        parser.error('--timeout must be positive')
    if args.jobs < 1 or args.jobs > 32:
        parser.error('--jobs must be between 1 and 32')
    suite = json.loads(args.suite.read_text())
    commit = subprocess.check_output(['git', '-C', str(DUCKDB), 'rev-parse', 'HEAD'], text=True).strip()
    if commit != suite['duckdb_commit']:
        parser.error('DuckDB revision changed: review the suite and skips before updating duckdb_commit')
    skips = {}
    for skip in suite['skips']:
        path = skip['path']
        if path in skips or not (DUCKDB / path).is_file() or not skip.get('reason') or not skip.get('category'):
            parser.error(f'invalid, duplicate or stale skip: {path}')
        if skip.get('intermittent') and not skip.get('evidence'):
            parser.error(f'intermittent exclusion requires observed native-baseline evidence: {path}')
        if skip.get('execution') == 'never':
            if (skip['category'] != 'excessive_disk_space' or
                    skip.get('source_sha256') != hashlib.sha256((DUCKDB / path).read_bytes()).hexdigest()):
                parser.error(f'review the source and disk-space justification for {path}')
            skips[path] = skip
            continue
        expected = skip.get('expected', {})
        if (set(expected) != {'native', 'differential'} or
                any(not isinstance(value, dict) or value.get('status') not in ('passed', 'failed', 'not_executed')
                    or (value['status'] != 'passed' and not value.get('message')) for value in expected.values()) or
                all(value['status'] == 'passed' for value in expected.values())):
            parser.error(f'skip needs explicit observed outcomes and signatures for both backends: {path}')
        skips[path] = skip
    requirements = suite.get('requirements', {})
    for path, requirement in requirements.items():
        if path in skips or not (DUCKDB / path).is_file() or not requirement.get('reason'):
            parser.error(f'invalid conditional requirement: {path}')
        lines = (DUCKDB / path).read_text(errors='surrogateescape').splitlines()
        if not requirement.get('directives') or any(line not in lines for line in requirement['directives']):
            parser.error(f'upstream requirement changed: {path}')
    configs = suite.get('configs', {})
    for path, config in configs.items():
        if (not (DUCKDB / path).is_file() or not config.get('reason') or
                set(config.get('options', {})) - {'block_size', 'max_threads'} or
                set(config.get('settings', {})) - {'vacuum_rebuild_indexes', 'memory_limit',
                                                   'storage_compatibility_version'} or
                set(config.get('preload', [])) - OFFICIAL_EXTENSIONS):
            parser.error(f'invalid per-test configuration: {path}')
        for fixture in config.get('fixtures', []):
            if not fixture.startswith('data/') or '..' in Path(fixture).parts or not (DUCKDB / fixture).exists():
                parser.error(f'invalid fixture for {path}: {fixture}')
    patterns = args.patterns or suite['include']
    tests = sorted({str(p.relative_to(DUCKDB)) for pattern in patterns for p in DUCKDB.glob(pattern) if p.is_file()})
    if args.profile == 'pr' and not args.patterns:
        pr_tests = {str(p.relative_to(DUCKDB)) for pat in suite['pr_include'] for p in DUCKDB.glob(pat)}
        tests = [p for p in tests if p in pr_tests]
    if args.audit_skips:
        tests = sorted(p for p in skips if not args.patterns or any(fnmatch.fnmatch(p, pattern) for pattern in patterns))
    elif not args.survey:
        tests = [p for p in tests if p not in skips]
    if args.retry_from:
        retry = {item['test'] for item in json.loads(args.retry_from.read_text())['results']
                 if item['status'] not in ('passed', 'confirmed_skip', 'quarantine_passed',
                                           'unavailable', 'confirmed_policy_exclusion')}
        tests = [p for p in tests if p in retry]
    if args.shard:
        try:
            shard, count = map(int, args.shard.split('/'))
            assert 0 <= shard < count
        except (ValueError, AssertionError):
            parser.error('--shard must be INDEX/COUNT with 0 <= INDEX < COUNT')
        tests = [p for p in tests if int(hashlib.sha256(p.encode()).hexdigest(), 16) % count == shard]
    if args.list:
        print('\n'.join(tests))
        return
    if not tests:
        if args.audit_skips and (not skips or args.shard):
            print('PASS no upstream skips to audit')
            return
        parser.error('no tests selected')
    build = args.build_dir.resolve()
    runner = build / 'test/unittest'
    extension = (args.extension or build / 'extension/differential/differential.duckdb_extension').resolve()
    bridge = (args.bridge or build / 'extension/upstream_bridge/upstream_bridge.duckdb_extension').resolve()
    if not all(p.is_file() for p in (runner, extension, bridge)):
        parser.error(f'missing runner, extension or test bridge in {build}; run make integration first')
    output = evidence_path('upstream-' + ('audit' if args.audit_skips else 'survey' if args.survey else 'tests') +
                           '-' + str(time.time_ns()))
    output.mkdir(exist_ok=True)
    print(f'Evidence directory: {output}', flush=True)
    extension_cache = build.parent / 'upstream_extensions'
    required = set().union(*(required_extensions(test) | set(configs.get(test, {}).get('preload', []))
                             for test in tests if skips.get(test, {}).get('execution') != 'never'))
    dependencies = prepare_extensions(build / 'duckdb', extension_cache, required, output)
    report = {'duckdb_commit': commit, 'build': str(build), 'memory_limit': MEMORY_LIMIT,
              'harness_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
              'suite_sha256': hashlib.sha256(args.suite.read_bytes()).hexdigest(),
              'extension': str(extension),
              'bridge': str(bridge),
              'extension_sha256': hashlib.sha256(extension.read_bytes()).hexdigest(), 'results': [],
              'bridge_sha256': hashlib.sha256(bridge.read_bytes()).hexdigest(),
              'profile': args.profile, 'shard': args.shard, 'selected': len(tests),
              'diagnostic_timeout_seconds': args.timeout,
              'official_extensions': dependencies,
              'scope': 'Unmodified SQLLogicTests with native and local Differential attachments; upstream fixtures may use network.'}
    failed = 0

    def compare(test):
        item = {'test': test}
        policy = skips.get(test, {})
        if policy.get('execution') == 'never':
            # The owner explicitly excludes these disk-heavy workloads. Auditing
            # them must not execute the very workloads the policy excludes.
            message = f"NOT EXECUTED: reviewed disk-space exclusion. {policy['reason']}\n"
            for mode in ('native', 'differential'):
                log = output / (test.replace('/', '__') + '.' + mode + '.log')
                log.write_text(message)
                item[mode] = {'status': 'not_executed', 'exit_code': None, 'assertions': 0,
                              'seconds': 0, 'log': str(log)}
            item.update(status='confirmed_policy_exclusion', skip=policy,
                        validation='Pinned source hash and reviewed disk-space requirement; workload not executed.')
            return item, True
        texts = {}
        for mode in ('native', 'differential'):
            timeout = args.timeout
            item[mode], texts[mode] = run_test(runner, extension, bridge, test, mode, timeout, output,
                                              test_config=configs.get(test), extension_cache=extension_cache)
        if test in configs:
            item['config'] = configs[test]
        if args.audit_skips:
            skip = skips[test]
            ok = True
            for mode in ('native', 'differential'):
                expected = skip['expected'][mode]
                if skip.get('intermittent') and item[mode]['status'] == 'passed':
                    continue
                normalized = ' '.join(texts[mode].split())
                ok &= item[mode]['status'] == expected['status']
                # Neither timeouts nor signal termination reproduce an exclusion.
                # A killed runner may already have printed the expected diagnostic.
                ok &= item[mode]['exit_code'] >= 0 and item[mode]['exit_code'] != 124
                if expected['status'] != 'passed':
                    ok &= ' '.join(expected['message'].split()) in normalized
            item['skip'] = skip
            both_passed = all(item[m]['status'] == 'passed' for m in ('native', 'differential'))
            item['status'] = ('quarantine_passed' if both_passed else 'confirmed_skip') if ok else 'stale_skip'
        else:
            ok = all(item[m]['status'] == 'passed' for m in ('native', 'differential'))
            item['status'] = 'passed' if ok else 'needs_review'
            if not ok and test in requirements:
                requirement = requirements[test]
                messages = [line.split()[1] for line in requirement['directives']]
                unavailable = all(item[m]['status'] == 'not_executed' and
                                  any('require ' + value + ': 1' in texts[m] for value in messages)
                                  for m in ('native', 'differential'))
                if unavailable:
                    ok = True
                    item['status'] = 'unavailable'
                    item['requirement'] = requirement
        return item, ok

    with ThreadPoolExecutor(max_workers=args.jobs) as pool:
        futures = [pool.submit(compare, test) for test in tests]
        for future in as_completed(futures):
            item, ok = future.result()
            failed += not ok
            report['results'].append(item)
            pending_report = output / 'results.json.tmp'
            pending_report.write_text(json.dumps(report, indent=2) + '\n')
            pending_report.replace(output / 'results.json')
            print(f"{item['status'].upper()} {item['test']} (native={item['native']['status']}, differential={item['differential']['status']})", flush=True)
    print(f'{len(tests)} tests compared; {failed} require review; evidence: {output / "results.json"}')
    sys.exit(1 if failed and not args.survey else 0)


if __name__ == '__main__':
    main()
