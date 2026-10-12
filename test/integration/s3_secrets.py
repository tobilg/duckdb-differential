#!/usr/bin/env python3
"""DuckDB secrets: TYPE s3 secrets supply credentials and connection settings for S3 locations
(matched by SCOPE), and TYPE differential secrets supply DATA_PATH and S3 options. Sessions run
without AWS credentials in the environment, so every successful S3 call used a secret. Needs the
disposable MinIO bucket initialized by minio.py and network access for the official httpfs."""
import json
import os
from pathlib import Path
import subprocess
import sys
from urllib.parse import urlparse
from evidence import evidence_path
from paths import DUCKDB_CLI, EXTENSION

endpoint, base = sys.argv[1:]
base = Path(base) / 'secrets'
base.mkdir()
host = urlparse(endpoint).netloc
KEY, PASSWORD = os.environ['AWS_ACCESS_KEY_ID'], os.environ['AWS_SECRET_ACCESS_KEY']
LOCATION = 's3://differential-sdk-test/secrets-test/'
# No AWS credentials outside DuckDB secrets: no environment keys, no profile files, no IMDS.
ENV = {k: v for k, v in os.environ.items() if not k.startswith('AWS_')}
ENV.update(AWS_EC2_METADATA_DISABLED='true', AWS_CONFIG_FILE=str(base / 'none'),
           AWS_SHARED_CREDENTIALS_FILE=str(base / 'none'))
PRELUDE = (f"SET extension_directory='{base / 'extensions'}'; SET secret_directory='{base / 'stored'}';"
           f"LOAD '{EXTENSION}';")
MINIO = (f"CREATE SECRET minio (TYPE s3, KEY_ID '{KEY}', SECRET '{PASSWORD}', ENDPOINT '{host}', "
         f"URL_STYLE 'path', USE_SSL false, SCOPE '{LOCATION}');")
results = []


def passed(name):
    results.append(name)
    print('PASS', name, flush=True)


def run(repository, sql, expected=0, httpfs=True, env=None):
    r = subprocess.run([DUCKDB_CLI, '-init', '/dev/null', '-unsigned', '-batch', '-bail', '-json'],
                       input=PRELUDE + ('LOAD httpfs;' if httpfs else '') + sql, text=True, capture_output=True,
                       env=dict(ENV, DIFFERENTIAL_STORE_ROOT=str(base / repository), **(env or {})), timeout=300)
    assert r.returncode == expected, (sql, r.stdout, r.stderr)
    if expected:
        return r.stderr
    out, text, decoder = [], r.stdout.strip(), json.JSONDecoder()
    while text:
        value, end = decoder.raw_decode(text)
        out.append(value)
        text = text[end:].lstrip()
    return out


run('install', 'INSTALL httpfs; INSTALL aws;', httpfs=False)

# Without a secret (and without AWS credentials) the export fails.
error = run('no-secret', f"ATTACH 'differential:sales' AS sales (DATA_PATH '{LOCATION}no-secret/', ENDPOINT '{endpoint}', "
                         "PATH_STYLE true, ALLOW_HTTP true); CREATE TABLE sales.t AS SELECT 1 AS v;"
                         "CALL differential_create_snapshot('sales');", 1)
assert 'incomplete_export' in error, error
passed('without a secret or AWS credentials, S3 exports fail')

# A scoped s3 secret is all a writer needs: endpoint, path style, HTTP and credentials.
rows = run('writer', MINIO + f"""
    ATTACH 'differential:sales' AS sales (DATA_PATH '{LOCATION}');
    CREATE TABLE sales.t AS SELECT 1 AS v;
    SELECT export_state FROM differential_create_snapshot('sales');
    SELECT endpoint, region, credential_source FROM differential_providers() WHERE data_path LIKE 's3://%';""")
assert rows[-2] == [{'export_state': 'complete'}], rows
assert rows[-1] == [{'endpoint': endpoint, 'region': 'us-east-1', 'credential_source': 'DuckDB secret minio'}], rows
passed('a scoped TYPE s3 secret supplies endpoint, URL style, SSL setting and credentials; no ATTACH options')

# Credentials are looked up in every session; nothing about them is stored.
error = run('writer', "ATTACH 'differential:sales' AS sales; UPDATE sales.t SET v = 2;"
                      "CALL differential_create_snapshot('sales');", 1)
assert 'incomplete_export' in error, error
rows = run('writer', MINIO + "SELECT export_state FROM differential_retry_export('sales');"
                     "ATTACH 'differential:sales' AS sales; UPDATE sales.t SET v = 3;"
                     "SELECT export_state FROM differential_create_snapshot('sales');")
assert rows[-2] == [{'export_state': 'complete'}] and rows[-1] == [{'export_state': 'complete'}], rows
passed('sessions without the secret cannot export; a later session with it retries and continues')

# A replaced secret applies to existing clients in the same session.
r = subprocess.run([DUCKDB_CLI, '-init', '/dev/null', '-unsigned', '-batch', '-json'],
                   input=PRELUDE + f"""LOAD httpfs;
    CREATE SECRET minio (TYPE s3, KEY_ID '{KEY}', SECRET 'wrong', ENDPOINT '{host}', URL_STYLE 'path',
                         USE_SSL false, SCOPE '{LOCATION}');
    ATTACH 'differential:sales' AS sales; UPDATE sales.t SET v = 4;
    SELECT export_state FROM differential_create_snapshot('sales');
    {MINIO.replace('CREATE SECRET', 'CREATE OR REPLACE SECRET')}
    SELECT export_state FROM differential_retry_export('sales');""",
                   text=True, capture_output=True, env=dict(ENV, DIFFERENTIAL_STORE_ROOT=str(base / 'writer')), timeout=300)
assert 'incomplete_export' in r.stderr and r.stdout.rstrip().endswith('[{"export_state":"complete"}]'), (r.stdout, r.stderr)
passed('a wrong secret fails the export; CREATE OR REPLACE SECRET applies to the next request in the same session')

# Readers: a fresh repository with only the secret lists heads and attaches by name.
rows = run('reader', MINIO + f"""
    SELECT database_name, branch_name FROM differential_remote_heads('{LOCATION}');
    ATTACH 'differential:{LOCATION}heads/sales' AS latest;
    SELECT v FROM latest.t;""")
assert rows[-2] == [{'database_name': 'sales', 'branch_name': 'main'}] and rows[-1] == [{'v': 4}], rows
passed('readers use the s3 secret for differential_remote_heads and head/export attachments')

# credential_chain secrets created with REFRESH: the AWS SDK resolves the same (non-default) profile.
profiles = base / 'profiles'
profiles.mkdir()
(profiles / 'credentials').write_text(f'[minio-test]\naws_access_key_id = {KEY}\naws_secret_access_key = {PASSWORD}\n')
(profiles / 'config').write_text('[profile minio-test]\nregion = us-east-1\n')
rows = run('chain', f"""LOAD aws;
    CREATE SECRET chain (TYPE s3, PROVIDER credential_chain, CHAIN 'config', PROFILE 'minio-test', REFRESH auto,
                         ENDPOINT '{host}', URL_STYLE 'path', USE_SSL false, SCOPE '{LOCATION}');
    ATTACH 'differential:chained' AS chained (DATA_PATH '{LOCATION}chain/');
    CREATE TABLE chained.t AS SELECT 1 AS v;
    SELECT export_state FROM differential_create_snapshot('chained');
    SELECT credential_source FROM differential_providers() WHERE data_path LIKE 's3://%';""",
           env={'AWS_CONFIG_FILE': str(profiles / 'config'), 'AWS_SHARED_CREDENTIALS_FILE': str(profiles / 'credentials')})
assert rows[-2] == [{'export_state': 'complete'}] and rows[-1] == [{'credential_source': 'DuckDB secret chain'}], rows
passed('credential_chain secrets with REFRESH use the AWS SDK on their profile (refreshing credentials)')

# TYPE differential secrets: named (SECRET) and the unnamed default; settings apply at creation.
rows = run('lake', MINIO + f"""
    CREATE SECRET lake (TYPE differential, DATA_PATH '{LOCATION}lake/');
    ATTACH 'differential:orders' AS orders (SECRET lake);
    CREATE TABLE orders.t AS SELECT 1 AS v;
    SELECT export_state FROM differential_create_snapshot('orders');
    ATTACH 'differential:local_only' AS local_only;
    CREATE SECRET (TYPE differential, DATA_PATH '{LOCATION}default/');
    ATTACH 'differential:events' AS events;
    CREATE TABLE events.t AS SELECT 1 AS v;
    SELECT export_state FROM differential_create_snapshot('events');
    DETACH local_only; ATTACH 'differential:local_only' AS local_only;
    SELECT database_name, data_path FROM differential_databases() ORDER BY 1;""")
assert rows[-4] == [{'export_state': 'complete'}] and rows[-2] == [{'export_state': 'complete'}], rows
assert rows[-1] == [{'database_name': 'events', 'data_path': f'{LOCATION}default/'},
                    {'database_name': 'local_only', 'data_path': None},
                    {'database_name': 'orders', 'data_path': f'{LOCATION}lake/'}], rows
passed('TYPE differential secrets (named via SECRET, or the unnamed default) set DATA_PATH for new databases only')

# Precedence: explicit option > differential secret > s3 secret > built-in default.
rows = run('precedence', f"""
    CREATE SECRET (TYPE s3, KEY_ID 'k', SECRET 's', REGION 'eu-central-1', SCOPE 's3://differential-sdk-test/p/');
    CREATE SECRET d (TYPE differential, REGION 'eu-west-1', MAX_REQUESTS 2);
    ATTACH 'differential:a' AS a (DATA_PATH 's3://differential-sdk-test/p/a/');
    ATTACH 'differential:b' AS b (DATA_PATH 's3://differential-sdk-test/p/b/', SECRET d);
    ATTACH 'differential:c' AS c (DATA_PATH 's3://differential-sdk-test/p/c/', SECRET d, REGION 'ap-south-1');
    ATTACH 'differential:e' AS e (DATA_PATH 's3://differential-sdk-test/q/');
    SELECT data_path, region, max_requests, credential_source FROM differential_providers()
      WHERE data_path LIKE 's3://%' ORDER BY 1;""")
assert rows[-1] == [
    {'data_path': 's3://differential-sdk-test/p/a/', 'region': 'eu-central-1', 'max_requests': None, 'credential_source': 'DuckDB secret __default_s3'},
    {'data_path': 's3://differential-sdk-test/p/b/', 'region': 'eu-west-1', 'max_requests': 2, 'credential_source': 'DuckDB secret __default_s3'},
    {'data_path': 's3://differential-sdk-test/p/c/', 'region': 'ap-south-1', 'max_requests': 2, 'credential_source': 'DuckDB secret __default_s3'},
    {'data_path': 's3://differential-sdk-test/q/', 'region': 'us-east-1', 'max_requests': None, 'credential_source': 'AWS SDK default chain'}], rows
passed('explicit options beat the differential secret, which beats the s3 secret; unmatched paths use the AWS SDK chain')

# Persistent differential secrets survive restarts; sessions without the extension still start.
run('persistent', f"CREATE PERSISTENT SECRET kept (TYPE differential, DATA_PATH '{LOCATION}kept/');", httpfs=False)
rows = run('persistent', MINIO + "ATTACH 'differential:kept' AS kept (SECRET kept);"
                         "SELECT data_path FROM differential_databases();")
assert rows[-1] == [{'data_path': f'{LOCATION}kept/'}], rows
plain = subprocess.run([DUCKDB_CLI, '-init', '/dev/null', '-batch', '-json'],
                       input=f"SET secret_directory='{base / 'stored'}'; SELECT name, type FROM duckdb_secrets();",
                       text=True, capture_output=True, env=ENV, timeout=60)
assert plain.returncode == 0 and '"kept"' in plain.stdout, (plain.stdout, plain.stderr)
run('persistent', "DROP PERSISTENT SECRET kept;", httpfs=False)
passed('persistent TYPE differential secrets work across sessions and do not break sessions without the extension')

# Errors.
assert 'does not exist' in run('errors', "ATTACH 'differential:x' AS x (SECRET missing);", 1)
assert 'SECRET takes a TYPE differential secret' in run('errors', MINIO + "ATTACH 'differential:x' AS x (SECRET minio);", 1)
assert "Unknown parameter 'key_id'" in run('errors', "CREATE SECRET bad (TYPE differential, KEY_ID 'x');", 1)
assert 'take no SCOPE' in run('errors', "CREATE SECRET bad (TYPE differential, DATA_PATH '/x', SCOPE 'a');", 1)
passed('unknown or wrong-type SECRET, credentials and SCOPE in TYPE differential secrets are refused')

# Credentials are never written: no repository file and no status/listing contains them.
leaked = [str(p) for p in base.rglob('*') if p.is_file() and not {'extensions', 'stored', 'profiles'} & set(p.parts)
          and PASSWORD.encode() in p.read_bytes()]
assert not leaked, leaked
listing = run('writer', MINIO + "FROM differential_providers(); FROM differential_status();")
assert PASSWORD not in json.dumps(listing), listing
passed('secret credentials appear in no repository file, listing or status output')
evidence_path('s3-secrets.json').write_text(json.dumps({'scope': 'disposable pinned MinIO; no real AWS', 'passed': results}, indent=2) + '\n')
