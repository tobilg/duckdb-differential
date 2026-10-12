#!/usr/bin/env python3
"""S3 layout: per-database data pools (dbs/<uuid>/packs|indexes), commit records in a newest-first
index (dbs/<uuid>/snapshots/<9999999999 - n>), branch heads (dbs/<uuid>/branches/<name>), the
names index (names/<name>/<uuid>) and pool-qualified pack references (a clone's export reuses its
source database's packs). The bucket is inspected with DuckDB's official httpfs. Needs the
disposable MinIO bucket initialized by minio.py and network access for httpfs."""
import json
import os
from pathlib import Path
import subprocess
import sys
from urllib.parse import urlparse
from evidence import evidence_path
from paths import DUCKDB_CLI, EXTENSION

endpoint, base = sys.argv[1:]
base = Path(base) / 'layout'
base.mkdir()
LOCATION = 's3://differential-sdk-test/layout-test/'
S3 = f"ENDPOINT '{endpoint}', PATH_STYLE true, ALLOW_HTTP true"
HTTPFS = (f"SET extension_directory='{base / 'extensions'}'; LOAD httpfs; CREATE SECRET (TYPE s3, KEY_ID "
          f"'{os.environ['AWS_ACCESS_KEY_ID']}', SECRET '{os.environ['AWS_SECRET_ACCESS_KEY']}', ENDPOINT "
          f"'{urlparse(endpoint).netloc}', URL_STYLE 'path', USE_SSL false, REGION 'us-east-1');")
results = []


def passed(name):
    results.append(name)
    print('PASS', name, flush=True)


def run(repository, sql, expected=0):
    r = subprocess.run([DUCKDB_CLI, '-init', '/dev/null', '-unsigned', '-batch', '-bail', '-json'],
                       input=f"LOAD '{EXTENSION}';" + sql, text=True, capture_output=True,
                       env=dict(os.environ, DIFFERENTIAL_STORE_ROOT=str(base / repository)), timeout=300)
    assert r.returncode == expected, (sql, r.stdout, r.stderr)
    out, text, decoder = [], r.stdout.strip(), json.JSONDecoder()
    while text:
        value, end = decoder.raw_decode(text)
        out.append(value)
        text = text[end:].lstrip()
    return out


def keys():
    rows = run('inspect', HTTPFS + f"SELECT file FROM glob('{LOCATION}**') ORDER BY file;")[-1]
    return [row['file'][len(LOCATION):] for row in rows]


def text(key):
    return run('inspect', HTTPFS + f"SELECT content FROM read_text('{LOCATION}{key}');")[-1][0]['content']


run('install', "SET extension_directory='" + str(base / 'extensions') + "'; INSTALL httpfs;")
rows = run('writer', f"""
    ATTACH 'differential:sales' AS sales (DATA_PATH '{LOCATION}', {S3});
    CREATE TABLE sales.t AS SELECT range AS id, sha256(range::VARCHAR) AS payload FROM range(100000);
    CALL differential_create_snapshot('sales');
    UPDATE sales.t SET payload = 'x' WHERE id = 1;
    CALL differential_create_snapshot('sales', name => 'second', message => 'line one\nline two');
    CALL differential_create_branch('sales', 'dev');
    SELECT database_id, snapshot_id, export_locator FROM differential_snapshots('sales') ORDER BY snapshot_id;""")
snapshots = rows[-1]
db = snapshots[0]['database_id']
found = keys()
assert not [k for k in found if k.startswith(('pools/', 'heads/'))], found
assert f'names/sales/{db}' in found, found
assert f'dbs/{db}/branches/main' in found and f'dbs/{db}/branches/dev' in found, found
assert any(k.startswith(f'dbs/{db}/packs/') for k in found) and any(k.startswith(f'dbs/{db}/indexes/') for k in found)
passed('exports write the names index, branch heads and data under dbs/<database uuid>/; no pools/ or heads/ keys')

commits = sorted(k for k in found if k.startswith(f'dbs/{db}/snapshots/'))
assert commits == [f'dbs/{db}/snapshots/9999999997', f'dbs/{db}/snapshots/9999999998'], commits  # newest first
record = text(commits[0])
assert record.startswith('differential-commit-v1\n') and '\nsnapshot 2\n' in record and '\nparent 1\n' in record, record
assert f"\ndescriptor {snapshots[1]['export_locator']}\n" in record and '\nbranch_name main\n' in record, record
assert '\nmessage line one%0Aline two\n' in record and '\nname second\n' in record, record
assert '\nparent 0\n' in text(commits[1]), text(commits[1])
passed('commit records list newest first (inverted snapshot numbers) and link each snapshot to its parent')

# A clone is a new database: its export reuses the source's packs through pool-qualified references.
rows = run('writer', f"""
    SELECT database_id FROM differential_clone_database('sales', 'copy');
    SELECT metric, value FROM differential_storage_stats() WHERE metric IN ('export_packs_uploaded', 'export_chunks_reused');
    SELECT export_state, export_locator FROM differential_snapshots('copy');""")
clone = rows[0][0]['database_id']
stats = {row['metric']: row['value'] for row in rows[1]}
assert stats['export_packs_uploaded'] == 0 and stats['export_chunks_reused'] > 0, stats
assert rows[2][0]['export_state'] == 'complete', rows[2]
found = keys()
assert f'names/copy/{clone}' in found and not any(k.startswith(f'dbs/{clone}/packs/') for k in found), found
value = run('reader', f"ATTACH 'differential:{LOCATION}heads/copy' AS c ({S3}); SELECT payload FROM c.t WHERE id = 1;")[-1]
assert value == [{'payload': 'x'}], value
passed("a clone's export references its source's packs (no new packs) and reads back across pools")
evidence_path('s3-layout.json').write_text(json.dumps({'scope': 'disposable pinned MinIO; no real AWS', 'passed': results}, indent=2) + '\n')
