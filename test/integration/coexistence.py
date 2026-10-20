#!/usr/bin/env python3
"""One process loads differential together with DuckDB's official aws, httpfs and sqlite
extensions, which bundle their own AWS SDK, OpenSSL and SQLite. Both load orders are tested,
and S3 is used through both SDK copies. Needs network access to extensions.duckdb.org and the
disposable MinIO endpoint started by minio.py."""
import json
import os
from pathlib import Path
import subprocess
import sys
from urllib.parse import urlparse
from evidence import evidence_path
from paths import DUCKDB_CLI, EXTENSION

endpoint, tmp = sys.argv[1:]
folder = Path(tmp) / 'coexistence'
folder.mkdir()
host = urlparse(endpoint).netloc
OURS = f"LOAD '{EXTENSION}';"
OFFICIAL = "LOAD httpfs; LOAD aws; LOAD sqlite;"
results = []


def run(sql, repository):
    r = subprocess.run([DUCKDB_CLI, '-init', '/dev/null', '-unsigned', '-batch', '-bail', '-json'],
                       input=f"SET extension_directories=['{folder / 'extensions'}'];" + sql, text=True,
                       capture_output=True, env=dict(os.environ, DIFFERENTIAL_STORE_ROOT=str(repository)), timeout=300)
    assert r.returncode == 0, (sql, r.stdout, r.stderr)
    out, text, decoder = [], r.stdout.strip(), json.JSONDecoder()
    while text:
        value, end = decoder.raw_decode(text)
        out.append(value)
        text = text[end:].lstrip()
    return out


run("INSTALL httpfs; INSTALL aws; INSTALL sqlite;", folder / 'install')
for order, loads in (('differential first', OURS + OFFICIAL), ('official first', OFFICIAL + OURS)):
    tag = order.split()[0]
    location = f's3://differential-sdk-test/coexistence-{tag}/'
    s3 = f"ENDPOINT '{endpoint}', PATH_STYLE true, ALLOW_HTTP true, REGION 'us-east-1'"
    blocks = run(loads + f"""
        SELECT extension_name FROM duckdb_extensions()
          WHERE loaded AND extension_name IN ('aws','httpfs','sqlite_scanner','differential') ORDER BY 1;
        -- differential: export through its own AWS SDK copy
        ATTACH 'differential:co' AS co (DATA_PATH '{location}', {s3});
        CREATE TABLE co.t AS SELECT range AS i FROM range(1000);
        SELECT export_state, export_locator FROM differential_create_snapshot('co');
        -- aws + httpfs: the official SDK copy reads the same environment credentials
        CREATE SECRET minio (TYPE s3, PROVIDER credential_chain, CHAIN 'env', ENDPOINT '{host}',
                             URL_STYLE 'path', USE_SSL false, REGION 'us-east-1');
        COPY (SELECT 42 AS answer) TO 's3://differential-sdk-test/coexistence-{tag}-httpfs/a.parquet';
        SELECT answer FROM 's3://differential-sdk-test/coexistence-{tag}-httpfs/a.parquet';
        -- sqlite_scanner: its own SQLite next to the differential authority
        ATTACH '{folder / (tag + '.sqlite')}' AS lite (TYPE sqlite);
        CREATE TABLE lite.t AS SELECT 7 AS v;
        SELECT v FROM lite.t;
        -- differential keeps working after the others ran
        UPDATE co.t SET i = i + 1;
        SELECT snapshot_id, export_state FROM differential_create_snapshot('co');
    """, folder / f'repo-{tag}')
    loaded = [r['extension_name'] for r in blocks[0]]
    assert loaded == ['aws', 'differential', 'httpfs', 'sqlite_scanner'], loaded
    export = blocks[1][0]
    assert export['export_state'] == 'complete', export
    assert blocks[2] == [{'Success': True}], blocks  # CREATE SECRET
    assert blocks[3] == [{'answer': 42}] and blocks[4] == [{'v': 7}], blocks
    assert blocks[5] == [{'snapshot_id': 2, 'export_state': 'complete'}], blocks[5]
    # A fresh process attaches the export by locator with all extensions loaded.
    read = run(loads + f"""
        ATTACH 'differential:{location}{export['export_locator']}' AS old ({s3});
        SELECT count(*) AS n, sum(i)::BIGINT AS total FROM old.t;""", folder / f'reader-{tag}')
    assert read[-1] == [{'n': 1000, 'total': 499500}], read
    results.append(order)
    print(f'PASS coexistence ({order}): differential S3 export and attach, aws credential_chain + httpfs '
          f'parquet round trip, sqlite_scanner, in one process', flush=True)
evidence_path('coexistence.json').write_text(json.dumps({'passed': results, 'endpoint': 'disposable MinIO'}, indent=2) + '\n')
