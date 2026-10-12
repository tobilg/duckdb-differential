#!/usr/bin/env python3
"""Runs the SQL examples of docs/sql-reference.md and docs/shared-databases.md as written, against
the disposable MinIO bucket initialized by minio.py (network access for httpfs).

Each ```sql block is one session (one CLI process) on the host named by its first line
("-- main", "-- host A", "-- reader", ...), in document order; every host has its own repository.
Blocks whose first line starts with "-- not run" or "-- every host" (installation and credentials,
which the runner provides) are skipped. "-- expect error: <text>" as the second line means the
block must fail with that text; "-- expect output: <text>" lines must appear in its JSON output. Substitutions: s3://my-bucket/lake/ becomes a fresh MinIO prefix
per document, '/absolute/repository' the host's repository and /data/ a scratch directory."""
import json
import os
from pathlib import Path
import re
import subprocess
import sys
from urllib.parse import urlparse
from evidence import evidence_path
from paths import DUCKDB_CLI, EXTENSION

ROOT = Path(__file__).resolve().parents[2]
endpoint, base = sys.argv[1:]
base = Path(base) / 'doc-examples'
base.mkdir()
extensions = base / 'extensions'
SECRET = (f"SET extension_directory='{extensions}'; LOAD httpfs; CREATE SECRET docs_minio (TYPE s3, KEY_ID "
          f"'{os.environ['AWS_ACCESS_KEY_ID']}', SECRET '{os.environ['AWS_SECRET_ACCESS_KEY']}', ENDPOINT "
          f"'{urlparse(endpoint).netloc}', URL_STYLE 'path', USE_SSL false, REGION 'us-east-1', "
          "SCOPE 's3://differential-sdk-test/');")
results = []


def session(repository, sql):
    return subprocess.run([DUCKDB_CLI, '-init', '/dev/null', '-unsigned', '-batch', '-bail', '-json'],
                          input=f"LOAD '{EXTENSION}';" + SECRET + sql, text=True, capture_output=True, timeout=300,
                          env=dict(os.environ, DIFFERENTIAL_STORE_ROOT=str(repository)))


setup = subprocess.run([DUCKDB_CLI, '-init', '/dev/null', '-batch', '-bail'],
                       input=f"SET extension_directory='{extensions}'; INSTALL httpfs;", text=True,
                       capture_output=True, timeout=300)
assert setup.returncode == 0, setup.stderr
for document in ('docs/sql-reference.md', 'docs/shared-databases.md'):
    name = Path(document).stem
    location = f's3://differential-sdk-test/docs-{name}/'
    data = base / name / 'data'
    data.mkdir(parents=True)
    blocks = re.findall(r'```sql\n(.*?)```', (ROOT / document).read_text(), re.S)
    executed = skipped = 0
    for number, block in enumerate(blocks, 1):
        lines = block.splitlines()
        label = lines[0][3:].strip() if lines and lines[0].startswith('-- ') else ''
        if not label or label.startswith('not run') or label.startswith('every host'):
            skipped += 1
            continue
        expect = lines[1][len('-- expect error:'):].strip() if len(lines) > 1 and \
            lines[1].startswith('-- expect error:') else None
        output = [line[len('-- expect output:'):].strip() for line in lines if line.startswith('-- expect output:')]
        repository = base / name / re.sub(r'[^a-z0-9]+', '-', label.lower())
        sql = (block.replace('s3://my-bucket/lake/', location)
               .replace("'/absolute/repository'", f"'{repository}'")
               .replace("'/data/", f"'{data}/"))
        r = session(repository, sql)
        context = f'{document} block {number} ({label})'
        if expect is None:
            assert r.returncode == 0, (context, sql, r.stdout, r.stderr)
            for text in output:
                assert text in r.stdout, (context, text, r.stdout)
        else:
            assert r.returncode != 0 and expect in r.stderr, (context, sql, r.stdout, r.stderr)
        executed += 1
    assert executed, document
    results.append({'document': document, 'executed_blocks': executed, 'skipped_blocks': skipped})
    print(f'PASS {document}: {executed} SQL blocks executed as written ({skipped} not run)', flush=True)
evidence_path('doc-examples.json').write_text(json.dumps({'scope': 'disposable pinned MinIO; one host',
                                                          'results': results}, indent=2) + '\n')
