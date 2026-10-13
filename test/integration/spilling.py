#!/usr/bin/env python3
"""Early spill policy checks and real out-of-core execution on local storage."""
import json
import os
from pathlib import Path
import subprocess
import tempfile
from evidence import evidence_path
from paths import DUCKDB_CLI, EXTENSION

results = []
with tempfile.TemporaryDirectory(prefix='differential-spill-') as tmp:
    root = Path(tmp)
    def run(sql, expected=0):
        result = subprocess.run([DUCKDB_CLI, '-init', '/dev/null', '-unsigned', '-batch', '-bail', '-json'],
                                input=sql, text=True, capture_output=True, timeout=90,
                                env=dict(os.environ, DIFFERENTIAL_STORE_ROOT=str(root / 'repo')))
        assert result.returncode == expected, (result.stdout, result.stderr)
        return result
    # Configured before LOAD: ATTACH rejects the path before opening the repository.
    for directory in ('diff://spill', 's3://unused/spill', 'differential:spill'):
        error = run(f"SET temp_directory='{directory}'; LOAD '{EXTENSION}'; ATTACH 'differential:test' AS d;", 1)
        assert 'local_temp_directory_required' in error.stderr, error.stderr
        assert not (root / 'repo').exists()
    results.append('preexisting nonlocal temp_directory rejected at ATTACH before repository creation')
    # Normal local spill must actually evict buffers, then complete a sort correctly.
    output = run(f"""LOAD '{EXTENSION}'; SET threads=1; SET preserve_insertion_order=false;
        SET temp_directory='{root / 'spill'}'; ATTACH 'differential:test' AS d;
        SET memory_limit='16MB';
        CREATE TEMP TABLE spilled AS SELECT i, hash(i) AS value FROM range(1000000) t(i);
        SELECT count(*) > 0 AS spilled FROM duckdb_temporary_files();
        SELECT count(*) AS n, sum(i)::BIGINT AS total FROM (SELECT * FROM spilled ORDER BY value);
        """)
    decoder = json.JSONDecoder()
    text, blocks = output.stdout.strip(), []
    while text:
        value, end = decoder.raw_decode(text)
        blocks.append(value)
        text = text[end:].lstrip()
    assert blocks == [[{'spilled': True}], [{'n': 1000000, 'total': 499999500000}]], blocks
    results.append('local spill files created under 16 MB; external sort preserves one million rows')
    assert not list((root / 'spill').glob('*')), 'spill files were not cleaned up'
    results.append('native spill cleanup removes temporary files')
for result in results:
    print('PASS', result)
evidence_path('spilling.json').write_text(json.dumps(results, indent=2) + '\n')
