#!/usr/bin/env python3
"""Actual process death inside SQLite payload/map/length transactions and the write journal."""
import os
from pathlib import Path
import subprocess
import tempfile
ROOT=Path(__file__).resolve().parents[2]
import sys as _sys
_sys.path.insert(0,str(ROOT/'test/integration'))
from paths import DUCKDB_CLI, EXTENSION, DUCKDB_LIBRARY, BUILD_DIR, TEST_BINARIES

exe=TEST_BINARIES/'store_test'
points=['object_inserted','mapping_updated','length_updated','before_commit','after_commit']
for operation,boundary in [('rename','namespace_updated'),('remove','namespace_updated'),('seal','manifest_inserted'),('truncate','length_updated')]:
    points.extend(operation+':'+point for point in (boundary,'before_commit','after_commit'))
# Completed writes survive process death through the journal; a torn journal replays only
# the valid prefix; a lost (unsynced) journal falls back to the last published generation.
points+=['unsynced','torn']+['lost:'+p for p in ('before_commit','after_commit','truncate:before_commit','truncate:after_commit')]
# Two databases committed in one group-commit transaction: both or neither.
points+=['group:group_applied','group:after_commit','lost:group:group_applied','lost:group:after_commit']
for point in points:
    with tempfile.TemporaryDirectory(prefix='differential-map-crash-') as folder:
        result=subprocess.run([str(exe),'--crash',folder,point],timeout=20)
        assert result.returncode==86,(point,result.returncode)
        subprocess.run([str(exe),'--recover',folder,point],check=True,timeout=20)
        print('PASS',point,'recovered complete generation')
