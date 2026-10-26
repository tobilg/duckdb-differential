#!/usr/bin/env python3
"""Stock-host integration suites against `make integration` (no network, no containers).

The host CLI and libduckdb do not link the extension; every suite LOADs the loadable artifact."""
import os
from pathlib import Path
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'test/integration'))
from paths import DUCKDB_CLI, EXTENSION

SUITES = [
    ('phase0.py', ['--duckdb', DUCKDB_CLI], {}),
    ('phase0.py', ['--duckdb', DUCKDB_CLI, '--durable'], {}),
    ('store_crash.py', [], {}),
    ('native_matrix.py', [], {}),
    ('spilling.py', [], {}),
    ('qualified_catalog.py', [], {}),
    ('checkpoint_boundaries.py', [], {}),
    ('block_reuse.py', [], {}),
    ('uri_status.py', [], {}),
    ('observability.py', [], {}),
    ('lifecycle.py', [], {}),
    ('logical_merge.py', [], {}),
    ('merge_types.py', [], {}),
    ('merge_hardening.py', [], {}),
    ('merge_lifecycle.py', [], {}),
    ('merge_schema.py', [], {}),
    ('merge_tables.py', [], {}),
    ('merge_catalog.py', [], {}),
    ('merge_catalog_safety.py', [], {}),
    ('merge_columns.py', [], {}),
    ('merge_tables_spill.py', [], {}),
    ('merge_native_spill.py', [], {}),
    ('merge_scale_verifier.py', [], {}),
    ('merge_branches.py', [], {}),
    ('merge_target.py', [], {}),
    ('merge_preflight.py', [], {}),
    ('merge_diagnostics.py', [], {}),
    ('merge_compatibility.py', [], {}),
    ('merge_qualification.py', [], {}),
    ('merge_ancestry.py', [], {}),
    ('merge_resolutions.py', [], {}),
    ('lifecycle_crash.py', [], {}),
    ('lifecycle_crash.py', [], {'DIFFERENTIAL_TEST_LOCAL_POOL': '1'}),
    ('lifecycle_crash.py', [], {'DIFFERENTIAL_TEST_RETIRE_TARGET': '1'}),
    ('management_races.py', [], {}),
    ('local_pools.py', [], {}),
    ('write_path.py', [], {}),
    ('sql_surface.py', [], {}),
    ('time_travel.py', [], {}),
    ('import_native.py', [], {}),
    ('cancellation.py', [], {}),
    ('snapshot_concurrency.py', [], {}),
    ('retention.py', [], {}),
    ('sidecars.py', [], {}),
    ('sidecars.py', [], {'DIFFERENTIAL_TEST_DURABLE': '1'}),
    ('provider_configuration.py', [], {}),
    ('pending_deletions.py', [], {}),
    ('shared_repository.py', [], {}),
]


def main():
    if not EXTENSION.exists() or not Path(DUCKDB_CLI).exists():
        sys.exit(f'Missing {EXTENSION} or {DUCKDB_CLI}: run `make integration` first')
    started = time.monotonic()
    for script, arguments, extra in SUITES:
        label = ' '.join([script, *arguments[2:], *(f'{k}={v}' for k, v in extra.items())])
        print(f'=== {label}', flush=True)
        subprocess.run([sys.executable, str(ROOT / 'test/integration' / script), *arguments],
                       env=dict(os.environ, **extra), cwd=ROOT, check=True)
    print(f'PASS {len(SUITES)} integration suites in {time.monotonic() - started:.0f}s', flush=True)


if __name__ == '__main__':
    main()
