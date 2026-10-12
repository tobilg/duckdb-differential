"""Build artifacts used by the Python suites (template layout, see the Makefile).

`make integration` builds an extension-free DuckDB CLI and libduckdb plus the loadable
extension with test hooks; `make release` builds the hooks-free loadable. Override with
DIFFERENTIAL_BUILD_DIR (a build directory), DIFFERENTIAL_EXTENSION, DUCKDB_BINARY and
DUCKDB_LIBRARY."""
import os
import platform
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
BUILD_DIR = Path(os.environ.get('DIFFERENTIAL_BUILD_DIR', str(ROOT / 'build/integration')))
EXTENSION = Path(os.environ.get('DIFFERENTIAL_EXTENSION',
                                str(BUILD_DIR / 'extension/differential/differential.duckdb_extension')))
# The host never links the extension: LOAD must exercise the loadable artifact.
DUCKDB_CLI = os.environ.get('DUCKDB_BINARY', str(ROOT / 'build/integration/duckdb'))
DUCKDB_LIBRARY = os.environ.get('DUCKDB_LIBRARY', str(
    ROOT / 'build/integration/src' / ('libduckdb.dylib' if platform.system() == 'Darwin' else 'libduckdb.so')))
# C++ unit tests and the SDK probe (`make integration` builds them with the hooks).
TEST_BINARIES = Path(os.environ.get('DIFFERENTIAL_TEST_BINARIES', str(ROOT / 'build/integration/extension/differential')))
