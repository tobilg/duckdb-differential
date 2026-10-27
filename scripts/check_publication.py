#!/usr/bin/env python3
"""Reject known private notes and generated artifacts in the publishable source tree.

Checks the checkout, including pending additions and deletions, without changing the
index. This is a publication boundary check, not a secret scanner or a history scrub.
"""
import fnmatch
from pathlib import Path, PurePosixPath
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
PRIVATE_DIRS = {'build', 'Build', 'vcpkg_installed', 'ci-artifacts', 'plans', 'adrs', 'internal'}
PRIVATE_DOCS = {'implementation-status.md', 'merge-release-readiness.md', 'feasibility.md', 'security-review.md'}
GENERATED_SUFFIXES = ('.log', '.pyc', '.o', '.obj', '.so', '.dylib', '.dll', '.duckdb_extension')


def private_path(name):
    path = PurePosixPath(name)
    return (path.parts[0] in PRIVATE_DIRS or path.parts[0].startswith('build-') or
            '__pycache__' in path.parts or name.startswith('docs/internal/') or
            (path.parent == PurePosixPath('docs') and path.name in PRIVATE_DOCS) or
            (path.parent == PurePosixPath('benchmarks') and fnmatch.fnmatchcase(path.name, '*-results*.json')) or
            name.endswith(GENERATED_SUFFIXES))


def main():
    names = subprocess.check_output(['git', 'ls-files', '-z', '--cached', '--others', '--exclude-standard'], cwd=ROOT)
    # A local pending deletion is safe; the commit/CI checkout will have no entry.
    violations = sorted({name for name in names.decode().split('\0')
                         if name and private_path(name) and
                         ((ROOT / name).exists() or (ROOT / name).is_symlink())})
    if violations:
        print('Private/generated files must not be committed:', file=sys.stderr)
        for name in violations: print('  ' + name, file=sys.stderr)
        return 1
    print('PASS publication boundaries: no known private/generated files in source inputs')
    return 0


if __name__ == '__main__':
    sys.exit(main())
