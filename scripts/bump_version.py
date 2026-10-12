#!/usr/bin/env python3
"""Set the extension version everywhere it is recorded: make bump-version V=X.Y.Z"""
import re
import sys
from pathlib import Path

VERSION = r"(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)"
VERSION_LOCATIONS = (
    ("CMakeLists.txt", re.compile(rf"project\(\$\{{TARGET_NAME\}} VERSION (?P<version>{VERSION})\)")),
    ("extension_config.cmake", re.compile(rf"EXTENSION_VERSION v(?P<version>{VERSION})")),
    ("extension_config_integration.cmake", re.compile(rf"EXTENSION_VERSION v(?P<version>{VERSION})")),
    ("src/differential_extension.cpp", re.compile(rf'return "v(?P<version>{VERSION})";')),
    ("test/sql/differential.test", re.compile(rf"^v(?P<version>{VERSION})$", re.M)),
    ("community/description.yml", re.compile(rf"^  version: (?P<version>{VERSION})$", re.M)),
)


def fail(message):
    print(f"error: {message}", file=sys.stderr)
    raise SystemExit(2)


def main():
    if len(sys.argv) != 2 or re.fullmatch(VERSION, sys.argv[1]) is None:
        fail("version must use X.Y.Z format (for example, 0.2.0)")
    new_version = sys.argv[1]
    root = Path(__file__).resolve().parent.parent
    files = {}
    for relative, pattern in VERSION_LOCATIONS:
        path = root / relative
        content = path.read_text()
        matches = list(pattern.finditer(content))
        if len(matches) != 1:
            fail(f"expected exactly one version reference in {relative}, found {len(matches)}")
        files[path] = (content, matches[0])
    current = {match.group("version") for _, match in files.values()}
    if len(current) != 1:
        details = ", ".join(f"{p.relative_to(root)}={m.group('version')}" for p, (_, m) in files.items())
        fail(f"version references are inconsistent: {details}")
    current = current.pop()
    if current == new_version:
        print(f"Extension version is already {new_version}; all references are consistent.")
        return
    for path, (content, match) in files.items():
        start, end = match.span("version")
        path.write_text(content[:start] + new_version + content[end:])
        print(f"{path.relative_to(root)}: {current} -> {new_version}")


if __name__ == "__main__":
    main()
