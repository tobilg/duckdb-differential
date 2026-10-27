# Third-party dependencies

This repository's source license is in LICENSE. Dependencies are not re-licensed.

| Dependency | Version/revision | License |
| --- | --- | --- |
| DuckDB (submodule, built from source) | v2.0.0 (untagged) / d54cbbe3df17e8e5ce68312f483b4155836112aa | MIT |
| DuckDB extension-ci-tools (submodule, build tooling) | `v2.0-cyanoptera` branch | MIT |
| AWS SDK for C++ (`s3` component) with its AWS CRT and curl dependencies | 1.11.820 (vcpkg) | Apache-2.0 (curl: curl license) |
| SQLite | 3.53.2 (vcpkg) | Public domain / blessing |
| OpenSSL | 3.6.3 (vcpkg) | Apache-2.0 |
| MinIO (external disposable test endpoint only) | RELEASE.2025-10-15T17-29-55Z (`ghcr.io/coollabsio/minio`, pinned by digest) | AGPLv3 |

Library versions are those of vcpkg commit `cd61e1e26a038e82d6550a3ebbe0fbbfe7da78e3`
(`VCPKG_COMMIT` in the `Makefile`), declared in `vcpkg.json`. They are linked statically into
the extension; the built artifact exports only its DuckDB entry point (`-exported_symbol` on
macOS, hidden visibility and `--exclude-libs,ALL` on Linux). MinIO is a separate test-only
container and is not bundled. A final redistribution/license audit remains a release gate.
