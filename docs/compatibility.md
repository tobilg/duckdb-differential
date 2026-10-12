# Compatibility and verification

| Component | Identity and observed result |
| --- | --- |
| DuckDB | v1.5.6 (`069cc9f9b5`), the community repository's current stable; built from the `duckdb/` submodule |
| Build | DuckDB C++ extension template with `extension-ci-tools` (`v1.5-variegata`), `EXTENSION_STATIC_BUILD=1` |
| Dependencies | vcpkg `cd61e1e26a03`: AWS SDK for C++ 1.11.820 (`s3`), SQLite 3.53.2, OpenSSL 3.6.3, statically linked; the artifact exports only `differential_duckdb_cpp_init` |
| macOS arm64 | macOS 26.7, Apple clang 21, CMake 4.1.2, Ninja. `make release`, `make test` (SQL logic tests), `make integration`, `make test_integration`, `make test_minio`: all pass |
| macOS amd64 | Built by CI only (`osx_amd64`); not run locally |
| Linux amd64 / arm64 | NOT YET BUILT. All non-AWS sources pass a syntax-only g++ 13 compile (Ubuntu 24.04) against DuckDB 1.5.6 headers; the first full build and test runs are the CI workflows |
| Windows, wasm | Not supported (POSIX file locking, `fsync`, `mmap`); excluded from all builds |
| Official DuckDB extensions in the same process | `aws`, `httpfs` and `sqlite` (each bundling its own SDK/OpenSSL/SQLite) loaded before and after `differential`; S3 used through both SDK copies (`coexistence.py`, macOS arm64) |
| MinIO | RELEASE.2025-09-07T16-13-09Z, image digest `sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`; SDK transport, end-to-end export/read/recovery and branch-head tests pass |
| DuckDB secrets | `TYPE s3` secrets from the official `httpfs` (config keys and `credential_chain`, with and without `REFRESH`, via the official `aws`) and `TYPE differential` secrets, temporary and persistent; MinIO only (`s3_secrets.py`) |
| Shared repositories (several local users) | File and directory modes verified under umask `000`/`077` on macOS (`shared_repository.py`); access by a second OS account and Linux untested |
| AWS S3 | Owner-run on us-east-1 (2026-10-11) from macOS arm64 over the internet with a named profile: exports, cold attach and full scans (`benchmarks/s3_throughput.py`, no SDK retries), branch-head attach and listing. Not run on AWS: the capability check (`existing_provider.py`), fault behaviour, `TYPE s3` secrets, caching |
| Other DuckDB releases | Not built; see [UPDATING.md](UPDATING.md) |

No existing provider data is used. HTTP protocol fault tests are distinct from provider
compatibility tests. Fresh-reader recovery runs in a different process/repository on the
same physical host; an actual second-host test remains unrun.

**Artifacts.** `make release` produces the hooks-free loadable
`build/release/extension/differential/differential.duckdb_extension` (with DuckDB statically
linked in, like DuckDB's own extensions; 40 MB on macOS arm64) and a CLI/unittest with the
extension linked in. `make integration` produces the test build: hooks compiled in, an
extension-free CLI and `libduckdb`, the C++ unit tests and the SDK probe. Community
builds are signed by DuckDB; local builds are unsigned and need `-unsigned`.

**Durability by platform.** At every native sync, publication is one SQLite transaction with
`synchronous=FULL`. On macOS SQLite's `fullfsync` and the store's directory syncs use
`F_FULLFSYNC`, which also flushes the device's write cache (stronger than stock DuckDB on macOS,
which uses `fsync`). On Linux the same points use `fsync`/`fdatasync`, matching native DuckDB on
Linux; whether the device cache is flushed then depends on the filesystem and device (write
barriers). Power-loss behaviour is untested on both.

There is exactly one local store format (1: paged manifests, write journal, durable inode
allocator, collection queues), one control format (1) and one export format
(`differential-export-v1`, packs and `DIX1` indexes). Repositories and exports in any other
format are refused with `incompatible_format`; there are no migrations (`write_path.py` checks
the refusal). Descriptors record the writing DuckDB version; readers accept any version line,
and DuckDB decides at attach time whether it can open the native file (`s3_tiering.py` attaches
and recovers an export labelled `duckdb-v9.9.9`). Compaction is described in
[architecture.md](architecture.md).

Native SQL has upstream 1.5.6 semantics and limitations. External files, credentials,
native extension binaries and session state are not included in snapshots. Explicitly
qualified catalog references are not rewritten for clones. Cross-database writes retain
upstream transaction limitations. Disk-full tests inject SQLite's FULL result; physical
power-loss/torn-sector behavior has not been emulated or certified.
