# Compatibility and verification

| Component | Identity and observed result |
| --- | --- |
| DuckDB | v2.0.0 as the community repository builds it before the tag: untagged commit `d54cbbe3df`; built from the `duckdb/` submodule. 1.5.6 is no longer built |
| Build | DuckDB C++ extension template with `extension-ci-tools` (`v2.0-cyanoptera`), `EXTENSION_STATIC_BUILD=1`; `OVERRIDE_GIT_DESCRIBE` names the untagged pre-release |
| Dependencies | vcpkg `cd61e1e26a03`: AWS SDK for C++ 1.11.820 (`s3`), SQLite 3.53.2, OpenSSL 3.6.3, statically linked; the artifact exports only `differential_duckdb_cpp_init` |
| macOS arm64 | DuckDB 1.5.6 only: macOS 26.7, Apple clang 21, CMake 4.1.2, Ninja; `make release`, `make test`, `make integration`, `make test_integration`, `make test_minio` passed. Not yet rerun on DuckDB 2.0 |
| macOS amd64 | DuckDB 2.0 at `d54cbbe3df` (2026-10-21): macOS 15.8.1, Apple clang 17, CMake 4.4.4, Ninja. `make release`, `make test` (8 SQL test files), `make integration`, `make test_integration` (7 C++ tests, 28 suites), the MinIO suites (`test_minio`), `make test_upstream_harness`, and the upstream SQLLogicTests added or changed since the previous pin (510 files) with their exclusions audited: all pass. The full upstream comparison and exclusion audit last ran at the previous pre-release commit `b15358c371` |
| Linux amd64 | DuckDB 1.5.6 only: integration build, 6 C++ tests and 27 local integration suites passed in [CI run 38291321428](https://github.com/tobilg/duckdb-differential/actions/runs/38291321428/job/114951862323) (Ubuntu 24.04); MinIO image pull failed before S3 tests, which remain unverified on Linux |
| Linux arm64 | Not yet verified; on DuckDB 1.5.6 all non-AWS sources passed a syntax-only g++ 13 compile (Ubuntu 24.04) |
| Windows, wasm | Not supported (POSIX file locking, `fsync`, `mmap`); excluded from all builds |
| Official DuckDB extensions in the same process | `aws`, `httpfs` and `sqlite` (each bundling its own SDK/OpenSSL/SQLite) loaded before and after `differential`; S3 used through both SDK copies (`coexistence.py`, macOS arm64) |
| MinIO | RELEASE.2025-10-15T17-29-55Z, upstream source built by [coollabsio/minio](https://github.com/coollabsio/minio), image `ghcr.io/coollabsio/minio@sha256:69b55a1c1c5dc285ce04db96689f5b2102317fc77a50680a1874ca6efd1c87f9` (linux/amd64 and linux/arm64). Anonymous manifest lookup, pull and full `make test_minio` pass (macOS arm64, 2026-10-13; 105 PASS lines); hosted CI with this image remains unrun |
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
CLI and `libduckdb` without Differential linked in, the C++ unit tests, the upstream SQLLogicTest runner and the SDK probe. Community
builds are signed by DuckDB; local builds are unsigned and need `-unsigned`.

**Durability by platform.** At every native sync, publication syncs the file group's segment and
then commits one SQLite transaction with `synchronous=FULL`. On every OS, macOS included, SQLite and the store's own syncs use
`fsync`/`fdatasync`, matching native DuckDB (no `F_FULLFSYNC` on macOS, which native DuckDB does
not use either). Whether the device's write cache is flushed then depends on the filesystem and
device (on macOS, `fsync` does not flush it). Power-loss behaviour is untested.

There is exactly one local store format (2: 256 KiB block-aligned chunks, log-file byte ranges,
paged manifests, append-only segments holding the write journal and payload, CRC32C-verified local
reads, durable inode allocator, collection queues), one control format (1) and one export format
(`differential-export-v2`, packs of 256 KiB chunks and `DIX2` indexes). Repositories and exports in any other
format are refused with `incompatible_format`; there are no migrations (`write_path.py` checks
the refusal). Descriptors record the writing DuckDB version; readers accept any version line,
and DuckDB decides at attach time whether it can open the native file (`s3_tiering.py` attaches
and recovers an export labelled `duckdb-v9.9.9`). Compaction is described in
[architecture.md](architecture.md).

Native SQL has upstream DuckDB 2.0 semantics and limitations. External files, credentials,
native extension binaries and session state are not included in snapshots. Explicitly
qualified catalog references are not rewritten for clones. Cross-database writes retain
upstream transaction limitations. Disk-full tests inject SQLite's FULL result; physical
power-loss/torn-sector behavior has not been emulated or certified.

Upstream SQLLogicTests are run unchanged against a native attachment and a Differential
attachment by `make test_upstream`. The default discovers the complete SQLLogicTest tree,
including slow tests; CI uses a smaller PR profile, and the full inventory runs manually. Individual
exclusions, native-baseline results and resource requirements are documented in
[upstream testing](upstream-testing.md). Compiled upstream C++ tests are outside this harness.

Spill files require a local `temp_directory`, including when the database is stored in S3.
Empty paths remain valid and disable spilling. Nonlocal paths are rejected by SET after LOAD
and by Differential ATTACH; local spill creation, an external sort and cleanup are covered
by `test/integration/spilling.py`.

## Merge support

Logical merges support registered local and single-writer S3 databases. Snapshot merging takes
explicit retained inputs; branch merging selects a recorded common ancestor or accepts an
explicit base. Results can use a new branch or the detached existing `ours` target. Existing
targets require exclusive ownership and a live image matching their published snapshot.

Supported native scalar values use DuckDB equality; nested columns merge as whole values.
Schema policies are strict, additive, or catalog mode for the documented table/column/view
changes. See [branch merging](branch-merge.md#supported-scope) for the complete type, key,
constraint, catalog and input restrictions. General schema merging, foreign keys, live-target
replacement and shared-writer S3 merging are outside that scope.

Repositories written by 1.0.0 open without migration; their snapshots have no parent records,
which limits automatic base selection as described in
[upgrading from 1.0.0](branch-merge.md#upgrading-from-100). Downgrading a repository to 1.0.0
after this version has written to it is not supported
([details](branch-merge.md#downgrading-is-not-supported)).
