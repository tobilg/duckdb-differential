# Contributing

Target stock DuckDB **v2.0.0** using the pinned submodules and vcpkg dependencies. Linux on
amd64/arm64 and macOS on arm64 (Apple silicon) are supported build targets. See [compatibility](docs/compatibility.md),
[architecture](docs/architecture.md) and [storage formats](docs/storage-format.md).

## Build and test

Follow the [build instructions](README.md#build), then use the appropriate checks:

```sh
make test_publication            # known private/generated files must stay out of the source tree
make test                       # release SQL logic tests; hooks must be absent
make integration                # extension-free host plus loadable with test hooks
make test_integration            # C++ and Python unit, fault, recovery and concurrency suites
make test_merge_qualification    # pilot rehearsal, stateful merge checks and compatibility fixtures
make test_upstream_harness
make test_upstream_pr            # smaller upstream comparison profile used by CI
make test_upstream               # full upstream inventory; manual only
make test_upstream_skips         # reviewed exclusion audit; manual only
```

Add SQL behavior to `test/sql/`. Add process-death, race and storage tests to the existing
integration runners. Every management function must have complete `FunctionDocs()` metadata
and an entry in [the SQL reference](docs/sql-reference.md). SQL-reference and shared-database
examples are exercised by `test/integration/doc_examples.py`.

Integration builds use `DIFFERENTIAL_TEST_HOOKS=ON`. Release/community builds omit the hooks.
Test programs find artifacts through `test/integration/paths.py`; overrides are
`DIFFERENTIAL_BUILD_DIR`, `DIFFERENTIAL_EXTENSION`, `DUCKDB_BINARY` and `DUCKDB_LIBRARY`.
Upstream comparisons may download official DuckDB test dependencies into `build/`.
See [upstream testing](docs/upstream-testing.md) for coverage and exclusions.

## S3 and merge validation

Use disposable directories, containers and provider namespaces for destructive tests.
All S3 object operations must use the extension's pinned AWS SDK transport.

```sh
docker pull ghcr.io/coollabsio/minio@sha256:69b55a1c1c5dc285ce04db96689f5b2102317fc77a50680a1874ca6efd1c87f9
make test_minio
python3 test/integration/minio.py --merge-only
python3 test/integration/minio.py --merge-native-spill-only
```

The MinIO harness owns/removes its provider container and generates temporary test credentials.
It does not provision cloud infrastructure. Official extension coexistence tests require network
access. Tests against real AWS need a disposable bucket prefix you control; the tools leave
immutable exports there for you to clean up.

[Merge validation](docs/merge-qualification.md) documents representative-data pilots,
stateful tests, exact result verification and bounded spill/recovery cases. The
[benchmark inventory](benchmarks/README.md) lists measurement programs. Measure with a release
loadable, identify the exact source/artifacts, and distinguish test costs from performance
measurements. Existing target branches must be detached and their reviewed inputs guarded.

## Public documentation and private output

Public docs describe current behavior, usage, supported scope, limitations and stable design.
The API, storage-format and recovery contracts belong in the source repository. Contributor
instructions, test programs and deliberately versioned synthetic compatibility fixtures are
also source inputs; keep the fixtures immutable.

Per-run reports, transcripts, local paths, implementation journals, review notes and release
checklists belong under ignored `build/evidence/` (or an explicitly private output directory).
`DIFFERENTIAL_EVIDENCE_DIR` can redirect reports; the chosen directory must remain private.
Raw legacy benchmark JSON is ignored under `benchmarks/`; `benchmarks/report.py` renders its
summary under `build/evidence/`, never into the public documentation by default.

`make test_publication` checks for known private/generated paths in tracked or pending source
files. Ignore rules prevent accidental additions; `.gitattributes` excludes those paths and
agent instructions from source archives. These controls do not remove files from Git history.
Review a deliberate public summary rather than copying a run transcript into `docs/`.

CI runs the checks and leaves raw evidence on its runner. It does not automatically upload
`build/evidence/`. The pinned DuckDB distribution workflow uploads extension binaries and, on
some platforms, test-support bundles needed by downstream CI jobs. Those Actions artifacts
are separate from release deployment; do not copy the entire CI bundle into a release.
Ordinary CI logs and artifacts remain visible according to repository access.
Any deliberate diagnostic sharing needs a review for paths, provider locators, host details,
credentials and data. Never use integration binaries as release artifacts.

## Release checks

A release is built from one clean commit, with reproducible release builds and the SQL and
integration checks passing on all four supported platforms. Run the full upstream inventory and
exclusion audit manually, the S3 checks against AWS for the supported S3 scope, and a pilot with
representative data. MinIO and process-death checks do not replace AWS or physical power-loss
validation. Preserve the licenses and third-party notices in any distribution.

`make bump-version V=X.Y.Z` updates coordinated version references. Follow
[UPDATING.md](docs/UPDATING.md) when changing the DuckDB pin; released data-format changes
require a version bump, migration and compatibility tests.
