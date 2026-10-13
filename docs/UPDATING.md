# Updating to a new DuckDB release

Community extensions are built for the latest stable DuckDB release. When DuckDB releases a new
version, update this repository as follows:

1. **Submodules.** Check out the new release tag in `./duckdb`, and the matching branch of
   `./extension-ci-tools` (for DuckDB v1.5.x that branch is `v1.5-variegata`). Update the
   `branch` entries in `.gitmodules` accordingly.
2. **Workflows.** In `.github/workflows/MainDistributionPipeline.yml`, set `duckdb_version` to
   the new tag, and set `ci_tools_version` plus the reusable workflow ref
   (`_extension_distribution.yml@…`) to the new ci-tools branch.
3. **vcpkg.** Check which vcpkg commit the community repository uses for the new release
   (`COMMUNITY_EXTENSION_VCPKG_COMMIT` in its `.github/workflows/build.yml`). If it changes,
   update `VCPKG_COMMIT` in the `Makefile`, `vcpkg_commit` in both workflows and in
   `community/description.yml`, and the dependency versions in `docs/compatibility.md`. After
   `make release`, regenerate `THIRD_PARTY_NOTICES.md` with `python3 scripts/third_party_notices.py`.
4. **Build and test.** `make release && make test`, then `make integration`,
   `make test_integration` and `make test_minio`. Review the commit pin, individual exclusions,
   requirements and configurations in `test/upstream/suite.json`, then manually run
   `make test_upstream_harness test_upstream test_upstream_skips`. Review the test-only bridge
   against the new runner's load/restart semantics and inspect native-baseline failures before
   attributing them to Differential. Recheck the source and footprint of each owner-approved
   disk-space exclusion before updating its source hash; these audits never run the workload.
   New SQLLogicTest files enter full coverage automatically. CI runs only the harness checks
   and PR profile for upstream testing; the full suite and exclusion audit are manual upgrade
   verification steps.
5. **Release.** `make bump-version V=X.Y.Z`, add a `CHANGELOG.md` entry, and update `ref` (and,
   while a DuckDB release is pending, `ref_next`) in the community descriptor.

## API changes

This extension uses DuckDB's internal C++ API: a `DuckCatalog` subclass, the storage extension
attach callback, a custom `FileSystem` and transaction managers. That API is not stable, so a new
DuckDB release may require code changes. Useful references:

- DuckDB's [release notes](https://github.com/duckdb/duckdb/releases)
- DuckDB's history of [core extension patches](https://github.com/duckdb/duckdb/commits/main/.github/patches/extensions)
- the git history of the C++ headers that changed

## Stored data

Repositories and S3 exports must stay readable across DuckDB updates:

- The native database files inside the repository are ordinary DuckDB files. DuckDB itself
  reads files written by older releases (and upgrades their storage on write where needed).
- Export descriptors record the writing DuckDB version (`duckdb-vX.Y.Z`), but readers accept
  any version line. Whether a native file can be opened is decided by DuckDB at attach time.
- The extension's own formats (store, control, export; see `docs/storage-format.md`) are
  independent of the DuckDB version and do not change with a DuckDB update. If a later release
  ever changes one of them, it must ship a migration plus tests against data written by the
  previous release, because published repositories and exports must stay readable.

The storage adapter relies on these DuckDB source contracts; recheck them when updating:

- [`SingleFileBlockManager::IsRemote` and `Prefetch`](https://github.com/duckdb/duckdb/blob/069cc9f9b5be802405797faecc284961b07c70ef/src/storage/single_file_block_manager.cpp#L1072)
  use `FileHandle::OnDiskFile()` for `REMOTE_ONLY` prefetch.
- [`GetBlockLocation`](https://github.com/duckdb/duckdb/blob/069cc9f9b5be802405797faecc284961b07c70ef/src/storage/single_file_block_manager.cpp#L1108)
  places native blocks after three 4096-byte headers. The adapter has a compile-time assertion
  against the native header size; the native block-size matrix tests smaller allocations.
- [`RequireTemporaryDirectory`](https://github.com/duckdb/duckdb/blob/069cc9f9b5be802405797faecc284961b07c70ef/src/storage/standard_buffer_manager.cpp#L453)
  creates spill storage through the filesystem. Differential requires local spill storage
  and preserves the native buffer manager and cleanup behavior.
