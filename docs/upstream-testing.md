# Upstream DuckDB SQL tests

`make test_upstream` discovers the complete pinned DuckDB `test/` SQLLogicTest tree:
`.test`, `.test_slow` and `.test_coverage` files. Every selected file runs unmodified against
both a native file attachment and a Differential attachment, in separate processes with
disposable storage. Exclusions are individual paths with reviewed reasons and observed
outcomes on both backends; there are no excluded directory globs.

```sh
make integration
make test_upstream_harness
make test_upstream                    # manual complete inventory, including slow tests
make test_upstream_pr                 # faster PR profile
make test_upstream_skips              # manual exclusion audit (disk policies check source only)
# Exercise a release artifact with the integration runner and test bridge:
python3 scripts/test_upstream.py --extension build/release/extension/differential/differential.duckdb_extension
# Parallelism and stable sharding:
make test_upstream UPSTREAM_JOBS=4 UPSTREAM_ARGS="--shard 0/4"
# Investigate failures without editing exclusions:
python3 scripts/test_upstream.py --survey 'test/sql/storage/**/*.test'
python3 scripts/test_upstream.py --survey --retry-from build/evidence/upstream-survey-.../results.json
# Inspect the exact effective selections:
python3 scripts/test_upstream.py --list
python3 scripts/test_upstream.py --list --profile pr
```

## CI and manual coverage

The PR profile includes recursive ATTACH, catalog, constraints, ALTER, storage, indexes, transactions,
INSERT, DELETE, UPDATE, UPSERT, MERGE, prepared statements, types, aggregates, joins,
windows and subqueries. It uses regular `.test` files. The default/full profile adds every
other area, regression and fuzzer files, and slow tests. Tests outside the PR profile remain
included in manual full runs.

The Integration workflow runs the harness checks and PR profile on Linux amd64 and macOS
arm64 for pushes, pull requests and manual workflow dispatches. Logs and JSON results are
uploaded even on failure. The full suite and exclusion audit are outside CI: invoke them
explicitly with `make test_upstream` and `make test_upstream_skips`. Run the exclusion audit
when upgrading DuckDB, changing the adapter or reviewing the exclusion list. The runner's
default/full profile also remains available for manual full runs.

Manual runs can use `--shard INDEX/COUNT` to divide the inventory across workers. Each test
always belongs to the same shard for a given shard count, based on the SHA-256 of its path.
Both backends of a test stay in one shard; execute every shard to cover the full selection.

## How the storage comparison works

The integration build includes DuckDB's bundled core functions, Parquet, JSON, ICU, TPC-H,
TPC-DS and autocomplete dependencies. It also builds a separate, test-only
[`upstream_bridge`](../test/upstream/bridge/upstream_bridge.cpp) extension. The bridge is
not part of the production Differential artifact. DuckDB itself is unchanged.

DuckDB's `on_init` hook loads Differential and invokes the bridge. The new-connection
hook stays empty: upstream invokes it before every concurrent-loop statement, including
COMMIT/ROLLBACK in an aborted transaction.
The bridge replaces the runner's host with the tested attachment while preserving its
catalog name. A marker in the detached host identifies the corresponding attachment.
`restart` reuses that identity; a fresh `load` deletes the host and creates a new identity,
including when a test loads the same filename again. Named connections use the database's
normal default catalog and search path.

All creation and attachment statements execute as ordinary SQL. The pragma expansion
only reads host metadata and prepares SQL. For databases created by the test, the bridge
creates no snapshots and does not checkpoint on the test's behalf. Existing read-only native fixtures
use `differential_import_database` after closing the source: it copies the bytes unchanged
and creates its normal initial snapshot, allowing a read-only attachment. The native side
keeps the original fixture as its primary database. Differential's native durability remains enabled.

The runner uses DuckDB's normal PID-scoped temporary directory inside a disposable working
tree. It deliberately does **not** pass `--test-temp-dir`: in the pinned DuckDB version,
that option makes `DeleteDatabase()` a no-op, changing `load` semantics. Existing native
database fixtures and their WAL sidecars are copied before use. Other input fixtures are
shared from the pinned checkout.

The initial host has a file path, so DuckDB's own `require noforcestorage` guards work.
Default execution uses two DuckDB query threads, a 3 GB memory limit (`--memory-limit`) and two
concurrent test pairs; DuckDB 2.0's SF1 workloads and some CSV/Parquet tests exceed 1 GB. Individual configurations enable the upstream 16 KiB block-size variants,
500000-row vacuum-rebuild threshold, latest storage for VARIANT/geometry, and larger memory
allowances for ART, very wide joins, large CSV/Parquet buffers, giant strings and TPC-H generator
comparisons where required. These configurations apply equally to both backends. Tests can
also set their own SQL settings. SQL extension autoload and autoinstall
are disabled; extension installation tests may explicitly attempt downloads. The runner
installs required official HTTP, INET, SQLite, Quack and Encodings extensions into `build/upstream_extensions`,
then copies only the required artifacts into each test's disposable extension directory.
Installation and public remote fixture tests require network access; missing dependencies
fail setup instead of silently reducing coverage. Tests requiring `LOCAL_EXTENSION_REPO`
receive the repository generated by the integration build. The specialized update-CI
fixture with deliberately incompatible binaries remains separately excluded and audited.
The thread-reset test uses DuckDB's own default thread count. Tag-runner tests receive
their explicit opt-in flag. Public historical secret fixtures are copied and given their
required 0600 permissions; each process has a disposable default secret directory.

## Exclusions and failures

[`test/upstream/suite.json`](../test/upstream/suite.json) is the authoritative list of
individual exclusions. Each entry records its source path, category and explanation.
Compatibility exclusions also record the expected status and diagnostic for **both** backends. Cases that only fail on Differential
must identify the relevant product behavior or unsupported test assumption; a native
baseline failure alone does not automatically create an exclusion.

`make test_upstream_skips` runs the compatibility exclusions. It fails on an unexpected pass, changed
failure, stale path, timeout or signal interruption. An excluded test must keep reproducing its documented
reason. The one explicit intermittent exception is the upstream concurrent vacuum test:
repeated native and Differential runs both pass or retain the original row IDs because a
concurrent checkpoint does not vacuum. Its audit accepts a pass or only that exact row-ID
mismatch, and records which happened. A passing intermittent audit is not reported as a
reproduced failure.

Three workloads are excluded at the owner's request for excessive disk use: the distinct
TPC-H grouping test explicitly requires 40 GB free; encrypted block offloading retains two
500-million-row tables (about 8 GB of incompressible random values, plus row IDs, overhead
and spill); plain block offloading creates one 500-million-row table plus 1 GB of spill
and its incomplete Differential probe already occupied 4.3 GiB. Their audit checks the pinned source hash and
reviewed reason without executing the workload, reporting `confirmed_policy_exclusion`.
They do not run under `--survey` either. Other encrypted spill, TPC-H and DISTINCT tests
remain in coverage. Runtime alone is never an exclusion reason.

Individually listed RAM, long-double and compiled vector-size requirements remain in normal execution.
They run wherever the upstream guard is satisfied and otherwise report `unavailable`,
separately from `passed`. A different skip reason still fails. The ordinary run fails on
every unclassified failure, timeout or unexecuted test.
`--survey` records those same outcomes but exits successfully for investigation; it never
writes exclusions.

A zero exit code is insufficient evidence of execution: upstream requirements and `mode
skip` can skip files successfully. Conversely, a valid zero-row query can have zero Catch
assertions. Ambiguous zero-assertion results are rerun with SQL tracing and only counted
as executed if a statement from the source appears in the trace. A script consisting
only of `load`/`restart` commands must complete successfully and show the final bridge
initialization in its trace; an empty file still does not count as executed coverage.

There is no per-test time limit in normal or CI runs, including slow tests. The optional
`--timeout` flag is for an explicitly bounded diagnostic run only; interrupted tests remain
unverified and must be rerun without a cutoff. Runtime alone never justifies an exclusion.
Optional sharding distributes manual full runs across workers.

## What this does not establish

This is the complete SQLLogicTest **inventory**, subject to the documented individual
exclusions, not DuckDB's compiled C++ test suite. Those tests create their own databases and
do not use the SQL initialization hooks. Explicit native `ATTACH` statements in upstream
SQL still attach native databases; passing such a test does not prove every attachment
used Differential. Tests that only evaluate expressions exercise SQL with Differential
loaded but may perform no storage I/O.

Checkpoint-consistent native fixtures retain their binary encoding through byte import.
A native fixture requiring WAL replay cannot be imported: checkpointing it first would
remove the recovery state under test. Native read-only reopen tests also differ from Differential's
public read-only API, which requires a published snapshot. Dedicated extension suites cover
snapshots, recovery, process death and S3. This comparison does not replace them.

Each run records the pinned DuckDB revision, extension, bridge, harness and suite hashes, profile, shard,
assertions, execution status, elapsed time and individual logs in `build/evidence/`.
Hosted CI results require a push; a locally successful survey is not a hosted acceptance run.
