# Merge validation

These tools validate branch merging on your platform and with your data: a pilot that merges a
disposable copy of real data and verifies the result against an independently prepared expected
database, a stateful soak, and native scale and spill tests. They do not change merge semantics,
storage formats or publication. Linux and macOS are supported.

## Run the local checks

Build `make integration`, then run:

```sh
make test_merge_qualification
# Longer, still bounded run:
python3 scripts/qualify_merge.py --seeds 7 29 97 --steps 40 --rows 32 \
  --timeout-seconds 900 --output build/evidence/merge-qualification-long
```

The default run covers a generated pilot rehearsal, the scalar and native-type stateful profiles
(12 rounds each, seed 7) and every immutable compatibility fixture (currently data written by the
published v1.0.0 release). It also checks that incomplete reports and execution/resource limits
are rejected. `make test_integration` includes it.

Each run writes a report directory under `build/evidence/` with an `acceptance.json`, and
`latest.json` holds a copy of the newest outcome; component reports sit alongside. A report
records the source commit and a fingerprint of the sources used (implementation, tests, scripts,
benchmarks and build inputs, including uncommitted files), the DuckDB pin, platform, extension and
CLI SHA-256, fixture hashes, component outcomes and durations. The fingerprint describes what was
used; it does not prove that a binary was built from those sources, and reports are not signed.
A run only accepts components produced by itself, for the same sources and artifacts.

`status: passed` means every requested check passed for the recorded storage and platform.
It does not cover other platforms, providers or data, and the report's `release_ready` field is
always false. The S3 mode below creates and publishes synthetic databases in a location you
choose.

## Run a representative pilot

Use a disposable copy of representative local or registered single-writer S3 data first. Independently prepare a native DuckDB
expected database with the exact supported table/view definitions and final rows. Quiesce writers
and retain the snapshots for the duration. The pilot intentionally performs the requested merge;
it is not a dry-run command.

Explicit snapshots into a new branch:

```sh
python3 scripts/merge_pilot.py \
  --repository /path/to/disposable/repository --database sales \
  --base 1 --ours 2 --theirs 3 --destination reviewed-result --request-id sales-pilot-1 \
  --expected /path/to/expected.duckdb --report build/evidence/sales-pilot.json
```

Reviewed branch inputs into a detached existing target:

```sh
python3 scripts/merge_pilot.py \
  --repository /path/to/disposable/repository --database sales \
  --mode branches --ours work --theirs feature --destination work --request-id sales-pilot-2 \
  --schema-mode catalog --expected /path/to/expected.duckdb \
  --report build/evidence/sales-target-pilot.json
```

Use a different, unused destination name to create a new branch. Branch mode selects the recorded
base through full preflight, requires readiness, and supplies all three reviewed snapshot guards
to execution. It does not capture unsnapshotted edits. Input movement after review is rejected by
the extension. Snapshot mode requires all three explicit IDs. Shared-writer databases and
descriptor-only inputs are outside the helper's scope; the database must be registered in the
supplied repository.

The helper defaults to the release extension and stock integration CLI. Use `--extension` and
`--duckdb` to select exact artifacts. Defaults are two threads, 128 MB DuckDB memory, 512 MB
local temporary storage and 120 seconds per subprocess. The process deadline is configurable
from 1 to 3,600 seconds. Memory settings are not process-RSS limits. Preflight/application and
verification can scan/rebuild full tables; this is not evidence of changed-row-only cost.

Verification compares native table and column definitions, defaults, nullability, constraints,
indexes, view SQL/output metadata and native view dependencies. Typed SQL `EXCEPT ALL` in both
directions checks every table and view, including row multiplicity, NULLs and exact decimals.
It checks root/branch isolation, completed publication and exact request replay. Expected macros,
custom types and sequences are rejected rather than silently ignored. Only the merge engine's
supported main-schema catalog is covered; this does not establish SQL equivalence between
differently written view definitions. Reports contain identities, hashes, metadata and counts,
not table rows or raw SQL error messages.

For a registered single-writer S3 database, the same command also checks remote input-head
isolation and publication. It verifies the result's immutable export descriptor through a
new temporary repository, using the extension's AWS SDK transport. Completed replay verifies
that original export even if the destination advanced, was removed, or the local snapshot
expired; it does not compare the latest branch head against an old expectation.
The fresh reader's repository is removed after verification, including failures.

Use `--s3-region`, `--s3-endpoint`, `--s3-path-style`, `--s3-allow-http` and `--s3-profile`
when the independent reader needs non-default connection settings. For example, append
`--s3-endpoint http://127.0.0.1:9000 --s3-path-style --s3-allow-http` for a local MinIO
endpoint. These do not change the registered writer's provider settings. Credentials come
from the SDK chain, named profile or native persistent secrets; the tool accepts no credential
values and never includes credentials/provider responses in its report. A profile selection
also sets `AWS_PROFILE` in child processes. No cloud resource is provisioned by the pilot.

`--timezone` and `--calendar` pass settings to native DuckDB for execution and verification.
Use the context intended for the supplied expected database. A pending request still uses its
already-frozen expression settings when recomputing; verification queries run in the caller's
selected context. Neither the helper nor the merge engine invents timestamp conversion rules.

Typed resolutions may be provided from a committed native database:

```sh
# Append these arguments to the appropriate pilot invocation:
--resolutions /path/to/decisions.duckdb --resolution-table main.items=fixes
```

`fixes` must have the exact typed merged columns plus `_merge_action` and `_merge_columns`, as
specified in [branch merging](branch-merge.md). Repeat `--resolution-table`
for additional tables. The helper attaches the database read-only as `decisions`. Request identity
includes those source-table names; resolutions captured by the extension remain frozen on retry.
Once decisions have been frozen, an interrupted or completed retry needs the same mapping arguments but no longer needs the source file. A request interrupted before capture still needs its original resolution source.

The same request ID returns and verifies the original result, even if its destination later moves.
An interrupted request uses its recorded input IDs and guards; it does not run a new preflight or
substitute another base. Existing target fences can therefore be recovered. To abandon a request,
use its operation UUID and a separate abort request ID:

```sh
python3 scripts/merge_pilot.py --repository /path/to/disposable/repository --database sales \
  --abort-operation OPERATION_UUID --request-id sales-pilot-abort-1 \
  --report build/evidence/sales-abort.json
```

Safe abandonment checks idempotent replay and unchanged branch heads/snapshot roots. It can
release a pending target after an input expires. If an S3 result was already published, abort
completes local recovery instead; the pilot allows only that destination to advance and checks
input/remote-head isolation. Supply `--expected` to verify the recovered result. Without it,
the helper records the completed recovery but exits 2 with verification incomplete. An uncertain
PUT remains a failure with `merge_publication_uncertain`, never a successful abort.
Inspect `differential_merge_requests` for the operation and recovery state. Request IDs are
repository-wide, so use distinct IDs across databases.

Exit 0 means verification passed; exit 1 means failure. Exit 2 means a completed replay returned
its recorded result but that snapshot is no longer readable, or abort recovered a published result
without an expected database, so result verification is incomplete.
A failed post-publication comparison leaves the completed result available for inspection; it does
not undo or hide publication. On failure before publication, inspect/retry/abort the recorded request.
A process killed at the deadline may leave recovery work, just like any other interrupted client.

## Stateful soak coverage and bounds

Each seed starts a fresh disposable repository. A Python model carries the expected result forward
across branches and merges. It implements typed, state-based three-way cell/existence rules without
reading generated changesets or using merged output to calculate expectations. Each result is compared
with an independently constructed native expected database. The bounded row map exists only in this
test driver; production comparison remains inside DuckDB.

The repeating twelve-round schedule covers:

- New-branch and existing-target merging, different-column updates, identical inserts, NULL transitions,
  composite keys, primary-key moves, strings, booleans and exact decimals.
- Unresolved cell and delete/update conflicts, explicit typed decisions, replay without the resolution
  source, and fast-forward followed by an already-incorporated merge.
- Process death after staging, inside the publication transaction, and after publication; restart/retry,
  SIGINT cancellation after staging, a staging-write limit, and abort after base expiry.
- Nullable column/table addition and removal, view replacement, ART index/default/CHECK preservation.
  Column-removal rounds respect the documented restriction on retaining-side inserts/key moves.
- Expiry and explicit GC after every round; no leftover reader pins, merge staging, target intents or
  unexpected live branches. Metadata integrity and retained root mappings are checked after restart.

Seeds vary typed values and the affected keys. This is bounded stateful testing, not an exhaustive
scheduler or arbitrary schema fuzzer. History/audit metadata intentionally grows with operations;
retained payload and staging are measured separately. No assertion claims that SQLite file size or
operation history stays constant.

The standalone `scripts/merge_soak.py` accepts 1–10 unique seeds, 12–200 rounds and 8–1,024 initial
rows (the model also caps each side at 2,048 rows). Defaults are 40 rounds for seeds 7/29/97, two
threads, 128 MB DuckDB memory, 512 MB temporary storage, 60 seconds per subprocess and 900 seconds
overall. It samples a 1 GiB disposable-directory disk budget and 1 GiB host free-space reserve.
`--disk-limit-mib`, `--free-space-mib` and `--timeout-seconds` adjust those guards. A test-hook artifact
is required for a passing full soak; running a release artifact does not silently skip crash tests.

Reports include per-round comparison/application/publication timings, peak child RSS, child I/O
block counters, sampled disk usage and quiescent storage counts. Sampling may miss brief peaks;
zero OS I/O counters are not proof of zero device I/O. Process-death tests are not power-loss tests.
Repeat the long run with representative catalogs and data volumes on every platform you deploy
to. See [performance and sizing](benchmarks.md).

## Native-type stateful profile

```sh
python3 scripts/merge_soak.py --profile native --seeds 7 29 --steps 24 --rows 32 \
  --timeout-seconds 900 --report build/evidence/native-soak.json
```

This profile carries 23 scalar/container shapes plus two ordinary control columns through
repeated branch merges. It covers UUID keys, UHUGEINT extrema, BLOB bytes, FLOAT/DOUBLE,
the six native timestamp types, TIME variants, INTERVAL, STRUCT, LIST, ARRAY, MAP, UNION,
TUPLE and nested combinations. The original scalar profile retains its independent Python
decimal/composite-key/catalog model.

The native model asks DuckDB for `IS NOT DISTINCT FROM` over a bounded alphabet of input
values, then applies the row/cell rules independently in Python. It never reads merge output
to calculate expectations and never compares values by text/JSON. This exercises native NaN,
signed-zero, equivalent timestamp-offset and interval equality. Nested columns remain whole
cells; conflicting nested edits require explicit typed choices. A model fingerprint records the
equality relation used. This is an oracle for merge rules using native equality, not an independent
implementation or validation of DuckDB's type semantics.

The twelve-round schedule includes independent/agreed changes, cell and row-existence
conflicts, frozen resolution recovery without its source, changed native settings on retry,
tracked-view rebuild interruption in strict/additive modes, staging limits and publication
retry/abort. The baseline uses `America/New_York`/`gregorian`; selected retries use
`UTC`/`buddhist` to verify frozen expression context. Expected catalogs retain tracked view
chains and a temporal CHECK. Every completed round verifies typed data, catalog, parents,
request replay, input isolation, expiry and GC. Quiescence checks pins, staging, intents,
pending exports/reservations and local retained-root mappings. Bounds are the same as the
original profile; generated input alphabets and row maps are explicitly bounded.

## S3 checks

The disposable MinIO entry point sets up the pinned provider fixture and runs the complete
S3 checks. It is also included in the full MinIO and merge-only suites:

```sh
python3 test/integration/minio.py --merge-qualification-only
```

They run the S3 pilot rehearsal, the native stateful profile on S3 and all immutable
**local** compatibility fixtures. The latter do not establish immutable S3 compatibility.
The pilot covers new/existing destinations, guarded execution, original-export replay,
wrong expected data, unsupported shared writers, interrupted/prepared/published recovery,
safe and uncertain aborts, and frozen nested choices. The S3 soak checks a fresh remote result
after each round. Prepared new destinations must report their reservation owner without
changing publication metadata; abort must release the name and retry must publish once.
It also exercises evicted inputs and interruption after the conditional head PUT.
Reports without fresh remote verification or reservation checks, or with only local evidence,
are rejected.

Against a disposable prefix in an existing bucket (AWS S3 or another provider), run:

```sh
python3 scripts/qualify_merge.py --s3-location s3://BUCKET/disposable-merge-qualification/ \
  --s3-region REGION --s3-profile PROFILE --timeout-seconds 900 \
  --output build/evidence/s3-qualification
```

Each generated case/seed creates a fresh UUID namespace below the supplied prefix. S3 object
operations all go through the extension. The helper deletes no S3 objects: completed exports
and immutable uploads remain under the generated prefixes. The MinIO container is removed by
its outer fixture; on a real provider, clean up the prefix yourself. The local disk/time
budgets do not constitute a bucket quota. Reports identify generated namespaces and the exact
sources and artifacts, without credentials or row values. No real AWS run is implied by a MinIO success.

Local and S3 reports have distinct required components and storage scopes. Validation rejects
missing native/remote verification, the wrong provider scope, missing reservation checks,
incomplete rounds, missing fixtures and components from different sources or artifacts.

## Native scale and observed spill

After building the integration host and release loadable (MinIO commands also require Docker
and the [pinned image](../README.md#integration-suites)):

```sh
DIFFERENTIAL_EXTENSION="$PWD/build/release/extension/differential/differential.duckdb_extension" \
  python3 benchmarks/merge_native_scale.py --report build/evidence/native-scale-local.json
DIFFERENTIAL_EXTENSION="$PWD/build/release/extension/differential/differential.duckdb_extension" \
  python3 benchmarks/merge_native_scale_minio.py --output build/evidence/native-scale-minio
python3 test/integration/merge_native_spill.py
python3 test/integration/minio.py --merge-native-spill-only
python3 test/integration/merge_scale_verifier.py
```

The benchmark defaults to 100,000 and 1,000,000 rows, 1% and 20% updated keys, and three
repeats per case. It performs guarded merges into the detached `ours` branch, with two threads,
`memory_limit='1GB'`, `max_temp_directory_size='2GB'`, UTC and Gregorian settings. Native
external execution is not forced. Limits are 300 seconds per child, 900 seconds per case,
a sampled 6 GiB working-directory cap and a 2 GiB free-space reserve. Extension limits are
4,000,000 total input rows, 2 GiB input logical bytes and 2 GiB staging writes. The memory
setting is a native buffer budget, not a process RSS quota. Native MB/GB and reported MiB/GiB
are different units.

The table contains a BIGINT primary key, UUID, constrained/defaulted BIGINT, DOUBLE including
NaN, nanosecond zoned timestamp, nullable ARRAY, STRUCT containing ARRAY/LIST/BLOB/UHUGEINT,
and LIST containing nullable ARRAY values. Both sides update the first K keys: ours changes
the integer and DOUBLE; theirs agrees on the DOUBLE and replaces the temporal/container cells.
Each also deletes a distinct key and inserts a distinct key. Inputs and output have N rows;
there are K+2 changed rows per side and K+4 distinct changed keys. Exact DDL and generating
expressions are retained in each report. Containers merge as whole values.

Verification derives every expected cell from the key and workload specification using native
typed SQL. It checks the complete key domain and cardinality, the native primary-key constraint,
all remaining constraints/defaults, index and tracked view definitions/dependencies. No table
map, serialized-value comparison or row hash is used. Eight negative tests reject wrong NULL
arrays, STRUCT children, LIST-of-ARRAY cells, timestamps, missing/out-of-domain keys, a missing
index, and duplicate keys in a table without its PK. Completed replay must retain diagnostics
and operation identity and create exactly one result snapshot. Inputs retain their roots and
all non-target branches retain their heads.

Each MinIO sample gets a fresh pinned container with a 1 GiB provider pool; the container is
removed afterward. The common base is exported, evicted and proven reclaimed locally before
the merge; input heads remain local. The result's exact immutable export is independently
verified in a fresh reader repository. All S3 operations use the extension's AWS SDK path.
The benchmark also accepts an existing `--s3-location` plus the same connection options as the
pilot; on a real provider it leaves immutable objects under the prefix for you to clean up.

Reports separate selection, target checking, input materialization, comparison, application
and publication timers. They include whole-merge process wall time/RSS, observed native spill
files, sampled local disk use, OS block counters and extension S3/local byte/request counters.
Merge timers exclude fixture setup, verification, replay and cleanup. Initial base creation and
row verification have separate process measurements; not every setup/catalog-check call is timed
individually. Run the matrices without competing test workloads;
OS caches are not flushed. Sampling can miss peaks, and logical/SDK counters are not device or
wire throughput. Source and artifact identities are checked throughout; reports are written under
`build/evidence/`. See [performance and sizing](benchmarks.md).

The spill suite uses 100,000 rows/20% changes and tests both new branches and existing targets,
with forced external execution at a 64 MB memory budget. A run with an 8 MB temporary-directory
budget must fail with DuckDB's own temporary-directory limit error. SIGINT and SIGKILL are sent
while the merge waits at its phase gate before the native comparison (`comparison`), so no result
depends on host speed or on sampling short-lived spill files; these cases need the integration
build's test hooks. Every failure must preserve input roots, branch heads and snapshot history,
and publish no result. The same request retries at 256 MB/2 GB, verifies exact data/catalogs (also
remotely on S3), replays without another result, and checks that staging, pins, intents, exports
and reservations are cleaned up.

After SIGKILL an existing target remains durably fenced as `target_locked`; a new writer must
be refused until retry/abort. Cleanup of abandoned temporary files is DuckDB's own behaviour and
is not part of these tests. These failure tests and verifier checks are included in
`make test_integration`; the S3 cases are also included in `make test_minio`. Large benchmark
matrices remain manual.
