# Immutable merge compatibility fixtures

These archives contain small synthetic local repositories produced by real extension binaries.
Normal tests verify their SHA-256 hashes and extract disposable copies; they never regenerate
or modify the archives. `make test_integration` runs `test/integration/merge_compatibility.py`
against every committed manifest here.

| Fixture | Producer and coverage |
| --- | --- |
| `v1.0.0-release` | Written by the published v1.0.0 extension (installed from the DuckDB community repository), before branch merging existed: three snapshots on sibling branches with a composite key, defaults, a CHECK, an index and a view, and no merge or snapshot-parent records. The current extension reads every snapshot, merges the sibling branches through the fork proof, runs explicit-snapshot and existing-target merges, and keeps the recorded ancestry through metadata cleanup. A repeated merge that reaches release snapshots without parent records must name its base explicitly; branches forked after the upgrade select it automatically. |

Manifests identify the producer's artifact SHA-256 and version, where it was installed from,
the stock DuckDB version, platform, generator digest and the store/control/export formats.

The archive loader rejects unexpected or duplicate paths, links, traversal and checksum
mismatches, with limits of 1,000 files and 16 MiB uncompressed. Archives contain no external
pools, credentials or S3 namespaces.

To run against a candidate binary:

```sh
make integration
python3 test/integration/merge_compatibility.py
# The unmodified integration host can also load a release artifact:
DIFFERENTIAL_EXTENSION="$PWD/build/release/extension/differential/differential.duckdb_extension" \
  python3 test/integration/merge_compatibility.py
```

To add a fixture deliberately, use a **new** identity and review the manifest/archive in the
same change. The generator refuses to overwrite either file. A release fixture comes from the
published artifact, for example after `INSTALL differential FROM community` into a disposable
extension directory:

```sh
DIFFERENTIAL_EXTENSION=/path/to/published/differential.duckdb_extension \
  python3 scripts/create_merge_fixture.py --profile release \
  --release-source "DuckDB community extensions repository" \
  --label vX.Y.Z --output test/fixtures/merge/vX.Y.Z-release.json
```

The `classic` and `native-context` profiles capture merge requests in every durable state
(completed, guarded, aborted, expired and a target killed at `staged`, with frozen typed
choices and expression context) with a hook-enabled integration build. Capture them from the
first release that ships branch merging, so that later releases can prove they reopen its
pending and completed requests.

Once a version is published, retain its fixtures unchanged. Every later release must run the
old-fixture suite on each supported platform. A store/control/export format change requires its
version bump, migration and old-data tests under the repository policy.

Earlier candidate fixtures from unreleased prototype builds used store format 1. Store format 2
(the published 1.0.0 layout) refuses them, so they were removed; no released repository can
contain those records.

Current gaps: the fixture originates on macOS arm64, and there are no immutable S3 or
shared-writer fixtures. Live S3/MinIO recovery tests remain separate. This suite does not
establish power-loss safety or a downgrade guarantee.
