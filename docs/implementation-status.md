# Implementation status

**Full v1 acceptance is not complete.** The extension implements the local and S3
snapshot profiles on DuckDB 1.5.6 (macOS arm64 verified). Real AWS has been exercised by the
owner (exports, cold reads, branch heads), but the AWS capability, fault and secrets suites, Linux
S3 tests, remaining platform builds and release qualification remain open. `differential_status()` reports `v1_complete = false`.
A PASS below means the listed executable evidence passed on this platform; it does
not certify every failure schedule, filesystem or provider.

At a glance:

| | State |
| --- | --- |
| Built and tested | macOS arm64: SQL logic tests, C++ unit tests, crash/fault/race suites, MinIO S3 suites. Linux amd64: integration build, 6 C++ tests and 27 local integration suites passed in CI; MinIO pull failed before S3 tests |
| Remaining platform qualification | Linux arm64, macOS amd64 (all sources pass a GCC 13 syntax check) |
| Not supported | Windows, wasm |
| Real AWS | Exercised by the owner: exports, cold reads, branch heads, throughput (us-east-1, eu-central-1). The SDK capability and fault suites have not run on AWS |
| Untested | Power loss, a second OS user on a shared repository, a second physical host |

The sections below record the evidence in detail, including the measurements behind the
current defaults.

## Requirement-to-phase plan

- [x] Phase 0 — Stock 1.5.6 C++ extension, filesystem/native alias, SQL execution and
  completed-checkpoint boundary, immutable readers, writer exclusion, sidecar recovery.
  AC-01–03, 19–21, 33.
- [x] Phase 1 — Durable byte ranges/maps/lengths/namespaces, SQLite metadata store, immutable
  manifests, default and additional local pools; native/recovery/crash tests.
  AC-16–19, 28. Physical volume failures remain a qualification limit.
- [x] Phase 2 — IDs/names/times, snapshots/history, branches/clones, restore/delete/retire,
  ownership/epochs, idempotency, import and SQL diagnostics. AC-04–13, 20–22, 35, 37.
- [x] Phase 3 — Complete local root accounting, atomic pins/publication, bounded/resumable
  GC and physical page compaction; retention/concurrency evidence. AC-14–15, 31.
  Compaction is an equivalent mechanism for unlayered chunk maps (see `architecture.md`).
- [ ] Phase 4 — S3 implementation and MinIO integration complete; SDK-only immutable
  export, query-driven reads, bounded resources and total-local-loss recovery pass.
  AC-23–30, 36–37. AC-24 real AWS: product paths exercised by the owner; the capability suite is unrun.
- [ ] Phase 5 — Community-extension build (C++ extension template, vcpkg pins, CI workflows),
  fault suites, diagnostics, examples and benchmarks implemented. AC-01, 32–34 and all release
  gates. Linux amd64 local integration passed in CI; Linux S3 tests, remaining platform
  qualification, the AWS capability suite and broader host/volume qualification remain open.

This page describes the current state.

## Current implementation

Built with DuckDB's C++ extension template against DuckDB v1.5.6 (`069cc9f9b5`) from the
`duckdb/` submodule, with `extension-ci-tools` (`v1.5-variegata`) and vcpkg `cd61e1e` (AWS SDK for
C++ 1.11.820, SQLite 3.53.2, OpenSSL 3.6.3, statically linked). Verified host: macOS 26.7, arm64,
Apple clang 21, CMake 4.1.2, Python 3.14.

Product decisions in force (repository owner):
- native-equivalent durability for unsynced writes;
- local eviction of completed S3 exports;
- the DuckLake-aligned SQL surface, with time travel over public snapshots, opt-in snapshot on
  checkpoint, and `AT` reader pins at bind time;
- community-extension distribution (Linux and macOS only);
- export readers accept any DuckDB engine line;
- exactly one store, control and export format, with no migrations before the first release;
- repositories private by default, shared between local users only when created so;
- S3 branch heads for remote readers, with one writer host per database unless it is created
  as a shared database (optional multi-host writers, metadata in S3);
- credentials from DuckDB `TYPE s3` secrets (never as options, never stored) and a
  `TYPE differential` secret for DATA_PATH and S3 options;
- S3 reads cached in DuckDB's external file cache and an optional repository disk cache.

| Area | Implementation | Evidence |
| --- | --- | --- |
| Write path | Chunk overlay + unsynced journal; one FULL commit per native sync; NORMAL on pressure/close; eager reclamation; background SQLite WAL checkpoints; WAL file truncated to 64 MiB (`journal_size_limit`) | 23-point `store_crash.py` (completed-unsynced, torn and lost journal), `store_test` (incl. a WAL grown past 64 MiB behind a reader is truncated once the reader ends; fails without the limit), `store_faults`, `write_path.py` |
| Read path | Pooled memory-mapped readers, no global lock, 32 MiB verified chunk cache, fence only for external pools | 20M-row cold scan at 8 threads: 0.28 s (native 0.11 s) |
| Manifests | 16 MiB content-addressed pages shared across roots; seal re-hashes dirty pages only | `store_test`, `write_path.py` |
| Formats | One each: store 1, control 1, `differential-export-v1` with `DIX1` index pages; anything else refused with `incompatible_format` | `write_path.py` (other store version refused); format identifiers checked on disk and in S3 |
| Collection | Trigger-maintained queues, indexed liveness, one-call cascade, automatic bounded background maintenance, lock-free stats/dry run, incremental compaction | `retention.py`, `control_races`, `write_path.py` |
| Local pools | Fan-out directories, batched publication with one device flush | `local_pools` (unit and integration), `lifecycle_crash.py` with pool |
| S3 | 8 MiB packs, paged indexes, parallel verified uploads without per-object HEAD; parallel coalesced remote reads with in-flight dedupe and read-ahead; descriptors record the writer's DuckDB version, any version accepted | `s3_product.py`, `s3_publication_faults.py`, `s3_resources.py`, `s3_tiering.py` (`duckdb-v9.9.9` export attaches and recovers), MinIO benchmark |
| Tiering | Completed exports stop pinning local payload; `differential_evict_snapshots`; S3 read-through; rehydration for branch/clone/restore; deferred export | `s3_tiering.py` |
| Capture | Generations sealed at the sync after a native header write; snapshots seal after a native checkpoint; optional snapshot on every checkpoint | `snapshot_concurrency.py`, `checkpoint_boundaries.py`, `sql_surface.py` |
| SQL surface | `ATTACH 'differential:<db>'` options, `SET differential_*`, database-first functions with named options, commit metadata, bulk expiry/eviction; every function described in `duckdb_functions()` (description, example, categories, parameter names per overload) | `test/sql/*.test`, `sql_surface.py`, `lifecycle.py` |
| Catalog | `DifferentialCatalog`: `<catalog>.snapshots()/current_snapshot()/branches()` in memory; `AT (VERSION/TIMESTAMP)` through hidden read-only attachments (LRU 8, released on DETACH, prepared statements rebind) | `time_travel.py`, `test/sql/time_travel.test` |
| Build and CI | Template layout, `make release`/`make test` (community CI), `make integration` (hooks, extension-free host), `MainDistributionPipeline.yml`, `Integration.yml` | local builds; Linux amd64 integration build, 6 C++ tests and 27 local suites passed in run `38291321428`; MinIO image pull failed before S3 tests |
| Sharing | `differential_repository_shared` creates a group-writable repository (setgid `02770` / `0660`), recorded at creation and applied to journal, locks, pins, staging and pools; private (`0700`/`0600`) by default | `shared_repository.py` (umask 000/077, joining, refusals); second OS account untested |
| S3 layout | Per database `dbs/<uuid>/`: branch heads, commit records (`snapshots/<9999999999 - n>`, newest first, parent links), data pool (`packs/`, `indexes/`); `names/<name>/<uuid>`; index pages reference packs in any database's pool. A commit-record collision (split brain) is `head_conflict`; claiming abandons such exports and renumbers | `s3_layout.py` (keys, newest-first order, record contents, clone export reusing its source's packs and reading across pools), `s3_heads.py` (copied repository incl. abandoned export and renumbering) |
| Branch heads | `dbs/<uuid>/branches/<branch>` per active branch, republished after exports, branch creation, expiry and `retry_export`; `ATTACH 'differential:s3://…/heads/…'`; `differential_remote_heads` | `s3_heads.py` (MinIO): publication, branches, deferred/out-of-order exports, restore, expiry, same-named databases, listing, errors; guarded by S3 conditional writes (`head_conflict` for heads this repository did not write last; `differential_claim_branch_heads`): copied-repository scenario in `s3_heads.py`, lost head response with an SDK retry in `s3_publication_faults.py` (fails without the fix) |
| Shared databases | `WRITERS 'shared'` (fixed at creation, S3 only). The identity is derived from the name. ATTACH reads the branch head with one GET and adopts it when the local branch is missing or unchanged: the map is imported, the objects are remote-only (`remote_objects`), and reads are batched and fetched on demand. Snapshot numbers come from the commit index (LIST, then a create-only record; unexported snapshots are renumbered, up to 8 attempts). The head is written If-Match on the writer's base, and a moved head is `branch_conflict` (the snapshot is kept and exported; the branch is recorded as diverged in `branch_conflicts` and publishes nothing more until it is reconciled). Reconciling: `differential_resolve_conflict(keep => 'branch' | 'remote')` (recovery checkpoint and preservation snapshot, optional fork `<branch>-conflict-<n>`, then the S3 head is adopted; request IDs replay). `ON_CONFLICT 'branch'` forks at once, and the branch follows S3 at its next unchanged ATTACH. Each completed DuckDB checkpoint is a snapshot published by a per-process background worker (requests coalesce per branch). The database-creating sync and explicit snapshots are excluded, and so are management captures. `PUBLISH_ON_CHECKPOINT false` turns this off. `PUBLISH_ON_DETACH` publishes when the main file closes, recording errors in `differential_status`. Unpublished snapshots are published at the next writable ATTACH. Advisory writer markers (`dbs/<uuid>/writers/<branch>/<writer>`) are written when the main file opens for writing, renewed every quarter lease by one thread per process, and deleted at close. Expired markers and the repository's own leftovers are removed at ATTACH. `ON_BUSY 'warn'` (stderr), `'error'` (`writer_busy`) or `'ignore'`; `differential_writers` lists them. History: S3 commit records are cached locally (`remote_commits`; one listing, then one GET per new record). Sync points: listing, snapshot selectors at ATTACH, and the background after a writable ATTACH; never while binding. `SNAPSHOT_VERSION`/`NAME`/`TIME` and `AT` resolve other writers' snapshots through their exports; time lookups follow the parent chain. Branches and restores import another writer's snapshot map. Expiry writes create-only `expired/` records (heads refused). Branch drops write a conditional tombstone head; unchanged copies elsewhere drop at ATTACH, and names are reusable. `differential_branches` reports `remote_state`. Operations publish only their own branch's head. A name used by a single-writer database is refused, and claims are refused | `shared_test` (unit): adopted map under the same root, nothing downloaded, batched remote reads, `Verify`, partial overwrite, renumbering of snapshot/export/operation/history/allocator, collection of remote-only objects, conflict records cleared by adoption. `test/sql/attach.test`: option and argument errors. `s3_shared.py` (MinIO, one repository per simulated host): join with no local payload and no uploads, conflict with renumbering and the kept snapshot as a branch, branch-name conflicts, fast-forward, 2×5 concurrent snapshots on two branches (distinct numbers, one commit record each), a race of two writers on one branch (exactly one wins), name and claim refusals, read-only join, a diverged branch refusing further snapshots, resolve with `keep => 'branch'` (unsnapshotted work kept as `main-conflict-4`, needs DETACH, replays by request ID) and `keep => 'remote'`, `ON_CONFLICT 'branch'` (fork, then the branch follows S3 at the next ATTACH), background publication while a session runs on (head polled from another process), `PUBLISH_ON_CHECKPOINT false`, `PUBLISH_ON_DETACH` (head moved when DETACH returns), a deferred snapshot published at the next ATTACH, writer markers (listed while a writer runs, still live after more than a lease thanks to renewal, warning, refusal, ignore, expiry after SIGKILL and removal at the next ATTACH, removal at DETACH; 3 s test lease). History across writers (listing as `remote`, attach by version and name, `AT` by version and timestamp along the ancestry), expiry for every writer and refusal for a head, branch drop and name reuse, branch and restore from another writer's snapshot. `shared_test` adds the commit cache, remote lookup, expiry and ancestry walk. `s3_shared_faults.py` (MinIO behind a fault relay, test-build failpoints `DIFFERENTIAL_SHARED_CRASH`, clock hook `DIFFERENTIAL_TEST_CLOCK_SKEW_MS`) covers: process death after renumbering, after the commit record, and before and after the head write, each recovered by `differential_retry_export` (no conflict, no second number); process death during adoption (rolled back), between the local drop and the tombstone (finished by repeating the drop; the test fails without that fix), after an expiry record, and after a resolution's fork (repeated with one fork); a lost head response and a lost commit-record response, S3 failing at ATTACH, failing writer markers, and an unreachable commit index during publication (pending, then retried); clocks two hours apart (the conflict is still detected), a writer an hour ahead invisible to `AT (TIMESTAMP => now())`, and a marker from a writer an hour behind looking expired (documented). `s3_shared.py aws` runs the functional suite against real AWS in a fresh prefix (owner-run; not yet run). Read-ahead for remote-only objects: per-file sequential streams in the store; prefetched chunks go to the reader's DuckDB external file cache and disk cache. `shared_test` checks a batch larger than one read, unchanged bytes, and no read-ahead without a reader context. `s3_shared.py`: a cold full scan on a freshly joined repository took 9 S3 GETs with read-ahead against 27 with the external file cache off, and a repeated scan took none. **Untested:** a second physical host (simulated by separate repositories on one host), real AWS (the suite's `aws` mode is ready), clock skew beyond the hook (real NTP behaviour) |
| Secrets | `TYPE s3` secrets matched by SCOPE supply credentials, region, endpoint, URL style and SSL; per-process credential registry read per request; `TYPE differential` secret (named via `SECRET`, or the unnamed default) for DATA_PATH and S3 options at creation; precedence statement > differential > s3 > default | `test/sql/secrets.test`; `s3_secrets.py` (MinIO, no AWS credentials in the environment): writers and readers, sessions without the secret, rotation, `credential_chain` with `REFRESH`, precedence, persistent secrets, errors, no credential bytes stored |
| S3 parallelism | Process-wide `differential_s3_max_requests` (1–64, default 16, chosen from the eu-central-1 run below) for uploads and reads; per-location `MAX_REQUESTS` is an optional lower cap; fetches split into ≥1 MiB requests across the limit; read-ahead per sequential stream (one per scanning thread), doubling to 4 MiB | `test/sql/differential.test` (setting bounds), `provider_configuration.py`, `s3_resources.py` (16 parallel uploads of 11 packs need 128 MiB of staging; fails with the former fixed 64 MiB quota exactly as in the owner's run); `benchmarks/s3_throughput.py`: loopback MinIO (limits 1/4/16 all ~45–52 MB/s up, ~74–92 MB/s down: no latency to hide); AWS over the owner's internet link: 200000 rows show no difference (two packs, two row groups); 2000000 rows in eu-central-1: cold scans 25 → 42 → 47 → 49 MB/s at 4/8/16/32 before per-stream read-ahead, 48 → 74 → 64 → 65 MB/s after; uploads flat at ~25–29 MB/s |
| Remote caching | DuckDB's `ExternalFileCache` per instance (verified index pages and coalesced pack spans, `differential:s3://…` paths); repository disk cache by content hash (`differential_remote_cache_size`, off by default), verified on read, LRU trim by one process; bulk copies never fill caches | `s3_cache.py` (re-attach needs only the descriptor GET, layer off, within memory_limit, new process reads from disk, modes, damaged entries, size limit, recovery fills nothing, invalid sizes); `s3_resources.py` (scan larger than memory_limit, two providers); S3 benchmark |
| Packaging | Static build exporting only `differential_duckdb_cpp_init`; hooks compiled only into `make integration`; `make bump-version`; `community/description.yml` draft; `THIRD_PARTY_NOTICES.md` generated from the pinned vcpkg packages and upstream NOTICE files (`scripts/third_party_notices.py`) | `nm -gU`; `coexistence.py` (official `aws`, `httpfs`, `sqlite` in one process) |

Verification run on the current code (macOS arm64, 2026-10-12; all passed):

```sh
make vcpkg
export CCACHE_DISABLE=1 VCPKG_TOOLCHAIN_PATH=$PWD/build/vcpkg/scripts/buildsystems/vcpkg.cmake \
  VCPKG_TARGET_TRIPLET=arm64-osx-release VCPKG_HOST_TRIPLET=arm64-osx-release GEN=ninja
make release && make test                   # All tests passed (184 assertions in 6 test cases)
make integration && make test_integration   # 6/6 CTest; 27 suites, 122 PASS lines
make test_minio                             # 102 PASS lines: SDK faults, product, publication faults,
                                            # resources, tiering, branch heads, S3 layout, shared
                                            # databases and their faults, documentation examples,
                                            # secrets, caching, coexistence, example, capability
```

MinIO registry replacement (2026-10-13):

- `gh run view 38291321428 --job 114951862323 --log-failed` confirmed the Linux amd64
  integration build, 6/6 C++ tests and all 27 local integration suites passed on 2026-10-12.
  The job then failed pulling `minio/minio` from Docker Hub with `pull access denied`,
  before running any S3 suites.
- `docker manifest inspect quay.io/minio/minio@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`
  and the equivalent `:RELEASE.2025-09-07T16-13-09Z` lookup both failed with `no such manifest`.
- Switched CI and the disposable test runner to upstream RELEASE.2025-10-15T17-29-55Z,
  [built from source by coollabsio/minio](https://github.com/coollabsio/minio), pinned to
  `ghcr.io/coollabsio/minio@sha256:69b55a1c1c5dc285ce04db96689f5b2102317fc77a50680a1874ca6efd1c87f9`.
  `docker pull` of that exact reference passed. `docker manifest inspect` of it with a
  temporary empty `--config` directory passed anonymously and confirmed linux/amd64 and
  linux/arm64 images. This is a different upstream release and image build, not a mirror
  of the old digest.
- `make test_minio` passed on macOS arm64 with the new image (102 PASS lines), including
  protocol faults, SDK capabilities, publication/recovery, shared databases and their faults,
  documentation examples, secrets, caching and coexistence. Python syntax and matching
  image pins were checked; `git diff --check` passed. No C++ rebuild was needed.
- Evidence: `build/evidence/minio-ci-failure-2026-10-13.log`,
  `build/evidence/minio-ghcr-manifest-2026-10-13.json`,
  `build/evidence/minio-ghcr-2026-10-13.log` and `build/evidence/minio-sdk-results.json`.
  The replacement has not yet run in hosted CI; Linux S3 and real-AWS qualification
  remain open. Registry availability is verified as of this check, not guaranteed indefinitely.

Format identifiers were checked on the output: `engine.sqlite` has `user_version` 1 and
`control_format` 1, descriptors start with `differential-export-v1`, and index pages with `DIX1`.
An independent Python decoder rebuilt exported snapshots from descriptor → index → packs,
byte-identical to their local roots, and recomputed page, root and chunk hashes from
`engine.sqlite`. That was on 2026-10-10, before the identifiers were renamed; the layouts are
unchanged.

The SQL in `README.md` and `docs/lifecycle.md` was executed as documented against the release
loadable (S3 sections against MinIO, with credentials from `TYPE s3` secrets only). The SQL blocks of `docs/sql-reference.md` and `docs/shared-databases.md` are executed as written
by `test/integration/doc_examples.py` (part of `make test_minio`): 35 and 22 blocks, plus expected
errors and expected outputs (`in_sync`, `diverged`, `main-conflict-6`, `published_as_branch`). Five
blocks are marked "not run": installation and credentials, placeholders for export locators and
UUIDs, and a refusal that needs a concurrent writer. User documentation is written as if `differential` were a
published community extension; **it has not been submitted or published**.

Release loadable: about 40 MB (DuckDB statically linked, like DuckDB's own extensions),
metadata `v0.1.0` / `v1.5.6` / `osx_arm64`, one exported symbol. Benchmarks: `docs/benchmarks.md`.
Indicative `AT` cost (release build, 1M-row table): live point query 0.46 ms, first AT use
1.55 ms, repeated AT 0.55 ms.

Not verified: any Linux build (all non-AWS sources pass a syntax-only g++ 13 compile on Ubuntu
24.04), macOS amd64, either CI workflow (needs a push), Windows/wasm (excluded), the AWS capability
and fault suites (real AWS was only exercised through the product paths below).

Real AWS (owner-run, us-east-1, macOS arm64 laptop over the internet, named profile, 2026-10-11):
- The lifecycle guide's S3 flow: export on snapshot, branch-head attach from a fresh repository,
  `differential_remote_heads`, re-attach after a new snapshot.
- `benchmarks/s3_throughput.py --rows 200000 --requests 4,8,16,32` (13.8 MB per database, three
  cold scans each, no SDK retries), same laptop and link:

  | Region | Export (5 PUTs) | Upload | Cold scan (median) | Download | ATTACH |
  | --- | ---: | ---: | ---: | ---: | ---: |
  | us-east-1 | 4.3–5.1 s | 2.7–3.3 MB/s | 3.3–3.6 s | 3.8–4.2 MB/s | ~1.0 s |
  | eu-central-1 | 1.0–2.1 s | 6.6–13.4 MB/s | 1.3–1.8 s | 7.8–10.3 MB/s | 0.23–0.42 s |

  Latency, not bandwidth, sets these numbers (the same link is 3× faster to the nearer region),
  and the request limit made no difference in either region because this fixture offers almost
  no parallel work: an export has two 8 MiB packs, and a 200000-row table has two row groups,
  so DuckDB scans it with about two threads.
- The same with `--rows 2000000` in eu-central-1 (about 136–144 MB per database, 20 PUTs, three
  cold scans each, no SDK retries), after upload staging was sized to the limit:

  | max_requests | Export | Upload | Cold scan (median) | Download | GETs |
  | ---: | ---: | ---: | ---: | ---: | ---: |
  | 4 | 4.79 s | 28.4 MB/s | 5.36 s | 25.3 MB/s | ~542 |
  | 8 | 5.87 s | 24.5 MB/s | 3.40 s | 42.4 MB/s | ~560 |
  | 16 | 4.64 s | 29.8 MB/s | 2.94 s | 47.0 MB/s | ~547 |
  | 32 | 4.83 s | 28.2 MB/s | 2.78 s | 49.0 MB/s | 542 |

  Uploads are flat at about 28–30 MB/s (the link's upstream). Cold scans gain 1.7× from 4 to 8
  requests, then +11% at 16 and +4% at 32, levelling off near 50 MB/s with 8 DuckDB threads.
  GETs average about 250 KB (one DuckDB block each). One laptop and link only. The owner set the
  default to 16 (near the plateau; 128 MiB of upload staging).
- Read-pattern investigation (MinIO behind a 30 ms round-trip proxy, the same 2,000,000-row
  table, release build): a cold full scan makes 526 reads of one 256 KiB block each. With 8
  threads, 94% of reads continue the previous read of one of the scanning threads, but only 8 of
  526 continue the previous read of the file, so read-ahead (one stream per file) never engages.
  Each thread waits one round trip per block: 2.7 s with 8 threads, 16.6 s with 1 thread (1.2 s
  and 2.2 s without added latency). This matches the owner's plateau (8 threads × 256 KiB per
  round trip ≈ 50 MB/s). `storage_block_prefetch = 'ALWAYS_PREFETCH'` changes nothing (DuckDB
  batches blocks per 2048-row vector, i.e. one block here). Known issue: in single-threaded
  sequential scans, read-ahead re-requested its whole window on every read (95,840 memory-cache
  lookups of 64 KiB for 2,077 chunks) and still cost one round trip per read.
- Fixed by read-ahead per sequential stream (one per scanning thread, up to 16 per file; a read
  that needs unrequested data also fetches the stream's next window, doubling to 4 MiB; each chunk
  requested once). Same experiment after the change (two runs each, same bytes downloaded):

  | Cold full scan | 30 ms round trip | No added latency | GETs | Memory-cache hits |
  | --- | ---: | ---: | ---: | ---: |
  | 8 threads, before | 2.7 s | 1.2 s | 540 | 642 |
  | 8 threads, after | 1.2 s | 1.0 s | 199 | 2,829 |
  | 1 thread, before | 16.6 s | 2.2 s | 404 | 95,907 |
  | 1 thread, after | 3.0 s | 1.2 s | 179 | 2,480 |

  `s3_cache.py` checks that read-ahead engages with 1 and 4 threads, requests each chunk at most
  once, and stays off without a cache.
- Owner's eu-central-1 re-run after the change (`--rows 2000000`, three cold scans each):

  | max_requests | Cold scan before → after | Download before → after | GETs before → after | SDK retries |
  | ---: | ---: | ---: | ---: | ---: |
  | 4 | 5.36 → 2.88 s | 25.3 → 47.9 MB/s | ~542 → ~200 | 0 |
  | 8 | 3.40 → 1.84 s | 42.4 → 74.0 MB/s | ~560 → ~198 | 0 |
  | 16 | 2.94 → 2.11 s | 47.0 → 64.3 MB/s | ~547 → ~200 | 0–2 per scan |
  | 32 | 2.78 → 2.10 s | 49.0 → 64.6 MB/s | 542 → ~196 | 0–1 per scan |

  Uploads unchanged (~25–29 MB/s, the uplink). Downloads now level off around 65–75 MB/s, likely
  the link's downstream; 8 requests were consistently fastest on this link (72.8–74.7 MB/s), and
  16/32 showed occasional SDK retries. A second run the same day reproduced the order (median cold
  scan 50.3 / 72.5 / 68.5 / 61.5 MB/s at 4 / 8 / 16 / 32) with no retries; two of its 12 ATTACHes
  took 3.3 s and 5.3 s instead of ~0.3 s (cause not visible in the counters then). ATTACH is now
  timed step by step (`attach_resolve`, `branch_head_resolve`, `remote_descriptor_open`,
  `s3_client_create`, `s3_credentials`, and the SDK's per-request `s3_dns_lookup`, `s3_connect`
  (DNS + TCP + TLS), `s3_first_byte`, each with a maximum), and `s3_throughput.py` prints the
  breakdown of the slowest ATTACH per limit. (The SDK's own TLS latency is reported in nanoseconds
  with current curl, despite its millisecond contract, so it is not used.)
- First owner run with the breakdown (eu-central-1, 12 ATTACHes, 0.25–0.34 s, no outliers this
  time): `attach_resolve` 23–32 ms, `remote_descriptor_open` 70–122 ms, two new connections per
  ATTACH with DNS 1–4 ms and TLS about 25 ms each, six requests with time to first byte 52–67 ms
  at most, credentials and client creation under 6 ms. Median cold scans 51.2 / 75.7 / 65.7 /
  57.7 MB/s at 4 / 8 / 16 / 32.
- ATTACH round trips (2026-10-12): descriptors are read with one GET (no HEAD) and embed the
  first index page, and verified descriptors are kept in process memory and the disk cache. On
  MinIO behind a 30 ms round trip the slowest ATTACH went from 0.24 s (5 requests, descriptor open
  71 ms) to 0.16 s (3–4 requests, 37–38 ms). `s3_cache.py` now expects a re-attach in the same
  instance, and a new process with the disk cache, to need no GET at all. The descriptor cache
  is keyed by repository: `s3_cache.py` checks that a second DuckDB instance with its own
  repository in the same process fetches the descriptor itself (it did not before the fix).
- Slower exports at 8 requests in the owner's runs (always the second limit of `4,8,16,32`): not
  reproduced locally through a proxy with a 30 ms round trip and a 30 MB/s upload cap (1.5M rows,
  orders `4,8,16,32` and `8,4,16,32`: every limit 4.8–5.5 s, about 95% of it S3 upload). The
  benchmark now reports export steps (`snapshot_publish`, `manifest_seal`, `local_sync`, `export`,
  and per pack `export_pack_build`, `export_stage`, `export_put`, `export_verify`) to locate it on
  the real link.
- Owner run with order `8,4,16,32`: 8 was fast (4.58 s) and 4 slow (6.07 s), so the slowdown
  follows the second position, not the value. In that export the measured steps covered about
  3.9 s of 6.07 s. Two ATTACHes took 3.25 s and 5.24 s, almost all in `attach_resolve` (local);
  DNS, TLS and S3 were normal. `F_FULLFSYNC` does not stall behind unflushed writes on the owner's
  Mac (4–13 ms after 400 MB of dirty data). Further timers added to locate both:
  `attach_repository_open`, `s3_secret_lookup`, `s3_provider_registration`,
  `snapshot_checkpoint`, `branch_head_publish`.
- Cause found with those timers (owner run, 2026-10-12): the slow second export spent 2.1 s in
  `branch_head_publish` and the slow ATTACH 3.0 s in `s3_provider_registration`, both in creating
  an SDK client (`s3_client_create` max 3,016 ms). The SDK's default client configuration asks the
  EC2 instance metadata service for a region whenever none is configured in the environment or
  profile (the owner's case); off EC2 that request usually fails at once but sometimes waits for
  its timeout. Reproduced with a slow fake metadata endpoint: up to 10 s per client, 22 requests
  per process. Fixed by creating client configurations with the metadata service disabled (the
  region is always explicit), building the SDK's default credential chain once per process and
  only when used, and keeping the 8 most recent S3 clients alive (an export's branch-head update
  and consecutive ATTACHes reuse them). With a named profile: no metadata requests and 1.4 ms per
  client; without one, the default chain's providers still query it once per process.
  `s3_resources.py` checks the named-profile case.
- Owner run after the fix (eu-central-1, 2,000,000 rows, `4,8,16,32`): all 12 ATTACHes 0.20–0.28 s
  with 4 requests each and client creation ≤ 1.5 ms; all exports 4.0–4.8 s (28–34 MB/s up) with
  branch-head updates of 66–69 ms; median cold scans 49.3 / 70.2 / 67.3 / 76.4 MB/s; one SDK retry
  in 12 scans.

## Reproducible commands and evidence

Run from the repository root (see README#development):

```sh
git submodule update --init
make vcpkg
export VCPKG_TOOLCHAIN_PATH=$PWD/build/vcpkg/scripts/buildsystems/vcpkg.cmake GEN=ninja
make release && make test
make integration && make test_integration
make test_minio
python3 examples/local.py && python3 examples/local.py --external-pool
DIFFERENTIAL_EXTENSION=$PWD/build/release/extension/differential/differential.duckdb_extension \
  python3 benchmarks/{local,lifecycle,operations}.py   # plus benchmarks/s3.py <minio> <dir>; report.py
```

The integration runner passes all **five CTest suites** and every listed suite against the
loadable artifact. Tests cover
250 oracle cases, 23 store process-death points, three native allocation sizes,
eight external-pool publication points, 48 retention/publication races, 16 writer
management races, and compound recovery across default/external/retired-target fixtures.
Counts describe selected test schedules, not an exhaustive state space.

The eleven SDK protocol cases pass, including reflected-error redaction and bounded
retries/concurrency. Pinned MinIO passes SDK capabilities, multipart completion/abort,
product publication, historical native schemas, uncertain export retention,
writer-catalog loss, explicit recovery, staging/cache bounds and fault retries
(`test/integration/minio.py`, `s3_faults.py`). A larger native
scan larger than `memory_limit` stays correct while DuckDB's external file cache stays within it.
These are compatible-endpoint and HTTP fault tests, **not real AWS**.

The artifact is built like DuckDB's own extensions (statically linked, single exported entry
point); distribution is through the community repository, which signs the builds.

Raw benchmark results are written locally to `benchmarks/*-results.json` (not tracked);
`docs/benchmarks.md` reports
native/default/external local comparisons, lifecycle sharing, commit/maintenance
costs, selective/full/concurrent S3 scans and limitations. No sync setting is weakened.

## Acceptance checklist

Tests and evidence below are the current release checklist; partial rows remain release
gates.

| ID | Phase | State | Evidence / qualification |
| --- | --- | --- | --- |
| AC-01 | 0 | PASS | DuckDB 1.5.6 CLI/library load of the loadable artifact (extension-free integration host; Phase 0 also used the official and Homebrew binaries, see feasibility.md). |
| AC-02 | 0–1 | PASS | Main file plus .wal/.wal.checkpoint/.wal.recovery traces, detach and fresh-process reopen. |
| AC-03 | 0–2 | PASS | Native comparison suite, transactions, constraints, error/rollback cases. |
| AC-04 | 2 | PASS | native_matrix.py at 16/64/256 KiB allocation; qualified_catalog.py native-copy comparison. |
| AC-05 | 2 | PASS | Historical schema/data after ALTER, DROP/recreate, index/default changes; local and remote fresh readers. |
| AC-06 | 2 | PASS | Rollback/management transaction guards and concurrent public checkpoint: old sum 1, snapshot 3, live 6. |
| AC-07 | 2 | PASS | Interleaved branch allocator, reserved retired names, restore and signed-BIGINT exhaustion. |
| AC-08 | 2 | PASS | New IDs share unchanged roots; native checkpoints do not publish; unchanged export makes one descriptor PUT. |
| AC-09 | 2 | PASS | Eight branches/eight clones each add zero payload bytes/objects; map metadata grows. |
| AC-10 | 2 | PASS | Parent/branch/clone/sibling writes isolated; clone survives source branch deletion and retirement/GC. |
| AC-11 | 2 | PASS | Preservation/restoration IDs; four crash points in default pool, additional pool, and retired-target variants. |
| AC-12 | 2 | PASS | Live attachment rejection, preservation/tombstone/name reuse, sole historical pin, 16 writer races and pending-deletion regression. |
| AC-13 | 2–3 | PASS | Retirement rejects new attachment; existing native readers survive retirement/deletion/GC. |
| AC-14 | 3–4 | PASS | Exact dry run on a read snapshot; bounded, replayed, queue-driven local GC with one-call cascade and automatic background steps; local, in-flight, recovery and pending export roots retained; completed exports remain remotely retained. |
| AC-15 | 3 | PASS | 36 pin/retire/GC/compaction and 12 capture/publication/GC races; external-payload fence read/GC test. |
| AC-16 | 1 | PASS | 250-case byte oracle across overlay, NORMAL and synced states; arbitrary ranges/holes/truncate/regrow, block reuse and three allocation sizes. |
| AC-17 | 1–3 | PASS — process death | 23 store cases (including completed-unsynced writes, torn journal, lost-journal emulation), eight external-pool publication points plus journal replay, native WAL/header/namespace boundaries, compound/GC/compaction crashes. |
| AC-18 | 1–4 | PARTIAL | Acknowledged commits are published with SQLite FULL + `F_FULLFSYNC` at every native sync and survive tested process failures; completed unsynced writes survive process death via the journal. Physical power-loss/torn-sector model untested. |
| AC-19 | 0–1 | PASS | Small WAL commits, large/automatic checkpoints with and without WAL, concurrent checkpoint/recovery sidecars. |
| AC-20 | 0–2 | PASS | Second writer process rejection, stale epoch/pending-intent fence, clean/crash reopen, writer/management races. |
| AC-21 | 0–2 | PASS | LOAD/EXPLAIN/PREPARE have no persistence; execution once, ANALYZE effects, autocommit guards, cancellation after publication. |
| AC-22 | 2–4 | PASS | Durable request replay/mismatch rejection across lifecycle, compound, GC/compaction, export loss, data-path registration and retry after branch deletion. |
| AC-23 | 4 | PASS | SDK-only S3 transport, credential chain/signing/retries/conditional/multipart, source review. |
| AC-24 | 4 | PARTIAL | Pinned MinIO capability/product/fault suites pass. Real AWS: owner-run product paths (export, cold reads, branch heads) and the throughput benchmark pass on us-east-1; the capability suite (`existing_provider.py`) and fault behaviour are not run on AWS. |
| AC-25 | 4 | PASS — same host | Writer directory deleted; another process/repository reads identical native schema/data using descriptor/access only. Separate physical host unrun. |
| AC-26 | 4 | PASS | 250k sorted fixture, three history/fragmentation levels; selective cold reads 0.41–0.54 MB of 9–19 MB logical with 5–8 GETs across runs; warm query zero GETs; pack ranges coalesced. |
| AC-27 | 4 | PASS — tested bounds | `differential_s3_max_requests` parallel SDK requests per process (default 16, 1–64; optional per-location `MAX_REQUESTS`), in-flight dedupe, memory cache within `memory_limit` (DuckDB's external file cache), size-limited repository disk cache (off by default), upload staging quota of 8 MiB per parallel request (64–512 MiB), 8 MiB packs, bounded descriptors/indexes and streamed metadata. |
| AC-28 | 1–4 | PARTIAL | Range/corruption/denial/throttle/timeout/multipart/staging faults and injected SQLite FULL pass. Physical ENOSPC/volume fault tests unrun. |
| AC-29 | 4 | PASS | Dependency-first checksummed publication of packs, indexes and descriptor; pack denial, corrupted pack verification, descriptor denial, lost reply, exact retry. |
| AC-30 | 4 | PASS — MinIO | No writer metadata; writable recovery copies into new identity/allocator, default and additional local pools. |
| AC-31 | 3 | PASS | Transactional physical page compaction with unchanged roots; shared manifest pages; two crash/retry points, live readers, durable report and pending export graph retained. |
| AC-32 | 5 | PASS | Identity/lineage, local/public/evicted/remote states, logical/provider bytes, sync/namespace/pin timings, GC queues, write journal, WAL checkpoints, cache/coalescing/prefetch/pack metrics and stable error categories. |
| AC-33 | 0 | PASS | Built with DuckDB's extension template from unmodified v1.5.6 source; the artifact exports one symbol; no copied engine, fork, daemon, FUSE or core patch. |
| AC-34 | 5 | PASS — scoped review | SDK-only and no-secret source review; generated response canary absent from errors/stdout/stderr; fault hooks absent from release builds (SQL test asserts `test_hooks = absent`); coexistence with official `aws`/`httpfs`/`sqlite` in one process passes (macOS). Independent audit unrun. |
| AC-35 | 2 | PASS | Injected-clock ties, retirement, branch creation/interleaving/restore/deleted UUID and no-match cases. |
| AC-36 | 4 | PASS — MinIO | Descriptor upload process death, retirement/GC/compaction, publishing-branch deletion and retry retain a readable exported graph; evicted and retired exported snapshots read and rehydrate from S3. |
| AC-37 | 2–4 | PASS | Offline import rejects pending WAL/live source; double validation under native lock; export recovery new identity and own pool copy. |

## Remaining gates and operational limits

There is **no proven stock-DuckDB integration blocker**, and no core patch was adopted.
Resolved defects and before/after reproductions are recorded in `feasibility.md`.
The remaining gates are not a local-only implementation milestone:

1. **AC-24, real AWS:** product paths were exercised by the owner; the capability suite is still open. Run
   `test/integration/existing_provider.py --bucket <existing> --prefix <owned>/
   --region <region> --report <path>` with SDK credentials configured outside chat.
   It creates a random child namespace, exercises the SDK capability suite, and
   reads an export after deleting its own temporary writer catalog. It creates no
   infrastructure. Exports remain retained. Run the fault/product suite separately;
   a capability result alone does not qualify every AWS failure path.
2. **AC-18/28, volume failures:** process death and SQLite FULL injection pass. Physical
   ENOSPC, device power loss, torn sectors and recovery after actual host loss were not
   tested. The local profile relies on SQLite and durable local volumes honoring sync.
   This host accepts file and directory `F_FULLFSYNC`; unsupported sync fails closed.
3. **Release qualification:** macOS arm64 and pinned MinIO are verified; Linux amd64's
   integration build, C++ tests and local integration suites passed in CI. Linux S3 tests,
   Linux arm64 and macOS amd64 qualification, a successful complete hosted CI run, an actual second
   physical reader host and an independent security audit are unrun. Windows and wasm are
   unsupported. No release was published, the community descriptor was not submitted, and no
   billed cloud infrastructure was provisioned.
4. **Performance qualification:** modest fixtures and loopback measurements are not
   production capacity limits. Deep-history/large-metadata-store stress, WAN latency,
   physical-disk-cold measurements and complete wire-I/O attribution remain unmeasured.

Implemented limits remain visible in status/API/recovery documentation:

- Local commits; only successfully published S3 snapshots are remotely durable.
  Commit acknowledgement is never undone by snapshot/export failure.
- One host-local metadata store per repository and one writer process per branch. No
  automatic multi-host failover or writable continuation of a lost identity.
- Immutable reader attachments reopen to refresh. Retired snapshots reject new local
  attachments; exported descriptors are not revoked by local retirement.
- Remote exports are retained indefinitely. Pending exports retain their local graph;
  completed exports do not (local copies are collectible after retirement or explicit
  eviction). No remote expiry or distributed garbage-collection claim.
- Same-pool forks/clones copy maps, not payload; clones have new identities/allocators.
  Full indexed maps avoid history walks but copy metadata proportional to file extents.
- GC is queue-driven with bounded work per call; explicit dry runs still scan the
  repository (on a read snapshot). Compaction reclaims SQLite physical pages with
  unchanged roots; external chunks are individually collectible.
- 64 KiB chunks, 16 MiB manifest pages, 1 TiB logical-file cap, 64 MiB write overlay,
  32 MiB local verified-chunk cache, 8 MiB S3 packs, remote reads cached in DuckDB's external
  file cache (memory_limit) and an optional repository disk cache, upload staging of 8 MiB per parallel request (64–512 MiB), `differential_s3_max_requests` (default 16) parallel S3 requests per process. Provider request,
  retry, timeout, range and multipart policies are persisted and introspectable.
- Product objects (packs, indexes, descriptors) stay below the multipart threshold and
  never invoke multipart. Larger-object transport
  multipart is tested but lacks durable process-death reconciliation; it is not a
  path used by the snapshot format.
- Pending import names and missing pin-file metadata retain conservatively until
  successful retry/reconciliation; no abort-import or unsafe expiry command exists.
- One store format (1), control format (1) and export format (`differential-export-v1`); any
  other format is refused, with no migrations. Additional pool replicas of a shared hash remain until its
  final global mapping reference is gone. Keep the metadata store and payload volumes.
