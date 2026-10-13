# Feasibility evidence

2026-10-09, macOS 26.7 (25G229), arm64, Apple clang 21.0.0, CMake 4.1.2,
Release extension build. No DuckDB engine was rebuilt or patched.

DuckDB source: `069cc9f9b5be802405797faecc284961b07c70ef` (`v1.5.6`).
Both stock binaries report `v1.5.6,069cc9f9b5,Variegata`, platform `osx_arm64`.

| Host | SHA-256 |
| --- | --- |
| Upstream universal release CLI, arm64 slice executed | `2c75d4758c9d74cb0f4642f4e8d3c1a13e8a888a1e475fffd90934ad5ae3dd17` |
| Homebrew 1.5.6 CLI | `d485672a16b9f167a33e14ad4ce350abebc38e357c064faa9de84eaa09464391` |
| Homebrew 1.5.6 dylib (C API sidecar host) | `b36d22b47c649b266380d02e4d0940e4b7a5c38fe4bf5370b777742386650c8a` |

Phase 0 ran `test/integration/phase0.py` against both stock CLIs (`--duckdb <binary>`).
Upstream CLI obtained from the v1.5.6 `duckdb_cli-osx-universal.zip` release asset.
Always use `-init /dev/null` to avoid user startup SQL. The hardened upstream CLI
requires an absolute extension LOAD path; a relative path was rejected by dyld.
No metadata-mismatch override was used. Unsigned development loading is required.

Both hosts passed 12 initial probes (`test/integration/phase0.py`).
Coverage: native and TYPE-alias ATTACH; CTAS, INSERT/UPDATE/DELETE, MERGE, UPSERT,
ALTER, view/index/sequence/defaults, commit, rollback, constraint error, checkpoint,
fresh-process reopen, fixed historical capture, EXPLAIN/PREPARE execution timing,
second process writer rejection, SIGKILL after commit, and four interrupted
WAL/data/header/cleanup transitions. Each run writes a fresh trace under `build/evidence/`.

`nm -gU ... | c++filt` confirms native constructors, registration, RTTI/vtables;
the extension instantiates them successfully, which is stronger than header checks.

Pinned source inspected: filesystem API; storage-extension hooks; native attachment;
transaction manager and transaction context; storage manager; single-file block
manager; WAL replay; standard buffer manager and native checkpoint table function.
In particular, `.wal.checkpoint` and `.wal.recovery` are constructed by the engine;
concurrent creation of the former and merge/rename recovery through the latter
are now observed in both pass-through and durable-store traces. Loaded native blocks can bypass the filesystem cache.
WAL replay can depend on persistent data blocks: the backend must retain both.
No supported block-manager factory has been assumed or implemented.

Phase 0 establishes a credible stock-extension path on macOS arm64. The initial
pass-through uses local native files and whole-file capture; the subsequent durable
backend stores native bytes in transactional immutable chunks and seals maps.
`sidecars.py` uses the unmodified Homebrew library through C API/ctypes, holds an old
reader, starts a concurrent checkpoint, commits to the checkpoint WAL, kills the
process and verifies recovery through `.wal.recovery`. Both backends recover all
three rows. `checkpoint_boundaries.py` observes WAL-first and WAL-skipping automatic
checkpoints and a skipped end callback on exception; earlier commits survive.

The durable integration suite and extended SQL matrix pass at 16/64/256 KiB native
allocation sizes. `block_reuse.py` verifies six reused native offsets while all
30,000 historical rows and payload hashes remain intact. Mapping transaction crashes
are exercised by `store_crash.py`. These are intermediate gates, not full v1 acceptance.

A fail-closed URI regression caught the default filesystem canonicalizer turning an
unconfigured `diff://` into local `diff:/`; explicit absolute/canonical URI handling
now prevents that fall-through. Pinned `extension_load.cpp` separately treats a
single-colon `diff:/...` as a storage-extension prefix before filesystem routing;
use the documented `://` URI and TYPE alias. Malformed typed URIs, traversal and
independent live read-only attachments are rejected by `uri_status.py`.

Subsequent phases implement snapshot publication, native historical reads, lifecycle,
retention, cancellation and remote recovery. The full local and MinIO suites now
exercise them.
No stock-symbol blocker or need for a core patch has been found. Linux/Windows,
real AWS, a separate physical reader host and physical power-loss tests remain unrun.

Later implementation defects were resolved without changing the delivery contract:

- A thread-local recovery lease failed when native work moved to another thread.
  ClientContext registered state now carries ownership through FileOpener; default
  and additional-local-pool compound crash/retry suites pass.
- A crashed pending branch deletion allowed deletion of the only unfenced survivor.
  Reproduce with `make integration && python3 test/integration/pending_deletions.py`; the
  test fails on the old behaviour.
  Deletion now accounts for other pending deletion intents atomically. The retry
  succeeds once and the surviving branch remains writable. This affected AC-12/20/22;
  it was an implementation defect on the stock identity recorded above.
- A pending export could outlive its publishing branch. The execution-only
  `differential_retry_export` function resumes its immutable root without an
  attachment or new ID. MinIO verifies retirement, GC, branch deletion, retry and
  catalog-free reading. No change to native SQL or durability was necessary.

2026-10-10 architecture revision. Facts established from the pinned
source and the stock binaries:

- Native durability points: the WAL is synced before a commit is acknowledged
  (`WriteAheadLog::Flush`); optimistically written row-group blocks are synced before the
  WAL references them (`DuckTransaction::WriteToWAL`); checkpoint blocks are synced before
  and after the alternating header write (`SingleFileBlockManager::WriteHeader`). The
  buffered write path publishes at exactly these `FileSync` calls.
- `HAVE_FULLFSYNC` is never defined in the 1.5.6 tree, so stock `LocalFileSystem::FileSync`
  on macOS is a plain `fsync`. Measured on this host: native single-row commits take about
  0.16 ms, raw `F_FULLFSYNC` about 3.3 ms, and a SQLite FULL+fullfsync commit about 3.5 ms.
  The extension keeps `F_FULLFSYNC` at native sync points (stronger than native).
- `ExtractExtensionPrefixFromPath` ignores `scheme://` paths, so a plain
  `ATTACH 'diff://…'` uses the native storage type. Generations are therefore detected
  below the filesystem (the sync after a header write at bytes 4096–12287), which works
  with or without `TYPE differential`.
- `DBConfig::AddExtensionOption` and `FileOpener::TryGetCurrentSetting` are exported by the
  stock CLI and Homebrew library (`nm -gU`); `differential_auto_gc` uses them.
- On a 20M-row table, cold parallel scans had been limited by SQLite reading each 64 KiB
  blob as 16 separate `pread` calls (profiled with `sample`). Memory-mapped reader
  connections removed that bottleneck: 8-thread scans take 0.28 s, versus 1.75 s without
  them and 0.11 s native.
