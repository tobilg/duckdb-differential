# Security and ownership review

Review scope: the extension source, checked-in test harnesses/examples and local
package recipe on the recorded macOS arm64 host. This is an implementation review
and fault test, not an independent security audit or a guarantee about another
extension's AWS SDK runtime/logging configuration.

- S3 requests, credentials, signing, retries and conditional/multipart operations use
  the pinned AWS SDK. Production code has no HTTP client, shell AWS invocation or
  independent signing implementation. The HTTP servers in tests inject protocol
  faults; MinIO health polling is fixture setup.
- Provider metadata stores a credential profile reference or selects the SDK chain.
  Credentials are accepted only through DuckDB secrets (`TYPE s3`), never as ATTACH or
  function options. The extension reads a matching secret at every ATTACH and management
  call and keeps its keys in process memory only, keyed by provider location; they are not
  written to the metadata store, descriptors, heads, listings, status or errors
  (`s3_secrets.py` scans every repository file and that output for the secret). DuckDB
  itself stores `PERSISTENT` secrets unencrypted in `~/.duckdb/stored_secrets`; the docs
  recommend temporary or `credential_chain` secrets. Within one process, the latest lookup
  for a location applies to all DuckDB instances using it. Endpoint user information,
  query strings and fragments are rejected. TLS verification stays enabled; an s3 secret
  with `USE_SSL false` opts into HTTP like `ALLOW_HTTP`.
- Export descriptors contain format/engine versions, UUIDs, IDs, timestamps,
  lengths and object/index hashes. They contain no credential material or SQL data.
  Payload objects necessarily contain the user's native database contents; they
  require normal filesystem/bucket access controls and are not encrypted by this
  extension. No S3 ACL/publication change is made.
- Default errors retain operation category and numeric HTTP/SDK codes. They omit
  reflected provider messages and signed URLs. The protocol suite includes a random
  private-response canary and checks both errors and process output. All eleven
  protocol scenarios passed; no access-key, secret-key or response canary appeared.
- Native I/O tracing, failpoints, export/checkpoint gates and raw-path probes exist
  only in test builds (`-DDIFFERENTIAL_TEST_HOOKS=ON`). Release builds compile them out:
  the environment variables have no effect, the diagnostic checkpoint function is absent,
  and the packaging script refuses a hooks build. In test builds the trace records
  operation, managed URI, byte length and offset, not buffer contents or SQL values.
- The unsynced write journal (`<repository>/journal/`, mode 0600) holds recently written
  native bytes until they are published, so it carries database contents like the
  metadata store itself. It is removed after a clean close or a replay.
- Repositories are private by default: directories 0700, files and staging 0600. A repository
  created with `differential_repository_shared` is group-writable (setgid 02770 directories,
  0660 files) for its directory's group. Every member of that group can read and modify all
  databases in it, so use a dedicated group. The mode is fixed at creation, and modes are set
  explicitly, independent of the umask.
  Owned local payload namespaces include repository and provider UUIDs. Paths are
  canonicalized at provider registration. Object names are checked SHA-256 hex;
  payload opens reject symlinks. GC uses persisted reachability and owned namespaces,
  not prefix-wide deletion. Missing pin files conservatively retain their roots.
- Reader connections memory-map the metadata store file. Only SQLite modifies it; an external
  actor truncating it is outside the ownership contract, as is any other modification of
  owned files.
- The optional remote read cache (`<repository>/cache/`) holds verified S3 chunks and index
  pages, i.e. database contents, with the repository's file modes. Entries are named by and
  re-verified against their SHA-256 on every read, so a modified entry is discarded, not
  served. It is never authoritative.
- Local export staging is isolated by repository UUID. Quota accounting and stale
  cleanup use process locks. Authoritative maps/bytes and uncertain export roots
  are never treated as evictable cache.
- A pattern scan of source, scripts, examples, tests and documentation found no
  checked-in AWS access-key IDs, secret-key assignments or private-key blocks.
  Test credentials/canaries are generated per disposable fixture and are not logged.

- S3 branch heads (`dbs/<uuid>/branches/…`) and, for shared databases, advisory writer markers
  (`dbs/<uuid>/writers/…`: host name, process ID and repository UUID of each writer session;
  nothing depends on them for correctness) are the only mutable objects; they are not
  content-addressed.
  The extension writes them conditionally and never overwrites a head another writer wrote
  (`head_conflict`), but anyone with write access to the prefix can still repoint a head with
  other tools. Readers check that a head matches
  its key and that its descriptor describes the same database and snapshot, so a bad head can
  only point at another export of that database, which is itself verified. Protect write access
  to the prefix with bucket policies as for the exports.

Remaining verification: real AWS IAM/profile behavior (including SSO and refreshed
`credential_chain` secrets outside MinIO), access by a second OS user to a shared
repository, an independent release audit,
SDK coexistence, other filesystems/platforms, and hostile changes by an actor who can
already rewrite the authoritative repository or its owned directories. Do not modify
owned files outside the extension or copy a metadata store to create another writer.
