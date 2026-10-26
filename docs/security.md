# Security and ownership

Differential follows the filesystem and S3 permissions of the process running DuckDB.
A repository is an ownership boundary, not a tenant isolation boundary. See
[deployment](deployment.md) and [multitenancy](concurrency-and-multitenancy.md) for process,
user and host layouts.

## Credentials and transport

S3 requests, signing, retries and conditional operations use the pinned AWS SDK for C++.
Credentials come from a matching DuckDB `TYPE s3` secret, a named profile or the SDK chain.
ATTACH and management options do not accept access keys or secret keys. Secret lookups happen
at each ATTACH and management call; keys stay in process memory and are not written to
Differential metadata, descriptors, heads, listings or status. Within a process, the latest
lookup for a provider location applies to all DuckDB instances using that location.

DuckDB owns the storage of its own secrets. Prefer temporary secrets or credential-chain
providers where persistent plaintext secrets are unsuitable for the deployment. Do not put
credentials in SQL scripts, command history, documentation or reports.

TLS certificate verification is enabled. `ALLOW_HTTP` or a secret with `USE_SSL false`
explicitly selects unencrypted HTTP. Endpoint user information, query strings and fragments
are rejected. Normal provider errors expose operation categories and numeric status codes,
not reflected provider messages or signed URLs.

## Stored data and access

The metadata store, write journals, payload pools, native temporary files and remote read
cache can contain database data. S3 packs contain native DuckDB bytes. This extension does
not encrypt those contents or change bucket ACLs; protect them with filesystem permissions,
bucket policies and the encryption controls of the deployment.

Repositories are private by default: directories use `0700`, files use `0600`. A repository
created with `differential_repository_shared` uses setgid `02770` directories and `0660`
files for its group. Every group member can access every database in that repository.
The mode is fixed at creation and is independent of the caller's umask.

Use the extension to manage its files. Do not edit, truncate or replace the SQLite metadata,
journals, pools or immutable S3 objects externally. Local payload paths are canonicalized,
object names are validated hashes, and payload opens reject symlinks. Collection operates
only on persisted reachability and owned namespaces. Keep the metadata store and every
referenced payload pool together.

Remote cache entries are verified against their content hashes on read. The cache is never
authoritative. Export retirement or local eviction does not revoke access to an S3 export;
there is no distributed remote-reader expiry or automatic remote garbage collection.

## Branch heads and shared writers

S3 branch heads and advisory shared-writer markers are mutable. Conditional head writes
prevent an extension writer from silently replacing a head changed by another writer.
Readers validate the head's database/snapshot identity and its immutable descriptor.
Anyone with unrestricted write access to the prefix can still change objects using other
tools, so protect the prefix as an authority boundary.

Advisory writer markers include a host name, process ID and repository UUID. Correctness
does not depend on these markers. A copied metadata store is not a second authorized writer;
use the documented branch-head ownership and shared-database protocols.

## Development builds and reports

Fault injection, native I/O tracing and raw-path probes are compiled only with
`DIFFERENTIAL_TEST_HOOKS=ON`. Release and community builds omit these hooks. Test builds and
their diagnostic output are for development use.

Per-run logs, provider diagnostics and implementation notes stay in ignored local directories.
Review any report before sharing it: paths, object locators and host identities can disclose
deployment details even when credentials and row values are omitted. Automated tests and
these design controls do not constitute an independent security audit.
