# Shared databases: several writers through S3

This guide follows one database written by two hosts and read by a third, from creation to
recovery after conflicts and failures. A *host* is any process with its own repository: two
laptops, a laptop and a CI job, two containers. The [SQL reference](sql-reference.md) lists every
function and option; the semantics are in the [SQL API](api.md#shared-databases-several-writers)
and [concurrency guide](concurrency-and-multitenancy.md#shared-databases-several-writing-hosts).

Each SQL block below is one session on the host named in its first line, run in order. The
blocks are executed as written by the test suite (against MinIO, `test/integration/doc_examples.py`).

| Stage | What happens |
| --- | --- |
| [1. Set up](#1-set-up-each-host) | Each host loads the extension and gets S3 credentials |
| [2. Create](#2-create-the-database) | Host A creates the shared database and publishes a first snapshot |
| [3. Join](#3-join-from-another-host) | Host B attaches it: nothing is downloaded, data is read on demand |
| [4. Publish](#4-publish-changes) | Snapshots and checkpoints become visible to everyone |
| [5. Read](#5-read-from-anywhere) | Readers follow branches, list history, travel in time |
| [6. Branches](#6-branches-across-hosts) | Branches are created, shared and dropped across hosts |
| [7. Who is writing](#7-see-who-is-writing) | Advisory "being written" markers |
| [8. Conflicts](#8-when-two-hosts-change-the-same-branch) | One host wins; the other keeps its work and reconciles |
| [9. Failures](#9-recover-from-failures) | Outages, crashes, lost repositories |
| [10. Housekeeping](#10-housekeeping) | Expiring history, local clean-up |

## When to use a shared database

Use one when several hosts must **write** the same data and publish it under the same names. A
single-writer database (the default) is simpler and is enough when one host writes and others only
read ([lifecycle guide](lifecycle.md#11-publish-to-s3)).

What a shared database gives you:
- every host writes locally at native speed;
- publishing is optimistic: a branch head only moves from the version its writer started from, so
  no update is ever lost;
- all metadata lives in S3 next to the data, with no catalog server and no lock service.

What it does not do:
- merge concurrent changes of one branch: the second host to publish gets `branch_conflict` and
  keeps its work as a snapshot (and, if it wants, a branch);
- make commits between snapshots durable off the host: they reach S3 when published.

## 1. Set up each host

```sql
-- every host
INSTALL differential FROM community;
LOAD differential;
SET differential_repository = '/absolute/repository';   -- one repository per host
CREATE SECRET lake (TYPE s3, PROVIDER credential_chain, REFRESH auto, REGION 'us-east-1', SCOPE 's3://my-bucket/lake/');
```

Each host needs read and write access to the prefix. Credentials come from a `TYPE s3` secret
(keys, or your AWS environment as above) or the AWS SDK; they are never given to ATTACH.

## 2. Create the database

```sql
-- host A
ATTACH 'differential:team' AS team (DATA_PATH 's3://my-bucket/lake/', WRITERS 'shared');
CREATE TABLE team.orders AS
    SELECT range AS id, 'new' AS status, range * 10 AS amount FROM range(1000);
SELECT snapshot_id, export_state FROM differential_create_snapshot('team', name => 'initial-load');
```

`WRITERS 'shared'` is chosen once, when the database is created, and needs an S3 `DATA_PATH`. The
database's identity is derived from its name, so every host that attaches `team` in this location
writes the same database. `differential_create_snapshot` returns once snapshot 1 is in S3 and is
the head of `main`.

## 3. Join from another host

```sql
-- host B
-- expect output: "remote_state":"in_sync"
ATTACH 'differential:team' AS team (DATA_PATH 's3://my-bucket/lake/', WRITERS 'shared');
SELECT count(*) AS orders, sum(amount) AS total FROM team.orders;
SELECT branch_name, remote_state, remote_snapshot_id FROM differential_branches('team');
```

Host B had no copy of `team`: ATTACH read the head of `main`, imported the snapshot's map and
downloaded nothing. The query reads what it needs from S3, with read-ahead for scans, and keeps it
in DuckDB's external file cache. `remote_state` is `in_sync`: B's `main` is the S3 head.

Every later ATTACH on any host reads the head again (one request) and moves a branch that has no
local changes to it.

## 4. Publish changes

```sql
-- host B
ATTACH 'differential:team' AS team;
UPDATE team.orders SET status = 'shipped' WHERE id < 100;
SELECT snapshot_id, export_state FROM differential_create_snapshot('team', message => 'ship first 100');
```

`differential_create_snapshot` publishes synchronously: it uploads the new data, takes the next
free snapshot number and moves the branch head, or reports a conflict.

Without it, **every DuckDB checkpoint is published in the background**: `CHECKPOINT`, the automatic
ones and the one at DETACH each become a snapshot (`commit_message = 'checkpoint'`) that a worker
publishes while the session goes on. DETACH does not wait for it; a snapshot left unpublished
(for example because the process exited) is published at the next writable ATTACH. To make DETACH
wait, or to publish only explicitly:

```sql
-- host A
ATTACH 'differential:team' AS team (PUBLISH_ON_DETACH true);   -- DETACH waits for publication
UPDATE team.orders SET amount = amount + 1 WHERE id = 500;
DETACH team;                                                   -- published when this returns
ATTACH 'differential:team' AS quiet (PUBLISH_ON_CHECKPOINT false);   -- only explicit snapshots publish
SELECT amount FROM quiet.orders WHERE id = 500;
```

Host A's ATTACH also moved its unchanged `main` to host B's snapshot first, so the update above
builds on B's work.

## 5. Read from anywhere

A reader needs only read access to the prefix, no repository of a writer:

```sql
-- reader
ATTACH 'differential:s3://my-bucket/lake/heads/team/main' AS latest;
SELECT amount FROM latest.orders WHERE id = 500;
SELECT database_name, branch_name, snapshot_id FROM differential_remote_heads('s3://my-bucket/lake/');
```

Writers see the whole published history, whoever published it:

```sql
-- host A
ATTACH 'differential:team' AS team;
SELECT snapshot_id, publication_state, snapshot_name, commit_message
FROM differential_snapshots('team') ORDER BY snapshot_id;
SELECT status FROM team.orders AT (VERSION => 1) WHERE id = 1;
ATTACH 'differential:team' AS before_shipping (SNAPSHOT_NAME 'initial-load');
SELECT count(*) FILTER (status = 'shipped') AS shipped FROM before_shipping.orders;
```

Snapshots another host published show `publication_state = 'remote'`; they are read through their
S3 exports. `AT (TIMESTAMP => …)` and `SNAPSHOT_TIME` follow the branch's ancestry by publication
time. `AT` uses the history as last listed (by ATTACH in the background, by `differential_snapshots`
or by a snapshot ATTACH); it never reads S3 while a query is bound.

## 6. Branches across hosts

```sql
-- host B
CALL differential_create_branch('team', 'experiment');
ATTACH 'differential:team' AS experiment (BRANCH 'experiment');
DELETE FROM experiment.orders WHERE amount > 9000;
CALL differential_create_snapshot('experiment');
```

```sql
-- host A
ATTACH 'differential:team' AS experiment (BRANCH 'experiment');   -- joins the branch from S3
SELECT count(*) AS orders FROM experiment.orders;
```

A branch name is taken in S3 when the branch is first published: creating `experiment` again on any
host fails with `name_conflict`. Dropping a branch drops it for everyone:

```sql
-- host B
CALL differential_drop_branch('team', 'experiment');
```

```sql
-- host A
-- expect error: was dropped
ATTACH 'differential:team' AS experiment (BRANCH 'experiment');
```

Host A's unchanged copy of `experiment` was dropped at that ATTACH. A copy with local changes
would stay and be reconciled like a conflict (§8). The name can be used again.

## 7. See who is writing

Every writable attachment keeps an advisory "being written" marker in S3 while it lasts:

```sql
-- host A
ATTACH 'differential:team' AS team;
SELECT branch_name, host, live, this_process FROM differential_writers('team');
```

Attaching a branch another host is writing prints a warning; `ON_BUSY 'error'` refuses instead,
`'ignore'` stays silent. The markers prevent nothing, and nothing depends on them; a crashed
writer's marker expires after 2 minutes.

```sql
-- not run: needs another host writing main at the same time
ATTACH 'differential:team' AS team (ON_BUSY 'error');   -- [differential:writer_busy] Branch main is being written by …
```

## 8. When two hosts change the same branch

Both hosts change `main` from the same snapshot. Here they keep the changes local first, so the
outcome is the same whatever the timing:

```sql
-- host A
ATTACH 'differential:team' AS team (PUBLISH_ON_CHECKPOINT false);
UPDATE team.orders SET status = 'cancelled' WHERE id = 7;
```

```sql
-- host B
ATTACH 'differential:team' AS team (PUBLISH_ON_CHECKPOINT false);
UPDATE team.orders SET status = 'returned' WHERE id = 8;
CALL differential_create_snapshot('team');                 -- B publishes first and wins
```

```sql
-- host A
-- expect error: branch_conflict
ATTACH 'differential:team' AS team;
CALL differential_create_snapshot('team');
```

Host A's call fails with `[differential:branch_conflict] Branch main of database team moved in S3 …`.
A's snapshot was uploaded and is kept, but `main` stays at B's snapshot, and A's `main` is now
*diverged*: it publishes nothing more until it is reconciled.

```sql
-- host A
-- expect output: "remote_state":"diverged"
SELECT branch_name, remote_state, remote_snapshot_id FROM differential_branches('team');
```

### Recovery strategies

| You want to | Do this (after DETACH) |
| --- | --- |
| Keep your work and follow S3 | `differential_resolve_conflict('team', keep => 'branch')`: your state becomes branch `main-conflict-<n>`, `main` becomes the S3 head |
| Discard your work and follow S3 | `differential_resolve_conflict('team', keep => 'remote')`: your state stays only as a local snapshot |
| Never fail; fork automatically | `ATTACH … (ON_CONFLICT 'branch')`: a conflicting snapshot is published as `main-conflict-<n>` and the call succeeds |
| Combine both changes | Keep your work as a branch, then copy it into `main` with SQL and publish (below) |

Both resolutions first preserve the branch's current contents as a snapshot, including changes
that were not yet in one, so nothing is lost. Here host A keeps its work and merges it by hand:

```sql
-- host A
-- expect output: "kept_branch":"main-conflict-6"
SELECT branch_name, kept_snapshot_id, kept_branch
FROM differential_resolve_conflict('team', keep => 'branch', request_id => 'resolve-cancel-7');
ATTACH 'differential:team' AS team;                              -- main is B's snapshot again
ATTACH 'differential:team' AS mine (BRANCH 'main-conflict-6');   -- A's work, published as a branch
UPDATE team.orders SET status = (SELECT status FROM mine.orders WHERE id = 7) WHERE id = 7;
CALL differential_create_snapshot('team', message => 'merge cancellation of order 7');
```

The fork's name contains the kept snapshot's number (`kept_branch` in the result; 6 here). Drop the
fork when it is no longer needed, after DETACH:

```sql
-- host A
CALL differential_drop_branch('team', 'main-conflict-6');
```

With `ON_CONFLICT 'branch'` the fork happens at once:

```sql
-- host B
ATTACH 'differential:team' AS team (PUBLISH_ON_CHECKPOINT false);
UPDATE team.orders SET amount = 0 WHERE id = 9;
```

```sql
-- host A
ATTACH 'differential:team' AS team;
UPDATE team.orders SET amount = 1 WHERE id = 9;
CALL differential_create_snapshot('team');                      -- A wins this time
```

```sql
-- host B
-- expect output: "state":"published_as_branch"
ATTACH 'differential:team' AS team (ON_CONFLICT 'branch');
SELECT state, export_state FROM differential_create_snapshot('team');   -- published_as_branch
```

B's snapshot is now branch `main-conflict-<n>`, B's `main` shows `remote_state = 'forked'`, and
at its next ATTACH without new changes B's `main` follows S3 again.

## 9. Recover from failures

| Situation | What you see | What to do |
| --- | --- | --- |
| S3 unreachable while publishing | `[differential:incomplete_export] … remains pending` | Nothing is lost locally. `CALL differential_retry_export('team')` when S3 is back, or let the next writable ATTACH publish it |
| Process crashed or was killed mid-operation | The operation did not return | Repeat it (with the same `request_id` if it had one); a snapshot publication finishes with `differential_retry_export`. No second snapshot number, no false conflict |
| S3 unreachable at ATTACH | ATTACH fails with the S3 error | Attach again when S3 is back; nothing changed locally |
| Background publication failed | `remote_state = 'ahead'`; `differential_status()` shows the error under `shared_publication` | `CALL differential_retry_export('team')` |
| Another host moved the head | `[differential:branch_conflict]`, `remote_state = 'diverged'` | §8 |
| Branch dropped elsewhere while you have changes | Your publication fails with `branch_conflict` ("was dropped") | `differential_resolve_conflict(…, keep => 'branch')` keeps your work as a new branch |
| Dropping a branch that moved in S3 | `[differential:branch_conflict] … ATTACH it … then drop it` | ATTACH the branch (it follows S3 if unchanged), then drop again |
| A writer crashed | Others see its marker for up to 2 minutes (warning at ATTACH) | Nothing; it expires and is removed |
| A host's repository is lost | Its unpublished commits are gone | Attach again with `WRITERS 'shared'` in a new repository: it joins from S3. Published snapshots are all in S3 |
| A snapshot number changed | An unexported (e.g. deferred) snapshot was renumbered when published | Expected: numbers are final once published |

```sql
-- host B
CALL differential_retry_export('team');                 -- publish whatever is still pending
SELECT branch_name, remote_state FROM differential_branches('team') WHERE state = 'active';
SELECT status FROM differential_status() WHERE capability = 'shared_publication';
```

Commits between snapshots are durable locally (as for any database) but reach S3 only when
published. Host clocks matter only for time travel by time and for the markers' expiry; numbers,
heads and conflicts never use them.

## 10. Housekeeping

```sql
-- host A
SELECT snapshot_id FROM differential_expire_snapshots('team', versions => [1]);
SELECT deleted_objects FROM differential_cleanup_old_files(cleanup_all => true);
```

- Expiring a snapshot retires it for every host (it is recorded in S3). Hosts read new snapshots
  and expiries incrementally; `differential_snapshots('team', refresh => 'full')` re-reads the whole
  history (done automatically at most once a day). A snapshot that is the head
  of a branch cannot expire.
- S3 data of expired snapshots and dropped branches is deleted only by
  `differential_cleanup_remote_files`, run by any host: it condemns unreachable data first and
  deletes it a grace period later (default a week). A host whose branch reads its base snapshot
  from S3 copies condemned data locally when it next attaches or publishes, so keep the grace
  period longer than any host stays offline with unpublished work.
- `differential_cleanup_old_files` and `differential_compact` maintain each host's repository as
  for any database. Data of another host's snapshots is not stored locally in the first place.
