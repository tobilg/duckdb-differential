# Logical branch merge

`differential_merge_snapshots` accepts three explicit public snapshot IDs.
`differential_merge_branch` accepts two branch names and resolves their published heads and
a unique best common ancestor automatically. It can create a new branch or update its
`ours` branch when `destination = ours`. Each has a matching `_preview` function.
Both use the same typed logical merge engine and support one local or single-writer S3
database registered in the same repository. Automatic ancestry uses that repository’s records. DuckDB still owns the
native catalog, data, indexes and transactions; the differential store supplies immutable images.

```sql
ATTACH 'differential:example' AS d;
CREATE TABLE d.items(id INTEGER PRIMARY KEY, label VARCHAR, amount DECIMAL(18,3));
INSERT INTO d.items VALUES (1, 'original', 1.250);
CALL differential_create_snapshot('d'); -- 1: base
CALL differential_create_branch('d', 'ours', snapshot => 1);
CALL differential_create_branch('d', 'theirs', snapshot => 1);
ATTACH 'differential:example' AS o (BRANCH 'ours');
UPDATE o.items SET label = 'edited' WHERE id = 1;
CALL differential_create_snapshot('o'); -- 2: ours
ATTACH 'differential:example' AS t (BRANCH 'theirs');
UPDATE t.items SET amount = 2.375 WHERE id = 1;
CALL differential_create_snapshot('t'); -- 3: theirs
SELECT * FROM differential_merge_branch_preview('d', 'ours', 'theirs', validate => true);
SELECT * FROM differential_merge_branch('d', 'ours', 'theirs', 'merged', request_id => 'merge-example');
ATTACH 'differential:example' AS m (BRANCH 'merged');
SELECT * FROM m.items; -- (1, 'edited', 2.375)
```

Inputs must still be retained and readable from supported storage when selected. Each takes a reader
pin. Changes after these snapshots are excluded. Sources may remain attached; an existing
target must be detached and exactly match its selected public snapshot.
For the snapshot APIs, the caller chooses the base; snapshot numbers, timestamps and equal
roots are never used to infer ancestry. The extension does not verify that the supplied base is an ancestor. A wrong
base can change the result or produce conflicts. For example, after incorporating snapshot
3 into result 4, `(base=3, ours=4, theirs=3)` preserves 4. Repeating the original `(1,2,3)`
produces the same logical state under another new branch, unless replaying its request ID.

## Merge into an existing target

Use the target as both `ours` and `destination`; the source is `theirs`. Any valid branch
names work. This is implemented entirely by the extension on unmodified DuckDB v2.0.0.

```sql
-- Following the example above: snapshot target writes, then release its attachment.
DETACH o;
SELECT snapshot_id, merge_action, target_check_ms
FROM differential_merge_branch('example', 'ours', 'theirs', 'ours',
                               request_id => 'merge-into-ours');
ATTACH 'differential:example' AS o (BRANCH 'ours');
SELECT * FROM o.items; -- (1, 'edited', 2.375)
```

This mode supports local and single-writer S3 storage within the same database/repository. Snapshot APIs still require a new
name. A destination equal to an existing branch other than `ours` remains `name_conflict`.
Two distinct branch identities are required, as for the existing branch selector.
The target UUID/name are preserved. The source head and live data are unchanged by merging.

| Proven ancestry | `merge_action` | Publication |
| --- | --- | --- |
| Source head is an ancestor of target head (including equal heads) | `already_up_to_date` | Keep target head; no new snapshot or history event. |
| Target head is an ancestor of source head | `fast_forward` | Point target at the existing source snapshot/root; add a target history event, no new snapshot. |
| Both diverged | `merged` | Stage and validate the logical result, then publish one new target snapshot with both input heads as parents. |
| New destination name | `created_branch` | Existing new-branch behavior, including when inputs have an ancestor relationship. |

All paths check supported schemas/types, budgets and retained inputs and run the typed
comparison/resolution checks. Fast-forward and no-op skip table rebuilding; they are not
constant-time operations. Existing strict/additive/catalog scope applies even to these paths.
A fast-forward preserves the exact source native image, including physical column order; it
does not rewrite that snapshot's metadata or ancestry.

Before reading the mutable target, the request takes its branch lease and records a durable
intent containing its selected head and writer epoch. Active attachments, including a
transaction that still holds a detached attachment, cause `active_attachment_or_transaction`.
The extension does not detach another connection or change DuckDB's catalog caches. Source
attachments, other branches and immutable historical readers may remain active.

The target must exactly match the selected snapshot: the live image is durably sealed and
compared by root, and nonempty native WAL/recovery sidecars are rejected. This conservative
byte check can reject a logically unchanged database whose native image changed. Any mismatch
is `merge_target_dirty`, never a logical conflict. Recover/checkpoint normally if needed,
create a public snapshot, detach, then use a **new** request ID. Committed unsnapshotted writes
are never silently overwritten. With snapshot-on-checkpoint enabled, the final public head
must still match the detached image. Target checking never opens a writable native attachment
and does not publish a snapshot or export.

The selected target UUID/head must still match when ownership is acquired; otherwise the
request fails with `merge_target_changed`. Pending requests never follow a reused target name
or rebase onto a changed head. Changes to source live data after its selected snapshot are excluded.
A preview checks logical results. The destination-aware preflight below also checks target
ownership and cleanliness. Neither reserves inputs or ownership across calls; use expected
snapshot IDs to guard the subsequent merge against a changed selection.

A successful diverged result is checkpointed before one FULL metadata transaction installs
its native file mapping, moves the existing branch head, records ancestry and provenance,
clears the intent and completes the request. Fast-forward uses the same atomic replacement
with the source image. Old immutable readers retain their own pins. A staged partial result
is never exposed as the target.

For local storage, errors/cancellation before publication release the target intent and leave its data/head unchanged.
For S3, this applies only before durable preparation; a prepared result retains the fence until publication or safe abort.
Once the final transaction starts, cancellation can race a successful commit: inspect or replay
the request to learn its durable outcome; cancellation does not undo a completed merge.
If cleanup itself cannot commit, the durable intent remains fenced for retry/abort. Process
death before publication also leaves it fenced: `differential_merge_requests` reports
`target_locked` (before S3 preparation) and `destination_reserved = true`. Prepared S3 requests instead report `prepared`, `uploaded` or `publication_uncertain`. Retry the same request to recompute from
its frozen retained inputs, or use `differential_abort_merge` to release the fence. Abort
works after input expiry. No ancestor payload is retained indefinitely. A death during the
publication transaction rolls it back; a death after commit replays the completed result.
No-op/fast-forward/merge outcomes persist an atomic `merge_target_result` audit companion
with all three input IDs/roots and the algorithm/provenance; the request links its caller ID.

Completed retries return the original snapshot/action without checking attachments or current
heads, even after input expiry or target deletion/name reuse. They never install an old result
again. Changed arguments still fail. `target_check_ms` separates target ownership/cleanliness
cost; `publication_ms` measures publication separately from staged `application_ms`, including S3 upload/head update when applicable.
Replay timings are zero. See the qualification record for measured costs and remaining limits.

## Preflight and guarded execution

`differential_merge_branch_preflight(database, ours, theirs, destination)` returns one report,
including for an empty database. It selects the published inputs and expected action, checks
supported catalogs, compares rows, validates the final native constraints in disposable staging,
and checks destination availability or the existing target's ownership/cleanliness. Validation
defaults to `true`. It uses the same schema, resolution and resource options as branch merging.

```sql
CREATE TEMP TABLE reviewed_merge AS
SELECT * FROM differential_merge_branch_preflight('example', 'ours', 'theirs', 'reviewed');
SELECT ready, merge_action, target_state, validation_state, issues FROM reviewed_merge;
-- Continue after reviewing ready=true and the validation/diagnostic fields.
SET VARIABLE reviewed_base = (SELECT base_snapshot_id FROM reviewed_merge);
SET VARIABLE reviewed_ours = (SELECT ours_snapshot_id FROM reviewed_merge);
SET VARIABLE reviewed_theirs = (SELECT theirs_snapshot_id FROM reviewed_merge);
SELECT * FROM differential_merge_branch('example', 'ours', 'theirs', 'reviewed',
  expected_base_snapshot_id => getvariable('reviewed_base'),
  expected_ours_snapshot_id => getvariable('reviewed_ours'),
  expected_theirs_snapshot_id => getvariable('reviewed_theirs'),
  request_id => 'reviewed-merge');
```

Use `destination = ours` for an existing target. `ready` means the checks performed found no
blocker; it is not a reservation or a promise of publication. `validate => false` skips staged
constraint validation and reports `validation_state = 'not_checked'`, even if `ready = true`.
Execution always performs its normal validation and target checks. A preflight can become
stale while running or immediately after it returns. A head change observed by its final input
check reports `merge_inputs_changed` and `ready = false`. Expected action remains advisory when issues exist.

Diagnostics distinguish data conflicts, schema conflicts, unsupported objects, final constraint
violations, missing/expired inputs, an occupied destination, a busy/dirty target, and pending
restore/drop/merge intents. Pending entries include the owning operation ID and retry/abort
guidance; preflight does not complete or abort those operations. It can inspect their currently
published snapshots. A new S3 destination reserved by a prepared merge reports
`target_state = 'reserved'` and `merge_pending_operation`, with the owner's operation ID,
even before any public branch or remote head exists. An unsupported catalog reports the **first unsupported object per input**,
then stops planning that input; it is not an exhaustive catalog audit. Counts are NULL when
comparison did not run. Detailed typed row values remain in the paginated preview API.
The report retains at most 100 issues, 1 MiB of diagnostic string payload, and 4,096 UTF-8 bytes
per field; `issues_truncated` reports clipping. Aggregate counts still cover all compared tables.

Preflight creates no branch, snapshot, request, writer epoch or durable target intent. Reader
pins, scratch files and temporary target leases last only for the call. The byte cleanliness
check can replay abandoned store journals and durably seal the existing image as internal
bookkeeping; it never opens a writable native target attachment or checkpoints a native WAL.
Staging/pins are cleaned on ordinary failure or cancellation and recovered after process death
using existing preview cleanup. Cancellation, deadlines and internal/fatal errors remain SQL
errors; ordinary blockers appear in `issues`. Missing database resolution and invalid SQL
arguments can also fail before a report is produced. Binding, PREPARE and EXPLAIN do no work.

The three optional `expected_*_snapshot_id` arguments on `differential_merge_branch` must be
supplied together as positive, non-NULL IDs. They also accept IDs returned by branch preview.
The first execution checks both published heads and the selected base **in the same metadata
transaction that reserves the request**. A mismatch fails with `merge_inputs_changed` before
reserving an operation or output; refresh the review. The check concerns snapshot identity,
not equal data/roots. It does not freeze resolution table contents, options, branch-name
incarnations, or unsnapshotted live data. Use the reviewed schema mode and resolution mapping
again. A recreated branch with the same snapshot head represents the same reviewed input state.

Guard presence and values become request identity. A pending retry must supply the original
guard and uses the request's frozen input IDs, even if the source later moves. The existing
target must still have its frozen UUID/head. A completed retry returns its original result
after input expiry or later branch changes, without reinstalling it. Adding, omitting or
changing a guard on retry fails. Unguarded calls retain the existing selection behavior.
No inputs remain pinned between calls, no expired base is substituted, and these small audit
records introduce no indefinite payload retention. Explicit snapshot calls already identify
their inputs and need no additional guard.

## Automatic base selection and repeated merges

```sql
SELECT base_snapshot_id, ours_snapshot_id, theirs_snapshot_id, conflicts, validation_state,
       selection_ms, ancestry_nodes
FROM differential_merge_branch_preview('example', 'ours', 'theirs', validate => true);
```

Automatic selection supports parent/child branches, different fork points, nested branches,
and previously merged branches. It follows explicitly recorded snapshot parents and selects
one **best common ancestor**: a common ancestor that is not an ancestor of another common
ancestor. Numeric IDs, timestamps and equal data or byte roots never determine ancestry.
Two incomparable best ancestors produce `merge_base_ambiguous`; there is no recursive virtual
base or arbitrary tie-break. Use `differential_merge_snapshots` with a caller-chosen base instead.
The two branch identities must be distinct; two branches sharing the same head are allowed.

Each new local snapshot records its previous published head as its parent. A new branch starts
at its selected fork snapshot without creating another snapshot. A merge result records both
input heads (one edge if identical), including when the caller supplied an explicit base.
The base argument itself is not an additional parent. Explicit-base callers remain responsible
for choosing the base; its ancestry is not validated by the snapshot API.

For example, after merging `ours=2` and `theirs=3` into result `4`, a later snapshot `5` on
that result and snapshot `6` on theirs merge using base `3`. Merging the resulting state with
`6` again uses base `6` and preserves already-incorporated changes. Continue from the result
branch, or merge into `ours` explicitly: new-destination merging does not advance either source's history.

Restore is a new state change following the previous head, or a new preservation snapshot if
one was published. The snapshot whose bytes were restored is **not** a second parent. Reusing
an equal-root snapshot for preservation does not invent an edge. Native imports, recovered
exports and clones start new local ancestry roots; no cross-database ancestry is inferred.
Pending restore/drop/target-merge intents block fresh selection. Already resolved requests keep their inputs.

The resolver reads both heads and their ancestry in one authority transaction at execution time.
Only published snapshots are selected; active attachments and later live writes are never captured.
A merge's first request durably records that selection before comparison. Results and every
preview row expose `base_snapshot_id`, `ours_snapshot_id` and `theirs_snapshot_id`.

A fresh preview and a later unguarded branch merge can select different heads. Preview does not reserve
inputs or a destination. To reject a changed selection, pass all three returned IDs as the
branch merge's `expected_*_snapshot_id` arguments. To merge the earlier snapshots regardless
of current heads into a new branch, pass them to `differential_merge_snapshots` instead.
For stable conflict pagination, use those IDs with
`differential_merge_snapshots_preview`; each new branch preview otherwise resolves current heads.

The same request reuses its recorded snapshot IDs and branch UUIDs, even after head movement
or source-name reuse. Completed requests replay after input expiry. Changed database, ordered
branch names or destination fail. Resource limits may change. Unknown pending selection/merge
algorithms fail; completed records remain replayable.

### Expiry and incomplete history

Small parent records survive snapshot retirement and payload GC. They contain only identities,
not retention roots. The graph is resolved before checking base availability: an expired best
base fails without selecting an older retained ancestor, and expiring one of two ambiguous
candidates does not resolve the ambiguity. Base and input heads must be locally readable when
pinned. There is no new indefinite retention of snapshot bytes.

Older snapshots without parent records remain unknown. There is no history backfill from
snapshot numbers, timestamps, user commit metadata, or equal roots. An unknown history boundary
is safe when known edges already place it below the unique common base: older unknown ancestors
cannot supersede that base. Otherwise selection fails with `merge_base_unavailable`, except for
the fork proof used for history without parent records, such as snapshots written by 1.0.0:
two branches whose completed fork operations name the same fork snapshot, with no restore or
target merge since. Its selection tag is `sibling-fork-v1`. Malformed/cyclic records and
unsupported ancestry versions fail explicitly rather than using the fork proof. See
[upgrading from 1.0.0](#upgrading-from-100).

The graph walk uses indexed point lookups and bounded C++ memory, with `max_ancestry_nodes`
(default 100,000; range 1–1,000,000) on branch merge/preview. Exceeding it returns `merge_limit`
before a new request is recorded. `timeout_ms` covers selection plus logical work; cancellation
is checked during graph traversal. `selection_ms` reports resolution time separately from
`comparison_ms` and `application_ms`, and `ancestry_nodes` counts distinct visited nodes.
These fields are zero for explicit snapshot calls and frozen-request replay. SQLite lock waits
and individual native tasks are not forcibly interrupted. Process admission currently applies
when the logical engine starts; authority transactions serialize graph resolution.

## Supported scope

* Local single-writer databases, including local payload pools, and single-writer S3 databases
  registered in the same repository, including evicted inputs
  with completed exports. Shared writers, arbitrary descriptor-only attachments, and automatic
  cross-repository ancestry discovery are excluded. See the S3 publication contract below.
* Only the `main` schema. The default `schema_mode => 'strict'` requires identical table/column
  definitions and constraints. Opt-in `schema_mode => 'additive'` permits the nullable column
  and new-table additions described below. `schema_mode => 'catalog'` also supports table and plain nullable column removals
  and whole-view definition merging. Every table needs a declared primary key; composite keys are supported. Keyless
  tables are rejected, including empty ones. Native key/index type restrictions still apply
  (for example, DuckDB rejects INTERVAL index keys). No table is silently skipped.
* BOOLEAN, TINYINT/SMALLINT/INTEGER/BIGINT/HUGEINT and their unsigned variants through
  UHUGEINT, FLOAT, DOUBLE, UUID, BLOB, VARCHAR with the default binary collation, DECIMAL up
  to precision 38, DATE, TIME, TIME_NS, TIME WITH TIME ZONE and INTERVAL. All timestamp
  types implemented by the pinned DuckDB v2.0.0 are supported: TIMESTAMP_S, TIMESTAMP_MS,
  TIMESTAMP, TIMESTAMP_NS, TIMESTAMPTZ and TIMESTAMPTZ_NS, including native infinities.
  There is no custom timestamp representation, precision conversion or time-zone parser.
* Non-key STRUCT, TUPLE, LIST, ARRAY, MAP and UNION columns may recursively contain these
  supported types. Each complete column value is one merge cell; no nested field, element,
  map-entry or union-member merging occurs. A type is bounded to 32 nested levels and
  1,024 type nodes per column. Nested primary keys, dynamic VARIANT, JSON, BIT, BIGNUM,
  GEOMETRY, enum/custom/aliased types and non-default collations remain rejected, including
  unsupported leaves inside otherwise supported containers. Nullable non-key columns are supported.
* Native PRIMARY KEY, UNIQUE, NOT NULL and supported CHECK constraints are preserved and
  enforced. Simple column/composite ART indexes, including unique indexes, are rebuilt after
  loading affected tables. Expression indexes, other index types and index options are rejected.
* Defaults and CHECK expressions support constants, casts, arithmetic (`+ - * / // %`),
  comparisons, Boolean operators, CASE, BETWEEN, `abs`, `length`, `lower` and `upper`.
  CAST/TRY_CAST targets must be unqualified built-in supported types, recursively, with
  constant type modifiers. Custom or catalog-qualified type references are rejected before
  binding, including references nested inside a supported container or another cast.
  Defaults cannot reference columns. Volatile/session-dependent functions, sequences, macros,
  UDFs, subqueries and functions outside this allowlist are rejected explicitly.
* Local SELECT views are supported; strict/additive modes require unchanged definitions,
  bound output types and native dependency registrations. Deferred/unbound views and unsupported
  output types are rejected in every mode. Unchanged views are removed and recreated around
  affected-table rebuilding in dependency order; their native registrations are preserved
  independently of the caller's `enable_view_dependencies` setting.
  Catalog mode reconciles their definitions and dependencies as described below. Unqualified or `main`
  table/view references, joins, filters, projections and `count`, `sum`, `min`, `max`, `avg`
  aggregates are supported. Catalog-qualified/external references, table functions, CTEs,
  subqueries, windows and set operations are excluded. Views may reference other local views.
* Generated columns, foreign keys, triggers, sequences, macros, custom types, extra schemas,
  explicit column compression, and table/schema/column/object comments or tags remain rejected.
  Foreign keys need dependency-aware multi-table validation and application beyond this increment.
* Catalog bounds: 1,024 tables, 1,024 views/indexes combined, 4,096 table columns, 256 columns
  per table, 1,024 UTF-8 bytes per object/column identifier, and 16 MiB of catalog definitions.

Temporal casts and arithmetic use DuckDB's native `TimeZone` and `Calendar` settings from
the caller. Preview/preflight use the current settings without persisting them. A mutation
freezes both settings at its first data execution, before reading catalogs or building staging;
retries that recompute use those original settings even from a differently configured connection.
Use the same settings for preflight and first execution. Input guards cover snapshot identities,
not session settings. CHECK validation is native to that context; a later reader using another
zone/calendar can evaluate a temporal expression differently, just as with ordinary DuckDB.
A pending request that stopped before its first data execution captures the context when it
is retried. Prepared S3 output and completed requests replay without reevaluation.

All four merge/preview functions execute only when scanned: binding, PREPARE and plain EXPLAIN create no
repository, pins, branch or operation. Merge requires autocommit. Destinations are new names
except for the explicit `destination = ours` mode above. Even if snapshot-on-checkpoint is enabled,
staging cannot publish checkpoint snapshots or exports.

### Native equality and whole-value nested merging

Every supported value uses DuckDB's typed `IS [NOT] DISTINCT FROM` comparisons. The merge
does not compare JSON/text encodings, use approximate floating-point tolerances, normalize
maps or reimplement time/interval equality. Native NaN equality, signed-zero equality,
timestamp instant/precision rules and container ordering therefore determine changes.
The selected native value is preserved; equality need not mean identical physical bits.

For example, a STRUCT column `settings` with base `{'theme':'light','language':'en'}`, ours
`{'theme':'dark','language':'en'}`, and theirs `{'theme':'light','language':'de'}` produces one
`cell` conflict on `settings`. The merge does not combine the independent fields. A separate
non-key column can still merge independently. The same whole-value rule applies to lists,
arrays, maps, tuples, unions and combined supported containers within the bounds above.
There is no list append/reorder inference or union-tag reconciliation. Map comparison
uses native ordering semantics. A NULL container, an empty container, a NULL child and an
absent map key retain their native distinctions; a missing row still has its own presence flag.

Comparison has an extension-side workaround for a stock DuckDB 2.0 NULL-array outer-join
failure: array payloads use lossless native ARRAY/LIST casts around a materialized join,
then regain their declared types before any change/equality decision. This includes arrays
inside containers. The intermediate data uses DuckDB memory/spill handling; it adds work
for array-containing tables without changing stored schemas or global optimizer settings.

`resolution_tables` supplies an entire typed column value for a nested conflict. Its
`_merge_columns` map names the top-level column, never a nested path. The existing schema
rules remain unchanged: this increment permits additional column types, not type widening,
renames, nested type evolution or changes to defaults/constraints/indexes. The same rules
apply to local and single-writer S3 merges, preview, preflight and frozen resolution recovery.

## Additive column merging

All four merge/preview APIs accept `schema_mode => 'additive'`; the default remains `strict`.
This works with local snapshot/branch merges and single-writer S3 snapshot and branch merges.
For example, continuing from the local example above:

```sql
ALTER TABLE o.items ADD COLUMN note VARCHAR;
UPDATE o.items SET note = 'reviewed' WHERE id = 1;
CALL differential_create_snapshot('o');
ALTER TABLE t.items ADD COLUMN priority INTEGER;
UPDATE t.items SET priority = 2 WHERE id = 1;
CALL differential_create_snapshot('t');
SELECT table_name, schema_changes, schema_conflicts, conflicts, validation_state
FROM differential_merge_branch_preview('d', 'ours', 'theirs',
                                       schema_mode => 'additive', validate => true);
SELECT * FROM differential_merge_branch('d', 'ours', 'theirs', 'with_columns',
                                        schema_mode => 'additive', request_id => 'columns-1');
```

Only nullable, non-key columns without defaults may be added to existing tables. The supported
scalar/nested type allowlist above applies. Existing columns must keep their names (case-insensitively), types,
defaults and constraints. Existing table identities, keys, indexes and view definitions must remain unchanged.
Deleted/renamed tables, deleted/renamed/retyped columns, new constraints/indexes on existing tables,
new views and defaults on column additions (including `DEFAULT NULL`) return `merge_unsupported`
in additive mode. Catalog mode separately handles removals and views below.

One-sided additions are included; distinct additions on both sides are combined. Same-name
additions, matched with DuckDB's case folding, require the same supported type. Different
types produce a `column_definition` schema conflict. Unsupported types still fail explicitly.
The output retains the base column order, then appends additions sorted by folded name; ours
supplies the spelling of a name added on both sides. Input column order is matched by name,
including when a previous merge ordered additions differently from either contributing parent.
This makes repeated merges with the correct base work without inferring column renames.

An input lacking an added column contributes `CAST(NULL AS <declared type>)` for that cell.
All existing row/cell rules then apply unchanged. A missing row remains missing. In particular,
independent same-key inserts with different values still conflict, and identical additions can
have ordinary same-cell data conflicts. Schema-only changes, including empty tables, are applied.

Preview exposes `schema_mode`, `schema_changes`, `schema_conflicts`, `schema_conflict_details`,
`table_change` and `schema_algorithm`. `schema_changes` is a list of
`STRUCT(column_name VARCHAR, source VARCHAR, sql_type VARCHAR, action VARCHAR)`, with source `ours`, `theirs`
or `both` and action `add` or (in catalog mode) `drop`. The conflict count is per table; its details are
`STRUCT(column_name VARCHAR, kind VARCHAR, base_definition VARCHAR, ours_definition VARCHAR,
theirs_definition VARCHAR, object_name VARCHAR, reason VARCHAR)`. `object_name` identifies the table or
colliding catalog name; `column_name` is NULL for table/name conflicts. The base definition
is NULL for a conflicting addition. These
lists describe catalog objects and are bounded by the input and combined catalog limits
above. They can include several versions of a definition: the 16 MiB definition limit is
not a 16 MiB result-size budget. They are separate from the byte/page budgets for row conflicts.

Any schema conflict stops row comparison for the entire operation. Its preview table reports
`validation_state='schema_conflicts'`; other tables report `blocked_by_schema_conflicts`.
Data change/conflict/resolution counts are NULL because they were not computed; data detail
lists are empty. Mutation fails with `merge_schema_conflict` and publishes no destination.
`resolution_tables` resolves data conflicts only and must use the planned combined column
names, order and types. Schema conflicts cannot be overridden by row choices.

Affected tables are rebuilt in native staging, preserving supported defaults, constraints and
indexes. Views must have unchanged definitions **and bound output names/types** across all
inputs and the staged result. A `SELECT *` view whose output changes is rejected explicitly;
explicit projections and unaffected wildcard views are supported. Views are never rewritten.
Final constraint violations remain `merge_constraint`, separate from schema/data conflicts.

Each mutation durably freezes the mode and planner version (`strict-v1` or, for new additive
requests, `additive-tables-v2`) with its request, before comparison. A pending request whose
recorded planner version is unavailable fails explicitly; retry never upgrades it silently.
A fresh preview uses the current planner and reports its version. Changing the mode on retry fails,
including after an unsuccessful attempt. Both fields appear in `differential_merge_requests`
and result provenance. A request without its policy record is corrupt. Pending
recomputation requires the recorded planner; completed replay and prepared S3 publication
reuse their frozen result without replanning. Preview writes no policy record. Policy records
retain no input payloads, and existing cleanup/abort/publication rules apply unchanged.

### New tables in additive mode

The current additive planner also accepts tables absent from the base:

| Input state | Result |
| --- | --- |
| New table on one side | Include its supported definition and rows, including empty tables. |
| Different new tables on each side | Include both. |
| Same-name new tables with identical supported definitions | Compare their rows against an empty base; disjoint keys combine, identical same-key inserts deduplicate, different same-key inserts are `insert_insert` conflicts. |
| Same-name new tables with different definitions | `table_definition` schema conflict. |
| New table/index name collides with another table, view or index | `object_name_collision` schema conflict. |

The existing type, main-schema, declared-primary-key and catalog limits still apply.
A new table may contain the already supported defaults, NOT NULL/UNIQUE/CHECK constraints
and simple ART indexes; these are part of its complete native definition. It is created with
that definition and bulk loaded in staging, then its explicit indexes are rebuilt. It is
never copied using CTAS as its final definition. Additive mode excludes new views and drops/renames.
Foreign keys, sequences and changes to existing table constraints/defaults/indexes remain unsupported
in every mode.

Table and index names match case-insensitively; ours supplies their spelling when both sides
add the table. Canonical definitions normalize table/index identifier spelling, but column
spelling/order, types, defaults, constraints and index expressions must otherwise match exactly.
Unequal definitions are reported even when a more general schema merger might reconcile them.
The planner does not infer table identity across different names.

Name checks cover the combined catalog before any row comparison or staging. As a deliberate
restriction for new objects, cross-kind table/index names conflict even where DuckDB's separate
namespaces would allow them. Unchanged pre-existing cross-kind names remain accepted. Each
colliding folded name is reported once on a deterministically chosen added-table row; other
tables show `blocked_by_schema_conflicts`. Definition details identify the participating objects.
When a new table collides with a view, preview reports that collision; otherwise new/changed
views fail as unsupported. No existing object is overwritten to resolve a collision.

`table_change` is NULL for a table that existed in the base. For an added table it is
`STRUCT(source VARCHAR, definition VARCHAR, indexes STRUCT(index_name VARCHAR, definition VARCHAR)[])`.
Source is `ours`, `theirs` or `both`. The definition/indexes describe the proposed table;
when definitions conflict, ours supplies this display and both definitions remain in the
conflict detail. This display is not a conflict resolution: any schema conflict blocks publication.
Column changes remain in the separate `schema_changes` list.

Missing input tables are typed empty relations, not rows containing NULLs. Normal typed row
conflicts and `resolution_tables` therefore apply to independently added tables. Final-state
constraint violations remain separate from data/schema conflicts. Input row limits count each
present table once per distinct snapshot ID, and catalog limits apply to the combined result.
The same local/S3 staging, checkpoint, publication, abort and recovery rules apply.

## Catalog merging: table removal and views

All four APIs also accept `schema_mode => 'catalog'`. This explicitly includes additive table/
column support, table and plain nullable column removals, and whole-view changes. `strict` remains the default and `additive`
never removes tables/columns or changes views. Catalog requests freeze `catalog-v1` in the existing policy
companion. Retrying cannot change modes; `strict-v1`, `additive-tables-v2` and `catalog-v1`
requests keep their recorded planners. Local branch/snapshot and registered
single-writer S3 snapshot and branch calls share these rules.
Pending `catalog-v1` recomputation uses the current rules, including column removal.
Prepared and completed results replay their frozen output.

### Table removal rules

For a table present in the base:

| Ours/theirs relative to base | Result |
| --- | --- |
| Both remove the table | Remove it. |
| One removes it; the other has the same supported definition and typed rows | Remove it. |
| One removes it; the other changes any row (insert, update or delete) | `table_delete_modify` schema conflict. |
| One removes it; the other changes its supported definition/indexes | `table_delete_modify` schema conflict. |

The removal proof uses a native full outer join on declared keys and `IS DISTINCT FROM` on
all typed columns. Equal row counts alone never prove equality. Composite keys and NULLs use
the ordinary type rules. Budgets count the removed table's rows in each distinct input snapshot,
and normal DuckDB spilling remains available. Unsupported objects/types still fail during
input validation, even if both sides removed the object. Empty tables and removal of the last
table are supported. Deleting every row leaves a table present and is distinct from removing it.

These are name-based snapshot-state semantics. No rename or drop/recreate continuity is inferred.
A rename appears as an independent removal and addition; edits on the other side to the old
name conflict with the removal. Drop/recreate under the same name cannot be distinguished from
ordinary changes to the final definition/rows. Replacing a table with a view, or vice versa,
is rejected as `object_kind` rather than inferring a new identity.

### Column removal rules

A base column can be removed only if it is nullable, non-key, has no default (including
`DEFAULT NULL`), and has no dependent CHECK, UNIQUE/NOT NULL/PRIMARY KEY constraint or index.
The supported column types still apply to every input. Removing a base column outside this
scope fails with `merge_unsupported`, even when both sides remove it. Surviving columns,
constraints, defaults and indexes retain the additive planner's unchanged-definition rules.

| Ours/theirs relative to base | Result |
| --- | --- |
| Both remove the eligible column | Remove it. |
| One removes it; the other retains its definition and each surviving existing key's typed value | Remove it; reconcile the remaining columns normally. |
| Retaining side changes its definition, default, constraint or index dependency | `column_delete_modify` schema conflict. |
| Retaining side changes an existing key's value, including a NULL transition | `column_delete_modify` schema conflict. |
| Retaining side inserts a new key, even with a NULL value | `column_delete_modify` schema conflict. |
| Retaining side deletes a key | Use ordinary row-existence rules on the remaining columns. |

For example, removing `obsolete` on one side and updating `amount` on the other can merge.
Removing `obsolete` versus changing `obsolete` cannot. A row deleted by the retaining side
is deleted when its remaining values are unchanged on the removing side; if the removing
side updates a remaining value, it is an ordinary `delete_update` data conflict. New rows
on the removing side are allowed. Key changes remain delete-plus-insert, so a key change on
the retaining side conflicts. Edits undone before the snapshot are unchanged state.

For each retaining side, one DuckDB aggregate joins the table to base on the declared keys,
counts newly inserted keys and compares all removed columns with `IS DISTINCT FROM`. There
are no per-row round trips or C++ row maps. Rows and logical input bytes count against the
existing budgets before the proof; DuckDB may spill under its native memory/temp settings.
The proof must scan retained values even when few rows changed. It does not infer continuity
for renamed columns: absence plus a new name is an independent removal/addition.

`schema_changes` lists drops in base-column order, then additions in folded-name order.
Each drop includes the base name/type and removing side. Surviving base columns keep their
relative order; supported additions follow. `column_delete_modify` details identify the
table/column, include native input column definitions (NULL means absent), and give a bounded
reason with changed-value/new-row counts or the changed definition/dependency. These are
catalog conflicts; row details are not generated and data counts remain NULL. Schema conflicts
cannot be overridden with row resolution tables. After a successful schema plan, row-conflict
values and resolution tables use only the final columns; a dropped column is not a NULL cell
or a row update. In particular, a schema-only incoming drop still rebuilds the result table.

Views must be removed/replaced in the input snapshots as needed. Every input view must bind;
the selected final graph must bind with its selected output names/types. A surviving reference
to a removed column produces `view_binding`; a wildcard view whose combined output changes
produces `view_output`. There is no implicit cascade, column rename inference or view rewriting.
The [SQL reference](sql-reference.md#differential_merge_snapshots_preview) includes a complete
column-removal example.

### View rules and dependencies

A view's merge state consists of its native canonical CREATE definition, bound output names/
types, and native dependency registrations. View names match case-insensitively; SQL reference
spelling and expressions otherwise use DuckDB's serialized definition without semantic SQL
rewriting. Regular and secure views, explicit output aliases, local joins and the supported
scalar/aggregate expressions are supported. Unbound/deferred views must be bound before an
input snapshot is captured; recursive views, CTEs, subqueries, windows, external references,
unsupported functions and comments/tags remain excluded.

| View states | Result |
| --- | --- |
| Ours equals theirs, including both absent | Keep that state. |
| Ours equals base | Take theirs. |
| Theirs equals base | Keep ours. |
| Both change differently | `view_definition` conflict, or `view_delete_modify` when one removes it. |

This covers one-sided/independent additions, identical additions/replacements, whole-definition
replacements, and removals. It does not combine SQL clauses or automatically choose a side.
`resolution_tables` still applies only to retained tables' row conflicts; it cannot override
catalog conflicts or restore a removed table. Adjust inputs and use a new request to resolve
these conflicts.

The selected graph must reference surviving tables/views and be acyclic. Missing references
or cycles produce `view_dependency` conflicts. Views are created in dependency order against
an isolated in-memory catalog with empty native tables, before row reconciliation. Binding
failures produce `view_binding`; an output contract that changes in the combined catalog
produces `view_output`. For example, an incoming `SELECT *` view cannot silently gain columns
added independently on the other side. Recreate/project the intended columns in an input.

Native dependency registrations are preserved per selected view, independently of the caller's
`enable_view_dependencies` setting. The planner recreates and compares them, including native
blocking flags. Unsupported registrations fail explicitly; changes caused by the combined
catalog are `view_dependency` conflicts. No `CASCADE` is used. Table/view/index collisions are
checked in the final catalog; unchanged pre-existing cross-kind names remain accepted. Names
freed by a removed table's indexes may be reused if the final catalog has no collision.

Every input and the final catalog retains the existing table/object/DDL limits. Catalog mode
also limits each view to 256 output columns, all view outputs to 4,096 columns, and dependency
chains to 64 views. Catalog conflict reasons are bounded to 4 KiB plus a truncation marker.
Preview may report objects removed from the base as well as final objects; these rows are
bounded by the input/final catalog limits, not by the row-conflict page/byte budget.

### Preview, application and recovery

Preview adds `object_type` (`table`/`view`), `object_name`, `table_action` (`add`/`drop`/NULL),
and `view_change`. Existing table rows retain `table_name`; view rows have NULL `table_name`
and use `object_name`. Tables come first in the existing name order, followed by views in
folded-name order. View rows and removed-table rows have NULL row-change/conflict counts and
empty row-conflict details. Their catalog actions are not counted as row deletes.

For a removed table, `table_change` describes the base definition/indexes and the removing
side (`ours`, `theirs`, `both`). For an added table its existing meaning is unchanged.
`view_change` is NULL for unchanged views; otherwise its type is:

```text
STRUCT(action VARCHAR, source VARCHAR, definition VARCHAR, dependencies VARCHAR[],
       base_columns STRUCT(column_name VARCHAR, sql_type VARCHAR)[],
       ours_columns STRUCT(column_name VARCHAR, sql_type VARCHAR)[],
       theirs_columns STRUCT(column_name VARCHAR, sql_type VARCHAR)[],
       base_tracks_dependencies BOOLEAN, ours_tracks_dependencies BOOLEAN,
       theirs_tracks_dependencies BOOLEAN)
```

Action is `add`, `replace`, `drop` or `conflict`; definition is NULL for a selected removal.
Dependencies list folded query references and stored native dependency names (from the base
for a removal). Input columns/
tracking fields are NULL for a missing input view. A conflicting definition is only a proposed
display, never an automatic resolution. `schema_conflict_details` includes base/ours/theirs
native definitions, object name, kind and a bounded `reason`. Absent definitions are NULL.
Tracking fields report whether the input has native dependency registrations, not the session
setting that created it; a constant view has no relation dependencies to register.
Any catalog conflict blocks all row reconciliation and publication. Default preview validates
catalog structure/binding but leaves `validation_state='not_checked'` for final data constraints;
`validate => true` additionally uses the same complete staging path as mutation.
Dependency/binding validation reports its first blocking error rather than enumerating every
downstream failure; correct the inputs and preview again.

Staging begins from ours. It removes ours' views in reverse dependency order, removes selected
tables and their owned indexes, rebuilds affected retained/new tables with native constraints,
then creates selected views in dependency order. Bound outputs and native registrations are
checked again before checkpointing/publication. Unexpected staged binding/dependency/output
failures report `merge_application`; final row constraint violations remain `merge_constraint`.
Neither is relabeled as a data conflict.
Column projection copies native definitions and remaps positional primary-key/unique/NOT NULL
constraints by name. It preserves unrelated indexes/defaults/CHECK expressions. Rebuilding
also handles valid final states that native `ALTER DROP COLUMN` or eager UNIQUE updates cannot
apply in place; it never disables constraints or drops dependent objects implicitly.

Input pins, publication, safe abort and replay boundaries are unchanged. Before durable S3
preparation, retry recomputes with retained inputs and its recorded planner. After preparation,
it resumes the frozen validated result, even after input expiry. Failure/cancellation never
exposes a completed partial destination; existing process-death cleanup applies to private
staging and the temporary schema catalog. Catalog policy records add no input-retention root.
The `column_drop_check` and `column_drops_checked` execution phases bracket each table's typed
column-removal proof. The existing rebuild/checkpoint/publication phases cover application.

## S3 snapshot and branch merges

Use the snapshot functions with explicit IDs, or the branch preview/preflight/merge functions
with branch names from one registered single-writer S3 database. The
database's configured S3 location is also the result's location. Credentials are refreshed
from DuckDB S3 secrets, a named profile or the SDK chain at execution. Schema rules, typed
resolutions and native final-state constraint validation are unchanged; resolution source
tables still have to be local. A new destination preserves both inputs. An existing target
updates only `ours`; the source is never republished by the merge.

```sql
-- Given retained snapshots 1, 2 and 3 in an existing single-writer S3 database:
SELECT * FROM differential_merge_snapshots_preview('sales', 1, 2, 3, validate => true);
SELECT snapshot_id, materialization_ms, comparison_ms, application_ms, publication_ms
FROM differential_merge_snapshots('sales', 1, 2, 3, 'combined', request_id => 's3-merge-1');
```

For a branch workflow, create public snapshots for both branches, detach the target, review
preflight, and pass the three returned IDs as execution guards:

```sql
SELECT * FROM differential_merge_branch_preflight('sales', 'release', 'feature', 'release');
-- Example only: substitute the three IDs returned above.
SELECT * FROM differential_merge_branch('sales', 'release', 'feature', 'release',
    request_id => 's3-target-1', expected_base_snapshot_id => 1,
    expected_ours_snapshot_id => 2, expected_theirs_snapshot_id => 3);
```

Use an unused destination name for a new branch. `schema_mode` and explicit typed
`resolution_tables` work identically with both storage profiles. The target must be clean,
detached, completely exported and still owned by this repository in S3. Preflight checks its
remote identity/ownership without claiming or publishing anything. Its target state describes
local cleanliness; remote failures appear in `issues` and make `ready` false. An unexported
fast-forward source needs `differential_retry_export` first (`merge_export_required`).

Ordinary single-writer S3 snapshots now record parents in the same local FULL transaction as
publication; merges record both parents. These records are not a portable remote DAG. Older
S3 snapshots with missing parents remain unknown. The existing direct-sibling fallback can
prove an initial merge, but never substitutes the original fork after a target merge/restore.
Incomplete or ambiguous history requires the explicit snapshot API. No ancestor is inferred
from snapshot numbers, equal roots or timestamps.

Inputs are frozen and pinned before comparison. Evicted inputs are reconstructed from their
verified immutable descriptors into disposable local files, without changing their eviction
flags. The input byte limit is checked before download. Downloaded payload writes and native
result writes share `max_stage_write_bytes`; local staging, journal and SQLite overhead are
not a physical disk quota. DuckDB's local temporary-directory and spill limits still apply.
Cancellation/deadline checks occur between transfer batches, export objects and SDK calls;
an in-flight SDK request may run until its configured timeout/retries finish.

Publication has separate durable steps:

1. Validate/checkpoint the result, reserve the destination name and snapshot number, and
   FULL-commit a private publication record plus an `export_pending` retention root. A new destination is still absent; an existing target still has its old local head and a
   durable intent excluding native attachments and lifecycle changes. Normal snapshots can continue,
   skipping the reserved number; snapshot numbers may therefore have gaps.
2. Upload and verify packs, indexes, descriptor and commit record using the existing SDK
   export path. Ordinary/background export retries do not pick up private merge results.
3. FULL-commit head-write intent, then publish **only the destination head**. New names use `If-None-Match: *`; any existing key,
   including tombstones, is refused. Existing targets use `If-Match` with the exact ETag frozen
   after verifying the selected target snapshot and repository ownership. A changed remote
   head is `head_conflict`; retries never adopt its ETag or silently rebase. The precommitted write ID
   identifies this exact result if the response is lost. This head write is the remote
   publication point; there is no distributed transaction spanning SQLite and S3.
4. In one local FULL transaction, expose the branch/snapshot/export record, record provenance
   and mark the merge complete. A crash between steps 3 and 4 leaves a valid remote result;
   the same request completes local bookkeeping without publishing another result.

Fast-forward reuses the source snapshot/export and conditionally moves the target head;
no-op verifies the frozen remote head and completes locally without an S3 PUT. Neither
allocates a new snapshot. All paths still perform the supported-input/comparison checks.
Their provenance is a request audit, never a rewrite of source snapshot metadata. A prepared
fast-forward/no-op temporarily blocks expiry of its result snapshot (`merge_pending_operation`)
until completion or abort, so recovery cannot install an expired result. Input ancestry payload
is otherwise not retained indefinitely.

Ordinary retry-export/background/claim publication skips a target with a pending merge intent.
Skipped targets are omitted from claim results; a returned row must not imply that they were claimed.
This prevents an already-published S3 result from being rolled back to the old local head before
local finalization. Prepared target failures/cancellation keep the intent and result root; retries
reacquire the target lease and can finish without input snapshots or decision tables. A safe
abort releases them. After a possible conditional PUT, an unchanged old ETag cannot prove
failure: a request may still arrive after process death. Abort reports `merge_publication_uncertain`
until the result is observed, or another version fences that pending conditional write. If the
result is already remote, abort completes local publication instead of discarding it. A no-op
has no remote write to make uncertain and may be safely aborted before local completion.

S3 mutations require a nonempty `request_id` (`merge_request_required` otherwise), so an
interrupted caller can always address its reservation. Repeat the same call after interruption.
Before durable preparation a retry recomputes and requires retained inputs. After preparation
it resumes the frozen validated result even if inputs subsequently expire or resolution
sources disappear. Completed replay needs no S3 access. Changed semantic arguments fail.
Reserved names reject competing local branch creation; conditional publication rejects remote
competitors without overwriting them. `differential_merge_requests(database)` shows durable
request states and retained results; `differential_merge_status()` shows active phases including
`materializing`, `uploading`, `head_checking`, `head_creating`, `head_updating` and `finalizing`.

Before preparation, failures/cancellation clean up temporary files and pins (or GC reconciles
them after process death). After preparation, failure retains the validated output and name
reservation for retry or safe abort using `differential_abort_merge` below.
Immutable objects uploaded before a failure are not deleted, consistent with existing export
behavior. No extra ancestor retention is introduced. Remote commit metadata carries both
input IDs/roots and the algorithm/operation identity; it is not portable merge-DAG support.

`materialization_ms` measures evicted input reconstruction; `comparison_ms` excludes that
time; `application_ms` measures native staging/validation/checkpoint and durable preparation;
`publication_ms` measures upload, conditional head publication and finalization for S3, or
the atomic metadata commit for local publication. Local calls have zero materialization time.
A resumed prepared result
reports only publication work; completed replay reports zero timings.

## Durable requests and safe abort

`differential_merge_requests(database, request_id => '', max_results => 1000,
after_operation_id => '')` reads durable local metadata without credentials, S3 calls, input
materialization or pins. It covers snapshot and branch requests, including failed requests
and calls without a user request ID. Results are ordered by operation UUID; pass the last
`operation_id` as `after_operation_id` for the next bounded page (1–1000 rows). Pages are
independent metadata views, not a frozen listing across concurrent new requests. A nonempty
`request_id` filters the listing exactly. It is separate from process-local live progress.

Each row identifies the inputs and destination, frozen `schema_mode`/`schema_algorithm`, any prepared/result snapshot and root, remote
descriptor, whether the destination is reserved, retained logical bytes, creation time and
recovery guidance. Retained bytes describe the **private output's logical size**, not unique
physical disk usage or all input/staging pins. `result_snapshot_id` on a pending/aborted S3
request is only its reserved number, not proof of a public snapshot.

### Durable attempt diagnostics

Request inspection also reports the original `ours_branch_name`/`theirs_branch_name` and
branch UUIDs, even after branch names are reused. `inputs_guarded` says whether the caller
supplied all three expected snapshot IDs; the frozen IDs in the same row are those values.
These five fields are NULL for explicit-snapshot requests.

An attempt begins only after admission, acquiring the request lock, and checking that the
request is still pending and active. `attempt_count` counts these durable starts. It excludes
preflight/preview, binding/planning, rejected guards, admission failures, cancelled lock waiters
and completed replay (including a waiter that finds the result already complete). A request
can therefore exist with zero attempts.

Only the latest attempt is stored: UUID, count, state, phase, safe error code, and started,
updated and finished timestamps. A retry replaces it and increments the count. The record is
at most 1 KiB and adds no retention root. At most twelve distinct coarse milestones are
written once each per attempt, plus start and success/failure; there is no per-table or per-row
durable progress write. Live table progress remains in `differential_merge_status()`.

| `last_attempt_state` | Meaning |
| --- | --- |
| `incomplete` | Start/milestone recorded, no final diagnostic. It may be active, interrupted, or have failed while diagnostics were unwritable. Finished timestamp and error code are NULL. |
| `failed` | A caught failure was recorded, with its actual work phase and safe error code. The request remains retryable unless separately aborted. |
| `succeeded` | The request completed publication, with phase `completed` and no error code. Recorded in the same FULL transaction as the result. |

No exception text, SQL, table names, provider responses or row values are copied to this
record. Known errors retain an allowlisted category (for example `merge_conflict`,
`merge_schema_conflict`, `merge_constraint`, `cancelled`, `merge_timeout`, `disk_full`);
unclassified errors become `merge_error`. A failure while logging a failure does not replace
the original SQL error. A failure writing completion rolls back local publication. For S3,
a head may already exist, so the existing uncertain-publication recovery procedure applies.

Timestamps use microsecond wall-clock time and are clamped against the preceding update to
remain nondecreasing, including across retries. They are not monotonic duration measurements.
The request's `state` and `recovery_action` remain the recovery authority. Ordinary abort does
not count as a merge attempt or erase its diagnostics. Abort that recovers an already-published
S3 result marks that attempt successful atomically with local completion, even if its previous
failure had been recorded. Neither inspection nor replay probes for process liveness or adds
ancestor retention.

| State | Meaning |
| --- | --- |
| `pending` | Request recorded; no durable private output. Retry needs the original retained inputs. |
| `prepared` | Validated S3 output retained; no completed immutable export recorded. |
| `uploaded` | Immutable export recorded; no head-write attempt recorded. |
| `publication_uncertain` | A head write may have been sent; local completion is absent. Inspection does not query S3. |
| `complete` | Result published and local completion recorded. |
| `aborted` | Terminal request; it cannot be resumed or reused. |

To abandon a request, use its **operation UUID**, not its user request ID:

```sql
SELECT operation_id, state, destination, retained_logical_bytes, recovery_action
FROM differential_merge_requests('sales', request_id => 's3-merge-1');
-- Substitute the operation_id returned above:
SELECT * FROM differential_abort_merge('sales', 'merge-operation-uuid', request_id => 'abort-1');
```

Abort accepts `timeout_ms` (default 600,000) and requires autocommit. Its own `request_id`
identifies the abort action; changed arguments are rejected and completed retries return the
same action/result without network access. A fresh abort action against an already aborted
or completed merge is also safe. Output is `operation_id` (the abort action),
`merge_operation_id`, `state` (`aborted` or `complete`) and nullable `snapshot_id`.

All attempts for one merge hold a repository-local OS file lock from before input work
through publication and cleanup. Abort waits for that lock, honoring cancellation and its
deadline. It does not forcibly stop a running merge: if the publisher completes first,
abort returns `complete` and preserves that result. A publisher waiting behind an abort
rechecks the durable terminal marker and fails with `merge_aborted`. Lock files remain in
`locks/`; kernel ownership is released on process death. Locks have the same repository-local
scope as existing branch locks; this is not cross-host merge coordination. Abort uses the
existing process admission limit and may return `merge_busy` while all slots are occupied.
Deploy this increment after stopping older extension processes: every publisher must use
the request lock and terminal-marker checks. Concurrent mixed-version writers are unsupported.

For an unpublished local merge, abort records a terminal marker without changing branches.
For a prepared S3 merge, it first checks the remote destination under the publication lock:

- An exact matching published head completes local recovery and returns `complete`.
- No head and **no recorded write intent** permits abandonment. A verified foreign existing head
  also fences the merge's create-only PUT under the single-writer protocol and is preserved.
- An absent head after write intent, a mismatched head carrying this result's identity, or
  an unreadable object (including an unrecognized tombstone), or a failed remote check retains
  the output and reservation. `merge_publication_uncertain`
  instructs the caller to retry the original merge. Absence cannot disprove a delayed PUT
  from a process that has died.

Safe abandonment FULL-commits the terminal marker, releases the owned name reservation and
marks the output retention root abandoned atomically with completion of the abort action.
Before-commit crashes roll back; after-commit retries return the same outcome. Stale pins and
stage files are reclaimed by ordinary GC. Audit records, frozen resolutions and reserved
snapshot-number gaps remain. Uploaded immutable remote objects are never deleted. A new
request may use a released local destination; any existing S3 head/tombstone still forbids
remote name reuse. No ancestor payload retention or source-branch mutation is introduced.

Binding, PREPARE and plain EXPLAIN for both lifecycle functions have no persistent effects.

## State-based semantics

Values are compared in their native types using `IS [NOT] DISTINCT FROM`. For each non-key
cell in a row present in all three inputs:

| Condition | Result |
| --- | --- |
| Ours equals theirs | Agreed value |
| Ours equals base | Theirs |
| Theirs equals base | Ours |
| All differ | `cell` conflict |

Row presence is independent of nullable cell values:

| Situation | Result |
| --- | --- |
| One-sided insert | Insert |
| Identical insert on both sides | Insert once |
| Different inserts with the same key | `insert_insert` conflict |
| Both delete | Delete |
| One deletes, other unchanged | Delete |
| One deletes, other updates | `delete_update` conflict |

A primary-key change is deletion plus insertion. Counter changes from 0 to 1 on both sides
produce 1. There is no additive counter arithmetic or inferred row continuity.

## Preview and conflict details

```sql
SELECT schema_name, table_name, ours_inserts, ours_updates, ours_deletes,
       theirs_inserts, theirs_updates, theirs_deletes, conflicts
FROM differential_merge_snapshots_preview('example', 1, 2, 3, conflict_limit => 0);

SELECT unnest(conflict_details) AS conflict
FROM differential_merge_snapshots_preview('example', 1, 2, 3,
                               conflict_limit => 100, conflict_offset => 0);
```

Data counts are exact per table after a successful schema plan (NULL if schema conflicts block
comparison). A data conflict counts a row once, regardless of conflicting columns.
`conflict_limit` defaults to 100, ranges from 0 to 1000, and bounds details **across the whole preview**;
`conflict_offset` is a nonnegative global offset in table-name order, then primary-key order. Subsequent pages
recompute the comparison from the same immutable inputs. By default, preview validates schema
and detects data conflicts. `validation_state='not_checked'` explicitly indicates that final
constraints have not been tested.

Pass `validate => true` to build and checkpoint a disposable result through the same application
path as merge, without creating an operation, snapshot or branch. `validation_state` is `valid`,
`constraint_violation`, `data_conflicts`, or `blocked_by_conflicts`; the last state means another
table has conflicts and application was skipped. `validation_error` gives a bounded native
constraint diagnostic. Application/I/O/resource errors still fail the query. A valid preview
does not reserve a destination or resources for a subsequent merge.

Each detail is a STRUCT with `key`, `kind`, `columns`, `base_present`, `ours_present`,
`theirs_present`, and `base_values`, `ours_values`, `theirs_values`. Key and value rows are
lists of typed cells: `name`, `sql_type`, `is_null`, `boolean_value`, `integer_value`
(HUGEINT), `text_value`, `decimal_unscaled` (HUGEINT), `decimal_width`, `decimal_scale`,
`date_value` (DATE) and `timestamp_value` (TIMESTAMP).
The additional native payload fields are:

| Field | Native SQL type |
| --- | --- |
| `unsigned_integer_value` | UHUGEINT |
| `float_value`, `double_value` | FLOAT, DOUBLE respectively |
| `uuid_value`, `blob_value` | UUID, BLOB respectively |
| `time_value`, `time_ns_value`, `time_tz_value` | TIME, TIME_NS, TIME WITH TIME ZONE respectively |
| `timestamp_s_value`, `timestamp_ms_value`, `timestamp_ns_value` | TIMESTAMP_S, TIMESTAMP_MS, TIMESTAMP_NS respectively |
| `timestamp_tz_value`, `timestamp_tz_ns_value` | TIMESTAMPTZ, TIMESTAMPTZ_NS respectively |
| `interval_value` | INTERVAL |

Only the field appropriate to the declared scalar type is populated. Decimal unscaled values are
integers, never strings or floating point. Missing rows have an empty value list and a false
presence flag; a present row containing NULL retains its column list and declared types.
For delete/update conflicts, `columns` names the surviving side's updated columns.

Each cell also has `nested_value`, a list of `STRUCT(parent BIGINT, position BIGINT, cell STRUCT(...))`.
Its `cell` contains all scalar fields above, including `name`, `sql_type` and `is_null`, but no
recursive `nested_value` field. Scalar values have an empty list. A nested value has a flat
tree in depth-first order, with one-based node indexes. Its root is node 1, with parent/position
zero and the column name. Every child identifies its parent node and one-based position.
STRUCT children carry field names; TUPLE/list/array children have empty names and positional
identity. MAP children are ordered entry STRUCTs containing `key` and `value` children.
A non-NULL UNION has one active child: its name and position identify the declared member,
even if that member's value is NULL. A NULL container has one NULL node and no children;
an empty container has a non-NULL node and no children. Container nodes have NULL scalar
payloads; leaf nodes populate their exact native payload field.

This is a queryable typed representation, not JSON or a recursive merge policy. For example,
unnest `conflict_details[1].ours_values[2].nested_value` and select `node.parent`,
`node.position`, `node.cell.name`, `node.cell.sql_type` and the desired payload field.
The same representation is returned by the durable resolution audit. Resolution source tables
still contain the original native column types, not diagnostic trees.

Details obey both a row budget and `conflict_bytes` (default 1 MiB, maximum 16 MiB, 0 for no
details), shared across all tables. SQL computes a conservative payload charge before fetching
values into C++: 128 bytes per top-level typed cell plus name/type bytes, container/column-name
allowances, every UTF-8/BLOB byte, and recursive allowances of at least 512 bytes per nested
node (including NULLs and empty containers). Resolution capture uses the same recursive
payload accounting before its existing 8 MiB logical/serialized limits. `detail_bytes` reports
that conservative charge, not exact heap/RSS.
Values are never silently truncated. `details_truncated` identifies tables with further conflicts;
`next_conflict_offset` is the same global continuation offset on every output row. An oversized
first conflict returns no details and leaves the offset unchanged: increase the budget or inspect
those input rows directly. Later smaller conflicts are never returned ahead of it.
Engine temporary relations may spill through normal local `temp_directory` storage.

## Explicit typed conflict resolution

All four merge/preview functions accept `resolution_tables MAP(VARCHAR,VARCHAR)`. Each entry
maps a target `main.table` to a **committed local native table** named `catalog.schema.table`.
Caller-local temporary tables and views are not resolution sources. The source must have
exactly the target's column names, order and types, followed by `_merge_action VARCHAR` and
`_merge_columns MAP(VARCHAR,VARCHAR)`. These two names are reserved when supplying resolutions.
Primary-key columns identify the conflicting row; other columns supply custom values.

| Conflict | `_merge_action` | `_merge_columns` | Meaning |
| --- | --- | --- | --- |
| Cell | `cells` | Exactly every conflicting column mapped to `ours`, `theirs` or `custom` | Resolve only those cells; retain automatic merging of other cells. |
| Insert/insert or delete/update | `ours` or `theirs` | NULL or empty | Choose that entire row state, including absence. |
| Insert/insert or delete/update | `delete` | NULL or empty | Omit the row. |
| Insert/insert or delete/update | `custom` | NULL or empty | Use the supplied typed row, preserving its identifying key. |

Unused source values are ignored but retained in the audit. `custom` can supply a genuine SQL
NULL; row absence is represented only by a row action. Primary keys cannot be changed through
a decision. Duplicate/NULL keys, keys without a conflict, incorrect types, invalid actions or
column maps fail with `merge_resolution`. Missing decisions leave unresolved conflicts;
publication still requires zero unresolved conflicts. Native constraints are checked afterward.

For a table `items(id INTEGER PRIMARY KEY, label VARCHAR, amount DECIMAL(18,3))`, a decision
for a conflicting label can be prepared as follows (use the IDs returned by your preview):

```sql
CREATE TABLE item_choices(id INTEGER, label VARCHAR, amount DECIMAL(18,3),
                         _merge_action VARCHAR, _merge_columns MAP(VARCHAR,VARCHAR));
INSERT INTO item_choices VALUES(1, 'reviewed label', NULL, 'cells', map{'label':'custom'});
SELECT conflicts, resolved_conflicts, validation_state
FROM differential_merge_snapshots_preview('example', 1, 5, 6, validate => true,
  resolution_tables => map{'main.items':'memory.main.item_choices'});
SELECT * FROM differential_merge_snapshots('example', 1, 5, 6, 'resolved', request_id => 'resolve-1',
  resolution_tables => map{'main.items':'memory.main.item_choices'});
SELECT * FROM differential_merge_resolutions('example', 'resolve-1');
```

`conflicts` and `conflict_details` describe remaining conflicts. `resolved_conflicts` counts
rows resolved by explicit decisions. Input change counts still describe the original snapshots.
Use a nonempty `request_id` for audit retrieval and safe retries. The audit returns `schema_name`, `table_name`, typed `key`, `action`, the column-choice map
`columns`, typed source `values`, and the frozen `base_snapshot_id`, `ours_snapshot_id`,
`theirs_snapshot_id`, using the same cell representation as preview.
An audit records submitted choices, including choices from failed attempts; it is not proof of
publication. Check the parent merge operation's state for completion.

A request reserves its resolution-table mapping in the same FULL transaction as its input
selection. The first successful capture reads all source tables in one DuckDB transaction,
then durably freezes their typed contents before reconciliation. Pending and completed retries
use those contents even if source tables change or disappear. Changing the mapping with the
same request fails. To change already frozen choices, use a new request ID. A failure before
capture can retry reading the same source names; no choice data was frozen yet. Preview reads
current committed choices and creates no durable reservation. To freeze exactly the reviewed
choices, leave those source tables unchanged between preview and the first merge capture.

Completed merge replay validates the reserved resolution mapping without decoding the choice
envelope, so it survives an unavailable resolution encoding. Pending execution and audit
retrieval still reject unavailable encodings explicitly.

Resolution capture is bounded to 10,000 rows and an 8 MiB conservative logical budget across
all tables (512 bytes per cell, row overhead, and string/map bytes). The serialized envelope
also has an 8 MiB hard limit. The source mapping is limited to 64 KiB. The worker resolves generated SQL against built-in functions, isolated from caller macros.
SQL performs joins and validation; bulk append transfers bounded frozen values without per-row SQL calls. Audit data
persists with operation history and can contain sensitive user values, like the database itself.
There is currently no resolution-record pruning API. These records retain no input snapshots.

## Inspecting ancestry

`differential_merge_ancestry(database, ours BIGINT, theirs BIGINT,
max_ancestry_nodes => 100000, timeout_ms => 600000)` walks the same recorded graph as automatic
selection, without selecting live branch state or taking snapshot payload pins. Supply published
head IDs from `differential_branches` when inspecting branch names.

Each reachable snapshot returns `snapshot_id`, `branch_id`, `kind`, `parents_known`, a typed
BIGINT list `parents`, `reachable_from` (`ours`, `theirs`, `both`), `candidate`, `retired`,
`readable`, `selection_state` and nullable `selected_base`. `selection_state` is `unique`,
`ambiguous`, `unrelated` or `incomplete`. Candidates in incomplete graphs are provisional;
`selected_base` is populated only for `unique`. Availability remains separate: an expired
unique base can be shown but cannot be merged. `readable` describes retention/eviction
metadata; inspection does not probe the payload files. Unknown parents are never guessed; an empty
list with `parents_known=false` is not evidence of a root. This inspection does not apply the
fork proof. Unsupported ancestry versions and corrupt graphs still fail.

Rows are sorted by ID for presentation only. Graph memory/output is bounded by
`max_ancestry_nodes` (1–1,000,000). All work happens at execution, including cancellation and
timeout checks. Inspection does not create operations, branches, snapshots or retention roots.

## Resource controls and live status

All merge/preview functions accept positive `max_input_rows` (default 10,000,000 across distinct input snapshot
IDs), `max_input_bytes` (default 1 GiB, summed logical input file sizes), `max_stage_write_bytes`
(default 1 GiB), and `timeout_ms` (default 600,000). These options do not change merge semantics
and are excluded from request identity, so a resource failure can be retried with larger budgets.
The input row limit is checked before comparison.

Staging counts attempted writes and file growth across the main file and sidecars, including
overwrites, before entry into the byte store. Forked bytes are counted by the input limit.
This is not a physical disk quota: SQLite/journal overhead, existing repository data and temporary
spill files are separate. Configure native `memory_limit`, `threads`, `temp_directory` and
`max_temp_directory_size`. Native index construction must fit its working set in memory; spilling
comparison operators cannot guarantee this. Deadlines/cancellation are checked between engine
tasks and phases. A native task is not forcibly terminated; cleanup can outlast the deadline.

`SET differential_merge_max_concurrent=2` limits active merges, previews and aborts across all DuckDB
instances in this process (1–16; the last value set applies). Excess calls fail with `merge_busy`;
there is no implicit queue. Completed-request replay needs no admission slot.

`differential_merge_status()` lists this repository's active executions in the current process:
execution/operation/database IDs, mode, phase, table, table progress, input rows/bytes, changed
rows, conflicts, detail bytes, staging-write usage/limit, elapsed time and deadline. Query from
another connection in the same process. Entries disappear after cleanup; they are not durable
history. `differential_merge_requests()` reports durable pending, completed and aborted outcomes.
A pending record may represent an interrupted or failed attempt. Retry with the same arguments;
ordinary GC reclaims abandoned staging after a dead process.
Durably prepared S3 output is retained separately until publication finishes or safe abort releases it.
`context_frozen` marks native expression-context capture. Catalog mode adds removal-proof
phases; view removal and recreation phases also occur in strict/additive mode. During
`view_created`, the existing `table_name` progress field carries the view's name. Table
progress includes planned removals but excludes views; catalog removals do not increment
the row-change counter.

## Application, provenance and recovery

The phases are separate:

1. Pin and attach immutable inputs. Validate/plan all catalogs; schema conflicts stop here.
   Generate typed temporary
   changesets with table identity, key, old/new values, row-presence flags, side operation
   and per-column changed flags. Reconcile all tables before creating output.
2. Apply explicit frozen resolutions, if supplied. Any remaining data conflict fails with
   `merge_conflict` and publishes nothing.
3. Fork **ours** into a process-local, pin-owned staging namespace. For each affected table,
   materialize the final rows, drop/recreate its planned supported definition, and bulk insert.
   Fresh indexes avoid eager uniqueness failures against old entries. Unaffected tables
   retain their native image. All schema objects are either supported or rejected beforehand.
4. Validate through native constraints, checkpoint, close the staging attachment and seal
   its root. Publish the new branch (or atomically replace the detached local target), public snapshot and completed operation together in
   one existing FULL metadata transaction. For S3, upload and conditional head creation
   precede this local transaction, as described above. No pending local branch is exposed.

Final-state uniqueness, NOT NULL and CHECK violations report `merge_constraint`, separately from data conflicts.
Other native application/resource failures propagate as application errors, not conflicts.
The staged file is disposable, so partial table rebuilds cannot affect an input or publish
an incomplete destination. No constraint is disabled.

`operations.kind='merge'` uses the existing length-prefixed payload for database UUID,
base/ours/theirs IDs, destination and `logical-cell-v1`. The published snapshot's
`commit_extra_info` contains JSON with the algorithm, operation UUID and all three snapshot
IDs and roots, plus `schema_mode` and `schema_algorithm`. The operation links the request identity. Branch requests use `operations.kind='merge_branch'`
with `unique-base-v1` (or the `sibling-fork-v1` fork proof), both source branch UUIDs and the frozen input IDs; their provenance
also includes `base_selection`, `ours_branch_id` and `theirs_branch_id`. This adds no control schema or
native storage format; `logical-cell-v1` versions the merge algorithm. Provenance is a record,
not a retention root. Separate `snapshot_ancestry` records provide the local parent graph.

For explicit snapshots, the same completed request with the same five positional arguments,
schema mode and resolution mapping returns its original snapshot ID,
even if inputs were retired or its stored algorithm tag is no longer implemented; timing columns
return zero on replay. Unknown algorithms on pending requests are refused. Different
arguments with that request fail. An interrupted local request (or S3 request before durable
preparation) recomputes from the fixed snapshot
inputs and a new staging namespace. If inputs expired meanwhile it fails, without choosing
another base. Concurrent destination publication is checked again atomically.

Normal failure/cancellation detaches inputs and staging, unlinks staging logical files and
releases pins. After process death, existing stale-pin reconciliation unlinks the dead pin's
staging main/WAL names; ordinary GC can collect their data. The retry can run before GC.
Local pending operation records do not retain ancestors or staging; prepared S3 requests retain
their validated output for publication retry. A crash after the publication
commit leaves one completed output and replay returns it. There is no automatic resume
worker. Cancellation before publication stops that attempt and leaves the request retryable;
only explicit safe abort makes it terminal. Cancellation racing an already committed
publication is recovered through request replay.

Errors use `merge_base_unavailable`, `merge_base_ambiguous`, `merge_unsupported`, `merge_schema_conflict`, `merge_conflict`, `merge_resolution`, `merge_constraint`, `merge_application`, `merge_limit`,
`merge_timeout`, `merge_busy`, `merge_request_required`, `merge_request`, `merge_aborted`,
`merge_publication_uncertain`, `merge_target_unpublished`, `merge_export_required`, `merge_pending_operation`, `head_conflict`, `name_conflict`, and
existing missing/retired/request errors. Run preview to inspect unresolved data conflicts.
Resolution choices must be explicit; there is no automatic global winner policy.

## Performance and validation

Comparison scans immutable snapshots and can rebuild full affected tables even when few keys
change. Native memory and temporary-disk budgets, table width, indexes and S3 input/output all
contribute to cost. See [performance and sizing](benchmarks.md) for capacity considerations.

Use [merge qualification](merge-qualification.md) for a pilot with independently prepared
expected data, bounded scale measurements and interruption/recovery checks. Contributor test
commands are in [CONTRIBUTING.md](../CONTRIBUTING.md). Tests and measured workloads do not
replace validation of the intended platform, provider, filesystem and data.

## Upgrading from 1.0.0

Repositories written by 1.0.0 open unchanged (store format 2, control format 1). Nothing is
migrated and no history is backfilled. From then on, every snapshot records its parents in
the same transaction that publishes it; merges record both input heads.

Snapshots written by 1.0.0 have no parent records, so automatic base selection works as follows:

- **Branches forked in 1.0.0 from the same snapshot** merge through the fork proof
  (`sibling-fork-v1`), as long as neither was restored or merged into since. This covers the
  first merge of existing branches.
- **A repeated merge that reaches 1.0.0 snapshots** fails with `merge_base_unavailable` when one
  side reaches a 1.0.0 snapshot that is not below the common base. Its unknown history could hold
  a better base, so the selector refuses instead of guessing. For example, after merging two 1.0.0
  branches into `merged`, merging `merged` with one of them again needs
  `differential_merge_snapshots` with the base you choose.
- **Branches forked after the upgrade** select their base automatically, also when their history
  reaches back into 1.0.0 below that base.

The fork proof reads 1.0.0's completed branch-creation records. `differential_cleanup_metadata`,
which 1.0.0 also provides, deletes completed operation records older than its cutoff, including
these. Once they are gone, the first merge of those branches needs an explicit base as well.
Cleanup deletes records by age, so it never removes a restore or merge record while keeping the
older fork record it would contradict. The proof fails safe and never selects a wrong base.
Cleanup by this version never deletes the parent records it writes.

## Downgrading is not supported

After this version has written to a repository, do not open the repository with 1.0.0 or any
other release without branch merging. Keep a copy of the repository directory from before the
upgrade if you may need to go back.

- 1.0.0 does not recognize merge records. Its `differential_cleanup_metadata` deletes completed
  snapshot parent records and merge companion records along with other old operations. Later
  merges then lose their recorded ancestry, and pending or aborted merges lose the records that
  define their retry or terminal state; an aborted request can become retryable again.
- 1.0.0 does not honor merge fences or destination reservations. Its export retry, background
  publication and head claims do not skip a target that a pending S3 merge holds, so they can
  roll an already published S3 merge result back to the target's old head.
- 1.0.0 knows none of the merge functions, so pending requests cannot be retried or aborted.

## Deferred work

Multiple best bases still require caller input; recursive virtual merge bases remain deferred. Ancestry compaction and broader history-scale qualification need further work. Existing-target publication supports local and single-writer S3 storage with exclusive ownership; live target attachments remain outside scope.
Removal of constrained/defaulted/key columns, type/default/constraint changes, schema-conflict resolution and broader view
SQL remain deferred. Renames are represented only as independent name-based removals/additions;
there is no rename tracking. Foreign keys require a separately tested constraint strategy.
Shared-writer S3 merging, cross-host request recovery and portable remote ancestry need separate protocols
and qualification. Snapshot and branch single-writer S3 merging are supported as described above.
Nested columns use whole-value merging. Recursive field/element resolution, dynamic VARIANT,
JSON-specific behavior, custom types and nested primary keys remain outside the supported scope.
