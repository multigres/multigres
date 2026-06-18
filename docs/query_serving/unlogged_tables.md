# Unlogged Tables

## Overview

Multigres supports PostgreSQL `UNLOGGED` tables. They are useful for short-lived,
high-throughput workloads (scratch/staging data, storage benchmarks) where the
write amplification of WAL is not worth paying and the data does not need to
survive a crash.

The trade-off is failover behaviour. Unlogged table **data** is never written to
the WAL, so it is never streamed to standbys. After a failover the new primary
has the table's catalog entry but no data — PostgreSQL resets every unlogged
table to empty as part of promotion. To avoid silently presenting an empty table
as if nothing happened, the freshly promoted primary **best-effort drops** every
unlogged table so clients get an explicit error and can rebuild from scratch.

## When to use one — and when not to

Reach for an unlogged table only when **all** of these hold: the data is shared
across sessions, it is large or write-heavy enough that skipping WAL is a
meaningful win, and losing it on a crash or failover is acceptable.

Prefer an alternative when you can:

- **Temporary tables** (`CREATE TEMP TABLE`) are the better choice when the data
  is scoped to a single session. They are also unlogged (fast writes), but they
  are private to the connection and disappear at session end, so they sidestep
  the failover problem entirely — there is no shared state to lose. Multigres
  pins a temp table to its backend connection for the session's lifetime, so it
  behaves as expected. Reach for an unlogged table only when the data genuinely
  needs to be visible to other sessions.
- **Regular (logged) tables** when the data must survive crashes/failovers. The
  WAL overhead is the price of durability and replication.

In short: if a temporary table would do, use one — it avoids everything
described below.

## Background

An `UNLOGGED` table skips WAL for its contents. This makes writes faster but has
two consequences:

- **Not crash-safe.** On an unclean shutdown PostgreSQL truncates the table
  during recovery (it reinitializes the relation from its empty init fork).
- **Not replicated.** Because there is no WAL for the data, standbys never
  receive it. A standby's copy of an unlogged table is always empty. When a
  standby is promoted (failover), the table is reset to empty.

The catalog row for the table _is_ WAL-logged, so `CREATE UNLOGGED TABLE`,
`CREATE VIEW`, and other DDL replicate normally. After a failover the new primary
therefore knows the table exists; it just has no rows.

## Create-Time Warning

Because the failover behaviour is surprising, the multigateway emits a `WARNING`
NoticeResponse (SQLSTATE `01000`) every time a client creates an unlogged table —
`CREATE UNLOGGED TABLE`, `CREATE UNLOGGED TABLE ... AS`, and `SELECT ... INTO
UNLOGGED`. The statement still succeeds; the warning just rides alongside the
`CommandComplete` and points the user at this document:

```text
WARNING:  unlogged table data is not replicated and is lost on failover
HINT:  On failover the table is dropped, or left empty if other objects depend
on it; rebuild it from scratch. See docs/query_serving/unlogged_tables.md.
```

`CREATE UNLOGGED SEQUENCE` gets a sequence-specific warning. Unlogged sequence
state is likewise not replicated, but the sweep does not drop sequences — on
failover the sequence simply restarts from its initial value:

```text
WARNING:  unlogged sequence is reset to its start value on failover
HINT:  Unlogged sequence state is not replicated; a failover restarts it from
its initial value. See docs/query_serving/unlogged_tables.md.
```

Materialized views cannot be unlogged in PostgreSQL — `CREATE UNLOGGED
MATERIALIZED VIEW` fails with `ERROR: materialized views cannot be unlogged` — so
there is no unlogged materialized-view state to worry about.

## Behaviour on Failover

When a standby is promoted to primary, the multipooler runs a best-effort sweep
that drops every unlogged ordinary table before the node begins serving client
traffic as PRIMARY. The two observable outcomes:

| Scenario                                  | After failover                                                                 |
| ----------------------------------------- | ------------------------------------------------------------------------------ |
| Unlogged table with no dependents         | **Dropped.** Queries return `42P01` (`relation does not exist`).               |
| Unlogged table a view/function depends on | **Kept but empty.** The drop is blocked by the dependency, so the table stays. |

The sweep deliberately uses plain `DROP TABLE` (never `CASCADE`) so it can never
destroy dependent user objects — a view or function built on top of an unlogged
table is always left intact.

For clients the contract is: **a previously populated unlogged table either
disappears (clear error) or comes back empty after a failover.** Either way the
data is gone, and the application should treat a failover as a signal to rebuild
the table and everything derived from it. The client's existing connection
survives the failover and transparently re-routes to the new primary, so the
client observes this directly on its next query rather than via a disconnect.

## Why a Drop Instead of an Empty Table

Leaving the reset (empty) table in place is the PostgreSQL default, but it is a
silent failure: `SELECT count(*)` returns `0` and the application has no way to
tell "the table was always empty" from "a failover wiped it". Dropping the table
turns that silent data loss into a loud `42P01` error, which is far easier to
detect and recover from. The drop is best effort precisely because we refuse to
cascade into the user's dependent objects; when a dependency blocks the drop the
table falls back to the silent-empty behaviour.

## Limitations

- **Best effort, not guaranteed.** A table referenced by a view, function, or
  other dependent object is left in place (empty) rather than dropped.
- **Extension-owned unlogged tables** (e.g. queue tables created by an
  extension) are swept the same as user tables. They typically have dependent
  functions, so the drop is usually blocked and the table is left empty.
- **Partitioned unlogged tables.** Only the leaf partitions (ordinary tables)
  are dropped individually; an empty partitioned parent may remain.
- The sweep runs on the promotion path only, so it does not affect a primary
  that restarts in place without a failover (PostgreSQL's own crash recovery
  already truncates unlogged tables in that case).

## Code Map

- **Create-time warning** — `UnloggedWarning` primitive (table/sequence/
  materialized-view variants) in
  `go/services/multigateway/engine/unlogged_warning.go`, attached ahead of the
  CREATE route by `maybeWrapUnloggedWarning` in
  `go/services/multigateway/planner/planner.go`.
- **Sweep logic** — `dropUnloggedTablesAfterPromotion` in
  `go/services/multipooler/internal/manager/manager.go`, invoked from
  `promoteStandbyToPrimary` after the node becomes a writable primary and before
  the topology transitions to PRIMARY/SERVING.
- **Unit tests** — `TestPromoteDropsUnloggedTables` in
  `go/services/multipooler/internal/manager/rpc_consensus_test.go` (verifies the
  sweep issues the drops and that a dependency-blocked drop does not fail the
  promotion).
- **End-to-end test** — `TestUnloggedTablesAfterFailover` in
  `go/test/endtoend/queryserving/unlogged_test.go` (real cluster: kills
  the primary, waits for a new one, and asserts the dropped/empty outcomes).
