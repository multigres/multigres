# Prepared Statements and Portals in Multigres

## Overview

Multigres manages client prepared-statement names at the gateway and caches
named PostgreSQL statements on each pooler backend. Inside a transaction, a
client Parse prepares the named backend statement immediately; ordinary
Describe and Execute operations reuse that same statement.

See [prepared-statement testing](testing_strategy.md#prepared-statement-preparation-and-ddl)
for regression coverage and validation commands.

## Background: Extended Query Protocol

The PostgreSQL Extended Query Protocol enables clients to split query
execution into three distinct phases:

1. **PARSE**: Analyze and validate the query structure with placeholders
2. **BIND**: Bind concrete parameter values to the prepared statement
   (creating a "portal")
3. **EXECUTE**: Run the bound query

This separation allows the same query pattern to be executed multiple times
with different parameters, avoiding repeated preparation. PostgreSQL may
still choose or build an execution plan at Bind time; a backend Parse, the
gateway's SQL-to-AST parser, and the gateway's routing plan cache are separate
operations.

### Example

```sql
-- Parse phase: Create a prepared statement
PREPARE fooplan (int, text, bool, numeric) AS
    INSERT INTO foo VALUES($1, $2, $3, $4);

-- Execute phase: Run with different parameter sets
EXECUTE fooplan(1, 'Hunter Valley', 't', 200.00);
EXECUTE fooplan(2, 'Mountain Valley', 'f', 100.00);
```

## Core Design: Gateway-Level Management

### Statement Ownership

Multigateway owns prepared statements and portals at the client connection
level. This involves:

- **Connection-scoped storage**: Each client connection maintains its own
  namespace for prepared statements and portals
- **Parse phase**: Protocol Parse and SQL PREPARE reach `HandleParse`.
  Gateway function folding runs before preparation and registration. When
  `conn.TxnStatus()` is `TxnStatusInBlock`, the handler also prepares the
  named statement on the transaction's backend before acknowledging success
- **Bind phase**: Parameters are bound to create a portal
- **Execute phase**: The portal is planned and executed using the standard
  query execution path

### Design Benefits

- **Client namespaces**: Clients retain their own statement and portal names
  while backend statements can be shared across clients
- **Transaction semantics**: Backend preparation at Parse time preserves
  relation locking and semantic error timing inside a transaction

The current prepare and Describe paths target the default table group and
shard. This document describes that implementation; preparing across multiple
shards would require corresponding routing and per-backend tracking.

## Optimization 1: Cross-Connection Consolidation

### Motivation

Multiple client connections often create identical prepared statements.
Consolidating these statements can reduce memory overhead and improve
efficiency.

### Implementation

Instead of storing prepared statements per-connection, we maintain a shared
map at the Multigateway handler level:

- **Shared statement pool**: A single prepared statement is reused across
  multiple connections
- **Reference counting**: Track how many connections are using each prepared
  statement
- **Lifecycle management**: Automatically clean up statements when the
  reference count reaches zero

## Optimization 2: Pooler-Level Statement Management

> **Scope:** This pooler-level `ppstmt*` consolidation serves the **wire-protocol**
> extended-query path (client `Parse`/`Bind`/`Execute` messages), which reuses a
> backend prepared statement across binds. **SQL-level** `PREPARE`/`EXECUTE`
> commands no longer use it — they are materialized entirely at the gateway by
> argument substitution (see "SQL-level PREPARE / EXECUTE" below).

### Backend Selection and Reuse

The multipooler first selects the connection using normal pool acquisition or
an existing reservation. `ensurePrepared` then checks the prepared-statement
cache on **that connection**; it does not search the pool for a connection
already holding the statement.

1. Resolve `(query text, parameter types)` to a pooler name such as `ppstmt0`.
2. Reuse the per-connection cache entry when present and `force_reparse` is false.
3. If `force_reparse` is true, close the existing statement and remove its
   cache entry before preparing it again.
4. Parse the named statement when needed, storing the cache entry only after
   PostgreSQL accepts the Parse.

Autocommit statements can use regular pooled connections. Transactions use a
reserved backend, so preparation and subsequent operations on the same target
share the same PostgreSQL session. A statement cached on one backend says
nothing about whether another backend has prepared it.

### One Named Preparation Inside a Transaction

The receipt-time flow is:

```text
Client Parse / SQL PREPARE
  -> HandleParse: fold gateway functions
  -> PrepareInTransaction
  -> StreamExecute with prepare_only=true and force_reparse=true
  -> reserve/reuse backend and replay deferred BEGIN if needed
  -> ensurePrepared: Close existing ppstmt if present, then Parse ppstmt
  -> register statement in gateway consolidator; acknowledge success

Later Describe / Execute on the same backend and SQL
  -> ensurePrepared: cache hit
  -> Describe / Bind+Execute without another Parse
```

`prepare_only` is carried on `ExecuteSqlPreparedStatement`, but the pooler
executes no SQL wrapper for such a request. The preparation happens after any
deferred `BEGIN`, including when an existing non-transaction reservation is
promoted into a transaction. This keeps relation locks in the transaction and
surfaces missing-relation or other semantic errors at prepare time. A backend
Parse failure does not register the statement in the gateway and follows the
existing transaction-error handling.

The previous path used an unnamed backend Parse for validation and locks, then
separately prepared a named statement at Describe/Execute. The named cache now
retains the result of the receipt-time preparation, eliminating that duplicate
Parse for ordinary statements. Autocommit Parse remains lazy; preparing early
there would not guarantee that later operations use the same pooled backend.

This removes a duplicate backend Parse, not the required Describe or Execute
RPCs. Refreshing an existing named statement still requires Close followed by
Parse. The reduction in preparation work is covered by regression tests; it
does not establish an end-to-end latency improvement of any particular size.

### Prepare-Only Protocol Compatibility

`ExecuteSqlPreparedStatement.prepare_only` uses boolean field number 4,
previously named `force_unnamed_parse`. Retaining the field number permits
binary decoding, but the operation's semantics have changed: the gateway now
expects the pooler to prepare and cache the named statement before returning.
An older pooler only performs unnamed validation and does not satisfy that
expectation, even though it can decode the request.

The single-preparation contract requires both peers to implement the named
preparation behavior. Mixed-version operation needs separate compatibility
validation; it is not established by the same-version regression suites.

### Query Identity and Execution-Time Rewrites

The pooler's key uses exact SQL text and parameter type hints. A normalized
routing-cache key must not accidentally select a different backend statement.
`Route.PortalStreamExecute` preserves the stored SQL when the route SQL equals
either that SQL or its AST's `SqlString()` representation. For example, extra
spaces in `SELECT  *  FROM t WHERE id = $1` do not cause Execute to prepare a
second, normalized variant after Describe used the original.

If the route SQL differs semantically, the rewrite must still run. Examples
include changing a tracked `set_config` call to use `is_local=true` or replacing
a gateway-managed setting call with a constant. Such a route creates a new
`PreparedStatementInfo` with the rewritten SQL and the original name and
parameter types, and forwards the original portal's Bind values.

`reparsePending` records a fresh in-transaction Parse by the gateway's canonical
statement name. The original statement is already prepared; only a semantic
rewrite consumes this signal and sets `ForceReparse` on its newly allocated
metadata. Describe of the original never consumes it, and shared consolidator
metadata is never mutated. Subsequent use of that rewritten route reuses its
cache entry. Commit and rollback clear unused signals.

Thus the single-Parse optimization applies to an unchanged backend query on
the same reserved connection. A semantic rewrite is a distinct backend
statement and can require its own preparation.

### Schema Changes and Recovery

Backend statements are consolidated across clients. After DDL, a fresh client
Parse must not inherit the old parameter types or result shape from another
client's cached statement. In a transaction, `force_reparse` refreshes the
original immediately and, when applicable, the rewritten variant at its first
execution.

For portal execution and Describe, `cachedPlanRetry` also provides reactive
recovery. It closes and evicts a statement after a recognized stale-statement
error, then prepares and retries once if the backend transaction is not failed.
The recognized errors are `0A000` with the cached-plan result-type message,
`22P02`, and `42883`. The latter two can also be ordinary input/type errors;
retrying does not guarantee success.

If PostgreSQL has already aborted the transaction, the retry helper returns
the original error without re-Parse/retry. This is why proactive preparation
is needed for a fresh Parse inside a transaction. Autocommit recovery remains
reactive because later operations can land on different pooled backends.

## Prepared Statement Consolidation

The gateway and pooler have different consolidation needs and use separate
implementations.

### Deduplication Key

Both consolidators deduplicate by **(query text, parameter types)** — not
query text alone. The same SQL with different type hints (e.g.,
`SELECT $1` with `INT4` vs `TEXT`) produces different PostgreSQL prepared
statements with different plans and type coercion, so they must be tracked
separately.

### Gateway Consolidator (`Consolidator`)

The gateway consolidator maps `(connectionID, clientName) → canonical name`.
It has real per-client connection IDs to namespace by.

```go
type Consolidator struct {
    // Map from (query, paramTypes) dedup key to canonical prepared statement
    Stmts map[string]*PreparedStatementInfo

    // Map from connection ID and statement name to prepared statement reference
    Incoming map[uint32]map[string]*PreparedStatementInfo

    // Reference count: number of connections using each prepared statement
    UsageCount map[*PreparedStatementInfo]int
}
```

**Algorithm** — when processing `PREPARE stmt1 AS body1` with `paramTypes`:

1. **Check for existing statement**: Look up `dedupKey(body1, paramTypes)` in
   `Stmts`
2. **If exists**: increment usage count, store
   `Incoming[connectionId]["stmt1"] = existingPS`
3. **If not exists**: create a new canonical name (e.g., `stmt0`), store in
   `Stmts`, initialize usage count, store incoming mapping

**Name translation**: clients use their own names (`stmt1`, `myquery`); the
consolidator maps these to canonical names (`stmt0`, `stmt1`) shared across
connections with the same query.

### Pooler Consolidator (`PoolerConsolidator`)

The pooler consolidator is intentionally simpler. It receives requests from
multiple stateless gateway replicas, each of which independently assigns
canonical names starting from `stmt0`. Since different gateways can assign
the same name to different queries, the pooler **ignores incoming names
entirely** and deduplicates purely by (query text, parameter types).

```go
type PoolerConsolidator struct {
    // Map from (query, paramTypes) dedup key to canonical name
    Stmts map[string]string
}
```

**Algorithm** — `CanonicalName(query, paramTypes) → name`:

1. Compute `dedupKey(query, paramTypes)`
2. If key exists in `Stmts`, return the existing canonical name
3. Otherwise, generate a new name (e.g., `ppstmt0`), store it, return it

The `ppstmt` prefix distinguishes pooler-level names from gateway-level names.

Per-postgres-connection state (which statements are prepared on which backend
connection) is tracked separately by `connstate.ConnectionState`, not by the
consolidator.

### Why Two Consolidators?

The gateway consolidator needs per-connection name tracking, reference
counting, and lifecycle management because it maps client-chosen names to
shared canonical names across long-lived client connections.

The pooler consolidator needs none of that — it just needs a stable
`(query, paramTypes) → canonical name` mapping. Using the gateway
consolidator at the pooler level with a shared `connId=0` caused name
collisions when multiple gateway replicas sent the same canonical name
for different queries.

## SQL-level PREPARE / EXECUTE: argument substitution

SQL-level `PREPARE p AS <body>` / `EXECUTE p(args)` are **not** run as backend
prepared statements. The gateway substitutes the EXECUTE arguments into the
prepared body and runs the result as an ordinary query:

`PREPARE p AS SELECT $1, $2, $1` + `EXECUTE p(5, 10)` →
`SELECT CAST(5 AS int4), CAST(10 AS int4), CAST(5 AS int4)`

Each `$N` is replaced by its argument cast to the parameter's resolved type
(`RewritePreparedBody` in `execute_rewrite.go`), then deparsed and routed as a
plain query — no pooler-side `ppstmt*`, no name resolution, no backend
PREPARE/EXECUTE pair. This mirrors what PostgreSQL's EXECUTE does internally
(coerce each argument to the parameter's resolved type, then evaluate the body),
but as an AST rewrite. Extended-protocol bound arguments (`EXECUTE p($1)` with an
outer Bind) are resolved to literals first; see the `resolveExecuteArgs` doc
comment for that two-scope `$N` model.

### Parameter type resolution

The cast targets are the parameters' **resolved** type OIDs, obtained by an eager
backend `Describe` at PREPARE time (`SetResolvedParamTypes`). Resolved, not
declared, because a client may declare parameter types partially or not at all
(`PREPARE p AS SELECT $1`); PostgreSQL infers the concrete types by analyzing the
query against the catalog at PREPARE, and only the backend Describe reports the
result. Casting to the resolved base type (typmod −1) matches PostgreSQL's own
EXECUTE coercion (`prepare.c:EvaluateParams`).

Storage (`resolved` field on `PreparedStatementInfo`):

- **Last-writer-wins refresh.** Every PREPARE re-Describes and overwrites the
  shared resolution. A client PREPARE means "give me current info", so a PREPARE
  issued after DDL re-Describes and heals the entry for every connection that
  dedups onto it — the same principle as re-Parsing a stale wire-protocol
  statement. (A lone PREPARE, then DDL, then EXECUTE needs no heal: like
  PostgreSQL, parameter types are frozen at PREPARE, and the substituted body is
  re-planned fresh by PostgreSQL on every EXECUTE.)
- **Parameter types only, not the result shape.** PostgreSQL freezes parameter
  types at PREPARE, so a frozen copy stays faithful. It does **not** freeze the
  result shape — a Describe re-derives it against the current catalog (and raises
  `0A000` rather than serve a stale shape) — so a stored copy would diverge after
  DDL. Describe of a statement/portal is therefore answered by a live backend
  round-trip, never from stored fields.
- **Lock-free reads.** An `atomic.Pointer[[]uint32]`: EXECUTE-path reads are
  lock-free and each refresh is visible across the connections sharing the
  statement.

### In-transaction PREPARE

Inside an explicit transaction the eager Describe must observe transaction-local
state and take transaction-scoped locks, so it routes to the reserved backend via
`ExecuteOptions.eager_parse_prepared_statement`. The pooler prepares the
consolidated backend statement (after replaying any deferred `BEGIN`),
preserving PostgreSQL's transaction-time validation and lock-acquisition timing
and leaving the statement ready for a later `Describe`/`Execute` to reuse. The
carried `PreparedStatement.force_reparse` re-Parses a stale shared statement so
a fresh client `Parse` reflects the current catalog.

## Wrapped EXECUTE forms

PostgreSQL grammar allows an `ExecuteStmt` in three places: as a top-level
statement, inside `EXPLAIN`, and as the body of `CREATE TABLE ... AS EXECUTE`
(plus the nested `EXPLAIN CREATE TABLE ... AS EXECUTE`). The wrapped forms use the
same substitution: `MaterializeWrappedExecute` splices the substituted body into a
clone of the wrapper, which is then routed as plain SQL:

`EXPLAIN EXECUTE p(5)` → `EXPLAIN SELECT CAST(5 AS int4)`;
`CREATE TABLE t AS EXECUTE p(5)` → `CREATE TABLE t AS SELECT CAST(5 AS int4)`

Because the output is ordinary SQL with no backend prepared statement to keep
alive, the wrapped path has no connection-stickiness constraint: a silent
reconnect cannot lose statement state that no longer exists.

When a wrapped form actually executes the body (`CREATE TABLE ... AS EXECUTE`
always; `EXPLAIN ANALYZE EXECUTE` because `ANALYZE` runs the query), the body
is re-analyzed at plan time so the resulting route reserves whatever the body
itself needs — a temporary logical replication slot, a session advisory lock,
or `setseed(...)` — exactly as a plain `EXECUTE` of the same body would. A
body carrying a tracked `set_config` is refused here rather than run verbatim,
because this route has no session-state channel to record the value (see
`tryUnwrapWrappedExecute`); the same body under a plain `EXECUTE` still works.

### Scope and Known Limitation

This handles **SQL-level** wrapped EXECUTE reachable via the
PostgreSQL grammar:

- `EXPLAIN [options] EXECUTE p [(params)]`
- `CREATE [TEMP] TABLE t AS EXECUTE p [(params)]`
- `EXPLAIN [options] CREATE [TEMP] TABLE t AS EXECUTE p [(params)]`

It does **not** handle EXECUTE reached through PL/pgSQL dynamic SQL
(e.g. `EXECUTE format('explain execute %s', ...)` inside a server-side
function like `explain_filter` or `explain_parallel_append`). Those cases
run entirely on the backend session, which only sees the outer `SELECT`
that invokes the function — the gateway never parses the wrapped EXECUTE
and therefore cannot rewrite it. PostgreSQL's own `pg_regress` suite
exercises this pattern heavily (e.g. `explain.sql`, `partition_prune.sql`
parallel-append tests). Preparing a pooler-named `ppstmt` at receipt time does
not make the client's name available inside a backend function. Supporting
that namespace remains a separate architectural change.
