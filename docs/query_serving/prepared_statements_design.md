# Prepared Statements and Portals in Multigres

## Overview

This document outlines the design for implementing prepared statements and
portals in Multigres, leveraging PostgreSQL's Extended Query Protocol to
optimize query execution performance.

## Background: Extended Query Protocol

The PostgreSQL Extended Query Protocol enables clients to split query
execution into three distinct phases:

1. **PARSE**: Analyze and validate the query structure with placeholders
2. **BIND**: Bind concrete parameter values to the prepared statement
   (creating a "portal")
3. **EXECUTE**: Run the bound query

This separation allows the same query pattern to be executed multiple times
with different parameters, avoiding the overhead of parsing and planning on
each iteration.

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

Multigateway will own and manage prepared statements and portals at the
connection level. This approach involves:

- **Connection-scoped storage**: Each client connection maintains its own
  namespace for prepared statements and portals
- **Parse phase**: Query parsing occurs when the PREPARE command is received
- **Bind phase**: Parameters are bound to create a portal
- **Execute phase**: The portal is planned and executed using the standard
  query execution path

### Design Benefits

- **Sharding agnostic**: This design works seamlessly across both single-shard
  and multi-shard scenarios
- **Consistent semantics**: Prepared statements behave as clients expect,
  regardless of the underlying data distribution

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

### Single-Shard Scenario

For the common case where a prepared statement targets a single shard, we can
push statement management down to the Multipooler level.

### How It Works

When a Multigateway executes a prepared statement that targets a single shard:

1. **Connection lookup**: The Multipooler checks if any connection in the pool
   already has this prepared statement
2. **Reuse or create**:
   - If found: Use the existing connection with the prepared statement
   - If not found: Prepare the statement on a new connection from the pool
3. **Execution**: Run the query using the prepared connection

### Pooler Optimization Benefits

- **No reserved connections**: Prepared statements work with connection pooling
  without requiring dedicated connections
- **Efficient reuse**: Statements are reused across multiple client requests
- **Transparent optimization**: This optimization is invisible to clients

### Connection Pool Tracking

Each connection in the Multipooler's pool must track:

- Which prepared statements exist on that connection
- The mapping between logical statement names and physical statement names

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
`ExecuteOptions.eager_parse_prepared_statement` (an unnamed backend `Parse`),
preserving PostgreSQL's transaction-time validation and lock-acquisition timing.

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
parallel-append tests); those tests continue to fail until multigres
supports pushing SQL-level PREPARE down to a backend session, which is a
separate architectural change.
