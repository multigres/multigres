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

## Wrapped EXECUTE Forms

PostgreSQL grammar allows an `ExecuteStmt` in three places: as a top-level
statement, inside `EXPLAIN`, and as the body of `CREATE TABLE ... AS EXECUTE`.
The top-level case is handled by `ExecutePrimitive` routing through
`PortalStreamExecute`, which calls `ensurePrepared()` on the chosen backend
connection and then runs a protocol-level Bind+Execute.

The wrapped forms — `EXPLAIN EXECUTE p`, `CREATE TABLE t AS EXECUTE p`, and
the nested `EXPLAIN CREATE TABLE t AS EXECUTE p` — cannot use the same path:
they are not "execute a prepared statement" operations, they are raw SQL
statements that happen to reference a prepared statement by name. The backend
session must therefore have a prepared statement under exactly that name when
the raw SQL runs.

### Planner-Side AST Rewrite

When the planner sees a wrapped EXECUTE, it:

1. Looks up the inner `ExecuteStmt.Name` (e.g. `p`) in the gateway
   consolidator, obtaining the canonical name (e.g. `stmt42`), the inner
   query body, and the param type OIDs.
2. **Mutates `ExecuteStmt.Name` in place** from the user name to the
   canonical name. The AST is freshly parsed per query in `HandleQuery`,
   so in-place mutation is safe.
3. Regenerates the wrapper SQL via `SqlString()`. The result references the
   canonical name, e.g. `EXPLAIN (COSTS OFF) EXECUTE stmt42`.
4. Builds a `Route` (or `TempTableRoute` for `CREATE TEMP TABLE ... AS
EXECUTE`) that carries the `PreparedStatement` metadata (the
   **gateway-assigned** canonical name, plus the query body and param types).

### Multipooler-Side ensurePreparedWithName

The multipooler's `StreamExecute` RPC reads `options.PreparedStatement`
(an optional field on `ExecuteOptions`). When set, it calls
`ensurePreparedWithName()` on the chosen backend connection before running
the query. Unlike `ensurePrepared()`, this variant uses the caller-supplied
name directly rather than deriving a pooler-canonical name: the rewritten
SQL references a specific name, and the backend must have a prepared
statement under exactly that name.

This works because the gateway consolidator assigns globally unique
monotonic names (`stmt0`, `stmt1`, ...) deduplicated by `(query, paramTypes)`,
so using them as backend-session prepared statement names is safe across
multiple gateway client sessions sharing a pool connection. On a given
backend connection it is possible to end up with **both** a pooler-canonical
entry (from top-level EXECUTE via `PortalStreamExecute`) **and** a
gateway-canonical entry (from wrapped EXECUTE via `StreamExecute`) for the
same query body. They are different names on the same connection, which
PostgreSQL allows; the cost is one extra `Parse` the first time each path
is exercised for a given query.

### Connection Stickiness

Because reconnection on a regular pool connection wipes per-connection
prepared statement state, the wrapped-EXECUTE path on the regular pool
cannot use the retry-on-connection-error variant of `QueryStreaming`: a
silent reconnect would leave the backend without the statement the
rewritten SQL references. The regular path uses plain `QueryStreaming`
instead and surfaces connection errors to the caller, who can reissue
the query at the application level.

The reserved-pool wrapped-EXECUTE path (`reserveAndStreamExecute` in
`executor.go` and the `portalExecuteWithReserved` new-conn branch) does
recover from stale sockets transparently: it wires `ensurePreparedWithName`
/ `ensurePrepared` through `reserved.WithValidate`, so a connection-class
error on the Parse triggers a retry on a fresh socket and re-runs the
Parse there before the conn is registered. Both the empty-settings and
non-empty-settings cases benefit. See `connection_pooling.md` for the
underlying primitive.

### Scope

This rewrite handles **SQL-level** wrapped EXECUTE reachable via the
PostgreSQL grammar:

- `EXPLAIN [options] EXECUTE p [(params)]`
- `CREATE [TEMP] TABLE t AS EXECUTE p [(params)]`
- `EXPLAIN [options] CREATE [TEMP] TABLE t AS EXECUTE p [(params)]`

It does not reach EXECUTE issued through PL/pgSQL dynamic SQL (e.g.
`EXECUTE format('explain execute %s', ...)` inside a server-side function
like `explain_filter` or `explain_parallel_append`). Those run entirely on
the backend session, which only sees the outer `SELECT` that invokes the
function, so there is no wrapped EXECUTE for the gateway to parse. Prepared
statement aliases cover that case instead.

## Prepared Statement Aliases

Rewriting a name only works when the gateway can see the name. For
server-side dynamic SQL it cannot, so the backend must already carry the
client's prepared statement under the name the client used. Aliases
reconcile that namespace onto whichever backend serves the session's next
query.

### What Becomes an Alias

Only names introduced by **SQL-level `PREPARE`** become aliases. The
planner registers one via `MarkSQLPreparedStatementAlias` after the
statement has been parsed and described successfully, and
`LogicalPreparedStatements(connID)` returns the resulting set.

Names created by protocol-level Parse stay gateway-local. They are not part
of the SQL namespace the client wrote, so materializing them on a backend
would expose statements the session never named in SQL.

### Reconciliation

`attachPreparedStatementAliases` puts the session's complete alias set on
`ExecuteOptions.prepared_statement_aliases` for every routed query and
portal execute. On the multipooler, `reconcilePreparedAliases` makes the
selected backend match that set before the query runs: it closes entries
whose body or parameter types have drifted, parses the ones that are
missing, and closes any pooler-internal (`ppstmt*`) statement occupying a
name an alias needs. Physical names never cross the gateway boundary, so a
collision resolves in favour of the client-visible name — see
`materializeExecuteSQLPreparedStatement`.

Because reconciliation targets whichever backend was selected, a session
that moves between pooled backends re-parses its alias namespace on each
new one. The cost scales with the number of live `PREPARE`s, paid once per
backend per session.

### Dormant Aliases

A `PREPARE` can outlive the objects its body references. When such an alias
fails to parse on a newly selected backend, reconciliation logs it and
continues rather than failing the unrelated query that happened to trigger
the switch. If dynamic SQL later executes that name, PostgreSQL reports its
own missing-statement error, which is the diagnostic the client would
expect anyway.
