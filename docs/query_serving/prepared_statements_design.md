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

MultiGateway will own and manage prepared statements and portals at the
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
map at the MultiGateway handler level:

- **Shared statement pool**: A single prepared statement is reused across
  multiple connections
- **Reference counting**: Track how many connections are using each prepared
  statement
- **Lifecycle management**: Automatically clean up statements when the
  reference count reaches zero

## Optimization 2: Pooler-Level Statement Management

### Single-Shard Scenario

For the common case where a prepared statement targets a single shard, we can
push statement management down to the MultiPooler level.

### How It Works

When a MultiGateway executes a prepared statement that targets a single shard:

1. **Connection lookup**: The MultiPooler checks if any connection in the pool
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

Each connection in the MultiPooler's pool must track:

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
