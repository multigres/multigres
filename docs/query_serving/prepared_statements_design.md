# Prepared Statements and Portals in MultiGres

## Overview

This document outlines the design for implementing prepared statements and
portals in MultiGres, leveraging PostgreSQL's Extended Query Protocol to
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

## Prepared Statement Consolidator

Both MultiGateway and MultiPooler can use the same consolidation strategy to
manage prepared statements efficiently.

### Data Structure

```go
type PreparedStatementConsolidator struct {
    // Map from statement body to canonical prepared statement
    Stmts map[string]*PreparedStatement

    // Map from connection ID and statement name to prepared statement reference
    Incoming map[int]map[string]*PreparedStatement

    // Map from prepared statement to its name on the underlying connection
    Outgoing map[*PreparedStatement]string

    // Reference count: number of connections using each prepared statement
    UsageCount map[*PreparedStatement]int
}
```

### Algorithm

When processing a PREPARE request like `PREPARE stmt1 AS body1`:

1. **Check for existing statement**: Look up `body1` in the `Stmts` map
2. **If exists**:
   - Increment the usage count for the statement
   - Store the mapping: `Incoming[connectionId]["stmt1"] = preparedStatement`
   - Return the existing prepared statement
3. **If not exists**:
   - Create a new prepared statement on the underlying connection with a
     unique name (e.g., `ppstmt1`)
   - Store it in `Stmts[body1]`
   - Store the outgoing name mapping:
     `Outgoing[preparedStatement] = "ppstmt1"`
   - Initialize usage count to 1
   - Store the incoming mapping:
     `Incoming[connectionId]["stmt1"] = preparedStatement`

### Name Translation

Since multiple clients may use different names for the same logical prepared
statement:

- **Client-facing names**: Stored in the `Incoming` map (e.g., `stmt1`,
  `myquery`)
- **Backend names**: Stored in the `Outgoing` map (e.g., `ppstmt1`, `ppstmt2`)

This allows clients to use their own naming conventions while sharing the
underlying prepared statement.
