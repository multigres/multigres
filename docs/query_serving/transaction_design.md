# Transaction Handling

## Overview

Multigres supports both explicit (`BEGIN`/`COMMIT`) and implicit (multi-statement
batch) transactions while maintaining compatibility with PostgreSQL semantics. The
implementation spans multigateway (handler, planner, scatterconn) and multipooler
(executor, reserved connections), connected via gRPC RPCs.

Key capabilities:

- Implicit-to-explicit transaction transformation for multi-statement batches
- Deferred `BEGIN` execution (no backend call until the first real query)
- Transaction state tracking via the PostgreSQL wire protocol's `ReadyForQuery`
  status byte
- Reserved connection management with multi-reason bitmask tracking
- Aborted transaction handling matching PostgreSQL behavior

## PostgreSQL Transaction Semantics

### Explicit Transactions

```sql
BEGIN;
INSERT INTO users VALUES (1, 'Alice');
INSERT INTO users VALUES (2, 'Bob');
COMMIT;
```

### Implicit Transactions (Multi-Statement Batches)

When multiple statements are sent in a single simple-query message, PostgreSQL
wraps them in an implicit transaction. If any statement fails, all preceding
statements in the batch are rolled back:

```sql
-- Sent as a single query string
INSERT INTO users VALUES (1, 'Alice');
INSERT INTO users VALUES (2, 'Bob');
INSERT INTO users VALUES (3, 'Charlie');
```

### Adoption Behavior

When `BEGIN` appears mid-batch, it adopts the preceding statements into the
explicit transaction rather than committing them:

```sql
INSERT INTO users VALUES (1, 'Alice');  -- Part of implicit transaction
BEGIN;                                   -- Adopts Alice into explicit tx
INSERT INTO users VALUES (2, 'Bob');
COMMIT;                                  -- Alice AND Bob committed together
```

## Architecture

```
Client
  │
  ▼
MultiGateway Handler (handler.go, transaction_helpers.go)
  │  - Parses SQL, detects multi-statement batches
  │  - Injects synthetic BEGIN/COMMIT for implicit transactions
  │  - Tracks transaction state via conn.TxnStatus()
  │  - Handles aborted transaction rejection
  │
  ▼
Planner (planner.go, transaction_stmt.go)
  │  - Routes BEGIN/COMMIT/ROLLBACK/SAVEPOINT to TransactionPrimitive
  │  - Routes regular queries to normal execution path
  │
  ▼
TransactionPrimitive (transaction_primitive.go)
  │  - BEGIN: deferred (sets state, no backend call)
  │  - COMMIT/ROLLBACK: delegates to ScatterConn.ConcludeTransaction
  │
  ▼
ScatterConn (scatter_conn.go)
  │  - 3-case routing: reserved conn / reserve+execute / pooled conn
  │  - ConcludeTransaction: sends COMMIT/ROLLBACK per shard
  │
  ▼
Multipooler Executor (executor.go)
  │  - ReserveStreamExecute: creates reserved conn, optionally BEGIN, executes query
  │  - ConcludeTransaction: COMMIT/ROLLBACK, manages reason bitmask
  │  - ReleaseReservedConnection: forceful cleanup on disconnect
  │
  ▼
PostgreSQL
```

## Transaction State

Transaction state is tracked on the `server.Conn` using PostgreSQL's protocol-level
`TxnStatus` byte. This is automatically included in every `ReadyForQuery` message
sent to the client, so the client always knows the current transaction state.

| State | Byte | Meaning |
|-------|------|---------|
| `TxnStatusIdle` | `'I'` | No active transaction |
| `TxnStatusInBlock` | `'T'` | In a transaction block |
| `TxnStatusFailed` | `'E'` | In a failed transaction (must ROLLBACK) |

State transitions:

```
        TxnStatusIdle ('I')
              │
     [BEGIN / first query in batch]
              │
              ▼
      TxnStatusInBlock ('T') ◄──┐
              │                  │
         ┌────┴────┐            │
    [COMMIT]   [error]          │
         │         │            │
         ▼         ▼            │
   TxnStatusIdle  TxnStatusFailed ('E')
                       │
                  [ROLLBACK]
                       │
                       ▼
                 TxnStatusIdle
```

## Implicit Transaction Transformation

When multigateway receives a multi-statement batch (`len(asts) > 1`), it uses
`executeWithImplicitTransaction()` to wrap implicit segments in synthetic
`BEGIN`/`COMMIT` boundaries.

### Transformation Rules

1. **Prepend synthetic `BEGIN`** at the start of the batch (unless already in a
   transaction)
2. **Skip user's `BEGIN`** if encountered (redundant), but mark the segment as
   explicit. A synthetic `BEGIN` result is sent to the client so response count
   matches the number of statements
3. **After user's `COMMIT`/`ROLLBACK`**, set `needsBegin = true` so the next
   statement gets a new synthetic `BEGIN`
4. **Append synthetic `COMMIT`** at end only if still in an implicit segment

Synthetic `BEGIN`/`COMMIT`/`ROLLBACK` are executed via `silentExecute()` which
suppresses the result callback, so the client only sees results for statements
they actually sent.

### Example: Mixed Batch

```sql
-- Input:
INSERT INTO users VALUES (1, 'Alice');
BEGIN;
INSERT INTO users VALUES (2, 'Bob');
COMMIT;
INSERT INTO users VALUES (3, 'Charlie');

-- Execution:
BEGIN;                                   -- Synthetic (silent)
INSERT INTO users VALUES (1, 'Alice');   -- Executed, result sent
-- BEGIN;                                -- Skipped, synthetic "BEGIN" result sent
INSERT INTO users VALUES (2, 'Bob');     -- Executed, result sent
COMMIT;                                  -- User's COMMIT, result sent
BEGIN;                                   -- Synthetic (silent)
INSERT INTO users VALUES (3, 'Charlie'); -- Executed, result sent
COMMIT;                                  -- Synthetic (silent)
```

### Error Handling in Batches

| Segment type | On failure | Behavior |
|--------------|------------|----------|
| Implicit (no user BEGIN) | Auto-ROLLBACK (silent) | Client sees error, previous statements rolled back |
| Explicit (user sent BEGIN) | Set `TxnStatusFailed` | Client must ROLLBACK to recover |
| Already in transaction at batch start | Set `TxnStatusFailed` | Same as explicit |
| After user COMMIT, new implicit segment | Auto-ROLLBACK (silent) | Previously committed data preserved |

## Deferred BEGIN Execution

When `BEGIN` is received (explicit or synthetic), the TransactionPrimitive sets
`conn.TxnStatus = TxnStatusInBlock` and returns a synthetic `BEGIN` result to
the client without contacting any backend. The actual `BEGIN` is sent atomically
with the first real query via `ReserveStreamExecute`.

This means:

- Empty transactions (`BEGIN; COMMIT;`) never reserve a backend connection
- `BEGIN` + first query execute as a single round-trip to the multipooler
- Connections are held for the minimum necessary duration

## ScatterConn: 3-Case Reservation Logic

When `ScatterConn.StreamExecute()` is called for a query, it uses three cases:

**Case 1: Already have a reserved connection for this shard**
```
state.GetMatchingShardState(target) has ReservedConnectionId != 0
→ Execute on existing reserved connection via qs.StreamExecute()
```

**Case 2: In transaction but no reserved connection yet (deferred BEGIN)**
```
conn.IsInTransaction() == true
→ Call gateway.ReserveStreamExecute() with transaction reason
→ Multipooler creates reserved conn, executes BEGIN + query atomically
→ Store reserved connection info in state
```

**Case 3: Not in transaction**
```
→ Use regular pooled connection via gateway.StreamExecute()
```

## Reserved Connection Management

### Reservation Reasons (Bitmask)

Connections can be reserved for multiple concurrent reasons. The `ReservationReasons`
field is a `uint32` bitmask:

| Reason | Value | Meaning |
|--------|-------|---------|
| `ReasonTransaction` | `1` | Active transaction (`BEGIN` executed) |
| `ReasonTempTable` | `2` | Temporary table exists on connection |
| `ReasonPortal` | `4` | Suspended portal/cursor |
| `ReasonCopy` | `8` | Active COPY operation |

When a reason is removed (e.g., transaction committed), the connection is only
released back to the pool if **all** reasons are cleared. This allows, for example,
a connection with both a transaction and a portal to keep the portal alive after
`COMMIT`.

### Shard State Tracking

`MultiGatewayConnectionState` maintains a `ShardStates []*ShardState` array
tracking reserved connections per shard:

```go
type ShardState struct {
    Target               *query.Target
    PoolerID             *clustermetadata.ID
    ReservedConnectionId int64
    ReservationReasons   uint32  // Bitmask of Reason* constants
}
```

When `StoreReservedConnection` is called for a target that already has a
ShardState, the reasons are OR'd together (`ss.ReservationReasons |= reasons`).
When a reason is removed and the bitmask reaches zero, the ShardState entry
is removed.

## RPCs

### ReserveStreamExecute

Creates a reserved connection on the multipooler, optionally executes `BEGIN`,
and executes the query — all atomically.

```
MultiGateway → Multipooler:
  ReserveStreamExecuteRequest {
    query, target, options, reservation_options { reasons }
  }

Multipooler → MultiGateway:
  ReserveStreamExecuteResponse {
    result, reserved_connection_id, pooler_id
  }
```

If `reasons` includes `ReasonTransaction`, the multipooler executes `BEGIN` before
the query. If the query fails after `BEGIN`, the multipooler automatically rolls
back and releases the connection.

### ConcludeTransaction

Commits or rolls back a transaction on a reserved connection. The connection may
remain reserved if other reasons exist.

```
MultiGateway → Multipooler:
  ConcludeTransactionRequest {
    target, options { reserved_connection_id }, conclusion (COMMIT|ROLLBACK)
  }

Multipooler → MultiGateway:
  ConcludeTransactionResponse {
    result, remaining_reasons
  }
```

`remaining_reasons = 0` means the connection was fully released. Non-zero means
the connection is still reserved for other reasons (e.g., temp tables).

### ReleaseReservedConnection

Forceful cleanup used during client disconnect. Performs a 4-step cleanup:

1. If transaction active: `ROLLBACK`
2. If COPY active: send `CopyFail` and read response
3. Release all portals
4. Release connection (back to pool if clean, close if errors occurred)

## Aborted Transaction Handling

When a query fails while in `TxnStatusInBlock`, the connection transitions to
`TxnStatusFailed`. In this state:

- **HandleQuery**: Rejects all queries unless the first statement is `ROLLBACK`.
  This allows both single `ROLLBACK` and batches like `ROLLBACK; SELECT 1;`
  (matching PostgreSQL behavior)
- **HandleExecute** (extended query protocol): Rejects all Execute messages.
  In the extended protocol, `ROLLBACK` is typically sent via simple query

State transitions to `TxnStatusFailed` happen in three places:

1. `HandleQuery`: single-statement error while `TxnStatusInBlock`
2. `HandleExecute`: portal execution error while `TxnStatusInBlock`
3. `executeWithImplicitTransaction`: error in an explicit segment

Timeouts (context cancellation) flow through the same error paths — a timed-out
query in a transaction triggers the same aborted state transition.

## Connection Cleanup

When a client disconnects, `ConnectionClosed` is called (before the connection
context is cancelled):

1. If reserved connections exist, calls `executor.ReleaseAll()` which invokes
   `ReleaseReservedConnection` on each multipooler — rolling back transactions,
   aborting COPYs, releasing portals
2. Cleans up prepared statement state via `psc.RemoveConnection()`

## Files

### Multigateway

| File | Role |
|------|------|
| `go/services/multigateway/handler/handler.go` | HandleQuery, HandleExecute, ConnectionClosed |
| `go/services/multigateway/handler/transaction_helpers.go` | `executeWithImplicitTransaction` |
| `go/services/multigateway/handler/connection_state.go` | ShardState, reservation tracking |
| `go/services/multigateway/engine/transaction_primitive.go` | Deferred BEGIN, COMMIT/ROLLBACK dispatch |
| `go/services/multigateway/planner/transaction_stmt.go` | Routes transaction statements to primitive |
| `go/services/multigateway/scatterconn/scatter_conn.go` | 3-case reservation logic, ConcludeTransaction |
| `go/services/multigateway/poolergateway/grpc_query_service.go` | gRPC client for RPCs |
| `go/services/multigateway/poolergateway/pooler_gateway.go` | Gateway interface |

### Multipooler

| File | Role |
|------|------|
| `go/multipooler/executor/executor.go` | ReserveStreamExecute, ConcludeTransaction, ReleaseReservedConnection |
| `go/multipooler/grpcpoolerservice/service.go` | gRPC server handlers |

### Shared

| File | Role |
|------|------|
| `proto/multipoolerservice.proto` | RPC definitions, ReservationReason, TransactionConclusion |
| `go/common/queryservice/queryservice.go` | QueryService interface (ReservedState type) |
| `go/common/protoutil/reservation.go` | Bitmask helpers (HasTransactionReason, RequiresBegin, etc.) |

### Tests

| File | Coverage |
|------|----------|
| `go/services/multigateway/handler/handler_test.go` | Aborted state, connection cleanup, batch handling |
| `go/services/multigateway/handler/transaction_helpers_test.go` | Implicit/explicit transformation, error handling |
| `go/services/multigateway/engine/transaction_primitive_test.go` | Deferred BEGIN, COMMIT/ROLLBACK behavior |
| `go/services/multigateway/scatterconn/scatter_conn_test.go` | 3-case reservation logic |
| `go/services/multigateway/poolergateway/grpc_query_service_test.go` | gRPC client tests |
| `go/test/endtoend/queryserving/transaction_test.go` | End-to-end integration tests |

## Known Limitations

- **SQLSTATE 25P02**: Aborted transaction errors currently return a plain error
  message without the proper PostgreSQL SQLSTATE code. This will be addressed
  when `PgError` propagation is implemented.
- **Single-shard only**: `ConcludeTransaction` iterates all shards but there is
  no 2PC coordinator yet. Multi-shard transactions will need a prepare/commit
  protocol.
