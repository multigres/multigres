# Transaction Handling

## Overview

Multigres supports both explicit (`BEGIN`/`COMMIT`) and implicit
(multi-statement batch) transactions while maintaining compatibility
with PostgreSQL semantics. The implementation spans multigateway
(handler, planner, scatterconn) and multipooler (executor, reserved
connections), connected via gRPC RPCs.

Key capabilities:

- Implicit-to-explicit transaction transformation for multi-statement
  batches
- Deferred `BEGIN` execution (no backend call until the first real
  query)
- Transaction state tracking via the PostgreSQL wire protocol's
  `ReadyForQuery` status byte
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

When multiple statements are sent in a single simple-query message,
PostgreSQL wraps them in an implicit transaction. If any statement
fails, all preceding statements in the batch are rolled back:

```sql
-- Sent as a single query string
INSERT INTO users VALUES (1, 'Alice');
INSERT INTO users VALUES (2, 'Bob');
INSERT INTO users VALUES (3, 'Charlie');
```

### Adoption Behavior

When `BEGIN` appears mid-batch, it adopts the preceding statements
into the explicit transaction rather than committing them:

```sql
INSERT INTO users VALUES (1, 'Alice');  -- Part of implicit transaction
BEGIN;                                   -- Adopts Alice into explicit tx
INSERT INTO users VALUES (2, 'Bob');
COMMIT;                                  -- Alice AND Bob committed together
```

## Architecture

```text
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
  │  - 3-case routing: reserved / reserve+execute / pooled
  │  - ConcludeTransaction: sends COMMIT/ROLLBACK per shard
  │
  ▼
Multipooler Executor (executor.go)
  │  - ReserveStreamExecute: creates reserved conn, optionally BEGIN
  │  - ConcludeTransaction: COMMIT/ROLLBACK, manages reason bitmask
  │  - ReleaseReservedConnection: forceful cleanup on disconnect
  │
  ▼
PostgreSQL
```

## Transaction State

Transaction state is tracked on the `server.Conn` using PostgreSQL's
protocol-level `TxnStatus` byte. This is automatically included in
every `ReadyForQuery` message sent to the client, so the client
always knows the current transaction state.

| State              | Byte  | Meaning                                 |
| ------------------ | ----- | --------------------------------------- |
| `TxnStatusIdle`    | `'I'` | No active transaction                   |
| `TxnStatusInBlock` | `'T'` | In a transaction block                  |
| `TxnStatusFailed`  | `'E'` | In a failed transaction (must ROLLBACK) |

State transitions:

```text
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

When multigateway receives a multi-statement batch (`len(asts) > 1`),
it uses `executeWithImplicitTransaction()` to wrap implicit segments
in synthetic `BEGIN`/`COMMIT` boundaries.

### Transformation Rules

1. **Prepend synthetic `BEGIN`** at the start of the batch (unless
   already in a transaction)
2. **Skip user's `BEGIN`** if encountered (redundant), but mark the
   segment as explicit. A synthetic `BEGIN` result is sent to the
   client so response count matches the number of statements
3. **After user's `COMMIT`/`ROLLBACK`**, set `needsBegin = true` so
   the next statement gets a new synthetic `BEGIN`
4. **Append synthetic `COMMIT`** at end only if still in an implicit
   segment

Synthetic `BEGIN`/`COMMIT`/`ROLLBACK` are executed via
`silentExecute()` which suppresses the result callback, so the
client only sees results for statements they actually sent.

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
-- BEGIN;                                -- Skipped, synthetic result
INSERT INTO users VALUES (2, 'Bob');     -- Executed, result sent
COMMIT;                                  -- User's COMMIT, result sent
BEGIN;                                   -- Synthetic (silent)
INSERT INTO users VALUES (3, 'Charlie'); -- Executed, result sent
COMMIT;                                  -- Synthetic (silent)
```

### Error Handling in Batches

| Segment type               | On failure            | Behavior                       |
| -------------------------- | --------------------- | ------------------------------ |
| Implicit (no user BEGIN)   | Auto-ROLLBACK         | Client sees error, rolled back |
| Explicit (user sent BEGIN) | Set `TxnStatusFailed` | Client must ROLLBACK           |
| Already in transaction     | Set `TxnStatusFailed` | Same as explicit               |
| After COMMIT, new implicit | Auto-ROLLBACK         | Committed data preserved       |

### CommandComplete Deferral for Implicit Commit

When PostgreSQL executes a multi-statement simple query, it commits the
implicit transaction **before** sending the last statement's
`CommandComplete`. If the commit fails (e.g., deferred constraint
violation, serialization failure), the client never receives
`CommandComplete` for the last statement — it sees an `ErrorResponse`
instead. This is by design: it makes the commit error appear as if the
last statement failed, which is the only way to surface the error
without introducing an unexpected extra message into the protocol.

#### PostgreSQL wire protocol on implicit commit failure

```text
Client sends: "INSERT INTO t1 ...; INSERT INTO t2 ...; SELECT * FROM big_table;"

Commit succeeds:                    Commit fails:
→ CommandComplete (INSERT 0 1)      → CommandComplete (INSERT 0 1)
→ CommandComplete (INSERT 0 1)      → CommandComplete (INSERT 0 1)
→ RowDescription                    → RowDescription
→ DataRow ...                       → DataRow ...
→ CommandComplete (SELECT 3)        → ErrorResponse        ← replaces CommandComplete
→ ReadyForQuery 'I'                 → ReadyForQuery 'I'    ← nothing committed
```

Note that `DataRow` messages for the last statement are already sent
to the client during execution — PostgreSQL streams rows via
`printtup()` during `PortalRun()`, well before the commit attempt.
Only the `CommandComplete` is withheld. If the commit fails, the rows
traveled across the network for nothing, but the missing
`CommandComplete` (replaced by `ErrorResponse`) tells client libraries
to discard them.

#### How multigres replicates this behavior

We replicate PostgreSQL's behavior by intercepting the streaming
callback for the **last statement** in an implicit transaction. The
streaming callback is invoked multiple times per statement:

1. **Intermediate callbacks** carry `Fields` and batched `Rows` with
   `CommandTag=""` — these are forwarded to the client immediately
   (DataRow messages stream normally)
2. **Final callback** carries the last batch of `Rows` along with
   `CommandTag="SELECT 42"` — we forward the rows but **hold the
   CommandTag** in `heldCommandTag`

After the last statement finishes executing, we attempt the implicit
`COMMIT`:

- **Commit succeeds**: we flush `heldCommandTag` via the callback →
  the client sees `CommandComplete`
- **Commit fails**: we `ROLLBACK`, discard `heldCommandTag`, and
  return the error → the client sees `ErrorResponse` instead of
  `CommandComplete`

This matches PostgreSQL's wire protocol exactly. All statements before
the last one have their `CommandComplete` sent immediately — those are
misleading if the commit later fails (the changes didn't persist), but
that is PostgreSQL's behavior too.

#### Why only the CommandTag is deferred (not the rows)

Holding back rows in memory is not feasible — a `SELECT` returning
millions of rows would require unbounded buffering. PostgreSQL doesn't
buffer them either; it streams rows during execution and only withholds
the final `CommandComplete` message. Our approach mirrors this: rows
stream to the client as they arrive, so memory usage stays bounded
regardless of result set size.

## Deferred BEGIN Execution

When `BEGIN` is received (explicit or synthetic), the
TransactionPrimitive sets `conn.TxnStatus = TxnStatusInBlock` and
returns a synthetic `BEGIN` result to the client without contacting
any backend. The actual `BEGIN` is sent atomically with the first
real query via `ReserveStreamExecute`.

This means:

- Empty transactions (`BEGIN; COMMIT;`) never reserve a backend
  connection
- `BEGIN` + first query execute as a single round-trip to the
  multipooler
- Connections are held for the minimum necessary duration

## ScatterConn: 3-Case Reservation Logic

When `ScatterConn.StreamExecute()` is called for a query, it uses
three cases:

### Case 1: Already Have a Reserved Connection

```text
state.GetMatchingShardState(target) has ReservedConnectionId != 0
→ Execute on existing reserved connection via qs.StreamExecute()
```

### Case 2: In Transaction but No Reserved Connection

```text
conn.IsInTransaction() == true
→ Call gateway.ReserveStreamExecute() with transaction reason
→ Multipooler creates reserved conn, executes BEGIN + query
→ Store reserved connection info in state
```

### Case 3: Not in Transaction

```text
→ Use regular pooled connection via gateway.StreamExecute()
```

## Reserved Connection Management

### Reservation Reasons (Bitmask)

Connections can be reserved for multiple concurrent reasons. The
`ReservationReasons` field is a `uint32` bitmask:

| Reason              | Value | Meaning                               |
| ------------------- | ----- | ------------------------------------- |
| `ReasonTransaction` | `1`   | Active transaction (`BEGIN` executed) |
| `ReasonTempTable`   | `2`   | Temporary table exists on connection  |
| `ReasonPortal`      | `4`   | Suspended portal/cursor               |
| `ReasonCopy`        | `8`   | Active COPY operation                 |

When a reason is removed (e.g., transaction committed), the
connection is only released back to the pool if **all** reasons are
cleared. This allows, for example, a connection with both a
transaction and a portal to keep the portal alive after `COMMIT`.

### Shard State Tracking

`MultiGatewayConnectionState` maintains a
`ShardStates []*ShardState` array tracking reserved connections per
shard:

```go
type ShardState struct {
    Target               *query.Target
    PoolerID             *clustermetadata.ID
    ReservedConnectionId int64
    ReservationReasons   uint32  // Bitmask of Reason* constants
}
```

When `StoreReservedConnection` is called for a target that already
has a ShardState, the reasons are OR'd together
(`ss.ReservationReasons |= reasons`). When a reason is removed and
the bitmask reaches zero, the ShardState entry is removed.

## RPCs

### ReserveStreamExecute

Creates a reserved connection on the multipooler, optionally
executes `BEGIN`, and executes the query — all atomically.

```text
MultiGateway → Multipooler:
  ReserveStreamExecuteRequest {
    query, target, options, reservation_options { reasons }
  }

Multipooler → MultiGateway:
  ReserveStreamExecuteResponse {
    result, reserved_connection_id, pooler_id
  }
```

If `reasons` includes `ReasonTransaction`, the multipooler executes
`BEGIN` before the query. If the query fails after `BEGIN`, the
multipooler automatically rolls back and releases the connection.

### ConcludeTransaction

Commits or rolls back a transaction on a reserved connection. The
connection may remain reserved if other reasons exist.

```text
MultiGateway → Multipooler:
  ConcludeTransactionRequest {
    target, options { reserved_connection_id },
    conclusion (COMMIT|ROLLBACK)
  }

Multipooler → MultiGateway:
  ConcludeTransactionResponse {
    result, remaining_reasons
  }
```

`remaining_reasons = 0` means the connection was fully released.
Non-zero means the connection is still reserved for other reasons
(e.g., temp tables).

### ReleaseReservedConnection

Forceful cleanup used during client disconnect. Performs a 4-step
cleanup:

1. If transaction active: `ROLLBACK`
2. If COPY active: send `CopyFail` and read response
3. Release all portals
4. Release connection (back to pool if clean, close if errors)

## Aborted Transaction Handling

When a query fails while in `TxnStatusInBlock`, the connection
transitions to `TxnStatusFailed`. In this state:

- **HandleQuery**: Rejects all queries unless the first statement
  is `ROLLBACK`. This allows both single `ROLLBACK` and batches
  like `ROLLBACK; SELECT 1;` (matching PostgreSQL behavior)
- **HandleExecute** (extended query protocol): Rejects all Execute
  messages. In the extended protocol, `ROLLBACK` is typically sent
  via simple query

The error returned carries SQLSTATE `25P02`
(`in_failed_sql_transaction`), matching PostgreSQL behavior.

State transitions to `TxnStatusFailed` happen in three places:

1. `HandleQuery`: single-statement error while `TxnStatusInBlock`
2. `HandleExecute`: portal execution error while `TxnStatusInBlock`
3. `executeWithImplicitTransaction`: error in an explicit segment

Timeouts (context cancellation) flow through the same error paths —
a timed-out query in a transaction triggers the same aborted state
transition.

## Connection Cleanup

When a client disconnects, `ConnectionClosed` is called (before the
connection context is cancelled):

1. If reserved connections exist, calls `executor.ReleaseAll()`
   which invokes `ReleaseReservedConnection` on each multipooler —
   rolling back transactions, aborting COPYs, releasing portals
2. Cleans up prepared statement state via `psc.RemoveConnection()`

## Files

### Multigateway

| File                                  | Role                                         |
| ------------------------------------- | -------------------------------------------- |
| `handler/handler.go`                  | HandleQuery, HandleExecute, ConnectionClosed |
| `handler/transaction_helpers.go`      | `executeWithImplicitTransaction`             |
| `handler/connection_state.go`         | ShardState, reservation tracking             |
| `engine/transaction_primitive.go`     | Deferred BEGIN, COMMIT/ROLLBACK              |
| `planner/transaction_stmt.go`         | Routes transaction statements                |
| `scatterconn/scatter_conn.go`         | 3-case reservation, ConcludeTransaction      |
| `poolergateway/grpc_query_service.go` | gRPC client for RPCs                         |
| `poolergateway/pooler_gateway.go`     | Gateway interface                            |

### Multipooler

| File                           | Role                                      |
| ------------------------------ | ----------------------------------------- |
| `executor/executor.go`         | ReserveStreamExecute, ConcludeTransaction |
| `grpcpoolerservice/service.go` | gRPC server handlers                      |

### Shared

| File                                  | Role                   |
| ------------------------------------- | ---------------------- |
| `proto/multipoolerservice.proto`      | RPC definitions        |
| `common/queryservice/queryservice.go` | QueryService interface |
| `common/protoutil/reservation.go`     | Bitmask helpers        |

### Tests

| File                                       | Coverage                         |
| ------------------------------------------ | -------------------------------- |
| `handler/handler_test.go`                  | Aborted state, batch handling    |
| `handler/transaction_helpers_test.go`      | Implicit/explicit transformation |
| `engine/transaction_primitive_test.go`     | Deferred BEGIN, COMMIT/ROLLBACK  |
| `scatterconn/scatter_conn_test.go`         | 3-case reservation logic         |
| `poolergateway/grpc_query_service_test.go` | gRPC client tests                |
| `test/endtoend/.../transaction_test.go`    | End-to-end integration tests     |

## Known Limitations

- **Single-shard only**: `ConcludeTransaction` iterates all shards
  but there is no 2PC coordinator yet. Multi-shard transactions will
  need a prepare/commit protocol.
