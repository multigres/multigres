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
- Deferred `BEGIN` execution (replayed when an operation first needs a
  backend, including an in-transaction Parse or SQL PREPARE)
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
Multigateway Handler (handler.go, transaction_helpers.go)
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
  │  - StreamExecute + ReservationOptions: creates reserved conn, optionally BEGIN
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

## Prepared Statements Inside a Transaction

When `HandleParse` receives protocol Parse or SQL PREPARE while the connection
is `TxnStatusInBlock`, `PrepareInTransaction` sends a prepare-only request
through `StreamExecute`. The multipooler reserves or reuses the backend,
replays deferred BEGIN if needed, and prepares a fresh named `ppstmt` with
`force_reparse=true`. Preparation runs inside the transaction, so relation
locks last until transaction end and semantic errors surface before Parse or
PREPARE is acknowledged.

A successful preparation is recorded in the backend connection's statement
cache. Describe and Execute reuse it when they address the same SQL and
parameter types; there is no separate unnamed validation Parse. Failed backend
preparation follows the existing transaction-error path, and the handler does
not register the statement in its consolidator.

A semantic execution-time rewrite can require a separate prepared statement.
The gateway's `reparsePending` signal applies only to that variant; Describe of
the original leaves it intact. Commit and rollback clear unused signals. This
cleanup does not deallocate the backend's session-level prepared statements.

Autocommit Parse remains lazy. Reactive stale-statement recovery cannot retry
inside an already-failed transaction, so it cannot replace receipt-time
preparation. See [prepared statements](prepared_statements_design.md) for
query identity, DDL recovery, and the distinction between original and
rewritten SQL.

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
real query: the gateway calls `StreamExecute` with `ReservationOptions`
carrying `ReasonTransaction` and the `begin_query` to replay.

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

### Case 2: Needs a Reservation but No Reserved Connection

```text
conn.IsInTransaction() == true
  (or the statement needs a pin of its own: temp table, session advisory
   lock, WITH HOLD cursor, setseed, logical replication slot, or an unsafe
   connection)
→ Call gateway.StreamExecute() with ReservationOptions{reasons, begin_query}
→ Multipooler creates reserved conn, executes BEGIN (if ReasonTransaction) + query
→ Store the returned ReservedState in state
```

### Case 3: No Reservation Needed

```text
→ Use regular pooled connection via gateway.StreamExecute()
  (nil ReservationOptions)
```

## Reserved Connection Management

### Reservation Reasons (Bitmask)

Connections can be reserved for multiple concurrent reasons. The
`ReservationReasons` field is a `uint32` bitmask:

| Reason                      | Value | Meaning                                                       |
| --------------------------- | ----- | ------------------------------------------------------------- |
| `ReasonTransaction`         | `1`   | Active transaction (`BEGIN` executed)                         |
| `ReasonTempTable`           | `2`   | Temporary table exists on connection                          |
| `ReasonPortal`              | `4`   | Suspended portal/cursor                                       |
| `ReasonCopy`                | `8`   | Active COPY operation                                         |
| `ReasonListen`              | `16`  | Active LISTEN subscription                                    |
| `ReasonLogicalReplication`  | `32`  | Logical-replication session (owned slot or active stream)     |
| `ReasonSessionAdvisoryLock` | `64`  | One or more session-level advisory locks held                 |
| `ReasonSetSeed`             | `128` | `setseed()` called; backend's PRNG is seeded for this session |
| `ReasonUnsafeConnection`    | `256` | Session opted into `multigres.unsafe_connection`              |

`ReasonSetSeed` and `ReasonUnsafeConnection` are **sticky**
(`protoutil.StickyReasons`): no PostgreSQL command undoes them, so they hold
the backend until the session's real teardown.

When a reason is removed (e.g., transaction committed), the
connection is only released back to the pool if **all** reasons are
cleared. This allows, for example, a connection with both a
transaction and a portal to keep the portal alive after `COMMIT`.

### Shard State Tracking

`MultigatewayConnectionState` maintains a
`ShardStates []*ShardState` array tracking reserved connections per
shard:

```go
type ShardState struct {
    // Target stores the information about the shard
    Target *query.Target

    // ReservedState holds the authoritative reservation state from
    // the multipooler, including the pooler ID, reserved connection
    // ID, and reservation reasons bitmask.
    ReservedState *query.ReservedState
}
```

The bundled `ReservedState` message (from the multipooler RPC
responses) carries:

- `PoolerId`: the multipooler instance that owns the connection
- `ReservedConnectionId`: the connection ID on that multipooler
- `ReservationReasons`: `uint32` bitmask of `Reason*` constants

`SetReservedConnection` stores the authoritative reservation state
from the multipooler, setting the reasons exactly as returned (not
OR'd). `ClearReservedConnection` removes the `ShardState` entry
when the reserved connection is released.

## RPCs

### StreamExecute with ReservationOptions

There is no separate reserve RPC. `StreamExecute` (and
`PortalStreamExecute`, `CopyReady`, `CopyOutReady`) take an optional
`query.ReservationOptions`. With non-zero `reasons` and no
`options.reserved_connection_id`, the multipooler creates a reserved
connection, optionally executes `BEGIN`, and executes the query — all
atomically. With an existing reserved connection, the reasons are added to it.

```text
Multigateway → Multipooler:
  StreamExecuteRequest {
    query, target, options,
    reservation_options { reasons, begin_query, pin_portal_names, ... }
  }

Multipooler → Multigateway:
  StreamExecuteResponse {
    result, reserved_state { reserved_connection_id, pooler_id, reservation_reasons }
  }
```

If `reasons` includes `ReasonTransaction`, the multipooler executes
`begin_query` before the query. If the query then fails with a PostgreSQL
error, the transaction stays open in failed state and the reservation is
**kept** (statement-local reasons such as temp table or portal pins are
unwound), so the client's eventual `ROLLBACK` reaches the same backend. Only a
dead connection is rolled back and released, and the response then carries no
reserved state.

### ConcludeTransaction

Commits or rolls back a transaction on a reserved connection. The
connection may remain reserved if other reasons exist.

```text
Multigateway → Multipooler:
  ConcludeTransactionRequest {
    target, options { reserved_connection_id },
    conclusion (COMMIT|ROLLBACK),
    release_portal_names, release_all_portals,
    chain,                      // COMMIT/ROLLBACK AND CHAIN
    rollback_session_settings   // pre-BEGIN settings snapshot
  }

Multipooler → Multigateway:
  ConcludeTransactionResponse {
    result, reserved_state
  }
```

`reserved_state.reserved_connection_id = 0` means the connection was fully
released. Non-zero means the connection is still reserved for the reasons in
`reserved_state.reservation_reasons` (e.g., temp tables).

### ReleaseReservedConnection

Forceful cleanup used during client disconnect (and by `DISCARD ALL`).
Cleanup steps:

1. If transaction active: `ROLLBACK`
2. If COPY active: send `CopyFail` and read response
3. If temp tables exist: `DISCARD TEMP`
4. If session advisory locks are held: `SELECT pg_advisory_unlock_all()`
5. Release all portals
6. Release connection (back to pool if clean, close if any step failed)

With `keep_sticky_reservations` (passed only by `DISCARD ALL`), a connection
that still holds a sticky reason after cleanup stays reserved and its state is
returned; client-disconnect cleanup always releases fully.

## Aborted Transaction Handling

When a query fails while in `TxnStatusInBlock`, the connection
transitions to `TxnStatusFailed`. In this state:

- **HandleQuery**: Rejects all queries unless the first statement
  is one of the recovery statements PostgreSQL accepts in this
  state: `ROLLBACK`, `ROLLBACK TO SAVEPOINT`, or `COMMIT`
  (treated as `ROLLBACK`). Multi-statement batches are permitted
  as long as the first statement is a recovery statement (e.g.,
  `ROLLBACK TO sp1; SELECT 1;`). A successful `ROLLBACK TO
SAVEPOINT` transitions the connection back to `TxnStatusInBlock`
  so subsequent statements can execute normally.
- **HandleExecute** (extended query protocol): Rejects Execute
  unless the portal's statement is one of the same recovery
  statements (`ast.IsAllowedInAbortedTransaction`). An empty-query
  portal is answered with `EmptyQueryResponse` regardless of
  transaction state, as in PostgreSQL

The error returned carries SQLSTATE `25P02`
(`in_failed_sql_transaction`), matching PostgreSQL behavior.

State transitions to `TxnStatusFailed` happen in three places:

1. `HandleQuery`: single-statement error while `TxnStatusInBlock`
2. `HandleExecute`: portal execution error while `TxnStatusInBlock`
3. `executeWithImplicitTransaction`: error in an explicit segment

Timeouts (context cancellation) flow through the same error paths —
a timed-out query in a transaction triggers the same aborted state
transition.

## LISTEN/NOTIFY in Transactions

`LISTEN` and `UNLISTEN` inside an explicit transaction do not take
effect immediately. The connection state records them in an ordered
`pendingActions` queue (see `pendingListenAction` in
`handler/connection_state.go`). At `COMMIT`, the handler drains the
queue via `CommitPendingListens` and applies the net subscription
changes against the gateway's `SubscriptionSync` (which talks to the
`NotificationManager` and, through it, the multipooler
`PubSubListener`). If the transaction rolls back, the pending
actions are discarded.

This matches PostgreSQL's behavior of making `LISTEN`/`UNLISTEN`
visible only on successful commit, and ensures the client does not
start receiving notifications on channels that its transaction
never actually committed.

## Connection Cleanup

When a client disconnects, `ConnectionClosed` is called (before the
connection context is cancelled):

1. If reserved connections exist, calls `executor.ReleaseAll()`
   which invokes `ReleaseReservedConnection` on each multipooler —
   rolling back transactions, aborting COPYs, discarding temp tables,
   releasing advisory locks and portals
2. Cleans up prepared statement state via `psc.RemoveConnection()`

## Files

### Multigateway

| File                                  | Role                                         |
| ------------------------------------- | -------------------------------------------- |
| `handler/handler.go`                  | Query/execute handling, connection lifecycle |
| `handler/transaction_helpers.go`      | Implicit-transaction wrapping for batches    |
| `handler/connection_state.go`         | ShardState, reservation tracking             |
| `engine/transaction_primitive.go`     | Deferred BEGIN, COMMIT/ROLLBACK              |
| `planner/transaction_stmt.go`         | Routes transaction statements                |
| `scatterconn/scatter_conn.go`         | 3-case reservation, transaction conclusion   |
| `poolergateway/grpc_query_service.go` | gRPC client for RPCs                         |
| `poolergateway/pooler_gateway.go`     | Gateway interface                            |

### Multipooler

| File                            | Role                                                  |
| ------------------------------- | ----------------------------------------------------- |
| `internal/executor/executor.go` | Reserved-connection execution, transaction conclusion |
| `grpcpoolerservice/service.go`  | gRPC server handlers                                  |

### Shared

| File                                  | Role                   |
| ------------------------------------- | ---------------------- |
| `proto/multipoolerservice.proto`      | RPC definitions        |
| `common/queryservice/queryservice.go` | QueryService interface |
| `common/protoutil/reservation.go`     | Bitmask helpers        |

### Tests

| File                                             | Coverage                         |
| ------------------------------------------------ | -------------------------------- |
| `handler/handler_test.go`                        | Aborted state, batch handling    |
| `handler/transaction_helpers_test.go`            | Implicit/explicit transformation |
| `engine/transaction_primitive_test.go`           | Deferred BEGIN, COMMIT/ROLLBACK  |
| `scatterconn/scatter_conn_test.go`               | 3-case reservation logic         |
| `poolergateway/grpc_query_service_test.go`       | gRPC client tests                |
| `test/endtoend/queryserving/transaction_test.go` | End-to-end integration tests     |

## Known Limitations

- **Single-shard only**: `ConcludeTransaction` iterates all shards
  but there is no 2PC coordinator yet. Multi-shard transactions will
  need a prepare/commit protocol.
