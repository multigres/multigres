# Graceful Drain via Connection Pool Manager

## Overview

When multipooler transitions to `NOT_SERVING` (e.g., during demotion
from primary to replica), it must let in-flight work finish cleanly
before the cutover, while steering new work to the new primary. A naive
"reject everything immediately" cutover would abort in-flight
transactions and needlessly bounce simple queries that the still-primary
could have served.

The drain runs in **two stages** under a single grace period:

1. **Drain reserved** — reject new transactions/reservations (the gateway
   buffers them for the new primary), but keep serving single autocommit
   queries and let existing transactions finish.
2. **Drain regular** — once transactions are done, also stop serving
   single queries and wait for the in-flight ones to finish, so the
   pooler reports `NOT_SERVING` with zero in-flight work and the
   subsequent postgres demotion kills nothing.

Key capabilities:

- **Two-stage drain**: drain transactions first (while single queries
  keep flowing), then drain single queries, then complete the transition
- **Reserved connection exemption**: existing transactions and portals
  always continue; only new reservations are rejected
- **Single queries served during stage 1**: simple autocommit queries are
  served on the still-primary until transactions have drained, then
  buffered — minimizing latency during what can be a multi-second wait
- **Bounded grace period**: both stages share a configurable timeout
  (default 3s); on expiry everything is force-closed so shutdown is never
  blocked indefinitely
- **Pool-level drain tracking**: borrow/recycle and reserve/release
  events feed two zero-polling counters — a combined count and a
  reserved-only count — so each stage waits on exactly its pool

## Background

The `QueryPoolerServer.OnStateChange` method is called by the
`StateManager` during serving state transitions. Previously it was a
no-op TODO that set the status field without any drain behavior. This
meant a `NOT_SERVING` transition would immediately flip the status,
but in-flight queries on reserved connections would continue running
with no coordination — and new queries would only be rejected if
individual gRPC handlers checked the serving status (which they did
not).

PostgreSQL clients using transactions or extended query protocol
expect that once a `BEGIN` succeeds, subsequent statements on that
connection will execute until `COMMIT`/`ROLLBACK`. While server
crashes can also abort transactions (and clients must handle that),
graceful drain minimizes unnecessary mid-transaction disruptions
during planned state transitions like demotion.

## Architecture

```text
                    StateManager
                        │
                        │ OnStateChange(NOT_SERVING)
                        ▼
              ┌──────────────────────────────┐
              │     QueryPoolerServer        │
              │                              │
              │  1. drainPhase=drainReserved │
              │     WaitForReservedDrain()   │◄── stage 1: serve single
              │                              │    queries, finish txns
              │  2. drainPhase=drainRegular  │
              │     WaitForDrain()           │◄── stage 2: drain single
              │                              │    queries (one deadline,
              │  3. status=NOT_SERVING       │    bounded by gracePeriod)
              │     drainPhase=drainNone     │
              └──────────────────────────────┘

    ┌──────────────────────────────────────────────────┐
    │                gRPC Service Layer                 │
    │                                                   │
    │  StartRequest(target, RequestKind)                │
    │  ┌───────────────┐ ┌────────────┐ ┌─────────────┐ │
    │  │ ExistingReser-│ │ SingleQuery│ │ NewReserva- │ │
    │  │ ved → ALLOW   │ │ stage1 ✓   │ │ tion → BUF  │ │
    │  └───────────────┘ └────────────┘ └─────────────┘ │
    └──────────────────────────────────────────────────┘

    ┌──────────────────────────────────────────┐
    │        Connection Pool Manager           │
    │                                          │
    │  regularCount  (single-query borrows)    │
    │  reservedCount (reserved conns)          │
    │  combined drain = regularCount+reserved  │
    │                                          │
    │  OnBorrow()  → regularAdd(+1)            │
    │  OnRecycle() → regularAdd(-1)            │
    │  OnReserve() → reservedAdd(+1)           │
    │  OnRelease() → reservedAdd(-1)           │
    └──────────────────────────────────────────┘
```

## Design

### Drain Tracking

The `connpoolmanager.Manager` keeps two independent counters and two
zero-polling channels:

- **`regularCount`**: regular (single-query) borrows.
- **`reservedCount`**: reserved connections (transactions, temp tables,
  portals, COPY, etc.).
- **`zeroCh`** closes when `regularCount + reservedCount == 0`;
  `WaitForDrain(ctx)` blocks on it (the full drain).
- **`reservedZeroCh`** closes when `reservedCount == 0`;
  `WaitForReservedDrain(ctx)` blocks on it (the reserved-only drain).

`regularAdd(n)` and `reservedAdd(n)` adjust one counter each and
reconcile the affected channel(s) via `signalZeroChanLocked`. The
two-stage drain waits on `WaitForReservedDrain` first (transactions only
— single-query borrows don't touch `reservedCount`, so they never hold
it up), then on `WaitForDrain`.

The counters are bumped by callbacks on borrow/recycle and
reserve/release:

| Pool type | Increment callback | Decrement callback | Counter bumped  |
| --------- | ------------------ | ------------------ | --------------- |
| Regular   | `OnBorrow`         | `OnRecycle`        | `regularCount`  |
| Reserved  | `OnReserve`        | `OnRelease`        | `reservedCount` |

Keeping the counters separate means each event takes the drain lock
exactly once: a regular borrow touches only `regularCount` (and `zeroCh`),
a reserve touches only `reservedCount` (and both channels). The reserved
pool's inner regular connpool does **not** get `OnBorrow`/`OnRecycle`
callbacks: creating a reserved connection is a single logical lend
tracked by `OnReserve`/`OnRelease`, so it is counted once, under
`reservedCount`.

### Admission Control

`QueryPoolerServer.StartRequest(target, RequestKind)` is the admission
gate called by every gRPC handler before acquiring an executor. The
handler classifies the request into one of three kinds, and the gate
decides per kind and drain stage. Rejection returns `MTF01`, which the
gateway buffers and retries on the new primary.

| `RequestKind`      | SERVING | drainReserved (stage 1) | drainRegular (stage 2) | NOT_SERVING      |
| ------------------ | ------- | ----------------------- | ---------------------- | ---------------- |
| `ExistingReserved` | Allow   | Allow                   | Allow                  | Allow¹           |
| `SingleQuery`      | Allow   | Allow                   | Reject (`MTF01`)       | Reject (`MTF01`) |
| `NewReservation`   | Allow   | Reject (`MTF01`)        | Reject (`MTF01`)       | Reject (`MTF01`) |

¹ `ExistingReserved` is always admitted regardless of serving status or
demotion: the reserved connection itself is the real gate, not the
pooler's serving state. Tablegroup/shard mismatches (`MTD01`) are still
rejected for every kind. Once an `ExistingReserved` op is admitted, two
outcomes are possible:

- **Connection still alive** (the normal in-drain case): the operation
  runs on the live backend. PostgreSQL is demoted only _after_ the
  drain finishes, so a surviving reserved connection is still on the
  primary and the transaction concludes cleanly.
- **Connection already force-closed** (the drain exceeded its grace
  period): the executor returns SQLSTATE `40001` (`serialization_failure`,
  _"transaction aborted: connection terminated during a planned
  failover"_) so the client retries the whole transaction. This is
  deliberately **not** `MTF01` — the transaction no longer exists, so
  there is nothing for the gateway to buffer or auto-retry; only the
  client can replay it.

Each gRPC handler reports `id = ReservedConnectionId > 0` (existing
reserved) and, when `id == 0`, whether the request will create a new
reservation — mirroring the executor's own reserve decision:

| Method                      | Reserves when (id == 0)                    |
| --------------------------- | ------------------------------------------ |
| `StreamExecute`             | `ReservationOptions` reasons != 0          |
| `ExecuteQuery`, `Describe`  | never (no reservation path) → single query |
| `PortalStreamExecute`       | `MaxRows > 0` (suspendable cursor)         |
| `CopyBidiExecute`           | always (COPY pins a connection)            |
| `ConcludeTransaction`       | always existing reserved                   |
| `DiscardTempTables`         | always existing reserved                   |
| `ReleaseReservedConnection` | always existing reserved                   |
| `GetAuthCredentials`        | treated as new reservation (buffered)      |
| `StreamPoolerHealth`        | no gate (health streaming is independent)  |

Each predicate matches what the executor actually does: a `MaxRows == 0`
portal (fetch-all) runs on a pooled connection, so it is a single query;
a COPY adds `ReasonCopy` pooler-side (the request's reservation reasons
can be 0 for an autocommit COPY), so it is always classified as a
reservation by `id` alone rather than by reasons.

#### Gateway cooperation

The pooler willingly serving single queries during stage 1 is only
useful if the gateway actually sends them. Today, once a shard enters
its failover buffer the gateway _proactively_ blocks every PRIMARY
request. So `PoolerGateway.withBuffering` takes an `isSingleQuery` flag
(set when there is no reservation and no existing reserved connection):
single queries **skip the proactive block** and are sent straight to the
pooler, buffering only _reactively_ if they bounce with `MTF01` (stage 2
onward). New transactions keep the proactive block — the pooler rejects
them throughout the drain, so a wasted round-trip is pointless.

### Two-Stage Drain

When `OnStateChange` receives `NOT_SERVING`, it runs two drain stages
under one `gracePeriod`-bounded deadline (default 3s). The `poolerType`
is **not** updated until the very end, so in-flight reserved-connection
ops still see the old type and pass `checkTargetLocked` throughout.

1. **Stage 1 — drain reserved**: set `drainPhase=drainReserved` (under
   mutex), then call `poolManager.WaitForReservedDrain(ctx)`. New
   reservations are rejected (`MTF01`); single queries are still served;
   existing transactions run to completion. Blocks until `reservedCount`
   hits zero or the deadline elapses.

2. **Stage 2 — drain regular**: set `drainPhase=drainRegular`, then call
   `poolManager.WaitForDrain(ctx)`. Now single queries are rejected too;
   the wait blocks until the remaining in-flight single queries finish
   (the combined total reaches zero, since reserved is already zero).

3. **Complete**: re-acquire the mutex, set `poolerType`,
   `servingStatus=NOT_SERVING`, and `drainPhase=drainNone`.

If the shared deadline expires in either stage, all reserved connections
are force-closed via `poolManager.CloseReservedConnections()` (killing
the backend processes; their transactions surface `40001`), and the
transition completes. Any still-running single queries are then killed
by the postgres demotion that follows — acceptable, since that is
exactly what the grace period bounds.

A late operation (e.g. a `COMMIT`) that arrives after its reserved
connection was force-closed is admitted by the gate (`ExistingReserved`)
but finds the connection gone, and the executor returns `40001` so the
client retries the whole transaction. See Admission Control above.

When `OnStateChange` receives `SERVING`, the transition is immediate:
set `servingStatus=SERVING` and `drainPhase=drainNone`.

## Callback Plumbing

Callbacks flow through the pool creation hierarchy:

```text
Manager.createUserPoolSlow()
  │
  │  creates closures: onBorrow = m.regularAdd(1), onReserve = m.reservedAdd(1), etc.
  │
  └─► UserPoolConfig { OnBorrow, OnRecycle, OnReserve, OnRelease }
        │
        ├─► connpool.Config { OnBorrow, OnRecycle }
        │     └─► Regular pool get()/put()
        │
        └─► reserved.PoolConfig { OnReserve, OnRelease }
              └─► Reserved pool NewConn()/release()/KillConnection()
```

Each user pool created by the manager gets its own set of callback
closures that funnel into the `Manager.regularAdd` / `Manager.reservedAdd`
counters.

## Configuration

The drain grace period is configurable via the
`--connpool-drain-grace-period` flag (default: 3s). It is the single
deadline shared by both drain stages: when it expires, remaining reserved
connections are force-closed and the NOT_SERVING transition completes.

## Testing

- **`connpoolmanager/drain_test.go`**: unit tests for the `regularAdd` and
  `reservedAdd` counters and their channels, `WaitForDrain` /
  `WaitForReservedDrain` immediate-return, blocking-until-zero, and
  context cancellation — including that a regular borrow does **not**
  hold up the reserved-only wait
- **`poolerserver/pooler_test.go`**: unit tests for `StartRequest` across
  all `RequestKind` × drain-phase combinations, plus `OnStateChange`
  two-stage drain behavior (single query served in stage 1, rejected in
  stage 2), grace-period expiry in both stages, and SERVING/NOT_SERVING
  round-trips
- **`grpcpoolerservice/admission_test.go`**: the `admissionKind` classifier
  (existing reserved / new reservation / single query)
- **`poolergateway/pooler_gateway_test.go`**: the gateway `isSingleQuery`
  classifier that decides which requests skip proactive failover buffering
- End-to-end failover survival (continuous traffic across a planned
  failover with zero client-visible errors) is covered by the existing
  `queryserving` buffer tests (e.g. `TestBufferPlannedFailover`).
