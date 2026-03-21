# Graceful Drain via Connection Pool Manager

## Overview

When multipooler transitions to `NOT_SERVING` (e.g., during demotion
from primary to replica), it must reject new queries immediately while
allowing existing reserved connections — transactions, portals, COPY
operations — to finish cleanly. Without graceful drain, a sudden
cutover would abort in-flight transactions, causing client errors and
client-visible errors.

Key capabilities:

- **Two-phase shutdown**: gate new requests first, then wait for
  in-flight connections to drain before completing the state transition
- **Reserved connection exemption**: existing transactions and portals
  continue through shutdown; only new reservations are rejected
- **Bounded grace period**: drain waits up to a configurable timeout
  (default 3s) so shutdown is never blocked indefinitely
- **Pool-level drain tracking**: connection borrow/recycle and
  reserve/release events are tracked via callbacks, enabling efficient
  zero-polling drain detection

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
              ┌─────────────────────┐
              │  QueryPoolerServer  │
              │                     │
              │  1. shuttingDown=true│
              │     (gate new reqs) │
              │                     │
              │  2. WaitForDrain()  │◄──── bounded by gracePeriod
              │                     │
              │  3. status=NOT_SERVING│
              │     shuttingDown=false│
              └─────────────────────┘

    ┌──────────────────────────────────────────┐
    │           gRPC Service Layer             │
    │                                          │
    │  StartRequest(allowOnShutdown)           │
    │  ┌──────────┐  ┌───────────────────────┐ │
    │  │ New req  │  │ Existing reserved conn│ │
    │  │ → REJECT │  │ → ALLOW              │ │
    │  └──────────┘  └───────────────────────┘ │
    └──────────────────────────────────────────┘

    ┌──────────────────────────────────────────┐
    │        Connection Pool Manager           │
    │                                          │
    │  lentCount: atomic counter               │
    │  zeroCh: closed when lentCount == 0      │
    │                                          │
    │  OnBorrow() → lentAdd(+1)                │
    │  OnRecycle() → lentAdd(-1)               │
    │  OnReserve() → lentAdd(+1)               │
    │  OnRelease() → lentAdd(-1)               │
    └──────────────────────────────────────────┘
```

## Design

### Drain Tracking

The `connpoolmanager.Manager` tracks how many connections are
currently lent out across all user pools via a `lentCount` counter and
a `zeroCh` channel:

- **`lentAdd(n)`**: adjusts the counter. When transitioning to zero,
  `zeroCh` is closed to unblock any `WaitForDrain` caller. When
  transitioning away from zero, a new open channel is allocated.
- **`WaitForDrain(ctx)`**: blocks on `zeroCh` until either the count
  reaches zero or the context is cancelled.

This channel-based approach avoids polling. The counter increments on
connection borrow (regular pool) or reserve (reserved pool), and
decrements on recycle or release/kill.

Regular pool connections and reserved pool connections use separate
callback pairs to avoid double-counting:

| Pool type | Increment callback | Decrement callback |
| --------- | ------------------ | ------------------ |
| Regular   | `OnBorrow`         | `OnRecycle`        |
| Reserved  | `OnReserve`        | `OnRelease`        |

The reserved pool's inner regular connpool does **not** get
`OnBorrow`/`OnRecycle` callbacks. This is intentional: the reserved
pool borrows a regular connection internally when creating a reserved
connection, but from the drain-tracking perspective this is a single
logical lend tracked by `OnReserve`/`OnRelease`.

### Admission Control

`QueryPoolerServer.StartRequest(allowOnShutdown bool)` is the
admission gate called by every gRPC handler before acquiring an
executor:

| State                      | `allowOnShutdown=false`    | `allowOnShutdown=true`   |
| -------------------------- | -------------------------- | ------------------------ |
| SERVING, not shutting down | Allow                      | Allow                    |
| SERVING, shutting down     | Reject (`ErrShuttingDown`) | Allow                    |
| NOT_SERVING                | Reject (`ErrNotServing`)   | Reject (`ErrNotServing`) |

Each gRPC method sets `allowOnShutdown` based on whether it operates
on an existing reserved connection:

| Method                      | `allowOnShutdown`    | Rationale                                |
| --------------------------- | -------------------- | ---------------------------------------- |
| `StreamExecute`             | `reservedConnId > 0` | Allow if continuing existing reservation |
| `ExecuteQuery`              | `reservedConnId > 0` | Same                                     |
| `Describe`                  | `reservedConnId > 0` | Same                                     |
| `PortalStreamExecute`       | `reservedConnId > 0` | Same                                     |
| `CopyBidiExecute`           | `reservedConnId > 0` | Same                                     |
| `ReserveStreamExecute`      | `false`              | Always a new reservation                 |
| `ConcludeTransaction`       | `true`               | Always on existing reserved conn         |
| `ReleaseReservedConnection` | `true`               | Always on existing reserved conn         |
| `GetAuthCredentials`        | `false`              | Admin operation, not query path          |
| `StreamPoolerHealth`        | No gate              | Health streaming is independent          |

### Two-Phase Shutdown

When `OnStateChange` receives `NOT_SERVING`:

1. **Phase 1 — Gate**: set `shuttingDown=true` (under mutex), then
   release the mutex. New `StartRequest(false)` calls immediately
   return `ErrShuttingDown`. Existing reserved connections continue.

2. **Phase 2 — Drain**: call `poolManager.WaitForDrain(ctx)` with a
   context bounded by `gracePeriod` (default 3s). This blocks until
   all lent connections are returned or the timeout elapses.

3. **Phase 3 — Complete**: re-acquire the mutex, set
   `servingStatus=NOT_SERVING` and `shuttingDown=false`.

If the grace period expires before drain completes, all remaining
reserved connections are forcibly killed via
`poolManager.CloseReservedConnections()`. This ensures no reserved
connections survive into a non-serving state where they could execute
queries against a demoted replica. The force-close kills the backend
PostgreSQL processes and returns the connections to the pool.

When `OnStateChange` receives `SERVING`, the transition is immediate:
set `servingStatus=SERVING` and `shuttingDown=false`.

## Callback Plumbing

Callbacks flow through the pool creation hierarchy:

```text
Manager.createUserPoolSlow()
  │
  │  creates closures: onBorrow = m.lentAdd(1), etc.
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
closures that all funnel into the single `Manager.lentAdd` counter.

## Configuration

The drain grace period is configurable via the
`--connpool-drain-grace-period` flag (default: 3s). This controls how
long `OnStateChange` waits for in-flight connections to drain before
force-closing reserved connections and completing the NOT_SERVING
transition.

## Testing

- **`connpoolmanager/drain_test.go`**: unit tests for `lentAdd`
  counter and channel behavior, `WaitForDrain` immediate return,
  blocking until zero, and context cancellation
- **`poolerserver/pooler_test.go`**: unit tests for `StartRequest`
  across all state combinations (serving, not serving, shutting down
  with and without `allowOnShutdown`), concurrency tests for
  `OnStateChange` drain behavior, grace period expiry, and
  SERVING/NOT_SERVING round-trips
