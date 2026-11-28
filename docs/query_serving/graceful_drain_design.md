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
  (default 3s); on expiry the drain ends the work left (below), and the
  transition completes whatever the outcome, so shutdown is never blocked
  indefinitely
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

Logical-replication tunnels (`NewLogicalReplicationConn`) are the one
exception: they fire neither `OnReserve` nor `OnRelease`, so a streaming
walsender never holds up the drain, and `KillAllForDrain` skips them when the
grace period expires. A walsender is a read-only WAL producer; the drain
exists to stop client writes.

### Admission Control

`QueryPoolerServer.StartRequest(target, RequestKind)` is the admission
gate every query-path call passes before reaching the executor. The gate
is applied by the `admittedQueryService` wrapper
(`poolerserver/query_service.go`) returned from
`QueryPoolerServer.QueryService()`, so it holds whatever the transport:
the gRPC handlers and, in minigres, the gateway's direct in-process
calls. Each method classifies the request into one of three kinds
(`AdmissionKind`), and the gate decides per kind and drain stage. Rejection returns `MTF01`, which the
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

Each method reports `id = ReservedConnectionId > 0` (existing
reserved) and, when `id == 0`, whether the request will create a new
reservation — mirroring the executor's own reserve decision:

| Method                      | Reserves when (id == 0)                    |
| --------------------------- | ------------------------------------------ |
| `StreamExecute`             | `ReservationOptions` reasons != 0          |
| `ExecuteQuery`, `Describe`  | never (no reservation path) → single query |
| `PortalStreamExecute`       | `MaxRows > 0` or reasons != 0              |
| `CopyBidiExecute`           | always (COPY pins a connection)            |
| `StreamReplication`         | always (new reservation)                   |
| `ConcludeTransaction`       | always existing reserved                   |
| `DiscardTempTables`         | always existing reserved                   |
| `ReleaseReservedConnection` | always existing reserved                   |
| `GetAuthCredentials`        | treated as new reservation (buffered)      |
| `StreamPoolerHealth`        | no gate (health streaming is independent)  |

Each predicate matches what the executor actually does: a `MaxRows == 0`
portal (fetch-all) with no reservation reasons runs on a pooled
connection, so it is a single query (`PortalReserves` also counts
reasons because a deferred `BEGIN` can be folded into the first portal
as `ReasonTransaction` while `MaxRows == 0`);
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
under one `gracePeriod`-bounded deadline (default 3s). The `routingRole`
is **not** updated until the very end, so in-flight requests still see
the old role and pass `checkTargetLocked` throughout.

1. **Stage 1 — drain reserved**: set `drainPhase=drainReserved` (under
   mutex), then call `poolManager.WaitForReservedDrain(ctx)`. New
   reservations are rejected (`MTF01`); single queries are still served;
   existing transactions run to completion. Blocks until `reservedCount`
   hits zero or the deadline elapses.

2. **Stage 2 — drain regular**: set `drainPhase=drainRegular`, then call
   `poolManager.WaitForDrain(ctx)`. Now single queries are rejected too;
   the wait blocks until the remaining in-flight single queries finish
   (the combined total reaches zero, since reserved is already zero).

3. **Complete**: re-acquire the mutex, set `routingRole`,
   `servingStatus` to the not-serving target, and `drainPhase=drainNone`.

If the shared deadline expires in either stage, the drain ends the work
left (see [Ending a Drain at its Timeout](#ending-a-drain-at-its-timeout)).
The transition completes whatever the outcome.

A late operation (e.g. a `COMMIT`) that arrives after its reserved
connection was force-closed is admitted by the gate (`ExistingReserved`)
but finds the connection gone, and the executor returns `40001` so the
client retries the whole transaction. See Admission Control above, and
[Reserved Connection Lifecycle and Loss](./pooler_reserved_connection_lifecycle.md)
for what the gateway does with that error.

When `OnStateChange` receives `SERVING`, the transition is immediate:
set `routingRole`, `servingStatus=SERVING` and `drainPhase=drainNone`.

## Ending a Drain at its Timeout

Every drain of the pooler runs the same routine (`runDrain` in
`poolerserver/drain.go`): the not-serving drain above, used by demotion,
graceful shutdown, pause and the serving holds (migration IMPORT, suspected
divergence), and the fence drain of a fenced primary (see
[routing rules](../sharding/routing_rules.md#how-poolers-enforce-fences)).
They differ only in what bounds them, which phase they move, and which error
they refuse work with.

### What a timed-out drain must stop

When the timeout fires, the gate already refuses all new work. What is left
is work admitted **before** the drain:

- **Single queries still running.** Stage 1 admits single queries, so one
  admitted just before the timeout, or one still running when stage 2
  times out, runs to the end. An autocommit `UPDATE` commits.
- **Transactions still open,** on reserved connections. Their statements
  are always admitted, because they run on an existing reserved connection.
- **Requests still waiting for a connection.** A `BEGIN` admitted before
  the drain may still be queued for a connection. It is not a reserved
  connection yet, so killing reserved connections misses it, and it gets
  its connection as soon as one frees up. Its whole transaction then runs.

Closing a connection's socket is not enough either: postgres notices a
client has gone only when it next reads from or writes to the socket, so a
running statement finishes, and commits, after its socket is closed. The
backend has to be terminated.

A drain that reports drained while any of this can still run breaks what
its caller relies on: a fence's caller moves the scope's data once
`RefreshFences` succeeds, and the IMPORT hold's caller drops target tables.

### The steps

1. **Stage 1:** refuse new reservations, and wait for the reserved
   connections, and the admitted reservations still waiting for one, to be
   released (`WaitForReservedDrain`), bounded by the timeout.
2. **Stage 2:** refuse single queries too, and wait for everything lent and
   admitted to be returned (`WaitForDrain`), within the same timeout. If it
   finishes in time the drain is **graceful**: nothing was sealed or killed.
3. **At the timeout,** in either stage:
   1. Move to stage 2, so nothing new is admitted.
   2. **Seal** the user pools (`SealPools`, below). From now on no request
      gets a connection: the ones waiting are refused, and so is every later
      one, with the drain's retryable error.
   3. **Kill** what is still lent (`KillLent`, below): terminate the
      backend of every reserved and regular connection still lent, and
      confirm each one exited. An unmanaged pooler skips this step.
4. **Settle:** wait, with a fresh bound of 10 seconds, until nothing is
   lent and nothing admitted is left (`WaitForDrain`).
5. **Unseal** the pools, which refill on demand. The drain is
   **force_close**: drained, after ending the work left.

If a kill is not confirmed (step 3), or something is still lent or admitted
at the settle bound (step 4), the drain **fails**
(`ErrDrainIncomplete`, `UNAVAILABLE`) and leaves the pools sealed. See
[A drain that fails](#a-drain-that-fails).

### Why two waits for the drain

Steps 2 and 4 wait for the same thing, nothing lent and nothing admitted,
but for different reasons, and neither can go:

- **Step 2** is the graceful wait, inside the drain's timeout: it gives
  in-flight single queries until the deadline to finish on their own.
  Without it, every drain that reached stage 2 would kill them at once.
- **Step 4** runs after the kills, when step 2's deadline has passed, so it
  needs a bound of its own. It waits for the killed and refused work to come
  back: a borrower whose backend was killed returns its connection, which
  the sealed pool closes, and a request still between admission and the
  pool reaches the sealed pool, is refused, and its admission ends. A kill
  being confirmed does not mean that has happened yet, so without step 4 a
  drain could report drained while such a request was still on its way to
  the pool.

### Sealing

A sealed pool works like a closed pool that stays open
(`connpool.Pool.Seal`):

- `Get` fails with the seal's error. Clients already waiting for a
  connection are woken with it, and no new client is queued.
- Its idle connections are closed, and a connection returned to it is
  closed rather than kept.
- A `Get` that was already past its first check when the pool was sealed
  checks again once it holds a connection, and closes the connection
  instead of lending it. So a sealed pool never lends a connection,
  whatever point a `Get` had reached.
- A user pool created while the pools are sealed starts sealed.

The seal's error is the sealing drain's: `MTF01` for a not-serving drain,
which the gateway buffers for the new primary, and `MTF02` for a fence
drain, which it retries once the fence lifts. Either is safe to retry,
because a refused request ran nothing. The error reaches the gateway
unchanged: the pools and the executor wrap it with `%w`, and
`mterrors.ToGRPC` finds the diagnostic through the wrapping. When two drains
hold the seal, the pools keep the error of the one that sealed first; the
gate refuses new work at admission with the right code meanwhile, so the
seal's error only reaches work admitted before both drains.

The manager's own queries use the admin pool, which is never sealed or
killed.

### Killing what is still lent

`KillLent` terminates each backend with `pg_terminate_backend(pid,
timeout)`, which waits up to the timeout (5 seconds) for the backend to exit.
`pg_terminate_backend` returns false when the backend did not exit in time,
did not exist, or the caller lacks permission, so on false the backend
counts as gone only if `pg_stat_activity` no longer lists it. An error, or a
backend still listed, is an unconfirmed kill. Up to 16 kills run at once.

- **Reserved connections** are found in the reserved pool's active set.
  Logical-replication tunnels are kept (see above). A reserved connection is
  released, and leaves the drain's accounting, only once its backend is
  confirmed gone; one whose kill is not confirmed stays reserved and counted,
  so the drain fails and a retry kills it again.
- **Regular connections** are found through the pool's record of the
  connections it has dialed (`connpool.Pool.OpenConns`). The record is
  written when a connection is dialed and pruned of closed ones lazily, so
  lending and returning a connection cost nothing extra. A sealed pool
  keeps no connection idle, so its open connections are the lent ones.
- Killed transactions surface `40001`.

### Unmanaged poolers

An unmanaged pooler fronts a database Multigres does not own, so its drains
kill nothing (the manager passes `killOnDrainTimeout=false` to
`NewQueryPoolerServer`). Step 3 seals the pools and
skips the kills; step 4 waits for the work to end on its own. If anything is
still lent at the bound, the drain fails.

### A drain that fails

A failed drain keeps the pools sealed, because work admitted before it may
still run or still be on its way to the pool: unsealing would hand such a
request a connection. The pools stay sealed until the condition the drain
was for ends, or a later drain succeeds.

- **Not-serving drain:** the transition completes anyway (a pooler that is
  not serving refuses new work), and the failure is logged and counted
  (`outcome=failed`), not returned: demotion, resignation, shutdown, pause and
  the divergence hold all carry on, since none of them acts on it. The pools
  stay sealed until the pooler serves again.
- **The migration IMPORT hold** is the one caller that must know: it drops
  target tables next. After draining the pooler it calls `EnsureDrained`,
  which returns at once if the drain succeeded, and otherwise runs the drain
  again, refusing everything from the start. "Otherwise" includes a drain
  whose caller's context ended first: it holds no seal, but it did not drain
  the pooler either. If the drain still does not drain the pooler,
  `EnsureDrained` returns its error, and the migration coordinator retries
  before touching the tables.
- **Fence drain:** the gate stays draining, refusing all new work, and the
  next read that finds the fence drains again. See
  [routing rules](../sharding/routing_rules.md#how-poolers-enforce-fences).

### Several drains at once

A fence drain can run during a not-serving drain. Each drain holds the seal
through a bool of its own (`holdSeal` / `releaseSeal`), and a count of the
holds taken unseals the pools when the last one is released. A drain only
ever releases its own hold: a cancelled drain that finishes late cannot
unseal pools a newer drain sealed, and a failed fence drain's hold can be
handed to the next fence drain.

### Cancellation

A fence drain is cancelled by a read that finds no fence, or by the pooler
losing or regaining the primary role; its callers then read the fences
again. A not-serving drain is cancelled only if its caller's context ends,
and the transition still completes. A cancelled drain stops, past any kill
already under way, and releases its own hold, so the pools unseal only once
no kill of it can still be running.

### Bounds

| Bound                     | Value                                                                                         |
| ------------------------- | --------------------------------------------------------------------------------------------- |
| Not-serving drain timeout | `--connpool-drain-grace-period` (default 3s; zero waits without a bound and never ends early) |
| Fence drain timeout       | `RefreshFences.drain_timeout` (at most 10 minutes), else the grace period, else 30s           |
| Each kill                 | 5s for the backend to exit                                                                    |
| Settle (step 4)           | 10s                                                                                           |

### Known limitation

A Describe outside a transaction borrows a regular connection from the user
pools, so while they are sealed it fails, and the gateway does not retry
Describe. The window runs from the drain's timeout until it settles. A
Describe inside a transaction uses the transaction's reserved connection and
is unaffected, unless that connection was killed.

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
deadline shared by both drain stages: when it expires, the drain ends the
work left (above) and the NOT_SERVING transition completes.

## Metrics

| Metric                         | Meaning                                                                                                    |
| ------------------------------ | ---------------------------------------------------------------------------------------------------------- |
| `mg.pooler.drain.duration`     | Wall-clock duration of the drain on a not-serving transition                                               |
| `mg.pooler.drain.outcome`      | Drain events by outcome: `graceful`, `force_close` (ended the work left), `failed`, or `cancelled` (fence) |
| `mg.pooler.drain.force_closed` | Lent connections terminated because a drain ran out of time                                                |

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
- **`poolerserver/admission_test.go`**: the `AdmissionKind` classifier
  (existing reserved / new reservation / single query)
- **`poolergateway/pooler_gateway_test.go`**: the gateway `isSingleQuery`
  classifier that decides which requests skip proactive failover buffering
- **`pools/connpool/seal_test.go`**: sealing refuses `Get`, wakes and
  refuses waiters, closes idle and returned connections, and the dialed
  record lists the lent ones; unsealing lends again
- **`pools/regular/drain_test.go`**: parallel kills, and no kill starting
  once the drain is cancelled
- **`poolerserver/drain_test.go`**: a timed-out drain that ends its work,
  an unconfirmed kill, no-kill mode, the drain's error on the seal, the
  fence drain failing, retrying, being lifted and being cancelled while it
  ends the work, several drains holding the seal, and `EnsureDrained`
- **`endtoend/multipooler/fence_gate_test.go`**: a `pg_sleep` single query
  still running at the fence drain's timeout is killed, and its backend is
  gone when `RefreshFences` succeeds
- End-to-end failover survival (continuous traffic across a planned
  failover with zero client-visible errors) is covered by the existing
  `queryserving` buffer tests (e.g. `TestBufferPlannedFailover`).
