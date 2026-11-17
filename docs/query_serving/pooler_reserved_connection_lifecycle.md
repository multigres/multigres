# Reserved Connection Lifecycle and Loss

## Overview

A reserved connection pins one PostgreSQL backend to one client session for as
long as some reservation reason holds (transaction, temp table, portal, COPY,
session advisory lock, `setseed`, unsafe connection, ...). Other docs cover how
a reservation is _created_ ([Transaction Handling](./transaction_design.md),
[Connection Pooling](./connection_pooling.md)). This doc covers the other half:

- how a reservation ends, and whether its backend is reused or closed;
- what happens when a reservation is lost underneath a session, and what the
  client sees.

Code: `go/services/multipooler/internal/pools/reserved/` (pool and release),
`go/services/multipooler/internal/executor/executor.go` (per-RPC decisions),
`go/services/multigateway/scatterconn/reservation_loss.go` (gateway reaction).

## Who decides what

The multipooler is the authority on whether a reservation still exists. Every
query-path RPC returns a `query.ReservedState`
(`reserved_connection_id`, `pooler_id`, `reservation_reasons`):

- a non-zero `reserved_connection_id` means the connection is still reserved,
  for exactly the returned reasons (the gateway stores them as returned, it
  does not OR them with what it had);
- zero means the reservation is gone, and the gateway clears its `ShardState`
  for that target (`ScatterConn.applyReservedState`).

The gateway never releases a reservation by forgetting it. It ends one only
through an RPC: `ConcludeTransaction`, `DiscardTempTables`,
`ReleaseReservedConnection`, or a statement whose completion removes the last
reason.

## Release: reuse or close

`reserved.Conn.Release(reason, gatewaySessionSettings)` removes the connection
from the pool's active map and then either recycles the backend or closes it
(`reserved.Pool.release`). The backend is **closed** (tainted) when any of
these hold:

- the release reason indicates uncertain backend state
  (`ReleaseReason.preventsReuse`): `ReleaseError`, `ReleaseKill`, and the
  defined-but-currently-unused `ReleaseTimeout`;
- the connection is marked close-on-release:
  - a temp-table statement succeeded on it (`MarkTempTainted`). `DISCARD TEMP`
    does not clear the mark;
  - it was reserved with `ReasonUnsafeConnection` (marked when the reason is
    added, even if the statement fails);
- a release cleanup hook fails (the backend-vpid row cleanup, when
  `--backend-vpid-tracking-enabled` is set).

Otherwise the release is **clean** (`ReleaseCommit`, `ReleaseRollback`,
`ReleasePortalComplete`, `ReleaseAdvisoryUnlock`, `ReleaseUnreserved`,
`ReleaseStatementError`): the gateway's session-settings map is stamped onto
the connection as its settings label, with no SQL, and the backend goes back
into the reserved pool's underlying pool in the matching settings bucket.

| Release reason          | Typical cause                                                        | Backend |
| ----------------------- | -------------------------------------------------------------------- | ------- |
| `ReleaseCommit`         | `COMMIT` removed the last reason                                     | reused  |
| `ReleaseRollback`       | `ROLLBACK`, or `ReleaseReservedConnection` cleanup succeeded         | reused  |
| `ReleasePortalComplete` | portal or COPY finished                                              | reused  |
| `ReleaseAdvisoryUnlock` | last session advisory lock released (`pg_locks` probe)               | reused  |
| `ReleaseUnreserved`     | took the reserved path but ended with no reason; reset before reuse  | reused  |
| `ReleaseStatementError` | statement failed with a clean PostgreSQL error and no reason remains | reused  |
| `ReleaseError`          | connection-level or indeterminate failure, failed cleanup            | closed  |
| `ReleaseKill`           | `KillConnection` (inactivity killer, drain force-close)              | closed  |

Releases are counted in `mg.pooler.txn.outcomes` /
`mg.pooler.txn.duration` for transactions; a connection released while still
in a transaction is recorded as an abort.

## Ways a reservation is lost

"Lost" means the pooler no longer holds the reservation although the gateway
did not ask to end it.

- **Inactivity timeout.** Each reserved connection has an expiry of
  `--connpool-user-reserved-inactivity-timeout` (default 30s). The expiry is
  reset when the reservation is created and each time an operation looks the
  connection up by ID (`reserved.Pool.Get`). A background `idleKiller` scans at
  1/10th of the timeout and kills expired connections with
  `pg_terminate_backend()` through the admin pool. A lookup of an expired
  connection that the killer has not reached yet also fails. Logical-replication
  connections have no inactivity timeout.
- **Graceful drain exceeded its grace period.** On a not-serving transition the
  pooler force-closes every reserved connection that outlives
  `--connpool-drain-grace-period` (see
  [Graceful Drain](./graceful_drain_design.md)).
- **Dead backend.** PostgreSQL terminated the backend, crashed, or the socket
  broke. The executor releases the reservation with `ReleaseError` when an
  operation on it fails with a connection-dead error (`reservedConnError`).
- **Pooler gone.** The pooler left the topology. The gateway's
  `QueryServiceByID` lookup of the owning pooler fails with `UNAVAILABLE`
  before any RPC is sent; a statement fails with that error, and
  `ConcludeTransaction`, `DiscardTempTables` and connection release treat it
  as a lost reservation.
- **Pool manager reopen.** After a PostgreSQL restart the pooler closes and
  reopens all pools, which drops every reserved connection.

## What the pooler returns

When a request names a `reserved_connection_id` the pooler no longer holds, the
executor returns `mterrors.NewReservedConnectionTerminated` and no reserved
state:

```text
ERROR:  reserved connection terminated; please retry
SQLSTATE: 40001 (serialization_failure)
```

The pooler cannot say why the reservation is gone, or what it held, so the
message names no cause. When the backend died mid-operation and PostgreSQL
sent a diagnostic before closing (for example its own `57P01`), that diagnostic
is returned instead (`reservedConnTerminatedError`).

`40001` is deliberate: it is not `MTF01`, so the gateway does not buffer or
retry it (the transaction no longer exists), and clients and ORMs treat it as
"retry the whole transaction".

A PostgreSQL-level error that leaves the backend healthy is **not** a loss.
The executor returns the error together with the surviving reserved state, and
the gateway keeps the reservation. In particular, a statement that fails inside
an explicit transaction keeps the transaction's reservation, so the client's
`ROLLBACK` reaches the same backend.

## What the gateway does

`ScatterConn.applyReservedStateOnError` compares the state the RPC returned
with what the gateway was tracking:

- **The reservation held only a transaction** (or a portal owned by that
  transaction). The error is passed to the client, the `ShardState` is cleared,
  and a session that was in a transaction block moves to the failed state, so
  the client must `ROLLBACK` and can then retry.
- **The reservation held session state.** A retry cannot bring that state
  back, and the next statement would silently run on a fresh backend without
  it: an advisory lock another session can now take, a temp table that no
  longer shadows a permanent one. The gateway ends the client session instead:

  ```text
  FATAL:  terminating connection because the backend holding its session state was lost
  DETAIL: Lost session state: <reasons>.
  SQLSTATE: 57P01 (admin_shutdown)
  ```

  If the cause is already `FATAL`, it is returned unchanged. The gateway logs
  `ending session: reserved connection holding session state was lost` with the
  lost reasons and the cause's SQLSTATE.

Session state here means these reasons (`sessionStateReasons`): temp table,
logical replication, session advisory lock, `setseed`, unsafe connection. A
portal counts when it is not owned by a transaction, or when the session has
any `WITH HOLD` cursor open.

A reservation that disappears **without** an error was released on purpose
(`COMMIT`, an advisory unlock, a closed cursor) and loses nothing.

## Operational notes

- Sessions that hold a reservation and then idle (a temp table, a session
  advisory lock, an open transaction) are terminated after the inactivity
  timeout. Raise `--connpool-user-reserved-inactivity-timeout` for workloads
  that legitimately idle while pinned.
- `mg.pooler.reserved.active_by_reason` shows what is holding reserved
  connections; `mg.pooler.drain.force_closed` counts reservations lost to a
  drain timeout.
- Client retry logic should handle `40001` by replaying the transaction and
  `57P01` by reconnecting.

## Tests

- `go/services/multigateway/scatterconn/` unit tests cover `lostSessionState`
  and the error translation.
- `go/test/endtoend/queryserving/reservation_loss_test.go`,
  `reserved_conn_error_recovery_test.go`, `temp_table_timeout_test.go` and
  `transaction_failover_test.go` cover the end-to-end behaviour.
