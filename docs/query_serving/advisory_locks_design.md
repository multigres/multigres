# Session-Level Advisory Locks

## Overview

PostgreSQL session-level advisory locks (`pg_advisory_lock`,
`pg_advisory_lock_shared`, and their `try` variants) live on a specific backend
and survive transaction boundaries. Under multigateway's pooled model a client's
logical session is not bound to one backend, so without special handling these
locks would be taken on whatever backend ran the acquiring statement and then
lost — or leaked to the next client — on the following query.

Multigres makes session-level advisory locks work by **pinning** the backend to
the client session for the lifetime of the lock, using the same
reserved-connection machinery as transactions, temp tables, and `WITH HOLD`
cursors. The key design choice is that **PostgreSQL remains the authority on the
(reference-counted) lock state**: the gateway decides when to pin and when to
ask, but `pg_locks` decides when to unpin.

Transaction-level advisory locks (`pg_advisory_xact_lock*`) are out of scope:
they are released at transaction end and never outlive a pooled backend's
involvement in the session, so they need no pinning.

## Background

- A session advisory lock is acquired with `pg_advisory_lock(key)` /
  `pg_advisory_lock(key1, key2)` (exclusive) or the `_shared` variant, plus the
  non-blocking `pg_try_advisory_lock*` forms. It is **reference-counted**: N
  acquires of the same key require N unlocks.
- It is released by `pg_advisory_unlock` / `_shared` (one ref), by
  `pg_advisory_unlock_all()` (all at once), by `DISCARD ALL`, or implicitly when
  the session ends (disconnect, `pg_terminate_backend`, crash).
- It is **not** released by COMMIT or ROLLBACK — even rolling back the
  transaction that acquired it leaves the lock held.

## Approach

Three pieces, split across the gateway and the multipooler:

1. **Detect + pin (gateway).** The planner inspects a statement's function calls.
   A session-level advisory _acquire_ routes the query through an
   `AdvisoryLockRoute`, which makes ScatterConn reserve (or promote an existing
   reservation to) a backend with `ReasonSessionAdvisoryLock`. The connection
   stays pinned to the session while that reason is set.

2. **Unpin authoritatively (multipooler).** After a statement that _touches_
   advisory locks, the multipooler probes `pg_locks` for the backend; if no
   advisory lock remains (and it is not inside a transaction), it clears
   `ReasonSessionAdvisoryLock` and releases the connection when no other reason
   keeps it pinned. Because PostgreSQL answers the probe, this is correct for
   reference-counted stacking, failed `try` locks, `pg_advisory_unlock_all()`,
   and unlocks issued in surprising ways.

3. **Scrub on release (multipooler).** When a reserved connection is handed back
   to the pool (client disconnect, or `DISCARD ALL`), the release path runs
   `SELECT pg_advisory_unlock_all()` before recycling it, so a backend never
   returns to the pool still holding a session advisory lock. This is the narrow
   fix — unlike forwarding `DISCARD ALL` to the backend, it leaves prepared
   statements intact, keeping the multipooler's per-connection tracking in sync.

## Detection

The planner's pre-dispatch analysis (`analyzeFunctionCalls`) walks every
`FuncCall` and classifies advisory functions:

- **Acquire family** (`sessionAdvisoryLockAcquireFuncs`): `pg_advisory_lock`,
  `pg_advisory_lock_shared`, `pg_try_advisory_lock`,
  `pg_try_advisory_lock_shared`. Sets `AcquiresSessionAdvisoryLock`, which drives
  the pin.
- **Release family** (`sessionAdvisoryLockReleaseFuncs`): `pg_advisory_unlock`,
  `pg_advisory_unlock_shared`, `pg_advisory_unlock_all`. Sets
  `ReleasesSessionAdvisoryLock`.

A statement that acquires or releases is wrapped in `AdvisoryLockRoute` with two
intents:

- **pin** (acquires only) → `ReservationOptions.reasons |= SESSION_ADVISORY_LOCK`.
- **recheck** (acquires and releases) → `ReservationOptions.recheck_advisory_locks`.

Detection is best-effort: it sees advisory calls in the statement text — the
overwhelming common case — but not locks acquired or released indirectly (inside
a PL/pgSQL function, a trigger, or dynamic SQL). A missed acquire means the
session isn't pinned (the same pre-existing limitation as temp tables created via
dynamic SQL); a missed release means the session stays pinned conservatively
until the next observed advisory statement, `DISCARD ALL`, or disconnect — never
a leak.

## Pinning and the recheck signal

Acquire and release both flow through `AdvisoryLockRoute`, which sets one-shot
flags on the connection state that ScatterConn turns into `ReservationOptions`:

- `PendingAdvisoryLockReservation` → adds `ReasonSessionAdvisoryLock`.
- `PendingAdvisoryLockRecheck` → sets `recheck_advisory_locks`.

This works on both the simple and extended (portal) protocols: portals carry
`reservation_options` to the multipooler, which reserves-and-runs atomically, so
an advisory lock acquired via a bound-parameter `pg_advisory_lock($1)` pins the
same way a simple-protocol call does.

The recheck signal is what keeps the probe off the hot path. Without it, the
multipooler would have to probe `pg_locks` after _every_ statement on a pinned
connection. Instead it probes only when the gateway flags a statement as
touching advisory locks:

- An **acquire** sets recheck so a failed `pg_try_advisory_lock` (which pinned
  optimistically but took no lock) unpins immediately.
- A **release** sets recheck so the connection unpins once the last lock is gone.
- Any other statement on a pinned connection skips the probe entirely.

The probe is still the authority — the flag only decides _when_ to ask. We
deliberately do not count acquires/unlocks at the gateway: reference counting,
failed `try` locks, and hidden unlocks make gateway-side counting unreliable.

## Release paths

| Trigger                          | How it's handled                                               |
| -------------------------------- | -------------------------------------------------------------- |
| `pg_advisory_unlock` / `_shared` | recheck signal → `pg_locks` probe → unpin if zero              |
| `pg_advisory_unlock_all()`       | recheck signal → `pg_locks` probe → unpin                      |
| `DISCARD ALL`                    | reserved-connection release → `pg_advisory_unlock_all()` scrub |
| Disconnect / terminate / crash   | reserved-connection release → scrub (or socket close)          |
| COMMIT / ROLLBACK                | not a release — lock survives, connection stays pinned         |

## Limitations

- Advisory locks acquired or released inside a function body, trigger, or
  dynamic SQL are not detected; the session pins/unpins conservatively (never
  leaks, but may hold a pin longer than strictly necessary).
- Transaction-level advisory locks (`pg_advisory_xact_lock*`) are intentionally
  not pinned.

## Code map

- Detection: `go/services/multigateway/planner/unsafe_funccall.go`
  (`sessionAdvisoryLockAcquireFuncs`, `sessionAdvisoryLockReleaseFuncs`).
- Routing: `planner.go` (`PlannerOptions`, `routePrimitive`),
  `engine/advisory_lock_route.go`.
- Reservation reason: `go/common/protoutil/reservation.go`
  (`ReasonSessionAdvisoryLock`), `proto/multipoolerservice.proto`.
- Gateway wiring: `scatterconn/scatter_conn.go`,
  `handler/connection_state.go` (`PendingAdvisoryLockReservation`,
  `PendingAdvisoryLockRecheck`).
- Probe + scrub: `go/services/multipooler/internal/executor/executor.go`
  (`maybeUnpinSessionAdvisoryLock`, `ReleaseReservedConnection`),
  probe SQL in `go/common/constants/postgres.go`
  (`PgLocksAdvisoryProbeSQL`).
- Tests: `go/test/endtoend/queryserving/advisory_lock_test.go`,
  `planner/advisory_lock_test.go`,
  `multipooler/internal/executor/executor_test.go`.
