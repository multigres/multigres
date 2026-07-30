# Backend PRNG Seeding (`setseed`)

## Overview

`setseed(x)` seeds the calling backend's pseudo-random number generator, so
subsequent `random()` / `random_normal()` calls in that session follow a
reproducible sequence. The seed is pure backend-local process state: it has no
SQL-visible representation (no GUC, no row in any catalog) and no PostgreSQL
command resets it, not even `DISCARD ALL`.

Under multigateway's pooled model a client's logical session is not bound to
one backend. Without special handling, `setseed()` would take effect on
whatever backend happened to run that one statement, and the very next
`random()` call could land on a different pooled backend with a different (or
no) seed, silently breaking the reproducible sequence the client expects.

Multigres makes this work the same way it does for temp tables and logical
replication slots: **pinning** the backend to the client session using the
reserved-connection machinery, for the rest of the session's lifetime. Unlike
session-level advisory locks, there is no PostgreSQL-visible signal that "the
seed no longer matters," so this reservation reason is **sticky**: once set,
it is never released by an unpin probe, and it uniquely survives `DISCARD
ALL`, only clearing on the connection's real teardown (client disconnect).

## Background

- `setseed(x)` takes a double in `[-1, 1]` and returns `void`. It has no
  reset command; the only way to change the seed is to call it again.
- Verified against a real backend: `DISCARD ALL` does not reset a seeded
  PRNG. A `random()` call issued right after `DISCARD ALL` continues the
  exact same sequence as if `DISCARD ALL` had never run.
- This makes `setseed` different from every other pinning reason in
  Multigres: temp tables are dropped by `DISCARD TEMP` (part of
  `DISCARD ALL`), session advisory locks are dropped by
  `pg_advisory_unlock_all()` (also part of `DISCARD ALL`), but nothing
  analogous exists for a seed.

## Approach

1. **Detect + pin (gateway).** The planner's function-call analysis
   (`analyzeFunctionCalls`) flags any statement containing a call to
   `setseed`, setting `CallsSetSeed`. This flows through `PlanOptions` into
   `PlanExecInfo.SetSeed`, which `ScatterConn` turns into a reserved
   connection (or promotes an existing reservation) with `ReasonSetSeed`.
   Acquire-only, like logical replication slots: there is no recheck signal,
   because there is nothing to recheck.

2. **Stay pinned through `DISCARD ALL` (gateway + multipooler).** Every other
   reservation reason is released generically when `DiscardAllPrimitive`
   calls `ReleaseAllReservedConnections`. `ReasonSetSeed` is the one
   exception: the RPC that releases a reserved connection
   (`ReleaseReservedConnection`) takes a `keep_sticky_reservations` flag.
   `DiscardAllPrimitive` passes `true`, so a connection whose only remaining
   reason is `ReasonSetSeed` after the usual `DISCARD ALL` cleanup (rollback,
   COPY abort, temp table discard, advisory unlock) stays reserved instead of
   returning to the pool. Real client-disconnect cleanup
   (`Executor.ReleaseAll`) always passes `false`, so a sticky reason never
   blocks a connection's actual teardown.

3. **Accept the residual gap.** Because the reservation is released the
   generic way on real teardown, but that release does not itself clear the
   seed, a later, unrelated session can be handed a backend whose PRNG is
   still seeded from a prior session. This is accepted: it only affects the
   statistical freshness of an unrelated session's own `random()` sequence,
   not the correctness bug this reason exists to prevent (a session's own
   reproducible sequence silently changing mid-use).

## Detection

`sessionSetSeedFuncs` in `unsafe_funccall.go` lists `setseed` as a function
whose presence anywhere in a statement's expression tree (however nested)
sets `CallsSetSeed`, mirroring the detection style already used for advisory
locks and logical replication slot creation.

Detection is best-effort: a `setseed()` call hidden inside a PL/pgSQL function
body or dynamic SQL is not detected, the same class of limitation the
advisory-lock and logical-replication-slot detection already documents.

## Release paths

| Trigger                        | How it's handled                                                                                                                 |
| ------------------------------ | -------------------------------------------------------------------------------------------------------------------------------- |
| `DISCARD ALL`                  | reserved-connection release with `keep_sticky_reservations=true`: stays reserved if `ReasonSetSeed` is the only remaining reason |
| Disconnect / terminate / crash | reserved-connection release with `keep_sticky_reservations=false`: always fully releases                                         |
| COMMIT / ROLLBACK              | not a release; the reservation (and the seed) survive transaction boundaries the same as temp tables and advisory locks          |
| Another `setseed(...)` call    | no-op for the reservation; the backend is already pinned, the new seed simply replaces the old one                               |

## Limitations

- `setseed()` calls hidden inside a function body, trigger, or dynamic SQL
  are not detected; such a session runs unpinned and its `random()` sequence
  can move between backends.
- A backend's seed is never cleared once a session that called `setseed()`
  disconnects; a later session can inherit it (see "Accept the residual gap"
  above).

## Code map

- Detection: `go/services/multigateway/planner/unsafe_funccall.go`
  (`sessionSetSeedFuncs`, `CallsSetSeed`).
- Routing: `planner.go` (`PlanOptions.PinForSetSeed`, `execInfoFromOpts`,
  `planType`), `engine/engine.go` (`PlanExecInfo.SetSeed`), `engine/plan.go`
  (`PlanTypeSetSeedRoute`).
- Reservation reason: `go/common/protoutil/reservation.go` (`ReasonSetSeed`),
  `proto/multipoolerservice.proto`
  (`RESERVATION_REASON_SET_SEED`, `keep_sticky_reservations`).
- Gateway wiring: `scatterconn/scatter_conn.go`
  (`reservationReasonsForExecInfo`, `ReleaseAllReservedConnections`),
  `engine/discard_all_primitive.go`, `executor/executor.go` (`ReleaseAll`).
- Sticky release: `go/services/multipooler/internal/executor/executor.go`
  (`ReleaseReservedConnection`), `go/services/multipooler/grpcpoolerservice/service.go`.
- Tests: `planner/unsafe_funccall_test.go`, `scatterconn/scatter_conn_test.go`,
  `multipooler/internal/executor/executor_test.go`.
