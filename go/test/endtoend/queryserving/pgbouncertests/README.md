# pgbouncertests — pooling & resilience e2e tests

End-to-end tests for the `multigateway → multipooler → postgres` path, covering
connection pooling, session-state handling, and fault recovery. The scenarios
are derived from [PgBouncer's test suite][pgbouncer-test] (ISC-licensed; see
[`LICENSE`](./LICENSE)).

Multigres is a passthrough proxy — Postgres computes every result — so these
tests are **not** about result correctness (that is covered by `sqllogictest`
and `pgregresstest`). They check that the proxy faithfully relays protocol and
session state, and behaves correctly under pooling and faults.

## What's covered

Each `*_test.go` mirrors a PgBouncer `test_*.py` scenario; its doc comment
explains the specific behavior and why it matters.

| Test file                 | Scenario                                                                   |
| ------------------------- | -------------------------------------------------------------------------- |
| `prepared_swap_test.go`   | a prepared statement is re-prepared correctly across a pooled-backend swap |
| `large_statement_test.go` | large (>4 KB) Parse / Bind messages relay intact                           |
| `prepared_reset_test.go`  | `DEALLOCATE ALL` / `DISCARD ALL` drop the session's prepared statements    |
| `backend_restart_test.go` | backend crash recovery under load, and transparent reconnect               |
| `pool_exhaustion_test.go` | a full pool queues and proceeds (multigres blocks; it does not reject)     |
| `reserved_conn_test.go`   | reserved-connection pin/release boundary and idle-in-transaction timeout   |
| `cancel_race_test.go`     | a cancel for one client never hits another client reusing its backend      |
| `copy_fault_test.go`      | `COPY FROM STDIN` interrupted by a backend crash                           |
| `nonexistent_db_test.go`  | connecting to a nonexistent database (differential vs. PostgreSQL)         |

## Approach

The tests are **black-box**: they speak only the PostgreSQL wire protocol to the
multigateway and observe only what a client can — query results, errors, and
`pg_backend_pid()` read from the client's own rows (never a side channel into the
underlying PostgreSQL, except where a test explicitly samples `pg_stat_activity`
to measure concurrency).

Which backend a pooled (non-pinned) session lands on is an internal detail, so
the swap-boundary tests don't force a specific swap. Instead `startPoolChurn`
drives concurrent load that keeps the pool rotating, and `runAcrossBackends`
asserts a statement ran on ≥2 distinct backends (so the test isn't vacuous)
while every execute returns correct results. Fault tests inject crashes with
`KillPostgres` and run in their own `shardsetup.NewIsolated` cluster so the
damage can't leak into the shared-cluster tests.

## Running

This suite runs as part of the standard e2e integration job. Each test
self-skips in `-short` mode or when the PostgreSQL
binaries are absent. Use the `/mt-dev` skill (integration tests build
first); don't run `go test` directly.

## Known divergences from PostgreSQL

- **Nonexistent connection database:** the gateway authenticates the client and
  serves its backend database instead of rejecting with `3D000`
  (guarded by `nonexistent_db_test.go`).
- **`DISCARD ALL`** does not run `pg_advisory_unlock_all()` on the backend, so
  session-level advisory locks are not released (a pre-existing pooling
  limitation).

## Out of scope

PgBouncer features with no multigres analog are not ported: the admin console
(`PAUSE`/`RESUME`/`SHOW`/`RELOAD`), HBA/userlist auth, peering / `SO_REUSEPORT`,
and `load_balance_hosts`.

## Attribution

Scenarios are derived from [PgBouncer][pgbouncer-test] (ISC License, © 2007-2009
Marko Kreen, Skype Technologies OÜ); the upstream license is in
[`LICENSE`](./LICENSE). New code is licensed under Apache-2.0 (the repository-root
`LICENSE`); each `.go` file carries the standard Apache header plus a PgBouncer
attribution line.

[pgbouncer-test]: https://github.com/pgbouncer/pgbouncer/tree/master/test
