# pgbouncertests — pooling & resilience suite (triage)

> **Status: PORTING COMPLETE — all (A) groups landed.** This document started as
> Step 1 of the PgBouncer-port effort. It categorizes every PgBouncer test file
> as **(A) port**, **(B) out of scope**, or **(C) already covered**, and tracks
> the net-new Go tests to land. The triage has been reviewed; the resolved
> decisions are recorded in [Review decisions](#review-decisions). The point of
> the triage is to land only the net-new coverage and avoid re-porting what we
> already test. All seven (A) groups are **complete** — Group 1 (prepared
> statements across a pooled-backend swap), Group 2 (continuous-load-under-
> disruption), Group 3 (pool exhaustion / capacity), Group 4 (reserved-connection
> lifecycle & timeout), Group 5 (cancel-race under pooling), Group 6 (COPY under
> fault), and Group 7 (small differential deltas) — see
> [Implementation status](#implementation-status).

The scenarios in the (A) set are **derived from PgBouncer's own test suite**,
which is ISC-licensed. The upstream license is reproduced in the `LICENSE` file
in this directory; see [Attribution and license](#attribution-and-license).

## Mission and framing

We want the behavioral coverage of [PgBouncer's test suite][pgbouncer-test]
(`~/pgbouncer/test/`, Python/pytest) re-expressed as Go end-to-end tests on the
`multigateway → multipooler → postgres` path, reusing the existing
`go/test/endtoend/` harness.

**Multigres is currently a passthrough proxy.** Postgres computes every result;
multigres does not rewrite or shard queries. So this suite's value is **not
result correctness** — that is Postgres's job, covered by `sqllogictest` and
`pgregresstest`. The value is whether the proxy **faithfully relays
protocol/session state and behaves correctly under pooling and faults**:

- Does session state and prepared statements survive a **pooled-backend swap**?
- Does the **reserved (pinned) connection** lifecycle behave correctly?
- Does **pool exhaustion** queue/block correctly instead of corrupting state?
- Does the proxy **recover** when a backend is killed/restarted mid-load?

Treat PostgreSQL as the oracle wherever a differential comparison makes sense
(e.g. error SQLSTATEs), exactly as `pgproto`/`sqllogictest` do — but note most of
the net-new value here is **stateful scenario + fault-injection tests**, not
statement-result diffs (see [Differential vs. scenario testing](#differential-vs-scenario-testing)).

This is a **triage-and-port** job, not a translation job. PgBouncer's tests are
wired to PgBouncer's own admin console, INI config files, and `utils.py` fault
helpers. We re-express only the **proxy-generic** scenarios on top of our
harness; PgBouncer-specific machinery (admin console, HBA/userlist auth, peering,
`SO_REUSEPORT`) has no multigres analog and is excluded with rationale below.

## The multigres pooling model (the facts the triage rests on)

These are the proxy behaviors the suite exercises. Verified in-repo:

- **Multigateway does no connection pooling.** It load-balances across
  multipooler instances and buffers during failover
  (`go/services/multigateway/poolergateway/pooler_gateway.go`). All backend
  pooling lives in **multipooler**.
- **Multipooler pools postgres backends per statement.** A clean backend is
  borrowed, settings applied, statement executed, then returned to the pool
  (`go/services/multipooler/pools/connpool/pool.go`). Pooled connections are
  bucketed by a settings hash so a session's `SET`s are replayed onto whichever
  backend it next lands on (`pools/regular/regular_conn.go` `ApplySettings`).
  **This is the "pooled-backend-swap boundary" — where pooling bugs hide.**
- **Reserved (pinned) connections** keep one physical backend attached to a
  client session across statements when work is non-poolable. The reasons
  (`go/common/protoutil/reservation.go`) are: **transaction, temp table, portal
  (suspended cursor), COPY, LISTEN, logical replication**. There is **no
  `ReasonPrepared`** — a bare named `Parse` does _not_ pin the connection.
- **Prepared statements are re-prepared on swap, not pinned.** The executor's
  `ensurePrepared` / `ensurePreparedWithName`
  (`go/services/multipooler/executor/executor.go:308,682`) re-parse a client's
  named statement onto whichever backend a later Bind/Execute lands on. So a
  statement parsed once and executed after a swap _should_ still work — exactly
  PgBouncer's `test_prepared.py` scenario, and it exercises real code.
- **Pool exhaustion blocks, it does not error.** When capacity is reached
  callers wait on a waitlist until a connection recycles
  (`connpool/pool.go`); there is no overflow error, no per-db/per-user _client_
  connection cap. Capacity is `--connpool-global-capacity` (default 100) split
  by a fair-share rebalancer; reserved gets `--connpool-reserved-ratio` (0.2).
- **Timeouts that exist:** `--connpool-user-regular-idle-timeout` (5m,
  ≈ `server_idle_timeout`), `--connpool-user-regular-max-lifetime` (1h,
  ≈ `server_lifetime`), `--connpool-user-reserved-inactivity-timeout` (30s,
  ≈ idle-in-transaction killer). `statement_timeout` is enforced at the gateway.
- **No PgBouncer-style admin console.** Neither multigateway nor multipooler
  exposes `PAUSE`/`RESUME`/`SUSPEND`/`RECONNECT`/`RELOAD`/`SHOW`. Pool config is
  fixed at process start via flags; there is no live reconfiguration.
  (`multiadmin` is a cluster-admin gRPC/HTTP service, not a PG-protocol console.)

## Triage table

`A` = port net-new · `B` = out of scope (no analog) · `C` = already covered.
"Existing test" names files under `go/test/endtoend/queryserving/`.

| PgBouncer file               | Verdict | Net-new delta to port / why excluded                                                                                                                                                                                                     | Existing coverage                                                             |
| ---------------------------- | ------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------- |
| `test_prepared.py`           | **A**   | Prepared statement + session state survive a **pooled-backend swap** (re-prepare via `ensurePrepared`); large Parse/Bind (>4 KB) relay; DISCARD ALL across pooling                                                                       | `prepared_stmt_test.go`, `extended_query_protocol_test.go` (no swap boundary) |
| `test_operations.py`         | **A**   | **Backend restart/reconnect under continuous load** (the `stress.py` ethos): pool transparently reconnects new statements; in-flight statement sees a clean error. _Admin-command parts → B._                                            | `postgres_crash_recovery_test.go`, `buffer_test.go` (single events, not load) |
| `test_limits.py`             | **A**   | **Pool exhaustion / capacity**: with `--connpool-global-capacity=N`, the (N+1)th concurrent statement blocks then proceeds; no state corruption. _`max_client_conn`/`min_pool_size`/per-db caps/dynamic reload → B._                     | none                                                                          |
| `test_timeouts.py`           | **A**   | **Reserved-connection inactivity timeout** (idle-in-txn killer): pinned conn killed after timeout → next statement errors cleanly. **Backend recycle on max-lifetime/idle** → seamless for new statements. _`statement_timeout` → C._    | `statement_timeout_test.go` (query timeout only)                              |
| `test_cancel.py`             | **A**   | **Cancel-race under pooling**: cancel for client A must not hit a backend just reused by client B; cancel while pool exhausted. _Basic cancel/cross-conn/gRPC-TLS forwarding → C._                                                       | `query_cancel_test.go` (no pooling-reuse race)                                |
| `test_copy.py`               | **A**   | **COPY interrupted by backend fault** (kill mid-stream → clean error + recovery); COPY in **pipeline/extended** mode. _Formats, TO STDOUT, txn, multi-statement, error recovery → C._                                                    | `copy_test.go` (extensive, no fault path)                                     |
| `test_no_database.py`        | **A**   | Connect to **nonexistent database** → clean SQLSTATE `3D000`, connection closed cleanly; auth-vs-db error ordering (differential vs PG). _`auto_database`/`auth_user` variants → B._                                                     | none (no explicit nonexistent-db test)                                        |
| `test_misc.py`               | **A/B** | A: pool **queue-wait** behavior (overlaps `test_limits`). B: `query_wait_notify` (no admin notice), `server_check_query` (no per-conn health check), `fast_close`/`wait_close`/`auto_database`/`host_list` (PgBouncer config).           | `startup_params_test.go` (connect_query/options → C)                          |
| `test_ssl.py`                | **B/C** | C: full static TLS matrix already covered + multipooler↔PG TLS. B: live TLS toggle / CA change via SIGHUP (no live reconfig).                                                                                                            | `ssl_test.go`, `WithMultipoolerPGTLS`                                         |
| `test_auth.py`               | **B/C** | C: SCRAM correct/wrong/nonexistent, replication-role rejection. B: `auth_type` matrix, `auth_query`, `auth_dbname`, userlist/HBA file, mock auth, VALID UNTIL — all PgBouncer auth machinery with no analog (auth is passthrough to PG). | `authentication_test.go`                                                      |
| `test_replication.py`        | **B**   | Replication protocol passthrough is **stubbed** (gateway returns `0A000 feature_not_supported`); porting would test unimplemented features. Existing test already asserts the stub. Revisit when replication lands.                      | `replication_test.go`, `replica_reads_test.go`                                |
| `test_no_user.py`            | **B/C** | C: nonexistent user → clean auth error. B: mock-auth across trust/plain/md5/scram + forced-user (PgBouncer-specific).                                                                                                                    | `authentication_test.go`                                                      |
| `test_admin.py`              | **B**   | Admin console `SHOW`/`RELOAD`/config validation — no multigateway analog.                                                                                                                                                                | —                                                                             |
| `test_load_balance_hosts.py` | **B**   | `load_balance_hosts` over a server host-list — multigres routes by topology, not a static host-list with round-robin/disable.                                                                                                            | (routing covered by `replica_reads_test.go`)                                  |
| `test_peering.py`            | **B**   | Peering / `SO_REUSEPORT` / rolling restart via peer takeover — gateways coordinate via etcd/topology, not PgBouncer peering. Linux-only.                                                                                                 | —                                                                             |

## (A) Port — the net-new suite

Package name: **`pgbouncertests`** — names the source of the scenarios so the
provenance (and the ISC attribution) is obvious. The net-new value is stateful
pooling + fault-resilience, since the protocol basics are already well covered.
Concrete test groups, in rough priority order:

1. **Prepared statements across a pooled-backend swap (headline).** Open a
   session, `Parse` a named statement, force the session to land on a _different_
   backend before `Bind`/`Execute` (e.g. interleave another client to occupy the
   first backend, or exhaust+recycle), assert the statement still executes and
   the result matches direct-PG. Probe `ensurePrepared` re-prepare path. Also:
   `DISCARD ALL` / `DEALLOCATE ALL` semantics across the swap; Parse/Bind > 4 KB
   relay. _Asserts the boundary, not just the happy path._
2. **Continuous-load-under-disruption (the `stress.py` ethos).** Drive
   continuous statements through the gateway while `KillPostgres` /
   `StopPostgres(...)` / restart the backend; assert new statements reconnect
   transparently, in-flight statements get a clean error (not a hang or torn
   packet), and no session-state corruption survives the event. Reuse
   `postgres_crash_recovery_test.go` primitives; extend to _under load_ + the
   reconnect/PID-change assertion from `test_operations.py::test_reconnect`.
3. **Pool exhaustion / capacity.** Configure a small `--connpool-global-capacity`
   (needs a harness option — see [gaps](#harness-gaps-to-close)); launch N+1
   concurrent slow statements; assert the (N+1)th **blocks then completes** when a
   slot frees, and that a context-cancelled waiter is cleaned up. (Multigres
   blocks rather than erroring — the assertion differs from PgBouncer's reject.)
4. **Reserved-connection lifecycle & timeout.** Open an explicit transaction
   (pins a backend), idle past `--connpool-user-reserved-inactivity-timeout`,
   assert the next statement on the pinned connection errors cleanly and the
   backend is reclaimed. Verify temp-table / LISTEN / portal reservations pin and
   release at the right boundaries.
5. **Cancel-race under pooling.** Issue a cancel for a query whose backend may be
   released and reused by another client; assert the cancel never disturbs the
   reusing client (the `test_cancel.py::test_cancel_race` concern).
6. **COPY under fault.** Kill the backend mid-`COPY FROM STDIN`; assert a clean
   error and that the connection (or a fresh one) is usable afterward.
7. **Small differential deltas (PG-as-oracle).** Connect to a nonexistent
   database → `3D000`; auth-failure-vs-db-error ordering. These _do_ fit the
   differential model and can reuse `setup.GetComparisonTargets`.

### Implementation status

Groups 1 and 2 are **complete**.

**Group 1 — prepared statements across a pooled-backend swap.** Tests:

- `prepared_swap_test.go` — `TestPreparedStatementAcrossPooledBackends`: prepare
  once, then execute many times; asserts every execute returns correct results
  (exercising the multipooler `ensurePrepared` re-prepare path) and that the
  statement was observed running on ≥2 distinct backends.
- `large_statement_test.go` — `TestLargePreparedStatementRelay` (differential:
  16 KB Parse + 64 KB Bind relay, PG-as-oracle) and
  `TestLargePreparedStatementAcrossPooledBackends` (a 16 KB statement re-prepares
  correctly on every backend the pool serves it from, literal intact).
- `prepared_reset_test.go` — `TestPreparedStatementResetSemantics`
  (differential): both `DEALLOCATE ALL` and `DISCARD ALL` drop the session's
  prepared statements identically on PG and the gateway. `DISCARD ALL` used to
  fail here (it surfaced a real bug); it now passes after the fix described in
  the note below.

**How the swap is exercised — black-box.** Which physical backend an Execute
lands on is an internal pooling detail; an e2e test treats the proxy as a black
box, so it does not (and cannot) deterministically force a _specific_ internal
swap. Instead the tests assert the invariant — _a statement prepared once keeps
returning correct results no matter which backend serves each Execute_ — under
conditions that make backends rotate:

- **Churn** (`startPoolChurn`): a handful of ordinary background clients run a
  cheap query in a tight loop. Because the multipooler pool is LIFO, their
  constant borrow/return keeps changing which connection is on top, so a
  foreground session's consecutive Executes are handed different backends instead
  of its own just-returned one. This is pure wire-protocol load.
- **Observation**: the foreground client reads `pg_backend_pid()` from its _own_
  query results through the gateway — a normal result value, not a side channel
  into the underlying PostgreSQL.
- **Assertion** (`runAcrossBackends`): run the statement until it has been served
  by ≥2 distinct backends (and at least `minExecutes` times); every execute must
  succeed (a missing re-prepare would surface as "prepared statement does not
  exist"), and seeing ≥2 backends proves the test is not vacuous.

Earlier attempts that depended on proxy internals were rejected as not
black-box: terminating the backend (`pg_terminate_backend`) leaves a dead socket
the next streaming Execute hits as a broken pipe; pinning a specific backend with
holder connections, and reaping via a short idle timeout read through
`pg_stat_activity`, both coupled the test to pool internals and the heartbeat
interval.

`shardsetup.WithMultipoolerExtraArgs` was added during this work as a general
harness option for connpool tuning; group 1 doesn't use it, but groups 3 and 4
(pool exhaustion, reserved-connection timeout) do.

**Bug found and fixed — `DISCARD ALL` no longer poisons pooled connections.**
The original `DISCARD ALL` plan forwarded the statement to a pooled backend
(where it ran `DEALLOCATE ALL`, dropping that backend's server-side prepared
statements) but invalidated none of the matching bookkeeping: the gateway's
consolidator stayed populated, so `DISCARD ALL` didn't actually drop the
client's prepared statements (the next EXECUTE succeeded instead of failing —
diverging from PostgreSQL); and the multipooler's per-connection tracking
(`connState.PreparedStatements`) stayed stale on the backend it ran on, so a
later session reusing that pooled backend skipped re-`Parse` and failed at
`Bind` with "prepared statement does not exist" (SQLSTATE 26000).

The fix (commit `50417fb0`) handles `DISCARD ALL` **entirely at the gateway and
never forwards it to a shared pooled backend** — a new `DiscardAllPrimitive`
(`go/services/multigateway/engine/discard_all_primitive.go`, planned via
`planDiscardStmt`). In the pooled model the session state `DISCARD ALL` resets
lives at the gateway, not on any one backend: prepared statements (the
consolidator), session GUCs (`ResetAllSessionVariables` / `ResetStatementTimeout`),
and LISTEN subscriptions are all gateway-side. The only backend-resident session
state is whatever a _reserved_ connection holds (open transaction, temp tables,
`WITH HOLD` cursors), which the primitive cleans up via
`ReleaseAllReservedConnections`. The primitive also rejects `DISCARD ALL` inside
a transaction block (matching PG's `PreventInTransactionBlock`). Already-prepared
statements on pooled backends are intentionally left in place — they act as a
pool-wide dedup cache and the multipooler's `connState` stays accurate.

Because the bug is fixed, the test now lives as the `DISCARD ALL` case of the
differential `TestPreparedStatementResetSemantics` and passes against both PG
and the gateway. The earlier failing-on-purpose `TestPreparedStatementDiscardAll`
and its `shardsetup.NewIsolated` cluster are gone — the whole package runs in the
shared cluster.

**Group 2 — continuous-load-under-disruption (the `stress.py` ethos).** Tests
in `backend_restart_test.go`, ported from `test_operations.py`'s
`test_database_restart` and `test_reconnect`. Both inject the fault with
`KillPostgres` (SIGKILL — a hard crash); the multipooler's postgres monitor then
auto-restarts the _same_ backend (crash recovery; the primary stays primary —
there is no failover). Each runs in its own `shardsetup.NewIsolated` cluster so
the deliberate crash cannot leak into the shared-cluster tests.

- `TestBackendCrashRecoveryUnderLoad` — drives continuous INSERT load through the
  gateway with a `shardsetup.WriterValidator` (6 workers), hard-kills postgres
  mid-load, waits for the monitor to restart it, then asserts the load **resumes
  on its own** (success count climbs past the recovery point with no client
  reconnect) and that **every acknowledged write survived** crash recovery
  (`count(*) ≥ successful` — a SIGKILL mid-commit can leave a committed row whose
  ack never reached the client, so the table may hold a few _extra_ rows).
- `TestBackendCrashTransparentReconnect` — the `test_reconnect` analog. A single
  client session reads `pg_backend_pid()`, postgres is hard-killed and restarted,
  and the **same** session must recover: its next statement lands on a fresh
  backend (a _different_ pid). The first post-crash statement may error, but
  cleanly and within a deadline (the "clean error, not a hang or torn packet"
  property) — never a hang.

**What Group 2 establishes about multigres.** The transparent-reconnect test
confirms a property stronger than PgBouncer's: a client's gateway connection
**survives a backend crash**. PgBouncer's `test_database_restart` tolerates
in-flight `OperationalError`s and then opens a _new_ client connection; in
multigres the client connection stays up across the crash because backend
pooling lives in the multipooler, decoupled from client sessions — the gateway
swaps in a fresh backend underneath the same session.

**Group 3 — pool exhaustion / capacity.** Tests in `pool_exhaustion_test.go`,
ported from `test_limits.py`. Each runs in its own `NewIsolated` cluster started
with a deliberately tiny pool via `shardsetup.WithMultipoolerExtraArgs`
(`--connpool-global-capacity=4 --connpool-reserved-ratio=0.5
--connpool-rebalance-interval=1s`), which settles the lone test user's regular
sub-pool to 2 backends. Where PgBouncer **rejects** connections on a full pool,
multigres **blocks** on the waitlist (`connpool/{pool.go,waitlist.go}`) — so the
assertions are inverted accordingly.

- `TestPoolExhaustionBlocksThenProceeds` — launches 12 concurrent `pg_sleep(1)`
  statements at a pool that can run only ~2 at once, and asserts **all 12
  succeed** (queue-and-proceed, no rejection) while the **peak concurrency
  observed on postgres stays capped below the number launched** (a peak equal to
  the launch count would mean the pool never limited). Concurrency is measured
  through a side connection straight to postgres — bypassing the saturated pool —
  that samples `pg_stat_activity` for active `pg_sleep` backends. Observed peak:
  exactly 2.
- `TestPoolExhaustionWaiterCancelledCleanly` — saturates the pool with long
  holders, then issues one more statement with an 800 ms deadline. That statement
  must **block on the waitlist and fail on its deadline** (elapsed ≈ the full
  deadline proves it queued rather than being served or rejected); then, after
  the holders release, a fresh statement must **succeed** — proving the
  cancelled waiter was removed from the waitlist cleanly, leaking no slot and
  causing no deadlock.

**Group 4 — reserved-connection lifecycle & timeout.** Tests in
`reserved_conn_test.go`, ported from `test_timeouts.py` (the idle-in-transaction
killer). The timeout tests use `NewIsolated` clusters started with a short
`--connpool-user-reserved-inactivity-timeout=2s` via
`shardsetup.WithMultipoolerExtraArgs`. All observe pinning black-box via
`pg_backend_pid()`.

- `TestReservedConnectionInactivityTimeout` — opens a transaction (pinning a
  backend), idles past the inactivity timeout so the reaper kills the pinned
  backend, and asserts the session's next statement **fails cleanly** (a returned
  error within the deadline, not a hang) — multigres's idle-in-transaction
  killer. It then asserts the session **recovers**: after a ROLLBACK a fresh
  statement succeeds on a pooled backend, proving the reaped reservation was
  reclaimed rather than wedging the session.
- `TestReservedConnectionSurvivesWithActivity` — the inverse control: a
  transaction issuing a statement every second (faster than the 2s timeout) is
  **never reaped**, and because it is pinned every statement runs on the **same
  backend**. Proves the timeout is inactivity-based (reset by activity).
- `TestTransactionPinsBackendThenReleases` — the pin/release boundary, on the
  shared cluster under pool churn: inside the transaction every statement stays
  on **one backend** despite churn that rotates the pool (the inverse of Group
  1's pooled rotation); after COMMIT the session rejoins the pool and its
  statements **rotate across ≥2 backends** again, confirming the reservation
  ended at COMMIT.

**Group 5 — cancel-race under pooling.** Test in `cancel_race_test.go`, ported
from `test_cancel.py::test_cancel_race`. PgBouncer's worry is that a
CancelRequest for client A could cancel a query client B now runs on a backend A
released — because in raw postgres the cancel key maps to a _backend_. Multigres
routes cancels by the _client↔gateway connection_ (the gateway's synthetic
BackendKeyData + per-connection secret; `multigateway/{cancel.go,listener.go}`),
so the backend a query borrows is never the cancel's addressing unit and the
reuse race is structurally absent.

- `TestCancelForReleasedBackendDoesNotHitReuser` — runs in a `NewIsolated`
  cluster with a capacity-1 regular pool (`--connpool-global-capacity=2
--connpool-reserved-ratio=0.5`) so backend reuse is forced. Client A runs a
  statement then idles (releasing the single backend); client B starts a long
  statement that **provably reuses A's exact backend** (asserted via equal
  `pg_backend_pid()`); a stale out-of-band CancelRequest for A is then delivered
  (raw socket, same machinery as `query_cancel_test.go`). B finishes
  **uncancelled**, proving A's cancel hit only A's idle connection. A then stays
  usable (its cancel was a harmless no-op).

The secondary "cancel while pool exhausted" case from the triage is **not**
ported here: cancelling a pool-blocked waiter is already covered by Group 3's
`TestPoolExhaustionWaiterCancelledCleanly` (via context cancellation), and
whether an out-of-band CancelRequest interrupts a query still _queued_ for a slot
(vs. one executing on a backend) is a separate multigres-cancel-semantics
question left for its own investigation.

**Group 6 — COPY under fault.** Test in `copy_fault_test.go`, ported from
`test_copy.py`. COPY's happy paths (formats, TO STDOUT, transactions,
multi-statement, error recovery) are already covered by `copy_test.go`, so the
net-new delta is the fault path. Runs in a `NewIsolated` cluster.

- `TestCopyInterruptedByBackendCrash` — streams a large `COPY FROM STDIN` (a
  reader generating rows on demand, so CopyData keeps flowing), hard-kills
  postgres mid-stream with `KillPostgres`, and asserts: the COPY **fails cleanly**
  (a returned error within a deadline, not a hang or torn stream — observed as a
  closed-connection write error, since a crashed reserved-COPY backend tears down
  the client connection), the proxy **recovers** so a fresh connection serves
  queries again after the monitor restarts postgres, and the interrupted COPY
  **committed nothing** (the durable pre-crash table survives crash recovery with
  zero rows — COPY FROM is all-or-nothing). Note this differs from Group 2's
  regular-query case, where the same client connection survived a backend crash;
  a reserved COPY stream does not.

The triage's "COPY in pipeline/extended mode" sub-item is not ported — extended
COPY happy-path sequencing is part of the existing `copy_test.go` /
`extended_query_protocol_test.go` coverage, and the net-new value here is the
fault path above.

**Group 7 — small differential deltas (PG-as-oracle).** Tests in
`nonexistent_db_test.go`, ported from `test_no_database.py`. These fit the
differential model (run against both `GetComparisonTargets`) and use the shared
cluster (no fault injection).

- `TestNonexistentDatabaseConnectBehavior` — documents and guards a real
  **divergence**: direct PostgreSQL validates the catalog during startup and
  rejects a nonexistent database at connect time with `3D000`
  (invalid_catalog_name), whereas the multigateway authenticates the client and
  never validates the requested database — so connecting to `no_such_db_xyz`
  succeeds and `SELECT current_database()` returns the served `postgres`. The
  multigateway branch asserts that current behavior, so if gateway-side database
  validation is later added the test flags it for an intended update.
- `TestAuthErrorPrecedesDatabaseError` — the matching case: with a wrong password
  **and** a nonexistent database, both targets report the auth failure (`28P01`)
  at connect time, confirming authentication is checked before the database is
  resolved on both PostgreSQL and the gateway.

**⚠️ Divergence worth a maintainer's eye:** the gateway accepting an unknown
connection database and silently serving its backend database (rather than
rejecting with `3D000`) means a client asking for database `X` can be served
`postgres`. This may be an artifact of the single-database test topology /
passthrough routing; it is captured by the test above but may warrant a separate
look at whether the gateway should validate the connection database.

- **Verified:** all seven groups pass at `go test -count=3` (Group 1 and Group 7
  differential cases on both the postgres and multigateway targets); pgx sessions
  rotate across 5–8 backends within the first ~40 executes; killed backends
  recover and the same client session resumes on a fresh backend pid; a 12-wide
  load peaks at 2 concurrent backends with all statements succeeding, and a
  cancelled pool waiter leaves the pool healthy; an idle pinned backend is reaped
  and the session recovers while an active one stays pinned to a single backend; a
  reused backend is unaffected by the previous owner's cancel; a COPY hard-killed
  mid-stream fails cleanly, recovers, and commits nothing; a nonexistent database
  yields `3D000` on direct PG while the gateway diverges (documented), and auth
  errors precede database errors on both; `go vet` / golangci-lint clean.
- **Not started:** none — the (A) suite is complete.

## (B) Out of scope — rationale

- **Admin console** (`test_admin.py`, admin parts of `test_operations.py`): no
  `PAUSE`/`RESUME`/`SHOW`/`RELOAD` surface on multigateway or multipooler.
- **Auth machinery** (`test_auth.py`, `test_no_user.py` mock-auth): no
  userlist/HBA file, `auth_query`, `auth_dbname`, or mock-auth — auth is passed
  through to postgres.
- **Replication passthrough** (`test_replication.py`): gateway stubs replication
  commands with `feature_not_supported`. Revisit when implemented.
- **Peering / load-balance-hosts** (`test_peering.py`,
  `test_load_balance_hosts.py`): no `SO_REUSEPORT` peering or static server
  host-list; routing is topology-driven.
- **Live reconfiguration** (TLS SIGHUP toggles, `RELOAD`-driven limit changes):
  config is fixed at process start.
- **PgBouncer config knobs** (`fast_close`, `wait_close`, `auto_database`,
  `host_list`, `track_extra_parameters`, `query_wait_notify`): PgBouncer-specific
  with no multigres setting.

## (C) Already covered — do not duplicate

Mapped above. The existing suite (`go/test/endtoend/queryserving/`) already
covers, with differential PG-as-oracle where applicable: transactions, prepared
statements (happy path), query cancel (incl. gRPC-TLS forwarding),
LISTEN/NOTIFY, COPY (formats/txn/multi-statement/error-recovery), session
settings, statement timeout, the full SSL matrix, startup params/PGOPTIONS,
SCRAM auth, error/notice formatting, replica reads, replication stubs, failover
buffering, postgres crash recovery, and extended-protocol sequencing. Port only
the **delta** (a new assertion across a swap or under a fault), ideally as added
cases in the existing file rather than a parallel file.

## Differential vs. scenario testing

Unlike `pgproto`/`sqllogictest`, **most of this suite is not corpus-driven
differential**. The pooling/resilience scenarios are assertions about proxy
_behavior under fault_ (reconnect, block-then-proceed, clean error), not
statement-result diffs — so the `suiteutil` corpus/patch/`results.json` model
mostly does **not** apply here. Recommendation:

- Use plain Go test assertions (testify) for the scenario + fault tests, built on
  `shardsetup` + the pgprotocol/pgx/lib-pq clients + the fault helpers.
- Reserve `suiteutil` differential reporting + `.patch` known-divergence files
  **only** for the small differential deltas (group 7 above), where PG is a true
  oracle. If we add no corpus-driven cases, we don't need the patch machinery —
  and we should say so rather than bolt it on for its own sake.

This is an honest deviation from the original spec's assumption that the suite is
corpus-driven; flagging it for review.

## Harness reuse (do not reinvent)

- **Cluster:** `shardsetup.New(t, shardsetup.WithMultipoolerCount(2),
shardsetup.WithMultigateway())`; `NewSharedSetupManager` +
  `RunTestMain(m)` in `TestMain`; `SetupTest(t)` per test — copy
  `queryserving/main_test.go`. Use `NewIsolated(t, ...)` for tests that kill the
  primary/backend so cleanup is clean.
- **Clients:** `go/common/pgprotocol/client` for adversarial/cancel sequences;
  `pgx/v5` or `database/sql`+`lib/pq` for SQL-level assertions. Cancellation
  follows `query_cancel_test.go` (out-of-band CancelRequest).
- **Faults:** `setup.KillPostgres`, `StopPostgres(...) (resume)`,
  `ShutdownPostgres`, `ProcessInstance.StopPostgresImmediate` /
  `TerminateGracefully`, `GetPgctld`/`GetMultipooler`, `StopEtcd`,
  `DisableRecovery`/`EnableRecovery`/`TriggerRecoveryOnce`. Pattern reference:
  `postgres_crash_recovery_test.go`.
- **Differential targets (group 7):** `setup.GetComparisonTargets(t)` →
  `[]TestTarget{{"postgres",...},{"multigateway",...}}`; creds via
  `shardsetup.DefaultTestUser`, `TestPostgresPassword`, `GetTestUserDSN`.
- **Reporting (only if corpus-driven cases land):** `suiteutil` `report.go` /
  `diffreport.go` / `corpus.go` / `patch.go` and `ResetPublicSchema` /
  `NewSchemaResetterWithCleanup` / `RunWithTimeout`.

## Harness gaps to close

**Closed.** The connpool-tuning gap is resolved: `shardsetup` now exposes
`WithMultipoolerExtraArgs(...)`, which appends arbitrary flags to every
multipooler (mirroring the existing `MultigatewayExtraArgs` plumbing in
`shardsetup/setup.go`). Group 3 uses it for `--connpool-global-capacity`,
`--connpool-reserved-ratio`, and `--connpool-rebalance-interval`; group 4 uses
it for `--connpool-user-reserved-inactivity-timeout`. Targeted convenience
wrappers (`WithConnpoolCapacity(n)` /
`WithReservedInactivityTimeout(d)`) were deemed unnecessary — the generic
extra-args option is enough.

## Gating, CI, and how to run

This suite runs **directly in the standard e2e integration framework**, not
behind the extended-query-serving label. The package lives under
`go/test/endtoend/queryserving/pgbouncertests/`, so the
**`.github/workflows/test-integration.yml`** job — which runs
`gotestsum --packages="./go/test/endtoend/..."` on every PR and push to `main`
— picks it up automatically. No `suiteutil.SkipUnlessEnabled` /
`RUN_EXTENDED_QUERY_SERVING_TESTS` gate, no PR label, and no separate
pgbouncer-specific workflow. Tests only self-skip in `-short` mode or when the
PostgreSQL binaries are absent (`utils.ShouldSkipRealPostgres`), matching the
rest of the e2e suite.

This decision was made because the suite's value is core proxy behavior
(pooling, prepared-statement relay, fault recovery) that should be exercised on
every change, not gated behind an opt-in label that only runs on cron or
demand. The fault-injection groups (2–6) kill/restart backends, so keep using
`shardsetup.NewIsolated` for those individual tests to keep cleanup contained
within the shared-cluster package.

Run locally via the project conventions (use the `/mt-dev` skill; integration
tests build first). Do not run `go test` directly per repo policy.

## Review decisions

The triage was reviewed; the following are settled:

1. **Name/scope:** the suite lives in a new package, **`pgbouncertests`**, to
   make the provenance and ISC attribution explicit (rather than folding cases
   into the existing files).
2. **Differential vs. scenario:** approved — use plain Go assertions for the
   scenario + fault tests, and reserve the `suiteutil` corpus/patch model only
   for the small differential deltas (group 7).
3. **Harness option:** approved — add the
   `WithMultipoolerExtraArgs`/connpool option to `shardsetup` as part of this
   work (see [Harness gaps to close](#harness-gaps-to-close)).
4. **Replication (B):** confirmed — defer all of `test_replication.py` until
   replication passthrough is implemented; we'll add tests when we add support.

## Attribution and license

The (A) scenarios are ported from [PgBouncer's test suite][pgbouncer-test],
which is distributed under the **ISC License** (Copyright © 2007-2009 Marko
Kreen, Skype Technologies OÜ). We re-implement the scenarios in Go rather than
copying code, but because they are a derivative of an ISC-licensed work we
reproduce the upstream license, following the same in-repo convention used for
other vendored third-party code (e.g. `go/common/cache/theine/LICENSE`):

- A `LICENSE` file in this directory holds the verbatim PgBouncer ISC license,
  which applies to the derived test scenarios.
- All new code and modifications in this directory (the Go re-implementation and
  harness) are part of Multigres and licensed under the **Apache License,
  Version 2.0** — the repository-root `LICENSE`.
- Each ported `.go` file carries the standard multigres Apache-2.0 header (the
  `check-license` hook requires a `Copyright … Supabase, Inc.` line) **plus** an
  upstream-attribution line crediting PgBouncer's ISC copyright — mirroring the
  multi-copyright header pattern in `theine`'s Go files. For example:

  ```go
  // Copyright 2026 Supabase, Inc.
  // Portions derived from PgBouncer (ISC License),
  // Copyright (c) 2007-2009 Marko Kreen, Skype Technologies OÜ.
  //
  // Licensed under the Apache License, Version 2.0 (the "License");
  // ... (standard Apache-2.0 header) ...
  ```

[pgbouncer-test]: https://github.com/pgbouncer/pgbouncer/tree/master/test

</content>
</invoke>
