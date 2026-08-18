# Unsafe Statement Rejection

## Overview

The multigateway inspects every SQL statement at plan time and
classifies it into one of two statement-level tiers, plus an
expression-level filter that runs across all tiers:

- **Tier 1 — statements embedding procedural code** (DO, CREATE
  FUNCTION / PROCEDURE, CREATE TRIGGER, CREATE RULE, CREATE EVENT
  TRIGGER). Risk is session-state changes _inside_ an opaque body.
- **Tier 2 — server-level infrastructure operations** (LOAD, ALTER
  SYSTEM, CREATE/DROP DATABASE, CREATE LANGUAGE, CREATE SUBSCRIPTION,
  CREATE FDW, CREATE SERVER). Risk is modifying the shared server
  process or crossing tenant boundaries.
- **Expression-level filter — dangerous built-in function calls**
  (`set_config`, `dblink*`, `pg_read_file*`, `lo_import/export`,
  `pg_execute_server_program`, `{query,table,cursor}_to_xml*`).
  Risk is reaching the same Tier 2 capabilities (filesystem, outbound
  connections, shell, arbitrary SQL) or bypassing the pooler's
  session-state tracker (set_config).

The tiers are handled differently because the mitigations are
different (see [Handling](#handling) below). In the current
implementation, **Tier 2 and the expression-level filter block at plan
time, and Tier 1 is closed by body analysis** — the gateway parses a
`DO` / `CREATE FUNCTION` body and runs the expression-level filter over
every embedded fragment (see [Tier 1](#tier-1--procedural-code-statements-body-analysis)).

Blocked statements return a PostgreSQL `feature_not_supported` error
(SQLSTATE `0A000`) with a descriptive message.

An operator opt-out, `--unsafe-pooler-mode` (off by default), disables
**all** of these rejections for trusted, single-tenant deployments that
accept the risk; see [Unsafe pooler mode](#unsafe-pooler-mode).

## Background

A connection pooler sits between clients and PostgreSQL, routing
queries and managing backend connections. In a hosted multi-tenant
environment, a shared pooler must not expose operations that:

- change the server process itself (loaded shared libraries,
  `postgresql.conf`),
- cross tenant boundaries (creating or dropping databases,
  installing languages), or
- open outbound connections to external hosts (subscriptions,
  foreign data wrappers, foreign servers).

These are managed by the control plane, not by tenant users, even
when the tenant has the raw Postgres privileges to run them. That is
the Tier 2 concern.

The Tier 1 concern is narrower but real: procedural-language bodies
are opaque to our SQL parser, so a `DO $$ BEGIN SET work_mem = '10GB';
END $$` can change backend session state without the gateway's
session tracker ever seeing the `SET`. When that connection is
recycled to another client, the state leaks. The same leak vector
exists via `SELECT set_config(...)` at the expression level, so
Tier 1 is not the _only_ path — just one of several.

### Prior Art

Neither PgBouncer nor Supavisor implement statement-level blocking:

- **PgBouncer** operates at the wire protocol level and performs no
  SQL parsing at all. All statements are forwarded transparently to
  PostgreSQL. The only restriction is `disable_pqexec`, which disables
  the simple query protocol entirely (forcing clients to use the
  extended protocol). No statement types are blocked.

- **Supavisor** (Supabase's Elixir-based pooler) parses SQL via
  `pg_query.rs` for features like read-replica load balancing and
  query cancellation, but does not restrict any statement types. The
  primary documented limitation in transaction mode is that named
  prepared statements don't work (stateless model).

Because we have a full PostgreSQL parser integrated into the query
planning pipeline, we can add safety layers beyond what a
protocol-level-only proxy can do.

## Handling

The two tiers diverge because blocking outright has very different
cost/benefit profiles.

### Tier 1 — procedural-code statements (body analysis)

| Statement                              | AST Node                | Example                                                            |
| -------------------------------------- | ----------------------- | ------------------------------------------------------------------ |
| `DO $$ ... $$`                         | `T_DoStmt`              | `DO $$ BEGIN EXECUTE 'DROP TABLE t'; END $$`                       |
| `CREATE FUNCTION` / `CREATE PROCEDURE` | `T_CreateFunctionStmt`  | `CREATE FUNCTION f() RETURNS void AS $$ ... $$ LANGUAGE plpgsql`   |
| `CREATE TRIGGER`                       | `T_CreateTriggerStmt`   | `CREATE TRIGGER t BEFORE INSERT ON users EXECUTE FUNCTION f()`     |
| `CREATE RULE`                          | `T_RuleStmt`            | `CREATE RULE r AS ON INSERT TO t DO INSTEAD DELETE FROM t`         |
| `CREATE EVENT TRIGGER`                 | `T_CreateEventTrigStmt` | `CREATE EVENT TRIGGER t ON ddl_command_start EXECUTE FUNCTION f()` |

**Why not a blanket block:** blocking all of these (an earlier
iteration of this layer) broke real applications — migrations, ORMs,
and the proxy's own NOTICE/ERROR-forwarding tests — without actually
closing the session-state leak, since `SELECT set_config(...)` achieves
the same effect at the expression level. So Tier 1 is not rejected
wholesale; instead its embedded code is analyzed.

**How they are handled: body analysis.** For a statement that carries an
opaque procedural body — `DO` and `CREATE FUNCTION` / `PROCEDURE`
whose body is a dollar-quoted `AS '…'` string — the planner extracts
the body, parses it, and runs the same expression-level filter over
every embedded SQL fragment (`analyzeProceduralBody`, `unsafe_plpgsql.go`):

- **PL/pgSQL bodies** (`LANGUAGE plpgsql`, the default for `DO`) are
  parsed with the [PL/pgSQL parser](./plpgsql_parser.md). The body is
  walked (its generated `Rewrite` helper); each embedded `PLpgSQL_expr`
  fragment is turned into a SQL AST — a statement-mode fragment is
  parsed directly, a bare-expression fragment (an `IF`/`WHILE`
  condition, an assignment RHS, a `PERFORM`/`RETURN` value, …) is
  wrapped as `SELECT <expr>` — and run through the same
  `analyzeFunctionCalls` + restricted-GUC checks as top-level SQL.
- **SQL bodies** (`LANGUAGE sql`, the `AS '…'` string form) are a list
  of SQL statements; they are parsed and analyzed the same way, with no
  PL/pgSQL parser needed. The SQL-standard `BEGIN ATOMIC … END` body is
  already a parsed SQL tree that the top-level `FuncCall` walk reaches.
- **`LANGUAGE c` / `internal` bodies** are a symbol reference into a shared
  library or the server binary, not SQL — there is nothing session-state-shaped
  to hide, and creating one already requires the library to be present on the
  server (gated elsewhere: no `LOAD`, no `pg_read_file`, no filesystem writes
  through the pooler). So they are **allowed** — the pooler has nothing to
  analyze and no Tier 1 vector to close.
- **Other opaque procedural languages** (`plperl`, `plpython`, `pltcl`, …) are
  arbitrary code that can change backend session state we cannot observe, so
  they **fail closed** and are rejected.
- **`CREATE RULE` / `CREATE TRIGGER` / `CREATE EVENT TRIGGER`** carry
  their code as ordinary SQL AST (rule actions and qualifications,
  trigger `WHEN` expressions) that the top-level `analyzeFunctionCalls`
  walk already descends into; a trigger's referenced function was
  analyzed when it was created. They need no separate body extraction.

**Session-state changes inside a body are rejected, not tracked.** At
the top level an accepted `set_config` / `SET` is mirrored into the
gateway's session tracker. Inside a body it may run conditionally (in an
`IF`/`LOOP`/exception handler) or not at all, so the gateway cannot
faithfully reproduce it — the conservative choice is to reject any
`set_config`, `SET`, `RESET`, `DISCARD`, or `LISTEN`/`UNLISTEN` anywhere
in a body, matching how a top-level `set_config` in a disallowed
position is already rejected.

**Dynamic `EXECUTE`** (and the dynamic `RETURN QUERY EXECUTE`,
`FOR … IN EXECUTE`, `OPEN … FOR EXECUTE` forms): if the executed string
is a string literal it is parsed and analyzed exactly as if written
inline; if it is built at runtime (concatenation, `format()`, a
variable) it cannot be proven safe and is **rejected** — the same way a
non-literal `set_config` argument is rejected at the top level.

**Fail closed.** A body that does not parse, or that uses an unsupported
construct, is rejected rather than passed through. All of this is
disabled by [`--unsafe-pooler-mode`](#unsafe-pooler-mode).

### Tier 2 — infrastructure operations (blocked at plan time)

| Statement                     | AST Node                    | Example                                                                  | Reason                                                            |
| ----------------------------- | --------------------------- | ------------------------------------------------------------------------ | ----------------------------------------------------------------- |
| `LOAD`                        | `T_LoadStmt`                | `LOAD 'auto_explain'`                                                    | Loads shared libraries into the PostgreSQL server process         |
| `ALTER SYSTEM`                | `T_AlterSystemStmt`         | `ALTER SYSTEM SET max_connections = 1`                                   | Modifies `postgresql.conf` — can break the service                |
| `CREATE DATABASE`             | `T_CreatedbStmt`            | `CREATE DATABASE foo`                                                    | Structural server changes not permitted in a hosted environment   |
| `DROP DATABASE`               | `T_DropdbStmt`              | `DROP DATABASE foo`                                                      | Structural server changes not permitted in a hosted environment   |
| `CREATE LANGUAGE`             | `T_CreatePLangStmt`         | `CREATE LANGUAGE plpython3u`                                             | Installs procedural language runtimes (especially untrusted ones) |
| `CREATE SUBSCRIPTION`         | `T_CreateSubscriptionStmt`  | `CREATE SUBSCRIPTION s CONNECTION '...' PUBLICATION p`                   | Opens outbound replication connections to external servers        |
| `CREATE FOREIGN DATA WRAPPER` | `T_CreateFdwStmt`           | `CREATE FOREIGN DATA WRAPPER myfdw`                                      | Installs external data access layers                              |
| `CREATE SERVER`               | `T_CreateForeignServerStmt` | `CREATE SERVER s FOREIGN DATA WRAPPER postgres_fdw OPTIONS (host '...')` | Configures outbound connections to external hosts                 |

**Why blocked:** these are properties of the shared server process or
its outbound connections. Unlike Tier 1, there is no "inspect the
body" mitigation — the operation itself is the problem. In a hosted
environment the control plane owns these, not tenant users.

**No follow-up planned:** Tier 2 stays blocked. The only anticipated
change is a configurable allowlist for self-hosted deployments where
the operator explicitly opts into these.

### Expression-level filter — dangerous built-in function calls

The statement-level tiers above block whole statements. A second layer
walks every `FuncCall` in a statement's expression trees (SELECT target
list, WHERE, JOIN, INSERT values, DEFAULT expressions, CTEs, subqueries)
and enforces rules on specific built-ins. Schema-qualified variants
(`pg_catalog.dblink`, etc.) resolve to the same entry.

| Function(s)                                                                                                     | Category                                | Action                                                                                                                                                                  |
| --------------------------------------------------------------------------------------------------------------- | --------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `set_config(name, value, is_local)`                                                                             | Session state                           | Tracked as a `SET`, but only as a top-level SELECT target-list entry; args may be literal or bound. Other positions and `is_local` handled specially — see prose below. |
| `dblink*` (sync entry points + async-cursor surface: `_open`, `_fetch`, `_close`, `_send_query`, `_get_result`) | Outbound connections                    | Reject                                                                                                                                                                  |
| `pg_read_file`, `pg_read_binary_file`, `pg_ls_dir`, `pg_stat_file`                                              | Filesystem read                         | Reject                                                                                                                                                                  |
| `lo_import`, `lo_export`                                                                                        | Filesystem read/write via large objects | Reject                                                                                                                                                                  |
| `pg_execute_server_program`                                                                                     | Shell execution                         | Reject                                                                                                                                                                  |
| `query_to_xml`, `query_to_xmlschema`, `query_to_xml_and_xmlschema`                                              | Arbitrary SQL via XML helpers           | Reject                                                                                                                                                                  |
| `table_to_xml`, `table_to_xmlschema`, `table_to_xml_and_xmlschema`                                              | Arbitrary SQL via XML helpers           | Reject                                                                                                                                                                  |
| `cursor_to_xml`, `cursor_to_xmlschema`                                                                          | Arbitrary SQL via XML helpers           | Reject                                                                                                                                                                  |

**Why these specific functions:** each one is a callable equivalent of
a Tier 2 operation or a session-state bypass. `dblink` opens outbound
connections (same as CREATE SERVER / SUBSCRIPTION). `pg_read_file`,
`lo_import`, and `pg_execute_server_program` reach the server
filesystem or shell. `query_to_xml` executes arbitrary SQL strings
inside the engine. `set_config(name, value, false)` is functionally
equivalent to `SET name = value` but goes through the expression path,
so the gateway's `SessionSettings` tracker never sees it — on the next
query the pool picks a different backend connection and the state is
gone.

**How `set_config` is handled.** The walker accepts a `set_config` call
only when it sits directly as a top-level SELECT target-list entry
(possibly alongside other entries or a FROM). Every accepted shape plans
the same way:

```text
Sequence[silent ApplySessionState per call, Route(original SQL)]
```

The silent primitives update the tracker without emitting anything to
the client; the Route sends the unmodified query to PG, which executes
set_config normally and streams the result back (set_config column plus
any sibling target-list entries and table rows). This costs one PG
round-trip even for bare `SELECT set_config(...)`, which is a deliberate
simplicity choice over a fast-path that would bypass PG.

**Literal and bound arguments.** Each of the three slots (`name`,
`value`, `is_local`) may be a literal constant **or** a bound parameter
(`$1`), e.g. `SELECT set_config('search_path', $1, false)`. When any
slot is bound, the tracking primitive is built via
`NewApplySessionStateFromBind`, which records the `ParamRef`s and defers
per-slot resolution to execute time — `executeSetWithBinds` decodes the
actual values from the portal's wire-protocol Bind values before
updating `SessionSettings`. All-literal calls use the simpler
`NewApplySessionStateSilent`. (An earlier iteration rejected non-literal
args outright; bound parameters are now supported because PostgREST and
ORMs routinely parameterize the value.)

`set_config(..., true)` — transaction-scoped — is accepted in any
position but never tracked: it doesn't outlive the transaction, so
there's nothing for the pool to carry forward. (A _bound_ `is_local`
can't be short-circuited at plan time; the track-or-not decision is
deferred to `executeSetWithBinds`.)

Anything else is rejected: `set_config` in a WHERE clause, nested inside
another function's arguments, inside a CTE, in a DEFAULT expression, in
INSERT/UPDATE — evaluation semantics in those positions (conditional,
per-row, or hidden in a subquery) can't be faithfully represented by a
SessionSettings update. Expression-shaped arguments that are neither a
literal nor a bound parameter (a column reference, a nested function
call, a cast of a non-constant) are also rejected, because the value we
would need to track is unknown at plan time. `SELECT ... INTO` is
excluded as an allowed position too: that shape dispatches to the
reserved temp-table route, which would drop the tracked set_config and
leave the gateway's tracker stale relative to the backend.

**Where it runs.** Inside the planner, in a single walk that runs as
part of `planUnsupportedConstructs()` — the orchestrator that pairs the
statement-level check (`planUnsupportedStmt`, covering Tier 2) with
this expression-level walk (`inspectExpressionFuncCalls`). Both the
simple-protocol `Plan()` and
the extended-protocol `PlanPortal()` call `planUnsupportedConstructs()`,
so neither path can skip a check. Running in the planner means the plan
cache short-circuits the walk: a cached plan is by construction safe.
The normalizer is configured to preserve literals inside
`set_config(...)` calls (see `go/common/parser/ast/normalizer.go`) so the
walker still sees the original A_Const arguments despite normalization
happening earlier in the pipeline for caching.

**Out of scope for this expression walk** (handled elsewhere, not gaps):

- Calls from inside PL/pgSQL / SQL bodies (DO, CREATE FUNCTION) — reached
  by Tier 1 [body analysis](#tier-1--procedural-code-statements-body-analysis),
  which parses the body and routes each fragment back through this same
  walk. Our SQL parser does not see the opaque body text; the PL/pgSQL /
  SQL parse does.
- Dynamic SQL (`EXECUTE 'SELECT '||var`) built from a non-literal — the
  body analysis rejects it (it cannot be proven safe); a plain string
  literal is parsed and analyzed.

### Restricted GUC value guard

Separate from the tiers above (which block whole statement types or whole
functions), a narrow guard rejects any attempt to **assign a value** to a
cluster-managed GUC, regardless of the statement carrying it. The set is a
single map (`restrictedGUCs` in `restricted_guc.go`, mirroring `funcBlocklist`):
add a GUC name + reason there and every path below enforces it.

Currently the list has one entry, **`synchronous_commit`**. Replication
durability is managed centrally: the multipooler rule store / `SyncStandbyManager`
is the sole writer of `synchronous_commit` (via `ALTER SYSTEM`), and the HA
contract requires `synchronous_commit = on` so that an acknowledged commit is
durably flushed on the synchronous standby (see
`docs/ha/decision-log/2026-02-12-synchronous-commit-on.md`). A session that
lowers the value silently weakens that guarantee for its writes — a footgun, so
we take the choice away. See
`docs/ha/decision-log/2026-05-29-block-synchronous-commit-changes.md`.

The paths below use `synchronous_commit` as the example; the same rules apply to
any GUC added to `restrictedGUCs`.

| Path                                          | Action                                   |
| --------------------------------------------- | ---------------------------------------- |
| `SET synchronous_commit = x`                  | Reject                                   |
| `SET LOCAL synchronous_commit = x`            | Reject                                   |
| `SET synchronous_commit FROM CURRENT`         | Reject                                   |
| `ALTER DATABASE d SET synchronous_commit = x` | Reject                                   |
| `ALTER ROLE r SET synchronous_commit = x`     | Reject                                   |
| `set_config('synchronous_commit', x, _)`      | Reject (both `is_local` variants)        |
| `ALTER SYSTEM SET synchronous_commit = x`     | Already rejected (Tier 2)                |
| `RESET synchronous_commit`                    | **Allowed** — restores the managed value |
| `SET synchronous_commit TO DEFAULT`           | **Allowed** — restores the managed value |
| `RESET ALL`                                   | **Allowed** — restores the managed value |

Reverts are allowed because they can only restore the cluster-managed value.
The rejection is a `feature_not_supported` (`0A000`) error pointing users at
`RESET`.

**Where it runs.** In `checkRestrictedGUCChange`, called from
`planUnsupportedConstructs` alongside the Tier 2 and expression-level checks, so
it covers both query protocols and is short-circuited by the plan cache. The
`set_config(...)` form is enforced inside the same expression walker that tracks
set_config. So that the planner can inspect the variable name even for
transaction-scoped `set_config(..., true)`, the normalizer keeps that name
literal (only the value is parameterized for plan-cache stability).

**Known gap** (consistent with the rest of this layer): a `set_config` call (or
a `SET`) whose variable _name_ is itself non-literal is not matched against the
restricted-GUC list. Assignments inside PL/pgSQL bodies are now caught by Tier 1
body analysis, and a dynamic `EXECUTE` with a non-literal argument is rejected
there.

## Unsafe pooler mode

`--unsafe-pooler-mode` (config key `unsafe-pooler-mode`, env
`MT_UNSAFE_POOLER_MODE`; default **off**) is an operator opt-out for
trusted, single-tenant deployments that accept the risk. When enabled it
suppresses **every** rejection layer above:

- Tier 1 PL/pgSQL / SQL body analysis,
- the Tier 2 statement blocklist,
- the restricted-GUC guard, and
- the expression-level function blocklist (`dblink`, `pg_read_file`, …).

Only the _rejections_ are relaxed. The planning signals the same pass
gathers — tracked `set_config` calls, advisory-lock pinning,
`current_setting` rewrites — are still collected, so routing stays
correct; a blocklisted call or an untrackable `set_config` simply goes
to PostgreSQL as written instead of being rejected. (The
non-temporary-replication-slot check is a failover-correctness
constraint rather than a tenant-isolation rejection, so it stays
enforced regardless.)

The flag is read once at startup and applied to the planner
(`Planner.SetUnsafePoolerMode`). Because analysis runs on the plan-cache
miss path, a plan cached while the mode was one value is not
re-evaluated if the process is restarted with the other — the setting is
process-static by design.

## Other Allowed Statements With Known Risk

Beyond Tier 1 (allowed pending body analysis), a few more statements
execute opaque server-side code or mutate connection-start defaults and cannot
be usefully blocked wholesale.

| Statement                                                  | AST Node                                        | Risk                                                                                                                                                             |
| ---------------------------------------------------------- | ----------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `CALL proc()`                                              | `T_CallStmt`                                    | Executes opaque procedure body; same risk class as Tier 1 once the procedure exists.                                                                             |
| `CREATE EXTENSION`                                         | `T_CreateExtensionStmt`                         | Extensions install shared code. Blocking breaks essential packages (`pgcrypto`, PostGIS).                                                                        |
| `ALTER DATABASE ... SET` / `ALTER ROLE ... SET`            | `T_AlterDatabaseSetStmt` / `T_AlterRoleSetStmt` | Changes connection-start defaults; existing pooled backends keep old values. See [Connection-start defaults](#connection-start-defaults-alter-databaserole-set). |
| `CREATE EVENT TRIGGER ... ON login`                        | `T_CreateEventTrigStmt`                         | Login triggers fire when pooled backend connections are created, not for each client session. See [Login event triggers](#login-event-triggers).                 |
| User-defined functions in expressions (`SELECT my_func()`) | N/A (expression-level)                          | Opaque function bodies. The expression-level filter only blocks built-ins known to breach the pooler boundary; arbitrary user functions are out of scope.        |

### Connection-start defaults (`ALTER DATABASE/ROLE SET`)

PostgreSQL stores `ALTER DATABASE ... SET` and `ALTER ROLE ... SET` values in
catalogs such as `pg_db_role_setting` and applies them when a backend connection
is established. In a transaction pooler, backend connections are long-lived and
shared across client sessions. Changing a database or role default therefore has
a delayed and partial effect: new PostgreSQL backends see the new default, but
already-open pooled backends continue with the values they had at connection
startup.

Multigateway tracks explicit session-level `SET` / accepted `set_config(...)`
changes made through the gateway, but it does not currently detect arbitrary
catalog changes to role/database defaults and proactively refresh or replay those
defaults across the pool. A client can therefore observe mixed behavior after an
`ALTER DATABASE/ROLE SET` until the affected backend connections are closed and
reopened.

Current mitigations are operational/test-harness scoped rather than a general
product guarantee. For example, the PostGIS regression harness installs PostGIS
directly on the primary because `postgis_topology` runs `ALTER DATABASE <db> SET
search_path = ..., topology`; after that install it terminates existing client
backends so multipooler reconnects with the new topology-aware startup default.
A future connection-default refresh mechanism could make this automatic, but it
must preserve reserved-session state and transaction-pinning invariants.

### Login event triggers

PostgreSQL 17 login event triggers fire when a backend process starts a session. In Multigres connection pooling, PostgreSQL backends are created by the pooler and reused across client sessions, so `CREATE EVENT TRIGGER ... ON login` succeeds but its trigger fires only when the pooler opens a backend connection.

It is not re-run for each client login, and `RAISE NOTICE` output from the trigger is not delivered to client sessions.

The gateway emits a self-contained `WARNING`/`HINT` at `CREATE EVENT TRIGGER ... ON login` time so the changed semantics are visible when the trigger is created.

There is no faithful emulation available: event trigger functions return the `event_trigger` pseudo-type and cannot be invoked from ordinary SQL when a client attaches to a pooled backend.

## Implementation

### Architecture

Both the statement-level filter (Tier 2) and the
expression-level walker run before dispatch, bundled behind
`planUnsupportedConstructs()`. Both `Planner.Plan()` (simple protocol)
and `Planner.PlanPortal()` (extended protocol) call that orchestrator,
so neither query path can bypass a check. Running in the planner (rather
than in the executor) means a plan cache hit short-circuits both checks:
a cached plan is by construction safe.

```text
Client SQL
  |
  v
Parser (ParseSQL) --> AST (with A_Const literals)
  |
  v
Executor.StreamExecute / PortalStreamExecute
  |
  v
Executor normalizes literals (A_Const -> $N) for plan cache
  (skips inside set_config() calls so the planner still sees its literals)
  |
  v
Planner.Plan() / Planner.PlanPortal()
  |
  +-- planUnsupportedConstructs(stmt)    <-- orchestrates both checks
  |     |
  |     +-- planUnsupportedStmt(stmt)         <-- Tier 2 rejection check
  |     |     +-- blocked? --> return feature_not_supported error
  |     |
  |     +-- inspectExpressionFuncCalls(stmt)  <-- expression-level filter
  |           +-- blocklisted func?       --> return feature_not_supported
  |           +-- rogue set_config?       --> return feature_not_supported
  |           +-- tracked set_configs found --> returned to dispatch (Plan only)
  |
  v
Statement-specific planning (switch on NodeTag)
  |
  +-- SelectStmt with tracked set_configs?
  |     +-- any shape   --> Sequence[silent ApplySessionState..., Route]
  |     +-- none        --> Route
  |
  v
Execution primitive (Route, Transaction, ApplySessionState, Sequence, etc.)
```

A Tier 1 statement with an opaque body is checked by `analyzeProceduralBody()`
in the same pre-dispatch `analyzeStatement()` pass; if its body reaches an
unsafe construct the statement is rejected, otherwise it falls through to
the default routing path and executes normally.

### Key Files

| File                                                       | Purpose                                                                                                                                                                                                      |
| ---------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `go/services/multigateway/planner/unsafe_stmt.go`          | `planUnsupportedStmt()` — switch on `NodeTag`, returns `feature_not_supported` for Tier 2 statements                                                                                                         |
| `go/services/multigateway/planner/unsafe_funccall.go`      | `analyzeStatement()` (pre-dispatch analysis pass) + `analyzeFunctionCalls()` — single walk that rejects blocklist and collects accepted (literal or bound) set_config calls                                  |
| `go/services/multigateway/planner/unsafe_plpgsql.go`       | `analyzeProceduralBody()` — extracts a DO / CREATE FUNCTION body, parses it (PL/pgSQL or SQL), and runs the expression-level filter over every embedded fragment; dynamic-EXECUTE and opaque-language policy |
| `go/services/multigateway/planner/restricted_guc.go`       | `restrictedGUCs` map + `checkRestrictedGUCChange()` — rejects value assignments to cluster-managed GUCs across SET / ALTER ROLE / ALTER DATABASE                                                             |
| `go/services/multigateway/planner/select_stmt.go`          | `planSelectStmt()` — dispatches SELECT based on set_config metadata; picks silent vs from-bind ApplySessionState                                                                                             |
| `go/services/multigateway/planner/planner.go`              | Calls `planUnsupportedConstructs()` at the top of both `Plan()` and `PlanPortal()` before dispatch                                                                                                           |
| `go/common/parser/ast/normalizer.go`                       | Skips normalization inside `set_config(...)` so the planner still sees literal args under plan caching                                                                                                       |
| `go/services/multigateway/engine/apply_session_state.go`   | `NewApplySessionStateSilent()` (literal) and `NewApplySessionStateFromBind()` (`executeSetWithBinds` defers slot resolution to portal Bind values)                                                           |
| `go/services/multigateway/engine/sequence.go`              | Sequence primitive used for mixed `SELECT set_config(...), * FROM t` — tracking step + route                                                                                                                 |
| `go/services/multigateway/planner/unsafe_stmt_test.go`     | Tests for blocked (Tier 2) and allowed (Tier 1 + regular) statement types                                                                                                                                    |
| `go/services/multigateway/planner/unsafe_funccall_test.go` | Tests for expression-level blocklist, set_config accept / reject positions, literal + bound args, bare vs mixed plan construction                                                                            |
| `go/test/endtoend/queryserving/unsafe_stmt_test.go`        | End-to-end coverage through a real multigateway → PostgreSQL: Tier 2, simple + extended protocol, in-transaction                                                                                             |

### Error Format

Blocked statements return a standard PostgreSQL error response:

```text
ERROR:  0A000: LOAD is not supported: loading shared libraries is not
permitted through the connection pooler
```

The SQLSTATE `0A000` (`feature_not_supported`) was chosen because:

- It's the standard PostgreSQL code for unimplemented features
- Client libraries and ORMs handle it gracefully
- It clearly communicates that this is a pooler limitation, not a
  syntax error

### Transaction behavior

A rejection does **not** abort the surrounding transaction. The gateway
raises the error itself — before the statement ever reaches a backend — so
the backend's transaction is untouched: it stays in whatever state it was
in, and the client can keep issuing statements or `COMMIT`. This is the
opposite of an ordinary backend error, which PostgreSQL raises on the
backend and which poisons the transaction (`25P02`, `current transaction is
aborted, commands ignored until end of transaction block`) until a
`ROLLBACK`.

Concretely, in

```sql
BEGIN;
LOAD 'somelib';                    -- rejected (0A000)
INSERT INTO audit VALUES ('ok');   -- still runs
COMMIT;                            -- succeeds
```

the `INSERT` runs and the `COMMIT` commits; the client sees only the single
`0A000` error for the `LOAD`. Were the rejection allowed to poison the
transaction, every following statement would instead fail with "current
transaction is aborted" — a cascade that buries the one real signal.

This is implemented by tagging every refusal as a `*GatewayRejection`
(`NewFeatureNotSupported` returns one; see `mterrors/pgdiagnostic.go`) and
skipping the `in_block → failed` transaction-status transition whenever
`mterrors.IsGatewayRejection(err)` holds — the guard on each
`SetTxnStatus(TxnStatusFailed)` call in `handler.go` and
`transaction_helpers.go`. It applies to every rejection layer below (Tier 1,
Tier 2, the restricted-GUC guard, and the dangerous-function filter), since
all of them raise `feature_not_supported` through the same constructor.

### Protocol Coverage

Both query protocols are covered, and both call the same
`planUnsupportedConstructs()` orchestrator (statement-level check +
expression walk) so the statement-level and expression-level rejections
apply identically:

- **Simple query protocol** (`Query` message / `'Q'`): `Plan()` calls
  `planUnsupportedConstructs()` before the main dispatch switch.
- **Extended query protocol** (`Parse`/`Bind`/`Execute`): `PlanPortal()`
  calls `planUnsupportedConstructs()` before checking whether the
  statement needs local handling.

## Future Work

The PL/pgSQL body-analysis layer that closed Tier 1 has landed (see
[Tier 1](#tier-1--procedural-code-statements-body-analysis)). The
remaining hardening is around dynamic SQL the body analysis
deliberately rejects rather than emulates:

- A dynamic `EXECUTE 'SET '||var` (a non-literal argument) is **rejected
  today**. A future connection-reset backstop (`DISCARD ALL` on client
  handoff) could let such statements through safely instead of rejecting
  them, for deployments that want the looser behavior without
  `--unsafe-pooler-mode`.
- Advisory locks, temp tables, and session state acquired via dynamic
  SQL the parser cannot see remain a pre-existing pooling limitation,
  the same as at the top level.
- Mutable process-global state maintained by C extensions (now that
  `LANGUAGE c` / `internal` definitions are allowed) is outside the
  gateway-authoritative model entirely; see
  [`session_settings.md`](./session_settings.md) for that known limitation.
