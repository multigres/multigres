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
time; Tier 1 is allowed through pending deeper analysis**; the tiers
will continue to diverge as follow-up layers land.

Blocked statements return a PostgreSQL `feature_not_supported` error
(SQLSTATE `0A000`) with a descriptive message.

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

### Tier 1 — procedural-code statements (currently allowed)

| Statement                              | AST Node                | Example                                                            |
| -------------------------------------- | ----------------------- | ------------------------------------------------------------------ |
| `DO $$ ... $$`                         | `T_DoStmt`              | `DO $$ BEGIN EXECUTE 'DROP TABLE t'; END $$`                       |
| `CREATE FUNCTION` / `CREATE PROCEDURE` | `T_CreateFunctionStmt`  | `CREATE FUNCTION f() RETURNS void AS $$ ... $$ LANGUAGE plpgsql`   |
| `CREATE TRIGGER`                       | `T_CreateTriggerStmt`   | `CREATE TRIGGER t BEFORE INSERT ON users EXECUTE FUNCTION f()`     |
| `CREATE RULE`                          | `T_RuleStmt`            | `CREATE RULE r AS ON INSERT TO t DO INSTEAD DELETE FROM t`         |
| `CREATE EVENT TRIGGER`                 | `T_CreateEventTrigStmt` | `CREATE EVENT TRIGGER t ON ddl_command_start EXECUTE FUNCTION f()` |

**Why allowed today:** blocking all of these (earlier iteration of
this layer) broke real applications — migrations, ORMs, and the
proxy's own NOTICE/ERROR-forwarding tests — without actually closing
the session-state leak, since `SELECT set_config(...)` achieves the
same effect at the expression level.

**How they will eventually be handled:** body analysis. Rather than a
blanket rejection, the planner will parse the PL/pgSQL body (for DO
and CREATE FUNCTION LANGUAGE plpgsql) and walk `RuleStmt.Actions` /
trigger functions, rejecting only bodies that contain dangerous
literal statements (`SET`, `DISCARD`, `LISTEN`, `PERFORM
set_config(...)`, `PREPARE TRANSACTION`, etc.). Dynamic `EXECUTE`
with non-literal arguments stays as an acknowledged gap until the
connection-reset backstop lands.

See [Future Work](#future-work) for the follow-up tickets that close
Tier 1.

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

| Function(s)                                                        | Category                                | Action                                                                                                                                                                           |
| ------------------------------------------------------------------ | --------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `set_config(name, value, is_local)`                                | Session state                           | Bare `SELECT set_config(lit, lit, false)` rewrites to `SET`. Non-literal args rejected. `is_local=true` passes through (transaction-scoped). Embedded `is_local=false` rejected. |
| `dblink`, `dblink_exec`, `dblink_connect`, `dblink_connect_u`      | Outbound connections                    | Reject                                                                                                                                                                           |
| `pg_read_file`, `pg_read_binary_file`, `pg_ls_dir`, `pg_stat_file` | Filesystem read                         | Reject                                                                                                                                                                           |
| `lo_import`, `lo_export`                                           | Filesystem read/write via large objects | Reject                                                                                                                                                                           |
| `pg_execute_server_program`                                        | Shell execution                         | Reject                                                                                                                                                                           |
| `query_to_xml`, `query_to_xmlschema`, `query_to_xml_and_xmlschema` | Arbitrary SQL via XML helpers           | Reject                                                                                                                                                                           |
| `table_to_xml`, `table_to_xmlschema`, `table_to_xml_and_xmlschema` | Arbitrary SQL via XML helpers           | Reject                                                                                                                                                                           |
| `cursor_to_xml`, `cursor_to_xmlschema`                             | Arbitrary SQL via XML helpers           | Reject                                                                                                                                                                           |

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

**How `set_config` is handled.** The walker accepts
`set_config(literal, literal, false)` only when it sits directly as a
top-level SELECT target-list entry (possibly alongside other entries
or a FROM). Every accepted shape plans the same way:

```text
Sequence[silent ApplySessionState per call, Route(original SQL)]
```

The silent primitives update the tracker without emitting anything to
the client; the Route sends the unmodified query to PG, which executes
set_config normally and streams the result back (set_config column plus
any sibling target-list entries and table rows). This costs one PG
round-trip even for bare `SELECT set_config(...)`, which is a deliberate
simplicity choice over a fast-path that would bypass PG.

`set_config(..., true)` — transaction-scoped — is accepted in any
position but never tracked: it doesn't outlive the transaction, so
there's nothing for the pool to carry forward.

Anything else is rejected: `set_config` in a WHERE clause, nested inside
another function's arguments, inside a CTE, in a DEFAULT expression, in
INSERT/UPDATE — evaluation semantics in those positions (conditional,
per-row, or hidden in a subquery) can't be faithfully represented by a
SessionSettings update. Non-literal arguments are always rejected,
because the value we would need to track is unknown at plan time.

**Where it runs.** Inside the planner, in a single walk at the top of
`Plan()` that also covers the statement-level Tier 2 check. Running here
means the plan cache short-circuits the walk: a cached plan is by
construction safe. The normalizer is configured to preserve literals
inside `set_config(...)` calls (see `go/common/parser/ast/normalizer.go`)
so the walker still sees the original A_Const arguments despite
normalization happening earlier in the pipeline for caching.

**Out of scope for this layer.**

- Calls from inside PL/pgSQL bodies (DO, CREATE FUNCTION LANGUAGE
  plpgsql) — covered by Tier 1 body-analysis work, since our SQL
  parser doesn't see the body.
- Dynamic SQL (`EXECUTE 'SELECT '||var`) — inherently unanalyzable at
  parse time.

## Other Allowed Statements With Known Risk

Beyond Tier 1 (allowed pending body analysis), a few more statements
execute opaque server-side code and cannot be usefully blocked.

| Statement                                                  | AST Node                | Risk                                                                                                                                                      |
| ---------------------------------------------------------- | ----------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `CALL proc()`                                              | `T_CallStmt`            | Executes opaque procedure body; same risk class as Tier 1 once the procedure exists.                                                                      |
| `CREATE EXTENSION`                                         | `T_CreateExtensionStmt` | Extensions install shared code. Blocking breaks essential packages (`pgcrypto`, PostGIS).                                                                 |
| User-defined functions in expressions (`SELECT my_func()`) | N/A (expression-level)  | Opaque function bodies. The expression-level filter only blocks built-ins known to breach the pooler boundary; arbitrary user functions are out of scope. |

## Implementation

### Architecture

Both the statement-level Tier 2 filter and the expression-level walker
run at the top of `Planner.Plan()`, before dispatch. Running in the
planner (rather than in the executor) means a plan cache hit
short-circuits both checks: a cached plan is by construction safe.

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
Planner.Plan()
  |
  +-- planUnsupportedStmt(stmt)          <-- Tier 2 rejection check
  |     |
  |     +-- blocked? --> return feature_not_supported error
  |
  +-- inspectExpressionFuncCalls(stmt)   <-- expression-level filter
  |     |
  |     +-- blocklisted func?            --> return feature_not_supported
  |     +-- rogue set_config?            --> return feature_not_supported
  |     +-- tracked set_configs found    --> returned to dispatch
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

Tier 1 statements fall through to the default routing path today. When
the follow-up layers land, a `planTier1Stmt()` check will sit
alongside `planUnsupportedStmt()` and run its own body walker.

### Key Files

| File                                                       | Purpose                                                                                                     |
| ---------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------- |
| `go/services/multigateway/planner/unsafe_stmt.go`          | `planUnsupportedStmt()` — switch on `NodeTag`, returns `feature_not_supported` for Tier 2 statements        |
| `go/services/multigateway/planner/unsafe_funccall.go`      | `inspectExpressionFuncCalls()` — single walk that rejects blocklist and collects accepted set_config calls  |
| `go/services/multigateway/planner/select_stmt.go`          | `planSelectStmt()` — dispatches SELECT based on set_config metadata (bare / mixed / plain)                  |
| `go/services/multigateway/planner/planner.go`              | Runs both checks at the top of `Plan()` before dispatch                                                     |
| `go/common/parser/ast/normalizer.go`                       | Skips normalization inside `set_config(...)` so the planner still sees literal args under plan caching      |
| `go/services/multigateway/engine/apply_session_state.go`   | `NewApplySessionStateSilent()` — the tracker-only variant used inside a Sequence                            |
| `go/services/multigateway/engine/sequence.go`              | Sequence primitive used for mixed `SELECT set_config(...), * FROM t` — tracking step + route                |
| `go/services/multigateway/planner/unsafe_stmt_test.go`     | Tests for blocked (Tier 2) and allowed (Tier 1 + regular) statement types                                   |
| `go/services/multigateway/planner/unsafe_funccall_test.go` | Tests for expression-level blocklist, set_config accept / reject positions, bare vs mixed plan construction |

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

### Protocol Coverage

Both query protocols are covered:

- **Simple query protocol** (`Query` message / `'Q'`): `Plan()` calls
  `planUnsupportedStmt()` before the main dispatch switch.
- **Extended query protocol** (`Parse`/`Bind`/`Execute`): `PlanPortal()`
  calls `planUnsupportedStmt()` before checking whether the statement
  needs local handling.

## Future Work

One follow-up layer remains to harden against session-state and
code-execution vectors that the current filters do not catch.

### PL/pgSQL body analysis (closes Tier 1)

Port PostgreSQL's PL/pgSQL parser (`src/pl/plpgsql/src/pl_gram.y`,
`pl_scanner.c`) so we can walk the body of `DO` blocks and
`CREATE FUNCTION ... LANGUAGE plpgsql` to detect literal `SET`,
`DISCARD`, `LISTEN`, `RESET`, `PREPARE TRANSACTION`, and
`PERFORM set_config(...)` uses. Combined with the expression walker
above, this closes the literal-text case.

Known gap (documented, not closed by this work): dynamic
`EXECUTE 'SET '||var` cannot be analyzed at parse time. Options under
consideration:

- reject any `EXECUTE` with a non-literal argument (most migrations
  don't need it), or
- rely on connection-reset on client handoff (`DISCARD ALL`) as the
  backstop.
