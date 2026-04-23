# Unsafe Statement Rejection

## Overview

The multigateway inspects every SQL statement at plan time and
classifies it into one of two tiers based on the kind of risk it
presents to a hosted connection pooler:

- **Tier 1 — statements embedding procedural code** (DO, CREATE
  FUNCTION / PROCEDURE, CREATE TRIGGER, CREATE RULE, CREATE EVENT
  TRIGGER). Risk is session-state changes _inside_ an opaque body.
- **Tier 2 — server-level infrastructure operations** (LOAD, ALTER
  SYSTEM, CREATE/DROP DATABASE, CREATE LANGUAGE, CREATE SUBSCRIPTION,
  CREATE FDW, CREATE SERVER). Risk is modifying the shared server
  process or crossing tenant boundaries.

The two tiers are handled differently because the mitigations are
different (see [Handling](#handling) below). In the current
implementation, **Tier 2 is blocked at plan time and Tier 1 is allowed
through pending deeper analysis**; the two tiers will continue to
diverge as follow-up layers land.

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

## Other Allowed Statements With Known Risk

Beyond Tier 1 (allowed pending body analysis), a few more statements
execute opaque server-side code and cannot be usefully blocked.

| Statement                                          | AST Node                | Risk                                                                                      |
| -------------------------------------------------- | ----------------------- | ----------------------------------------------------------------------------------------- |
| `CALL proc()`                                      | `T_CallStmt`            | Executes opaque procedure body; same risk class as Tier 1 once the procedure exists.      |
| `CREATE EXTENSION`                                 | `T_CreateExtensionStmt` | Extensions install shared code. Blocking breaks essential packages (`pgcrypto`, PostGIS). |
| Function calls in expressions (`SELECT my_func()`) | N/A (expression-level)  | Opaque function bodies; also the bypass for session-state changes via `set_config()`.     |

## Implementation

### Architecture

Rejection is implemented as a single check at the top of the planning
pipeline, before the main statement dispatch:

```text
Client SQL
  |
  v
Parser (ParseSQL) --> AST
  |
  v
Planner.Plan() / Planner.PlanPortal()
  |
  +-- planUnsupportedStmt(stmt)     <-- Tier 2 rejection check
  |     |
  |     +-- blocked (Tier 2)? --> return feature_not_supported error
  |     +-- allowed?            --> continue
  |
  v
Statement-specific planning (switch on NodeTag)
  |
  v
Execution primitive (Route, Transaction, etc.)
```

Tier 1 statements fall through to the default routing path today. When
the follow-up layers land, a `planTier1Stmt()` check will sit
alongside `planUnsupportedStmt()` and run its own body walker.

### Key Files

| File                                                   | Purpose                                                                                              |
| ------------------------------------------------------ | ---------------------------------------------------------------------------------------------------- |
| `go/services/multigateway/planner/unsafe_stmt.go`      | `planUnsupportedStmt()` — switch on `NodeTag`, returns `feature_not_supported` for Tier 2 statements |
| `go/services/multigateway/planner/planner.go`          | Calls `planUnsupportedStmt()` at the top of `Plan()` and `PlanPortal()`                              |
| `go/services/multigateway/planner/unsafe_stmt_test.go` | Tests for blocked (Tier 2) and allowed (Tier 1 + regular) statement types                            |

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

Two follow-up layers harden against session-state and code-execution
vectors that the current Tier 2 filter does not catch. Both are
tracked as separate tickets.

### Expression-level function walking

Walk the AST of every statement's expression trees and reject / route
calls to known-dangerous built-in functions. Examples:

- `set_config(name, value, is_local=false)` — changes session-level
  GUCs without going through a `VariableSetStmt`. The pooler's session
  state tracker never sees these today. Treat a literal call with
  `is_local=false` as equivalent to a `SET` statement (update tracked
  state), or reject calls with non-literal arguments.
- `dblink`, `dblink_exec`, `dblink_connect` — opens connections to
  external databases.
- `lo_import`, `lo_export`, `pg_read_file`, `pg_read_binary_file` —
  read/write server filesystem.
- `pg_execute_server_program` — runs shell commands on the server.
- `query_to_xml`, `query_to_json` — execute arbitrary SQL strings.

Implementation: a walker over `FuncCall` nodes in the expression tree,
checking the qualified name against a blocklist. Integrates with the
existing planner pipeline.

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
