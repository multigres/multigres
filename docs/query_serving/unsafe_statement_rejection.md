# Unsafe Statement Rejection

## Overview

The multigateway rejects SQL statements that are unsafe to execute
through a hosted connection pooler. Because we have a full PostgreSQL
parser, we can inspect every statement at plan time and block constructs
that contain code the pooler cannot verify or that compromise hosted
infrastructure.

Blocked statements return a PostgreSQL `feature_not_supported` error
(SQLSTATE `0A000`) with a descriptive message explaining why the
statement is rejected.

## Background

A connection pooler sits between clients and PostgreSQL, routing queries
and managing backend connections. For correct routing, transaction
tracking, and security, the pooler needs to understand what each query
does. Certain SQL constructs break this contract:

1. **Embedded code** — Statements like `DO $$ ... $$` and
   `CREATE FUNCTION ... AS $$ ... $$` contain PL/pgSQL (or other
   language) code that the pooler's SQL parser never sees. This code
   can modify session state, start transactions, or execute arbitrary
   SQL — all invisible to the pooler.

2. **Query rewriting** — `CREATE RULE` and `CREATE TRIGGER` can cause
   the actual SQL executed by PostgreSQL to differ from the SQL the
   pooler planned. A rule can silently replace an `INSERT` with a
   `DELETE`. The pooler's routing decision was based on a query that
   isn't what ran.

3. **Infrastructure operations** — Statements like `LOAD`, `ALTER
SYSTEM`, `CREATE DATABASE`, and `CREATE LANGUAGE` are server-level
   operations that should not be available through a shared pooler in
   a hosted environment.

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
planning pipeline, we can add this safety layer without the
limitations of protocol-level-only proxies.

## Blocked Statements

Statements are blocked at plan time, before any query reaches
PostgreSQL. Both the simple query protocol (`Plan()`) and the extended
query protocol (`PlanPortal()`) enforce the same restrictions.

### Tier 1: Statements Containing Unparsed Code

These statements embed code in procedural languages (PL/pgSQL, PL/Python,
etc.) that the pooler's SQL parser cannot inspect or verify.

| Statement                              | AST Node                | Example                                                            | Reason                                                                                      |
| -------------------------------------- | ----------------------- | ------------------------------------------------------------------ | ------------------------------------------------------------------------------------------- |
| `DO $$ ... $$`                         | `T_DoStmt`              | `DO $$ BEGIN EXECUTE 'DROP TABLE t'; END $$`                       | Anonymous code blocks execute arbitrary SQL invisible to the pooler                         |
| `CREATE FUNCTION` / `CREATE PROCEDURE` | `T_CreateFunctionStmt`  | `CREATE FUNCTION f() RETURNS void AS $$ ... $$ LANGUAGE plpgsql`   | Function and procedure bodies contain opaque code                                           |
| `CREATE TRIGGER`                       | `T_CreateTriggerStmt`   | `CREATE TRIGGER t BEFORE INSERT ON users EXECUTE FUNCTION f()`     | Triggers fire opaque functions on data events; pooler cannot predict side effects           |
| `CREATE RULE`                          | `T_RuleStmt`            | `CREATE RULE r AS ON INSERT TO t DO INSTEAD DELETE FROM t`         | Rules rewrite queries at the database level — the query the pooler planned is not what runs |
| `CREATE EVENT TRIGGER`                 | `T_CreateEventTrigStmt` | `CREATE EVENT TRIGGER t ON ddl_command_start EXECUTE FUNCTION f()` | System-wide DDL hooks that execute opaque code                                              |

### Tier 2: Unsafe for Hosted Infrastructure

These statements perform server-level operations that should not be
available through a shared connection pooler. Most require superuser
privileges in PostgreSQL, so blocking them is defense-in-depth.

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

## Allowed Statements With Risk

Some statements execute opaque server-side code but cannot be blocked
without breaking most applications. These are allowed through the
pooler.

| Statement                 | AST Node                | Risk                                             | Why Allowed                                                                                                                                                  |
| ------------------------- | ----------------------- | ------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `CALL proc()`             | `T_CallStmt`            | Executes opaque procedure body                   | Stored procedures are fundamental to application logic. Blocking `CALL` would break every app that uses procedures.                                          |
| `CREATE EXTENSION`        | `T_CreateExtensionStmt` | Extensions install code and shared libraries     | Many extensions are essential for normal operation (`uuid-ossp`, `pgcrypto`, `pg_stat_statements`, `PostGIS`).                                               |
| Function calls in queries | N/A (expression-level)  | `SELECT my_func()` executes opaque function body | Function calls are embedded in expressions, not top-level statements. Blocking them would require deep expression walking and would break virtually all SQL. |

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
  +-- planUnsupportedStmt(stmt)     <-- rejection check
  |     |
  |     +-- blocked? --> return feature_not_supported error
  |     +-- allowed? --> continue
  |
  v
Statement-specific planning (switch on NodeTag)
  |
  v
Execution primitive (Route, Transaction, etc.)
```

### Key Files

| File                                                   | Purpose                                                                                               |
| ------------------------------------------------------ | ----------------------------------------------------------------------------------------------------- |
| `go/services/multigateway/planner/unsafe_stmt.go`      | `planUnsupportedStmt()` — switch on `NodeTag`, returns `feature_not_supported` for blocked statements |
| `go/services/multigateway/planner/planner.go`          | Calls `planUnsupportedStmt()` at the top of `Plan()` and `PlanPortal()`                               |
| `go/services/multigateway/planner/unsafe_stmt_test.go` | Tests for all blocked and allowed statement types                                                     |

### Error Format

Blocked statements return a standard PostgreSQL error response:

```
ERROR:  0A000: DO blocks are not supported: anonymous code blocks execute
unparsed queries that cannot be verified by the connection pooler
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

- **Configurable blocking**: Allow operators to customize which
  statements are blocked (e.g., allow `CREATE FUNCTION` for specific
  users or databases).
- **Notice for risky statements**: Emit a PostgreSQL `NoticeResponse`
  for Tier 3 statements (`CALL`, `CREATE EXTENSION`) to inform users
  that the pooler cannot verify the executed code.
- **Expression-level function scanning**: Walk the AST expression tree
  to detect calls to known-dangerous functions and reject queries that
  use them. Examples include `dblink_exec` (executes SQL on remote
  servers), `dblink` (opens connections to external databases),
  `lo_import`/`lo_export` (read/write server filesystem via large
  objects), `pg_read_file`/`pg_read_binary_file` (read arbitrary server
  files), `pg_execute_server_program` (run shell commands), and
  `query_to_xml`/`query_to_json` (execute arbitrary SQL strings and
  return results as XML/JSON). The parser already produces `FuncCall`
  nodes in the AST — a walker over the expression tree can collect all
  function names and check them against a blocklist before routing.
