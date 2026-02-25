# Statement Timeout

## Overview

Multigres enforces query execution timeouts at the multigateway layer
to protect the connection pool from runaway queries. The gateway owns
session state and SQL parsing, making it the natural place to resolve
timeouts from multiple sources (per-query directives, session
variables, server flags). Context deadlines propagate through gRPC to
the multipooler, which already has infrastructure to cancel PostgreSQL
backend queries.

Key capabilities:

- `--statement-timeout` flag sets a server-wide default (default: 30s)
- `SET statement_timeout` / `RESET statement_timeout` per-session
  overrides, managed entirely at the gateway (not forwarded to
  PostgreSQL)
- Per-query directives via SQL comments (future:
  `/*mg+ STATEMENT_TIMEOUT_MS=500 */`)
- Context deadline propagation through gRPC to multipooler
- PostgreSQL-compatible error codes (SQLSTATE `57014`) and `SHOW`
  output (milliseconds)

## Timeout Priority

When multiple timeout sources apply, the highest-priority source wins.
A value of `0` means "no timeout" (unlimited).

```text
1. Per-query SQL comment directive      (highest priority, future)
2. Session variable (SET statement_timeout)
3. Multigateway --statement-timeout flag
4. Multipooler --statement-timeout flag  (lowest priority, future)
```

Examples:

```sql
-- Server defaults: gateway flag = 30s
SELECT * FROM users;
-- Effective timeout: 30s (flag default)

-- Session override
SET statement_timeout = 5000;  -- 5 seconds, in milliseconds
SELECT * FROM users;
-- Effective timeout: 5s (session variable)

-- Explicitly disable timeout for a known-slow query (future)
SELECT /*mg+ STATEMENT_TIMEOUT_MS=0 */ * FROM huge_table;
-- Effective timeout: unlimited (directive=0 disables)
```

## Architecture

```text
Client
  |
  v
MultiGateway Handler (handler.go)
  |  - getConnectionState(): initializes timeout default from
  |    startup params or --statement-timeout flag
  |  - executeWithTimeout(): resolves effective timeout, wraps
  |    ctx with context.WithTimeout, translates DeadlineExceeded
  |    to SQLSTATE 57014
  |
  v
Planner (variable_set_stmt.go, variable_show_stmt.go)
  |  - isGatewayManagedVariable("statement_timeout") -> true
  |  - SET: parses value at plan time, creates GatewaySessionState
  |  - SHOW: creates GatewayShowVariable (no PG round-trip)
  |  - RESET: creates GatewaySessionStateReset
  |
  v
Engine Primitives
  |  - GatewaySessionState: stores parsed duration in connection state
  |  - GatewayShowVariable: reads effective value, returns as milliseconds
  |  - ApplySessionState: RESET ALL also resets statement_timeout
  |
  v
gRPC to Multipooler (deadline in context metadata)
  |
  v
Multipooler (regular_conn.go)
     - execWithContextCancel: selects on ctx.Done()
     - On cancellation: CancelBackend(pid) via admin pool
     - Returns connection to pool or taints on error
```

## Gateway-Managed Variables

`statement_timeout` is the first "gateway-managed variable" — a
session variable that the gateway intercepts and does NOT forward to
PostgreSQL. This avoids polluting pooled backend connections with
per-client timeouts and allows the gateway to enforce timeouts via
context deadlines rather than relying on PostgreSQL's internal timer.

The `GatewayManagedVariable[T]` generic type holds a default value
(from the flag) and an optional session override:

```go
type GatewayManagedVariable[T comparable] struct {
    defaultValue T
    currentValue T
    isSet        bool
}
```

`GetEffective()` returns the session override if set, otherwise the
default. This cleanly handles the priority chain without per-query
resolution logic.

The planner intercepts gateway-managed variables before the normal
SET/RESET flow:

```text
planVariableSetStmt()
  |
  +-- isGatewayManagedVariable(name)?
  |     YES -> planGatewayManagedVariable() -> GatewaySessionState primitive
  |     NO  -> normal flow: Route + ApplySessionState
  |
  +-- SET LOCAL? -> pass through (not intercepted)
```

## Startup Parameter Handling

Clients can set `statement_timeout` in the connection string (e.g.,
`options='--statement-timeout=5s'` or as a pgx RuntimeParam). The
gateway intercepts this during connection initialization:

1. Parse the value with `ParsePostgresInterval`
2. Use it as the session default (overriding the flag)
3. Delete it from `StartupParams` to prevent forwarding to PostgreSQL
4. Log a warning if the value is invalid (falls back to flag default)

## Value Parsing

`ParsePostgresInterval` accepts two formats:

- Plain integers as milliseconds: `"5000"` -> 5s (PostgreSQL's
  default unit for `statement_timeout`)
- Go duration strings: `"30s"`, `"200ms"`, `"1m"`, `"1h"`

`SHOW statement_timeout` always returns the value as a plain
milliseconds integer string (e.g., `"30000"`), matching PostgreSQL's
display format.

## Error Behavior

| Cause                   | SQLSTATE | Message                                           |
|-------------------------|----------|---------------------------------------------------|
| Query timeout exceeded  | `57014`  | `canceling statement due to statement timeout`    |
| Client cancelled        | `57014`  | `canceling statement due to user request`         |
| Invalid SET value       | `22023`  | `invalid value for parameter "statement_timeout"` |
| Negative SET value      | `22023`  | `<value> is outside the valid range ...`          |

## File Organization

| File                                | Role                                           |
|-------------------------------------|-------------------------------------------------|
| `handler/handler.go`               | `executeWithTimeout`, `getConnectionState` init |
| `handler/statement_timeout.go`     | `ResolveStatementTimeout`, `ParsePostgresInterval`, `ParseStatementTimeoutDirective` (stub) |
| `handler/connection_state.go`      | `GatewayManagedVariable` storage, `Set`/`Reset`/`Get`/`Show`/`Init` methods |
| `handler/gateway_managed_variable.go` | Generic `GatewayManagedVariable[T]` type     |
| `planner/variable_set_stmt.go`     | `isGatewayManagedVariable`, `planGatewayManagedVariable` |
| `planner/variable_show_stmt.go`    | Gateway-managed SHOW interception               |
| `engine/gateway_session_state.go`  | `GatewaySessionState` SET/RESET primitive       |
| `engine/gateway_show_variable.go`  | `GatewayShowVariable` SHOW primitive            |
| `engine/apply_session_state.go`    | RESET ALL support for gateway-managed vars      |
| `init.go`                          | `--statement-timeout` flag registration          |

## Test Coverage

| File                                          | Covers                                    |
|-----------------------------------------------|-------------------------------------------|
| `handler/statement_timeout_test.go`           | `ResolveStatementTimeout`, `ParsePostgresInterval`, `GatewayManagedVariable`, connection state |
| `handler/handler_test.go`                     | `executeWithTimeout` integration with handler |
| `test/endtoend/queryserving/statement_timeout_test.go` | Full e2e: SET/SHOW/RESET, timeout enforcement, startup params, error codes |

## Future Work

- **Per-query directive parsing**: `/*mg+ STATEMENT_TIMEOUT_MS=500 */`
  parsed from SQL comments in the grammar. The `ParseStatementTimeoutDirective`
  stub returns `*time.Duration` (nil = no directive, non-nil including
  0 = override) so the resolution logic is already correct.
- **Multipooler safety net**: `--statement-timeout` flag at the
  multipooler layer as a floor timeout for non-gateway callers.
- **Transaction timeout**: Separate timeout bounding the time from
  `BEGIN` to `COMMIT`/`ROLLBACK`.
- **SET LOCAL support**: Transaction-scoped timeout overrides.
