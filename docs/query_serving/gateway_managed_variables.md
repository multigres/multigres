# Gateway-Managed Variables

Gateway-managed variables (GMVs) are session variables whose client-visible
state is owned by multigateway instead of being replayed to PostgreSQL backend
connections through `SessionSettings`.

Use this mechanism only for variables whose semantics are implemented at the
gateway. The current GMVs are:

- **`statement_timeout`** — enforced by multigateway with context deadlines
  rather than PostgreSQL's backend-local timer.
- **`idle_session_timeout`** — enforced by the protocol layer while the
  client-facing connection is idle outside a transaction.

Both are registered in the single source of truth,
`gatewayManagedVariables` in `handler/gateway_managed_variables.go` (see
[Adding or changing a GMV](#adding-or-changing-a-gmv)).

> **Not a GMV:** `SHOW multigres.server_version` (and `SELECT multigres.version()`) is
> also answered by the gateway, but it is a read-only pseudo-variable, not a
> GMV — it has no `SET`/`RESET`/`set_config`/transaction behavior and is not in
> this registry. See [`multigres_version.md`](./multigres_version.md). Don't add
> it here.

## Why GMVs exist

Regular session settings are stored in `SessionSettings` and applied to pooled
PostgreSQL connections before queries run. That is appropriate for variables
PostgreSQL itself owns, such as `search_path`.

Some variables need gateway ownership instead. For example,
`statement_timeout` must affect the full multigateway → multipooler →
PostgreSQL request path, so the gateway resolves the effective timeout and
propagates it as a context deadline. Forwarding that setting to arbitrary pooled
backends would be redundant and could leave backend-local state unrelated to the
client session that originally set it.

## Client-visible behavior

For a GMV, multigateway handles the SQL surfaces that it can plan safely:

- `SET <gmv> = ...` updates gateway-local session state.
- `SET LOCAL <gmv> = ...` updates gateway-local transaction state and is cleared
  at transaction end.
- `RESET <gmv>` / `SET <gmv> TO DEFAULT` restore the connection default.
- `SET LOCAL <gmv> TO DEFAULT` installs a transaction-local default override.
- `RESET ALL` clears all regular `SessionSettings` and resets every registered
  GMV.
- `SHOW <gmv>` reads gateway-local state without a PostgreSQL round trip.
- `current_setting('<gmv>'[, missing_ok])` in a value-evaluating DML statement
  returns the gateway-local value (same string as `SHOW`), not the backend GUC.
- Top-level `SELECT set_config('<gmv>', value, false)` updates gateway-local
  session state.
- Top-level `SELECT set_config('<gmv>', value, true)` updates gateway-local
  transaction state for known GMV names when an explicit transaction is active,
  matching `SET LOCAL` behavior.

GMV values are not stored in `SessionSettings` and are not forwarded to backend
connections as normal session settings.

## `set_config` in a SELECT is rewritten out of the backend query

A `SELECT set_config('<gmv>', value, …)` is special: unlike `SET`, it is a
value-returning expression, so the client expects the applied value back in the
result. The gateway does two things with it:

1. **Applies it to gateway-local state** (session or transaction-local per the
   `is_local` argument), exactly like `SET` / `SET LOCAL`. This is what makes
   `SHOW` and enforcement correct.
2. **Rewrites the call out of the query it routes to PostgreSQL**, replacing it
   with the value it would have returned. The real `set_config` therefore never
   runs on the pooled backend.

Step 2 is essential. If the `set_config` ran on the backend it would persist the
real GUC on that pooled connection, and because the gateway never forwards
`SET`/`RESET` of a GMV, a later reset at the gateway would not reach it — the
value would **leak across clients** sharing the connection. Rewriting the call
out is what prevents that leak.

How the value is substituted:

- **Literal value** (`'1000'`) → its canonical form is computed at plan time
  (`GatewayManagedCanonicalValue`, e.g. `'1000'` → `'1s'`) and inlined as a
  constant. No execute-time work.
- **Bound value** (`$N`, extended protocol) → the projection keeps a bind slot
  that `GatewayManagedValueRoute` canonicalizes at execute time. If `$N` is also
  used elsewhere in the query, a fresh synthetic slot is allocated so that other
  use keeps the raw value untouched.

The client always sees PostgreSQL's canonical form (`'1000'` → `'1s'`), so
return-value parity is preserved. In a **mixed** batch such as
`SELECT set_config('statement_timeout', …), set_config('work_mem', …)`, only the
GMV call is rewritten out; ordinary `set_config` calls still run on the backend.

Not handled (rejected up front, not leaked):

- A **NULL** value — rejected by the planner for every variable (PostgreSQL
  would reset-to-default; this is a pre-existing multigres limitation).
- An **expression** value (column reference, function call) on a GMV — rejected
  or routed through the dynamic `set_config` resolve path; it never reaches the
  backend as a raw GMV `set_config`.
- A **bound variable name** (`set_config($1, …)`) — not recognized as a GMV at
  plan time, so it routes to the backend. Exotic; real callers use a literal
  name.

## `SHOW` vs `current_setting()`

`SHOW <gmv>` is planned explicitly and returns the value from multigateway's
connection state.

`current_setting('<gmv>'[, missing_ok])` is also answered with the gateway-owned
value, so it agrees with `SHOW` — matching native PostgreSQL, where the two always
agree. A GMV is never applied to the pooled backend, so evaluating the call there
would observe the backend's local GUC and diverge; the gateway prevents that by
**rewriting the call out of the query**, the same technique used for `set_config`:

```sql
SELECT current_setting('statement_timeout', true);
```

Each `current_setting('<gmv>', …)` call whose name is a **literal** gateway-managed
variable is replaced by a synthetic bind slot that `GatewayManagedValueRoute` fills
from gateway connection state at execute time (the string `SHOW` returns). Because
the value is resolved per execution, the plan stays cacheable across value changes.
`missing_ok` is irrelevant — a registered GMV always exists, so the call never
returns `NULL`.

Scope and limits:

- Only statements that **evaluate** the call for a result are rewritten — `SELECT`
  (including `SELECT INTO`), `INSERT`, `UPDATE`, `DELETE`, and `CREATE TABLE AS`
  (all materialize the value once). Statements that **store** the call as a
  re-evaluable definition are left as-is so the value isn't frozen to the creating
  session: `CREATE VIEW` (re-evaluated on every read) and `CREATE MATERIALIZED VIEW`
  (re-run by `REFRESH`) — the latter shares the `CREATE TABLE AS` node but is
  excluded by its object type.
- A **bound variable name** (`current_setting($1)`) is not recognized as a GMV at
  plan time, so it routes to the backend — the same documented gap as a bound
  `set_config` name. Real callers use a literal name.
- The observable backend GUC (for leak checks and the like) is
  `SELECT setting FROM pg_settings WHERE name = '<gmv>'`, which reads the pooled
  connection's real state and is never rewritten.

## Transaction and savepoint lifecycle

Each GMV is represented by a typed `GatewayManagedVariable[T]` with three value
layers:

1. default value — initialized once for the connection,
2. session override — set by non-LOCAL `SET`,
3. transaction-local override — set by `SET LOCAL` or `set_config(..., true)`.

The effective value resolves as:

```text
transaction-local > session override > default
```

GMVs keep a snapshot stack in lockstep with the connection's savepoint stack.
`BEGIN`, `SAVEPOINT`, `RELEASE`, `ROLLBACK TO`, `COMMIT`, and `ROLLBACK` drive
that stack through the `gmvLifecycle` interface. This preserves PostgreSQL-like
GUC behavior across transaction boundaries without each lifecycle method naming
each variable individually.

## Adding or changing a GMV

The string-facing behavior of every GMV lives in one registry,
`gatewayManagedVariables` in `handler/gateway_managed_variables.go`. Each entry
is built by `newGMVSpec(canonicalize, applySet, reset, setLocalToDefault,
showEffective)` — a constructor whose parameters _are_ the behaviors, so a
registration that omits one is a compile error. Every string-facing dispatch
site (`set_config` canonicalization, `SET`/`RESET`, `SHOW`) routes through this
registry; the old per-variable switches in the planner and engine are gone.

Adding a GMV is therefore **one registry entry**, plus a few inherently
type-specific pieces the registry can't unify:

1. Add the `newGMVSpec(...)` entry to `gatewayManagedVariables`. The compiler
   makes you supply every behavior. (For a `GUC_UNIT_MS` timeout, reuse
   `msTimeoutCanonicalize` / `msTimeoutApplySet`.)
2. Add a typed `GatewayManagedVariable[T]` field to
   `MultigatewayConnectionState` for the per-connection state the behaviors
   adapt over.
3. Return that field from `gatewayManagedVariablesLocked()` and bump the
   method's fixed-size array length. This drives the
   [transaction/savepoint snapshot lifecycle](#transaction-and-savepoint-lifecycle)
   uniformly.
4. Seed its connection default in `getConnectionState` (`handler/handler.go`).
   A registry-driven safety net there strips every registered GMV from the
   startup params, so even a GMV added without this step can never leak to the
   backend via connection parameters — only its `SHOW` default would be wrong.
5. Add the strongly-typed enforcement accessor the variable needs (e.g.
   `GetStatementTimeout` for the query deadline). This is inherently
   type-specific and stays outside the registry.
6. Add tests for `SET`, `SET LOCAL`, `RESET`, `RESET ALL`, `SHOW`, `set_config`
   (literal and bound), and transaction/savepoint rollback. The registry
   backstop test (`handler/gateway_managed_variables_test.go`) already asserts
   every entry is non-nil, lower-case keyed, and round-trips set/show/reset.

See `docs/query_serving/statement_timeout_design.md` for the concrete
`statement_timeout` behavior and timeout-enforcement path.
