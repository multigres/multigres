# Gateway-Managed Variables

Gateway-managed variables (GMVs) are session variables whose client-visible
state is owned by multigateway instead of being replayed to PostgreSQL backend
connections through `SessionSettings`.

Use this mechanism only for variables whose semantics are implemented at the
gateway. The currently documented GMV is:

- **`statement_timeout`** — enforced by multigateway with context deadlines
  rather than PostgreSQL's backend-local timer.

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
- Top-level `SELECT set_config('<gmv>', value, false)` updates gateway-local
  session state before the query is routed to PostgreSQL.
- Top-level `SELECT set_config('<gmv>', value, true)` updates gateway-local
  transaction state for known GMV names when an explicit transaction is active,
  matching `SET LOCAL` behavior.

GMV values are not stored in `SessionSettings` and are not forwarded to backend
connections as normal session settings.

## `SHOW` vs `current_setting()`

`SHOW <gmv>` is planned explicitly and returns the value from multigateway's
connection state.

Arbitrary SQL expressions are not rewritten. A query such as:

```sql
SELECT current_setting('statement_timeout');
```

runs on PostgreSQL and observes that backend connection's local GUC state, not
multigateway's GMV state. Clients that need the gateway-owned value should use
`SHOW <gmv>` or rely on the gateway behavior itself (for `statement_timeout`,
the enforced request deadline).

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

## Implementation checklist

When adding or changing a GMV, keep these pieces in sync:

1. Add a typed `GatewayManagedVariable[T]` field to
   `MultiGatewayConnectionState` and initialize its default at connection setup.
2. Register the canonical lowercase name in `gatewayManagedVariableNames`.
3. Return the variable from `gatewayManagedVariablesLocked()` and update that
   method's fixed-size array length.
4. Route and validate values in `ApplyGatewayManagedVariable`.
5. Update the planner and engine switches that create and execute the typed SET,
   RESET, and SHOW primitives.
6. Add tests for SET, SET LOCAL, RESET, RESET ALL, SHOW, `set_config`, and
   transaction/savepoint rollback behavior.

See `docs/query_serving/statement_timeout_design.md` for the concrete
`statement_timeout` behavior and timeout-enforcement path.
