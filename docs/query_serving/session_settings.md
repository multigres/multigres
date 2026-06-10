# Session Settings (SET/RESET)

## Overview

Multigateway handles `SET` and `RESET` commands locally without forwarding them to PostgreSQL. Session variables are tracked in the gateway's connection state and propagated to backend connections via the pool's `ApplySettings` mechanism on each subsequent query.

Inside transactions, the connection is reserved and bypasses the pool's normal `ApplySettings` path. To ensure settings changes (e.g., `SET search_path`) take effect on the reserved backend, the multipooler applies settings diffs to reserved connections before each query execution.

## How It Works

1. **SET variable = value**: The variable and value are stored in `SessionSettings`. A synthetic `CommandComplete (SET)` is returned to the client immediately.
2. **RESET variable**: The variable is removed from `SessionSettings`. A synthetic `CommandComplete (RESET)` is returned.
3. **RESET ALL**: All variables in `SessionSettings` are cleared. A synthetic `CommandComplete (RESET)` is returned.
4. **SET variable TO DEFAULT**: Normalized to `RESET variable` during planning (matching PostgreSQL, where `VAR_SET_DEFAULT` falls through to `VAR_RESET`).
5. **SET LOCAL**: Passed through to PostgreSQL (transaction-scoped, not tracked by the gateway).
6. **SET TRANSACTION / SET SESSION CHARACTERISTICS / SET var FROM CURRENT**: Passed through to PostgreSQL; not tracked by the gateway.

On the next query, the pool merges `SessionSettings` with `StartupParams` (startup params from the client's initial connection). `SessionSettings` entries take precedence. The merged settings are applied to the backend connection before executing the query.

When a variable is `RESET`, its entry is removed from `SessionSettings`, and the `StartupParams` value (if any) becomes visible again through the merge.

## Gateway-Managed Variables

Some variables are owned by multigateway rather than stored in `SessionSettings`
or forwarded to PostgreSQL. See
[`gateway_managed_variables.md`](./gateway_managed_variables.md) for the GMV
contract and [`statement_timeout_design.md`](./statement_timeout_design.md) for
the concrete `statement_timeout` behavior.

## Per-Database / Per-Role Default Changes (ALTER DATABASE/ROLE … SET)

`ALTER DATABASE … SET` and `ALTER ROLE … SET` change _per-database_ / _per-role_
session GUC defaults, stored in the `pg_db_role_setting` catalog. PostgreSQL
applies these defaults to a session **only at session start** (they become the
session's GUC _reset values_). A connection pooler therefore has a problem:
backends opened **before** such an `ALTER` keep the old defaults until they
reconnect — `RESET`, `RESET ALL`, and `DISCARD ALL` all revert to the
session-start reset value, never re-reading `pg_db_role_setting`.

To keep pooled sessions consistent with a fresh connection, the multigateway
flags statements that change these defaults and the multipooler refreshes its
pooled connections:

1. **Planner detection** (`planner/connection_defaults.go`,
   `statementChangesConnectionDefaults`): a statement is flagged when it is
   - `ALTER DATABASE … SET`/`RESET` or `ALTER ROLE … SET`/`RESET` (any form —
     every one mutates `pg_db_role_setting`), or
   - `CREATE EXTENSION <name>` for a `name` on a small hardcoded allowlist of
     extensions whose install script runs such an `ALTER` internally
     (`extensionsAlteringConnectionDefaults`; today: `postgis_topology`, whose
     `topology.AddToSearchPath` runs `ALTER DATABASE … SET search_path`). The
     `ALTER` runs server-side inside the extension, invisible to the gateway,
     so the extension name is the only signal — hence an explicit allowlist
     rather than general detection.

   The flag rides the execute request as
   `ExecuteOptions.InvalidatesConnectionDefaults`.

2. **Durability-aware bump** (multipooler executor): on **successful**
   execution, if the connection is now idle (autocommit, or a
   non-transactional reserved connection) the change is durable, so the
   executor bumps the pool's _defaults generation_ immediately. If the
   statement ran inside an explicit transaction the change is durable only at
   `COMMIT`, so the executor marks the reserved connection pending and bumps on
   a successful `COMMIT`. `ROLLBACK` discards the mark (no bump). A
   `ROLLBACK TO SAVEPOINT` keeps the mark — a deliberately conservative choice
   (an extra refresh is harmless; a missed one would leak stale defaults). This
   mirrors PostgreSQL's transactional-DDL semantics.

3. **Lazy refresh** (`connpool.Pool`): bumping the generation does not close any
   connection. Each pooled connection records the generation it was
   established under; on its **next borrow**, a connection whose generation is
   stale is transparently reconnected (`refreshIfStale`) so the fresh backend
   re-reads `pg_db_role_setting`. The hot-path cost is one atomic load plus an
   int compare; an actual reconnect happens at most once per connection per
   bump, and only when that connection is next used.

### Deviations and limitations of this behaviour

- **More eager than stock pooling, by design.** A plain connection pooler
  (pgbouncer/odyssey) never refreshes pooled server connections after an
  `ALTER DATABASE/ROLE … SET`; the change is observed only as connections
  naturally cycle. Multigres refreshes them proactively so a pooled session
  behaves like a fresh PostgreSQL connection.
- **Standby/replica poolers refresh lazily.** The flagged statement executes
  on the **primary**, so only the primary's pooler bumps immediately. Replica
  poolers receive the catalog change via WAL but are not signalled, so their
  pooled backends pick up the new default on their own lifecycle (idle /
  max-lifetime recycle) or the next invalidating statement. Reads routed to a
  replica may briefly observe the old per-database/role default.
- **Cross-pooler over-approximation.** A bump invalidates _all_ of a pooler's
  user pools and its admin pool, not just the affected role/database. An
  unnecessary refresh only costs a reconnect on next borrow, whereas a missed
  one would leak stale defaults, so the bump is intentionally broad.

## Behaviour Deviations from PostgreSQL

### SET does not validate parameters

`SET` commands are **not validated** against PostgreSQL. A client can `SET` an invalid variable name or an invalid value for a valid variable without receiving an immediate error.

The error will surface on the **next query** when the connection pool attempts to apply the invalid setting to a backend connection. The client will receive repeated errors on every query until they `RESET` the problematic variable.

**Example:**

```sql
SET nonexistent_variable = 'value';  -- Succeeds (no error)
SELECT 1;                             -- Fails: unrecognized configuration parameter
RESET nonexistent_variable;           -- Succeeds
SELECT 1;                             -- Succeeds again
```

This trade-off was chosen to keep the SET/RESET code path simple. It may be revisited if stricter validation is needed.

### SET inside a rolled-back transaction persists

In PostgreSQL, `SET` (without `LOCAL`) inside a transaction that is later rolled back still reverts the variable to its pre-transaction value. Multigateway does **not** track transaction boundaries for session settings, so a `SET` inside a rolled-back transaction will persist in the gateway's tracked state.

**Example:**

```sql
SET work_mem = '256MB';
BEGIN;
SET work_mem = '512MB';
ROLLBACK;
SHOW work_mem;  -- PostgreSQL: '256MB', Multigateway: '512MB'
```

**TODO:** Fix this by snapshotting session settings at `BEGIN` and restoring on `ROLLBACK`.
