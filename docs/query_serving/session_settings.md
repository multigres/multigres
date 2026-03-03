# Session Settings (SET/RESET)

## Overview

Multigateway handles `SET` and `RESET` commands locally without forwarding them to PostgreSQL. Session variables are tracked in the gateway's connection state and propagated to backend connections via the pool's `ApplySettings` mechanism on each subsequent query.

Inside transactions, the connection is reserved and bypasses the pool's normal `ApplySettings` path. To ensure settings changes (e.g., `SET search_path`) take effect on the reserved backend, the multipooler applies settings diffs to reserved connections before each query execution.

## How It Works

1. **SET variable = value**: The variable and value are stored in `SessionSettings`. A synthetic `CommandComplete (SET)` is returned to the client immediately.
2. **RESET variable**: The variable is removed from `SessionSettings`. A synthetic `CommandComplete (RESET)` is returned.
3. **RESET ALL**: All variables in `SessionSettings` are cleared. A synthetic `CommandComplete (RESET)` is returned.
4. **SET LOCAL**: Passed through to PostgreSQL (transaction-scoped, not tracked by the gateway).

On the next query, the pool merges `SessionSettings` with `StartupParams` (startup params from the client's initial connection). `SessionSettings` entries take precedence. The merged settings are applied to the backend connection before executing the query.

When a variable is `RESET`, its entry is removed from `SessionSettings`, and the `StartupParams` value (if any) becomes visible again through the merge.

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
