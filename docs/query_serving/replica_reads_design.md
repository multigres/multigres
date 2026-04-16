# Replica Reads

## Overview

Replica reads allow read-heavy workloads to scale horizontally by routing queries to
PostgreSQL standby replicas. Clients connect to a dedicated replica port on the
multigateway; all traffic on that port is routed to replica multipoolers. PostgreSQL on
the standby enforces read-only semantics — write attempts receive the standard
`25006 read_only_sql_transaction` error.

## Architecture

```text
                        Primary Port (:5432)
                       ┌──────────────────────┐
    Client (r/w) ─────►│    MultiGateway      │──── ScatterConn ──► PRIMARY multipooler ──► PG Primary
                       │                      │
    Client (r/o) ─────►│    Replica Port      │──── ScatterConn ──► REPLICA multipooler ──► PG Standby
                       │    (:5433)           │
                       └──────────────────────┘
```

Connections on the primary port behave exactly as before — all queries route to the
primary multipooler. Connections on the replica port route to replica multipoolers
instead. The routing decision is made once per connection (based on which port it
arrived on) and applied transparently in `ScatterConn`.

## Design Decisions

### Port-based routing (not SQL-level hints)

An alternative approach is SQL-level hints (e.g., `USE db@replica`) to select the target.
We use a separate TCP listener instead. This has several advantages:

- **Invisible to the application** — switch by changing the connection string port, no
  SQL changes required.
- **Connection-pool friendly** — connection pools can maintain separate pools for
  primary and replica ports without per-query routing logic.
- **Simpler gateway implementation** — no need to parse routing hints out of SQL or
  track per-session routing state changes.

The trade-off is that switching between primary and replica requires a new connection
(not possible mid-session). This matches the typical usage pattern where read-only
workloads use a separate connection string.

### PostgreSQL enforces read-only (not the gateway)

The gateway does not inspect query types to reject writes. Instead, all queries are
forwarded to the replica and PostgreSQL itself returns `SQLSTATE 25006` for write
operations. This is simpler and more correct — PostgreSQL knows its own capabilities
better than we do, and read-only transactions (`BEGIN TRANSACTION READ ONLY`) work
naturally.

## Replication Lag Filtering

Replica selection uses a two-tier replication lag model:

| Threshold           | Flag                                  | Default      | Behavior                                    |
| ------------------- | ------------------------------------- | ------------ | ------------------------------------------- |
| Low (preferred)     | `--low-replication-lag-ms`            | 30000        | Replicas at or below this lag are preferred |
| High (absolute max) | `--high-replication-lag-tolerance-ms` | 0 (disabled) | Replicas above this are never selected      |

### Selection Algorithm

1. **Exclude** replicas with lag > high tolerance threshold (if configured).
2. **Prefer** replicas with lag <= low threshold ("healthy" tier).
3. If no healthy replicas, **fall back** to replicas between the low and high thresholds.
4. If no replicas pass either threshold, **return error** to client.
5. Within the selected tier, prefer **local-cell serving** replicas, with randomization
   to distribute load.

Replicas where lag is unknown (zero value, e.g., health stream not yet received) are
treated as healthy to avoid rejecting replicas during startup.

### Replication Lag Source

Replication lag is measured via the heartbeat mechanism already used for multiorch
reporting. The multipooler's health streamer includes `replication_lag_ns` (int64,
nanoseconds) in `StreamPoolerHealthResponse`. This value is updated on every heartbeat
tick and propagated to the multigateway's `PoolerHealth` struct via the health stream.

## Load Balancing

Replica connections are load-balanced across all healthy replicas using tiered random
selection:

1. **Local cell, serving** — highest priority
2. **Remote cell, serving** — second priority
3. **Any remaining** — last resort

Within each tier, a random replica is selected to distribute load evenly.

## Configuration

| Flag                                  | Default      | Description                            |
| ------------------------------------- | ------------ | -------------------------------------- |
| `--pg-replica-port`                   | 0 (disabled) | Port for replica-reads connections     |
| `--low-replication-lag-ms`            | 30000        | Preferred lag threshold (milliseconds) |
| `--high-replication-lag-tolerance-ms` | 0 (no limit) | Absolute max lag (milliseconds)        |

Environment variables: `MT_PG_REPLICA_PORT`, `MT_LOW_REPLICATION_LAG_MS`,
`MT_HIGH_REPLICATION_LAG_TOLERANCE_MS`.

## Error Handling

- **No replica available**: `GetConnection` returns `UNAVAILABLE` — the error surfaces
  to the client. There is no silent fallback to primary; the client should handle this
  by connecting to the primary port.
- **All replicas too lagged**: Same behavior as no replica available.
- **Write on replica port**: PostgreSQL returns `SQLSTATE 25006
(read_only_sql_transaction)` — the gateway does not intercept this.
- **No buffering for replicas**: Failover buffering only applies to primary targets.
  Replica errors are returned immediately without retry.

## Future Work

- **Per-query routing hints**: Support `SET target_session_attrs = 'read-write'` or
  similar to allow mid-session switching between primary and replica.
