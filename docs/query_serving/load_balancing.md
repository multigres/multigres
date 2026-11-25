# Load Balancing in Multigres

## Overview

The load balancer selects the multipooler for each query based on the query's target. Together with
the pooler cache that owns the connections, it is the connection management layer between
multigateway and multipoolers.

**Key Design Principles:**

- **Fail-fast**: Returns errors immediately rather than waiting or retrying
- **Locality-aware**: Prefers local cell replicas for read queries
- **Discovery-driven**: Connections managed automatically based on topology changes
- **Health-attested leadership**: The leader is whichever pooler's live health stream claims it,
  never the topology record
- **Thread-safe**: Safe for concurrent access from multiple query handlers

## Architecture

```text
┌─────────────────────────────────────────────────────────────────────┐
│                         Multigateway                                │
│                                                                     │
│  poolerwatch.PoolerCache ───────────────────► loadBalancer          │
│  (watches topology; OnLive / OnUpdate /       (selection, per-shard │
│   OnGone hooks own each poolerConnection)      routing-primary set) │
│                                                          │          │
│                                                          ▼          │
│                                              ┌────────────────────┐ │
│                                              │ poolerConnection   │ │
│                                              │ (per pooler)       │ │
│                                              └─────────┬──────────┘ │
└────────────────────────────────────────────────────────┼────────────┘
                                                         │ gRPC
                                                         ▼
                                             ┌───────────────────────┐
                                             │ Multipooler Instance  │
                                             └───────────────────────┘
```

All of this lives in `go/services/multigateway/poolergateway/`; the pooler cache is
`go/common/topoclient/poolerwatch`.

### Components

**PoolerGateway** (`pooler_gateway.go`) - Implements `queryservice.QueryService` for the rest of the
gateway. `NewPoolerGateway` builds the pooler cache and the load balancer, and wraps every query
call in `withBuffering` (see [Failover Buffering](./failover_buffering.md)).

**loadBalancer** (`load_balancer.go`) - Selects the pooler for each query. It owns no per-pooler
registry: connections live in the pooler cache. Its own state is one `shardSummary` per (database,
tablegroup, shard), holding the set of poolers currently claiming to be the writable routing
primary.

**poolerConnection** (`pooler_connection.go`) - Wraps a gRPC connection to a single multipooler
instance. Holds a `queryservice.Pooler` (query service, credential lookup, health, notifications),
tracks pooler metadata (cell, shard), and runs a background health stream (`StreamPoolerHealth`)
that reports serving status, replication lag, and the pooler's `RoutingState`. For minigres'
in-process pooler (`LocalPooler`) there is no gRPC connection; the same interface is called
directly.

**poolerwatch.PoolerCache** - Watches the topology and owns the lifecycle of each `poolerConnection`
through three hooks: `OnLive` constructs the connection, `OnUpdate` refreshes its topology metadata
(`UpdatePoolerInfo`), and `OnGone` shuts it down and calls `loadBalancer.onPoolerGone`.

## Selection Algorithm

### Routing by Mode

A query target is a `query.Target`: a `ShardRef` (database, tablegroup, shard) plus a `Mode`.
`ScatterConn` sets the mode from the listener the client connected to: `MODE_WRITABLE` on the
primary port, `MODE_INCONSISTENT` on the replica-reads port (see
[Replica Reads](./replica_reads_design.md)).

| Mode                                         | Routed to                                                      |
| -------------------------------------------- | -------------------------------------------------------------- |
| `MODE_WRITABLE`, `MODE_CONSISTENT`           | The shard's leader (routing primary)                           |
| `MODE_INCONSISTENT` (and `MODE_UNSPECIFIED`) | Any connected pooler in the shard that is not claiming primary |

### Leader Selection

The leader is the highest-rule member of the shard's routing-primary set (see
[Health Streaming and Leader Tracking](#health-streaming-and-leader-tracking)). Its connection is
looked up in the pooler cache by ID. Location doesn't matter. Two cases return a "no writable
primary" error, which the failover buffer treats as buffer-worthy: no leader observed yet, and
leader known but not connected.

### Replica Selection

Candidates are all connected poolers matching the target's database and tablegroup (and shard, when
the target names one) that do **not** claim to be a routing primary. Any primary claimant is
excluded, including a stale one that lost the election: it is headed for demotion, which restarts
PostgreSQL.

`selectReplicaConnection` then applies the lag filter (below) and, within the chosen lag tier, picks
at random from the first non-empty group of:

1. local cell, serving
2. remote cell, serving
3. local cell, not serving

### Example Scenarios

```go
// Write (any cell)
target := Target{ShardRef: {Database: "db", TableGroup: "users", Shard: "1"}, Mode: MODE_WRITABLE}
// → Returns the leader's pooler for users/shard-1

// Read from replica (prefer local cell)
target := Target{ShardRef: {Database: "db", TableGroup: "users", Shard: "1"}, Mode: MODE_INCONSISTENT}
// → Returns a serving follower in the local cell if available, else a remote one
```

### Reserved Connections

A session holding a reserved connection must return to the pooler that owns it. Those requests
bypass selection: `PoolerGateway.QueryServiceByID` looks the connection up by the pooler ID carried
in the session's `ReservedState` (`getConnectionByID`), and returns `UNAVAILABLE` if that pooler is
no longer connected.

## Connection Lifecycle

### Discovery-Driven Management

Connections are created and removed based on topology changes:

- **Pooler appears in the topology** → cache `OnLive` → `newPoolerConnection` (dials gRPC, starts
  the health stream)
- **Pooler removed from the topology** → cache `OnGone` → connection shut down, its primary claim
  retracted, and the shard summary dropped once no pooler of the shard remains

The gateway runs the cache with zero grace periods: membership changes are visible to selection
immediately.

### Metadata Updates Without Reconnection

**Design Decision:** When a pooler's topology record changes, only the metadata is updated
(`OnUpdate` → `UpdatePoolerInfo`) - the gRPC connection is **reused**.

**Rationale:**

- Failover is a critical path - reconnection adds unnecessary latency
- gRPC connection remains valid after promotion
- Avoids disrupting in-flight queries
- Saves connection reestablishment overhead

A topology update never changes who the leader is and never drains the failover buffer: both come
only from health-stream state, because a topology update can arrive before the matching health
update.

### Long-Lived Connections

Connections persist until the pooler is removed from topology. No per-query connection creation -
all queries share the same connections. gRPC handles multiplexing multiple concurrent queries over a
single connection.

## Health Streaming and Leader Tracking

Each `poolerConnection` runs a background `StreamPoolerHealth` RPC against its multipooler. Each
response carries:

- Serving status (e.g. `SERVING`, `NOT_SERVING`)
- Replication lag for replicas (`replication_lag_ns`)
- `RoutingState` — the pooler's own routing role (`PRIMARY` / `REPLICA`) and the consensus rule it
  holds that role under
- `routing_version` / `plan_epoch` — sharding catalog versions, reported only by the catalog shard's
  primary

If no message arrives within the staleness timeout, the stream is cancelled and retried, and the
connection's health records the error.

The loadBalancer consumes every update via the `onPoolerHealthUpdate` callback:

1. **Routing-primary set.** A pooler is added to its shard's set when a _live_ update (no stream
   error) advertises role `PRIMARY`, and removed when it advertises anything else, when its stream
   goes stale or errors, or when it leaves the topology. The topology record (pooler type) is never
   consulted for leader identity. During a failover overlap two poolers may both claim primary; the
   one with the highest rule number wins.

   A live update with a non-`SERVING` status keeps its claim. That is what lets a query reach a
   primary that is draining for a planned failover, get `MTF01`, and enter the failover buffer
   instead of failing.

2. **Failover coordination.** After folding the update in, if the elected leader is connected,
   `SERVING`, and itself advertises `PRIMARY`, the loadBalancer invokes `OnLeaderServing`.
   `PoolerGateway` wires that to `buffer.StopBuffering` for the shard (and to the notification
   manager, which reopens its streams after a leader change).

## Replication Lag Filtering

The lag thresholds (`--low-replication-lag-ms`, `--high-replication-lag-tolerance-ms`) configure a
two-tier filter for replica selection:

- **low lag**: replicas at or below this lag are considered "healthy" and are preferred. Unknown lag
  (0) counts as healthy.
- **high tolerance**: absolute upper bound. Replicas exceeding this are never returned. `0` disables
  the upper bound.

If all replicas exceed the low threshold but are still under the high tolerance, they remain
eligible — the filter degrades gracefully rather than returning no replicas.

## Error Handling

### Fail-Fast Design

The loadBalancer **never waits or retries**. If no suitable pooler is found, it returns an error
immediately.

**Why fail-fast?**

- **Separation of concerns**: loadBalancer handles selection, higher layers handle retry policy
- **Observability**: Caller sees exact state without hidden waiting
- **Timeout management**: Retry logic at PoolerGateway level can properly manage timeout budgets

Waiting and retrying is `PoolerGateway.withBuffering`'s job: for leader-routed modes it buffers on a
"no writable primary" error (and on the pooler's `MTF01`) and retries against the new leader, up to
`MaxBufferingRetries` times. See [Failover Buffering](./failover_buffering.md). Replica-routed
requests are never buffered.

### Common Error Scenarios

- **Leader election in progress**: "no writable primary" — buffered and retried when
  `--buffer-enabled`, otherwise returned to the client (transient, should retry)
- **No replicas available**: `UNAVAILABLE` ("no pooler found for target"); there is no fallback to
  the leader
- **All replicas beyond the high lag tolerance**: `UNAVAILABLE` ("no replica with acceptable
  replication lag")
- **TableGroup doesn't exist**: no pooler matches, so the same errors as above

## Integration Points

### PoolerGateway

PoolerGateway delegates to the loadBalancer for connection selection inside its buffering loop:

```go
err := pg.withBuffering(ctx, target, singleQuery, retryReadOnlyError,
    func(conn *poolerConnection) error {
        state, err = conn.QueryService().StreamExecute(ctx, target, sql, options, reservationOptions, callback)
        return err
    })
```

### Pooler cache

```go
cache := poolerwatch.New(ctx, poolerwatch.Config[*poolerConnection]{Source: topoStore, ...})
lb := newLoadBalancer(loadBalancerOpts{Cache: cache, ...})
cache.Start(poolerwatch.Hooks[*poolerConnection]{
    OnLive:   ..., // newPoolerConnection
    OnUpdate: ..., // conn.UpdatePoolerInfo
    OnGone:   ..., // conn.Shutdown + lb.onPoolerGone
})
```

### QueryService

`poolerConnection.QueryService()` returns the `queryservice.QueryService` for that pooler (backed by
the multipooler gRPC stub, or by direct calls for a local pooler), with the query methods
(`StreamExecute`, `PortalStreamExecute`, `ConcludeTransaction`, etc.).

## Operational Concerns

### Configuration

| Flag                                  | Default | Description                                              |
| ------------------------------------- | ------- | -------------------------------------------------------- |
| `--cell`                              | -       | The gateway's cell, used for replica locality preference |
| `--low-replication-lag-ms`            | 30000   | Preferred replica lag threshold                          |
| `--high-replication-lag-tolerance-ms` | 0       | Absolute maximum replica lag (`0` = no limit)            |

**Example**: `multigateway --cell=us-east-1` → replica queries prefer us-east-1 replicas

### Status page

The gateway status page labels each connected pooler `leader`, `stale-leader` (still claims primary
but a higher-rule claimant exists) or `follower` (`leadershipFor`).

## Future Improvements

### Deterministic Subsetting

If there are a large number of replicas, each gateway can connect to a different subset of them.

### Advanced Selection Strategies

- **Round-robin**: Distribute load across multiple replicas in same cell
- **Health-aware**: Skip poolers with high error rates or slow responses; treat a replica that is
  not actively replicating as degraded
- **Weighted selection**: Route more traffic to poolers with more capacity
