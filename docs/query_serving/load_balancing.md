# Load Balancing in Multigres

## Overview

The LoadBalancer manages connections to multipooler instances and routes queries based on target specifications. It's the connection management layer between multigateway and multipoolers.

**Key Design Principles:**

- **Fail-fast**: Returns errors immediately rather than waiting or retrying
- **Locality-aware**: Prefers local cell replicas for read queries
- **Discovery-driven**: Connections managed automatically based on topology changes
- **Thread-safe**: Safe for concurrent access from multiple query handlers

## Architecture

```text
┌─────────────────────────────────────────────────────────────────────┐
│                         MultiGateway                                │
│                                                                     │
│  GlobalPoolerDiscovery ──► LoadBalancerListener ──► LoadBalancer   │
│  (watches etcd)            (adapter)                (selection)     │
│                                                          │           │
│                                                          ▼           │
│                                              ┌────────────────────┐ │
│                                              │ PoolerConnection   │ │
│                                              │ (per pooler)       │ │
│                                              └─────────┬──────────┘ │
└────────────────────────────────────────────────────────┼────────────┘
                                                         │ gRPC
                                                         ▼
                                             ┌───────────────────────┐
                                             │ MultiPooler Instance  │
                                             └───────────────────────┘
```

### Components

**LoadBalancer** - Manages all pooler connections and selects the best one for each query. Maintains a map of pooler ID → PoolerConnection.

**PoolerConnection** - Wraps a gRPC connection to a single multipooler instance. Implements `QueryService` interface and tracks pooler metadata (type, cell, shard).

**LoadBalancerListener** - Adapter between discovery events and LoadBalancer operations. Translates `OnPoolerChanged` / `OnPoolerRemoved` callbacks into `AddPooler` / `RemovePooler` calls.

## Selection Algorithm

### Matching Logic

For a query target (tablegroup, shard, type), the LoadBalancer finds matching poolers:

1. **TableGroup**: Must match exactly
2. **Shard**: Must match exactly
3. **Type**: Must match after defaulting (UNKNOWN → PRIMARY)

### Locality Preference

After finding matches, selection depends on pooler type:

**PRIMARY**: Returns any match (there's only one primary per tablegroup/shard). Location doesn't matter since all writes go through it anyway.

**REPLICA**: Prefers local cell, falls back to remote cells if needed.

### Example Scenarios

```go
// Write to primary (any cell)
target := Target{TableGroup: "users", Shard: "1", Type: PRIMARY}
// → Returns PRIMARY pooler for users/shard-1

// Read from replica (prefer local cell)
target := Target{TableGroup: "users", Shard: "1", Type: REPLICA}
// → Returns REPLICA in local cell if available, else remote replica
```

## Connection Lifecycle

### Discovery-Driven Management

Connections are created and removed based on etcd topology changes:

- **Pooler appears in etcd** → Discovery notifies listener → LoadBalancer creates PoolerConnection
- **Pooler removed from etcd** → Discovery notifies listener → LoadBalancer closes connection

**Initial state replay:** When LoadBalancer registers with discovery, it receives all existing poolers immediately.

### Metadata Updates Without Reconnection

**Design Decision:** When a pooler's properties change (e.g., REPLICA promoted to PRIMARY), only the metadata is updated - the gRPC connection is **reused**.

**Rationale:**

- Failover is a critical path - reconnection adds unnecessary latency
- gRPC connection remains valid after promotion
- Avoids disrupting in-flight queries
- Saves connection reestablishment overhead

**Implementation:** `AddPooler` checks if connection already exists. If yes, calls `UpdateMetadata` instead of creating a new connection.

### Long-Lived Connections

Connections persist until the pooler is removed from topology. No per-query connection creation - all queries share the same connections. gRPC handles multiplexing multiple concurrent queries over a single connection.

## Error Handling

### Fail-Fast Design

The LoadBalancer **never waits or retries**. If no suitable pooler is found, it returns `UNAVAILABLE` error immediately.

**Why fail-fast?**

- **Separation of concerns**: LoadBalancer handles selection, higher layers handle retry policy
- **Observability**: Caller sees exact state without hidden waiting
- **Timeout management**: Retry logic at PoolerGateway level can properly manage timeout budgets

**Current limitation:** PoolerGateway doesn't yet implement retry logic (marked as TODO).

### Common Error Scenarios

- **Primary election in progress**: UNAVAILABLE until new primary is elected (transient, should retry)
- **No replicas available**: UNAVAILABLE (may be permanent, consider fallback to primary)
- **TableGroup doesn't exist**: UNAVAILABLE (application error, don't retry)

## Integration Points

### PoolerGateway

PoolerGateway delegates to LoadBalancer for connection selection:

```go
conn, err := loadBalancer.GetConnection(target)
if err != nil {
    return err  // TODO: Add retry logic here
}
return conn.StreamExecute(ctx, target, sql, options, callback)
```

### GlobalPoolerDiscovery

Discovery watches etcd and calls listener callbacks:

```go
// Setup during initialization
listener := NewLoadBalancerListener(loadBalancer)
discovery.RegisterListener(listener)

// Discovery automatically calls:
// - listener.OnPoolerChanged() for new/updated poolers
// - listener.OnPoolerRemoved() for deleted poolers
```

### QueryService Interface

PoolerConnection implements `queryservice.QueryService`, allowing it to be used interchangeably with other query service implementations. Internally it delegates to a gRPC client.

## Operational Concerns

### Configuration

**`--cell` flag**: Specifies local cell for replica locality preference. This is the only LoadBalancer-specific configuration.

**Example**: `multigateway --cell=us-east-1` → replica queries prefer us-east-1 replicas

## Future Improvements

### Retry Logic in PoolerGateway

Add exponential backoff retry for UNAVAILABLE errors. Currently marked as TODO - LoadBalancer returns immediately, caller should retry.

### Deterministic Subsetting

If there are a large number of replicas, each gate can connection to a different subset of them.

### Advanced Selection Strategies

- **Round-robin**: Distribute load across multiple replicas in same cell
- **Health-aware**: Skip poolers with high error rates or slow responses
- **Weighted selection**: Route more traffic to poolers with more capacity

### Health Streaming Integration

When health streaming is added, LoadBalancer can avoid routing to unhealthy poolers.
