# Dynamic Fair Share Pool Allocation

## Problem Statement

The current connection pool implementation uses **static per-user pool sizes**
configured at startup. This creates a fundamental problem when the number of
users is unknown and PostgreSQL's `max_connections` is fixed:

```text
Scenario:
- PostgreSQL max_connections = 500
- Static per-user capacity = 100

State 1: 5 users connect
  → 5 users × 100 connections = 500 (at capacity)

State 2: 6th user arrives
  → No connections available
  → User 6 either errors or waits indefinitely
```

### How Other Poolers Handle This

| Pooler | Approach | Limitation |
|--------|----------|------------|
| **PgBouncer** | Static `max_user_connections` per user; queues when exhausted | No dynamic rebalancing |
| **Supavisor** | Per-tenant pools with fixed `default_pool_size` | Designed for separate databases per tenant |

Neither actively resizes existing user pools when new users arrive. They simply
queue requests when connections are exhausted.

### What We Want

Dynamic fair share allocation that:

1. Distributes available connections fairly among active users
2. Automatically rebalances when users join or leave
3. Treats regular and reserved pools as separate resources with independent allocation
4. Runs as a background task without impacting query latency

## Goals

1. **Fair allocation**: Each user gets a fair share of the global connection budget
2. **Adaptive**: Automatically rebalance as users come and go
3. **Demand-aware**: Allocate based on actual usage, not just equal splits
4. **Non-blocking**: Rebalancing happens in background, not on the query hot path
5. **Separate resources**: Regular and reserved pools have independent budgets

## Non-Goals

1. **Weighted fairness by subscription tier** - Future enhancement, not in initial scope
2. **Cross-multipooler coordination** - Each multipooler manages its own pools independently
3. **Predictive scaling** - We react to demand, not predict it

## Implementation Checklist

### Phase 1: Lock-Free Hot Path ✅

- [x] Replace `sync.Mutex` with `atomic.Pointer[map[string]*UserPool]` in Manager
- [x] Implement copy-on-write for new user pool creation
- [x] Add separate `createMu` mutex only for pool creation (cold path)
- [x] Update `Close()` and `Stats()` to work with atomic snapshot
- [x] Benchmark before/after to measure latency improvement
- [x] Verify correctness with race detector (`go test -race`)

**Benchmark Results:**

| Approach | Latency | Speedup |
|----------|---------|---------|
| Mutex (old) | 67.33 ns/op | baseline |
| RWMutex | 100.8 ns/op | 0.67x (worse) |
| **AtomicPointer (new)** | **0.82 ns/op** | **82x faster** |

The lock-free hot path achieves sub-nanosecond latency, an **82x improvement** over the
previous mutex-based approach under parallel load.

### Phase 2: Demand Tracking ✅

- [x] Add `requested` atomic counter to `connpool.Pool` (like `borrowed`)
- [x] Increment on `Get()` start, decrement on `Get()` fail or `Recycle()`
- [x] Remove `BorrowCallback` - no longer needed
- [x] Create `DemandTracker` with sliding window buckets
- [x] Implement bucket rotation on rebalance interval
- [x] Add sampling goroutine (read `requested` at configured interval, update current bucket max)
- [x] Add `Requested()` method to `connpool.Pool` and include in `PoolStats`
- [x] Unit tests for sliding window accuracy

**Design:**
- Pool tracks `requested` count directly (no callback needed)
- `DemandTracker` maintains N buckets where N = window / poll_interval
- Each bucket stores max `requested` seen during that interval
- Sampling goroutine reads pool's `requested` at configured interval, updates current bucket max
- `GetPeakAndRotate()` returns max across all buckets and rotates to next bucket
- On rebalance: call `GetPeakAndRotate()` to get peak and start fresh bucket
- Configuration (window, poll interval, sample interval) set via flags on connpoolmanager

**Note:** Integration of `DemandTracker` into `UserPool` and exposing demand metrics will happen
in Phase 5 alongside the rebalancer, which owns the tracker lifecycle.

### Phase 3: Fair Share Allocator ✅

- [x] Create `FairShareAllocator` struct (resource-agnostic, single-resource allocator)
- [x] Add configuration flags:
  - [x] `--connpool-global-capacity`
  - [x] `--connpool-reserved-ratio`
- [x] Implement max-min fairness algorithm
- [x] Unit tests for allocation edge cases
- [x] Property-based tests (total <= capacity, each user >= 1)

**Design:**
- `FairShareAllocator` is resource-agnostic - it allocates a single resource type
- Create two instances: one for regular pools, one for reserved pools
- This mirrors the `DemandTracker` design (also resource-agnostic)

**Usage:**
```go
regularAlloc := NewFairShareAllocator(globalRegularCapacity)
reservedAlloc := NewFairShareAllocator(globalReservedCapacity)

regularResult := regularAlloc.Allocate(regularDemands)   // map[string]int64
reservedResult := reservedAlloc.Allocate(reservedDemands) // map[string]int64
```

**Implementation:**
- `FairShareAllocator` in `go/multipooler/connpoolmanager/fair_share_allocator.go`
- Config flags in `go/multipooler/connpoolmanager/config.go`
- 14 unit tests + 4 property-based tests in `fair_share_allocator_test.go`
- Algorithm: max-min fairness with progressive filling, minimum 1 connection per user

### Phase 4: UserPool Resize Support ✅

- [x] Add `SetCapacity(ctx, regularCap, reservedCap)` to `UserPool`
- [x] Add `SetCapacity()` to `regular.Pool` (wire to underlying connpool)
- [x] Add `SetCapacity()` to `reserved.Pool` (wire to underlying connpool)
- [x] Handle proportional `MaxIdle` adjustment on resize (handled by underlying connpool)
- [x] Test resize up (capacity increase)
- [x] Test resize down (capacity decrease)
- [x] Test resize while connections are borrowed (context timeout supported)

**Implementation:**
- `regular.Pool.SetCapacity()` delegates to `connpool.Pool.SetCapacity()`
- `reserved.Pool.SetCapacity()` delegates to internal regular pool
- `UserPool.SetCapacity()` calls both with mutex protection
- 3 tests in `user_pool_test.go`: basic resize, with idle connections, closed pool

### Phase 5: Background Rebalancer ✅

- [x] Add configuration flags:
  - [x] `--connpool-rebalance-interval` (default 10s)
  - [x] `--connpool-demand-window` (default 30s)
  - [x] `--connpool-demand-sample-interval` (default 100ms)
  - [x] `--connpool-inactive-timeout` (default 5m)
- [x] Create `rebalancer` goroutine in Manager
- [x] Create `DemandTracker` instances per UserPool (regular + reserved)
- [x] Implement rebalance loop: collect demand → allocate → apply
- [x] Expose demand metrics in `UserPoolStats`
- [x] Add garbage collection for inactive user pools
- [x] Add logging for rebalancing events
- [x] Integration tests with multiple users joining/leaving

**Implementation:**
- Configuration flags in `go/multipooler/connpoolmanager/config.go`
- `DemandTracker` per UserPool created in `user_pool.go`
- Activity tracking via internal `touchActivity()` method, automatically called by all connection acquisition methods (`GetRegularConn`, `GetRegularConnWithSettings`, `NewReservedConn`, `GetReservedConn`)
- `UserPoolStats` now includes `RegularDemand`, `ReservedDemand`, and `LastActivity`
- `reserved.Pool` exposes `Requested()` method for demand sampling (cleaner API than exposing inner pool)
- New user pools start with initial capacity of 10 connections; rebalancer adjusts within seconds
- Per-user capacity/maxIdle config flags removed - rebalancer manages all capacity dynamically
- `rebalancer.go` implements the rebalance loop with periodic:
  1. Demand collection from all UserPools
  2. Fair share allocation via two `FairShareAllocator` instances
  3. Capacity application via `UserPool.SetCapacity()`
  4. Garbage collection of inactive pools (lastActivity > inactiveTimeout)
- Manager lifecycle manages rebalancer goroutine (start in `Open()`, stop in `Close()`)
- 10 integration tests in `rebalancer_test.go` covering rebalancer lifecycle, GC, and allocation

### Phase 6: End-to-End Testing ✅

- [x] Add e2e test: users with different workload patterns
- [x] Add e2e test: user arrival/departure during load
- [x] Add e2e test: capacity exhaustion and recovery
- [x] Add e2e test: graceful degradation under overload
- [x] Performance benchmarks comparing static vs dynamic allocation
- [x] Update documentation with final configuration recommendations

**Implementation:**
- Lightweight integration tests in `go/multipooler/connpoolmanager/dynamic_allocation_integration_test.go`:
  - `TestDynamicAllocation_DifferentWorkloadPatterns`
  - `TestDynamicAllocation_UserArrivalDuringLoad`
  - `TestDynamicAllocation_UserDepartureDuringLoad`
  - `TestDynamicAllocation_CapacityExhaustion`
  - `TestDynamicAllocation_CapacityRecovery`
  - `TestDynamicAllocation_GracefulDegradation`
- Full e2e tests with real PostgreSQL in `go/test/endtoend/dynamicpooling/`:
  - `TestE2E_MultiUserWorkload`
  - `TestE2E_UserArrivalDeparture`
  - `TestE2E_CapacityUnderLoad`
- Performance benchmarking: Build binaries from old and new code, run load tests against
  real PostgreSQL to compare throughput and latency

## Design

### Architecture Overview

```text
┌─────────────────────────────────────────────────────────────────────────────┐
│                              Manager                                         │
│                                                                              │
│  ┌────────────────────────────────┐  ┌────────────────────────────────┐     │
│  │  FairShareAllocator (Regular)  │  │  FairShareAllocator (Reserved) │     │
│  │  Capacity: 400 (80% of 500)    │  │  Capacity: 100 (20% of 500)    │     │
│  │                                │  │                                │     │
│  │  Allocations:                  │  │  Allocations:                  │     │
│  │  ├── User A: 80                │  │  ├── User A: 20                │     │
│  │  ├── User B: 90                │  │  ├── User B: 10                │     │
│  │  └── User C: 85                │  │  └── User C: 15                │     │
│  └────────────────────────────────┘  └────────────────────────────────┘     │
│                                                                              │
│  userPoolsSnapshot (atomic pointer - lock-free reads)                        │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐              │
│  │  UserPool[A]    │  │  UserPool[B]    │  │  UserPool[C]    │              │
│  │  Regular: 80    │  │  Regular: 90    │  │  Regular: 85    │              │
│  │  Reserved: 20   │  │  Reserved: 10   │  │  Reserved: 15   │              │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘              │
└─────────────────────────────────────────────────────────────────────────────┘
```

The `FairShareAllocator` is resource-agnostic - it allocates a single resource type.
We create two instances: one for regular pools, one for reserved pools. This mirrors
the `DemandTracker` design.

### Two Separate Resources

Regular and reserved pools serve fundamentally different workloads:

| Resource | Use Case | Typical Pattern |
|----------|----------|-----------------|
| **Regular** | Simple queries, reads | High throughput, short duration |
| **Reserved** | Transactions, cursors | Lower throughput, longer duration |

A transaction-heavy user needs more reserved capacity, while a read-heavy user
needs more regular capacity. Treating them as a single resource would lead to
suboptimal allocation.

**Configuration:**

```text
--connpool-global-capacity=500        # Total PostgreSQL connections
--connpool-reserved-ratio=0.2         # 20% reserved, 80% regular

Derived:
  globalRegularCapacity  = 500 * 0.8 = 400
  globalReservedCapacity = 500 * 0.2 = 100
```

**Initial Capacity:**

New user pools start with an initial capacity of 10 connections for both regular
and reserved pools. The rebalancer adjusts these capacities within seconds based
on actual demand. This approach:

- Allows new users to immediately start working without waiting for rebalancing
- Prevents over-allocation when many users connect simultaneously
- Lets the rebalancer quickly right-size pools based on actual usage

### Demand Metric: Peak Requested (Sliding Window)

To allocate based on actual demand, we track the **peak requested connections**
(not just borrowed) over a configurable sliding window. This captures true demand
including users who are waiting for connections.

**Pool-Level Tracking:**

The connection pool tracks a `requested` counter (similar to `borrowed`):
- Incremented when `Get()` is called (request starts)
- Decremented when `Get()` fails OR when `Recycle()` is called (request ends)

This allows reading demand directly from the pool without callbacks.

**Memory-Efficient Sliding Window:**

Instead of storing all samples, we use a bucketed approach:

```text
Configuration:
  --connpool-demand-window=30s       # Total sliding window
  --connpool-rebalance-interval=10s  # Poll/rebalance interval

Buckets = window / interval = 30s / 10s = 3 buckets
```

```text
┌─────────────────────────────────────────────────────────────────┐
│                    Sliding Window (3 buckets)                    │
│                                                                  │
│   Bucket 0        Bucket 1        Bucket 2 (current)            │
│   [t-30s,t-20s]   [t-20s,t-10s]   [t-10s,now]                   │
│   max=15          max=25          max=20                         │
│                                                                  │
│   Peak for window = max(15, 25, 20) = 25                        │
└─────────────────────────────────────────────────────────────────┘
```

**Sampling Strategy:**

- Sampling goroutine reads `requested` count from pool at configured interval
- Update current bucket's max: `bucket[current] = max(bucket[current], sample)`
- On rebalance: call `GetPeakAndRotate()` which returns max across buckets and rotates

**Why this approach?**

- **Memory efficient**: Only N integers (where N = window/poll_interval)
- **No callback overhead**: Just read atomic counter from pool
- **Accurate peaks**: Fast sampling catches most spikes
- **Sliding window**: Smooths out transient spikes while capturing sustained demand

**Configuration:**

```text
--connpool-demand-window=30s            # Sliding window duration (default)
--connpool-rebalance-interval=10s       # Poll/rebalance interval (default)
--connpool-demand-sample-interval=100ms # How often to sample (default)
```

### Fair Share Algorithm: Max-Min Fairness

We use [max-min fairness](https://en.wikipedia.org/wiki/Max-min_fairness) to
allocate capacity based on demand:

```text
Algorithm: Progressive Filling

1. Start with all allocations at 0
2. Increase all allocations equally until:
   a. A user's allocation reaches their demand (they're satisfied)
   b. Total allocation reaches global capacity (resource exhausted)
3. Satisfied users stop growing; continue increasing others
4. Repeat until all users satisfied or capacity exhausted

Example with globalRegularCapacity=400:

  User A demand: 150    User B demand: 100    User C demand: 80

  Step 1: Allocate equally until someone is satisfied
    A=80, B=80, C=80 (total=240) → C is satisfied (demand=80)

  Step 2: Continue with A and B
    A=110, B=100, C=80 (total=290) → B is satisfied (demand=100)

  Step 3: Continue with A only
    A=150, B=100, C=80 (total=330) → A is satisfied (demand=150)

  Remaining capacity: 400-330 = 70 (distributed proportionally or held in reserve)
```

**Bounds:**

- **Minimum**: 1 connection per user (hard floor to ensure usability)
- **Maximum**: Full global capacity (single user can use everything)

```text
1 user  with globalCapacity=400 → user gets 400
2 users with globalCapacity=400 → each gets up to 200 (based on demand)
400 users with globalCapacity=400 → each gets 1
```

### Rebalancing: Periodic Background Task

Rebalancing runs as a **background goroutine** at a configurable interval:

```text
--connpool-rebalance-interval=10s     # How often to rebalance
```

**Why purely periodic (not event-driven)?**

1. **Simplicity**: No complex event handling or race conditions
2. **Batching**: Multiple user arrivals/departures handled in one pass
3. **Stability**: Prevents oscillation from rapid changes
4. **Predictable**: Easier to reason about and debug

**Rebalance Flow:**

```text
┌─────────────────────────────────────────────────────────────────┐
│                    Rebalance Goroutine                           │
│                                                                  │
│  Every 10 seconds:                                               │
│                                                                  │
│  1. Collect demand metrics from all UserPools                    │
│     └─ regularTracker.GetPeakAndRotate() for each user          │
│     └─ reservedTracker.GetPeakAndRotate() for each user         │
│                                                                  │
│  2. Run fair share algorithm (two allocators, one per resource)  │
│     └─ regularAlloc.Allocate(regularDemands)                    │
│     └─ reservedAlloc.Allocate(reservedDemands)                  │
│                                                                  │
│  3. Apply new capacities                                         │
│     └─ pool.SetCapacity(ctx, newRegularCap, newReservedCap)     │
│        (may block briefly if shrinking and connections in use)  │
│                                                                  │
│  4. Garbage collect inactive pools                               │
│     └─ Remove pools with no activity for > inactivity threshold │
└─────────────────────────────────────────────────────────────────┘
```

### Lock-Free Hot Path: Atomic Snapshot

The current implementation holds a mutex on every `GetRegularConn` call:

```go
// Current: mutex on every request (bad)
func (m *Manager) getOrCreateUserPool(ctx context.Context, user string) (*UserPool, error) {
    m.mu.Lock()
    defer m.mu.Unlock()
    // ...
}
```

We replace this with an **atomic pointer to an immutable snapshot**:

```text
┌─────────────────────────────────────────────────────────────────┐
│                    Atomic Snapshot Pattern                       │
│                                                                  │
│  Hot Path (every query):                                         │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │  pools := m.userPoolsSnapshot.Load()  // atomic, no lock    │ │
│  │  if pool, ok := (*pools)[user]; ok {                        │ │
│  │      return pool, nil                 // fast path done     │ │
│  │  }                                                           │ │
│  └─────────────────────────────────────────────────────────────┘ │
│                                                                  │
│  Cold Path (new user arrives - rare):                            │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │  m.createMu.Lock()                    // serialize creates  │ │
│  │  defer m.createMu.Unlock()                                  │ │
│  │                                                              │ │
│  │  // Double-check after lock                                 │ │
│  │  pools := m.userPoolsSnapshot.Load()                        │ │
│  │  if pool, ok := (*pools)[user]; ok {                        │ │
│  │      return pool, nil                                       │ │
│  │  }                                                           │ │
│  │                                                              │ │
│  │  // Create new pool                                         │ │
│  │  pool := m.createUserPool(ctx, user)                        │ │
│  │                                                              │ │
│  │  // Copy-on-write: create new map                           │ │
│  │  newPools := make(map[string]*UserPool, len(*pools)+1)      │ │
│  │  for k, v := range *pools { newPools[k] = v }               │ │
│  │  newPools[user] = pool                                      │ │
│  │                                                              │ │
│  │  // Atomic publish                                          │ │
│  │  m.userPoolsSnapshot.Store(&newPools)                       │ │
│  └─────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

**Why this approach?**

- Hot path is a single atomic load + map lookup (nanoseconds)
- New users are rare; copy-on-write overhead is acceptable
- Iteration for stats/rebalancing works on consistent snapshot
- No lock contention between readers

## Configuration Summary

### Dynamic Allocation Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--connpool-global-capacity` | 100 | Total PostgreSQL connections to manage |
| `--connpool-reserved-ratio` | 0.2 | Fraction of global capacity for reserved pools |
| `--connpool-demand-window` | 30s | Sliding window for peak demand tracking |
| `--connpool-rebalance-interval` | 10s | How often to run rebalancing |
| `--connpool-demand-sample-interval` | 100ms | How often to sample pool demand |
| `--connpool-inactive-timeout` | 5m | Remove user pools after this inactivity |

### Per-User Pool Flags (Timeouts Only)

| Flag | Default | Description |
|------|---------|-------------|
| `--connpool-user-regular-idle-timeout` | 5m | How long a regular connection can remain idle |
| `--connpool-user-regular-max-lifetime` | 1h | Maximum lifetime of a regular connection |
| `--connpool-user-reserved-inactivity-timeout` | 30s | How long a reserved connection can be inactive |
| `--connpool-user-reserved-idle-timeout` | 5m | How long a reserved pool connection can remain idle |
| `--connpool-user-reserved-max-lifetime` | 1h | Maximum lifetime of a reserved connection |

**Note:** Per-user capacity and max-idle settings are managed automatically by the rebalancer.
New user pools start with an initial capacity of 10 connections and are adjusted within
seconds based on demand. A single user can use the full global capacity, and with many
users each could scale down to 1 connection.

### Derived Values

```text
globalRegularCapacity  = globalCapacity * (1 - reservedRatio)
globalReservedCapacity = globalCapacity * reservedRatio
```

### Example Configuration

```bash
multipooler \
  --connpool-global-capacity=500 \
  --connpool-reserved-ratio=0.2 \
  --connpool-demand-window=30s \
  --connpool-rebalance-interval=10s \
  --connpool-demand-sample-interval=100ms \
  --connpool-inactive-timeout=5m
```

With this configuration:
- Total capacity of 500 connections (400 regular, 100 reserved)
- New users start with 10 connections each
- Rebalancer adjusts capacities every 10 seconds based on 30-second demand window
- Inactive user pools are garbage collected after 5 minutes

## Implementation Plan

### Phase 1: Lock-Free Hot Path

**Goal:** Remove mutex contention from the query path.

**Changes:**

1. Replace `sync.Mutex` with atomic pointer to map snapshot in `Manager`
2. Implement copy-on-write for new user pool creation
3. Add separate mutex only for pool creation (cold path)

**Files:**

- `go/multipooler/connpoolmanager/manager.go`

**Validation:**

- Benchmark before/after to measure latency improvement
- Verify correctness with race detector (`go test -race`)

### Phase 2: Demand Tracking

**Goal:** Collect per-user demand metrics without changing allocation.

**Changes:**

1. Add `DemandTracker` struct with atomic counters
2. Track peak borrowed for regular and reserved pools
3. Add sliding window reset logic
4. Expose demand metrics in `UserPoolStats`

**Files:**

- `go/multipooler/connpoolmanager/demand_tracker.go` (new)
- `go/multipooler/connpoolmanager/user_pool.go`

**Validation:**

- Unit tests for demand tracking
- Verify metrics are accurate under load

### Phase 3: Fair Share Allocator

**Goal:** Implement the allocation algorithm as a separate, resource-agnostic module.

**Changes:**

1. Create `FairShareAllocator` struct (single-resource allocator)
2. Implement max-min fairness algorithm
3. Add configuration for global capacity and reserved ratio
4. Unit test allocation logic in isolation
5. Create two instances at runtime: one for regular, one for reserved

**Files:**

- `go/multipooler/connpoolmanager/fair_share_allocator.go` (new)
- `go/multipooler/connpoolmanager/config.go`

**Validation:**

- Extensive unit tests for allocation edge cases
- Property-based tests (total allocation <= capacity, all users >= min)

### Phase 4: UserPool Resize Support

**Goal:** Enable dynamic capacity changes on existing pools.

**Changes:**

1. Add `SetCapacity(ctx, regularCap, reservedCap)` to `UserPool`
2. Wire through to underlying `connpool.Pool.SetCapacity()`
3. Handle proportional MaxIdle adjustment

**Files:**

- `go/multipooler/connpoolmanager/user_pool.go`
- `go/multipooler/pools/regular/regular_pool.go`
- `go/multipooler/pools/reserved/reserved_pool.go`

**Validation:**

- Test resize up and down
- Test resize while connections are borrowed

### Phase 5: Background Rebalancer

**Goal:** Connect everything with periodic rebalancing.

**Changes:**

1. Add rebalancer goroutine to `Manager`
2. Collect demand → calculate allocation → apply capacities
3. Add garbage collection for inactive user pools
4. Add logging and metrics for rebalancing events

**Files:**

- `go/multipooler/connpoolmanager/manager.go`
- `go/multipooler/connpoolmanager/rebalancer.go` (new)

**Validation:**

- Integration tests with multiple users joining/leaving
- Verify fair allocation under various demand patterns
- Benchmark to ensure rebalancing doesn't impact query latency

### Phase 6: End-to-End Testing

**Goal:** Validate the complete system under realistic conditions.

**Changes:**

1. Add e2e tests in `go/test/endtoend/queryserving/`
2. Test scenarios:
   - Users with different workload patterns
   - User arrival/departure during load
   - Capacity exhaustion and recovery
   - Graceful degradation under overload

**Validation:**

- All scenarios pass
- Performance benchmarks show improvement over static allocation

## Production Configuration Recommendations

### Sizing Global Capacity

Set `--connpool-global-capacity` based on your PostgreSQL `max_connections` setting:

```text
global-capacity = max_connections - reserved_for_admin - reserved_for_replication

Example:
  max_connections = 500
  reserved_for_admin = 5 (superuser connections)
  reserved_for_replication = 10 (streaming replicas)
  global-capacity = 500 - 5 - 10 = 485
```

**Guidelines:**

| Workload Type | Reserved Ratio | Notes |
|---------------|----------------|-------|
| Read-heavy (OLAP, analytics) | 0.1 - 0.15 | Few transactions, many simple queries |
| Mixed (typical web app) | 0.2 (default) | Balance of reads and transactions |
| Write-heavy (OLTP) | 0.25 - 0.3 | Many transactions, prepared statements |
| Transaction-intensive | 0.3 - 0.4 | Heavy use of cursors, long transactions |

### Tuning for Workload Patterns

**High User Churn (many short-lived connections):**

```bash
--connpool-rebalance-interval=5s     # Faster rebalancing
--connpool-inactive-timeout=2m       # Faster GC of idle pools
--connpool-demand-window=15s         # Shorter demand history
```

**Stable User Base (long-lived connections):**

```bash
--connpool-rebalance-interval=30s    # Less frequent rebalancing
--connpool-inactive-timeout=15m      # Longer idle tolerance
--connpool-demand-window=60s         # Longer demand history for stability
```

**Bursty Workloads (periodic spikes):**

```bash
--connpool-rebalance-interval=10s    # Default
--connpool-demand-window=60s         # Capture burst patterns
--connpool-demand-sample-interval=50ms  # Finer-grained sampling
```

### Monitoring Recommendations

Monitor these key metrics to validate allocation behavior:

1. **Per-user allocation vs demand:**
   - Check `UserPoolStats.Regular.Capacity` vs `UserPoolStats.RegularDemand`
   - If demand consistently exceeds capacity, consider increasing global capacity

2. **Connection utilization:**
   - Monitor `UserPoolStats.Regular.Borrowed` / `UserPoolStats.Regular.Capacity`
   - High utilization (>80%) across many users indicates capacity pressure

3. **Rebalancer activity:**
   - Log messages show when capacities are adjusted
   - Frequent large adjustments may indicate demand-window is too short

4. **Inactive pool garbage collection:**
   - Monitor `Manager.UserPoolCount()` over time
   - Unexpected growth may indicate inactive-timeout is too long

### Example Production Configuration

**Small deployment (< 100 users, PostgreSQL max_connections=200):**

```bash
multipooler \
  --connpool-global-capacity=180 \
  --connpool-reserved-ratio=0.2 \
  --connpool-rebalance-interval=10s \
  --connpool-demand-window=30s \
  --connpool-inactive-timeout=5m
```

**Medium deployment (100-500 users, PostgreSQL max_connections=500):**

```bash
multipooler \
  --connpool-global-capacity=480 \
  --connpool-reserved-ratio=0.2 \
  --connpool-rebalance-interval=10s \
  --connpool-demand-window=30s \
  --connpool-inactive-timeout=5m
```

**Large deployment (500+ users, PostgreSQL max_connections=1000):**

```bash
multipooler \
  --connpool-global-capacity=950 \
  --connpool-reserved-ratio=0.2 \
  --connpool-rebalance-interval=15s \
  --connpool-demand-window=45s \
  --connpool-inactive-timeout=10m
```

### Common Issues and Solutions

| Symptom | Likely Cause | Solution |
|---------|--------------|----------|
| New users get slow connections | Initial capacity too low, rebalance too slow | Decrease `rebalance-interval` |
| Idle users consuming capacity | Inactive timeout too long | Decrease `inactive-timeout` |
| Capacity oscillating rapidly | Demand window too short | Increase `demand-window` |
| Users starving under load | Global capacity too low | Increase `global-capacity` or reduce users |
| Too many reserved connections | Reserved ratio too high for workload | Decrease `reserved-ratio` |

## Future Enhancements

### Weighted Fair Share

Allow different users to have different weights (e.g., based on subscription tier):

```text
--connpool-user-weight-file=/path/to/weights.json

{
  "premium_user": 2.0,
  "standard_user": 1.0,
  "free_user": 0.5
}
```

### Predictive Scaling

Use historical patterns to predict demand and pre-allocate:

- Time-of-day patterns
- Day-of-week patterns
- Trend detection

### Cross-Multipooler Coordination

For deployments with multiple multipooler instances, coordinate allocation
through the topology service to ensure global fairness.

## References

- [Wikipedia: Max-min fairness](https://en.wikipedia.org/wiki/Max-min_fairness)
- [Dominant Resource Fairness (DRF)](https://amplab.cs.berkeley.edu/wp-content/uploads/2011/06/Dominant-Resource-Fairness-Fair-Allocation-of-Multiple-Resource-Types.pdf)
- [Cloudflare: Performance isolation in multi-tenant databases](https://blog.cloudflare.com/performance-isolation-in-a-multi-tenant-database-environment/)
- [HikariCP: About Pool Sizing](https://github.com/brettwooldridge/HikariCP/wiki/About-Pool-Sizing)
- [PgBouncer Configuration](https://www.pgbouncer.org/config.html)
