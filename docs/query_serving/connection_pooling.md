# Connection Pooling in MultiGres

## Overview

MultiGres implements a **per-user connection pooling** architecture in the
MultiPooler service to efficiently manage PostgreSQL connections. Each user
gets their own dedicated connection pools that authenticate directly as that
user via trust/peer authentication. This design ensures strong security
isolation while supporting Row-Level Security (RLS) policies.

Pool capacities are managed dynamically using **fair share allocation** -- the
system distributes the global connection budget among active users based on
actual demand and automatically rebalances as users come and go.

## Architecture

<!-- markdownlint-disable MD013 -->

```text
┌──────────────────────────────────────────────────────────────────────────────────┐
│                           ConnectionPoolManager                                  │
│                                                                                  │
│  ┌──────────────┐                                                                │
│  │  AdminPool   │  (shared - postgres superuser)                                 │
│  │  AdminConn   │                                                                │
│  │  AdminConn   │                                                                │
│  └──────────────┘                                                                │
│                                                                                  │
│  ┌────────────────────────────────┐  ┌────────────────────────────────┐          │
│  │  FairShareAllocator (Regular)  │  │  FairShareAllocator (Reserved) │          │
│  │  Capacity: 400 (80% of 500)    │  │  Capacity: 100 (20% of 500)    │          │
│  └────────────────────────────────┘  └────────────────────────────────┘          │
│                                                                                  │
│  userPoolsSnapshot (atomic pointer - lock-free reads)                            │
│  ┌──────────────────────────────────────────────────────────────────────────┐    │
│  │  UserPool["alice"]                                                       │    │
│  │  ┌──────────────────┐   ┌──────────────────────────┐                     │    │
│  │  │   RegularPool    │   │      ReservedPool        │                     │    │
│  │  │   (user: alice)  │   │      (user: alice)       │                     │    │
│  │  │   Capacity: 80   │   │      Capacity: 20        │                     │    │
│  │  │                  │   │ ┌─────────────────────┐  │                     │    │
│  │  │ RegularConn      │   │ │ internal RegularPool│  │                     │    │
│  │  │   └─ ConnState   │   │ └─────────────────────┘  │                     │    │
│  │  │      └─ Settings │   │ ReservedConn             │                     │    │
│  │  │      └─ Stmts    │   │   └─ ConnID              │                     │    │
│  │  └──────────────────┘   └──────────────────────────┘                     │    │
│  │  DemandTracker (regular)  DemandTracker (reserved)                       │    │
│  └──────────────────────────────────────────────────────────────────────────┘    │
│  ┌──────────────────────────────────────────────────────────────────────────┐    │
│  │  UserPool["bob"]                                                         │    │
│  │  ┌──────────────────┐   ┌──────────────────────────┐                     │    │
│  │  │   RegularPool    │   │      ReservedPool        │                     │    │
│  │  │   (user: bob)    │   │      (user: bob)         │                     │    │
│  │  │       ...        │   │          ...             │                     │    │
│  │  └──────────────────┘   └──────────────────────────┘                     │    │
│  └──────────────────────────────────────────────────────────────────────────┘    │
│                                                                                  │
│  Rebalancer goroutine (periodic: collect demand → allocate → apply → GC)        │
└──────────────────────────────────────────────────────────────────────────────────┘
```

<!-- markdownlint-enable MD013 -->

## Connection Pool Types

### 1. AdminPool (Shared)

**Purpose:** Control plane operations for connection management.

**Characteristics:**

- Small pool size (default: 5 connections)
- Connects as PostgreSQL superuser (default: `postgres`)
- Shared across all users
- Used exclusively for `pg_terminate_backend()` operations

**Use Cases:**

- Canceling long-running queries
- Terminating connections when clients disconnect unexpectedly
- Killing timed-out reserved connections

### 2. UserPool (Per-User)

Each user gets their own `UserPool` containing both a RegularPool and ReservedPool.
Pools are created **lazily** on first connection request for that user. New pools
start with an initial capacity of 10 connections; the background rebalancer adjusts
capacities within seconds based on actual demand.

#### RegularPool

**Purpose:** Data plane connections for query execution with session state.

**Characteristics:**

- Authenticates directly as the user (trust/peer auth)
- Supports the Extended Query Protocol (Parse/Bind/Execute)
- Tracks connection state including settings and prepared statements
- Uses settings-based bucket routing for connection reuse

**Use Cases:**

- Simple queries without transactions
- Queries that can be executed on any available connection with matching settings

**Context Cancellation:**

RegularConn handles Go context cancellation by cancelling the backend query via
AdminPool's `pg_cancel_backend()`. The underlying protocol client always drains
messages until `ReadyForQuery` to keep connections clean. Connection-level errors
(read failures, broken pipes) cause the connection to be closed rather than
returned to the pool.

#### ReservedPool

**Purpose:** Long-lived connections for transactions and portal operations.

**Characteristics:**

- Fully encapsulates its own underlying RegularPool (completely separate from
  the main RegularPool)
- Authenticates directly as the user (trust/peer auth)
- Assigns unique connection IDs via atomic counter for client-side reference
- Maintains an `active map[int64]*Conn` for ID-based lookup
- Tracks reservation state via `reservedProps` (transaction or portal reservation)
- Includes background `idleKiller` goroutine for timed-out connections

**Use Cases:**

- Explicit transactions (`BEGIN`/`COMMIT`/`ROLLBACK`)
- Cursor operations requiring persistent portal state
- Any operation requiring connection affinity

**Timeout Handling:**

Reserved connections have two timeout configurations:

| Timeout            | Default | Purpose                                                         |
| ------------------ | ------- | --------------------------------------------------------------- |
| Inactivity Timeout | 30s     | Kills reserved connections when client is inactive (aggressive) |
| Idle Timeout       | 5min    | Reduces pool size when connections sit idle in pool             |

A background goroutine runs at 1/10th of the inactivity timeout interval to scan
and kill connections that have exceeded their timeout. Each time a connection is
accessed via `Get()`, its expiry time is reset.

## Dynamic Fair Share Allocation

### Problem

Static per-user pool sizes create a fundamental problem when the number of users
is unknown and PostgreSQL's `max_connections` is fixed:

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

### Design Goals

1. **Fair allocation**: Each user gets a fair share of the global connection budget
2. **Adaptive**: Automatically rebalance as users come and go
3. **Demand-aware**: Allocate based on actual usage, not just equal splits
4. **Non-blocking**: Rebalancing happens in background, not on the query hot path
5. **Separate resources**: Regular and reserved pools have independent budgets

### Two Separate Resources

Regular and reserved pools serve fundamentally different workloads and are
allocated independently:

| Resource     | Use Case              | Typical Pattern                   |
| ------------ | --------------------- | --------------------------------- |
| **Regular**  | Simple queries, reads | High throughput, short duration   |
| **Reserved** | Transactions, cursors | Lower throughput, longer duration |

The global capacity is divided between the two resource types:

```text
--connpool-global-capacity=500        # Total PostgreSQL connections
--connpool-reserved-ratio=0.2         # 20% reserved, 80% regular

Derived:
  globalRegularCapacity  = 500 * 0.8 = 400
  globalReservedCapacity = 500 * 0.2 = 100
```

A `FairShareAllocator` instance manages each resource type independently. This
mirrors the `DemandTracker` design (also resource-agnostic, one per pool type).

### Demand Tracking

To allocate based on actual demand, we track the **peak requested connections**
(not just borrowed) over a configurable sliding window. This captures true demand
including users who are waiting for connections.

**Pool-Level Tracking:**

The connection pool tracks a `requested` counter (similar to `borrowed`):

- Incremented when `Get()` is called (request starts)
- Decremented when `Get()` fails OR when `Recycle()` is called (request ends)

**Memory-Efficient Sliding Window:**

Instead of storing all samples, we use a bucketed approach:

```text
Configuration:
  --connpool-demand-window=30s       # Total sliding window
  --connpool-rebalance-interval=10s  # Rebalance interval

Buckets = window / interval = 30s / 10s = 3 buckets

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

A sampling goroutine reads the pool's `requested` counter at a configured interval
and updates the current bucket's max. On rebalance, `GetPeakAndRotate()` returns
the max across all buckets and rotates to the next bucket.

### Fair Share Algorithm

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

  Remaining capacity: 400-330 = 70 (held in reserve)
```

**Bounds:**

- **Minimum**: 1 connection per user (hard floor to ensure usability)
- **Maximum**: Full global capacity (single user can use everything)

```text
1 user  with globalCapacity=400 → user gets 400
2 users with globalCapacity=400 → each gets up to 200 (based on demand)
400 users with globalCapacity=400 → each gets 1
```

### Background Rebalancer

Rebalancing runs as a **periodic background goroutine**:

```text
┌─────────────────────────────────────────────────────────────────┐
│                    Rebalance Goroutine                           │
│                                                                  │
│  Every 10 seconds (configurable):                                │
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
│        (non-blocking - excess borrowed connections closed on     │
│         recycle)                                                 │
│                                                                  │
│  4. Garbage collect inactive pools                               │
│     └─ Remove pools with no activity for > inactive timeout     │
└─────────────────────────────────────────────────────────────────┘
```

**Why purely periodic (not event-driven)?**

1. **Simplicity**: No complex event handling or race conditions
2. **Batching**: Multiple user arrivals/departures handled in one pass
3. **Stability**: Prevents oscillation from rapid changes
4. **Predictable**: Easier to reason about and debug

### Lock-Free Hot Path

The pool lookup on every query uses an **atomic pointer to an immutable snapshot**
instead of taking a mutex:

```text
Hot Path (every query):
  pools := m.userPoolsSnapshot.Load()  // atomic, no lock
  if pool, ok := (*pools)[user]; ok {
      return pool, nil                 // fast path done
  }

Cold Path (new user arrives - rare):
  m.createMu.Lock()                    // serialize creates only
  // Double-check after lock
  // Copy-on-write: new map with new pool
  // Atomic publish
  m.userPoolsSnapshot.Store(&newPools)
```

This achieves sub-nanosecond pool lookups. Under 10-goroutine parallel load,
the lock-free path provides an **~11x throughput improvement** over the previous
mutex-based approach.

## Settings Management

### Settings Stack

The connection pool uses a **settings stack** architecture where each unique
combination of session settings maps to a dedicated bucket of connections. This
enables efficient connection reuse when clients have matching settings.

**Key Properties:**

- Settings are defined by session variables set via `SET` commands
- Each unique settings combination gets a unique bucket number
- Connections are routed to stacks based on their bucket number
- Clean connections (no settings) use bucket 0

### Settings Cache

The connection pool manager maintains a shared `SettingsCache` that provides
bounded LRU caching for settings configurations. When callers provide settings
as a `map[string]string`, the manager internally converts them via the cache,
ensuring the same settings configuration always returns the same `*Settings`
pointer. This enables:

1. **Pointer equality for fast comparison** - Instead of comparing full maps
2. **Consistent bucket assignment** - Same settings always route to same bucket
3. **Reduced memory allocation** - Repeated settings configurations are reused
4. **Simplified API** - Callers just pass `map[string]string`, caching is automatic

```go
// Example: Settings are passed as a map, caching is handled internally
regularConn, _ := mgr.GetRegularConnWithSettings(ctx, map[string]string{
    "statement_timeout": "30s",
    "search_path":       "public,app",
}, user)
```

## User Management and RLS

### Per-User Connection Pools

The connection pool manager creates **separate pools for each user**. Each pool
authenticates directly as that PostgreSQL user via trust/peer authentication.
This design provides:

1. **Strong Security Isolation** - No role switching needed; connections are
   always authenticated as the correct user
2. **RLS Compatibility** - Row-Level Security policies see the correct
   `current_user` because connections actually belong to that user
3. **No Privilege Escalation** - A compromised connection cannot access another
   user's data since it has no privilege to change roles

### Connection Flow

**Simple Query (RegularPool):**

```text
1. Client request arrives for user "alice" with settings
2. Manager.GetRegularConn(ctx, "alice")
   └─ getOrCreateUserPool("alice") → creates pool if needed
   └─ userPool.GetRegularConn(ctx)
3. Execute user's query (RLS sees current_user = alice)
4. pooled.Recycle() → returns to alice's pool
```

**Transaction (ReservedPool):**

```text
1. Client request arrives for user "alice", BEGIN transaction
2. Manager.NewReservedConn(ctx, settings, "alice")
   └─ getOrCreateUserPool("alice")
   └─ userPool.NewReservedConn(ctx, settings)
      ├─ Gets RegularConn from alice's reserved pool
      ├─ Wraps in reserved.Conn with unique ConnID
      └─ Registers in active map
3. reserved.Begin() → executes BEGIN
4. Return ConnID to client
   ... client sends more queries with same ConnID ...
5. Manager.GetReservedConn(connID, "alice") → retrieves connection, resets expiry
6. reserved.Query(ctx, "INSERT ...")
   ... eventually ...
7. reserved.Commit() → executes COMMIT
8. reserved.Release(ReleaseCommit)
```

## Important Usage Guidelines

### Pool Pollution from SET ROLE

**Users must not execute `SET ROLE` commands directly in their queries.**

While each user has their own connection pool, executing `SET ROLE` within a
session would pollute that connection:

1. The connection's actual PostgreSQL role changes
2. Subsequent queries on that connection see incorrect `current_user`
3. RLS policies may return incorrect data
4. The connection state becomes inconsistent with the pool's expectations

**What We Cannot Prevent:**

- `SET ROLE` inside stored procedures or functions (PL/pgSQL)
- `SET ROLE` via dynamic SQL in procedural languages (PL/Python, PL/Perl, etc.)
- Any server-side code that modifies the session role

**What We Plan to Prevent (Future Work):**

- Direct `SET ROLE` commands in simple queries (detected and rejected)
- `SET SESSION AUTHORIZATION` commands

**Client Responsibility:**

Until query-level detection is implemented, it is the client's responsibility
to ensure that:

- Application code does not execute `SET ROLE` statements
- Stored procedures and functions do not change session roles
- If role changes are required within procedures, they must `RESET ROLE` before returning

If your application legitimately requires role changes within procedures, ensure
they are properly scoped and reset before the procedure returns.

### Session Variable Considerations

Session variables set via `SET` commands affect connection routing:

- Connections with identical settings share a bucket within a user's pool
- Different settings create separate buckets

## ConnectionPoolManager

The `Manager` in `go/multipooler/connpoolmanager/` orchestrates all pool types,
providing:

1. **Per-User Pool Management** - Lazy creation of user pools on first request
2. **Lock-Free Lookups** - Atomic pointer to immutable map snapshot for zero-contention reads
3. **Dynamic Capacity Management** - Background rebalancer adjusts pool sizes based on demand
4. **Unified Interface** - Single entry point for connection acquisition
5. **Pool Selection** - Routes requests to appropriate pool based on operation
6. **Lifecycle Management** - Handles connection creation, validation, and cleanup
7. **Inactive Pool GC** - Automatically removes user pools after configurable inactivity
8. **Metrics** - Exposes per-user pool statistics including demand and activity

### Usage

```go
import "github.com/multigres/multigres/go/multipooler/connpoolmanager"

// Create config with viper registry
cfg := connpoolmanager.NewConfig(reg)

// Register flags before parsing
cfg.RegisterFlags(cmd.Flags())

// After flag parsing, create and open the manager
mgr := cfg.NewManager()
connConfig := &connpoolmanager.ConnectionConfig{
    SocketFile: socketFilePath,
    Port:       pgPort,
    Database:   database,
}
mgr.Open(ctx, logger, connConfig)
defer mgr.Close()

// Get connections as needed
adminConn, _ := mgr.GetAdminConn(ctx)
regularConn, _ := mgr.GetRegularConn(ctx, user)
regularConnWithSettings, _ := mgr.GetRegularConnWithSettings(ctx, settings, user)
reservedConn, _ := mgr.NewReservedConn(ctx, settings, user)

// Resume a reserved connection by ID
conn, ok := mgr.GetReservedConn(connID, user)
```

### Interface for Testing

The `PoolManager` interface allows components to mock the manager in tests:

```go
type PoolManager interface {
    Open(ctx context.Context, logger *slog.Logger, connConfig *ConnectionConfig)
    Close()

    // Admin pool operations
    GetAdminConn(ctx context.Context) (admin.PooledConn, error)

    // Regular pool operations (per-user)
    GetRegularConn(ctx context.Context, user string) (regular.PooledConn, error)
    GetRegularConnWithSettings(ctx context.Context, settings map[string]string, user string) (regular.PooledConn, error)

    // Reserved pool operations (per-user)
    NewReservedConn(ctx context.Context, settings map[string]string, user string) (*reserved.Conn, error)
    GetReservedConn(connID int64, user string) (*reserved.Conn, bool)

    Stats() ManagerStats
}
```

## Configuration

The connection pool manager is configured via command-line flags (backed by viper).

### Dynamic Allocation Flags

These flags control how pool capacities are distributed across users:

| Flag                                | Default | Description                                    |
| ----------------------------------- | ------- | ---------------------------------------------- |
| `--connpool-global-capacity`        | 100     | Total PostgreSQL connections to manage         |
| `--connpool-reserved-ratio`         | 0.2     | Fraction of global capacity for reserved pools |
| `--connpool-rebalance-interval`     | 10s     | How often to run rebalancing                   |
| `--connpool-demand-window`          | 30s     | Sliding window for peak demand tracking        |
| `--connpool-demand-sample-interval` | 100ms   | How often to sample pool demand                |
| `--connpool-inactive-timeout`       | 5m      | Remove user pools after this inactivity        |

Derived values:

```text
globalRegularCapacity  = globalCapacity * (1 - reservedRatio)
globalReservedCapacity = globalCapacity * reservedRatio
```

### Admin Pool Flags

| Flag                        | Default    | Env Var                   | Description                            |
| --------------------------- | ---------- | ------------------------- | -------------------------------------- |
| `--connpool-admin-user`     | `postgres` | `CONNPOOL_ADMIN_USER`     | Admin pool user (PostgreSQL superuser) |
| `--connpool-admin-password` | -          | `CONNPOOL_ADMIN_PASSWORD` | Admin pool password                    |
| `--connpool-admin-capacity` | 5          | -                         | Maximum admin connections              |

### Per-User Pool Flags (Timeouts Only)

Pool capacities are managed automatically by the rebalancer. New user pools start
with an initial capacity of 10 connections and are adjusted within seconds based on
demand. These flags control timeout behavior only:

| Flag                                          | Default | Description                                     |
| --------------------------------------------- | ------- | ----------------------------------------------- |
| `--connpool-user-regular-idle-timeout`        | 5m      | Idle timeout before closing regular connections |
| `--connpool-user-regular-max-lifetime`        | 1h      | Maximum lifetime before recycling               |
| `--connpool-user-reserved-inactivity-timeout` | 30s     | Inactivity timeout for reserved connections     |
| `--connpool-user-reserved-idle-timeout`       | 5m      | Idle timeout for underlying pool                |
| `--connpool-user-reserved-max-lifetime`       | 1h      | Maximum lifetime before recycling               |

### Other Flags

| Flag                             | Default | Description                                             |
| -------------------------------- | ------- | ------------------------------------------------------- |
| `--connpool-max-users`           | 0       | Maximum number of user pools (0 = unlimited)            |
| `--connpool-settings-cache-size` | 1024    | Maximum number of unique settings combinations to cache |

**Note:** Connection settings (socket file, port, database) use the existing multipooler flags
(`--socket-file`, `--pg-port`, `--database`) and are passed to the connection pool manager
via `ConnectionConfig`.

### Example Configuration

```bash
multipooler \
  --connpool-global-capacity=500 \
  --connpool-reserved-ratio=0.2 \
  --connpool-rebalance-interval=10s \
  --connpool-demand-window=30s \
  --connpool-demand-sample-interval=100ms \
  --connpool-inactive-timeout=5m
```

With this configuration:

- Total capacity of 500 connections (400 regular, 100 reserved)
- New users start with 10 connections each
- Rebalancer adjusts capacities every 10 seconds based on 30-second demand window
- Inactive user pools are garbage collected after 5 minutes

## Statistics

The connection pool manager exposes statistics for monitoring via `Stats()`:

```go
stats := mgr.Stats()

// ManagerStats structure:
type ManagerStats struct {
    Admin     connpool.PoolStats           // Shared admin pool stats
    UserPools map[string]UserPoolStats     // Per-user pool stats
}

type UserPoolStats struct {
    Username       string
    Regular        connpool.PoolStats
    Reserved       reserved.PoolStats
    RegularDemand  int64 // Peak demand from tracker (0 if tracking not enabled)
    ReservedDemand int64 // Peak demand from tracker (0 if tracking not enabled)
    LastActivity   int64 // Unix nanos of last activity
}
```

## Future Improvements

### Single Connection Pool Architecture

A potential future improvement would be consolidating per-user pools into a
single connection pool that uses `SET ROLE` to switch users. This would provide:

- **Better Connection Utilization** - Connections shared across all users
- **Reduced Memory Footprint** - Fewer total connections to PostgreSQL
- **Simpler Pool Management** - Single pool instead of N user pools

**Why We Don't Do This Today:**

The primary blocker is **security**. A single pool would require a service
account with privileges to `SET ROLE` to any user. This creates privilege
escalation risks:

1. **Stored Procedure Exploitation** - Users can execute `SET ROLE` inside
   PL/pgSQL functions, and we cannot prevent this server-side
2. **Dynamic SQL in Procedural Languages** - PL/Python, PL/Perl, and other
   procedural languages can construct and execute `SET ROLE` statements
3. **Pool Pollution Attack** - A malicious user could `SET ROLE` to another
   user inside a procedure, and the connection would be returned to the pool
   with elevated privileges

As noted in the [Pool Pollution from SET ROLE](#pool-pollution-from-set-role)
section, we cannot fully prevent role switching inside server-side code. Until
PostgreSQL provides mechanisms to restrict `SET ROLE` within sessions (e.g.,
connection-level restrictions or procedure sandboxing), the single-pool
architecture poses unacceptable security risks.

**Potential Mitigations (Future Investigation):**

- PostgreSQL extensions to restrict `SET ROLE` per-connection
- Audit logging with real-time detection of unauthorized role switches
- Sandboxed execution environments for stored procedures

### SET ROLE Detection

Currently, direct `SET ROLE` and `SET SESSION AUTHORIZATION` commands in client
queries are not detected or rejected. Implementing query-level detection would:

- Parse incoming queries to detect role-changing statements
- Reject queries containing `SET ROLE` or `SET SESSION AUTHORIZATION`
- Protect against accidental pool pollution from client code

This detection would be implemented in MultiGateway, which already parses
queries and serves as the entry point for client connections.

### Password Authentication Support

Currently, per-user connection pools rely on trust/peer authentication
configured in `pg_hba.conf`. This requires the connection pooler to run on the
same host as PostgreSQL or have appropriate Unix socket access.

Future improvements could add support for:

- **Password-based authentication** - Store and use per-user credentials
- **Certificate authentication** - Use client certificates for user identity
- **External credential stores** - Integration with secret management systems

This would enable more flexible deployment topologies where the connection
pooler runs on separate hosts from PostgreSQL.

## Related Documentation

- [Prepared Statements Design](./prepared_statements_design.md) - Extended Query
  Protocol and statement management

## References

- [Wikipedia: Max-min fairness](https://en.wikipedia.org/wiki/Max-min_fairness)
- [Dominant Resource Fairness (DRF)](https://amplab.cs.berkeley.edu/wp-content/uploads/2011/06/Dominant-Resource-Fairness-Fair-Allocation-of-Multiple-Resource-Types.pdf)
