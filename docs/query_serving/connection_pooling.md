# Connection Pooling in MultiGres

## Overview

MultiGres implements a **per-user connection pooling** architecture in the
MultiPooler service to efficiently manage PostgreSQL connections. Each user
gets their own dedicated connection pools that authenticate directly as that
user via trust/peer authentication. This design ensures strong security
isolation while supporting Row-Level Security (RLS) policies.

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
│  userPools map[string]*UserPool                                                  │
│  ┌──────────────────────────────────────────────────────────────────────────┐    │
│  │  UserPool["alice"]                                                       │    │
│  │  ┌──────────────────┐   ┌──────────────────────────┐                     │    │
│  │  │   RegularPool    │   │      ReservedPool        │                     │    │
│  │  │   (user: alice)  │   │      (user: alice)       │                     │    │
│  │  │                  │   │ ┌─────────────────────┐  │                     │    │
│  │  │ RegularConn      │   │ │ internal RegularPool│  │                     │    │
│  │  │   └─ ConnState   │   │ └─────────────────────┘  │                     │    │
│  │  │      └─ Settings │   │ ReservedConn             │                     │    │
│  │  │      └─ Stmts    │   │   └─ ConnID              │                     │    │
│  │  └──────────────────┘   └──────────────────────────┘                     │    │
│  └──────────────────────────────────────────────────────────────────────────┘    │
│  ┌──────────────────────────────────────────────────────────────────────────┐    │
│  │  UserPool["bob"]                                                         │    │
│  │  ┌──────────────────┐   ┌──────────────────────────┐                     │    │
│  │  │   RegularPool    │   │      ReservedPool        │                     │    │
│  │  │   (user: bob)    │   │      (user: bob)         │                     │    │
│  │  │       ...        │   │          ...             │                     │    │
│  │  └──────────────────┘   └──────────────────────────┘                     │    │
│  └──────────────────────────────────────────────────────────────────────────┘    │
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
Pools are created **lazily** on first connection request for that user.

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
2. **Unified Interface** - Single entry point for connection acquisition
3. **Pool Selection** - Routes requests to appropriate pool based on operation
4. **Lifecycle Management** - Handles connection creation, validation, and cleanup
5. **Metrics** - Exposes per-user pool statistics for monitoring

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

### Available Flags

**Admin Pool Flags:**

| Flag                        | Default    | Env Var                   | Description                            |
| --------------------------- | ---------- | ------------------------- | -------------------------------------- |
| `--connpool-admin-user`     | `postgres` | `CONNPOOL_ADMIN_USER`     | Admin pool user (PostgreSQL superuser) |
| `--connpool-admin-password` | -          | `CONNPOOL_ADMIN_PASSWORD` | Admin pool password                    |
| `--connpool-admin-capacity` | 5          | -                         | Maximum admin connections              |

**Per-User Regular Pool Flags:**

| Flag                                   | Default | Description                                     |
| -------------------------------------- | ------- | ----------------------------------------------- |
| `--connpool-user-regular-capacity`     | 10      | Maximum regular connections per user            |
| `--connpool-user-regular-max-idle`     | 5       | Maximum idle regular connections per user       |
| `--connpool-user-regular-idle-timeout` | 5m      | Idle timeout before closing regular connections |
| `--connpool-user-regular-max-lifetime` | 1h      | Maximum lifetime before recycling               |

**Per-User Reserved Pool Flags:**

| Flag                                          | Default | Description                                 |
| --------------------------------------------- | ------- | ------------------------------------------- |
| `--connpool-user-reserved-capacity`           | 5       | Maximum reserved connections per user       |
| `--connpool-user-reserved-max-idle`           | 2       | Maximum idle reserved connections per user  |
| `--connpool-user-reserved-inactivity-timeout` | 30s     | Inactivity timeout for reserved connections |
| `--connpool-user-reserved-idle-timeout`       | 5m      | Idle timeout for underlying pool            |
| `--connpool-user-reserved-max-lifetime`       | 1h      | Maximum lifetime before recycling           |

**User Pool Limits:**

| Flag                   | Default | Description                                  |
| ---------------------- | ------- | -------------------------------------------- |
| `--connpool-max-users` | 0       | Maximum number of user pools (0 = unlimited) |

**Settings Cache:**

| Flag                             | Default | Description                                             |
| -------------------------------- | ------- | ------------------------------------------------------- |
| `--connpool-settings-cache-size` | 1024    | Maximum number of unique settings combinations to cache |

**Note:** Connection settings (socket file, port, database) use the existing multipooler flags
(`--socket-file`, `--pg-port`, `--database`) and are passed to the connection pool manager
via `ConnectionConfig`.

### Example Usage

```go
import "github.com/multigres/multigres/go/multipooler/connpoolmanager"

// In your service initialization
reg := viperutil.NewRegistry()

// Create config and register flags (before flag parsing)
cfg := connpoolmanager.NewConfig(reg)
cfg.RegisterFlags(cmd.Flags())

// After flag parsing, create manager and open pools
mgr := cfg.NewManager()
connConfig := &connpoolmanager.ConnectionConfig{
    SocketFile: socketFilePath,  // From --socket-file flag
    Port:       pgPort,          // From --pg-port flag
    Database:   database,        // From --database flag
}
mgr.Open(ctx, logger, connConfig)
defer mgr.Close()
```

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
    Username string
    Regular  connpool.PoolStats
    Reserved reserved.PoolStats
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
