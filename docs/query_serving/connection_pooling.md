# Connection Pooling in MultiGres

## Overview

MultiGres implements a sophisticated connection pooling architecture in the
MultiPooler service to efficiently manage PostgreSQL connections. The design
balances connection reuse, session state isolation, and Row-Level Security
(RLS) compatibility.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                     ConnectionPoolManager                           │
│                                                                     │
│  ┌──────────────┐   ┌──────────────────┐   ┌────────────────────┐   │
│  │  AdminPool   │   │   RegularPool    │   │   ReservedPool     │   │
│  │              │   │                  │   │                    │   │
│  │ AdminConn    │   │ RegularConn      │   │ ReservedConn       │   │
│  │ AdminConn    │   │ RegularConn      │   │   └─ PooledConn    │   │
│  │              │   │   └─ ConnState   │   │   └─ ConnID        │   │
│  │              │   │      └─ User     │   │   └─ reservedProps │   │
│  │              │   │      └─ Settings │   │                    │   │
│  │              │   │      └─ Stmts    │   │ active: map[ID]    │   │
│  └──────────────┘   └──────────────────┘   └────────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘
```

## Connection Pool Types

### 1. AdminPool

**Purpose:** Control plane operations for connection management.

**Characteristics:**
- Small pool size (2-5 connections)
- No connection state tracking
- Used exclusively for `CancelRequest` operations (killing queries/connections)
- Connections are not routed based on settings

**Use Cases:**
- Canceling long-running queries
- Terminating connections when clients disconnect unexpectedly

### 2. RegularPool

**Purpose:** Data plane connections for query execution with session state.

**Characteristics:**
- Main pool for executing user queries
- Supports the Extended Query Protocol (Parse/Bind/Execute)
- Tracks connection state including user, settings, and prepared statements
- Uses settings-based bucket routing for connection reuse

**Use Cases:**
- Simple queries without transactions
- Queries that can be executed on any available connection with matching settings

### 3. ReservedPool

**Purpose:** Long-lived connections for transactions and portal operations.

**Characteristics:**
- Wraps connections from RegularPool with reservation tracking
- Assigns unique connection IDs via atomic counter (`lastID`) for client-side reference
- Maintains an `active map[int64]*Conn` for ID-based lookup
- Tracks reservation state via `reservedProps` (transaction or portal reservation)
- Includes background `idleKiller` goroutine that periodically scans for timed-out connections
- Connections remain reserved until transaction completes or portal is closed

**Use Cases:**
- Explicit transactions (`BEGIN`/`COMMIT`/`ROLLBACK`)
- Cursor operations requiring persistent portal state
- Any operation requiring connection affinity

**Timeout Handling:**

Reserved connections have an idle timeout (default 30s). A background goroutine
runs at 1/10th of the idle timeout interval to
scan and kill connections that have exceeded their timeout. Each time a
connection is accessed via `Get()`, its expiry time is reset.

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

The `SettingsCache` provides bounded LRU caching that ensures the same settings
configuration always returns the same `*Settings` pointer. This enables:

1. **Pointer equality for fast comparison** - Instead of comparing full maps
2. **Consistent bucket assignment** - Same settings always route to same bucket
3. **Reduced memory allocation** - Repeated settings configurations are reused

```go
// Example: Settings are interned through the cache
cache := connstate.NewSettingsCache()
settings := cache.GetOrCreate(map[string]string{
    "statement_timeout": "30s",
    "search_path":       "public,app",
})
```

## User Management and RLS

### Service Account Architecture

The connection pool runs under a **service account** that has privileges to
assume any user role via `SET ROLE`. This design enables:

1. **Connection Reuse** - Connections aren't partitioned per-user
2. **RLS Compatibility** - Row-Level Security policies based on `current_user`
   work correctly because we actually change the session role
3. **Efficient Pooling** - Users don't fragment the pool into per-user buckets

### User Tracking

The `ConnectionState` tracks the current user role:

```go
type ConnectionState struct {
    User     string              // Current role via SET ROLE
    Settings *Settings           // Session variables
    // ... other state
}
```

**Important:** The `User` field is NOT used for bucket routing. Users are
managed via `SET ROLE`/`RESET ROLE` commands, keeping the connection pool
unified while still supporting RLS.

### Connection Flow with User Switching

**Simple Query (RegularPool):**

```
1. Client request arrives for user "alice" with settings
2. RegularPool.GetWithSettings(ctx, settings, "alice")
   └─ Internally executes SET ROLE alice
3. Execute user's query (RLS sees current_user = alice)
4. pooled.Recycle()
```

**Transaction (ReservedPool):**

```
1. Client request arrives for user "alice", BEGIN transaction
2. ReservedPool.NewConn(ctx, settings, "alice")
   ├─ Gets RegularConn from underlying pool with SET ROLE
   ├─ Wraps in reserved.Conn with unique ConnID
   └─ Registers in active map
3. reserved.Begin() → executes BEGIN
4. Return ConnID to client
   ... client sends more queries with same ConnID ...
5. ReservedPool.Get(connID) → retrieves connection, resets expiry
6. reserved.Query(ctx, "INSERT ...")
   ... eventually ...
7. reserved.Commit() → executes COMMIT
8. reserved.Release(ReleaseCommit)
```

## Important Usage Guidelines

### Do NOT Use SET ROLE Directly

**Users must not execute `SET ROLE` commands directly in their queries.**

The MultiPooler manages role switching to ensure connections are properly
returned to a clean state. If users bypass this by running `SET ROLE` in their
SQL:

1. The connection state tracking becomes inconsistent
2. RLS may see incorrect user context
3. Connection cleanup may fail, affecting subsequent users

**What We Prevent:**
- Direct `SET ROLE` commands in simple queries
- `SET SESSION AUTHORIZATION` commands

**What We Cannot Fully Prevent:**
- `SET ROLE` inside stored procedures or functions
- Dynamic SQL that constructs role-switching statements

If your application requires explicit role management, contact the platform
team to discuss the appropriate architecture.

### Session Variable Considerations

Session variables set via `SET` commands affect connection routing:

- Connections with identical settings share a bucket

## ConnectionPoolManager

The `Manager` in `go/multipooler/connpoolmanager/` orchestrates all three pool
types, providing:

1. **Unified Interface** - Single entry point for connection acquisition
2. **Pool Selection** - Routes requests to appropriate pool based on operation
3. **Lifecycle Management** - Handles connection creation, validation, and cleanup
4. **Metrics** - Exposes pool statistics for monitoring

### Usage

```go
import "github.com/multigres/multigres/go/multipooler/connpoolmanager"

// Create and open the manager
mgr := connpoolmanager.NewManager(config)
mgr.Open(ctx)
defer mgr.Close()

// Get connections as needed
adminConn, _ := mgr.GetAdminConn(ctx)
regularConn, _ := mgr.GetRegularConn(ctx, user)
regularConnWithSettings, _ := mgr.GetRegularConnWithSettings(ctx, settings, user)
reservedConn, _ := mgr.NewReservedConn(ctx, settings, user)

// Resume a reserved connection by ID
conn, ok := mgr.GetReservedConn(connID)
```

### Interface for Testing

The `PoolManager` interface allows components to mock the manager in tests:

```go
type PoolManager interface {
    Open(ctx context.Context)
    Close()

    // Admin pool operations
    GetAdminConn(ctx context.Context) (admin.PooledConn, error)

    // Regular pool operations
    GetRegularConn(ctx context.Context, user string) (regular.PooledConn, error)
    GetRegularConnWithSettings(ctx context.Context, settings *connstate.Settings, user string) (regular.PooledConn, error)

    // Reserved pool operations
    NewReservedConn(ctx context.Context, settings *connstate.Settings, user string) (*reserved.Conn, error)
    GetReservedConn(connID int64) (*reserved.Conn, bool)

    Stats() ManagerStats
}
```

## Related Documentation

- [Prepared Statements Design](./prepared_statements_design.md) - Extended Query
  Protocol and statement management
