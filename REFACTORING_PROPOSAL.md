# Refactoring Proposal: Implement Controller Pattern (Vitess-style)

## Problem Statement

Currently, `init.go` creates two separate high-level components:
1. `MultiPoolerManager` - manages lifecycle operations (promote, demote, topology, consensus, heartbeat)
2. `MultiPooler` (from poolerserver) - handles query serving (executor, gRPC query services)

Both components have overlapping responsibilities:
- Both create separate database connections (duplication)
- Both create the sidecar schema (duplication)
- Both register gRPC services separately
- Both are started independently

This creates confusion about which component is the "main" manager and leads to code duplication.

## Vitess Pattern Analysis

From studying `vitess/go/vt/vttablet/tabletmanager/tm_init.go` and the Controller pattern:

1. **TabletManager** - The main orchestrator (like our `MultiPoolerManager`)
   - Owns the database configuration and connection management
   - Has a field `QueryServiceControl tabletserver.Controller` to control query serving
   - Initializes the controller with: `tm.QueryServiceControl.InitDBConfig(target, tm.DBConfigs, tm.MysqlDaemon)`
   - Single entry point for all tablet operations

2. **Controller Interface** - Defines contract for query serving components
   - `InitDBConfig()` - Receives DB configs from manager (doesn't create own connection)
   - `SetServingType()` - Transitions serving state
   - `IsServing()`, `IsHealthy()` - Status methods
   - Clear lifecycle methods managed by TabletManager

3. **Key Benefits of This Pattern:**
   - Clear separation of concerns: Manager orchestrates, Controller serves queries
   - No duplicate connections - Controller receives configs from Manager
   - Manager controls all lifecycle transitions
   - Controller is a coordinated component, not independent

## Proposed Solution

Implement the same Controller pattern for multipooler:

1. **Define a `PoolerController` interface** (similar to `tabletserver.Controller`)
   - `InitDBConfig()` - Receives DB connection/config from manager
   - `SetServingType()` - Transitions between serving states (SERVING, NOT_SERVING, SERVING_RDONLY)
   - `Start()`, `IsServing()`, `IsHealthy()` - Lifecycle and status methods
   - `GetExecutor()` - Returns the query executor

2. **Make `MultiPooler` implement `PoolerController`**
   - Remove `connectDB()` - receives DB from manager instead
   - Add `InitDBConfig(*sql.DB)` method
   - Remove sidecar schema creation (manager handles this)

3. **Update `MultiPoolerManager`** to own and orchestrate the controller
   - Add field `queryServiceControl PoolerController`
   - Create pooler instance in constructor
   - Initialize pooler with DB connection: `pm.queryServiceControl.InitDBConfig(pm.db)`
   - Start pooler as part of manager's startup
   - Be the single entry point for all operations

## Detailed Implementation Steps

### Step 1: Define the PoolerController Interface

**File: `go/multipooler/poolerserver/controller.go` (NEW)**

Create a new interface following Vitess pattern:
```go
package poolerserver

import (
    "context"
    "database/sql"

    "github.com/multigres/multigres/go/multipooler/queryservice"
    clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// PoolerController defines the control interface for query serving.
// This follows the Vitess Controller pattern (see vitess/go/vt/vttablet/tabletserver/controller.go)
type PoolerController interface {
    // InitDBConfig initializes the controller with database connection.
    // This is called by MultiPoolerManager after it establishes the DB connection.
    // Similar to TabletManager calling QueryServiceControl.InitDBConfig()
    InitDBConfig(db *sql.DB) error

    // SetServingType transitions the query service to the required serving state.
    // servingStatus: SERVING, NOT_SERVING, SERVING_RDONLY, DRAINED
    // Returns error if the transition fails.
    SetServingType(ctx context.Context, servingStatus clustermetadatapb.PoolerServingStatus) error

    // IsServing returns true if the query service is currently serving requests
    IsServing() bool

    // IsHealthy returns nil if healthy, error describing the problem otherwise
    IsHealthy() error

    // GetExecutor returns the query executor for handling queries
    GetExecutor() (queryservice.QueryService, error)

    // Register registers gRPC services with the server
    Register()

    // Close shuts down the controller
    Close() error
}

// Ensure MultiPooler implements PoolerController
var _ PoolerController = (*MultiPooler)(nil)
```

### Step 2: Update MultiPooler to Implement PoolerController

**File: `go/multipooler/poolerserver/pooler.go`**

Remove DB connection creation and implement controller interface:
```go
type MultiPooler struct {
    logger   *slog.Logger
    config   *manager.Config
    db       *sql.DB  // Received from manager, not created here
    executor queryservice.QueryService

    mu            sync.Mutex
    servingStatus clustermetadatapb.PoolerServingStatus
}

// InitDBConfig receives the database connection from MultiPoolerManager
// This is called by the manager after it establishes the connection
func (s *MultiPooler) InitDBConfig(db *sql.DB) error {
    if db == nil {
        return fmt.Errorf("database connection cannot be nil")
    }
    s.db = db
    s.logger.Info("MultiPooler initialized with database connection")
    return nil
}

// SetServingType transitions the serving state
func (s *MultiPooler) SetServingType(ctx context.Context, servingStatus clustermetadatapb.PoolerServingStatus) error {
    s.mu.Lock()
    defer s.mu.Unlock()

    s.logger.Info("Transitioning serving type", "from", s.servingStatus, "to", servingStatus)
    s.servingStatus = servingStatus

    // TODO: Implement state-specific behavior:
    // - SERVING: Accept all queries
    // - SERVING_RDONLY: Accept only read queries
    // - NOT_SERVING: Reject all queries
    // - DRAINED: Gracefully drain existing connections, reject new queries

    return nil
}

// IsServing returns true if currently serving queries
func (s *MultiPooler) IsServing() bool {
    s.mu.Lock()
    defer s.mu.Unlock()
    return s.servingStatus == clustermetadatapb.PoolerServingStatus_SERVING ||
           s.servingStatus == clustermetadatapb.PoolerServingStatus_SERVING_RDONLY
}

// IsHealthy checks if the controller is healthy
func (s *MultiPooler) IsHealthy() error {
    if s.db == nil {
        return fmt.Errorf("database connection not initialized")
    }
    if err := s.db.Ping(); err != nil {
        return fmt.Errorf("database ping failed: %w", err)
    }
    return nil
}

// REMOVE these methods:
// - connectDB() - Manager handles this now
// - isPrimary() - Move to manager if needed
// - Sidecar schema creation - Manager handles this

// UPDATE GetExecutor to use received DB:
func (s *MultiPooler) GetExecutor() (queryservice.QueryService, error) {
    if s.db == nil {
        return nil, fmt.Errorf("database not initialized - call InitDBConfig first")
    }
    if s.executor == nil {
        s.executor = executor.NewExecutor(s.logger, s.db)
    }
    return s.executor, nil
}

// Register registers gRPC services (called by manager during startup)
func (s *MultiPooler) Register() {
    s.registerGRPCServices()
}
```

### Step 3: Add Controller Field to MultiPoolerManager

**File: `go/multipooler/manager/manager.go`**

Add the controller field following Vitess TabletManager pattern:
```go
type MultiPoolerManager struct {
    logger       *slog.Logger
    config       *Config
    db           *sql.DB
    topoClient   topo.Store
    serviceID    *clustermetadatapb.ID
    replTracker  *heartbeat.ReplTracker
    pgctldClient pgctldpb.PgCtldClient

    // Query service controller (similar to TabletManager.QueryServiceControl)
    queryServiceControl poolerserver.PoolerController  // NEW

    // ... rest of fields ...
}
```

### Step 4: Initialize Controller in Constructor

**File: `go/multipooler/manager/manager.go`**

Create the controller in the constructor:
```go
func NewMultiPoolerManager(logger *slog.Logger, config *Config) *MultiPoolerManager {
    ctx, cancel := context.WithCancel(context.Background())

    // ... pgctld client setup ...

    pm := &MultiPoolerManager{
        logger:            logger,
        config:            config,
        topoClient:        config.TopoClient,
        serviceID:         config.ServiceID,
        actionSema:        semaphore.NewWeighted(1),
        state:             ManagerStateStarting,
        ctx:               ctx,
        cancel:            cancel,
        loadTimeout:       5 * time.Minute,
        queryServingState: clustermetadatapb.PoolerServingStatus_NOT_SERVING,
        pgctldClient:      pgctldClient,
    }

    // Create the query service controller (NEW)
    pm.queryServiceControl = poolerserver.NewMultiPooler(logger, config)
    logger.Info("Created query service controller")

    return pm
}
```

### Step 5: Initialize Controller with DB Connection

**File: `go/multipooler/manager/manager.go`**

Update `connectDB()` to initialize the controller (following Vitess pattern):
```go
func (pm *MultiPoolerManager) connectDB() error {
    if pm.db != nil {
        return nil // Already connected
    }

    db, err := CreateDBConnection(pm.logger, pm.config)
    if err != nil {
        return err
    }
    pm.db = db

    // Test the connection
    if err := pm.db.Ping(); err != nil {
        pm.db.Close()
        pm.db = nil
        return fmt.Errorf("failed to ping database: %w", err)
    }

    pm.logger.Info("MultiPoolerManager: Connected to PostgreSQL")

    // Initialize query service controller with DB connection (NEW)
    // This follows TabletManager calling tm.QueryServiceControl.InitDBConfig()
    if err := pm.queryServiceControl.InitDBConfig(pm.db); err != nil {
        pm.logger.Error("Failed to initialize query service controller", "error", err)
        return fmt.Errorf("failed to initialize controller: %w", err)
    }
    pm.logger.Info("Initialized query service controller with database connection")

    // Create sidecar schema (only on primary)
    ctx := context.Background()
    isPrimary, err := pm.isPrimary(ctx)
    if err != nil {
        pm.logger.Error("Failed to check if database is primary", "error", err)
    } else if isPrimary {
        pm.logger.Info("Creating sidecar schema on primary database")
        if err := CreateSidecarSchema(pm.db); err != nil {
            return fmt.Errorf("failed to create sidecar schema: %w", err)
        }
    }

    // Start heartbeat tracking...
    // ... rest of existing code ...

    return nil
}
```

### Step 6: Register Controller Services in Start()

**File: `go/multipooler/manager/manager.go`**

Update `Start()` to register controller services:
```go
func (pm *MultiPoolerManager) Start(senv *servenv.ServEnv) {
    // Start loading multipooler record from topology asynchronously
    go pm.loadMultiPoolerFromTopo()

    // Start loading consensus term from local disk asynchronously
    if pm.config.ConsensusEnabled {
        go pm.loadConsensusTermFromDisk()
    }

    senv.OnRun(func() {
        pm.logger.Info("MultiPoolerManager started")

        // Connect to database and initialize controller
        if err := pm.connectDB(); err != nil {
            pm.logger.Error("Failed to connect to database during startup", "error", err)
            // Don't fail startup - will retry on demand
        }

        // Register query service controller gRPC services (NEW)
        pm.queryServiceControl.Register()
        pm.logger.Info("Query service controller registered")

        // Register manager gRPC services
        pm.registerGRPCServices()
        pm.logger.Info("MultiPoolerManager gRPC services registered")
    })
}
```

### Step 7: Add Controller Access Method

**File: `go/multipooler/manager/manager.go`**

Add method to access the controller (for tests and special cases):
```go
// QueryServiceControl returns the query service controller
// This follows the TabletManager pattern of exposing the controller
func (pm *MultiPoolerManager) QueryServiceControl() poolerserver.PoolerController {
    return pm.queryServiceControl
}
```

### Step 8: Update init.go

**File: `go/multipooler/init.go`**

Remove separate pooler creation - manager now handles everything:
```go
// BEFORE (remove this):
// pooler := poolerserver.NewMultiPooler(logger, &manager.Config{...})
// pooler.Start(mp.senv)

// AFTER (keep only manager):
poolerManager := manager.NewMultiPoolerManager(logger, &manager.Config{...})
poolerManager.Start(mp.senv)

// Manager now creates and manages the pooler internally
```

### Step 9: Update gRPC Service Registration

**File: `go/multipooler/grpcpoolerservice/service.go`**

Services now get pooler from manager:
```go
func RegisterPoolerServices(senv *servenv.ServEnv, grpc *servenv.GrpcServer) {
    // Register callback to be invoked when manager is ready
    manager.RegisterPoolerManagerServices = append(
        manager.RegisterPoolerManagerServices,
        func(pm *manager.MultiPoolerManager) {
            if grpc.CheckServiceMap("pooler", senv) {
                // Get pooler controller from manager
                poolerCtrl := pm.QueryServiceControl()

                srv := &poolerService{
                    poolerController: poolerCtrl,
                }
                multipoolerpb.RegisterMultiPoolerServiceServer(grpc.Server, srv)
            }
        },
    )
}
```

## Benefits

1. **Follows Proven Pattern**: Adopts the same Controller pattern used successfully in Vitess
2. **Clear Separation of Concerns**:
   - Manager orchestrates lifecycle, topology, consensus, replication
   - Controller handles query serving only
3. **Eliminates Duplication**:
   - Single database connection (manager owns it)
   - Single sidecar schema creation
   - Single startup flow
4. **Better Testability**:
   - Controller interface enables mocking
   - Manager and controller can be tested independently
   - Integration tests can test the full stack
5. **Cleaner Architecture**:
   - Manager is the single entry point
   - Controller is a coordinated component, not independent
   - Explicit dependencies via interface
6. **Future-Proof**:
   - Easy to add serving state transitions (SERVING → SERVING_RDONLY → DRAINED)
   - Controller interface can be extended without changing manager
   - Multiple controller implementations possible (for different backends)

## Implementation Order

Follow this order to ensure smooth, incremental changes:

### Phase 1: Define Interface (Non-Breaking)
1. Create `controller.go` with `PoolerController` interface
2. Add field `queryServiceControl` to `MultiPoolerManager`
3. Initialize controller in `NewMultiPoolerManager()` constructor

**Verification**: Code compiles, existing functionality unchanged

### Phase 2: Implement Interface (Non-Breaking)
4. Add `InitDBConfig()`, `SetServingType()`, `IsServing()`, `IsHealthy()`, `Register()` methods to `MultiPooler`
5. Add `QueryServiceControl()` accessor to manager
6. Update manager's `connectDB()` to call `InitDBConfig()`
7. Update manager's `Start()` to call controller's `Register()`

**Verification**: Code compiles, new methods exist but not yet used, existing flow still works

### Phase 3: Remove Duplication (Breaking for pooler standalone usage)
8. Remove `connectDB()` from `MultiPooler`
9. Remove `isPrimary()` from `MultiPooler` (if needed, move to manager)
10. Remove `Start()` from `MultiPooler` or make it call `Register()`
11. Remove sidecar schema creation from `MultiPooler`
12. Update `GetExecutor()` to check that `InitDBConfig` was called

**Verification**: Manager flow works, pooler can no longer be used standalone

### Phase 4: Update Clients
13. Update `init.go` to remove standalone pooler creation
14. Update `grpcpoolerservice` to get controller from manager
15. Update tests to use manager instead of creating pooler directly

**Verification**: All tests pass, gRPC services register correctly

## Testing Strategy

### Unit Tests
- Test `PoolerController` interface methods independently
- Mock the controller in manager tests
- Test serving state transitions

### Integration Tests
- Test full manager + controller initialization flow
- Test DB connection sharing between manager and controller
- Test gRPC service registration through manager

### Regression Tests
- Ensure all existing pooler tests still pass (with manager setup)
- Verify query execution still works
- Check topology operations (promote/demote) don't break

## Migration Notes

### For Developers
- **Old way**: `pooler := poolerserver.NewMultiPooler(...); pooler.Start(senv)`
- **New way**: `manager := manager.NewMultiPoolerManager(...); manager.Start(senv)`
- The pooler is now accessed via `manager.QueryServiceControl()`

### Breaking Changes
- `MultiPooler` can no longer be used standalone (must be created via manager)
- `MultiPooler.Start()` may be removed or changed to call `Register()`
- `MultiPooler.connectDB()` is removed

### Non-Breaking Additions
- New `PoolerController` interface
- New methods on `MultiPooler`: `InitDBConfig`, `SetServingType`, `IsServing`, `IsHealthy`
- New accessor on `MultiPoolerManager`: `QueryServiceControl()`

## Future Enhancements

After this refactoring, we can easily add:

1. **Serving State Management**: Implement TODO in `SetServingType()` to:
   - Accept/reject queries based on serving state
   - Drain connections gracefully
   - Support read-only mode

2. **Health Checks**: Use `IsHealthy()` for:
   - Kubernetes liveness/readiness probes
   - Load balancer health checks
   - Automatic failover decisions

3. **Query Routing**: Controller can route queries based on:
   - Serving state (read-only vs read-write)
   - Query type (SELECT vs DML)
   - Connection pool availability

4. **Multiple Controllers**: Support different backends:
   - PostgreSQL controller (current)
   - MySQL controller (future)
   - Mock controller (testing)

## Summary

This refactoring adopts the proven **Controller pattern from Vitess**, where:

- **MultiPoolerManager** acts like **TabletManager**: The main orchestrator that owns DB connections, manages lifecycle, handles topology, consensus, and replication
- **MultiPooler** acts like **TabletServer**: Implements the **PoolerController** interface to handle query serving only
- The manager calls `queryServiceControl.InitDBConfig(db)` to initialize the controller with the DB connection
- Clear separation: Manager orchestrates, Controller serves queries

### Key Changes
1. Define `PoolerController` interface with methods: `InitDBConfig()`, `SetServingType()`, `IsServing()`, `IsHealthy()`, `Register()`
2. MultiPooler implements this interface
3. Manager creates, initializes, and controls the pooler
4. Remove duplicate DB connection and sidecar schema creation from pooler
5. Update init.go to only create the manager

### Next Steps
Start with **Phase 1** (Define Interface) - it's non-breaking and sets the foundation for the rest of the refactoring.

