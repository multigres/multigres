// Copyright 2025 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package manager

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/safepath"
	"github.com/multigres/multigres/go/common/servenv"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/multipooler/connpoolmanager"
	"github.com/multigres/multigres/go/multipooler/executor"
	"github.com/multigres/multigres/go/multipooler/heartbeat"
	"github.com/multigres/multigres/go/multipooler/poolerserver"
	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/tools/retry"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	pgctldpb "github.com/multigres/multigres/go/pb/pgctldservice"
)

// ManagerState represents the state of the MultiPoolerManager
type ManagerState string

const (
	// ManagerStateStarting indicates the manager is starting and loading the multipooler record
	ManagerStateStarting ManagerState = "starting"
	// ManagerStateReady indicates the manager has successfully loaded the multipooler record
	ManagerStateReady ManagerState = "ready"
	// ManagerStateError indicates the manager failed to load the multipooler record
	ManagerStateError ManagerState = "error"
)

// MultiPoolerManager manages the pooler lifecycle and PostgreSQL operations
type MultiPoolerManager struct {
	logger       *slog.Logger
	config       *Config
	topoClient   topoclient.Store
	serviceID    *clustermetadatapb.ID
	replTracker  *heartbeat.ReplTracker
	pgctldClient pgctldpb.PgCtldClient

	// connPoolMgr manages all connection pools (admin, regular, reserved)
	connPoolMgr connpoolmanager.PoolManager

	// qsc is the query service controller
	// This controller handles query serving while the manager orchestrates lifecycle,
	// topology, consensus, and replication operations.
	qsc poolerserver.PoolerController

	// actionLock is there to run only one action at a time.
	// This lock can be held for long periods of time (hours),
	// like in the case of a restore. This lock must be obtained
	// first before other mutexes.
	actionLock *ActionLock

	// Multipooler record from topology and startup state

	// mu is the mutex for the manager's state. It must be held for the
	// following:
	// - Reading state
	// - Changing state. This can cause the lock to be held for long periods
	//   of time, particularly when updating external systems (e.g. the topo)
	//   to match the manager's state.
	mu sync.Mutex

	isOpen          bool
	multipooler     *topoclient.MultiPoolerInfo
	state           ManagerState
	stateError      error
	consensusState  *ConsensusState
	topoLoaded      bool
	consensusLoaded bool
	ctx             context.Context
	cancel          context.CancelFunc
	loadTimeout     time.Duration

	// readyChan is closed when state becomes Ready or Error, to broadcast to all waiters.
	// Unbuffered is safe here because we only close() the channel (which never blocks
	// and broadcasts to all receivers) rather than sending to it.
	readyChan chan struct{}

	// Cached MultipoolerInfo so that we can access its state without having
	// to wait a potentially long time on mu when accessing read-only fields.
	cachedMultipooler cachedMultiPoolerInfo

	// Cached backup location from the database topology record.
	// This is loaded once during startup and cached for fast access.
	backupLocation string

	// autoRestoreRetryInterval is the interval between auto-restore retry attempts.
	// Defaults to 1 second. Can be set to a shorter duration for testing.
	autoRestoreRetryInterval time.Duration

	// initialized tracks whether this pooler has been fully initialized.
	// Once true, stays true for the lifetime of the manager.
	initialized bool

	// TODO: Implement async query serving state management system
	// This should include: target state, current state, convergence goroutine,
	// and state-specific handlers (setServing, setServingReadOnly, setNotServing, setDrained)
	// See design discussion for full details.
	queryServingState clustermetadatapb.PoolerServingStatus

	// The following three variables are for pgbackrest.
	primaryPoolerID *clustermetadatapb.ID
	primaryHost     string
	primaryPort     int32
}

// promotionState tracks which parts of the promotion are complete
type promotionState struct {
	isPrimaryInPostgres    bool
	isPrimaryInTopology    bool
	syncReplicationMatches bool
	currentLSN             string
}

// demotionState tracks which parts of the demotion are complete
type demotionState struct {
	isServingReadOnly   bool   // PoolerServingStatus == SERVING_RDONLY (includes heartbeat stopped)
	isReplicaInTopology bool   // PoolerType == REPLICA
	isReadOnly          bool   // default_transaction_read_only = on
	finalLSN            string // Captured LSN before demotion
}

// cachedMultiPoolerInfo holds a thread-safe cached copy of the multipooler info
type cachedMultiPoolerInfo struct {
	mu          sync.Mutex
	multipooler *topoclient.MultiPoolerInfo
}

// NewMultiPoolerManager creates a new MultiPoolerManager instance
func NewMultiPoolerManager(logger *slog.Logger, config *Config) (*MultiPoolerManager, error) {
	return NewMultiPoolerManagerWithTimeout(logger, config, 5*time.Minute)
}

// NewMultiPoolerManagerWithTimeout creates a new MultiPoolerManager instance with a custom load timeout
func NewMultiPoolerManagerWithTimeout(logger *slog.Logger, config *Config, loadTimeout time.Duration) (*MultiPoolerManager, error) {
	// Validate required config fields
	if config.TableGroup == "" {
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "TableGroup is required")
	}
	if config.Shard == "" {
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "Shard is required")
	}

	// MVP validation: fail fast if tablegroup/shard are not the MVP defaults
	if err := constants.ValidateMVPTableGroupAndShard(config.TableGroup, config.Shard); err != nil {
		return nil, mterrors.Wrap(err, "MVP validation failed")
	}

	ctx, cancel := context.WithCancel(context.TODO())

	// Create pgctld gRPC client
	var pgctldClient pgctldpb.PgCtldClient
	if config.PgctldAddr != "" {
		conn, err := grpc.NewClient(config.PgctldAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			logger.ErrorContext(ctx, "Failed to create pgctld gRPC client", "error", err, "addr", config.PgctldAddr)
			// Continue without client - operations that need it will fail gracefully
		} else {
			pgctldClient = pgctldpb.NewPgCtldClient(conn)
			logger.InfoContext(ctx, "Created pgctld gRPC client", "addr", config.PgctldAddr)
		}
	}

	// Create connection pool manager from config
	var connPoolMgr connpoolmanager.PoolManager
	if config.ConnPoolConfig != nil {
		connPoolMgr = config.ConnPoolConfig.NewManager()
	}

	pm := &MultiPoolerManager{
		logger:                   logger,
		config:                   config,
		topoClient:               config.TopoClient,
		serviceID:                config.ServiceID,
		actionLock:               NewActionLock(),
		state:                    ManagerStateStarting,
		ctx:                      ctx,
		cancel:                   cancel,
		loadTimeout:              loadTimeout,
		autoRestoreRetryInterval: 1 * time.Second,
		queryServingState:        clustermetadatapb.PoolerServingStatus_NOT_SERVING,
		pgctldClient:             pgctldClient,
		connPoolMgr:              connPoolMgr,
		readyChan:                make(chan struct{}),
	}

	// Create the query service controller with the pool manager
	pm.qsc = poolerserver.NewQueryPoolerServer(logger, connPoolMgr)

	return pm, nil
}

func (pm *MultiPoolerManager) getPrimaryPoolerID() *clustermetadatapb.ID {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	return pm.primaryPoolerID
}

// internalQueryService returns the InternalQueryService for executing queries via the connection pool.
func (pm *MultiPoolerManager) internalQueryService() executor.InternalQueryService {
	if pm.qsc == nil {
		return nil
	}
	return pm.qsc.InternalQueryService()
}

// query executes a query using the internal query service and returns the result.
// This is a convenience method for internal manager operations.
func (pm *MultiPoolerManager) query(ctx context.Context, sql string) (*query.QueryResult, error) {
	queryService := pm.internalQueryService()
	if queryService == nil {
		return nil, fmt.Errorf("internal query service not available")
	}
	return queryService.Query(ctx, sql)
}

// exec executes a command that doesn't return rows.
// This is a convenience method for internal manager operations.
func (pm *MultiPoolerManager) exec(ctx context.Context, sql string) error {
	_, err := pm.query(ctx, sql)
	return err
}

// queryArgs executes a parameterized query using the internal query service and returns the result.
// This is a convenience method for internal manager operations that helps prevent SQL injection.
func (pm *MultiPoolerManager) queryArgs(ctx context.Context, sql string, args ...any) (*query.QueryResult, error) {
	queryService := pm.internalQueryService()
	if queryService == nil {
		return nil, fmt.Errorf("internal query service not available")
	}
	return queryService.QueryArgs(ctx, sql, args...)
}

// execArgs executes a parameterized command that doesn't return rows.
// This is a convenience method for internal manager operations that helps prevent SQL injection.
func (pm *MultiPoolerManager) execArgs(ctx context.Context, sql string, args ...any) error {
	_, err := pm.queryArgs(ctx, sql, args...)
	return err
}

// Open opens the database connections and starts background operations.
// TODO:
//   - Replace with proper state manager (like tm_state.go) that orchestrates
//     state transitions and manages Open/Close lifecycle.
//   - The replTracker is being Open/Close with a big hammer. A better approach
//     is to call MakePrimary / MakeNonPrimary during state transitions.
//     We can do this, once we introduce the proper state manager.
func (pm *MultiPoolerManager) Open() error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if pm.isOpen {
		return nil
	}

	pm.logger.Info("MultiPoolerManager: opening")

	// Open connection pool manager
	if pm.connPoolMgr != nil {
		connConfig := &connpoolmanager.ConnectionConfig{
			SocketFile: pm.config.SocketFilePath,
			Port:       pm.config.PgPort,
			Database:   pm.config.Database,
		}
		pm.connPoolMgr.Open(pm.ctx, pm.logger, connConfig)
		pm.logger.Info("Connection pool manager opened")
	}

	// Create sidecar schema and start heartbeat before opening query service controller
	// This ensures the schema exists before queries can be served
	if pm.replTracker == nil {
		pm.logger.Info("MultiPoolerManager: Starting database heartbeat")
		ctx := context.TODO()
		// TODO: populate shard ID
		shardID := []byte("0") // default shard ID

		// Use the multipooler name from serviceID as the pooler ID
		poolerID := pm.serviceID.Name

		// Schema creation is now handled by multiorch during bootstrap initialization
		// Do not auto-create schema when connecting to postgres

		if err := pm.startHeartbeat(ctx, shardID, poolerID); err != nil {
			pm.logger.ErrorContext(ctx, "Failed to start heartbeat", "error", err)
			// Don't fail the connection if heartbeat fails
		}
	}

	pm.isOpen = true
	pm.logger.Info("MultiPoolerManager opened database connection")

	return nil
}

// startHeartbeat starts the replication tracker and heartbeat writer if connected to a primary database
func (pm *MultiPoolerManager) startHeartbeat(ctx context.Context, shardID []byte, poolerID string) error {
	// Create the replication tracker using the executor's InternalQueryService
	pm.replTracker = heartbeat.NewReplTracker(pm.qsc.InternalQueryService(), pm.logger, shardID, poolerID, pm.config.HeartbeatIntervalMs)

	// Check if we're connected to a primary
	isPrimary, err := pm.isPrimary(ctx)
	if err != nil {
		return fmt.Errorf("failed to check if database is primary: %w", err)
	}

	if isPrimary {
		pm.logger.InfoContext(ctx, "Starting heartbeat writer - connected to primary database")
		pm.replTracker.MakePrimary()
	} else {
		pm.logger.InfoContext(ctx, "Not starting heartbeat writer - connected to standby database")
		pm.replTracker.MakeNonPrimary()
	}

	return nil
}

// QueryServiceControl returns the query service controller.
// This follows the TabletManager pattern of exposing the controller.
func (pm *MultiPoolerManager) QueryServiceControl() poolerserver.PoolerController {
	return pm.qsc
}

// Close closes the database connection and stops the async loader.
// Safe to call multiple times and safe to call even if never opened.
func (pm *MultiPoolerManager) Close() error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	// Always cancel context to stop async loaders (even if Open() was never called)
	pm.cancel()

	return pm.closeConnectionsLocked()
}

// closeConnectionsLocked closes the connection pool manager and query service controller
// without canceling the main context. Caller must hold pm.mu.
// This is used by reopenConnections() during auto-restore to avoid canceling
// the startup context that WaitUntilReady is waiting on.
func (pm *MultiPoolerManager) closeConnectionsLocked() error {
	if !pm.isOpen {
		pm.logger.Info("MultiPoolerManager: already closed")
		return nil
	}

	// Set isOpen to false
	pm.isOpen = false

	// Close resources (safe to call even if nil/never opened)
	if pm.replTracker != nil {
		pm.replTracker.Close()
		pm.replTracker = nil
	}

	// Close connection pool manager
	if pm.connPoolMgr != nil {
		pm.connPoolMgr.Close()
	}

	pm.logger.Info("MultiPoolerManager: closed")
	return nil
}

// reopenConnections closes and reopens database connections without canceling
// the main context. This is used during auto-restore to restart connections
// after PostgreSQL has been restored, without disrupting the startup flow.
func (pm *MultiPoolerManager) reopenConnections(_ context.Context) error {
	if err := func() error {
		pm.mu.Lock()
		defer pm.mu.Unlock()
		return pm.closeConnectionsLocked()
	}(); err != nil {
		return err
	}

	// Now reopen (Open() acquires its own lock)
	return pm.Open()
}

// GetState returns the current state of the manager
func (pm *MultiPoolerManager) GetState() ManagerState {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	return pm.state
}

// GetStateError returns the error that caused the manager to enter error state
func (pm *MultiPoolerManager) GetStateError() error {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	return pm.stateError
}

// GetMultiPooler returns the current multipooler record and state
func (pm *MultiPoolerManager) GetMultiPooler() (*topoclient.MultiPoolerInfo, ManagerState, error) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	return pm.multipooler, pm.state, pm.stateError
}

// stanzaName returns the pgbackrest stanza name
func (pm *MultiPoolerManager) stanzaName() string {
	return "multigres"
}

// getPgCtldClient returns the pgctld gRPC client
func (pm *MultiPoolerManager) getPgCtldClient() pgctldpb.PgCtldClient {
	return pm.pgctldClient
}

// getTableGroup returns the table group from the config (static, set at startup)
func (pm *MultiPoolerManager) getTableGroup() string {
	return pm.config.TableGroup
}

// getShard returns the shard from the config (static, set at startup)
func (pm *MultiPoolerManager) getShard() string {
	return pm.config.Shard
}

// getPoolerType returns the pooler type from the multipooler record
func (pm *MultiPoolerManager) getPoolerType() clustermetadatapb.PoolerType {
	pm.cachedMultipooler.mu.Lock()
	defer pm.cachedMultipooler.mu.Unlock()
	if pm.cachedMultipooler.multipooler != nil && pm.cachedMultipooler.multipooler.MultiPooler != nil {
		return pm.cachedMultipooler.multipooler.Type
	}
	return clustermetadatapb.PoolerType_UNKNOWN
}

// getMultipoolerIDString returns the multipooler ID as a string
func (pm *MultiPoolerManager) getMultipoolerIDString() (string, error) {
	pm.cachedMultipooler.mu.Lock()
	defer pm.cachedMultipooler.mu.Unlock()
	if pm.cachedMultipooler.multipooler != nil && pm.cachedMultipooler.multipooler.Id != nil {
		return topoclient.MultiPoolerIDString(pm.cachedMultipooler.multipooler.Id), nil
	}
	return "", mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION, "multipooler ID not available")
}

// getMultipoolerName returns just the name part of the multipooler ID (e.g., "4zrhr2mw").
func (pm *MultiPoolerManager) getMultipoolerName() (string, error) {
	pm.cachedMultipooler.mu.Lock()
	defer pm.cachedMultipooler.mu.Unlock()
	if pm.cachedMultipooler.multipooler != nil && pm.cachedMultipooler.multipooler.Id != nil {
		return pm.cachedMultipooler.multipooler.Id.Name, nil
	}
	return "", mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION, "multipooler ID not available")
}

// backupLocationPath returns the full backup location path for a given database, table group,
// and shard. The topology only stores the base path. Components are URL-encoded to prevent
// path traversal and support UTF-8 identifiers without collisions.
func (pm *MultiPoolerManager) backupLocationPath(baseBackupLocation string, database string, tableGroup string, shard string) (string, error) {
	// Validate non-empty components
	if database == "" {
		return "", fmt.Errorf("database cannot be empty")
	}
	if tableGroup == "" {
		return "", fmt.Errorf("table group cannot be empty")
	}
	if shard == "" {
		return "", fmt.Errorf("shard cannot be empty")
	}

	return safepath.Join(baseBackupLocation, database, tableGroup, shard)
}

// updateCachedMultipooler updates the cached multipooler info with the current multipooler
// This should be called whenever pm.multipooler is updated while holding pm.mu
func (pm *MultiPoolerManager) updateCachedMultipooler() {
	pm.cachedMultipooler.mu.Lock()
	defer pm.cachedMultipooler.mu.Unlock()
	clonedProto := proto.Clone(pm.multipooler.MultiPooler).(*clustermetadatapb.MultiPooler)
	pm.cachedMultipooler.multipooler = topoclient.NewMultiPoolerInfo(clonedProto, pm.multipooler.Version())
}

// checkReady returns an error if the manager is not in Ready state
func (pm *MultiPoolerManager) checkReady() error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	switch pm.state {
	case ManagerStateReady:
		return nil
	case ManagerStateStarting:
		return mterrors.New(mtrpcpb.Code_UNAVAILABLE, "manager is still starting up")
	case ManagerStateError:
		return mterrors.Wrap(pm.stateError, "manager is in error state")
	default:
		return mterrors.New(mtrpcpb.Code_INTERNAL, fmt.Sprintf("manager is in unknown state: %s", pm.state))
	}
}

// checkPoolerType verifies that the pooler matches the expected type
func (pm *MultiPoolerManager) checkPoolerType(expectedType clustermetadatapb.PoolerType, operationName string) error {
	pm.mu.Lock()
	poolerType := pm.multipooler.Type
	pm.mu.Unlock()

	if poolerType != expectedType {
		pm.logger.Error(fmt.Sprintf("%s called on incorrect pooler type", operationName),
			"service_id", pm.serviceID.String(),
			"pooler_type", poolerType.String(),
			"expected_type", expectedType.String())
		return mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION,
			fmt.Sprintf("operation not allowed: pooler type is %s, must be %s (service_id: %s)",
				poolerType.String(), expectedType.String(), pm.serviceID.String()))
	}
	return nil
}

// getCurrentTermNumber returns the current consensus term number in a thread-safe manner
func (pm *MultiPoolerManager) getCurrentTermNumber(ctx context.Context) (int64, error) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if pm.consensusState == nil {
		return 0, nil
	}
	return pm.consensusState.GetCurrentTermNumber(ctx)
}

// checkReplicaGuardrails verifies that the pooler is a REPLICA and PostgreSQL is in recovery mode
// This is a common guardrail for replication-related operations on standby servers
func (pm *MultiPoolerManager) checkReplicaGuardrails(ctx context.Context) error {
	// Guardrail: Check pooler type - only REPLICA poolers can perform replication operations
	if err := pm.checkPoolerType(clustermetadatapb.PoolerType_REPLICA, "Replication operation"); err != nil {
		return err
	}

	// Guardrail: Check if the PostgreSQL instance is in recovery (standby mode)
	isInRecovery, err := pm.isInRecovery(ctx)
	if err != nil {
		pm.logger.ErrorContext(ctx, "Failed to check if instance is in recovery", "error", err)
		return mterrors.Wrap(err, "failed to check recovery status")
	}

	if !isInRecovery {
		pm.logger.ErrorContext(ctx, "Replication operation called on non-standby instance", "service_id", pm.serviceID.String())
		return mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION,
			fmt.Sprintf("operation not allowed: the PostgreSQL instance is not in standby mode (service_id: %s)", pm.serviceID.String()))
	}

	return nil
}

// checkPrimaryGuardrails verifies that the pooler is a PRIMARY and PostgreSQL is not in recovery mode
// This is a common guardrail for primary-only operations
func (pm *MultiPoolerManager) checkPrimaryGuardrails(ctx context.Context) error {
	// Guardrail: Check pooler type - only PRIMARY poolers can perform primary operations
	if err := pm.checkPoolerType(clustermetadatapb.PoolerType_PRIMARY, "Primary operation"); err != nil {
		return err
	}

	// Guardrail: Check if the PostgreSQL instance is in standby mode
	isInRecovery, err := pm.isInRecovery(ctx)
	if err != nil {
		pm.logger.ErrorContext(ctx, "Failed to check if instance is in recovery", "error", err)
		return mterrors.Wrap(err, "failed to check recovery status")
	}

	if isInRecovery {
		pm.logger.ErrorContext(ctx, "Primary operation called on standby instance", "service_id", pm.serviceID.String())
		return mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION,
			fmt.Sprintf("operation not allowed: the PostgreSQL instance is in standby mode (service_id: %s)", pm.serviceID.String()))
	}

	return nil
}

// setStateError sets the manager state to error with the given error message
// Must be called without holding the mutex
func (pm *MultiPoolerManager) setStateError(err error) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	pm.state = ManagerStateError
	pm.stateError = err
	pm.logger.Error("Manager state changed", "state", ManagerStateError, "error", err.Error())

	// Signal that we've reached a terminal state
	select {
	case <-pm.readyChan:
		// Already closed
	default:
		close(pm.readyChan)
	}
}

// checkAndSetReady checks if all required resources are loaded and sets state to ready if so
// Must be called without holding the mutex
func (pm *MultiPoolerManager) checkAndSetReady() {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	// Check that topo is loaded and consensus is loaded (if enabled)
	consensusReady := !pm.config.ConsensusEnabled || pm.consensusLoaded
	if pm.topoLoaded && consensusReady {
		pm.state = ManagerStateReady
		pm.logger.Info("Manager state changed", "state", ManagerStateReady, "service_id", pm.serviceID.String())

		// Signal that we've reached ready state
		select {
		case <-pm.readyChan:
			// Already closed
		default:
			close(pm.readyChan)
		}
	}
}

// waitForReady blocks until the manager is ready (topo loaded) or context is cancelled
func (pm *MultiPoolerManager) waitForReady(ctx context.Context) {
	select {
	case <-ctx.Done():
	case <-pm.readyChan:
	}
}

// loadMultiPoolerFromTopo loads the multipooler record from topology asynchronously
func (pm *MultiPoolerManager) loadMultiPoolerFromTopo() {
	// Validate ServiceID is not nil
	if pm.serviceID == nil {
		pm.setStateError(fmt.Errorf("ServiceID cannot be nil"))
		return
	}

	// Set timeout for the entire loading process
	timeoutCtx, timeoutCancel := context.WithTimeout(pm.ctx, pm.loadTimeout)
	defer timeoutCancel()

	r := retry.New(100*time.Millisecond, 30*time.Second)
	for _, err := range r.Attempts(timeoutCtx) {
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				pm.setStateError(fmt.Errorf("timeout waiting for multipooler record to be available in topology after %v", pm.loadTimeout))
			} else {
				pm.setStateError(fmt.Errorf("manager context cancelled while loading multipooler record"))
			}
			return
		}

		ctx, cancel := context.WithTimeout(pm.ctx, 5*time.Second)
		mp, err := pm.topoClient.GetMultiPooler(ctx, pm.serviceID)
		cancel()

		if err != nil {
			continue // Will retry with backoff
		}

		// Successfully loaded multipooler record
		// Now load the backup location from the database topology
		database := mp.Database
		if database == "" {
			pm.setStateError(fmt.Errorf("database name not set in multipooler"))
			return
		}

		ctx, cancel = context.WithTimeout(pm.ctx, 5*time.Second)
		db, err := pm.topoClient.GetDatabase(ctx, database)
		cancel()
		if err != nil {
			pm.setStateError(fmt.Errorf("failed to get database %s from topology: %w", database, err))
			return
		}

		// Compute full backup location: base path + database/tablegroup/shard
		shardBackupLocation, err := pm.backupLocationPath(db.BackupLocation, database, pm.config.TableGroup, pm.config.Shard)
		if err != nil {
			pm.setStateError(fmt.Errorf("invalid backup location path: %w", err))
			return
		}

		pm.mu.Lock()
		pm.multipooler = mp
		pm.updateCachedMultipooler()
		pm.backupLocation = shardBackupLocation
		pm.topoLoaded = true
		pm.mu.Unlock()

		pm.logger.InfoContext(pm.ctx, "Loaded multipooler record from topology", "service_id", pm.serviceID.String(), "database", mp.Database, "backup_location", shardBackupLocation, "pooler_type", mp.Type.String())

		// Note: restoring from backup (for replicas) happens in a separate goroutine

		pm.checkAndSetReady()
		return
	}
}

// validateAndUpdateTerm validates the request term against the current term following Consensus rules.
// Returns an error if the request term is stale (less than current term).
// If the request term is higher, it updates the term in pgctld and the cache.
// If force is true, validation is skipped.
func (pm *MultiPoolerManager) validateAndUpdateTerm(ctx context.Context, requestTerm int64, force bool) error {
	if force {
		return nil // Skip validation if force is set
	}

	currentTerm, err := pm.getCurrentTermNumber(ctx)
	if err != nil {
		return fmt.Errorf("failed to get current term: %w", err)
	}

	// Check if consensus term has been initialized (term 0 means uninitialized)
	if currentTerm == 0 {
		pm.logger.ErrorContext(ctx, "Consensus term not initialized",
			"service_id", pm.serviceID.String())
		return mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION,
			"consensus term not initialized, must be explicitly set via SetTerm (use force=true to bypass)")
	}

	// If request term == current term: ACCEPT (same term, execute)
	// If request term < current term: REJECT (stale request)
	// If request term > current term: UPDATE term and ACCEPT (new term discovered)
	if requestTerm < currentTerm {
		// Request has stale term, reject
		pm.logger.ErrorContext(ctx, "Consensus term too old, rejecting request",
			"request_term", requestTerm,
			"current_term", currentTerm,
			"service_id", pm.serviceID.String())
		return mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION,
			fmt.Sprintf("consensus term too old: request term %d is less than current term %d (use force=true to bypass)",
				requestTerm, currentTerm))
	} else if requestTerm > currentTerm {
		// Request has newer term, update our term
		pm.logger.InfoContext(ctx, "Discovered newer term, updating",
			"request_term", requestTerm,
			"old_term", currentTerm,
			"service_id", pm.serviceID.String())

		pm.mu.Lock()
		cs := pm.consensusState
		pm.mu.Unlock()

		if cs == nil {
			return mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION, "consensus state not initialized")
		}

		// Update term atomically (resets accepted leader)
		if err := cs.UpdateTermAndSave(ctx, requestTerm); err != nil {
			pm.logger.ErrorContext(ctx, "Failed to update term", "error", err)
			return mterrors.Wrap(err, "failed to update consensus term")
		}

		pm.logger.InfoContext(ctx, "Consensus term updated successfully", "new_term", requestTerm)
	}
	// If requestTerm == currentCachedTerm, just continue (same term is OK)
	return nil
}

// validateTermExactMatch validates that the request term exactly matches the current term.
// Unlike validateAndUpdateTerm, this does NOT update the term automatically.
// This is used for Promote to ensure the node was properly recruited before promotion.
func (pm *MultiPoolerManager) validateTermExactMatch(ctx context.Context, requestTerm int64, force bool) error {
	if force {
		return nil // Skip validation if force is set
	}

	currentTerm, err := pm.getCurrentTermNumber(ctx)
	if err != nil {
		return fmt.Errorf("failed to get current term: %w", err)
	}

	// Check if consensus term has been initialized
	if currentTerm == 0 {
		pm.logger.ErrorContext(ctx, "Consensus term not initialized - node not recruited",
			"service_id", pm.serviceID.String())
		return mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION,
			"consensus term not initialized - node must be recruited via SetTerm first")
	}

	// Require exact match - do not update term automatically
	if requestTerm != currentTerm {
		pm.logger.ErrorContext(ctx, "Promote term mismatch - node not recruited for this term",
			"request_term", requestTerm,
			"current_term", currentTerm,
			"service_id", pm.serviceID.String())
		return mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION,
			fmt.Sprintf("term mismatch: node not recruited for term %d (current term is %d). "+
				"Coordinator must call SetTerm first to recruit this node",
				requestTerm, currentTerm))
	}

	return nil
}

// loadConsensusTermFromDisk loads the consensus term from local disk asynchronously
func (pm *MultiPoolerManager) loadConsensusTermFromDisk() {
	// Set timeout for the entire loading process
	timeoutCtx, timeoutCancel := context.WithTimeout(pm.ctx, pm.loadTimeout)
	defer timeoutCancel()

	r := retry.New(100*time.Millisecond, 30*time.Second)
	for _, err := range r.Attempts(timeoutCtx) {
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				pm.setStateError(fmt.Errorf("timeout waiting for consensus term from disk after %v", pm.loadTimeout))
			} else {
				pm.setStateError(fmt.Errorf("manager context cancelled while loading consensus term"))
			}
			return
		}

		// Initialize consensus state if not already done
		pm.mu.Lock()
		if pm.consensusState == nil {
			pm.consensusState = NewConsensusState(pm.config.PoolerDir, pm.serviceID)
		}
		cs := pm.consensusState
		pm.mu.Unlock()

		// Load term from local disk using the ConsensusState
		var currentTerm int64
		if currentTerm, err = cs.Load(); err != nil {
			pm.logger.Debug("Failed to load consensus term from disk, retrying", "error", err)
			continue // Will retry with backoff
		}

		// Successfully loaded (nil/empty term is OK)
		pm.mu.Lock()
		pm.consensusLoaded = true
		pm.mu.Unlock()

		pm.logger.Info("Loaded consensus term from disk", "current_term", currentTerm)
		pm.checkAndSetReady()
		return
	}
}

// checkDemotionState checks the current state to determine what steps remain
func (pm *MultiPoolerManager) checkDemotionState(ctx context.Context) (*demotionState, error) {
	state := &demotionState{}

	// Check topology state
	pm.mu.Lock()
	poolerType := pm.multipooler.Type
	servingStatus := pm.multipooler.ServingStatus
	pm.mu.Unlock()

	state.isReplicaInTopology = (poolerType == clustermetadatapb.PoolerType_REPLICA)
	state.isServingReadOnly = (servingStatus == clustermetadatapb.PoolerServingStatus_SERVING_RDONLY)

	// Check if PostgreSQL is in recovery mode (canonical way to check if read-only)
	isPrimary, err := pm.isPrimary(ctx)
	if err != nil {
		pm.logger.ErrorContext(ctx, "Failed to check recovery status", "error", err)
		return nil, mterrors.Wrap(err, "failed to check recovery status")
	}
	state.isReadOnly = !isPrimary

	// Capture current LSN
	state.finalLSN, err = pm.getWALPosition(ctx)
	if err != nil {
		pm.logger.ErrorContext(ctx, "Failed to get LSN", "error", err)
		return nil, mterrors.Wrap(err, "failed to get LSN")
	}

	pm.logger.InfoContext(ctx, "Checked demotion state",
		"is_serving_read_only", state.isServingReadOnly,
		"is_replica_in_topology", state.isReplicaInTopology,
		"is_read_only", state.isReadOnly,
		"is_primary", isPrimary,
		"pooler_type", poolerType,
		"serving_status", servingStatus)

	return state, nil
}

// setServingReadOnly transitions the pooler to SERVING_RDONLY status
// Note: the following code should be refactored to be async and work
// a state transita desired state that the manager converges, makes
// the pooler converge.
// Similar to how ManagerState works today.
// At the moment, setServingReadOnly means:
// - The heartbeat writer is stopped.
// - We update the topology serving state for the pooler record.
// - TODO: QueryPooler should stop accepting write traffic.
func (pm *MultiPoolerManager) setServingReadOnly(ctx context.Context, state *demotionState) error {
	if state.isServingReadOnly {
		pm.logger.InfoContext(ctx, "Already in SERVING_RDONLY state, skipping")
		return nil
	}

	pm.logger.InfoContext(ctx, "Transitioning to SERVING_RDONLY")

	// Update serving status in topology
	updatedMultipooler, err := pm.topoClient.UpdateMultiPoolerFields(ctx, pm.serviceID, func(mp *clustermetadatapb.MultiPooler) error {
		mp.ServingStatus = clustermetadatapb.PoolerServingStatus_SERVING_RDONLY
		return nil
	})
	if err != nil {
		pm.logger.ErrorContext(ctx, "Failed to update serving status in topology", "error", err)
		return mterrors.Wrap(err, "failed to transition to SERVING_RDONLY")
	}

	pm.mu.Lock()
	pm.multipooler.MultiPooler = updatedMultipooler
	pm.updateCachedMultipooler()
	pm.queryServingState = clustermetadatapb.PoolerServingStatus_SERVING_RDONLY
	pm.mu.Unlock()

	// Stop heartbeat writer
	if pm.replTracker != nil {
		pm.logger.InfoContext(ctx, "Stopping heartbeat writer")
		pm.replTracker.MakeNonPrimary()
	}

	// TODO: Configure QueryService to reject writes

	pm.logger.InfoContext(ctx, "Transitioned to SERVING_RDONLY successfully")
	return nil
}

// runCheckpointAsync runs a CHECKPOINT in a background goroutine
func (pm *MultiPoolerManager) runCheckpointAsync(ctx context.Context) chan error {
	checkpointDone := make(chan error, 1)
	go func() {
		pm.logger.InfoContext(ctx, "Starting checkpoint")
		err := pm.exec(ctx, "CHECKPOINT")
		if err != nil {
			pm.logger.WarnContext(ctx, "Checkpoint failed", "error", err)
			checkpointDone <- err
		} else {
			pm.logger.InfoContext(ctx, "Checkpoint completed")
			checkpointDone <- nil
		}
	}()
	return checkpointDone
}

// restartPostgresAsStandby restarts PostgreSQL as a standby server
// This creates standby.signal and restarts PostgreSQL via pgctld
func (pm *MultiPoolerManager) restartPostgresAsStandby(ctx context.Context, state *demotionState) error {
	if state.isReadOnly {
		pm.logger.InfoContext(ctx, "PostgreSQL already running as standby, skipping")
		return nil
	}

	if pm.pgctldClient == nil {
		return mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION, "pgctld client not initialized")
	}

	pm.logger.InfoContext(ctx, "Restarting PostgreSQL as standby")

	// Call pgctld to restart as standby
	// This will create standby.signal and restart the server
	req := &pgctldpb.RestartRequest{
		Mode:      "fast",
		Timeout:   nil, // Use default timeout
		Port:      0,   // Use default port
		ExtraArgs: nil,
		AsStandby: true, // Create standby.signal before restart
	}

	// Close the query service controller to release its stale database connection
	if err := pm.Close(); err != nil {
		pm.logger.WarnContext(ctx, "Failed to close query service controller after restart", "error", err)
		// Continue - we'll try to reconnect anyway
	}

	resp, err := pm.pgctldClient.Restart(ctx, req)
	if err != nil {
		pm.logger.ErrorContext(ctx, "Failed to restart PostgreSQL as standby", "error", err)
		return mterrors.Wrap(err, "failed to restart as standby")
	}

	pm.logger.InfoContext(ctx, "PostgreSQL restarted as standby",
		"pid", resp.Pid,
		"message", resp.Message)

	// Reopen the manager
	if err := pm.Open(); err != nil {
		pm.logger.ErrorContext(ctx, "Failed to reopen query service controller after restart", "error", err)
		return mterrors.Wrap(err, "failed to reopen query service controller")
	}

	// Verify server is in recovery mode (standby)
	inRecovery, err := pm.isInRecovery(ctx)
	if err != nil {
		pm.logger.ErrorContext(ctx, "Failed to verify recovery status", "error", err)
		return mterrors.Wrap(err, "failed to verify standby status")
	}

	if !inRecovery {
		pm.logger.ErrorContext(ctx, "PostgreSQL not in recovery mode after restart")
		return mterrors.New(mtrpcpb.Code_INTERNAL, "server not in recovery mode after restart as standby")
	}

	pm.logger.InfoContext(ctx, "PostgreSQL is now running as a standby")
	return nil
}

// updateTopologyAfterDemotion updates the pooler type in topology from PRIMARY to REPLICA
func (pm *MultiPoolerManager) updateTopologyAfterDemotion(ctx context.Context, state *demotionState) error {
	if state.isReplicaInTopology {
		pm.logger.InfoContext(ctx, "Topology already updated to REPLICA, skipping")
		return nil
	}

	pm.logger.InfoContext(ctx, "Updating pooler type in topology to REPLICA")
	updatedMultipooler, err := pm.topoClient.UpdateMultiPoolerFields(ctx, pm.serviceID, func(mp *clustermetadatapb.MultiPooler) error {
		mp.Type = clustermetadatapb.PoolerType_REPLICA
		return nil
	})
	if err != nil {
		pm.logger.ErrorContext(ctx, "Failed to update pooler type in topology", "error", err)
		return mterrors.Wrap(err, "demotion succeeded but failed to update topology")
	}

	pm.mu.Lock()
	pm.multipooler.MultiPooler = updatedMultipooler
	pm.updateCachedMultipooler()
	pm.mu.Unlock()

	pm.logger.InfoContext(ctx, "Topology updated to REPLICA successfully")
	return nil
}

// getActiveWriteConnections returns connections that are performing write operations
func (pm *MultiPoolerManager) getActiveWriteConnections(ctx context.Context) ([]int32, error) {
	// Query for connections doing write operations
	// Note: this is temporary, we can refactor this once we
	// have the query pool. Thinking that we should have a
	// specific user for the write pool and we can kill all connections
	// associated with that user.
	sql := `
		SELECT pid
		FROM pg_stat_activity
		WHERE pid != pg_backend_pid()
		  AND datname IS NOT NULL
		  AND backend_type = 'client backend'
		  AND state = 'active'
		  AND query NOT ILIKE 'SELECT%'
		  AND query NOT ILIKE 'SHOW%'
		  AND query NOT ILIKE 'BEGIN%'
		  AND query NOT ILIKE 'COMMIT%'
		  AND query NOT ILIKE 'ROLLBACK%'
		  AND query != '<IDLE>'`

	result, err := pm.query(ctx, sql)
	if err != nil {
		return nil, err
	}

	var pids []int32
	if result != nil {
		for _, row := range result.Rows {
			pid, err := executor.GetInt32(row, 0)
			if err != nil {
				return nil, fmt.Errorf("failed to parse pid: %w", err)
			}
			pids = append(pids, pid)
		}
	}

	return pids, nil
}

// terminateWriteConnections terminates connections performing write operations
func (pm *MultiPoolerManager) terminateWriteConnections(ctx context.Context) (int32, error) {
	pids, err := pm.getActiveWriteConnections(ctx)
	if err != nil {
		pm.logger.ErrorContext(ctx, "Failed to get active write connections", "error", err)
		return 0, mterrors.Wrap(err, "failed to get active write connections")
	}

	if len(pids) == 0 {
		pm.logger.InfoContext(ctx, "No active write connections to terminate")
		return 0, nil
	}

	pm.logger.WarnContext(ctx, "Terminating connections still performing writes after drain",
		"count", len(pids),
		"pids", pids)

	// Terminate each write connection
	for _, pid := range pids {
		if err := pm.execArgs(ctx, "SELECT pg_terminate_backend($1)", pid); err != nil {
			pm.logger.WarnContext(ctx, "Failed to terminate write connection", "pid", pid, "error", err)
		}
	}

	return int32(len(pids)), nil
}

// drainAndCheckpoint handles the drain timeout and checkpoint in parallel
// During the drain, it monitors for write activity every 100ms
// If 2 consecutive checks show no writes, exits early
func (pm *MultiPoolerManager) drainAndCheckpoint(ctx context.Context, drainTimeout time.Duration) error {
	// Start checkpoint in background
	checkpointDone := pm.runCheckpointAsync(ctx)

	// Monitor for write activity during drain
	pm.logger.InfoContext(ctx, "Monitoring for write activity during drain", "duration", drainTimeout)
	drainCtx, cancel := context.WithTimeout(ctx, drainTimeout)
	defer cancel()

	monitorTicker := time.NewTicker(100 * time.Millisecond)
	defer monitorTicker.Stop()

	consecutiveNoWrites := 0
	drainComplete := false

	for !drainComplete {
		select {
		case <-drainCtx.Done():
			pm.logger.InfoContext(ctx, "Drain timeout completed")
			drainComplete = true

		case err := <-checkpointDone:
			if err != nil {
				pm.logger.WarnContext(ctx, "Checkpoint completed with error during drain", "error", err)
			} else {
				pm.logger.InfoContext(ctx, "Checkpoint completed during drain")
			}

		case <-monitorTicker.C:
			// Check for write activity
			pids, err := pm.getActiveWriteConnections(ctx)
			if err != nil {
				pm.logger.WarnContext(ctx, "Failed to check for write activity during drain", "error", err)
				consecutiveNoWrites = 0 // Reset on error
			} else if len(pids) > 0 {
				pm.logger.WarnContext(ctx, "Detected write activity during drain",
					"count", len(pids),
					"pids", pids)
				consecutiveNoWrites = 0 // Reset counter
			} else {
				// No writes detected
				consecutiveNoWrites++
				if consecutiveNoWrites >= 2 {
					pm.logger.InfoContext(ctx, "No write activity detected for 2 consecutive checks, exiting drain early")
					drainComplete = true
				}
			}
		}
	}

	// Wait for checkpoint if it's still running
	select {
	case err := <-checkpointDone:
		if err != nil {
			pm.logger.WarnContext(ctx, "Checkpoint failed", "error", err)
			// Don't fail - checkpoint is an optimization
		}
	default:
		// Checkpoint still running, continue
		pm.logger.InfoContext(ctx, "Checkpoint still running, continuing with demotion")
	}

	return nil
}

// checkPromotionState checks the current state to determine what steps remain
func (pm *MultiPoolerManager) checkPromotionState(ctx context.Context, syncReplicationConfig *multipoolermanagerdatapb.ConfigureSynchronousReplicationRequest) (*promotionState, error) {
	state := &promotionState{}

	// Check PostgreSQL promotion state
	isInRecovery, err := pm.isInRecovery(ctx)
	if err != nil {
		pm.logger.ErrorContext(ctx, "Failed to check recovery status", "error", err)
		return nil, mterrors.Wrap(err, "failed to check recovery status")
	}

	state.isPrimaryInPostgres = !isInRecovery

	if state.isPrimaryInPostgres {
		// Get current primary LSN
		state.currentLSN, err = pm.getPrimaryLSN(ctx)
		if err != nil {
			pm.logger.ErrorContext(ctx, "Failed to get current LSN", "error", err)
			return nil, err
		}
	}

	// Check topology state
	pm.mu.Lock()
	poolerType := pm.multipooler.Type
	pm.mu.Unlock()

	state.isPrimaryInTopology = (poolerType == clustermetadatapb.PoolerType_PRIMARY)

	// Default: if no sync config requested, consider it as matching (no requirements to check)
	state.syncReplicationMatches = true

	// Check sync replication state if config was provided
	if syncReplicationConfig != nil && state.isPrimaryInPostgres {
		state.syncReplicationMatches = false
		currentConfig, err := pm.getSynchronousReplicationConfig(ctx)
		if err != nil {
			pm.logger.WarnContext(ctx, "Failed to get current sync replication config", "error", err)
		}
		if err == nil {
			state.syncReplicationMatches = pm.syncReplicationConfigMatches(currentConfig, syncReplicationConfig)
		}
	}

	pm.logger.InfoContext(ctx, "Checked promotion state",
		"is_primary_in_postgres", state.isPrimaryInPostgres,
		"is_primary_in_topology", state.isPrimaryInTopology,
		"sync_replication_matches", state.syncReplicationMatches)

	return state, nil
}

// promoteStandbyToPrimary calls pg_promote() and waits for promotion to complete
func (pm *MultiPoolerManager) promoteStandbyToPrimary(ctx context.Context, state *promotionState) error {
	// Return early if already promoted
	if state.isPrimaryInPostgres {
		pm.logger.InfoContext(ctx, "PostgreSQL already promoted, skipping")
		return nil
	}

	// Call pg_promote() to promote standby to primary
	pm.logger.InfoContext(ctx, "PostgreSQL promotion needed")
	pm.logger.InfoContext(ctx, "Calling pg_promote() to promote standby to primary")
	if err := pm.exec(ctx, "SELECT pg_promote()"); err != nil {
		pm.logger.ErrorContext(ctx, "Failed to call pg_promote()", "error", err)
		return mterrors.Wrap(err, "failed to promote standby")
	}

	// Wait for promotion to complete by polling pg_is_in_recovery()
	pm.logger.InfoContext(ctx, "Waiting for promotion to complete")
	return pm.waitForPromotionComplete(ctx)
}

// waitForPromotionComplete polls pg_is_in_recovery() until promotion is complete
func (pm *MultiPoolerManager) waitForPromotionComplete(ctx context.Context) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	promotionTimeout := 30 * time.Second
	promotionCtx, cancel := context.WithTimeout(ctx, promotionTimeout)
	defer cancel()

	for {
		select {
		case <-promotionCtx.Done():
			pm.logger.ErrorContext(ctx, "Timeout waiting for promotion to complete")
			return mterrors.New(mtrpcpb.Code_DEADLINE_EXCEEDED,
				fmt.Sprintf("timeout waiting for promotion to complete after %v", promotionTimeout))

		case <-ticker.C:
			isInRecovery, err := pm.isInRecovery(promotionCtx)
			if err != nil {
				pm.logger.ErrorContext(ctx, "Failed to check recovery status during promotion", "error", err)
				return mterrors.Wrap(err, "failed to check recovery status")
			}

			if !isInRecovery {
				pm.logger.InfoContext(ctx, "Promotion completed successfully - node is now primary")
				return nil
			}
		}
	}
}

// updateTopologyAfterPromotion updates the pooler type in topology from REPLICA to PRIMARY
func (pm *MultiPoolerManager) updateTopologyAfterPromotion(ctx context.Context, state *promotionState) error {
	// Return early if already updated
	if state.isPrimaryInTopology {
		pm.logger.InfoContext(ctx, "Topology already updated, skipping")
		return nil
	}

	pm.logger.InfoContext(ctx, "Topology update needed")
	pm.logger.InfoContext(ctx, "Updating pooler type in topology to PRIMARY")
	updatedMultipooler, err := pm.topoClient.UpdateMultiPoolerFields(ctx, pm.serviceID, func(mp *clustermetadatapb.MultiPooler) error {
		mp.Type = clustermetadatapb.PoolerType_PRIMARY
		return nil
	})
	if err != nil {
		pm.logger.ErrorContext(ctx, "Failed to update pooler type in topology", "error", err)
		return mterrors.Wrap(err, "promotion succeeded but failed to update topology")
	}

	pm.mu.Lock()
	pm.multipooler.MultiPooler = updatedMultipooler
	pm.updateCachedMultipooler()
	pm.mu.Unlock()

	// Update heartbeat tracker to primary mode
	if pm.replTracker != nil {
		pm.logger.InfoContext(ctx, "Updating heartbeat tracker to primary mode")
		pm.replTracker.MakePrimary()
	}

	return nil
}

// configureReplicationAfterPromotion applies synchronous replication configuration
func (pm *MultiPoolerManager) configureReplicationAfterPromotion(ctx context.Context, state *promotionState, syncReplicationConfig *multipoolermanagerdatapb.ConfigureSynchronousReplicationRequest) error {
	if syncReplicationConfig == nil {
		return nil // No configuration requested
	}

	// Return early if already configured
	if state.syncReplicationMatches {
		pm.logger.InfoContext(ctx, "Sync replication already configured, skipping")
		return nil
	}

	pm.logger.InfoContext(ctx, "Sync replication configuration needed")
	pm.logger.InfoContext(ctx, "Configuring synchronous replication for new cohort")
	err := pm.ConfigureSynchronousReplication(ctx,
		syncReplicationConfig.SynchronousCommit,
		syncReplicationConfig.SynchronousMethod,
		syncReplicationConfig.NumSync,
		syncReplicationConfig.StandbyIds,
		syncReplicationConfig.ReloadConfig)
	if err != nil {
		pm.logger.ErrorContext(ctx, "Failed to configure synchronous replication", "error", err)
		return mterrors.Wrap(err, "promotion succeeded but failed to configure synchronous replication")
	}

	return nil
}

// ReplicationLag returns the current replication lag from the heartbeat reader
func (pm *MultiPoolerManager) ReplicationLag(ctx context.Context) (time.Duration, error) {
	if err := pm.checkReady(); err != nil {
		return 0, err
	}

	if pm.replTracker == nil {
		return 0, mterrors.New(mtrpcpb.Code_UNAVAILABLE, "replication tracker not initialized")
	}

	return pm.replTracker.HeartbeatReader().Status()
}

// Start initializes the MultiPoolerManager
func (pm *MultiPoolerManager) Start(senv *servenv.ServEnv) {
	// Start loading multipooler record from topology asynchronously
	go pm.loadMultiPoolerFromTopo()
	// Start loading consensus term from local disk asynchronously (only if consensus is enabled)
	if pm.config.ConsensusEnabled {
		go pm.loadConsensusTermFromDisk()
	}
	// Start background restore from backup, for replica poolers
	go pm.tryAutoRestoreFromBackup(pm.ctx)

	// Open the database connections, connection pool manager, and start background operations
	// TODO: This should be managed by a proper state manager (like tm_state.go)
	if err := pm.Open(); err != nil {
		pm.logger.Error("Failed to open manager during startup", "error", err)
		// Don't fail startup if Open fails - will retry on demand
	}

	senv.OnRunE(func() error {
		// Block until manager is ready or error before registering gRPC services
		// Use load timeout from manager configuration
		waitCtx, cancel := context.WithTimeout(pm.ctx, pm.loadTimeout)
		defer cancel()

		pm.logger.Info("Waiting for manager to reach ready state before registering gRPC services")
		if err := pm.WaitUntilReady(waitCtx); err != nil {
			pm.logger.Error("Manager failed to reach ready state during startup", "error", err)
			return fmt.Errorf("manager failed to reach ready state: %w", err)
		}
		pm.logger.Info("Manager reached ready state, will register gRPC services")

		pm.logger.Info("MultiPoolerManager started")
		pm.qsc.RegisterGRPCServices()
		pm.logger.Info("Query service controller registered")

		// Register manager gRPC services
		pm.registerGRPCServices()
		pm.logger.Info("MultiPoolerManager gRPC services registered")
		return nil
	})
}

// WaitUntilReady blocks until the manager reaches Ready or Error state, or
// the context is cancelled. Returns nil if Ready, or an error if Error state
// or context cancelled. This should be called after Start() to ensure
// initialization is complete before accepting RPC requests.
//
// Thread-safety: This method waits on a channel that is closed when the state
// changes to Ready or Error, allowing efficient notification without polling.
func (pm *MultiPoolerManager) WaitUntilReady(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return fmt.Errorf("waiting for manager ready cancelled: %w", ctx.Err())
	case <-pm.readyChan:
		// State has changed to Ready or Error, check which one
		pm.mu.Lock()
		state := pm.state
		stateError := pm.stateError
		pm.mu.Unlock()

		switch state {
		case ManagerStateReady:
			pm.logger.InfoContext(ctx, "Manager is ready")
			return nil
		case ManagerStateError:
			pm.logger.ErrorContext(ctx, "Manager failed to initialize", "error", stateError)
			return fmt.Errorf("manager is in error state: %w", stateError)
		default:
			// This shouldn't happen - channel was closed but state isn't terminal
			return fmt.Errorf("unexpected state after ready signal: %s", state)
		}
	}
}
