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
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"sync"
	"time"

	"github.com/lib/pq"

	"github.com/multigres/multigres/go/clustermetadata/topo"
	"github.com/multigres/multigres/go/mterrors"
	"github.com/multigres/multigres/go/multipooler/heartbeat"
	"github.com/multigres/multigres/go/servenv"
	"github.com/multigres/multigres/go/tools/retry"

	"golang.org/x/sync/semaphore"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

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
	db           *sql.DB
	topoClient   topo.Store
	serviceID    *clustermetadatapb.ID
	replTracker  *heartbeat.ReplTracker
	pgctldClient pgctldpb.PgCtldClient

	// actionSema is there to run only one action at a time.
	// This semaphore can be held for long periods of time (hours),
	// like in the case of a restore. This semaphore must be obtained
	// first before other mutexes.
	actionSema *semaphore.Weighted

	// Multipooler record from topology and startup state
	mu              sync.Mutex
	multipooler     *topo.MultiPoolerInfo
	state           ManagerState
	stateError      error
	consensusTerm   *multipoolermanagerdatapb.ConsensusTerm
	topoLoaded      bool
	consensusLoaded bool
	ctx             context.Context
	cancel          context.CancelFunc
	loadTimeout     time.Duration

	// TODO: Implement async query serving state management system
	// This should include: target state, current state, convergence goroutine,
	// and state-specific handlers (setServing, setServingReadOnly, setNotServing, setDrained)
	// See design discussion for full details.
	queryServingState clustermetadatapb.PoolerServingStatus
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

// NewMultiPoolerManager creates a new MultiPoolerManager instance
func NewMultiPoolerManager(logger *slog.Logger, config *Config) *MultiPoolerManager {
	return NewMultiPoolerManagerWithTimeout(logger, config, 5*time.Minute)
}

// NewMultiPoolerManagerWithTimeout creates a new MultiPoolerManager instance with a custom load timeout
func NewMultiPoolerManagerWithTimeout(logger *slog.Logger, config *Config, loadTimeout time.Duration) *MultiPoolerManager {
	ctx, cancel := context.WithCancel(context.Background())

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

	return &MultiPoolerManager{
		logger:            logger,
		config:            config,
		topoClient:        config.TopoClient,
		serviceID:         config.ServiceID,
		actionSema:        semaphore.NewWeighted(1),
		state:             ManagerStateStarting,
		ctx:               ctx,
		cancel:            cancel,
		loadTimeout:       loadTimeout,
		queryServingState: clustermetadatapb.PoolerServingStatus_NOT_SERVING,
		pgctldClient:      pgctldClient,
	}
}

// lock is used at the beginning of an RPC call, to acquire the
// action semaphore. It returns ctx.Err() if the context expires.
func (pm *MultiPoolerManager) lock(ctx context.Context) error {
	if err := pm.actionSema.Acquire(ctx, 1); err != nil {
		return mterrors.Wrap(err, "failed to acquire action lock")
	}
	return nil
}

// unlock is the symmetrical action to lock.
func (pm *MultiPoolerManager) unlock() {
	pm.actionSema.Release(1)
}

// connectDB establishes a connection to PostgreSQL (reuses the shared logic)
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

	pm.logger.Info("MultiPoolerManager: Connected to PostgreSQL", "socket_path", pm.config.SocketFilePath, "database", pm.config.Database)

	// Start heartbeat tracking if not already started
	if pm.replTracker == nil {
		pm.logger.Info("MultiPoolerManager: Starting database heartbeat")
		ctx := context.Background()
		// TODO: populate shard ID
		shardID := []byte("0") // default shard ID

		// Use the multipooler name from serviceID as the pooler ID
		poolerID := pm.serviceID.Name

		// Check if connected to a primary database
		isPrimary, err := pm.IsPrimary(ctx)
		if err != nil {
			pm.logger.ErrorContext(ctx, "Failed to check if database is primary", "error", err)
			// Don't fail the connection if primary check fails
		} else if isPrimary {
			// Only create the sidecar schema on primary databases
			pm.logger.InfoContext(ctx, "MultiPoolerManager: Creating sidecar schema on primary database")
			if err := CreateSidecarSchema(pm.db); err != nil {
				return fmt.Errorf("failed to create sidecar schema: %w", err)
			}
		} else {
			pm.logger.InfoContext(ctx, "MultiPoolerManager: Skipping sidecar schema creation on replica")
		}

		if err := pm.startHeartbeat(ctx, shardID, poolerID); err != nil {
			pm.logger.ErrorContext(ctx, "Failed to start heartbeat", "error", err)
			// Don't fail the connection if heartbeat fails
		}
	}

	return nil
}

// startHeartbeat starts the replication tracker and heartbeat writer if connected to a primary database
func (pm *MultiPoolerManager) startHeartbeat(ctx context.Context, shardID []byte, poolerID string) error {
	// Create the replication tracker
	pm.replTracker = heartbeat.NewReplTracker(pm.db, pm.logger, shardID, poolerID, pm.config.HeartbeatIntervalMs)

	// Check if we're connected to a primary
	isPrimary, err := pm.IsPrimary(ctx)
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

// Close closes the database connection and stops the async loader
func (pm *MultiPoolerManager) Close() error {
	pm.cancel()
	if pm.replTracker != nil {
		pm.replTracker.Close()
	}
	if pm.db != nil {
		return pm.db.Close()
	}
	return nil
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
func (pm *MultiPoolerManager) GetMultiPooler() (*topo.MultiPoolerInfo, ManagerState, error) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	return pm.multipooler, pm.state, pm.stateError
}

// GetBackupConfigPath returns the path to the pgbackrest config file
func (pm *MultiPoolerManager) GetBackupConfigPath() string {
	return filepath.Join(pm.config.PoolerDir, "pgbackrest.conf")
}

// GetBackupStanza returns the pgbackrest stanza name
func (pm *MultiPoolerManager) GetBackupStanza() string {
	// Use configured stanza name if set, otherwise fallback to service ID
	if pm.config.PgBackRestStanza != "" {
		return pm.config.PgBackRestStanza
	}
	return pm.serviceID.Name
}

// GetPgCtldClient returns the pgctld gRPC client
func (pm *MultiPoolerManager) GetPgCtldClient() pgctldpb.PgCtldClient {
	return pm.pgctldClient
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

// getCurrentTerm returns the current consensus term in a thread-safe manner
func (pm *MultiPoolerManager) getCurrentTerm() int64 {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if pm.consensusTerm == nil {
		return 0
	}
	return pm.consensusTerm.GetCurrentTerm()
}

// checkReplicaGuardrails verifies that the pooler is a REPLICA and PostgreSQL is in recovery mode
// This is a common guardrail for replication-related operations on standby servers
func (pm *MultiPoolerManager) checkReplicaGuardrails(ctx context.Context) error {
	// Guardrail: Check pooler type - only REPLICA poolers can perform replication operations
	if err := pm.checkPoolerType(clustermetadatapb.PoolerType_REPLICA, "Replication operation"); err != nil {
		return err
	}

	// Ensure database connection
	if err := pm.connectDB(); err != nil {
		pm.logger.ErrorContext(ctx, "Failed to connect to database", "error", err)
		return mterrors.Wrap(err, "database connection failed")
	}

	// Guardrail: Check if the PostgreSQL instance is in recovery (standby mode)
	var isInRecovery bool
	err := pm.db.QueryRowContext(ctx, "SELECT pg_is_in_recovery()").Scan(&isInRecovery)
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

	// Ensure database connection
	if err := pm.connectDB(); err != nil {
		pm.logger.ErrorContext(ctx, "Failed to connect to database", "error", err)
		return mterrors.Wrap(err, "database connection failed")
	}

	// Guardrail: Check if the PostgreSQL instance is in standby mode
	var isInRecovery bool
	err := pm.db.QueryRowContext(ctx, "SELECT pg_is_in_recovery()").Scan(&isInRecovery)
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
}

// checkAndSetReady checks if all required resources are loaded and sets state to ready if so
// Must be called without holding the mutex
func (pm *MultiPoolerManager) checkAndSetReady() {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if pm.topoLoaded && pm.consensusLoaded {
		pm.state = ManagerStateReady
		pm.logger.Info("Manager state changed", "state", ManagerStateReady, "service_id", pm.serviceID.String())
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

		// Successfully loaded
		pm.mu.Lock()
		pm.multipooler = mp
		pm.topoLoaded = true
		pm.mu.Unlock()

		pm.logger.InfoContext(ctx, "Loaded multipooler record from topology", "service_id", pm.serviceID.String(), "database", mp.Database)
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

	currentTerm := pm.getCurrentTerm()

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

		newTerm := &multipoolermanagerdatapb.ConsensusTerm{
			CurrentTerm:        requestTerm,
			AcceptedLeader:     nil,
			LastAcceptanceTime: nil,
			LeaderId:           nil,
		}

		// Update term to local disk using the term_storage functions
		if err := SetTerm(pm.config.PoolerDir, newTerm); err != nil {
			pm.logger.ErrorContext(ctx, "Failed to update term to disk", "error", err)
			return mterrors.Wrap(err, "failed to update consensus term")
		}

		// Update our cached term
		pm.mu.Lock()
		pm.consensusTerm = newTerm
		pm.mu.Unlock()

		// Synchronize term to heartbeat writer if it exists
		if pm.replTracker != nil {
			pm.replTracker.HeartbeatWriter().SetLeaderTerm(newTerm.GetCurrentTerm())
			pm.logger.InfoContext(ctx, "Synchronized term to heartbeat writer", "term", newTerm.GetCurrentTerm())
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

	currentTerm := pm.getCurrentTerm()

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

		// Load term from local disk using the term_storage functions
		term, err := GetTerm(pm.config.PoolerDir)
		if err != nil {
			pm.logger.Debug("Failed to get consensus term from disk, retrying", "error", err)
			continue // Will retry with backoff
		}

		// Successfully loaded (nil/empty term is OK)
		pm.mu.Lock()
		pm.consensusTerm = term
		pm.consensusLoaded = true
		pm.mu.Unlock()

		// Synchronize term to heartbeat writer if it exists
		if pm.replTracker != nil && term != nil {
			pm.replTracker.HeartbeatWriter().SetLeaderTerm(term.GetCurrentTerm())
			pm.logger.Info("Synchronized loaded term to heartbeat writer", "term", term.GetCurrentTerm())
		}

		pm.logger.Info("Loaded consensus term from disk", "current_term", term.GetCurrentTerm())
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
	var inRecovery bool
	err := pm.db.QueryRowContext(ctx, "SELECT pg_is_in_recovery()").Scan(&inRecovery)
	if err != nil {
		pm.logger.ErrorContext(ctx, "Failed to check recovery status", "error", err)
		return nil, mterrors.Wrap(err, "failed to check recovery status")
	}
	state.isReadOnly = inRecovery

	// Capture current LSN - this method needs to be idempotent, so it's possible
	// it's called on a host where the primary was already changed to a standby.
	// Therefore, we try both LSN retrieval methods and use whichever succeeds.

	// Try getting current WAL LSN (works on primary)
	state.finalLSN, err = pm.getPrimaryLSN(ctx)
	if err != nil {
		// If that fails, try getting replay LSN (works on standby)
		state.finalLSN, err = pm.getStandbyReplayLSN(ctx)
		if err != nil {
			pm.logger.ErrorContext(ctx, "Failed to get LSN (tried both primary and standby methods)", "error", err)
			return nil, mterrors.Wrap(err, "failed to get LSN")
		}
	}

	pm.logger.InfoContext(ctx, "Checked demotion state",
		"is_serving_read_only", state.isServingReadOnly,
		"is_replica_in_topology", state.isReplicaInTopology,
		"is_read_only", state.isReadOnly,
		"in_recovery", inRecovery,
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
		_, err := pm.db.ExecContext(ctx, "CHECKPOINT")
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

	resp, err := pm.pgctldClient.Restart(ctx, req)
	if err != nil {
		pm.logger.ErrorContext(ctx, "Failed to restart PostgreSQL as standby", "error", err)
		return mterrors.Wrap(err, "failed to restart as standby")
	}

	pm.logger.InfoContext(ctx, "PostgreSQL restarted as standby",
		"pid", resp.Pid,
		"message", resp.Message)

	// Close database connection since PostgreSQL restarted
	if pm.db != nil {
		pm.db.Close()
		pm.db = nil
	}

	// Reconnect to PostgreSQL
	if err := pm.connectDB(); err != nil {
		pm.logger.ErrorContext(ctx, "Failed to reconnect to database after restart", "error", err)
		return mterrors.Wrap(err, "failed to reconnect to database")
	}

	// Verify server is in recovery mode (standby)
	var inRecovery bool
	err = pm.db.QueryRowContext(ctx, "SELECT pg_is_in_recovery()").Scan(&inRecovery)
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
	query := `
		SELECT COALESCE(array_agg(pid), ARRAY[]::integer[])
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

	var pids []int32
	err := pm.db.QueryRowContext(ctx, query).Scan(pq.Array(&pids))
	if err != nil {
		return nil, err
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
		_, err := pm.db.ExecContext(ctx, "SELECT pg_terminate_backend($1)", pid)
		if err != nil {
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
	var isInRecovery bool
	err := pm.db.QueryRowContext(ctx, "SELECT pg_is_in_recovery()").Scan(&isInRecovery)
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
	_, err := pm.db.ExecContext(ctx, "SELECT pg_promote()")
	if err != nil {
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
			var isInRecovery bool
			err := pm.db.QueryRowContext(promotionCtx, "SELECT pg_is_in_recovery()").Scan(&isInRecovery)
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
// This method follows the Vitess pattern similar to TabletManager.Start() in tm_init.go
func (pm *MultiPoolerManager) Start(senv *servenv.ServEnv) {
	// Start loading multipooler record from topology asynchronously
	go pm.loadMultiPoolerFromTopo()
	// Start loading consensus term from local disk asynchronously
	go pm.loadConsensusTermFromDisk()

	senv.OnRun(func() {
		pm.logger.Info("MultiPoolerManager started")
		// Additional manager-specific initialization can happen here

		// Connect to database and start heartbeats
		if err := pm.connectDB(); err != nil {
			pm.logger.Error("Failed to connect to database during startup", "error", err)
			// Don't fail startup if DB connection fails - will retry on demand
		}

		// Register all gRPC services that have registered themselves
		pm.registerGRPCServices()
		pm.logger.Info("MultiPoolerManager gRPC services registered")
	})
}
