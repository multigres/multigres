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
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/lib/pq"

	"github.com/multigres/multigres/go/clustermetadata/topo"
	"github.com/multigres/multigres/go/mterrors"
	"github.com/multigres/multigres/go/multipooler/heartbeat"
	"github.com/multigres/multigres/go/servenv"
	"github.com/multigres/multigres/go/tools/retry"

	"golang.org/x/sync/semaphore"
	"google.golang.org/protobuf/types/known/durationpb"

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
	consensusState  *ConsensusState
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
			logger.Error("Failed to create pgctld gRPC client", "error", err, "addr", config.PgctldAddr)
			// Continue without client - operations that need it will fail gracefully
		} else {
			pgctldClient = pgctldpb.NewPgCtldClient(conn)
			logger.Info("Created pgctld gRPC client", "addr", config.PgctldAddr)
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
		consensusState:    nil, // Will be initialized in Start() if consensus service is enabled
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

// InitializeConsensusState initializes the consensus state if not already initialized.
// This should be called when the consensus service is enabled.
func (pm *MultiPoolerManager) InitializeConsensusState() {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if pm.consensusState == nil {
		pm.consensusState = NewConsensusState(pm.config.PoolerDir, pm.serviceID)

		// Load existing consensus term from disk if it exists
		if err := pm.consensusState.Load(); err != nil {
			pm.logger.Error("Failed to load consensus state from disk", "error", err)
			// Don't return error here - we'll initialize with default values (term=0)
		} else {
			pm.logger.Info("Consensus state initialized and loaded from disk",
				"current_term", pm.consensusState.GetCurrentTerm(),
				"accepted_leader", pm.consensusState.GetAcceptedLeader())
		}
	}
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
		isPrimary, err := pm.isPrimary(ctx)
		if err != nil {
			pm.logger.Error("Failed to check if database is primary", "error", err)
			// Don't fail the connection if primary check fails
		} else if isPrimary {
			// Only create the sidecar schema on primary databases
			pm.logger.Info("MultiPoolerManager: Creating sidecar schema on primary database")
			if err := CreateSidecarSchema(pm.db); err != nil {
				return fmt.Errorf("failed to create sidecar schema: %w", err)
			}
		} else {
			pm.logger.Info("MultiPoolerManager: Skipping sidecar schema creation on replica")
		}

		if err := pm.startHeartbeat(ctx, shardID, poolerID); err != nil {
			pm.logger.Error("Failed to start heartbeat", "error", err)
			// Don't fail the connection if heartbeat fails
		}
	}

	return nil
}

// isPrimary checks if the connected database is a primary (not in recovery)
//
// TODO: replace with ReplicationStatus() when it's implemented
func (pm *MultiPoolerManager) isPrimary(ctx context.Context) (bool, error) {
	if pm.db == nil {
		return false, fmt.Errorf("database connection not established")
	}

	var inRecovery bool
	err := pm.db.QueryRowContext(ctx, "SELECT pg_is_in_recovery()").Scan(&inRecovery)
	if err != nil {
		return false, fmt.Errorf("failed to query pg_is_in_recovery: %w", err)
	}

	// pg_is_in_recovery() returns true if standby, false if primary
	return !inRecovery, nil
}

// startHeartbeat starts the replication tracker and heartbeat writer if connected to a primary database
func (pm *MultiPoolerManager) startHeartbeat(ctx context.Context, shardID []byte, poolerID string) error {
	// Create the replication tracker
	pm.replTracker = heartbeat.NewReplTracker(pm.db, pm.logger, shardID, poolerID, pm.config.HeartbeatIntervalMs)

	// Check if we're connected to a primary
	isPrimary, err := pm.isPrimary(ctx)
	if err != nil {
		return fmt.Errorf("failed to check if database is primary: %w", err)
	}

	if isPrimary {
		pm.logger.Info("Starting heartbeat writer - connected to primary database")
		pm.replTracker.MakePrimary()
	} else {
		pm.logger.Info("Not starting heartbeat writer - connected to standby database")
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

// checkReplicaGuardrails verifies that the pooler is a REPLICA and PostgreSQL is in recovery mode
// This is a common guardrail for replication-related operations on standby servers
func (pm *MultiPoolerManager) checkReplicaGuardrails(ctx context.Context) error {
	// Guardrail: Check pooler type - only REPLICA poolers can perform replication operations
	if err := pm.checkPoolerType(clustermetadatapb.PoolerType_REPLICA, "Replication operation"); err != nil {
		return err
	}

	// Ensure database connection
	if err := pm.connectDB(); err != nil {
		pm.logger.Error("Failed to connect to database", "error", err)
		return mterrors.Wrap(err, "database connection failed")
	}

	// Guardrail: Check if the PostgreSQL instance is in recovery (standby mode)
	var isInRecovery bool
	err := pm.db.QueryRowContext(ctx, "SELECT pg_is_in_recovery()").Scan(&isInRecovery)
	if err != nil {
		pm.logger.Error("Failed to check if instance is in recovery", "error", err)
		return mterrors.Wrap(err, "failed to check recovery status")
	}

	if !isInRecovery {
		pm.logger.Error("Replication operation called on non-standby instance", "service_id", pm.serviceID.String())
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
		pm.logger.Error("Failed to connect to database", "error", err)
		return mterrors.Wrap(err, "database connection failed")
	}

	// Guardrail: Check if the PostgreSQL instance is in standby mode
	var isInRecovery bool
	err := pm.db.QueryRowContext(ctx, "SELECT pg_is_in_recovery()").Scan(&isInRecovery)
	if err != nil {
		pm.logger.Error("Failed to check if instance is in recovery", "error", err)
		return mterrors.Wrap(err, "failed to check recovery status")
	}

	if isInRecovery {
		pm.logger.Error("Primary operation called on standby instance", "service_id", pm.serviceID.String())
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

	// If consensus service is not enabled, only require topo to be loaded
	consensusRequired := pm.consensusState != nil
	consensusReady := !consensusRequired || pm.consensusLoaded

	if pm.topoLoaded && consensusReady {
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

		pm.logger.Info("Loaded multipooler record from topology", "service_id", pm.serviceID.String(), "database", mp.Database)
		pm.checkAndSetReady()
		return
	}
}

// WaitForLSN waits for PostgreSQL server to reach a specific LSN position
func (pm *MultiPoolerManager) WaitForLSN(ctx context.Context, targetLsn string) error {
	if err := pm.checkReady(); err != nil {
		return err
	}
	pm.logger.Info("WaitForLSN called", "target_lsn", targetLsn)

	// Check REPLICA guardrails (pooler type and recovery mode)
	if err := pm.checkReplicaGuardrails(ctx); err != nil {
		return err
	}

	// Wait for the standby to replay WAL up to the target LSN
	// We use a polling approach to check if the replay LSN has reached the target
	pm.logger.Info("Waiting for standby to reach target LSN", "target_lsn", targetLsn)

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			pm.logger.Error("WaitForLSN context cancelled or timed out",
				"target_lsn", targetLsn,
				"error", ctx.Err())
			return mterrors.Wrap(ctx.Err(), "context cancelled or timed out while waiting for LSN")

		case <-ticker.C:
			// Check if the standby has replayed up to the target LSN
			var reachedTarget bool
			query := fmt.Sprintf("SELECT pg_last_wal_replay_lsn() >= '%s'::pg_lsn", targetLsn)
			err := pm.db.QueryRowContext(ctx, query).Scan(&reachedTarget)
			if err != nil {
				pm.logger.Error("Failed to check replay LSN", "error", err)
				return mterrors.Wrap(err, "failed to check replay LSN")
			}

			if reachedTarget {
				pm.logger.Info("Standby reached target LSN", "target_lsn", targetLsn)
				return nil
			}
		}
	}
}

// SetReadOnly makes the PostgreSQL instance read-only
func (pm *MultiPoolerManager) SetReadOnly(ctx context.Context) error {
	if err := pm.checkReady(); err != nil {
		return err
	}

	// Acquire the action lock to ensure only one mutation runs at a time
	if err := pm.lock(ctx); err != nil {
		return err
	}
	defer pm.unlock()

	pm.logger.Info("SetReadOnly called")
	return mterrors.New(mtrpcpb.Code_UNIMPLEMENTED, "method SetReadOnly not implemented")
}

// IsReadOnly checks if PostgreSQL instance is in read-only mode
func (pm *MultiPoolerManager) IsReadOnly(ctx context.Context) (bool, error) {
	if err := pm.checkReady(); err != nil {
		return false, err
	}
	pm.logger.Info("IsReadOnly called")
	return false, mterrors.New(mtrpcpb.Code_UNIMPLEMENTED, "method IsReadOnly not implemented")
}

// validateAndUpdateTerm validates the request term against the current term following Consensus rules.
// Returns an error if the request term is stale (less than current term).
// If the request term is higher, it updates the term in pgctld and the cache.
// If force is true, validation is skipped.
func (pm *MultiPoolerManager) validateAndUpdateTerm(ctx context.Context, requestTerm int64, force bool) error {
	if force {
		return nil // Skip validation if force is set
	}

	// Check if consensus service is enabled
	if pm.consensusState == nil {
		return mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION,
			"consensus service not enabled")
	}

	currentTerm := pm.consensusState.GetCurrentTerm()

	// Check if consensus term has been initialized (term 0 means uninitialized)
	if currentTerm == 0 {
		pm.logger.Error("Consensus term not initialized",
			"service_id", pm.serviceID.String())
		return mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION,
			"consensus term not initialized, must be explicitly set via SetTerm (use force=true to bypass)")
	}

	// If request term == current term: ACCEPT (same term, execute)
	// If request term < current term: REJECT (stale request)
	// If request term > current term: UPDATE term and ACCEPT (new term discovered)
	if requestTerm < currentTerm {
		// Request has stale term, reject
		pm.logger.Error("Consensus term too old, rejecting request",
			"request_term", requestTerm,
			"current_term", currentTerm,
			"service_id", pm.serviceID.String())
		return mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION,
			fmt.Sprintf("consensus term too old: request term %d is less than current term %d (use force=true to bypass)",
				requestTerm, currentTerm))
	} else if requestTerm > currentTerm {
		// Request has newer term, update our term
		pm.logger.Info("Discovered newer term, updating",
			"request_term", requestTerm,
			"old_term", currentTerm,
			"service_id", pm.serviceID.String())

		// Update consensus state (resets vote for new term)
		if err := pm.consensusState.UpdateTerm(requestTerm, ""); err != nil {
			pm.logger.Error("Failed to update term in consensus state", "error", err)
			return mterrors.Wrap(err, "failed to update consensus term")
		}

		// Persist to disk
		if err := pm.consensusState.Save(); err != nil {
			pm.logger.Error("Failed to save term to disk", "error", err)
			return mterrors.Wrap(err, "failed to save consensus term")
		}

		pm.logger.Info("Consensus term updated successfully", "new_term", requestTerm)
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

	currentTerm := pm.consensusState.GetCurrentTerm()

	// Check if consensus term has been initialized
	if currentTerm == 0 {
		pm.logger.Error("Consensus term not initialized - node not recruited",
			"service_id", pm.serviceID.String())
		return mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION,
			"consensus term not initialized - node must be recruited via SetTerm first")
	}

	// Require exact match - do not update term automatically
	if requestTerm != currentTerm {
		pm.logger.Error("Promote term mismatch - node not recruited for this term",
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
	// Check if consensus service is enabled
	pm.mu.Lock()
	if pm.consensusState == nil {
		pm.mu.Unlock()
		pm.logger.Debug("Consensus service not enabled, skipping consensus term load")
		return
	}
	pm.mu.Unlock()

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

		// Load consensus state from disk
		if err := pm.consensusState.Load(); err != nil {
			pm.logger.Debug("Failed to load consensus state from disk, retrying", "error", err)
			continue // Will retry with backoff
		}

		// Successfully loaded
		pm.mu.Lock()
		pm.consensusLoaded = true
		pm.mu.Unlock()

		pm.logger.Info("Loaded consensus term from disk", "current_term", pm.consensusState.GetCurrentTerm())
		pm.checkAndSetReady()
		return
	}
}

// generateApplicationName generates the application_name for a multipooler from its ID
// Format: {cell}_{name}
// This is used consistently for:
// - SetPrimaryConnInfo: standby's application_name when connecting to primary
// - ConfigureSynchronousReplication: standby names in synchronous_standby_names
func generateApplicationName(id *clustermetadatapb.ID) string {
	return fmt.Sprintf("%s_%s", id.Cell, id.Name)
}

// SetPrimaryConnInfo sets the primary connection info for a standby server
func (pm *MultiPoolerManager) SetPrimaryConnInfo(ctx context.Context, host string, port int32, stopReplicationBefore, startReplicationAfter bool, currentTerm int64, force bool) error {
	if err := pm.checkReady(); err != nil {
		return err
	}

	// Acquire the action lock to ensure only one mutation runs at a time
	if err := pm.lock(ctx); err != nil {
		return err
	}
	defer pm.unlock()

	pm.logger.Info("SetPrimaryConnInfo called",
		"host", host,
		"port", port,
		"stop_replication_before", stopReplicationBefore,
		"start_replication_after", startReplicationAfter,
		"current_term", currentTerm,
		"force", force)

	// Validate and update consensus term following consensus rules
	if err := pm.validateAndUpdateTerm(ctx, currentTerm, force); err != nil {
		return err
	}

	// Guardrail: Check pooler type - only REPLICA poolers can set primary_conninfo
	if err := pm.checkPoolerType(clustermetadatapb.PoolerType_REPLICA, "SetPrimaryConnInfo"); err != nil {
		return err
	}

	// Ensure database connection
	if err := pm.connectDB(); err != nil {
		pm.logger.Error("Failed to connect to database", "error", err)
		return mterrors.Wrap(err, "database connection failed")
	}

	// Guardrail: Check if the PostgreSQL instance is in recovery (standby mode)
	var isInRecovery bool
	err := pm.db.QueryRowContext(ctx, "SELECT pg_is_in_recovery()").Scan(&isInRecovery)
	if err != nil {
		pm.logger.Error("Failed to check if instance is in recovery", "error", err)
		return mterrors.Wrap(err, "failed to check recovery status")
	}

	if !isInRecovery {
		pm.logger.Error("SetPrimaryConnInfo called on non-standby instance", "service_id", pm.serviceID.String())
		return mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION,
			fmt.Sprintf("operation not allowed: the PostgreSQL instance is not in standby mode (service_id: %s)", pm.serviceID.String()))
	}

	// Optionally stop replication before making changes
	if stopReplicationBefore {
		pm.logger.Info("Stopping replication before setting primary_conninfo")
		_, err := pm.db.ExecContext(ctx, "SELECT pg_wal_replay_pause()")
		if err != nil {
			pm.logger.Error("Failed to pause WAL replay", "error", err)
			return mterrors.Wrap(err, "failed to pause WAL replay")
		}
	}

	// Build primary_conninfo connection string
	// Format: host=<host> port=<port> user=<user> application_name=<name>
	// The heartbeat_interval is converted to keepalives_interval/keepalives_idle
	pm.mu.Lock()
	database := pm.multipooler.Database
	pm.mu.Unlock()

	// Generate application name using the shared helper
	appName := generateApplicationName(pm.serviceID)
	connInfo := fmt.Sprintf("host=%s port=%d user=%s application_name=%s",
		host, port, database, appName)

	// Set primary_conninfo using ALTER SYSTEM
	pm.logger.Info("Setting primary_conninfo", "conninfo", connInfo)
	alterQuery := fmt.Sprintf("ALTER SYSTEM SET primary_conninfo = '%s'", connInfo)
	_, err = pm.db.ExecContext(ctx, alterQuery)
	if err != nil {
		pm.logger.Error("Failed to set primary_conninfo", "error", err)
		return mterrors.Wrap(err, "failed to set primary_conninfo")
	}

	// Reload PostgreSQL configuration to apply changes
	pm.logger.Info("Reloading PostgreSQL configuration")
	_, err = pm.db.ExecContext(ctx, "SELECT pg_reload_conf()")
	if err != nil {
		pm.logger.Error("Failed to reload configuration", "error", err)
		return mterrors.Wrap(err, "failed to reload PostgreSQL configuration")
	}

	// Optionally start replication after making changes.
	// Note: If replication was already running when calling SetPrimaryConnInfo,
	// even if we don't set startReplicationAfter to true, replication will be running.
	if startReplicationAfter {
		// Reconnect to database after restart
		if err := pm.connectDB(); err != nil {
			pm.logger.Error("Failed to reconnect to database after restart", "error", err)
			return mterrors.Wrap(err, "failed to reconnect to database")
		}

		pm.logger.Info("Starting replication after setting primary_conninfo")
		_, err := pm.db.ExecContext(ctx, "SELECT pg_wal_replay_resume()")
		if err != nil {
			pm.logger.Error("Failed to resume WAL replay", "error", err)
			return mterrors.Wrap(err, "failed to resume WAL replay")
		}
	}

	pm.logger.Info("SetPrimaryConnInfo completed successfully")
	return nil
}

// StartReplication starts WAL replay on standby (calls pg_wal_replay_resume)
func (pm *MultiPoolerManager) StartReplication(ctx context.Context) error {
	if err := pm.checkReady(); err != nil {
		return err
	}

	// Acquire the action lock to ensure only one mutation runs at a time
	if err := pm.lock(ctx); err != nil {
		return err
	}
	defer pm.unlock()

	pm.logger.Info("StartReplication called")

	// Check REPLICA guardrails (pooler type and recovery mode)
	if err := pm.checkReplicaGuardrails(ctx); err != nil {
		return err
	}

	// Resume WAL replay on the standby
	pm.logger.Info("Resuming WAL replay on standby")
	_, err := pm.db.ExecContext(ctx, "SELECT pg_wal_replay_resume()")
	if err != nil {
		pm.logger.Error("Failed to resume WAL replay", "error", err)
		return mterrors.Wrap(err, "failed to resume WAL replay")
	}

	pm.logger.Info("StartReplication completed successfully")
	return nil
}

// StopReplication stops WAL replay on standby (calls pg_wal_replay_pause)
func (pm *MultiPoolerManager) StopReplication(ctx context.Context) error {
	if err := pm.checkReady(); err != nil {
		return err
	}

	// Acquire the action lock to ensure only one mutation runs at a time
	if err := pm.lock(ctx); err != nil {
		return err
	}
	defer pm.unlock()

	pm.logger.Info("StopReplication called")

	// Check REPLICA guardrails (pooler type and recovery mode)
	if err := pm.checkReplicaGuardrails(ctx); err != nil {
		return err
	}

	// Pause WAL replay on the standby
	pm.logger.Info("Pausing WAL replay on standby")
	_, err := pm.db.ExecContext(ctx, "SELECT pg_wal_replay_pause()")
	if err != nil {
		pm.logger.Error("Failed to pause WAL replay", "error", err)
		return mterrors.Wrap(err, "failed to pause WAL replay")
	}

	// Wait for WAL replay to actually be paused
	// pg_wal_replay_pause() is asynchronous, so we need to wait for it to complete
	pm.logger.Info("Waiting for WAL replay to complete pausing")
	_, err = pm.waitForReplicationPause(ctx)
	if err != nil {
		return err
	}

	pm.logger.Info("StopReplication completed successfully")
	return nil
}

// ReplicationStatus gets the current replication status of the standby
func (pm *MultiPoolerManager) ReplicationStatus(ctx context.Context) (*multipoolermanagerdatapb.ReplicationStatus, error) {
	if err := pm.checkReady(); err != nil {
		return nil, err
	}
	pm.logger.Info("ReplicationStatus called")

	// Check REPLICA guardrails (pooler type and recovery mode)
	if err := pm.checkReplicaGuardrails(ctx); err != nil {
		return nil, err
	}

	status := &multipoolermanagerdatapb.ReplicationStatus{}

	// Get all replication status information in a single query
	var lsn string
	var isPaused bool
	var pauseState string
	var lastXactTime sql.NullString
	var primaryConnInfo string

	query := `SELECT
		pg_last_wal_replay_lsn(),
		pg_is_wal_replay_paused(),
		pg_get_wal_replay_pause_state(),
		pg_last_xact_replay_timestamp(),
		current_setting('primary_conninfo')`

	err := pm.db.QueryRowContext(ctx, query).Scan(
		&lsn,
		&isPaused,
		&pauseState,
		&lastXactTime,
		&primaryConnInfo,
	)
	if err != nil {
		pm.logger.Error("Failed to get replication status", "error", err)
		return nil, mterrors.Wrap(err, "failed to get replication status")
	}

	status.Lsn = lsn
	status.IsWalReplayPaused = isPaused
	status.WalReplayPauseState = pauseState
	if lastXactTime.Valid {
		status.LastXactReplayTimestamp = lastXactTime.String
	}

	// Parse primary_conninfo into structured format
	parsedConnInfo, err := parseAndRedactPrimaryConnInfo(primaryConnInfo)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to parse primary_conninfo")
	}
	status.PrimaryConnInfo = parsedConnInfo

	pm.logger.Info("ReplicationStatus completed",
		"lsn", status.Lsn,
		"is_paused", status.IsWalReplayPaused,
		"pause_state", status.WalReplayPauseState,
		"primary_conn_info", status.PrimaryConnInfo)

	return status, nil
}

// ResetReplication resets the standby's connection to its primary by clearing primary_conninfo
// and reloading PostgreSQL configuration. This effectively disconnects the replica from the primary
// and prevents it from acknowledging commits, making it unavailable for synchronous replication
// until reconfigured.
func (pm *MultiPoolerManager) ResetReplication(ctx context.Context) error {
	if err := pm.checkReady(); err != nil {
		return err
	}

	// Acquire the action lock to ensure only one mutation runs at a time
	if err := pm.lock(ctx); err != nil {
		return err
	}
	defer pm.unlock()

	pm.logger.Info("ResetReplication called")

	// Check REPLICA guardrails (pooler type and recovery mode)
	if err := pm.checkReplicaGuardrails(ctx); err != nil {
		return err
	}

	//  Clear primary_conninfo using ALTER SYSTEM
	pm.logger.Info("Clearing primary_conninfo")
	_, err := pm.db.ExecContext(ctx, "ALTER SYSTEM RESET primary_conninfo")
	if err != nil {
		pm.logger.Error("Failed to clear primary_conninfo", "error", err)
		return mterrors.Wrap(err, "failed to clear primary_conninfo")
	}

	//  Reload PostgreSQL configuration to apply changes
	pm.logger.Info("Reloading PostgreSQL configuration")
	_, err = pm.db.ExecContext(ctx, "SELECT pg_reload_conf()")
	if err != nil {
		pm.logger.Error("Failed to reload configuration", "error", err)
		return mterrors.Wrap(err, "failed to reload PostgreSQL configuration")
	}

	pm.logger.Info("ResetReplication completed successfully - standby disconnected from primary")
	return nil
}

// setSynchronousCommit sets the PostgreSQL synchronous_commit level
func (pm *MultiPoolerManager) setSynchronousCommit(ctx context.Context, synchronousCommit multipoolermanagerdatapb.SynchronousCommitLevel) error {
	// Convert enum to PostgreSQL string value
	var syncCommitValue string
	switch synchronousCommit {
	case multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_OFF:
		syncCommitValue = "off"
	case multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_LOCAL:
		syncCommitValue = "local"
	case multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_REMOTE_WRITE:
		syncCommitValue = "remote_write"
	case multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON:
		syncCommitValue = "on"
	case multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_REMOTE_APPLY:
		syncCommitValue = "remote_apply"
	default:
		return mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT,
			fmt.Sprintf("invalid synchronous_commit level: %s", synchronousCommit.String()))
	}

	pm.logger.Info("Setting synchronous_commit", "value", syncCommitValue)
	_, err := pm.db.ExecContext(ctx, fmt.Sprintf("ALTER SYSTEM SET synchronous_commit = '%s'", syncCommitValue))
	if err != nil {
		pm.logger.Error("Failed to set synchronous_commit", "error", err)
		return mterrors.Wrap(err, "failed to set synchronous_commit")
	}

	return nil
}

// formatStandbyList converts standby IDs to a comma-separated list of quoted application names
func formatStandbyList(standbyIDs []*clustermetadatapb.ID) string {
	quotedNames := make([]string, len(standbyIDs))
	for i, id := range standbyIDs {
		quotedNames[i] = fmt.Sprintf(`"%s"`, generateApplicationName(id))
	}
	return strings.Join(quotedNames, ", ")
}

// buildSynchronousStandbyNamesValue constructs the synchronous_standby_names value string
// This produces values like: FIRST 1 ("standby-1", "standby-2") or ANY 1 ("standby-1", "standby-2")
func buildSynchronousStandbyNamesValue(method multipoolermanagerdatapb.SynchronousMethod, numSync int32, standbyIDs []*clustermetadatapb.ID) (string, error) {
	if len(standbyIDs) == 0 {
		return "", nil
	}

	standbyList := formatStandbyList(standbyIDs)

	var methodStr string
	switch method {
	case multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST:
		methodStr = "FIRST"
	case multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_ANY:
		methodStr = "ANY"
	default:
		return "", mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT,
			fmt.Sprintf("invalid synchronous method: %s, must be FIRST or ANY", method.String()))
	}

	return fmt.Sprintf("%s %d (%s)", methodStr, numSync, standbyList), nil
}

// applySynchronousStandbyNames applies the synchronous_standby_names setting to PostgreSQL
func applySynchronousStandbyNames(ctx context.Context, db *sql.DB, logger *slog.Logger, value string) error {
	logger.Info("Setting synchronous_standby_names", "value", value)

	// Escape single quotes in the value by doubling them (PostgreSQL standard)
	escapedValue := strings.ReplaceAll(value, "'", "''")

	// ALTER SYSTEM SET doesn't support parameterized queries, so we use string formatting
	query := fmt.Sprintf("ALTER SYSTEM SET synchronous_standby_names = '%s'", escapedValue)
	_, err := db.ExecContext(ctx, query)
	if err != nil {
		logger.Error("Failed to set synchronous_standby_names", "error", err)
		return mterrors.Wrap(err, "failed to set synchronous_standby_names")
	}

	return nil
}

// setSynchronousStandbyNames builds and sets the PostgreSQL synchronous_standby_names configuration
// Format: https://www.postgresql.org/docs/current/runtime-config-replication.html#GUC-SYNCHRONOUS-STANDBY-NAMES
// Examples:
//
//	FIRST 2 (standby1, standby2, standby3)
//	ANY 1 (standby1, standby2)
//
// Note: Use '*' to match all connected standbys, or specify explicit standby application_name values
// Application names are generated from multipooler IDs using the shared generateApplicationName helper
func (pm *MultiPoolerManager) setSynchronousStandbyNames(ctx context.Context, synchronousMethod multipoolermanagerdatapb.SynchronousMethod, numSync int32, standbyIDs []*clustermetadatapb.ID) error {
	// If standby list is empty, clear synchronous_standby_names
	if len(standbyIDs) == 0 {
		pm.logger.Info("Clearing synchronous_standby_names (empty standby list)")
		query := "ALTER SYSTEM SET synchronous_standby_names = ''"
		_, err := pm.db.ExecContext(ctx, query)
		if err != nil {
			pm.logger.Error("Failed to clear synchronous_standby_names", "error", err)
			return mterrors.Wrap(err, "failed to clear synchronous_standby_names")
		}
		return nil
	}

	// If numSync was not provided, default to 1
	if numSync == 0 {
		numSync = 1
	}

	// Build the synchronous_standby_names value using the shared helper
	standbyNamesValue, err := buildSynchronousStandbyNamesValue(synchronousMethod, numSync, standbyIDs)
	if err != nil {
		return err
	}

	// Apply the setting using the shared helper
	return applySynchronousStandbyNames(ctx, pm.db, pm.logger, standbyNamesValue)
}

// validateStandbyIDs validates a list of standby IDs
func validateStandbyIDs(standbyIDs []*clustermetadatapb.ID) error {
	if len(standbyIDs) == 0 {
		return mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "standby_ids cannot be empty")
	}

	for i, id := range standbyIDs {
		if id == nil {
			return mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT,
				fmt.Sprintf("standby_ids[%d] is nil", i))
		}
		if id.Cell == "" {
			return mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT,
				fmt.Sprintf("standby_ids[%d] has empty cell", i))
		}
		if id.Name == "" {
			return mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT,
				fmt.Sprintf("standby_ids[%d] has empty name", i))
		}
	}

	return nil
}

// validateSyncReplicationParams validates the parameters for ConfigureSynchronousReplication
func validateSyncReplicationParams(numSync int32, standbyIDs []*clustermetadatapb.ID) error {
	// Validate numSync is non-negative
	if numSync < 0 {
		return mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT,
			fmt.Sprintf("num_sync must be non-negative, got: %d", numSync))
	}

	// If standbyIDs are provided, validate them
	if len(standbyIDs) > 0 {
		// Validate that numSync doesn't exceed the number of standbys (PostgreSQL requirement)
		// Note: numSync=0 is allowed and will be defaulted to 1 in setSynchronousStandbyNames
		if numSync > int32(len(standbyIDs)) {
			return mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT,
				fmt.Sprintf("num_sync (%d) cannot exceed number of standby_ids (%d)", numSync, len(standbyIDs)))
		}

		// Validate each standby ID
		if err := validateStandbyIDs(standbyIDs); err != nil {
			return err
		}
	}

	return nil
}

// ConfigureSynchronousReplication configures PostgreSQL synchronous replication settings
func (pm *MultiPoolerManager) ConfigureSynchronousReplication(ctx context.Context, synchronousCommit multipoolermanagerdatapb.SynchronousCommitLevel, synchronousMethod multipoolermanagerdatapb.SynchronousMethod, numSync int32, standbyIDs []*clustermetadatapb.ID, reloadConfig bool) error {
	if err := pm.checkReady(); err != nil {
		return err
	}

	// Acquire the action lock to ensure only one mutation runs at a time
	if err := pm.lock(ctx); err != nil {
		return err
	}
	defer pm.unlock()

	pm.logger.Info("ConfigureSynchronousReplication called",
		"synchronous_commit", synchronousCommit,
		"synchronous_method", synchronousMethod,
		"num_sync", numSync,
		"standby_ids", standbyIDs,
		"reload_config", reloadConfig)

	// Validate input parameters
	if err := validateSyncReplicationParams(numSync, standbyIDs); err != nil {
		return err
	}

	// Check PRIMARY guardrails (pooler type and non-recovery mode)
	if err := pm.checkPrimaryGuardrails(ctx); err != nil {
		return err
	}

	// Set synchronous_commit level
	if err := pm.setSynchronousCommit(ctx, synchronousCommit); err != nil {
		return err
	}

	// Build and set synchronous_standby_names
	if err := pm.setSynchronousStandbyNames(ctx, synchronousMethod, numSync, standbyIDs); err != nil {
		return err
	}

	// Reload configuration if requested
	if reloadConfig {
		pm.logger.Info("Reloading PostgreSQL configuration")
		_, err := pm.db.ExecContext(ctx, "SELECT pg_reload_conf()")
		if err != nil {
			pm.logger.Error("Failed to reload configuration", "error", err)
			return mterrors.Wrap(err, "failed to reload PostgreSQL configuration")
		}
	}

	pm.logger.Info("ConfigureSynchronousReplication completed successfully")
	return nil
}

// applyAddOperation adds new standbys to the standby list (idempotent)
func applyAddOperation(currentStandbys []*clustermetadatapb.ID, newStandbys []*clustermetadatapb.ID) []*clustermetadatapb.ID {
	updatedStandbys := append([]*clustermetadatapb.ID{}, currentStandbys...)
	existingMap := make(map[string]bool)
	for _, standby := range currentStandbys {
		existingMap[generateApplicationName(standby)] = true
	}
	for _, newStandby := range newStandbys {
		if !existingMap[generateApplicationName(newStandby)] {
			updatedStandbys = append(updatedStandbys, newStandby)
		}
	}
	return updatedStandbys
}

// applyRemoveOperation removes standbys from the standby list (idempotent)
func applyRemoveOperation(currentStandbys []*clustermetadatapb.ID, standbysToRemove []*clustermetadatapb.ID) []*clustermetadatapb.ID {
	removeMap := make(map[string]bool)
	for _, standby := range standbysToRemove {
		removeMap[generateApplicationName(standby)] = true
	}
	var updatedStandbys []*clustermetadatapb.ID
	for _, standby := range currentStandbys {
		if !removeMap[generateApplicationName(standby)] {
			updatedStandbys = append(updatedStandbys, standby)
		}
	}
	return updatedStandbys
}

// applyReplaceOperation replaces the entire standby list
func applyReplaceOperation(newStandbys []*clustermetadatapb.ID) []*clustermetadatapb.ID {
	return newStandbys
}

// UpdateSynchronousStandbyList updates PostgreSQL synchronous_standby_names by adding,
// removing, or replacing members. It is idempotent and only valid when synchronous
// replication is already configured.
func (pm *MultiPoolerManager) UpdateSynchronousStandbyList(ctx context.Context, operation multipoolermanagerdatapb.StandbyUpdateOperation, standbyIDs []*clustermetadatapb.ID, reloadConfig bool, consensusTerm int64, force bool) error {
	if err := pm.checkReady(); err != nil {
		return err
	}

	if err := pm.lock(ctx); err != nil {
		return err
	}
	defer pm.unlock()

	pm.logger.Info("UpdateSynchronousStandbyList called",
		"operation", operation,
		"standby_ids", standbyIDs,
		"reload_config", reloadConfig,
		"consensus_term", consensusTerm,
		"force", force)

	// === Validation ===
	// TODO: We need to validate consensus term here.
	// We should check if the request is a valid term.
	// If it's a newer term and probably we need to demote
	// ourself. But details yet to be implemented

	// Validate operation
	if operation == multipoolermanagerdatapb.StandbyUpdateOperation_STANDBY_UPDATE_OPERATION_UNSPECIFIED {
		return mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "operation must be specified")
	}

	// Validate standby IDs using the shared validation function
	if err := validateStandbyIDs(standbyIDs); err != nil {
		return err
	}

	// Check PRIMARY guardrails (pooler type and non-recovery mode)
	if err := pm.checkPrimaryGuardrails(ctx); err != nil {
		return err
	}

	// === Parse Current Configuration ===

	// Get current synchronous_standby_names value
	var currentValue string
	err := pm.db.QueryRowContext(ctx, "SHOW synchronous_standby_names").Scan(&currentValue)
	if err != nil {
		pm.logger.Error("Failed to get current synchronous_standby_names", "error", err)
		return mterrors.Wrap(err, "failed to get current synchronous_standby_names")
	}

	pm.logger.Info("Current synchronous_standby_names", "value", currentValue)

	// Parse current configuration
	cfg, err := parseSynchronousStandbyNames(currentValue)
	if err != nil {
		return err
	}

	// === Apply Operation ===

	// Apply the requested operation
	var updatedStandbys []*clustermetadatapb.ID
	switch operation {
	case multipoolermanagerdatapb.StandbyUpdateOperation_STANDBY_UPDATE_OPERATION_ADD:
		updatedStandbys = applyAddOperation(cfg.StandbyIDs, standbyIDs)

	case multipoolermanagerdatapb.StandbyUpdateOperation_STANDBY_UPDATE_OPERATION_REMOVE:
		updatedStandbys = applyRemoveOperation(cfg.StandbyIDs, standbyIDs)

	case multipoolermanagerdatapb.StandbyUpdateOperation_STANDBY_UPDATE_OPERATION_REPLACE:
		updatedStandbys = applyReplaceOperation(standbyIDs)

	default:
		return mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT,
			fmt.Sprintf("unsupported operation: %s", operation.String()))
	}

	// Validate that the final list is not empty
	if len(updatedStandbys) == 0 {
		return mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT,
			"resulting standby list cannot be empty after operation")
	}

	// === Build and Apply New Configuration ===

	// Build new synchronous_standby_names value using shared helper
	newValue, err := buildSynchronousStandbyNamesValue(cfg.Method, cfg.NumSync, updatedStandbys)
	if err != nil {
		return err
	}

	// Check if there are any changes (idempotent)
	if currentValue == newValue {
		pm.logger.Info("No changes needed - configuration already matches desired state")
		return nil
	}

	pm.logger.Info("Updating synchronous_standby_names",
		"old_value", currentValue,
		"new_value", newValue)

	// Apply the setting using shared helper
	if err = applySynchronousStandbyNames(ctx, pm.db, pm.logger, newValue); err != nil {
		return err
	}

	// Reload configuration if requested
	if reloadConfig {
		pm.logger.Info("Reloading PostgreSQL configuration")
		_, err := pm.db.ExecContext(ctx, "SELECT pg_reload_conf()")
		if err != nil {
			pm.logger.Error("Failed to reload configuration", "error", err)
			return mterrors.Wrap(err, "failed to reload PostgreSQL configuration")
		}
	}

	pm.logger.Info("UpdateSynchronousStandbyList completed successfully")
	return nil
}

// getSynchronousReplicationConfig retrieves and parses the current synchronous replication configuration
func (pm *MultiPoolerManager) getSynchronousReplicationConfig(ctx context.Context) (*multipoolermanagerdatapb.SynchronousReplicationConfiguration, error) {
	config := &multipoolermanagerdatapb.SynchronousReplicationConfiguration{}

	// Query synchronous_standby_names
	var syncStandbyNamesStr string
	err := pm.db.QueryRowContext(ctx, "SHOW synchronous_standby_names").Scan(&syncStandbyNamesStr)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to query synchronous_standby_names")
	}

	// Only parse standby names if not empty
	syncStandbyNamesStr = strings.TrimSpace(syncStandbyNamesStr)
	if syncStandbyNamesStr != "" {
		syncConfig, err := parseSynchronousStandbyNames(syncStandbyNamesStr)
		if err != nil {
			return nil, err
		}
		config.SynchronousMethod = syncConfig.Method
		config.NumSync = syncConfig.NumSync
		config.StandbyIds = syncConfig.StandbyIDs
	}

	// Query synchronous_commit
	var syncCommitStr string
	err = pm.db.QueryRowContext(ctx, "SHOW synchronous_commit").Scan(&syncCommitStr)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to query synchronous_commit")
	}

	// Map string to enum
	var syncCommitLevel multipoolermanagerdatapb.SynchronousCommitLevel
	switch strings.ToLower(syncCommitStr) {
	case "off":
		syncCommitLevel = multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_OFF
	case "local":
		syncCommitLevel = multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_LOCAL
	case "remote_write":
		syncCommitLevel = multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_REMOTE_WRITE
	case "on":
		syncCommitLevel = multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON
	case "remote_apply":
		syncCommitLevel = multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_REMOTE_APPLY
	default:
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT,
			fmt.Sprintf("unknown synchronous_commit value: %q", syncCommitStr))
	}
	config.SynchronousCommit = syncCommitLevel

	return config, nil
}

// PrimaryStatus gets the status of the leader server
func (pm *MultiPoolerManager) PrimaryStatus(ctx context.Context) (*multipoolermanagerdatapb.PrimaryStatus, error) {
	if err := pm.checkReady(); err != nil {
		return nil, err
	}

	pm.logger.Info("PrimaryStatus called")

	// Check PRIMARY guardrails (pooler type and non-recovery mode)
	if err := pm.checkPrimaryGuardrails(ctx); err != nil {
		return nil, err
	}

	status := &multipoolermanagerdatapb.PrimaryStatus{}

	// Get current LSN and recovery status in a single query
	var lsn string
	var isInRecovery bool
	err := pm.db.QueryRowContext(ctx, "SELECT pg_current_wal_lsn()::text, pg_is_in_recovery()").Scan(&lsn, &isInRecovery)
	if err != nil {
		pm.logger.Error("Failed to query LSN and recovery status", "error", err)
		return nil, mterrors.Wrap(err, "failed to query LSN and recovery status")
	}
	status.Lsn = lsn
	// If we got to this point, checkPrimaryGuardrails passed, so this is a
	// PRIMARY server from the PG perspective and should be ready to serve traffic.
	status.Ready = true

	// Get connected followers from pg_stat_replication
	rows, err := pm.db.QueryContext(ctx, "SELECT application_name FROM pg_stat_replication WHERE application_name IS NOT NULL AND application_name != ''")
	if err != nil {
		pm.logger.Error("Failed to query pg_stat_replication", "error", err)
		return nil, mterrors.Wrap(err, "failed to query connected followers")
	}
	defer rows.Close()

	followers := []*clustermetadatapb.ID{}
	for rows.Next() {
		var appName string
		if err := rows.Scan(&appName); err != nil {
			pm.logger.Error("Failed to scan application_name", "error", err)
			continue
		}
		// Parse application_name back to cluster ID
		followerID, err := parseApplicationName(appName)
		if err != nil {
			pm.logger.Warn("Failed to parse application_name, skipping", "application_name", appName, "error", err)
			continue
		}
		followers = append(followers, followerID)
	}
	if err := rows.Err(); err != nil {
		pm.logger.Error("Error iterating pg_stat_replication rows", "error", err)
		return nil, mterrors.Wrap(err, "failed to read connected followers")
	}
	status.ConnectedFollowers = followers

	// Get synchronous replication configuration
	syncConfig, err := pm.getSynchronousReplicationConfig(ctx)
	if err != nil {
		return nil, err
	}
	status.SyncReplicationConfig = syncConfig

	pm.logger.Info("PrimaryStatus completed", "lsn", lsn, "followers_count", len(followers))
	return status, nil
}

// PrimaryPosition gets the current LSN position of the leader
func (pm *MultiPoolerManager) PrimaryPosition(ctx context.Context) (string, error) {
	if err := pm.checkReady(); err != nil {
		return "", err
	}

	pm.logger.Info("PrimaryPosition called")

	// Check PRIMARY guardrails (pooler type and non-recovery mode)
	if err := pm.checkPrimaryGuardrails(ctx); err != nil {
		return "", err
	}

	// Get current primary LSN position
	return pm.getPrimaryLSN(ctx)
}

// StopReplicationAndGetStatus stops PostgreSQL replication and returns the status
func (pm *MultiPoolerManager) StopReplicationAndGetStatus(ctx context.Context) (*multipoolermanagerdatapb.ReplicationStatus, error) {
	if err := pm.checkReady(); err != nil {
		return nil, err
	}

	// Acquire the action lock to ensure only one mutation runs at a time
	if err := pm.lock(ctx); err != nil {
		return nil, err
	}
	defer pm.unlock()

	pm.logger.Info("StopReplicationAndGetStatus called")

	// Check REPLICA guardrails (pooler type and recovery mode)
	if err := pm.checkReplicaGuardrails(ctx); err != nil {
		return nil, err
	}

	// Pause WAL replay on the standby
	pm.logger.Info("Pausing WAL replay on standby")
	_, err := pm.db.ExecContext(ctx, "SELECT pg_wal_replay_pause()")
	if err != nil {
		pm.logger.Error("Failed to pause WAL replay", "error", err)
		return nil, mterrors.Wrap(err, "failed to pause WAL replay")
	}

	// Wait for WAL replay to actually be paused and capture status at that moment
	pm.logger.Info("Waiting for WAL replay to complete pausing")
	status, err := pm.waitForReplicationPause(ctx)
	if err != nil {
		return nil, err
	}

	pm.logger.Info("StopReplicationAndGetStatus completed",
		"lsn", status.Lsn,
		"is_paused", status.IsWalReplayPaused,
		"pause_state", status.WalReplayPauseState,
		"primary_conn_info", status.PrimaryConnInfo)

	return status, nil
}

// waitForReplicationPause polls until WAL replay is paused and returns the status at that moment.
// This ensures the LSN returned represents the exact point at which replication stopped.
func (pm *MultiPoolerManager) waitForReplicationPause(ctx context.Context) (*multipoolermanagerdatapb.ReplicationStatus, error) {
	// Create a context with timeout for the polling loop
	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	query := `SELECT
		pg_last_wal_replay_lsn(),
		pg_is_wal_replay_paused(),
		pg_get_wal_replay_pause_state(),
		pg_last_xact_replay_timestamp(),
		current_setting('primary_conninfo')`

	for {
		select {
		case <-waitCtx.Done():
			if waitCtx.Err() == context.DeadlineExceeded {
				pm.logger.Error("Timeout waiting for WAL replay to pause")
				return nil, mterrors.New(mtrpcpb.Code_DEADLINE_EXCEEDED, "timeout waiting for WAL replay to pause")
			}
			pm.logger.Error("Context cancelled while waiting for WAL replay to pause")
			return nil, mterrors.Wrap(waitCtx.Err(), "context cancelled while waiting for WAL replay to pause")

		case <-ticker.C:
			var lsn string
			var isPaused bool
			var pauseState string
			var lastXactTime sql.NullString
			var primaryConnInfo string

			// Query all status fields in each iteration
			err := pm.db.QueryRowContext(waitCtx, query).Scan(
				&lsn,
				&isPaused,
				&pauseState,
				&lastXactTime,
				&primaryConnInfo,
			)
			if err != nil {
				pm.logger.Error("Failed to get replication status", "error", err)
				return nil, mterrors.Wrap(err, "failed to get replication status")
			}

			// Once paused, we have the exact state at the moment replication stopped
			if isPaused {
				pm.logger.Info("WAL replay is now paused", "lsn", lsn, "pause_state", pauseState)

				// Build and return the status
				status := &multipoolermanagerdatapb.ReplicationStatus{
					Lsn:                 lsn,
					IsWalReplayPaused:   isPaused,
					WalReplayPauseState: pauseState,
				}

				if lastXactTime.Valid {
					status.LastXactReplayTimestamp = lastXactTime.String
				}

				// Parse primary_conninfo into structured format
				parsedConnInfo, err := parseAndRedactPrimaryConnInfo(primaryConnInfo)
				if err != nil {
					return nil, mterrors.Wrap(err, "failed to parse primary_conninfo")
				}
				status.PrimaryConnInfo = parsedConnInfo

				return status, nil
			}
		}
	}
}

// ChangeType changes the pooler type (PRIMARY/REPLICA)
func (pm *MultiPoolerManager) ChangeType(ctx context.Context, poolerType string) error {
	if err := pm.checkReady(); err != nil {
		return err
	}

	// Acquire the action lock to ensure only one mutation runs at a time
	if err := pm.lock(ctx); err != nil {
		return err
	}
	defer pm.unlock()

	// Validate pooler type
	var newType clustermetadatapb.PoolerType
	// TODO: For now allow to change type to PRIMARY, this is to make it easier
	// to perform tests while we are still developing HA. Once, we have multiorch
	// fully implemented, we shouldn't allow to change the type to Primary.
	// This would happen organically as part of Promote workflow.
	switch poolerType {
	case "PRIMARY":
		newType = clustermetadatapb.PoolerType_PRIMARY
	case "REPLICA":
		newType = clustermetadatapb.PoolerType_REPLICA
	default:
		return mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT,
			fmt.Sprintf("invalid pooler type: %s, must be PRIMARY or REPLICA", poolerType))
	}

	pm.logger.Info("ChangeType called", "pooler_type", poolerType, "service_id", pm.serviceID.String())
	// Update the multipooler record in topology
	updatedMultipooler, err := pm.topoClient.UpdateMultiPoolerFields(ctx, pm.serviceID, func(mp *clustermetadatapb.MultiPooler) error {
		mp.Type = newType
		return nil
	})
	if err != nil {
		pm.logger.Error("Failed to update pooler type in topology", "error", err, "service_id", pm.serviceID.String())
		return mterrors.Wrap(err, "failed to update pooler type in topology")
	}

	pm.mu.Lock()
	defer pm.mu.Unlock()
	pm.multipooler.MultiPooler = updatedMultipooler
	pm.logger.Info("Pooler type updated successfully", "new_type", poolerType, "service_id", pm.serviceID.String())

	return nil
}

// Status returns the current manager status and error information
func (pm *MultiPoolerManager) Status(ctx context.Context) (*multipoolermanagerdatapb.StatusResponse, error) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	state := string(pm.state)
	var errorMessage string
	if pm.stateError != nil {
		errorMessage = pm.stateError.Error()
	}

	return &multipoolermanagerdatapb.StatusResponse{
		State:        state,
		ErrorMessage: errorMessage,
	}, nil
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

// GetFollowers gets the list of follower servers with detailed replication status
func (pm *MultiPoolerManager) GetFollowers(ctx context.Context) (*multipoolermanagerdatapb.GetFollowersResponse, error) {
	if err := pm.checkReady(); err != nil {
		return nil, err
	}

	pm.logger.Info("GetFollowers called")

	// Check PRIMARY guardrails (only primary can have followers)
	if err := pm.checkPrimaryGuardrails(ctx); err != nil {
		return nil, err
	}

	// Get current synchronous replication configuration
	syncConfig, err := pm.getSynchronousReplicationConfig(ctx)
	if err != nil {
		return nil, err
	}

	// Query pg_stat_replication for all connected followers with full details
	query := `SELECT
		pid,
		application_name,
		client_addr::text,
		state,
		sync_state,
		sent_lsn::text,
		write_lsn::text,
		flush_lsn::text,
		replay_lsn::text,
		EXTRACT(EPOCH FROM write_lag),
		EXTRACT(EPOCH FROM flush_lag),
		EXTRACT(EPOCH FROM replay_lag)
	FROM pg_stat_replication
	WHERE application_name IS NOT NULL AND application_name != ''`

	rows, err := pm.db.QueryContext(ctx, query)
	if err != nil {
		pm.logger.Error("Failed to query pg_stat_replication", "error", err)
		return nil, mterrors.Wrap(err, "failed to query replication status")
	}
	defer rows.Close()

	// Build a map of connected followers by application_name
	connectedMap := make(map[string]*multipoolermanagerdatapb.ReplicationStats)
	for rows.Next() {
		var pid int32
		var appName string
		var clientAddr string
		var state string
		var syncState string
		var sentLsn string
		var writeLsn string
		var flushLsn string
		var replayLsn string
		var writeLagSecs sql.NullFloat64
		var flushLagSecs sql.NullFloat64
		var replayLagSecs sql.NullFloat64

		err := rows.Scan(
			&pid,
			&appName,
			&clientAddr,
			&state,
			&syncState,
			&sentLsn,
			&writeLsn,
			&flushLsn,
			&replayLsn,
			&writeLagSecs,
			&flushLagSecs,
			&replayLagSecs,
		)
		if err != nil {
			pm.logger.Error("Failed to scan replication row", "error", err)
			continue
		}

		stats := &multipoolermanagerdatapb.ReplicationStats{
			Pid:        pid,
			ClientAddr: clientAddr,
			State:      state,
			SyncState:  syncState,
			SentLsn:    sentLsn,
			WriteLsn:   writeLsn,
			FlushLsn:   flushLsn,
			ReplayLsn:  replayLsn,
		}

		// Convert lag values from seconds to Duration (only if not null)
		if writeLagSecs.Valid {
			stats.WriteLag = durationpb.New(time.Duration(writeLagSecs.Float64 * float64(time.Second)))
		}
		if flushLagSecs.Valid {
			stats.FlushLag = durationpb.New(time.Duration(flushLagSecs.Float64 * float64(time.Second)))
		}
		if replayLagSecs.Valid {
			stats.ReplayLag = durationpb.New(time.Duration(replayLagSecs.Float64 * float64(time.Second)))
		}

		connectedMap[appName] = stats
	}
	if err := rows.Err(); err != nil {
		pm.logger.Error("Error iterating pg_stat_replication rows", "error", err)
		return nil, mterrors.Wrap(err, "failed to read replication status")
	}

	// Build the response with all configured standbys
	followers := make([]*multipoolermanagerdatapb.FollowerInfo, 0, len(syncConfig.StandbyIds))
	for _, standbyID := range syncConfig.StandbyIds {
		appName := generateApplicationName(standbyID)

		followerInfo := &multipoolermanagerdatapb.FollowerInfo{
			FollowerId:      standbyID,
			ApplicationName: appName,
		}

		// Check if this standby is currently connected
		if stats, connected := connectedMap[appName]; connected {
			followerInfo.IsConnected = true
			followerInfo.ReplicationStats = stats
		} else {
			followerInfo.IsConnected = false
			// ReplicationStats remains nil for disconnected followers
		}

		followers = append(followers, followerInfo)
	}

	pm.logger.Info("GetFollowers completed",
		"total_configured", len(followers),
		"connected_count", len(connectedMap))

	return &multipoolermanagerdatapb.GetFollowersResponse{
		Followers:  followers,
		SyncConfig: syncConfig,
	}, nil
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
		pm.logger.Error("Failed to check recovery status", "error", err)
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
			pm.logger.Error("Failed to get LSN (tried both primary and standby methods)", "error", err)
			return nil, mterrors.Wrap(err, "failed to get LSN")
		}
	}

	pm.logger.Info("Checked demotion state",
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
		pm.logger.Info("Already in SERVING_RDONLY state, skipping")
		return nil
	}

	pm.logger.Info("Transitioning to SERVING_RDONLY")

	// Update serving status in topology
	updatedMultipooler, err := pm.topoClient.UpdateMultiPoolerFields(ctx, pm.serviceID, func(mp *clustermetadatapb.MultiPooler) error {
		mp.ServingStatus = clustermetadatapb.PoolerServingStatus_SERVING_RDONLY
		return nil
	})
	if err != nil {
		pm.logger.Error("Failed to update serving status in topology", "error", err)
		return mterrors.Wrap(err, "failed to transition to SERVING_RDONLY")
	}

	pm.mu.Lock()
	pm.multipooler.MultiPooler = updatedMultipooler
	pm.queryServingState = clustermetadatapb.PoolerServingStatus_SERVING_RDONLY
	pm.mu.Unlock()

	// Stop heartbeat writer
	if pm.replTracker != nil {
		pm.logger.Info("Stopping heartbeat writer")
		pm.replTracker.MakeNonPrimary()
	}

	// TODO: Configure QueryService to reject writes

	pm.logger.Info("Transitioned to SERVING_RDONLY successfully")
	return nil
}

// runCheckpointAsync runs a CHECKPOINT in a background goroutine
func (pm *MultiPoolerManager) runCheckpointAsync(ctx context.Context) chan error {
	checkpointDone := make(chan error, 1)
	go func() {
		pm.logger.Info("Starting checkpoint")
		_, err := pm.db.ExecContext(ctx, "CHECKPOINT")
		if err != nil {
			pm.logger.Warn("Checkpoint failed", "error", err)
			checkpointDone <- err
		} else {
			pm.logger.Info("Checkpoint completed")
			checkpointDone <- nil
		}
	}()
	return checkpointDone
}

// restartPostgresAsStandby restarts PostgreSQL as a standby server
// This creates standby.signal and restarts PostgreSQL via pgctld
func (pm *MultiPoolerManager) restartPostgresAsStandby(ctx context.Context, state *demotionState) error {
	if state.isReadOnly {
		pm.logger.Info("PostgreSQL already running as standby, skipping")
		return nil
	}

	if pm.pgctldClient == nil {
		return mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION, "pgctld client not initialized")
	}

	pm.logger.Info("Restarting PostgreSQL as standby")

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
		pm.logger.Error("Failed to restart PostgreSQL as standby", "error", err)
		return mterrors.Wrap(err, "failed to restart as standby")
	}

	pm.logger.Info("PostgreSQL restarted as standby",
		"pid", resp.Pid,
		"message", resp.Message)

	// Close database connection since PostgreSQL restarted
	if pm.db != nil {
		pm.db.Close()
		pm.db = nil
	}

	// Reconnect to PostgreSQL
	if err := pm.connectDB(); err != nil {
		pm.logger.Error("Failed to reconnect to database after restart", "error", err)
		return mterrors.Wrap(err, "failed to reconnect to database")
	}

	// Verify server is in recovery mode (standby)
	var inRecovery bool
	err = pm.db.QueryRowContext(ctx, "SELECT pg_is_in_recovery()").Scan(&inRecovery)
	if err != nil {
		pm.logger.Error("Failed to verify recovery status", "error", err)
		return mterrors.Wrap(err, "failed to verify standby status")
	}

	if !inRecovery {
		pm.logger.Error("PostgreSQL not in recovery mode after restart")
		return mterrors.New(mtrpcpb.Code_INTERNAL, "server not in recovery mode after restart as standby")
	}

	pm.logger.Info("PostgreSQL is now running as a standby")
	return nil
}

// resetSynchronousReplication clears the synchronous standby list
// This should be called after the server is read-only to safely clear settings
func (pm *MultiPoolerManager) resetSynchronousReplication(ctx context.Context) error {
	pm.logger.Info("Clearing synchronous standby list")

	// Clear synchronous_standby_names to remove all standbys
	_, err := pm.db.ExecContext(ctx, "ALTER SYSTEM SET synchronous_standby_names = ''")
	if err != nil {
		pm.logger.Error("Failed to clear synchronous_standby_names", "error", err)
		return mterrors.Wrap(err, "failed to clear synchronous_standby_names")
	}

	// Reload configuration to apply changes
	_, err = pm.db.ExecContext(ctx, "SELECT pg_reload_conf()")
	if err != nil {
		pm.logger.Error("Failed to reload configuration", "error", err)
		return mterrors.Wrap(err, "failed to reload configuration after clearing standby list")
	}

	pm.logger.Info("Successfully cleared synchronous standby list")
	return nil
}

// updateTopologyAfterDemotion updates the pooler type in topology from PRIMARY to REPLICA
func (pm *MultiPoolerManager) updateTopologyAfterDemotion(ctx context.Context, state *demotionState) error {
	if state.isReplicaInTopology {
		pm.logger.Info("Topology already updated to REPLICA, skipping")
		return nil
	}

	pm.logger.Info("Updating pooler type in topology to REPLICA")
	updatedMultipooler, err := pm.topoClient.UpdateMultiPoolerFields(ctx, pm.serviceID, func(mp *clustermetadatapb.MultiPooler) error {
		mp.Type = clustermetadatapb.PoolerType_REPLICA
		return nil
	})
	if err != nil {
		pm.logger.Error("Failed to update pooler type in topology", "error", err)
		return mterrors.Wrap(err, "demotion succeeded but failed to update topology")
	}

	pm.mu.Lock()
	pm.multipooler.MultiPooler = updatedMultipooler
	pm.mu.Unlock()

	pm.logger.Info("Topology updated to REPLICA successfully")
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
		pm.logger.Error("Failed to get active write connections", "error", err)
		return 0, mterrors.Wrap(err, "failed to get active write connections")
	}

	if len(pids) == 0 {
		pm.logger.Info("No active write connections to terminate")
		return 0, nil
	}

	pm.logger.Warn("Terminating connections still performing writes after drain",
		"count", len(pids),
		"pids", pids)

	// Terminate each write connection
	for _, pid := range pids {
		_, err := pm.db.ExecContext(ctx, "SELECT pg_terminate_backend($1)", pid)
		if err != nil {
			pm.logger.Warn("Failed to terminate write connection", "pid", pid, "error", err)
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
	pm.logger.Info("Monitoring for write activity during drain", "duration", drainTimeout)
	drainCtx, cancel := context.WithTimeout(ctx, drainTimeout)
	defer cancel()

	monitorTicker := time.NewTicker(100 * time.Millisecond)
	defer monitorTicker.Stop()

	consecutiveNoWrites := 0
	drainComplete := false

	for !drainComplete {
		select {
		case <-drainCtx.Done():
			pm.logger.Info("Drain timeout completed")
			drainComplete = true

		case err := <-checkpointDone:
			if err != nil {
				pm.logger.Warn("Checkpoint completed with error during drain", "error", err)
			} else {
				pm.logger.Info("Checkpoint completed during drain")
			}

		case <-monitorTicker.C:
			// Check for write activity
			pids, err := pm.getActiveWriteConnections(ctx)
			if err != nil {
				pm.logger.Warn("Failed to check for write activity during drain", "error", err)
				consecutiveNoWrites = 0 // Reset on error
			} else if len(pids) > 0 {
				pm.logger.Warn("Detected write activity during drain",
					"count", len(pids),
					"pids", pids)
				consecutiveNoWrites = 0 // Reset counter
			} else {
				// No writes detected
				consecutiveNoWrites++
				if consecutiveNoWrites >= 2 {
					pm.logger.Info("No write activity detected for 2 consecutive checks, exiting drain early")
					drainComplete = true
				}
			}
		}
	}

	// Wait for checkpoint if it's still running
	select {
	case err := <-checkpointDone:
		if err != nil {
			pm.logger.Warn("Checkpoint failed", "error", err)
			// Don't fail - checkpoint is an optimization
		}
	default:
		// Checkpoint still running, continue
		pm.logger.Info("Checkpoint still running, continuing with demotion")
	}

	return nil
}

// getPrimaryLSN gets the current WAL write location (primary only)
func (pm *MultiPoolerManager) getPrimaryLSN(ctx context.Context) (string, error) {
	var lsn string
	err := pm.db.QueryRowContext(ctx, "SELECT pg_current_wal_lsn()::text").Scan(&lsn)
	if err != nil {
		return "", mterrors.Wrap(err, "failed to get current WAL LSN")
	}
	return lsn, nil
}

// getStandbyReplayLSN gets the last replayed WAL location (standby only)
func (pm *MultiPoolerManager) getStandbyReplayLSN(ctx context.Context) (string, error) {
	var lsn string
	err := pm.db.QueryRowContext(ctx, "SELECT pg_last_wal_replay_lsn()::text").Scan(&lsn)
	if err != nil {
		return "", mterrors.Wrap(err, "failed to get replay LSN")
	}
	return lsn, nil
}

// captureFinalLSN captures the final LSN position before going read-only
func (pm *MultiPoolerManager) captureFinalLSN(ctx context.Context, state *demotionState) (string, error) {
	if state.isReadOnly && state.finalLSN != "" {
		pm.logger.Info("Using previously captured LSN", "lsn", state.finalLSN)
		return state.finalLSN, nil
	}

	// At this point in Demote, we should still be a primary
	finalLSN, err := pm.getPrimaryLSN(ctx)
	if err != nil {
		pm.logger.Error("Failed to capture final LSN", "error", err)
		return "", err
	}

	pm.logger.Info("Captured final LSN", "lsn", finalLSN)
	return finalLSN, nil
}

// Demote demotes the current primary server
// This can be called for any of the following use cases:
// - By orchestrator when fixing a broken shard.
// - When performing a Planned demotion.
// - When receiving a SIGTERM and the pooler needs to shutdown.
func (pm *MultiPoolerManager) Demote(ctx context.Context, consensusTerm int64, drainTimeout time.Duration, force bool) (*multipoolermanagerdatapb.DemoteResponse, error) {
	if err := pm.checkReady(); err != nil {
		return nil, err
	}

	// Acquire the action lock to ensure only one mutation runs at a time
	if err := pm.lock(ctx); err != nil {
		return nil, err
	}
	defer pm.unlock()

	pm.logger.Info("Demote called",
		"consensus_term", consensusTerm,
		"drain_timeout", drainTimeout,
		"force", force)

	// === Validation & State Check ===

	// Demote is an operational cleanup, not a leadership change.
	// Accept if term >= currentTerm to ensure the request isn’t stale.
	// Equal or higher terms are safe.
	// Note: we still update the term, as this may arrive after an election
	// this (now old) primary missed due to a network partition.
	if err := pm.validateAndUpdateTerm(ctx, consensusTerm, force); err != nil {
		return nil, err
	}

	// Ensure database connection
	if err := pm.connectDB(); err != nil {
		pm.logger.Error("Failed to connect to database", "error", err)
		return nil, mterrors.Wrap(err, "database connection failed")
	}

	// Guard rail: Demote can only be called on a PRIMARY
	if err := pm.checkPrimaryGuardrails(ctx); err != nil {
		return nil, err
	}

	// Check current demotion state
	state, err := pm.checkDemotionState(ctx)
	if err != nil {
		return nil, err
	}

	// If everything is already complete, return early (fully idempotent)
	if state.isServingReadOnly && state.isReplicaInTopology && state.isReadOnly {
		pm.logger.Info("Demotion already complete (idempotent)",
			"lsn", state.finalLSN)
		return &multipoolermanagerdatapb.DemoteResponse{
			WasAlreadyDemoted:     true,
			ConsensusTerm:         consensusTerm,
			LsnPosition:           state.finalLSN,
			ConnectionsTerminated: 0,
		}, nil
	}

	// Transition to Read-Only Serving
	// For now, this is not that useful as we have to restart
	// the server anyways to make it a standby.
	// However, we are setting the hooks to make sure that
	// we can make the primary readonly first,
	// drain write connections and then transition it
	// as a replica without restarting postgres
	if err := pm.setServingReadOnly(ctx, state); err != nil {
		return nil, err
	}

	// Drain & Checkpoint (Parallel)

	if err := pm.drainAndCheckpoint(ctx, drainTimeout); err != nil {
		return nil, err
	}

	// Terminate Remaining Write Connections

	connectionsTerminated, err := pm.terminateWriteConnections(ctx)
	if err != nil {
		// Log but don't fail - connections will eventually timeout
		pm.logger.Warn("Failed to terminate write connections", "error", err)
	}

	// Capture State & Make PostgreSQL Read-Only

	finalLSN, err := pm.captureFinalLSN(ctx, state)
	if err != nil {
		return nil, err
	}

	if err := pm.restartPostgresAsStandby(ctx, state); err != nil {
		return nil, err
	}

	// Reset Synchronous Replication Configuration
	// Now that the server is read-only, it's safe to clear sync replication settings
	// This ensures we don't have a window where writes could be accepted with incorrect replication config
	if err := pm.resetSynchronousReplication(ctx); err != nil {
		// Log but don't fail - this is cleanup
		pm.logger.Warn("Failed to reset synchronous replication configuration", "error", err)
	}

	// Update Topology

	if err := pm.updateTopologyAfterDemotion(ctx, state); err != nil {
		return nil, err
	}

	pm.logger.Info("Demote completed successfully",
		"final_lsn", finalLSN,
		"consensus_term", consensusTerm,
		"connections_terminated", connectionsTerminated)

	return &multipoolermanagerdatapb.DemoteResponse{
		WasAlreadyDemoted:     false,
		ConsensusTerm:         consensusTerm,
		LsnPosition:           finalLSN,
		ConnectionsTerminated: connectionsTerminated,
	}, nil
}

// UndoDemote undoes a demotion
func (pm *MultiPoolerManager) UndoDemote(ctx context.Context) error {
	if err := pm.checkReady(); err != nil {
		return err
	}

	// Acquire the action lock to ensure only one mutation runs at a time
	if err := pm.lock(ctx); err != nil {
		return err
	}
	defer pm.unlock()

	pm.logger.Info("UndoDemote called")
	return mterrors.New(mtrpcpb.Code_UNIMPLEMENTED, "method UndoDemote not implemented")
}

// checkPromotionState checks the current state to determine what steps remain
func (pm *MultiPoolerManager) checkPromotionState(ctx context.Context, syncReplicationConfig *multipoolermanagerdatapb.ConfigureSynchronousReplicationRequest) (*promotionState, error) {
	state := &promotionState{}

	// Check PostgreSQL promotion state
	var isInRecovery bool
	err := pm.db.QueryRowContext(ctx, "SELECT pg_is_in_recovery()").Scan(&isInRecovery)
	if err != nil {
		pm.logger.Error("Failed to check recovery status", "error", err)
		return nil, mterrors.Wrap(err, "failed to check recovery status")
	}

	state.isPrimaryInPostgres = !isInRecovery

	if state.isPrimaryInPostgres {
		// Get current primary LSN
		state.currentLSN, err = pm.getPrimaryLSN(ctx)
		if err != nil {
			pm.logger.Error("Failed to get current LSN", "error", err)
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
			pm.logger.Warn("Failed to get current sync replication config", "error", err)
		}
		if err == nil {
			state.syncReplicationMatches = pm.syncReplicationConfigMatches(currentConfig, syncReplicationConfig)
		}
	}

	pm.logger.Info("Checked promotion state",
		"is_primary_in_postgres", state.isPrimaryInPostgres,
		"is_primary_in_topology", state.isPrimaryInTopology,
		"sync_replication_matches", state.syncReplicationMatches)

	return state, nil
}

// syncReplicationConfigMatches checks if the current sync replication config matches the requested config
func (pm *MultiPoolerManager) syncReplicationConfigMatches(current *multipoolermanagerdatapb.SynchronousReplicationConfiguration, requested *multipoolermanagerdatapb.ConfigureSynchronousReplicationRequest) bool {
	// Check synchronous commit level
	if current.SynchronousCommit != requested.SynchronousCommit {
		return false
	}

	// Check synchronous method
	if current.SynchronousMethod != requested.SynchronousMethod {
		return false
	}

	// Check num_sync
	if current.NumSync != requested.NumSync {
		return false
	}

	// Check standby IDs (must match exactly 1:1, so sort and compare)
	if len(current.StandbyIds) != len(requested.StandbyIds) {
		return false
	}

	// Sort both lists by cell_name for comparison
	currentSorted := make([]string, len(current.StandbyIds))
	for i, id := range current.StandbyIds {
		currentSorted[i] = fmt.Sprintf("%s_%s", id.Cell, id.Name)
	}
	sort.Strings(currentSorted)

	requestedSorted := make([]string, len(requested.StandbyIds))
	for i, id := range requested.StandbyIds {
		requestedSorted[i] = fmt.Sprintf("%s_%s", id.Cell, id.Name)
	}
	sort.Strings(requestedSorted)

	// Compare sorted lists element by element
	for i := range currentSorted {
		if currentSorted[i] != requestedSorted[i] {
			return false
		}
	}

	return true
}

// validateExpectedLSN validates that the current replay LSN matches the expected LSN
func (pm *MultiPoolerManager) validateExpectedLSN(ctx context.Context, expectedLSN string) error {
	if expectedLSN == "" {
		return nil // No validation requested
	}

	var currentLSN string
	var isPaused bool
	query := "SELECT pg_last_wal_replay_lsn()::text, pg_is_wal_replay_paused()"
	err := pm.db.QueryRowContext(ctx, query).Scan(&currentLSN, &isPaused)
	if err != nil {
		pm.logger.Error("Failed to get current replay LSN and pause state", "error", err)
		return mterrors.Wrap(err, "failed to get current replay LSN and pause state")
	}

	// Best practice: WAL replay should be paused before promotion
	// The coordinator should have called StopReplication during Discovery stage
	if !isPaused {
		pm.logger.Warn("WAL replay is not paused before promotion - coordinator may have skipped Discovery stage",
			"current_lsn", currentLSN,
			"expected_lsn", expectedLSN)
		// Note: We don't fail here as this is a soft check, but it indicates
		// a potential issue in the consensus flow
	}

	if currentLSN != expectedLSN {
		pm.logger.Error("LSN mismatch - node does not have expected durable state",
			"expected_lsn", expectedLSN,
			"current_lsn", currentLSN)
		return mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION,
			fmt.Sprintf("LSN mismatch: expected %s, current %s. "+
				"This indicates an error in an earlier consensus stage.",
				expectedLSN, currentLSN))
	}

	pm.logger.Info("LSN validation passed",
		"lsn", currentLSN,
		"wal_replay_paused", isPaused)
	return nil
}

// promoteStandbyToPrimary calls pg_promote() and waits for promotion to complete
func (pm *MultiPoolerManager) promoteStandbyToPrimary(ctx context.Context, state *promotionState) error {
	// Return early if already promoted
	if state.isPrimaryInPostgres {
		pm.logger.Info("PostgreSQL already promoted, skipping")
		return nil
	}

	// Call pg_promote() to promote standby to primary
	pm.logger.Info("PostgreSQL promotion needed")
	pm.logger.Info("Calling pg_promote() to promote standby to primary")
	_, err := pm.db.ExecContext(ctx, "SELECT pg_promote()")
	if err != nil {
		pm.logger.Error("Failed to call pg_promote()", "error", err)
		return mterrors.Wrap(err, "failed to promote standby")
	}

	// Wait for promotion to complete by polling pg_is_in_recovery()
	pm.logger.Info("Waiting for promotion to complete")
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
			pm.logger.Error("Timeout waiting for promotion to complete")
			return mterrors.New(mtrpcpb.Code_DEADLINE_EXCEEDED,
				fmt.Sprintf("timeout waiting for promotion to complete after %v", promotionTimeout))

		case <-ticker.C:
			var isInRecovery bool
			err := pm.db.QueryRowContext(promotionCtx, "SELECT pg_is_in_recovery()").Scan(&isInRecovery)
			if err != nil {
				pm.logger.Error("Failed to check recovery status during promotion", "error", err)
				return mterrors.Wrap(err, "failed to check recovery status")
			}

			if !isInRecovery {
				pm.logger.Info("Promotion completed successfully - node is now primary")
				return nil
			}
		}
	}
}

// updateTopologyAfterPromotion updates the pooler type in topology from REPLICA to PRIMARY
func (pm *MultiPoolerManager) updateTopologyAfterPromotion(ctx context.Context, state *promotionState) error {
	// Return early if already updated
	if state.isPrimaryInTopology {
		pm.logger.Info("Topology already updated, skipping")
		return nil
	}

	pm.logger.Info("Topology update needed")
	pm.logger.Info("Updating pooler type in topology to PRIMARY")
	updatedMultipooler, err := pm.topoClient.UpdateMultiPoolerFields(ctx, pm.serviceID, func(mp *clustermetadatapb.MultiPooler) error {
		mp.Type = clustermetadatapb.PoolerType_PRIMARY
		return nil
	})
	if err != nil {
		pm.logger.Error("Failed to update pooler type in topology", "error", err)
		return mterrors.Wrap(err, "promotion succeeded but failed to update topology")
	}

	pm.mu.Lock()
	pm.multipooler.MultiPooler = updatedMultipooler
	pm.mu.Unlock()

	// Update heartbeat tracker to primary mode
	if pm.replTracker != nil {
		pm.logger.Info("Updating heartbeat tracker to primary mode")
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
		pm.logger.Info("Sync replication already configured, skipping")
		return nil
	}

	pm.logger.Info("Sync replication configuration needed")
	pm.logger.Info("Configuring synchronous replication for new cohort")
	err := pm.ConfigureSynchronousReplication(ctx,
		syncReplicationConfig.SynchronousCommit,
		syncReplicationConfig.SynchronousMethod,
		syncReplicationConfig.NumSync,
		syncReplicationConfig.StandbyIds,
		syncReplicationConfig.ReloadConfig)
	if err != nil {
		pm.logger.Error("Failed to configure synchronous replication", "error", err)
		return mterrors.Wrap(err, "promotion succeeded but failed to configure synchronous replication")
	}

	return nil
}

// Promote promotes a standby to primary
// This is called during the Propagate stage of generalized consensus to safely
// transition a standby to primary and reconfigure replication.
// This operation is fully idempotent - it checks what steps are already complete
// and only executes the missing steps.
func (pm *MultiPoolerManager) Promote(ctx context.Context, consensusTerm int64, expectedLSN string, syncReplicationConfig *multipoolermanagerdatapb.ConfigureSynchronousReplicationRequest, force bool) (*multipoolermanagerdatapb.PromoteResponse, error) {
	if err := pm.checkReady(); err != nil {
		return nil, err
	}

	// Acquire the action lock to ensure only one mutation runs at a time
	if err := pm.lock(ctx); err != nil {
		return nil, err
	}
	defer pm.unlock()

	// Guard rail: Promote can only be called on a REPLICA
	if err := pm.checkReplicaGuardrails(ctx); err != nil {
		return nil, err
	}

	pm.logger.Info("Promote called",
		"consensus_term", consensusTerm,
		"expected_lsn", expectedLSN,
		"force", force)

	// Validation & Readiness

	// Validate term - strict equality, no automatic updates
	if err := pm.validateTermExactMatch(ctx, consensusTerm, force); err != nil {
		return nil, err
	}

	// Ensure database connection
	if err := pm.connectDB(); err != nil {
		pm.logger.Error("Failed to connect to database", "error", err)
		return nil, mterrors.Wrap(err, "database connection failed")
	}

	// Check current promotion state to determine what needs to be done
	state, err := pm.checkPromotionState(ctx, syncReplicationConfig)
	if err != nil {
		return nil, err
	}

	// If everything is already complete, return early (fully idempotent)
	if state.isPrimaryInPostgres && state.isPrimaryInTopology && state.syncReplicationMatches {
		pm.logger.Info("Promotion already complete (idempotent)",
			"lsn", state.currentLSN)
		return &multipoolermanagerdatapb.PromoteResponse{
			LsnPosition:       state.currentLSN,
			WasAlreadyPrimary: true,
			ConsensusTerm:     consensusTerm,
		}, nil
	}

	// If PostgreSQL is not promoted yet, validate expected LSN before promotion
	if !state.isPrimaryInPostgres {
		if err := pm.validateExpectedLSN(ctx, expectedLSN); err != nil {
			return nil, err
		}
	}

	// Execute missing steps

	// Promote PostgreSQL if needed
	if err := pm.promoteStandbyToPrimary(ctx, state); err != nil {
		return nil, err
	}

	// Update topology if needed
	if err := pm.updateTopologyAfterPromotion(ctx, state); err != nil {
		return nil, err
	}

	// Configure sync replication if needed
	if err := pm.configureReplicationAfterPromotion(ctx, state, syncReplicationConfig); err != nil {
		return nil, err
	}

	// TODO: Populate consensus metadata tables.

	// Get final LSN position
	finalLSN, err := pm.getPrimaryLSN(ctx)
	if err != nil {
		pm.logger.Error("Failed to get final LSN", "error", err)
		return nil, err
	}

	pm.logger.Info("Promote completed successfully",
		"final_lsn", finalLSN,
		"consensus_term", consensusTerm,
		"was_already_primary", state.isPrimaryInPostgres)

	return &multipoolermanagerdatapb.PromoteResponse{
		LsnPosition:       finalLSN,
		WasAlreadyPrimary: state.isPrimaryInPostgres && state.isPrimaryInTopology && state.syncReplicationMatches,
		ConsensusTerm:     consensusTerm,
	}, nil
}

// SetTerm sets the consensus term information to local disk
func (pm *MultiPoolerManager) SetTerm(ctx context.Context, term *multipoolermanagerdatapb.ConsensusTerm) error {
	if err := pm.checkReady(); err != nil {
		return err
	}

	// Check if consensus service is enabled
	if pm.consensusState == nil {
		return mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION,
			"consensus service not enabled")
	}

	// Acquire the action lock to ensure only one mutation runs at a time
	if err := pm.lock(ctx); err != nil {
		return err
	}
	defer pm.unlock()

	pm.logger.Info("SetTerm called", "current_term", term.GetCurrentTerm())

	// Update consensus state
	pm.consensusState.SetTerm(term)

	// Persist to disk
	if err := pm.consensusState.Save(); err != nil {
		pm.logger.Error("Failed to save consensus term to disk", "error", err)
		return mterrors.Wrap(err, "failed to save consensus term")
	}

	// Synchronize term to heartbeat writer if it exists
	if pm.replTracker != nil {
		pm.replTracker.HeartbeatWriter().SetLeaderTerm(term.GetCurrentTerm())
		pm.logger.Info("Synchronized term to heartbeat writer", "term", term.GetCurrentTerm())
	}

	pm.logger.Info("SetTerm completed successfully", "current_term", term.GetCurrentTerm())
	return nil
}

// Start initializes the MultiPoolerManager
// This method follows the Vitess pattern similar to TabletManager.Start() in tm_init.go
func (pm *MultiPoolerManager) Start(senv *servenv.ServEnv) {
	// Start loading multipooler record from topology asynchronously
	go pm.loadMultiPoolerFromTopo()

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

		// Start loading consensus term from local disk asynchronously
		// This must happen after service registration so we know if consensus is enabled
		pm.mu.Lock()
		consensusEnabled := pm.consensusState != nil
		pm.mu.Unlock()

		if consensusEnabled {
			go pm.loadConsensusTermFromDisk()
		}
	})
}
