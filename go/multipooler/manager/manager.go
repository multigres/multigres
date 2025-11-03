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

		pm.logger.Info("Loaded multipooler record from topology", "service_id", pm.serviceID.String(), "database", mp.Database)
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

		newTerm := &multipoolermanagerdatapb.ConsensusTerm{
			CurrentTerm:        requestTerm,
			AcceptedLeader:     nil,
			LastAcceptanceTime: nil,
			LeaderId:           nil,
		}

		// Update term to local disk using the term_storage functions
		if err := SetTerm(pm.config.PoolerDir, newTerm); err != nil {
			pm.logger.Error("Failed to update term to disk", "error", err)
			return mterrors.Wrap(err, "failed to update consensus term")
		}

		// Update our cached term
		pm.mu.Lock()
		pm.consensusTerm = newTerm
		pm.mu.Unlock()

		// Synchronize term to heartbeat writer if it exists
		if pm.replTracker != nil {
			pm.replTracker.HeartbeatWriter().SetLeaderTerm(newTerm.GetCurrentTerm())
			pm.logger.Info("Synchronized term to heartbeat writer", "term", newTerm.GetCurrentTerm())
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

	currentTerm := pm.getCurrentTerm()

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

// generateApplicationName generates the application_name for a multipooler from its ID
// Format: {cell}_{name}
// This is used consistently for:
// - SetPrimaryConnInfo: standby's application_name when connecting to primary
// - ConfigureSynchronousReplication: standby names in synchronous_standby_names
func generateApplicationName(id *clustermetadatapb.ID) string {
	return fmt.Sprintf("%s_%s", id.Cell, id.Name)
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
