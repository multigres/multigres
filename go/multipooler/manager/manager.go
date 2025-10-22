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
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/multigres/multigres/go/clustermetadata/topo"
	"github.com/multigres/multigres/go/mterrors"
	"github.com/multigres/multigres/go/multipooler/heartbeat"
	"github.com/multigres/multigres/go/servenv"
	"github.com/multigres/multigres/go/tools/timertools"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolermanagerdata "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
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
	logger      *slog.Logger
	config      *Config
	db          *sql.DB
	topoClient  topo.Store
	serviceID   *clustermetadatapb.ID
	replTracker *heartbeat.ReplTracker

	// Multipooler record from topology and startup state
	mu              sync.RWMutex
	multipooler     *topo.MultiPoolerInfo
	state           ManagerState
	stateError      error
	consensusTerm   *pgctldpb.ConsensusTerm
	topoLoaded      bool
	consensusLoaded bool
	ctx             context.Context
	cancel          context.CancelFunc
	loadTimeout     time.Duration
}

// NewMultiPoolerManager creates a new MultiPoolerManager instance
func NewMultiPoolerManager(logger *slog.Logger, config *Config) *MultiPoolerManager {
	return NewMultiPoolerManagerWithTimeout(logger, config, 5*time.Minute)
}

// NewMultiPoolerManagerWithTimeout creates a new MultiPoolerManager instance with a custom load timeout
func NewMultiPoolerManagerWithTimeout(logger *slog.Logger, config *Config, loadTimeout time.Duration) *MultiPoolerManager {
	ctx, cancel := context.WithCancel(context.Background())
	return &MultiPoolerManager{
		logger:      logger,
		config:      config,
		topoClient:  config.TopoClient,
		serviceID:   config.ServiceID,
		state:       ManagerStateStarting,
		ctx:         ctx,
		cancel:      cancel,
		loadTimeout: loadTimeout,
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
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	return pm.state
}

// GetStateError returns the error that caused the manager to enter error state
func (pm *MultiPoolerManager) GetStateError() error {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	return pm.stateError
}

// GetMultiPooler returns the current multipooler record and state
func (pm *MultiPoolerManager) GetMultiPooler() (*topo.MultiPoolerInfo, ManagerState, error) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	return pm.multipooler, pm.state, pm.stateError
}

// checkReady returns an error if the manager is not in Ready state
func (pm *MultiPoolerManager) checkReady() error {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

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
	pm.mu.RLock()
	poolerType := pm.multipooler.Type
	pm.mu.RUnlock()

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

	ticker := timertools.NewBackoffTicker(100*time.Millisecond, 30*time.Second)
	<-ticker.C
	defer ticker.Stop()

	// Set timeout for the entire loading process
	timeoutCtx, timeoutCancel := context.WithTimeout(pm.ctx, pm.loadTimeout)
	defer timeoutCancel()

	for {
		select {
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(pm.ctx, 5*time.Second)
			mp, err := pm.topoClient.GetMultiPooler(ctx, pm.serviceID)
			cancel()

			if err != nil {
				continue
			}

			// Successfully loaded
			pm.mu.Lock()
			pm.multipooler = mp
			pm.topoLoaded = true
			pm.mu.Unlock()

			pm.logger.Info("Loaded multipooler record from topology", "service_id", pm.serviceID.String(), "database", mp.Database)
			pm.checkAndSetReady()
			return

		case <-timeoutCtx.Done():
			pm.setStateError(fmt.Errorf("timeout waiting for multipooler record to be available in topology after %v", pm.loadTimeout))
			return

		case <-pm.ctx.Done():
			pm.setStateError(fmt.Errorf("manager context cancelled while loading multipooler record"))
			return
		}
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

	pm.mu.RLock()
	currentTerm := int64(0)
	if pm.consensusTerm != nil {
		currentTerm = pm.consensusTerm.GetCurrentTerm()
	}
	pm.mu.RUnlock()

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

		newTerm := &pgctldpb.ConsensusTerm{
			CurrentTerm:  requestTerm,
			VotedFor:     nil,
			LastVoteTime: nil,
			LeaderId:     nil,
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

		pm.logger.Info("Consensus term updated successfully", "new_term", requestTerm)
	}
	// If requestTerm == currentCachedTerm, just continue (same term is OK)
	return nil
}

// loadConsensusTermFromDisk loads the consensus term from local disk asynchronously
func (pm *MultiPoolerManager) loadConsensusTermFromDisk() {
	ticker := timertools.NewBackoffTicker(100*time.Millisecond, 30*time.Second)
	defer ticker.Stop()

	// Set timeout for the entire loading process
	timeoutCtx, timeoutCancel := context.WithTimeout(pm.ctx, pm.loadTimeout)
	defer timeoutCancel()

	for {
		select {
		case <-ticker.C:
			// Load term from local disk using the term_storage functions
			term, err := GetTerm(pm.config.PoolerDir)
			if err != nil {
				pm.logger.Debug("Failed to get consensus term from disk, retrying", "error", err)
				continue
			}

			// Successfully loaded (nil/empty term is OK)
			pm.mu.Lock()
			pm.consensusTerm = term
			pm.consensusLoaded = true
			pm.mu.Unlock()

			pm.logger.Info("Loaded consensus term from disk", "current_term", term.GetCurrentTerm())
			pm.checkAndSetReady()
			return

		case <-timeoutCtx.Done():
			pm.setStateError(fmt.Errorf("timeout waiting for consensus term from disk after %v", pm.loadTimeout))
			return

		case <-pm.ctx.Done():
			pm.setStateError(fmt.Errorf("manager context cancelled while loading consensus term"))
			return
		}
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

// parseAndRedactPrimaryConnInfo parses a PostgreSQL primary_conninfo connection string into structured fields
// Example input: "host=localhost port=5432 user=postgres application_name=cell_name"
// Returns a PrimaryConnInfo message with parsed fields, or an error if parsing fails
// Note: Passwords are redacted in the raw field for security
func parseAndRedactPrimaryConnInfo(connInfoStr string) (*multipoolermanagerdata.PrimaryConnInfo, error) {
	connInfo := &multipoolermanagerdata.PrimaryConnInfo{}

	// Simple space-based parsing of key=value pairs
	parts := strings.Split(connInfoStr, " ")
	redactedParts := make([]string, 0, len(parts))

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		kv := strings.SplitN(part, "=", 2)
		if len(kv) != 2 {
			// Not a key=value pair - parsing failed
			return nil, fmt.Errorf("invalid key=value format in primary_conninfo: %q", part)
		}

		key := strings.TrimSpace(kv[0])
		value := strings.TrimSpace(kv[1])

		if key == "" {
			return nil, fmt.Errorf("empty key in primary_conninfo: %q", part)
		}

		// Redact sensitive fields in the raw string
		if key == "password" {
			redactedParts = append(redactedParts, key+"=[REDACTED]")
		} else {
			redactedParts = append(redactedParts, part)
		}

		// Parse specific fields we care about
		switch key {
		case "host":
			connInfo.Host = value
		case "port":
			if port, err := strconv.ParseInt(value, 10, 32); err == nil {
				connInfo.Port = int32(port)
			}
		case "user":
			connInfo.User = value
		case "application_name":
			connInfo.ApplicationName = value
		}
	}

	// Set the redacted raw string
	connInfo.Raw = strings.Join(redactedParts, " ")

	return connInfo, nil
}

// SetPrimaryConnInfo sets the primary connection info for a standby server
func (pm *MultiPoolerManager) SetPrimaryConnInfo(ctx context.Context, host string, port int32, stopReplicationBefore, startReplicationAfter bool, currentTerm int64, force bool) error {
	if err := pm.checkReady(); err != nil {
		return err
	}
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
	pm.mu.RLock()
	database := pm.multipooler.Database
	pm.mu.RUnlock()

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

	pm.logger.Info("StopReplication completed successfully")
	return nil
}

// ReplicationStatus gets the current replication status of the standby
func (pm *MultiPoolerManager) ReplicationStatus(ctx context.Context) (*multipoolermanagerdata.ReplicationStatus, error) {
	if err := pm.checkReady(); err != nil {
		return nil, err
	}
	pm.logger.Info("ReplicationStatus called")

	// Check REPLICA guardrails (pooler type and recovery mode)
	if err := pm.checkReplicaGuardrails(ctx); err != nil {
		return nil, err
	}

	status := &multipoolermanagerdata.ReplicationStatus{}

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
func (pm *MultiPoolerManager) setSynchronousCommit(ctx context.Context, synchronousCommit multipoolermanagerdata.SynchronousCommitLevel) error {
	// Convert enum to PostgreSQL string value
	var syncCommitValue string
	switch synchronousCommit {
	case multipoolermanagerdata.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_OFF:
		syncCommitValue = "off"
	case multipoolermanagerdata.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_LOCAL:
		syncCommitValue = "local"
	case multipoolermanagerdata.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_REMOTE_WRITE:
		syncCommitValue = "remote_write"
	case multipoolermanagerdata.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON:
		syncCommitValue = "on"
	case multipoolermanagerdata.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_REMOTE_APPLY:
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

// setSynchronousStandbyNames builds and sets the PostgreSQL synchronous_standby_names configuration
// Format: https://www.postgresql.org/docs/current/runtime-config-replication.html#GUC-SYNCHRONOUS-STANDBY-NAMES
// Examples:
//
//	FIRST 2 (standby1, standby2, standby3)
//	ANY 1 (standby1, standby2)
//
// Note: Use '*' to match all connected standbys, or specify explicit standby application_name values
// Application names are generated from multipooler IDs using the shared generateApplicationName helper
func (pm *MultiPoolerManager) setSynchronousStandbyNames(ctx context.Context, synchronousMethod multipoolermanagerdata.SynchronousMethod, numSync int32, standbyIDs []*clustermetadatapb.ID) error {
	if len(standbyIDs) == 0 {
		return nil
	}

	// If numSync was not provided, default to 1
	if numSync == 0 {
		numSync = 1
	}

	// Convert multipooler IDs to quoted application names using the shared helper
	// This ensures consistency with SetPrimaryConnInfo
	// Produces: "cell_name1", "cell_name2"
	quotedNames := make([]string, len(standbyIDs))
	for i, id := range standbyIDs {
		quotedNames[i] = fmt.Sprintf(`"%s"`, generateApplicationName(id))
	}
	standbyList := strings.Join(quotedNames, ", ")

	// Build the final value with method prefix
	// This produces: FIRST 1 ("standby-1", "standby-2") or ANY 1 ("standby-1", "standby-2")
	var standbyNamesValue string
	switch synchronousMethod {
	case multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST:
		standbyNamesValue = fmt.Sprintf("FIRST %d (%s)", numSync, standbyList)
	case multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_ANY:
		standbyNamesValue = fmt.Sprintf("ANY %d (%s)", numSync, standbyList)
	default:
		return mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT,
			fmt.Sprintf("invalid synchronous method: %s, must be FIRST or ANY", synchronousMethod.String()))
	}

	pm.logger.Info("Setting synchronous_standby_names", "value", standbyNamesValue)

	// Escape single quotes in the value by doubling them (PostgreSQL standard)
	escapedValue := strings.ReplaceAll(standbyNamesValue, "'", "''")

	// ALTER SYSTEM SET doesn't support parameterized queries, so we use string formatting
	// Final query: ALTER SYSTEM SET synchronous_standby_names = 'FIRST 1 ("standby-1", "standby-2")'
	query := fmt.Sprintf("ALTER SYSTEM SET synchronous_standby_names = '%s'", escapedValue)
	_, err := pm.db.ExecContext(ctx, query)
	if err != nil {
		pm.logger.Error("Failed to set synchronous_standby_names", "error", err)
		return mterrors.Wrap(err, "failed to set synchronous_standby_names")
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
	}

	return nil
}

// ConfigureSynchronousReplication configures PostgreSQL synchronous replication settings
func (pm *MultiPoolerManager) ConfigureSynchronousReplication(ctx context.Context, synchronousCommit multipoolermanagerdata.SynchronousCommitLevel, synchronousMethod multipoolermanagerdata.SynchronousMethod, numSync int32, standbyIDs []*clustermetadatapb.ID, reloadConfig bool) error {
	if err := pm.checkReady(); err != nil {
		return err
	}

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

// PrimaryStatus gets the status of the leader server
func (pm *MultiPoolerManager) PrimaryStatus(ctx context.Context) (map[string]interface{}, error) {
	if err := pm.checkReady(); err != nil {
		return nil, err
	}
	pm.logger.Info("PrimaryStatus called")
	return nil, mterrors.New(mtrpcpb.Code_UNIMPLEMENTED, "method PrimaryStatus not implemented")
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

	// Query PostgreSQL for the current LSN position
	// pg_current_wal_lsn() returns the current write-ahead log write location
	var lsn string
	err := pm.db.QueryRowContext(ctx, "SELECT pg_current_wal_lsn()::text").Scan(&lsn)
	if err != nil {
		pm.logger.Error("Failed to query LSN", "error", err)
		return "", mterrors.Wrap(err, "failed to query LSN")
	}

	return lsn, nil
}

// StopReplicationAndGetStatus stops PostgreSQL replication and returns the status
func (pm *MultiPoolerManager) StopReplicationAndGetStatus(ctx context.Context) (map[string]interface{}, error) {
	if err := pm.checkReady(); err != nil {
		return nil, err
	}
	pm.logger.Info("StopReplicationAndGetStatus called")
	return nil, mterrors.New(mtrpcpb.Code_UNIMPLEMENTED, "method StopReplicationAndGetStatus not implemented")
}

// ChangeType changes the pooler type (PRIMARY/REPLICA)
func (pm *MultiPoolerManager) ChangeType(ctx context.Context, poolerType string) error {
	if err := pm.checkReady(); err != nil {
		return err
	}

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
func (pm *MultiPoolerManager) Status(ctx context.Context) (*multipoolermanagerdata.StatusResponse, error) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	state := string(pm.state)
	var errorMessage string
	if pm.stateError != nil {
		errorMessage = pm.stateError.Error()
	}

	return &multipoolermanagerdata.StatusResponse{
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

// GetFollowers gets the list of follower servers
func (pm *MultiPoolerManager) GetFollowers(ctx context.Context) ([]string, error) {
	if err := pm.checkReady(); err != nil {
		return nil, err
	}
	pm.logger.Info("GetFollowers called")
	return nil, mterrors.New(mtrpcpb.Code_UNIMPLEMENTED, "method GetFollowers not implemented")
}

// Demote demotes the current leader server
func (pm *MultiPoolerManager) Demote(ctx context.Context) error {
	if err := pm.checkReady(); err != nil {
		return err
	}
	pm.logger.Info("Demote called")
	return mterrors.New(mtrpcpb.Code_UNIMPLEMENTED, "method Demote not implemented")
}

// UndoDemote undoes a demotion
func (pm *MultiPoolerManager) UndoDemote(ctx context.Context) error {
	if err := pm.checkReady(); err != nil {
		return err
	}
	pm.logger.Info("UndoDemote called")
	return mterrors.New(mtrpcpb.Code_UNIMPLEMENTED, "method UndoDemote not implemented")
}

// Promote promotes a follower to leader
func (pm *MultiPoolerManager) Promote(ctx context.Context) error {
	if err := pm.checkReady(); err != nil {
		return err
	}
	pm.logger.Info("Promote called")
	return mterrors.New(mtrpcpb.Code_UNIMPLEMENTED, "method Promote not implemented")
}

// SetTerm sets the consensus term information to local disk
func (pm *MultiPoolerManager) SetTerm(ctx context.Context, term *pgctldpb.ConsensusTerm) error {
	if err := pm.checkReady(); err != nil {
		return err
	}

	pm.logger.Info("SetTerm called", "current_term", term.GetCurrentTerm())

	// Write term to local disk using the term_storage functions
	if err := SetTerm(pm.config.PoolerDir, term); err != nil {
		pm.logger.Error("Failed to set consensus term to disk", "error", err)
		return mterrors.Wrap(err, "failed to set consensus term")
	}

	// Update our cached term
	pm.mu.Lock()
	pm.consensusTerm = term
	pm.mu.Unlock()

	pm.logger.Info("SetTerm completed successfully", "current_term", term.GetCurrentTerm())
	return nil
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
