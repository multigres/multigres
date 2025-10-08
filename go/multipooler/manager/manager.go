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
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/multigres/multigres/go/clustermetadata/topo"
	"github.com/multigres/multigres/go/mterrors"
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
	logger     *slog.Logger
	config     *Config
	db         *sql.DB
	topoClient topo.Store
	serviceID  *clustermetadatapb.ID

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
	return nil
}

// Close closes the database connection and stops the async loader
func (pm *MultiPoolerManager) Close() error {
	pm.cancel()
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
	return mterrors.New(mtrpcpb.Code_UNIMPLEMENTED, "method WaitForLSN not implemented")
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

		// Update term in pgctld
		conn, err := pm.createPgctldClient()
		if err != nil {
			pm.logger.Error("Failed to create pgctld client for term update", "error", err)
			return mterrors.Wrap(err, "failed to create pgctld client for term update")
		}
		client := pgctldpb.NewPgCtldClient(conn)
		_, err = client.SetTerm(ctx, &pgctldpb.SetTermRequest{Term: newTerm})
		conn.Close()
		if err != nil {
			pm.logger.Error("Failed to update term in pgctld", "error", err)
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

// loadConsensusTermFromPgctld loads the consensus term from pgctld asynchronously
func (pm *MultiPoolerManager) loadConsensusTermFromPgctld() {
	ticker := timertools.NewBackoffTicker(100*time.Millisecond, 30*time.Second)
	defer ticker.Stop()

	// Set timeout for the entire loading process
	timeoutCtx, timeoutCancel := context.WithTimeout(pm.ctx, pm.loadTimeout)
	defer timeoutCancel()

	for {
		select {
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(pm.ctx, 5*time.Second)
			conn, err := pm.createPgctldClient()
			if err != nil {
				cancel()
				pm.logger.Debug("Failed to create pgctld client, retrying", "error", err)
				continue
			}

			client := pgctldpb.NewPgCtldClient(conn)
			resp, err := client.GetTerm(ctx, &pgctldpb.GetTermRequest{})
			conn.Close()
			cancel()

			if err != nil {
				pm.logger.Debug("Failed to get consensus term from pgctld, retrying", "error", err)
				continue
			}

			// Successfully loaded (nil/empty term is OK)
			pm.mu.Lock()
			pm.consensusTerm = resp.Term
			pm.consensusLoaded = true
			pm.mu.Unlock()

			pm.logger.Info("Loaded consensus term from pgctld", "current_term", resp.Term.GetCurrentTerm())
			pm.checkAndSetReady()
			return

		case <-timeoutCtx.Done():
			pm.setStateError(fmt.Errorf("timeout waiting for consensus term from pgctld after %v", pm.loadTimeout))
			return

		case <-pm.ctx.Done():
			pm.setStateError(fmt.Errorf("manager context cancelled while loading consensus term"))
			return
		}
	}
}

// SetPrimaryConnInfo sets the primary connection info for a standby server
func (pm *MultiPoolerManager) SetPrimaryConnInfo(ctx context.Context, host string, port int32, heartbeatInterval time.Duration, stopReplicationBefore, startReplicationAfter bool, currentTerm int64, force bool) error {
	if err := pm.checkReady(); err != nil {
		return err
	}
	pm.logger.Info("SetPrimaryConnInfo called",
		"host", host,
		"port", port,
		"heartbeat_interval", heartbeatInterval,
		"stop_replication_before", stopReplicationBefore,
		"start_replication_after", startReplicationAfter,
		"current_term", currentTerm,
		"force", force)

	// Validate and update consensus term following consensus rules
	if err := pm.validateAndUpdateTerm(ctx, currentTerm, force); err != nil {
		return err
	}

	// Guardrail: Check pooler type - only REPLICA poolers can set primary_conninfo
	pm.mu.RLock()
	poolerType := pm.multipooler.Type
	pm.mu.RUnlock()

	if poolerType != clustermetadatapb.PoolerType_REPLICA {
		pm.logger.Error("SetPrimaryConnInfo called on non-replica pooler",
			"service_id", pm.serviceID.String(),
			"pooler_type", poolerType.String())
		return mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION,
			fmt.Sprintf("operation not allowed: pooler type is %s, must be REPLICA (service_id: %s)",
				poolerType.String(), pm.serviceID.String()))
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

	connInfo := fmt.Sprintf("host=%s port=%d user=%s application_name=%s_%s",
		host, port, database, pm.serviceID.Cell, pm.serviceID.Name)

	// Add keepalive settings if heartbeat interval is specified
	if heartbeatInterval > 0 {
		// Convert heartbeat to seconds for PostgreSQL keepalive settings
		keepaliveSeconds := int(heartbeatInterval.Seconds())
		connInfo = fmt.Sprintf("%s keepalives_idle=%d keepalives_interval=%d keepalives_count=3",
			connInfo, keepaliveSeconds, keepaliveSeconds)
	}

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

	// Optionally start replication after making changes
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

// createPgctldClient creates a gRPC client connection to pgctld
// Handles both Unix socket and TCP connections
func (pm *MultiPoolerManager) createPgctldClient() (*grpc.ClientConn, error) {
	if pm.config.PgctldAddr == "" {
		return nil, mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION, "pgctld address not configured")
	}

	pgctldTarget := pm.config.PgctldAddr

	// Check if it's a Unix socket (starts with "unix://" or "/")
	if len(pgctldTarget) > 0 && (pgctldTarget[0] == '/' || (len(pgctldTarget) > 7 && pgctldTarget[:7] == "unix://")) {
		pm.logger.Info("Connecting to pgctld via Unix socket", "socket", pgctldTarget)
	} else {
		pm.logger.Info("Connecting to pgctld via TCP", "address", pgctldTarget)
	}

	// Connect to pgctld
	conn, err := grpc.NewClient(pgctldTarget, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		pm.logger.Error("Failed to connect to pgctld", "error", err, "pgctld_addr", pgctldTarget)
		return nil, mterrors.Wrap(err, "failed to connect to pgctld")
	}

	return conn, nil
}

// StartReplication starts WAL replay on standby (calls pg_wal_replay_resume)
func (pm *MultiPoolerManager) StartReplication(ctx context.Context) error {
	if err := pm.checkReady(); err != nil {
		return err
	}
	pm.logger.Info("StartReplication called")
	return mterrors.New(mtrpcpb.Code_UNIMPLEMENTED, "method StartReplication not implemented")
}

// StopReplication stops WAL replay on standby (calls pg_wal_replay_pause)
func (pm *MultiPoolerManager) StopReplication(ctx context.Context) error {
	if err := pm.checkReady(); err != nil {
		return err
	}
	pm.logger.Info("StopReplication called")
	return mterrors.New(mtrpcpb.Code_UNIMPLEMENTED, "method StopReplication not implemented")
}

// ReplicationStatus gets the current replication status of the standby
func (pm *MultiPoolerManager) ReplicationStatus(ctx context.Context) (map[string]interface{}, error) {
	if err := pm.checkReady(); err != nil {
		return nil, err
	}
	pm.logger.Info("ReplicationStatus called")
	return nil, mterrors.New(mtrpcpb.Code_UNIMPLEMENTED, "method ReplicationStatus not implemented")
}

// ResetReplication resets the standby's connection to its primary
func (pm *MultiPoolerManager) ResetReplication(ctx context.Context) error {
	if err := pm.checkReady(); err != nil {
		return err
	}
	pm.logger.Info("ResetReplication called")
	return mterrors.New(mtrpcpb.Code_UNIMPLEMENTED, "method ResetReplication not implemented")
}

// ConfigureSynchronousReplication configures PostgreSQL synchronous replication settings
func (pm *MultiPoolerManager) ConfigureSynchronousReplication(ctx context.Context, synchronousCommit string) error {
	if err := pm.checkReady(); err != nil {
		return err
	}
	pm.logger.Info("ConfigureSynchronousReplication called", "synchronous_commit", synchronousCommit)
	return mterrors.New(mtrpcpb.Code_UNIMPLEMENTED, "method ConfigureSynchronousReplication not implemented")
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

	// Check pooler type - only PRIMARY poolers can report primary position
	pm.mu.RLock()
	poolerType := pm.multipooler.Type
	pm.mu.RUnlock()

	if poolerType != clustermetadatapb.PoolerType_PRIMARY {
		pm.logger.Error("PrimaryPosition called on non-primary pooler",
			"service_id", pm.serviceID.String(),
			"pooler_type", poolerType.String())
		return "", mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION,
			fmt.Sprintf("operation not allowed: pooler type is %s, must be PRIMARY (service_id: %s)",
				poolerType.String(), pm.serviceID.String()))
	}

	pm.logger.Info("PrimaryPosition called")

	// Ensure database connection
	if err := pm.connectDB(); err != nil {
		pm.logger.Error("Failed to connect to database", "error", err)
		return "", mterrors.Wrap(err, "database connection failed")
	}

	// Guardrail: Check if the PostgreSQL instance is in standby mode
	var isInRecovery bool
	err := pm.db.QueryRowContext(ctx, "SELECT pg_is_in_recovery()").Scan(&isInRecovery)
	if err != nil {
		pm.logger.Error("Failed to check if instance is in recovery", "error", err)
		return "", mterrors.Wrap(err, "failed to check recovery status")
	}

	if isInRecovery {
		pm.logger.Error("PrimaryPosition called on standby instance", "service_id", pm.serviceID.String())
		return "", mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION,
			fmt.Sprintf("operation not allowed: the PostgreSQL instance is in standby mode (service_id: %s)", pm.serviceID.String()))
	}

	// Query PostgreSQL for the current LSN position
	// pg_current_wal_lsn() returns the current write-ahead log write location
	var lsn string
	err = pm.db.QueryRowContext(ctx, "SELECT pg_current_wal_lsn()::text").Scan(&lsn)
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

// SetTerm sets the consensus term information by forwarding to pgctld
func (pm *MultiPoolerManager) SetTerm(ctx context.Context, term *pgctldpb.ConsensusTerm) error {
	if err := pm.checkReady(); err != nil {
		return err
	}

	pm.logger.Info("SetTerm called", "current_term", term.GetCurrentTerm())

	// Create pgctld client
	conn, err := pm.createPgctldClient()
	if err != nil {
		pm.logger.Error("Failed to create pgctld client", "error", err)
		return mterrors.Wrap(err, "failed to create pgctld client")
	}
	defer conn.Close()

	// Forward request to pgctld
	client := pgctldpb.NewPgCtldClient(conn)
	_, err = client.SetTerm(ctx, &pgctldpb.SetTermRequest{
		Term: term,
	})
	if err != nil {
		pm.logger.Error("Failed to set consensus term in pgctld", "error", err)
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
func (pm *MultiPoolerManager) Start() {
	// Start loading multipooler record from topology asynchronously
	go pm.loadMultiPoolerFromTopo()
	// Start loading consensus term from pgctld asynchronously
	go pm.loadConsensusTermFromPgctld()

	servenv.OnRun(func() {
		pm.logger.Info("MultiPoolerManager started")
		// Additional manager-specific initialization can happen here

		// Register all gRPC services that have registered themselves
		pm.registerGRPCServices()
		pm.logger.Info("MultiPoolerManager gRPC services registered")
	})
}
