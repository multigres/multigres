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

	"github.com/multigres/multigres/go/clustermetadata/topo"
	"github.com/multigres/multigres/go/mterrors"
	"github.com/multigres/multigres/go/multipooler/heartbeat"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolermanagerdata "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/servenv"
	"github.com/multigres/multigres/go/tools/timertools"
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

	// Multipooler record from topology
	mu          sync.RWMutex
	multipooler *topo.MultiPoolerInfo
	state       ManagerState
	stateError  error
	ctx         context.Context
	cancel      context.CancelFunc
	loadTimeout time.Duration
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

	// Create the multigres sidecar schema if it doesn't exist
	pm.logger.Info("MultiPoolerManager: Creating sidecar database")
	if err := CreateSidecarSchema(pm.db); err != nil {
		return fmt.Errorf("failed to create sidecar schema: %w", err)
	}

	// Start heartbeat tracking if not already started
	if pm.replTracker == nil {
		pm.logger.Info("MultiPoolerManager: Starting database heartbeat")
		ctx := context.Background()
		// TODO: populate shard ID
		shardID := []byte("0") // default shard ID

		// Use the multipooler name from serviceID as the pooler ID
		poolerID := pm.serviceID.Name

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
	pm.replTracker = heartbeat.NewReplTracker(pm.db, pm.logger, shardID, poolerID)

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

// loadMultiPoolerFromTopo loads the multipooler record from topology asynchronously
func (pm *MultiPoolerManager) loadMultiPoolerFromTopo() {
	// Validate ServiceID is not nil
	if pm.serviceID == nil {
		pm.mu.Lock()
		pm.state = ManagerStateError
		pm.stateError = fmt.Errorf("ServiceID cannot be nil")
		pm.mu.Unlock()
		pm.logger.Error("Manager state changed", "state", ManagerStateError, "error", pm.stateError.Error())
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
			pm.state = ManagerStateReady
			pm.mu.Unlock()

			pm.logger.Info("Manager state changed", "state", ManagerStateReady, "service_id", pm.serviceID.String(), "database", mp.Database)
			return

		case <-timeoutCtx.Done():
			pm.mu.Lock()
			pm.state = ManagerStateError
			pm.stateError = fmt.Errorf("timeout waiting for multipooler record to be available in topology after %v", pm.loadTimeout)
			pm.mu.Unlock()
			pm.logger.Error("Manager state changed", "state", ManagerStateError, "service_id", pm.serviceID.String(), "error", pm.stateError.Error())
			return

		case <-pm.ctx.Done():
			// Parent context cancelled - treat as error
			pm.mu.Lock()
			pm.state = ManagerStateError
			pm.stateError = fmt.Errorf("manager context cancelled while loading multipooler record")
			pm.mu.Unlock()
			pm.logger.Error("Manager state changed", "state", ManagerStateError, "service_id", pm.serviceID.String(), "error", pm.stateError.Error())
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

// SetPrimaryConnInfo sets the primary connection info for a standby server
func (pm *MultiPoolerManager) SetPrimaryConnInfo(ctx context.Context, host string, port int32) error {
	if err := pm.checkReady(); err != nil {
		return err
	}
	pm.logger.Info("SetPrimaryConnInfo called", "host", host, "port", port)
	return mterrors.New(mtrpcpb.Code_UNIMPLEMENTED, "method SetPrimaryConnInfo not implemented")
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

// Start initializes the MultiPoolerManager
func (pm *MultiPoolerManager) Start() {
	// Start loading multipooler record from topology asynchronously
	go pm.loadMultiPoolerFromTopo()

	servenv.OnRun(func() {
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
