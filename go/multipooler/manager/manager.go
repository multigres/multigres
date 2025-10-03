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
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
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
	logger     *slog.Logger
	config     *Config
	db         *sql.DB
	topoClient topo.Store
	serviceID  *clustermetadatapb.ID

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
	pm.logger.Info("WaitForLSN called", "target_lsn", targetLsn)
	return fmt.Errorf("method WaitForLSN not implemented")
}

// SetReadOnly makes the PostgreSQL instance read-only
func (pm *MultiPoolerManager) SetReadOnly(ctx context.Context) error {
	pm.logger.Info("SetReadOnly called")
	return fmt.Errorf("method SetReadOnly not implemented")
}

// IsReadOnly checks if PostgreSQL instance is in read-only mode
func (pm *MultiPoolerManager) IsReadOnly(ctx context.Context) (bool, error) {
	pm.logger.Info("IsReadOnly called")
	return false, fmt.Errorf("method IsReadOnly not implemented")
}

// SetPrimaryConnInfo sets the primary connection info for a standby server
func (pm *MultiPoolerManager) SetPrimaryConnInfo(ctx context.Context, host string, port int32) error {
	pm.logger.Info("SetPrimaryConnInfo called", "host", host, "port", port)
	return fmt.Errorf("method SetPrimaryConnInfo not implemented")
}

// StartReplication starts WAL replay on standby (calls pg_wal_replay_resume)
func (pm *MultiPoolerManager) StartReplication(ctx context.Context) error {
	pm.logger.Info("StartReplication called")
	return fmt.Errorf("method StartReplication not implemented")
}

// StopReplication stops WAL replay on standby (calls pg_wal_replay_pause)
func (pm *MultiPoolerManager) StopReplication(ctx context.Context) error {
	pm.logger.Info("StopReplication called")
	return fmt.Errorf("method StopReplication not implemented")
}

// ReplicationStatus gets the current replication status of the standby
func (pm *MultiPoolerManager) ReplicationStatus(ctx context.Context) (map[string]interface{}, error) {
	pm.logger.Info("ReplicationStatus called")
	return nil, fmt.Errorf("method ReplicationStatus not implemented")
}

// ResetReplication resets the standby's connection to its primary
func (pm *MultiPoolerManager) ResetReplication(ctx context.Context) error {
	pm.logger.Info("ResetReplication called")
	return fmt.Errorf("method ResetReplication not implemented")
}

// ConfigureSynchronousReplication configures PostgreSQL synchronous replication settings
func (pm *MultiPoolerManager) ConfigureSynchronousReplication(ctx context.Context, synchronousCommit string) error {
	pm.logger.Info("ConfigureSynchronousReplication called", "synchronous_commit", synchronousCommit)
	return fmt.Errorf("method ConfigureSynchronousReplication not implemented")
}

// PrimaryStatus gets the status of the leader server
func (pm *MultiPoolerManager) PrimaryStatus(ctx context.Context) (map[string]interface{}, error) {
	pm.logger.Info("PrimaryStatus called")
	return nil, fmt.Errorf("method PrimaryStatus not implemented")
}

// PrimaryPosition gets the current LSN position of the leader
func (pm *MultiPoolerManager) PrimaryPosition(ctx context.Context) (string, error) {
	pm.logger.Info("PrimaryPosition called")

	// Ensure database connection
	if err := pm.connectDB(); err != nil {
		pm.logger.Error("Failed to connect to database", "error", err)
		return "", fmt.Errorf("database connection failed: %w", err)
	}

	// Guardrail: Check if the PostgreSQL instance is in standby mode
	var isInRecovery bool
	err := pm.db.QueryRowContext(ctx, "SELECT pg_is_in_recovery()").Scan(&isInRecovery)
	if err != nil {
		pm.logger.Error("Failed to check if instance is in recovery", "error", err)
		return "", fmt.Errorf("failed to check recovery status: %w", err)
	}

	if isInRecovery {
		pm.logger.Error("PrimaryPosition called on standby instance", "service_id", pm.serviceID.String())
		return "", fmt.Errorf("operation not allowed: the PostgreSQL instance is in standby mode (service_id: %s)", pm.serviceID.String())
	}

	// Query PostgreSQL for the current LSN position
	// pg_current_wal_lsn() returns the current write-ahead log write location
	var lsn string
	err = pm.db.QueryRowContext(ctx, "SELECT pg_current_wal_lsn()::text").Scan(&lsn)
	if err != nil {
		pm.logger.Error("Failed to query LSN", "error", err)
		return "", fmt.Errorf("failed to query LSN: %w", err)
	}

	return lsn, nil
}

// StopReplicationAndGetStatus stops PostgreSQL replication and returns the status
func (pm *MultiPoolerManager) StopReplicationAndGetStatus(ctx context.Context) (map[string]interface{}, error) {
	pm.logger.Info("StopReplicationAndGetStatus called")
	return nil, fmt.Errorf("method StopReplicationAndGetStatus not implemented")
}

// ChangeType changes the pooler type (LEADER/FOLLOWER)
func (pm *MultiPoolerManager) ChangeType(ctx context.Context, poolerType string) error {
	pm.logger.Info("ChangeType called", "pooler_type", poolerType)
	return fmt.Errorf("method ChangeType not implemented")
}

// GetFollowers gets the list of follower servers
func (pm *MultiPoolerManager) GetFollowers(ctx context.Context) ([]string, error) {
	pm.logger.Info("GetFollowers called")
	return nil, fmt.Errorf("method GetFollowers not implemented")
}

// Demote demotes the current leader server
func (pm *MultiPoolerManager) Demote(ctx context.Context) error {
	pm.logger.Info("Demote called")
	return fmt.Errorf("method Demote not implemented")
}

// UndoDemote undoes a demotion
func (pm *MultiPoolerManager) UndoDemote(ctx context.Context) error {
	pm.logger.Info("UndoDemote called")
	return fmt.Errorf("method UndoDemote not implemented")
}

// Promote promotes a follower to leader
func (pm *MultiPoolerManager) Promote(ctx context.Context) error {
	pm.logger.Info("Promote called")
	return fmt.Errorf("method Promote not implemented")
}

// Start initializes the MultiPoolerManager
// This method follows the Vitess pattern similar to TabletManager.Start() in tm_init.go
func (pm *MultiPoolerManager) Start() {
	// Start loading multipooler record from topology asynchronously
	go pm.loadMultiPoolerFromTopo()

	servenv.OnRun(func() {
		pm.logger.Info("MultiPoolerManager started")
		// Additional manager-specific initialization can happen here

		// Register all gRPC services that have registered themselves
		// This follows the Vitess pattern - grpcmanagerservice will append to RegisterPoolerManagerServices in init()
		pm.registerGRPCServices()
		pm.logger.Info("MultiPoolerManager gRPC services registered")
	})
}
