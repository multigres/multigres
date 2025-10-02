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

	"github.com/multigres/multigres/go/clustermetadata/topo"
	"github.com/multigres/multigres/go/servenv"
)

// MultiPoolerManager manages the pooler lifecycle and PostgreSQL operations
type MultiPoolerManager struct {
	logger     *slog.Logger
	config     *Config
	db         *sql.DB
	topoClient topo.Store
	serviceID  string
}

// NewMultiPoolerManager creates a new MultiPoolerManager instance
func NewMultiPoolerManager(logger *slog.Logger, config *Config) *MultiPoolerManager {
	return &MultiPoolerManager{
		logger:     logger,
		config:     config,
		topoClient: config.TopoClient,
		serviceID:  config.ServiceID,
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

// Close closes the database connection
func (pm *MultiPoolerManager) Close() error {
	if pm.db != nil {
		return pm.db.Close()
	}
	return nil
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
		pm.logger.Error("PrimaryPosition called on standby instance", "service_id", pm.serviceID)
		return "", fmt.Errorf("operation not allowed: the PostgreSQL instance is in standby mode (service_id: %s)", pm.serviceID)
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
	servenv.OnRun(func() {
		pm.logger.Info("MultiPoolerManager started")
		// Additional manager-specific initialization can happen here

		// Register all gRPC services that have registered themselves
		// This follows the Vitess pattern - grpcmanagerservice will append to RegisterPoolerManagerServices in init()
		pm.registerGRPCServices()
		pm.logger.Info("MultiPoolerManager gRPC services registered")
	})
}
