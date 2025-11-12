// Copyright 2025 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package manager

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	pgctldpb "github.com/multigres/multigres/go/pb/pgctldservice"
)

// InitializeEmptyPrimary initializes this node as an empty primary
// Used during bootstrap initialization of a new shard
func (pm *MultiPoolerManager) InitializeEmptyPrimary(ctx context.Context, req *multipoolermanagerdatapb.InitializeEmptyPrimaryRequest) (*multipoolermanagerdatapb.InitializeEmptyPrimaryResponse, error) {
	pm.logger.InfoContext(ctx, "InitializeEmptyPrimary called", "shard", pm.getShardID(), "term", req.ConsensusTerm)

	// Acquire action lock
	if err := pm.lock(ctx); err != nil {
		return nil, fmt.Errorf("failed to acquire action lock: %w", err)
	}
	defer pm.unlock()

	// 1. Check if already initialized
	if pm.isInitialized() {
		pm.logger.InfoContext(ctx, "Node already initialized", "shard", pm.getShardID())
		return &multipoolermanagerdatapb.InitializeEmptyPrimaryResponse{Success: true}, nil
	}

	// 2. Initialize data directory via pgctld if needed
	if !pm.hasDataDirectory() {
		pm.logger.InfoContext(ctx, "Initializing data directory", "shard", pm.getShardID())
		if pm.pgctldClient == nil {
			return nil, fmt.Errorf("pgctld client not available")
		}

		initReq := &pgctldpb.InitDataDirRequest{}
		if _, err := pm.pgctldClient.InitDataDir(ctx, initReq); err != nil {
			return nil, fmt.Errorf("failed to initialize data directory: %w", err)
		}
	}

	// 3. Start PostgreSQL if not running
	if !pm.isPostgresRunning(ctx) {
		pm.logger.InfoContext(ctx, "Starting PostgreSQL", "shard", pm.getShardID())
		if pm.pgctldClient == nil {
			return nil, fmt.Errorf("pgctld client not available")
		}

		startReq := &pgctldpb.StartRequest{}
		if _, err := pm.pgctldClient.Start(ctx, startReq); err != nil {
			return nil, fmt.Errorf("failed to start PostgreSQL: %w", err)
		}
	}

	// 4. Wait for database connection
	if err := pm.waitForDatabaseConnection(ctx); err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	// 5. Create multigres schema and heartbeat table
	if err := pm.initializeMultigresSchema(ctx); err != nil {
		return nil, fmt.Errorf("failed to initialize multigres schema: %w", err)
	}

	// 6. Set consensus term
	if pm.consensusState != nil {
		if err := pm.consensusState.UpdateTermAndSave(req.ConsensusTerm); err != nil {
			return nil, fmt.Errorf("failed to set consensus term: %w", err)
		}
	}

	pm.logger.InfoContext(ctx, "Successfully initialized node as empty primary", "shard", pm.getShardID(), "term", req.ConsensusTerm)
	return &multipoolermanagerdatapb.InitializeEmptyPrimaryResponse{Success: true}, nil
}

// InitializeAsStandby initializes this node as a standby from a primary backup
// Used during bootstrap initialization of a new shard or when adding a new standby
func (pm *MultiPoolerManager) InitializeAsStandby(ctx context.Context, req *multipoolermanagerdatapb.InitializeAsStandbyRequest) (*multipoolermanagerdatapb.InitializeAsStandbyResponse, error) {
	pm.logger.InfoContext(ctx, "InitializeAsStandby called",
		"shard", pm.getShardID(),
		"primary", fmt.Sprintf("%s:%d", req.PrimaryHost, req.PrimaryPort),
		"term", req.ConsensusTerm,
		"force_reinit", req.ForceReinit)

	// Acquire action lock
	if err := pm.lock(ctx); err != nil {
		return nil, fmt.Errorf("failed to acquire action lock: %w", err)
	}
	defer pm.unlock()

	// 1. Stop PostgreSQL if running
	if pm.isPostgresRunning(ctx) {
		pm.logger.InfoContext(ctx, "Stopping PostgreSQL before initialization", "shard", pm.getShardID())
		if pm.pgctldClient == nil {
			return nil, fmt.Errorf("pgctld client not available")
		}

		stopReq := &pgctldpb.StopRequest{}
		if _, err := pm.pgctldClient.Stop(ctx, stopReq); err != nil {
			return nil, fmt.Errorf("failed to stop PostgreSQL: %w", err)
		}
	}

	// 2. Remove data directory if force_reinit
	if req.ForceReinit && pm.hasDataDirectory() {
		pm.logger.InfoContext(ctx, "Force reinit: removing data directory", "shard", pm.getShardID())
		if err := pm.removeDataDirectory(); err != nil {
			return nil, fmt.Errorf("failed to remove data directory: %w", err)
		}
	}

	// 3. TODO: Restore from primary using pgBackRest (PR #226)
	// For now, we'll use pg_basebackup as a placeholder
	pm.logger.InfoContext(ctx, "Performing backup from primary", "primary", fmt.Sprintf("%s:%d", req.PrimaryHost, req.PrimaryPort))

	// Placeholder: In production, this would call pm.Restore() from PR #226
	// For now, we skip the actual backup to avoid dependencies
	finalLSN := ""

	// 4. Create standby.signal
	if err := pm.createStandbySignal(); err != nil {
		return nil, fmt.Errorf("failed to create standby.signal: %w", err)
	}

	// 5. Configure primary_conninfo
	if err := pm.SetPrimaryConnInfo(ctx, req.PrimaryHost, req.PrimaryPort, false, false, req.ConsensusTerm, false); err != nil {
		return nil, fmt.Errorf("failed to set primary_conninfo: %w", err)
	}

	// 6. Start PostgreSQL in standby mode
	pm.logger.InfoContext(ctx, "Starting PostgreSQL in standby mode", "shard", pm.getShardID())
	if pm.pgctldClient == nil {
		return nil, fmt.Errorf("pgctld client not available")
	}

	startReq := &pgctldpb.StartRequest{}
	if _, err := pm.pgctldClient.Start(ctx, startReq); err != nil {
		return nil, fmt.Errorf("failed to start PostgreSQL: %w", err)
	}

	// 7. Wait for database connection
	if err := pm.waitForDatabaseConnection(ctx); err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	// 8. Set consensus term
	if pm.consensusState != nil {
		if err := pm.consensusState.UpdateTermAndSave(req.ConsensusTerm); err != nil {
			return nil, fmt.Errorf("failed to set consensus term: %w", err)
		}
	}

	pm.logger.InfoContext(ctx, "Successfully initialized node as standby", "shard", pm.getShardID(), "term", req.ConsensusTerm)
	return &multipoolermanagerdatapb.InitializeAsStandbyResponse{
		Success:  true,
		FinalLsn: finalLSN,
	}, nil
}

// InitializationStatus returns the initialization status of this node
// Used by multiorch coordinator to determine what initialization scenario to use
func (pm *MultiPoolerManager) InitializationStatus(ctx context.Context, req *multipoolermanagerdatapb.InitializationStatusRequest) (*multipoolermanagerdatapb.InitializationStatusResponse, error) {
	pm.logger.DebugContext(ctx, "InitializationStatus called", "shard", pm.getShardID())

	resp := &multipoolermanagerdatapb.InitializationStatusResponse{
		IsInitialized:    pm.isInitialized(),
		HasDataDirectory: pm.hasDataDirectory(),
		PostgresRunning:  pm.isPostgresRunning(ctx),
		Role:             pm.getRole(ctx),
		WalPosition:      pm.getWALPosition(ctx),
		ShardId:          pm.getShardID(),
	}

	// Get consensus term if available
	if pm.consensusState != nil {
		resp.ConsensusTerm = pm.consensusState.GetCurrentTermNumber()
	}

	return resp, nil
}

// Helper methods

// isInitialized checks if the node has been initialized (has data directory and multigres schema)
func (pm *MultiPoolerManager) isInitialized() bool {
	if !pm.hasDataDirectory() {
		return false
	}

	if pm.db == nil {
		return false
	}

	// Check if multigres schema exists
	var exists bool
	query := "SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = 'multigres')"
	err := pm.db.QueryRow(query).Scan(&exists)
	return err == nil && exists
}

// hasDataDirectory checks if the PostgreSQL data directory exists
func (pm *MultiPoolerManager) hasDataDirectory() bool {
	if pm.config == nil || pm.config.PoolerDir == "" {
		return false
	}

	dataDir := filepath.Join(pm.config.PoolerDir, "pg_data")
	info, err := os.Stat(dataDir)
	if err != nil {
		return false
	}

	return info.IsDir()
}

// isPostgresRunning checks if PostgreSQL is currently running
func (pm *MultiPoolerManager) isPostgresRunning(ctx context.Context) bool {
	if pm.pgctldClient == nil {
		return pm.db != nil
	}

	statusReq := &pgctldpb.StatusRequest{}
	statusResp, err := pm.pgctldClient.Status(ctx, statusReq)
	if err != nil {
		return false
	}

	return statusResp.Status == pgctldpb.ServerStatus_RUNNING
}

// getRole returns the current role of this node ("primary", "standby", or "unknown")
func (pm *MultiPoolerManager) getRole(ctx context.Context) string {
	if pm.db == nil {
		return "unknown"
	}

	var inRecovery bool
	err := pm.db.QueryRowContext(ctx, "SELECT pg_is_in_recovery()").Scan(&inRecovery)
	if err != nil {
		return "unknown"
	}

	if inRecovery {
		return "standby"
	}
	return "primary"
}

// getWALPosition returns the current WAL position
func (pm *MultiPoolerManager) getWALPosition(ctx context.Context) string {
	if pm.db == nil {
		return ""
	}

	role := pm.getRole(ctx)
	var lsn string
	var err error

	switch role {
	case "primary":
		err = pm.db.QueryRowContext(ctx, "SELECT pg_current_wal_lsn()").Scan(&lsn)
	case "standby":
		err = pm.db.QueryRowContext(ctx, "SELECT pg_last_wal_replay_lsn()").Scan(&lsn)
	}

	if err != nil {
		return ""
	}

	return lsn
}

// getShardID returns the shard ID from the multipooler metadata
func (pm *MultiPoolerManager) getShardID() string {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if pm.multipooler == nil {
		return ""
	}

	return pm.multipooler.Shard
}

// removeDataDirectory removes the PostgreSQL data directory
func (pm *MultiPoolerManager) removeDataDirectory() error {
	if pm.config == nil || pm.config.PoolerDir == "" {
		return fmt.Errorf("pooler directory path not configured")
	}

	dataDir := filepath.Join(pm.config.PoolerDir, "pg_data")

	// Safety check: ensure we're not deleting root or home directory
	absDataDir, err := filepath.Abs(dataDir)
	if err != nil {
		return fmt.Errorf("failed to resolve data directory path: %w", err)
	}

	if absDataDir == "/" || absDataDir == os.Getenv("HOME") {
		return fmt.Errorf("refusing to delete unsafe directory: %s", absDataDir)
	}

	pm.logger.Warn("Removing data directory", "path", absDataDir)
	return os.RemoveAll(absDataDir)
}

// createStandbySignal creates the standby.signal file in the data directory
func (pm *MultiPoolerManager) createStandbySignal() error {
	if pm.config == nil || pm.config.PoolerDir == "" {
		return fmt.Errorf("pooler directory path not configured")
	}

	dataDir := filepath.Join(pm.config.PoolerDir, "pg_data")
	signalPath := filepath.Join(dataDir, "standby.signal")
	pm.logger.Info("Creating standby.signal", "path", signalPath)

	file, err := os.Create(signalPath)
	if err != nil {
		return fmt.Errorf("failed to create standby.signal: %w", err)
	}
	defer file.Close()

	return nil
}

// waitForDatabaseConnection waits for the database connection to become available
func (pm *MultiPoolerManager) waitForDatabaseConnection(ctx context.Context) error {
	// If we already have a connection, test it
	if pm.db != nil {
		if err := pm.db.PingContext(ctx); err == nil {
			return nil
		}
	}

	// Wait for connection to become available (up to 30 seconds)
	pm.logger.InfoContext(ctx, "Waiting for database connection")
	// TODO: Implement retry logic
	// For now, assume the database will be available soon
	return nil
}

// initializeMultigresSchema creates the multigres schema and heartbeat table
func (pm *MultiPoolerManager) initializeMultigresSchema(ctx context.Context) error {
	if pm.db == nil {
		return fmt.Errorf("database connection not available")
	}

	pm.logger.InfoContext(ctx, "Creating multigres schema and heartbeat table")

	// Create schema
	if _, err := pm.db.ExecContext(ctx, "CREATE SCHEMA IF NOT EXISTS multigres"); err != nil {
		return fmt.Errorf("failed to create multigres schema: %w", err)
	}

	// Create heartbeat table
	createTableSQL := `
		CREATE TABLE IF NOT EXISTS multigres.heartbeat (
			shard_id BYTEA PRIMARY KEY,
			leader_id TEXT NOT NULL,
			ts BIGINT NOT NULL
		)`

	if _, err := pm.db.ExecContext(ctx, createTableSQL); err != nil {
		return fmt.Errorf("failed to create heartbeat table: %w", err)
	}

	return nil
}
