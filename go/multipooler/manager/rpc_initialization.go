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

	"github.com/multigres/multigres/go/mterrors"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	pgctldpb "github.com/multigres/multigres/go/pb/pgctldservice"
)

// InitializeEmptyPrimary initializes this pooler as an empty primary
// Used during bootstrap initialization of a new shard
func (pm *MultiPoolerManager) InitializeEmptyPrimary(ctx context.Context, req *multipoolermanagerdatapb.InitializeEmptyPrimaryRequest) (*multipoolermanagerdatapb.InitializeEmptyPrimaryResponse, error) {
	pm.logger.InfoContext(ctx, "InitializeEmptyPrimary called", "shard", pm.getShardID(), "term", req.ConsensusTerm)

	// Acquire action lock
	var err error
	ctx, err = pm.actionLock.Acquire(ctx, "InitializeEmptyPrimary")
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to acquire action lock")
	}
	defer pm.actionLock.Release(ctx)

	// 1. Validate consensus term must be 1 for new primary
	if req.ConsensusTerm != 1 {
		return nil, mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT, "consensus term must be 1 for new primary initialization, got %d", req.ConsensusTerm)
	}

	// 2. Check if already initialized
	if pm.isInitialized() {
		pm.logger.InfoContext(ctx, "Pooler already initialized", "shard", pm.getShardID())
		return &multipoolermanagerdatapb.InitializeEmptyPrimaryResponse{Success: true}, nil
	}

	// 3. Initialize data directory via pgctld if needed
	if !pm.hasDataDirectory() {
		pm.logger.InfoContext(ctx, "Initializing data directory", "shard", pm.getShardID())
		if pm.pgctldClient == nil {
			return nil, mterrors.New(mtrpcpb.Code_UNAVAILABLE, "pgctld client not available")
		}

		initReq := &pgctldpb.InitDataDirRequest{}
		if _, err := pm.pgctldClient.InitDataDir(ctx, initReq); err != nil {
			return nil, mterrors.Wrap(err, "failed to initialize data directory")
		}
	}

	// 4. Start PostgreSQL if not running
	if !pm.isPostgresRunning(ctx) {
		pm.logger.InfoContext(ctx, "Starting PostgreSQL", "shard", pm.getShardID())
		if pm.pgctldClient == nil {
			return nil, mterrors.New(mtrpcpb.Code_UNAVAILABLE, "pgctld client not available")
		}

		startReq := &pgctldpb.StartRequest{}
		if _, err := pm.pgctldClient.Start(ctx, startReq); err != nil {
			return nil, mterrors.Wrap(err, "failed to start PostgreSQL")
		}
	}

	// 5. Wait for database connection
	if err := pm.waitForDatabaseConnection(ctx); err != nil {
		return nil, mterrors.Wrap(err, "failed to connect to database")
	}

	// 6. Create multigres schema and heartbeat table
	if err := pm.initializeMultigresSchema(ctx); err != nil {
		return nil, mterrors.Wrap(err, "failed to initialize multigres schema")
	}

	// 7. Set consensus term
	if pm.consensusState != nil {
		if err := pm.consensusState.UpdateTermAndSave(ctx, req.ConsensusTerm); err != nil {
			return nil, mterrors.Wrap(err, "failed to set consensus term")
		}
	}

	pm.logger.InfoContext(ctx, "Successfully initialized pooler as empty primary", "shard", pm.getShardID(), "term", req.ConsensusTerm)
	return &multipoolermanagerdatapb.InitializeEmptyPrimaryResponse{Success: true}, nil
}

// InitializeAsStandby initializes this pooler as a standby from a primary backup
// Used during bootstrap initialization of a new shard or when adding a new standby
func (pm *MultiPoolerManager) InitializeAsStandby(ctx context.Context, req *multipoolermanagerdatapb.InitializeAsStandbyRequest) (*multipoolermanagerdatapb.InitializeAsStandbyResponse, error) {
	pm.logger.InfoContext(ctx, "InitializeAsStandby called",
		"shard", pm.getShardID(),
		"primary", fmt.Sprintf("%s:%d", req.PrimaryHost, req.PrimaryPort),
		"term", req.ConsensusTerm,
		"force", req.Force)

	// Acquire action lock
	var err error
	ctx, err = pm.actionLock.Acquire(ctx, "InitializeAsStandby")
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to acquire action lock")
	}
	defer pm.actionLock.Release(ctx)

	// 1. Check for existing data directory
	if pm.hasDataDirectory() {
		if !req.Force {
			return nil, mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION, "data directory already exists, use force=true to reinitialize")
		}
		// Remove data directory if force
		pm.logger.InfoContext(ctx, "Force reinit: removing data directory", "shard", pm.getShardID())
		if err := pm.removeDataDirectory(); err != nil {
			return nil, mterrors.Wrap(err, "failed to remove data directory")
		}
	}

	// 2. TODO: Restore from primary using pgBackRest (PR #226)
	// For now, we'll use pg_basebackup as a placeholder
	pm.logger.InfoContext(ctx, "Performing backup from primary", "primary", fmt.Sprintf("%s:%d", req.PrimaryHost, req.PrimaryPort))

	// Placeholder: In production, this would call pm.Restore() from PR #226
	// For now, we skip the actual backup to avoid dependencies
	finalLSN := ""

	// 3. Configure primary_conninfo
	if err := pm.SetPrimaryConnInfo(ctx, req.PrimaryHost, req.PrimaryPort, false, false, req.ConsensusTerm, false); err != nil {
		return nil, mterrors.Wrap(err, "failed to set primary_conninfo")
	}

	// 4. Restart PostgreSQL as standby (creates standby.signal and starts)
	pm.logger.InfoContext(ctx, "Restarting PostgreSQL as standby", "shard", pm.getShardID())
	if pm.pgctldClient == nil {
		return nil, mterrors.New(mtrpcpb.Code_UNAVAILABLE, "pgctld client not available")
	}

	restartReq := &pgctldpb.RestartRequest{
		AsStandby: true,
		Mode:      "fast",
	}
	if _, err := pm.pgctldClient.Restart(ctx, restartReq); err != nil {
		return nil, mterrors.Wrap(err, "failed to restart PostgreSQL as standby")
	}

	// 5. Wait for database connection
	if err := pm.waitForDatabaseConnection(ctx); err != nil {
		return nil, mterrors.Wrap(err, "failed to connect to database")
	}

	// 6. Set consensus term
	if pm.consensusState != nil {
		if err := pm.consensusState.UpdateTermAndSave(ctx, req.ConsensusTerm); err != nil {
			return nil, mterrors.Wrap(err, "failed to set consensus term")
		}
	}

	pm.logger.InfoContext(ctx, "Successfully initialized pooler as standby", "shard", pm.getShardID(), "term", req.ConsensusTerm)
	return &multipoolermanagerdatapb.InitializeAsStandbyResponse{
		Success:  true,
		FinalLsn: finalLSN,
	}, nil
}

// InitializationStatus returns the initialization status of this pooler
// Used by multiorch coordinator to determine what initialization scenario to use
func (pm *MultiPoolerManager) InitializationStatus(ctx context.Context, req *multipoolermanagerdatapb.InitializationStatusRequest) (*multipoolermanagerdatapb.InitializationStatusResponse, error) {
	pm.logger.DebugContext(ctx, "InitializationStatus called", "shard", pm.getShardID())

	// Get WAL position (ignore errors, just return empty string)
	walPosition, _ := pm.getWALPosition(ctx)

	resp := &multipoolermanagerdatapb.InitializationStatusResponse{
		IsInitialized:    pm.isInitialized(),
		HasDataDirectory: pm.hasDataDirectory(),
		PostgresRunning:  pm.isPostgresRunning(ctx),
		Role:             pm.getRole(ctx),
		WalPosition:      walPosition,
		ShardId:          pm.getShardID(),
	}

	// Get consensus term if available
	if pm.consensusState != nil {
		term, err := pm.consensusState.GetCurrentTermNumber(ctx)
		if err == nil {
			resp.ConsensusTerm = term
		}
	}

	return resp, nil
}

// Helper methods

// isInitialized checks if the pooler has been initialized (has data directory and multigres schema)
func (pm *MultiPoolerManager) isInitialized() bool {
	if !pm.hasDataDirectory() {
		return false
	}

	if pm.db == nil {
		return false
	}

	// Check if multigres schema exists
	exists, err := pm.querySchemaExists(context.Background())
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

// getRole returns the current role of this pooler ("primary", "standby", or "unknown")
func (pm *MultiPoolerManager) getRole(ctx context.Context) string {
	if pm.db == nil {
		return "unknown"
	}

	isPrimary, err := pm.IsPrimary(ctx)
	if err != nil {
		return "unknown"
	}

	if isPrimary {
		return "primary"
	}
	return "standby"
}

// getWALPosition returns the current WAL position and any error encountered
func (pm *MultiPoolerManager) getWALPosition(ctx context.Context) (string, error) {
	if pm.db == nil {
		return "", fmt.Errorf("database connection not available")
	}

	isPrimary, err := pm.IsPrimary(ctx)
	if err != nil {
		return "", err
	}

	if isPrimary {
		return pm.getPrimaryLSN(ctx)
	}
	return pm.getStandbyReplayLSN(ctx)
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
