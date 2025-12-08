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
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/multigres/multigres/go/common/mterrors"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	pgctldpb "github.com/multigres/multigres/go/pb/pgctldservice"
	"github.com/multigres/multigres/go/tools/retry"
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

	// Validate consensus term must be 1 for new primary
	if req.ConsensusTerm != 1 {
		return nil, mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT, "consensus term must be 1 for new primary initialization, got %d", req.ConsensusTerm)
	}

	// Check if already initialized
	if pm.isInitialized(ctx) {
		pm.logger.InfoContext(ctx, "Pooler already initialized", "shard", pm.getShardID())
		// Note: backup_id will be empty for idempotent case since we didn't create a new backup
		return &multipoolermanagerdatapb.InitializeEmptyPrimaryResponse{Success: true}, nil
	}

	// Initialize data directory via pgctld if needed
	if !pm.hasDataDirectory() {
		pm.logger.InfoContext(ctx, "Initializing data directory", "shard", pm.getShardID())
		if pm.pgctldClient == nil {
			return nil, mterrors.New(mtrpcpb.Code_UNAVAILABLE, "pgctld client not available")
		}

		initReq := &pgctldpb.InitDataDirRequest{}
		if _, err := pm.pgctldClient.InitDataDir(ctx, initReq); err != nil {
			return nil, mterrors.Wrap(err, "failed to initialize data directory")
		}

		// Configure archive_mode in postgresql.auto.conf BEFORE starting PostgreSQL
		// This must be done after InitDataDir creates pg_data but before Start
		pm.logger.InfoContext(ctx, "Configuring archive_mode for pgbackrest", "shard", pm.getShardID())
		if err := pm.configureArchiveMode(ctx); err != nil {
			return nil, mterrors.Wrap(err, "failed to configure archive mode")
		}
	}

	// Start PostgreSQL if not running
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

	// Wait for database connection
	if err := pm.waitForDatabaseConnection(ctx); err != nil {
		return nil, mterrors.Wrap(err, "failed to connect to database")
	}

	// Create multigres schema and tables (heartbeat, durability_policy, tablegroup, table, shard)
	if err := pm.createSidecarSchema(ctx); err != nil {
		return nil, mterrors.Wrap(err, "failed to initialize multigres schema")
	}

	// Insert initial multischema data (tablegroup and shard records)
	if err := pm.initializeMultischemaData(ctx); err != nil {
		return nil, mterrors.Wrap(err, "failed to initialize multischema data")
	}

	// Set consensus term
	if pm.consensusState != nil {
		if err := pm.consensusState.UpdateTermAndSave(ctx, req.ConsensusTerm); err != nil {
			return nil, mterrors.Wrap(err, "failed to set consensus term")
		}
	}

	// Initialize pgbackrest stanza (must be done after PostgreSQL is running)
	pm.logger.InfoContext(ctx, "Initializing pgbackrest stanza", "shard", pm.getShardID())
	if err := pm.initializePgBackRestStanza(ctx); err != nil {
		return nil, mterrors.Wrap(err, "failed to initialize pgbackrest stanza")
	}

	// Create initial backup for standby initialization
	pm.logger.InfoContext(ctx, "Creating initial backup for standby initialization", "shard", pm.getShardID())
	backupID, err := pm.backupLocked(ctx, true, "full")
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to create initial backup")
	}
	pm.logger.InfoContext(ctx, "Initial backup created", "backup_id", backupID)

	// Write initialization marker to indicate full initialization completed.
	// This marker is checked by isInitialized() when the database connection is not available.
	if err := pm.writeInitializationMarker(); err != nil {
		return nil, mterrors.Wrap(err, "failed to write initialization marker")
	}

	pm.logger.InfoContext(ctx, "Successfully initialized pooler as empty primary", "shard", pm.getShardID(), "term", req.ConsensusTerm)
	return &multipoolermanagerdatapb.InitializeEmptyPrimaryResponse{
		Success:  true,
		BackupId: backupID,
	}, nil
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

	// 2. Restore from the specified backup (or latest if not specified)
	if req.BackupId != "" {
		pm.logger.InfoContext(ctx, "Restoring from specified backup", "backup_id", req.BackupId, "primary", fmt.Sprintf("%s:%d", req.PrimaryHost, req.PrimaryPort))
	} else {
		pm.logger.InfoContext(ctx, "Restoring from latest backup on primary", "primary", fmt.Sprintf("%s:%d", req.PrimaryHost, req.PrimaryPort))
	}

	// Restore from backup (empty string means latest)
	// restoreFromBackupLocked will check that this is a standby and start PostgreSQL in standby mode
	err = pm.restoreFromBackupLocked(ctx, req.BackupId)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to restore from backup")
	}

	if req.BackupId != "" {
		pm.logger.InfoContext(ctx, "Successfully restored from specified backup", "backup_id", req.BackupId)
	} else {
		pm.logger.InfoContext(ctx, "Successfully restored from latest backup")
	}

	// Note: RestoreFromBackup already restarted PostgreSQL as standby, so we skip
	// the explicit restart here. The standby.signal is already in place.

	// Extract final LSN from backup metadata
	backups, err := pm.getBackupsLocked(ctx, 1) // Get latest backup
	if err != nil {
		pm.logger.WarnContext(ctx, "Failed to get backup metadata for LSN", "error", err)
		// Non-fatal: we can continue without LSN
	}

	finalLSN := ""
	if len(backups) > 0 && backups[0].FinalLsn != "" {
		finalLSN = backups[0].FinalLsn
		pm.logger.InfoContext(ctx, "Backup LSN captured", "backup_id", backups[0].BackupId, "final_lsn", finalLSN)
	} else {
		pm.logger.WarnContext(ctx, "No LSN available from backup metadata")
	}

	// 3. Wait for database connection
	if err := pm.waitForDatabaseConnection(ctx); err != nil {
		return nil, mterrors.Wrap(err, "failed to connect to database")
	}

	// 4. Configure primary_conninfo now that PostgreSQL is running
	// Use the locked version since we're already holding the action lock
	pm.logger.InfoContext(ctx, "Configuring primary connection info", "primary_host", req.PrimaryHost, "primary_port", req.PrimaryPort)
	if err := pm.setPrimaryConnInfoLocked(ctx, req.PrimaryHost, req.PrimaryPort, false, true); err != nil {
		return nil, mterrors.Wrap(err, "failed to set primary_conninfo")
	}

	// 5. Set consensus term
	if pm.consensusState != nil {
		if err := pm.consensusState.UpdateTermAndSave(ctx, req.ConsensusTerm); err != nil {
			return nil, mterrors.Wrap(err, "failed to set consensus term")
		}
	}

	// Write initialization marker to indicate full initialization completed.
	// This marker is checked by isInitialized() when the database connection is not available.
	if err := pm.writeInitializationMarker(); err != nil {
		return nil, mterrors.Wrap(err, "failed to write initialization marker")
	}

	pm.logger.InfoContext(ctx, "Successfully initialized pooler as standby", "shard", pm.getShardID(), "term", req.ConsensusTerm)
	return &multipoolermanagerdatapb.InitializeAsStandbyResponse{
		Success:  true,
		FinalLsn: finalLSN,
	}, nil
}

// Helper methods

// multigresInitMarker is the filename for the initialization marker.
// This file is created after full initialization completes (schema created, backup done).
const multigresInitMarker = "MULTIGRES_INITIALIZED"

// isInitialized checks if the pooler has been initialized (has data directory and multigres schema)
// This should return true even when postgres is not running, as long as the node was previously initialized.
func (pm *MultiPoolerManager) isInitialized(ctx context.Context) bool {
	if !pm.hasDataDirectory() {
		return false
	}

	// If database is connected, check if multigres schema exists
	if pm.db != nil {
		exists, err := pm.querySchemaExists(ctx)
		return err == nil && exists
	}

	// If database is not connected (e.g., postgres is down), check for the initialization marker.
	// This marker is created after full initialization completes (schema created, backup done).
	// It's more reliable than checking for PG_VERSION/global because those exist after initdb
	// but before the full initialization process completes.
	dataDir := filepath.Join(pm.config.PoolerDir, "pg_data")
	markerFile := filepath.Join(dataDir, multigresInitMarker)
	if _, err := os.Stat(markerFile); err != nil {
		return false // Marker doesn't exist, not fully initialized
	}

	return true
}

// writeInitializationMarker creates the initialization marker file to indicate
// that full initialization, including restore from backup (for standbys) has
// completed. This is called at the end of primary and standby initialization.
//
// The marker file is needed to determine whether a replica pooler is
// initialized, because replica initialization is not done until the restore
// from backup completes. There is no other persistent way to determine this.
func (pm *MultiPoolerManager) writeInitializationMarker() error {
	dataDir := filepath.Join(pm.config.PoolerDir, "pg_data")
	markerFile := filepath.Join(dataDir, multigresInitMarker)
	return os.WriteFile(markerFile, []byte("initialized\n"), 0o644)
}

// hasDataDirectory checks if the PostgreSQL data directory exists
func (pm *MultiPoolerManager) hasDataDirectory() bool {
	if pm.config == nil || pm.config.PoolerDir == "" {
		return false
	}

	// Check if PG_VERSION file exists to confirm the data directory is properly initialized.
	// This prevents treating an empty directory (e.g., left behind by a failed initdb) as initialized.
	dataDir := filepath.Join(pm.config.PoolerDir, "pg_data")
	pgVersionFile := filepath.Join(dataDir, "PG_VERSION")
	_, err := os.Stat(pgVersionFile)
	return err == nil
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

	isPrimary, err := pm.isPrimary(ctx)
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

	isPrimary, err := pm.isPrimary(ctx)
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
			// Start heartbeat tracker if not already running
			if pm.replTracker == nil {
				shardID := []byte("0") // default shard ID
				poolerID := pm.serviceID.Name
				if err := pm.startHeartbeat(ctx, shardID, poolerID); err != nil {
					pm.logger.WarnContext(ctx, "Failed to start heartbeat for existing DB connection", "error", err)
				}
			}
			return nil
		}
		// Close stale connection
		pm.db.Close()
		pm.db = nil
	}

	// Wait for connection to become available with retry logic
	pm.logger.InfoContext(ctx, "Waiting for database connection")

	// Use exponential backoff starting at 500ms, up to 30s max backoff
	r := retry.New(500*time.Millisecond, 30*time.Second)
	var lastErr error
	firstAttempt := true

	for attempt, err := range r.Attempts(ctx) {
		// Check if context was cancelled or exceeded deadline
		if err != nil {
			if lastErr != nil {
				return mterrors.Wrap(lastErr, fmt.Sprintf("failed to connect to database after %d attempts: %v", attempt, err))
			}
			return mterrors.Wrap(err, fmt.Sprintf("context error while waiting for database connection after %d attempts", attempt))
		}

		// Try to open the connection
		if err := pm.connectDB(); err == nil {
			pm.logger.InfoContext(ctx, "Database connection established successfully", "attempts", attempt)

			// Start heartbeat tracker if not already running
			if pm.replTracker == nil {
				shardID := []byte("0") // default shard ID
				poolerID := pm.serviceID.Name
				if err := pm.startHeartbeat(ctx, shardID, poolerID); err != nil {
					pm.logger.WarnContext(ctx, "Failed to start heartbeat after DB connection", "error", err)
					// Don't fail - heartbeat is not critical for initialization
				}
			}

			// Also open the query service controller (executor) now that DB is connected
			if err := pm.qsc.Open(); err != nil {
				pm.logger.WarnContext(ctx, "Failed to open query service controller after DB connection", "error", err)
			}
			return nil
		} else {
			lastErr = err
			if firstAttempt {
				pm.logger.InfoContext(ctx, "PostgreSQL not ready yet, will retry with exponential backoff", "error", err)
				firstAttempt = false
			}
		}
	}

	// This should not be reached due to the context check in the loop, but just in case
	return mterrors.Wrap(lastErr, "failed to connect to database after retries")
}

// configureArchiveMode configures archive_mode in postgresql.auto.conf for pgbackrest
// This must be called after InitDataDir but BEFORE starting PostgreSQL
func (pm *MultiPoolerManager) configureArchiveMode(ctx context.Context) error {
	configPath := pm.getBackupConfigPath()
	stanzaName := pm.getBackupStanza()

	// Validate required configuration
	if configPath == "" {
		return mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "backup config path not configured")
	}
	if stanzaName == "" {
		return mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "backup stanza name not configured")
	}

	// Check if pgbackrest config file exists before configuring archive mode
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		return mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION,
			fmt.Sprintf("pgbackrest config file not found at %s - cannot configure archive mode", configPath))
	}

	pgDataDir := filepath.Join(pm.config.PoolerDir, "pg_data")
	autoConfPath := filepath.Join(pgDataDir, "postgresql.auto.conf")

	// Check if archive_mode is already configured to avoid duplicates
	if _, err := os.Stat(autoConfPath); err == nil {
		content, err := os.ReadFile(autoConfPath)
		if err == nil && bytes.Contains(content, []byte("archive_mode")) {
			pm.logger.InfoContext(ctx, "archive_mode already configured, skipping", "auto_conf", autoConfPath)
			return nil
		}
	}

	// Configure archive_mode in postgresql.auto.conf
	// Following the pattern from test/endtoend/multipooler/setup_test.go:479-498
	archiveConfig := fmt.Sprintf(`
# Archive mode for pgbackrest backups
archive_mode = on
archive_command = 'pgbackrest --stanza=%s --config=%s --repo1-path=%s archive-push %%p'
`, stanzaName, configPath, pm.backupLocation)

	f, err := os.OpenFile(autoConfPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return mterrors.Wrap(err, "failed to open postgresql.auto.conf")
	}
	defer f.Close()

	if _, err := f.WriteString(archiveConfig); err != nil {
		return mterrors.Wrap(err, "failed to write archive config")
	}

	pm.logger.InfoContext(ctx, "Configured archive_mode in postgresql.auto.conf", "config_path", configPath, "stanza", stanzaName, "repo_path", pm.backupLocation)
	return nil
}

// initializePgBackRestStanza initializes the pgbackrest stanza
// This must be called after PostgreSQL is initialized and running
func (pm *MultiPoolerManager) initializePgBackRestStanza(ctx context.Context) error {
	configPath := pm.getBackupConfigPath()
	stanzaName := pm.getBackupStanza()

	// Validate required configuration
	if configPath == "" {
		return mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "backup config path not configured")
	}
	if stanzaName == "" {
		return mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "backup stanza name not configured")
	}

	// Check if config file exists
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		return mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION,
			fmt.Sprintf("pgbackrest config file not found at %s", configPath))
	}

	// Execute pgbackrest stanza-create command
	stanzaCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	cmd := exec.CommandContext(stanzaCtx, "pgbackrest",
		"--stanza="+stanzaName,
		"--config="+configPath,
		"--repo1-path="+pm.backupLocation,
		"stanza-create")

	output, err := safeCombinedOutput(cmd)
	if err != nil {
		return mterrors.New(mtrpcpb.Code_INTERNAL,
			fmt.Sprintf("failed to create pgbackrest stanza %s: %v\nOutput: %s", stanzaName, err, output))
	}

	pm.logger.InfoContext(ctx, "pgbackrest stanza initialized successfully", "stanza", stanzaName, "config", configPath)
	return nil
}
