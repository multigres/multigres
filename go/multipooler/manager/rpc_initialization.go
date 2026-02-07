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
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/multigres/multigres/go/common/mterrors"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	pgctldpb "github.com/multigres/multigres/go/pb/pgctldservice"
	"github.com/multigres/multigres/go/tools/retry"
)

// InitializeEmptyPrimary initializes this pooler as an empty primary
// Used during bootstrap initialization of a new shard
func (pm *MultiPoolerManager) InitializeEmptyPrimary(ctx context.Context, req *multipoolermanagerdatapb.InitializeEmptyPrimaryRequest) (*multipoolermanagerdatapb.InitializeEmptyPrimaryResponse, error) {
	pm.logger.InfoContext(ctx, "InitializeEmptyPrimary called", "shard", pm.getShardID(), "term", req.ConsensusTerm)

	// Wait for topology to be loaded (needed for backup location)
	if err := pm.checkReady(); err != nil {
		return nil, err
	}

	// Acquire action lock
	var err error
	ctx, err = pm.actionLock.Acquire(ctx, "InitializeEmptyPrimary")
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to acquire action lock")
	}
	defer pm.actionLock.Release(ctx)

	// Pause monitoring during initialization to prevent interference
	resumeMonitor, err := pm.PausePostgresMonitor(ctx)
	if err != nil {
		return nil, err
	}
	defer resumeMonitor(ctx)

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

	// Set pooler type to PRIMARY before creating backup so the backup annotation is correct
	if err := pm.changeTypeLocked(ctx, clustermetadatapb.PoolerType_PRIMARY); err != nil {
		return nil, mterrors.Wrap(err, "failed to set pooler type")
	}

	// Set primary term during bootstrap initialization
	if pm.consensusState != nil {
		if err := pm.consensusState.SetPrimaryTerm(ctx, req.ConsensusTerm, false /* force */); err != nil {
			return nil, mterrors.Wrap(err, "failed to set primary term")
		}
	}

	// Create initial backup for standby initialization
	pm.logger.InfoContext(ctx, "Creating initial backup for standby initialization", "shard", pm.getShardID())
	backupID, err := pm.backupLocked(ctx, true, "full", "")
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to create initial backup")
	}
	pm.logger.InfoContext(ctx, "Initial backup created", "backup_id", backupID)

	// Create durability policy if requested
	if req.DurabilityPolicyName != "" && req.DurabilityQuorumRule != nil {
		if err := pm.createDurabilityPolicyLocked(ctx, req.DurabilityPolicyName, req.DurabilityQuorumRule); err != nil {
			return nil, mterrors.Wrap(err, "failed to create durability policy")
		}
		pm.logger.InfoContext(ctx, "Created durability policy", "policy_name", req.DurabilityPolicyName)
	}

	// Get final LSN position for leadership history
	finalLSN, err := pm.getPrimaryLSN(ctx)
	if err != nil {
		pm.logger.ErrorContext(ctx, "Failed to get final LSN", "error", err)
		return nil, err
	}

	// Write leadership history record for bootstrap
	leaderID := generateApplicationName(pm.serviceID)
	coordinatorID := req.CoordinatorId
	reason := "ShardNeedsBootstrap"
	cohortMembers := []string{leaderID} // Only the initial primary during bootstrap
	acceptedMembers := []string{leaderID}

	if err := pm.insertHistoryRecord(ctx,
		req.ConsensusTerm,
		"promotion",
		leaderID,
		coordinatorID,
		finalLSN,
		"bootstrap", // operation
		reason,
		cohortMembers,
		acceptedMembers,
		false /* force */); err != nil {
		// Log but don't fail - history is for audit, not correctness
		pm.logger.WarnContext(ctx, "Failed to insert leadership history",
			"term", req.ConsensusTerm,
			"error", err)
	}

	// Mark as initialized after successful primary initialization.
	// This sets the cached boolean and writes the marker file.
	if err := pm.setInitialized(); err != nil {
		return nil, mterrors.Wrap(err, "failed to mark pooler as initialized")
	}

	pm.logger.InfoContext(ctx, "Successfully initialized pooler as empty primary", "shard", pm.getShardID(), "term", req.ConsensusTerm)
	return &multipoolermanagerdatapb.InitializeEmptyPrimaryResponse{
		Success:  true,
		BackupId: backupID,
	}, nil
}

// Helper methods

// multigresInitMarker is the filename for the initialization marker.
// This file is created after full initialization completes (schema created, backup done).
const multigresInitMarker = "MULTIGRES_INITIALIZED"

// isInitialized checks if the pooler has been initialized (has data directory and multigres schema)
// This should return true even when postgres is not running, as long as the node was previously initialized.
func (pm *MultiPoolerManager) isInitialized(ctx context.Context) bool {
	// Fast path: check cached state first
	if func() bool {
		pm.mu.Lock()
		defer pm.mu.Unlock()
		return pm.initialized
	}() {
		return true
	}

	if !pm.hasDataDirectory() {
		return false
	}

	var initialized bool

	// Try to check if multigres schema exists via a query
	exists, err := pm.querySchemaExists(ctx)
	if err == nil {
		initialized = exists
	} else {
		// If database is not connected (e.g., postgres is down), check for the initialization marker.
		// This marker is created after full initialization completes (schema created, backup done).
		// It's more reliable than checking for PG_VERSION/global because those exist after initdb
		// but before the full initialization process completes.
		dataDir := filepath.Join(pm.multipooler.PoolerDir, "pg_data")
		markerFile := filepath.Join(dataDir, multigresInitMarker)
		_, err := os.Stat(markerFile)
		initialized = err == nil
	}

	// Update cached state if we discovered initialization
	if initialized {
		pm.mu.Lock()
		pm.initialized = true
		pm.mu.Unlock()
	}

	return initialized
}

// setInitialized marks the pooler as initialized and writes the marker file.
// This should be called after successful initialization (primary init, standby init, or restore).
// Once set, the pooler will skip auto-restore attempts.
func (pm *MultiPoolerManager) setInitialized() error {
	pm.mu.Lock()
	pm.initialized = true
	pm.mu.Unlock()

	return pm.writeInitializationMarker()
}

// writeInitializationMarker creates the initialization marker file to indicate
// that full initialization, including restore from backup (for standbys) has
// completed. This is called at the end of primary and standby initialization.
//
// The marker file is needed to determine whether a replica pooler is
// initialized, because replica initialization is not done until the restore
// from backup completes. There is no other persistent way to determine this.
func (pm *MultiPoolerManager) writeInitializationMarker() error {
	dataDir := filepath.Join(pm.multipooler.PoolerDir, "pg_data")
	markerFile := filepath.Join(dataDir, multigresInitMarker)
	return os.WriteFile(markerFile, []byte("initialized\n"), 0o644)
}

// hasDataDirectory checks if the PostgreSQL data directory exists
func (pm *MultiPoolerManager) hasDataDirectory() bool {
	poolerDir := pm.multipooler.PoolerDir
	if poolerDir == "" {
		return false
	}

	// Check if PG_VERSION file exists to confirm the data directory is properly initialized.
	// This prevents treating an empty directory (e.g., left behind by a failed initdb) as initialized.
	dataDir := filepath.Join(poolerDir, "pg_data")
	pgVersionFile := filepath.Join(dataDir, "PG_VERSION")
	_, err := os.Stat(pgVersionFile)
	return err == nil
}

// isPostgresRunning checks if PostgreSQL is currently running
func (pm *MultiPoolerManager) isPostgresRunning(ctx context.Context) bool {
	if pm.pgctldClient == nil {
		// No pgctld client, try a simple query to check if PostgreSQL is responding
		_, err := pm.query(ctx, "SELECT 1")
		return err == nil
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
	isPrimary, err := pm.isPrimary(ctx)
	if err != nil {
		return "", err
	}

	if isPrimary {
		return pm.getPrimaryLSN(ctx)
	}
	return pm.getStandbyReplayLSN(ctx)
}

// getShardID returns the shard ID for this pooler.
// Prefers the topology value (pm.multipooler.Shard) but falls back to config
// if topology hasn't loaded yet. These should always be identical since
// the topology value is set from config at registration (init.go).
func (pm *MultiPoolerManager) getShardID() string {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if pm.multipooler.Shard != "" {
		return pm.multipooler.Shard
	}

	// Fall back to MultiPooler - always available and authoritative
	return pm.multipooler.Shard
}

// removeDataDirectory removes the PostgreSQL data directory
func (pm *MultiPoolerManager) removeDataDirectory() error {
	poolerDir := pm.multipooler.PoolerDir
	if poolerDir == "" {
		return errors.New("pooler directory path not configured")
	}

	dataDir := filepath.Join(poolerDir, "pg_data")

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
	// Test if database is already reachable
	if _, err := pm.query(ctx, "SELECT 1"); err == nil {
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

		// Try to query the database
		if _, queryErr := pm.query(ctx, "SELECT 1"); queryErr == nil {
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

			return nil
		} else {
			lastErr = queryErr
			if firstAttempt {
				pm.logger.InfoContext(ctx, "PostgreSQL not ready yet, will retry with exponential backoff", "error", queryErr)
				firstAttempt = false
			}
		}
	}

	// This should not be reached due to the context check in the loop, but just in case
	return mterrors.Wrap(lastErr, "failed to connect to database after retries")
}

// removeArchiveConfigFromAutoConf removes archive configuration lines from postgresql.auto.conf
// This is used after restore to remove the primary's archive config before applying the standby's config
func (pm *MultiPoolerManager) removeArchiveConfigFromAutoConf() error {
	autoConfPath := filepath.Join(pm.multipooler.PoolerDir, "pg_data", "postgresql.auto.conf")

	content, err := os.ReadFile(autoConfPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // File doesn't exist, nothing to remove
		}
		return fmt.Errorf("failed to read postgresql.auto.conf: %w", err)
	}

	var filtered []string
	for line := range strings.SplitSeq(string(content), "\n") {
		trimmed := strings.TrimSpace(line)
		// Skip archive-related lines
		if strings.HasPrefix(trimmed, "archive_mode") ||
			strings.HasPrefix(trimmed, "archive_command") ||
			trimmed == "# Archive mode for pgbackrest backups" {
			continue
		}
		filtered = append(filtered, line)
	}

	return os.WriteFile(autoConfPath, []byte(strings.Join(filtered, "\n")), 0o644)
}

// configureArchiveMode configures archive_mode in postgresql.auto.conf for pgbackrest
// This must be called after InitDataDir but BEFORE starting PostgreSQL
func (pm *MultiPoolerManager) configureArchiveMode(ctx context.Context) error {
	configPath, err := pm.initPgBackRest(ctx, NotForBackup)
	if err != nil {
		return mterrors.Wrap(err, "failed to initialize pgbackrest")
	}

	// Check if pgbackrest config file exists before configuring archive mode
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		return mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION,
			fmt.Sprintf("pgbackrest config file not found at %s - cannot configure archive mode", configPath))
	}

	autoConfPath := filepath.Join(pm.multipooler.PoolerDir, "pg_data", "postgresql.auto.conf")

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
archive_mode = 'on'
archive_command = 'pgbackrest --stanza=%s --config=%s archive-push %%p'
`, pm.stanzaName(), configPath)

	f, err := os.OpenFile(autoConfPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return mterrors.Wrap(err, "failed to open postgresql.auto.conf")
	}
	defer f.Close()

	if _, err := f.WriteString(archiveConfig); err != nil {
		return mterrors.Wrap(err, "failed to write archive config")
	}

	pm.logger.InfoContext(ctx, "Configured archive_mode in postgresql.auto.conf", "config_path", configPath, "stanza", pm.stanzaName(), "backup_type", pm.backupConfig.Type())
	return nil
}

// initializePgBackRestStanza initializes the pgbackrest stanza
// This must be called after PostgreSQL is initialized and running
func (pm *MultiPoolerManager) initializePgBackRestStanza(ctx context.Context) error {
	configPath, err := pm.initPgBackRest(ctx, NotForBackup)
	if err != nil {
		return mterrors.Wrap(err, "failed to initialize pgbackrest")
	}

	// Execute pgbackrest stanza-create command
	stanzaCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	cmd := exec.CommandContext(stanzaCtx, "pgbackrest",
		"--stanza="+pm.stanzaName(),
		"--config="+configPath,
		"stanza-create")

	output, err := safeCombinedOutput(cmd)
	if err != nil {
		return mterrors.New(mtrpcpb.Code_INTERNAL,
			fmt.Sprintf("failed to create pgbackrest stanza %s: %v\nOutput: %s", pm.stanzaName(), err, output))
	}

	pm.logger.InfoContext(ctx, "pgbackrest stanza initialized successfully", "stanza", pm.stanzaName(), "config", configPath)
	return nil
}
