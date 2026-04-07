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
	"path/filepath"
	"strings"
	"time"

	"github.com/multigres/multigres/go/common/mterrors"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	pgctldpb "github.com/multigres/multigres/go/pb/pgctldservice"
	"github.com/multigres/multigres/go/tools/retry"
)

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
	cacheInitialized := false

	// Try to check if multigres schema exists via a query
	exists, err := pm.querySchemaExists(ctx)
	if err == nil {
		initialized = exists
		cacheInitialized = true
	} else {
		// This marker is created after full initialization completes (schema created, backup done).
		// It's more reliable than checking for PG_VERSION/global because those exist after initdb
		// but before the full initialization process completes.
		markerFile := filepath.Join(multigresDataDir(), multigresInitMarker)
		_, err := os.Stat(markerFile)
		initialized = err == nil
	}

	// Update cached state if we discovered initialization
	if initialized && cacheInitialized {
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
	if err := os.MkdirAll(multigresDataDir(), 0o755); err != nil {
		return fmt.Errorf("failed to create multigres directory: %w", err)
	}
	markerFile := filepath.Join(multigresDataDir(), multigresInitMarker)
	return os.WriteFile(markerFile, []byte("initialized\n"), 0o644)
}

// hasDataDirectory checks if the PostgreSQL data directory exists
func (pm *MultiPoolerManager) hasDataDirectory() bool {
	// Check if PG_VERSION file exists to confirm the data directory is properly initialized.
	// This prevents treating an empty directory (e.g., left behind by a failed initdb) as initialized.
	pgVersionFile := filepath.Join(postgresDataDir(), "PG_VERSION")
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
	dataDir := postgresDataDir()
	if dataDir == "" {
		return errors.New("PGDATA environment variable not set")
	}

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
	autoConfPath := filepath.Join(postgresDataDir(), "postgresql.auto.conf")

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
	configPath, err := pm.pgBackRestConfig()
	if err != nil {
		return mterrors.Wrap(err, "failed to initialize pgbackrest")
	}

	// Check if pgbackrest config file exists before configuring archive mode
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		return mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION,
			fmt.Sprintf("pgbackrest config file not found at %s - ensure pgctld generated the config successfully", configPath))
	}

	autoConfPath := filepath.Join(postgresDataDir(), "postgresql.auto.conf")

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
