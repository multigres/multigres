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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/multigres/multigres/go/common/mterrors"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	pgctldpb "github.com/multigres/multigres/go/pb/pgctldservice"
	"github.com/multigres/multigres/go/tools/retry"
)

// multigresInitMarker is the filename for the initialization marker.
// This file is created after full initialization completes (schema created, backup done).
const multigresInitMarker = "MULTIGRES_INITIALIZED"

// isInitialized checks if the pooler has been initialized (has data directory and multigres schema)
// This should return true even when postgres is not running, as long as the node was previously initialized.
func (pm *MultipoolerManager) isInitialized(ctx context.Context) bool {
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
func (pm *MultipoolerManager) setInitialized() error {
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
func (pm *MultipoolerManager) writeInitializationMarker() error {
	if err := os.MkdirAll(multigresDataDir(), 0o755); err != nil {
		return fmt.Errorf("failed to create multigres directory: %w", err)
	}
	markerFile := filepath.Join(multigresDataDir(), multigresInitMarker)
	return os.WriteFile(markerFile, []byte("initialized\n"), 0o644)
}

// hasDataDirectory checks if the PostgreSQL data directory exists
func (pm *MultipoolerManager) hasDataDirectory() bool {
	// Check if PG_VERSION file exists to confirm the data directory is properly initialized.
	// This prevents treating an empty directory (e.g., left behind by a failed initdb) as initialized.
	pgVersionFile := filepath.Join(postgresDataDir(), "PG_VERSION")
	_, err := os.Stat(pgVersionFile)
	return err == nil
}

// isPostgresRunning checks if the PostgreSQL process exists, regardless of
// whether it accepts connections. Returns true if the process is running (even if
// suspended via SIGSTOP). Returns false if the process is dead (e.g. after SIGKILL).
//
// When pgctld is not available, falls back to isPostgresReady (which requires
// both process existence and connection acceptance).
func (pm *MultipoolerManager) isPostgresRunning(ctx context.Context) bool {
	if pm.pgctldClient == nil {
		// No pgctld client — fall back to connection-based check.
		// Without pgctld we can't distinguish a stopped-but-alive process from a dead one.
		_, err := pm.query(ctx, "SELECT 1")
		return err == nil
	}

	statusReq := &pgctldpb.StatusRequest{}
	statusResp, err := pm.pgctldClient.Status(ctx, statusReq)
	if err != nil {
		return false
	}

	// Only check if the process is running; do NOT require pg_isready (statusResp.Ready).
	return statusResp.Status == pgctldpb.ServerStatus_RUNNING
}

// isPostgresReady checks if PostgreSQL is currently running and accepting connections.
// Returns true only if the process is running AND pg_isready succeeds.
// Use isPostgresRunning to check only if the process exists.
func (pm *MultipoolerManager) isPostgresReady(ctx context.Context) bool {
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

	return statusResp.Status == pgctldpb.ServerStatus_RUNNING && statusResp.Ready
}

// getServerStatus returns the observed state of the PostgreSQL server process.
// Priority: STARTING (action lock) > PROMOTING (in-flight pg_promote) > PRIMARY/STANDBY (pg_is_in_recovery).
func (pm *MultipoolerManager) getServerStatus(ctx context.Context) multipoolermanagerdatapb.PostgresStatus {
	if action, _ := pm.actionLock.ActiveAction(); action == multipoolermanagerdatapb.PostgresAction_POSTGRES_ACTION_STARTING {
		return multipoolermanagerdatapb.PostgresStatus_POSTGRES_STATUS_STARTING
	}
	if pm.promotionInProgress.Load() {
		return multipoolermanagerdatapb.PostgresStatus_POSTGRES_STATUS_PROMOTING
	}
	pgMode, err := pm.postgresMode(ctx)
	if err != nil {
		return multipoolermanagerdatapb.PostgresStatus_POSTGRES_STATUS_UNKNOWN
	}
	if pgMode.OutOfRecovery() {
		return multipoolermanagerdatapb.PostgresStatus_POSTGRES_STATUS_PRIMARY
	}
	return multipoolermanagerdatapb.PostgresStatus_POSTGRES_STATUS_STANDBY
}

// getWALPosition returns the current WAL position and any error encountered
func (pm *MultipoolerManager) getWALPosition(ctx context.Context) (string, error) {
	pgMode, err := pm.postgresMode(ctx)
	if err != nil {
		return "", err
	}

	if pgMode.OutOfRecovery() {
		return pm.getPrimaryLSN(ctx)
	}
	return pm.getStandbyReplayLSN(ctx)
}

// getShardID returns the shard ID for this pooler.
// Prefers the topology value (pm.record.ShardKey().Shard) but falls back to config
// if topology hasn't loaded yet. These should always be identical since
// the topology value is set from config at registration (init.go).
func (pm *MultipoolerManager) getShardID() string {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if pm.record.ShardKey().GetShard() != "" {
		return pm.record.ShardKey().GetShard()
	}

	// Fall back to Multipooler - always available and authoritative
	return pm.record.ShardKey().GetShard()
}

// removeDataDirectory removes the PostgreSQL data directory
func (pm *MultipoolerManager) removeDataDirectory() error {
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
func (pm *MultipoolerManager) waitForDatabaseConnection(ctx context.Context) error {
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
