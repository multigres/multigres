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
	"sort"
	"strings"
	"time"

	"github.com/multigres/multigres/go/mterrors"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// ============================================================================
// PostgreSQL Replication Operations
//
// This file contains methods and functions for querying and configuring
// PostgreSQL replication settings. These are low-level operations that
// directly interact with the database.
//
// For high-level orchestration logic (promotion, demotion, etc.), see
// manager.go and rpc_manager.go.
// ============================================================================

// ----------------------------------------------------------------------------
// Application Name Helpers
// ----------------------------------------------------------------------------

// generateApplicationName generates the application_name for a multipooler from its ID
// Format: {cell}_{name}
// This is used consistently for:
// - SetPrimaryConnInfo: standby's application_name when connecting to primary
// - ConfigureSynchronousReplication: standby names in synchronous_standby_names
func generateApplicationName(id *clustermetadatapb.ID) string {
	return fmt.Sprintf("%s_%s", id.Cell, id.Name)
}

// formatStandbyList converts standby IDs to a comma-separated list of quoted application names
func formatStandbyList(standbyIDs []*clustermetadatapb.ID) string {
	quotedNames := make([]string, len(standbyIDs))
	for i, id := range standbyIDs {
		quotedNames[i] = fmt.Sprintf(`"%s"`, generateApplicationName(id))
	}
	return strings.Join(quotedNames, ", ")
}

// ----------------------------------------------------------------------------
// Replication Status Query Methods
// ----------------------------------------------------------------------------

// isPrimary checks if the connected database is a primary (not in recovery)
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

// getPrimaryLSN gets the current WAL write location (primary only)
func (pm *MultiPoolerManager) getPrimaryLSN(ctx context.Context) (string, error) {
	var lsn string
	err := pm.db.QueryRowContext(ctx, "SELECT pg_current_wal_lsn()::text").Scan(&lsn)
	if err != nil {
		return "", mterrors.Wrap(err, "failed to get current WAL LSN")
	}
	return lsn, nil
}

// getStandbyReplayLSN gets the last replayed WAL location (standby only)
func (pm *MultiPoolerManager) getStandbyReplayLSN(ctx context.Context) (string, error) {
	var lsn string
	err := pm.db.QueryRowContext(ctx, "SELECT pg_last_wal_replay_lsn()::text").Scan(&lsn)
	if err != nil {
		return "", mterrors.Wrap(err, "failed to get replay LSN")
	}
	return lsn, nil
}

// queryReplicationStatus queries PostgreSQL for all replication status fields.
// This method handles NULL values properly for LSN fields that may be NULL
// when not in recovery mode or when no WAL has been received/replayed.
func (pm *MultiPoolerManager) queryReplicationStatus(ctx context.Context) (*multipoolermanagerdatapb.ReplicationStatus, error) {
	var replayLsn sql.NullString
	var receiveLsn sql.NullString
	var isPaused bool
	var pauseState string
	var lastXactTime sql.NullString
	var primaryConnInfo string

	query := `SELECT
		pg_last_wal_replay_lsn(),
		pg_last_wal_receive_lsn(),
		pg_is_wal_replay_paused(),
		pg_get_wal_replay_pause_state(),
		pg_last_xact_replay_timestamp(),
		current_setting('primary_conninfo')`

	err := pm.db.QueryRowContext(ctx, query).Scan(
		&replayLsn,
		&receiveLsn,
		&isPaused,
		&pauseState,
		&lastXactTime,
		&primaryConnInfo,
	)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to query replication status")
	}

	status := &multipoolermanagerdatapb.ReplicationStatus{
		IsWalReplayPaused:   isPaused,
		WalReplayPauseState: pauseState,
	}

	if replayLsn.Valid {
		status.LastReplayLsn = replayLsn.String
	}
	if receiveLsn.Valid {
		status.LastReceiveLsn = receiveLsn.String
	}
	if lastXactTime.Valid {
		status.LastXactReplayTimestamp = lastXactTime.String
	}

	// Parse primary_conninfo into structured format
	parsedConnInfo, err := parseAndRedactPrimaryConnInfo(primaryConnInfo)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to parse primary_conninfo")
	}
	status.PrimaryConnInfo = parsedConnInfo

	return status, nil
}

// waitForReplicationPause polls until WAL replay is paused and returns the status at that moment.
// This ensures the LSN returned represents the exact point at which replication stopped.
func (pm *MultiPoolerManager) waitForReplicationPause(ctx context.Context) (*multipoolermanagerdatapb.ReplicationStatus, error) {
	// Create a context with timeout for the polling loop
	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-waitCtx.Done():
			if waitCtx.Err() == context.DeadlineExceeded {
				pm.logger.ErrorContext(ctx, "Timeout waiting for WAL replay to pause")
				return nil, mterrors.New(mtrpcpb.Code_DEADLINE_EXCEEDED, "timeout waiting for WAL replay to pause")
			}
			pm.logger.ErrorContext(ctx, "Context cancelled while waiting for WAL replay to pause")
			return nil, mterrors.Wrap(waitCtx.Err(), "context cancelled while waiting for WAL replay to pause")

		case <-ticker.C:
			// Query all replication status fields
			status, err := pm.queryReplicationStatus(waitCtx)
			if err != nil {
				pm.logger.ErrorContext(ctx, "Failed to get replication status", "error", err)
				return nil, err
			}

			// Once paused, we have the exact state at the moment replication stopped
			if status.IsWalReplayPaused {
				pm.logger.InfoContext(ctx, "WAL replay is now paused",
					"last_replay_lsn", status.LastReplayLsn,
					"last_receive_lsn", status.LastReceiveLsn,
					"pause_state", status.WalReplayPauseState)

				return status, nil
			}
		}
	}
}

// resetPrimaryConnInfo clears primary_conninfo and reloads PostgreSQL configuration.
// This effectively disconnects the replica from the primary.
func (pm *MultiPoolerManager) resetPrimaryConnInfo(ctx context.Context) error {
	// Clear primary_conninfo using ALTER SYSTEM
	pm.logger.Info("Clearing primary_conninfo")
	_, err := pm.db.ExecContext(ctx, "ALTER SYSTEM RESET primary_conninfo")
	if err != nil {
		pm.logger.Error("Failed to clear primary_conninfo", "error", err)
		return mterrors.Wrap(err, "failed to clear primary_conninfo")
	}

	// Reload PostgreSQL configuration to apply changes
	pm.logger.Info("Reloading PostgreSQL configuration")
	_, err = pm.db.ExecContext(ctx, "SELECT pg_reload_conf()")
	if err != nil {
		pm.logger.Error("Failed to reload configuration", "error", err)
		return mterrors.Wrap(err, "failed to reload PostgreSQL configuration")
	}

	return nil
}

// waitForReceiverDisconnect waits for the WAL receiver to fully disconnect after clearing primary_conninfo.
// It polls pg_stat_wal_receiver to confirm the receiver has stopped.
func (pm *MultiPoolerManager) waitForReceiverDisconnect(ctx context.Context) (*multipoolermanagerdatapb.ReplicationStatus, error) {
	// Create a context with timeout for the polling loop
	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-waitCtx.Done():
			if waitCtx.Err() == context.DeadlineExceeded {
				pm.logger.Error("Timeout waiting for WAL receiver to disconnect")
				return nil, mterrors.New(mtrpcpb.Code_DEADLINE_EXCEEDED, "timeout waiting for WAL receiver to disconnect")
			}
			pm.logger.Error("Context cancelled while waiting for WAL receiver to disconnect")
			return nil, mterrors.Wrap(waitCtx.Err(), "context cancelled while waiting for WAL receiver to disconnect")

		case <-ticker.C:
			// Check if WAL receiver has disconnected by counting rows in pg_stat_wal_receiver
			var receiverCount int
			query := "SELECT COUNT(*) FROM pg_stat_wal_receiver"
			err := pm.db.QueryRowContext(waitCtx, query).Scan(&receiverCount)
			if err != nil {
				pm.logger.Error("Failed to query pg_stat_wal_receiver", "error", err)
				return nil, mterrors.Wrap(err, "failed to query pg_stat_wal_receiver")
			}

			// Once receiver is disconnected, query final replication status
			if receiverCount == 0 {
				pm.logger.Info("WAL receiver has disconnected")

				// Get the final replication status
				status, err := pm.queryReplicationStatus(waitCtx)
				if err != nil {
					pm.logger.Error("Failed to get replication status", "error", err)
					return nil, err
				}

				return status, nil
			}
		}
	}
}

// pauseReplication pauses replication based on the specified mode.
// If wait is true, it waits for the pause operation to complete before returning.
// Returns the replication status after pausing (if wait is true) or nil (if wait is false).
func (pm *MultiPoolerManager) pauseReplication(ctx context.Context, mode multipoolermanagerdatapb.ReplicationPauseMode, wait bool) (*multipoolermanagerdatapb.ReplicationStatus, error) {
	switch mode {
	case multipoolermanagerdatapb.ReplicationPauseMode_REPLICATION_PAUSE_MODE_REPLAY_ONLY:
		// Pause WAL replay on the standby
		pm.logger.Info("Pausing WAL replay on standby")
		_, err := pm.db.ExecContext(ctx, "SELECT pg_wal_replay_pause()")
		if err != nil {
			pm.logger.Error("Failed to pause WAL replay", "error", err)
			return nil, mterrors.Wrap(err, "failed to pause WAL replay")
		}

		if wait {
			// Wait for WAL replay to actually be paused
			// pg_wal_replay_pause() is asynchronous, so we need to wait for it to complete
			pm.logger.Info("Waiting for WAL replay to complete pausing")
			status, err := pm.waitForReplicationPause(ctx)
			if err != nil {
				return nil, err
			}
			return status, nil
		}

		return nil, nil

	case multipoolermanagerdatapb.ReplicationPauseMode_REPLICATION_PAUSE_MODE_RECEIVER_ONLY:
		// Stop the WAL receiver by clearing primary_conninfo
		pm.logger.Info("Stopping WAL receiver")
		if err := pm.resetPrimaryConnInfo(ctx); err != nil {
			return nil, err
		}

		if wait {
			// Wait for receiver to fully disconnect
			pm.logger.Info("Waiting for WAL receiver to disconnect")
			status, err := pm.waitForReceiverDisconnect(ctx)
			if err != nil {
				return nil, err
			}
			return status, nil
		}

		return nil, nil

	case multipoolermanagerdatapb.ReplicationPauseMode_REPLICATION_PAUSE_MODE_REPLAY_AND_RECEIVER:
		// First pause replay
		pm.logger.Info("Pausing both WAL replay and receiver")
		_, err := pm.db.ExecContext(ctx, "SELECT pg_wal_replay_pause()")
		if err != nil {
			pm.logger.Error("Failed to pause WAL replay", "error", err)
			return nil, mterrors.Wrap(err, "failed to pause WAL replay")
		}

		// Then stop receiver
		if err := pm.resetPrimaryConnInfo(ctx); err != nil {
			return nil, err
		}

		if wait {
			// Wait for replay pause to complete
			pm.logger.Info("Waiting for WAL replay to complete pausing")
			_, err := pm.waitForReplicationPause(ctx)
			if err != nil {
				return nil, err
			}

			// Wait for receiver to disconnect
			pm.logger.Info("Waiting for WAL receiver to disconnect")
			status, err := pm.waitForReceiverDisconnect(ctx)
			if err != nil {
				return nil, err
			}
			return status, nil
		}

		return nil, nil

	default:
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT,
			fmt.Sprintf("invalid replication pause mode: %d", mode))
	}
}

// validateExpectedLSN validates that the current replay LSN matches the expected LSN
func (pm *MultiPoolerManager) validateExpectedLSN(ctx context.Context, expectedLSN string) error {
	if expectedLSN == "" {
		return nil // No validation requested
	}

	var currentLSN string
	var isPaused bool
	query := "SELECT pg_last_wal_replay_lsn()::text, pg_is_wal_replay_paused()"
	err := pm.db.QueryRowContext(ctx, query).Scan(&currentLSN, &isPaused)
	if err != nil {
		pm.logger.ErrorContext(ctx, "Failed to get current replay LSN and pause state", "error", err)
		return mterrors.Wrap(err, "failed to get current replay LSN and pause state")
	}

	// Best practice: WAL replay should be paused before promotion
	// The coordinator should have called StopReplication during Discovery stage
	if !isPaused {
		pm.logger.WarnContext(ctx, "WAL replay is not paused before promotion - coordinator may have skipped Discovery stage",
			"current_lsn", currentLSN,
			"expected_lsn", expectedLSN)
		// Note: We don't fail here as this is a soft check, but it indicates
		// a potential issue in the consensus flow
	}

	if currentLSN != expectedLSN {
		pm.logger.ErrorContext(ctx, "LSN mismatch - node does not have expected durable state",
			"expected_lsn", expectedLSN,
			"current_lsn", currentLSN)
		return mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION,
			fmt.Sprintf("LSN mismatch: expected %s, current %s. "+
				"This indicates an error in an earlier consensus stage.",
				expectedLSN, currentLSN))
	}

	pm.logger.InfoContext(ctx, "LSN validation passed",
		"lsn", currentLSN,
		"wal_replay_paused", isPaused)
	return nil
}

// ----------------------------------------------------------------------------
// Synchronous Replication Configuration
// ----------------------------------------------------------------------------

// setSynchronousCommit sets the PostgreSQL synchronous_commit level
func (pm *MultiPoolerManager) setSynchronousCommit(ctx context.Context, synchronousCommit multipoolermanagerdatapb.SynchronousCommitLevel) error {
	// Convert enum to PostgreSQL string value
	var syncCommitValue string
	switch synchronousCommit {
	case multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_OFF:
		syncCommitValue = "off"
	case multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_LOCAL:
		syncCommitValue = "local"
	case multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_REMOTE_WRITE:
		syncCommitValue = "remote_write"
	case multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON:
		syncCommitValue = "on"
	case multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_REMOTE_APPLY:
		syncCommitValue = "remote_apply"
	default:
		return mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT,
			fmt.Sprintf("invalid synchronous_commit level: %s", synchronousCommit.String()))
	}

	pm.logger.InfoContext(ctx, "Setting synchronous_commit", "value", syncCommitValue)
	_, err := pm.db.ExecContext(ctx, fmt.Sprintf("ALTER SYSTEM SET synchronous_commit = '%s'", syncCommitValue))
	if err != nil {
		pm.logger.ErrorContext(ctx, "Failed to set synchronous_commit", "error", err)
		return mterrors.Wrap(err, "failed to set synchronous_commit")
	}

	return nil
}

// buildSynchronousStandbyNamesValue constructs the synchronous_standby_names value string
// This produces values like: FIRST 1 ("standby-1", "standby-2") or ANY 1 ("standby-1", "standby-2")
func buildSynchronousStandbyNamesValue(method multipoolermanagerdatapb.SynchronousMethod, numSync int32, standbyIDs []*clustermetadatapb.ID) (string, error) {
	if len(standbyIDs) == 0 {
		return "", nil
	}

	standbyList := formatStandbyList(standbyIDs)

	var methodStr string
	switch method {
	case multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST:
		methodStr = "FIRST"
	case multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_ANY:
		methodStr = "ANY"
	default:
		return "", mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT,
			fmt.Sprintf("invalid synchronous method: %s, must be FIRST or ANY", method.String()))
	}

	return fmt.Sprintf("%s %d (%s)", methodStr, numSync, standbyList), nil
}

// applySynchronousStandbyNames applies the synchronous_standby_names setting to PostgreSQL
func applySynchronousStandbyNames(ctx context.Context, db *sql.DB, logger *slog.Logger, value string) error {
	logger.InfoContext(ctx, "Setting synchronous_standby_names", "value", value)

	// Escape single quotes in the value by doubling them (PostgreSQL standard)
	escapedValue := strings.ReplaceAll(value, "'", "''")

	// ALTER SYSTEM SET doesn't support parameterized queries, so we use string formatting
	query := fmt.Sprintf("ALTER SYSTEM SET synchronous_standby_names = '%s'", escapedValue)
	_, err := db.ExecContext(ctx, query)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to set synchronous_standby_names", "error", err)
		return mterrors.Wrap(err, "failed to set synchronous_standby_names")
	}

	return nil
}

// setSynchronousStandbyNames builds and sets the PostgreSQL synchronous_standby_names configuration
// Format: https://www.postgresql.org/docs/current/runtime-config-replication.html#GUC-SYNCHRONOUS-STANDBY-NAMES
// Examples:
//
//	FIRST 2 (standby1, standby2, standby3)
//	ANY 1 (standby1, standby2)
//
// Note: Use '*' to match all connected standbys, or specify explicit standby application_name values
// Application names are generated from multipooler IDs using the shared generateApplicationName helper
func (pm *MultiPoolerManager) setSynchronousStandbyNames(ctx context.Context, synchronousMethod multipoolermanagerdatapb.SynchronousMethod, numSync int32, standbyIDs []*clustermetadatapb.ID) error {
	// If standby list is empty, clear synchronous_standby_names
	if len(standbyIDs) == 0 {
		pm.logger.InfoContext(ctx, "Clearing synchronous_standby_names (empty standby list)")
		query := "ALTER SYSTEM SET synchronous_standby_names = ''"
		_, err := pm.db.ExecContext(ctx, query)
		if err != nil {
			pm.logger.ErrorContext(ctx, "Failed to clear synchronous_standby_names", "error", err)
			return mterrors.Wrap(err, "failed to clear synchronous_standby_names")
		}
		return nil
	}

	// If numSync was not provided, default to 1
	if numSync == 0 {
		numSync = 1
	}

	// Build the synchronous_standby_names value using the shared helper
	standbyNamesValue, err := buildSynchronousStandbyNamesValue(synchronousMethod, numSync, standbyIDs)
	if err != nil {
		return err
	}

	// Apply the setting using the shared helper
	return applySynchronousStandbyNames(ctx, pm.db, pm.logger, standbyNamesValue)
}

// getSynchronousReplicationConfig retrieves and parses the current synchronous replication configuration
func (pm *MultiPoolerManager) getSynchronousReplicationConfig(ctx context.Context) (*multipoolermanagerdatapb.SynchronousReplicationConfiguration, error) {
	config := &multipoolermanagerdatapb.SynchronousReplicationConfiguration{}

	// Query synchronous_standby_names
	var syncStandbyNamesStr string
	err := pm.db.QueryRowContext(ctx, "SHOW synchronous_standby_names").Scan(&syncStandbyNamesStr)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to query synchronous_standby_names")
	}

	// Only parse standby names if not empty
	syncStandbyNamesStr = strings.TrimSpace(syncStandbyNamesStr)
	if syncStandbyNamesStr != "" {
		syncConfig, err := parseSynchronousStandbyNames(syncStandbyNamesStr)
		if err != nil {
			return nil, err
		}
		config.SynchronousMethod = syncConfig.Method
		config.NumSync = syncConfig.NumSync
		config.StandbyIds = syncConfig.StandbyIDs
	}

	// Query synchronous_commit
	var syncCommitStr string
	err = pm.db.QueryRowContext(ctx, "SHOW synchronous_commit").Scan(&syncCommitStr)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to query synchronous_commit")
	}

	// Map string to enum
	var syncCommitLevel multipoolermanagerdatapb.SynchronousCommitLevel
	switch strings.ToLower(syncCommitStr) {
	case "off":
		syncCommitLevel = multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_OFF
	case "local":
		syncCommitLevel = multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_LOCAL
	case "remote_write":
		syncCommitLevel = multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_REMOTE_WRITE
	case "on":
		syncCommitLevel = multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON
	case "remote_apply":
		syncCommitLevel = multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_REMOTE_APPLY
	default:
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT,
			fmt.Sprintf("unknown synchronous_commit value: %q", syncCommitStr))
	}
	config.SynchronousCommit = syncCommitLevel

	return config, nil
}

// resetSynchronousReplication clears the synchronous standby list
// This should be called after the server is read-only to safely clear settings
func (pm *MultiPoolerManager) resetSynchronousReplication(ctx context.Context) error {
	pm.logger.InfoContext(ctx, "Clearing synchronous standby list")

	// Clear synchronous_standby_names to remove all standbys
	_, err := pm.db.ExecContext(ctx, "ALTER SYSTEM SET synchronous_standby_names = ''")
	if err != nil {
		pm.logger.ErrorContext(ctx, "Failed to clear synchronous_standby_names", "error", err)
		return mterrors.Wrap(err, "failed to clear synchronous_standby_names")
	}

	// Reload configuration to apply changes
	_, err = pm.db.ExecContext(ctx, "SELECT pg_reload_conf()")
	if err != nil {
		pm.logger.ErrorContext(ctx, "Failed to reload configuration", "error", err)
		return mterrors.Wrap(err, "failed to reload configuration after clearing standby list")
	}

	pm.logger.InfoContext(ctx, "Successfully cleared synchronous standby list")
	return nil
}

// syncReplicationConfigMatches checks if the current sync replication config matches the requested config
func (pm *MultiPoolerManager) syncReplicationConfigMatches(current *multipoolermanagerdatapb.SynchronousReplicationConfiguration, requested *multipoolermanagerdatapb.ConfigureSynchronousReplicationRequest) bool {
	// Check synchronous commit level
	if current.SynchronousCommit != requested.SynchronousCommit {
		return false
	}

	// Check synchronous method
	if current.SynchronousMethod != requested.SynchronousMethod {
		return false
	}

	// Check num_sync
	if current.NumSync != requested.NumSync {
		return false
	}

	// Check standby IDs (must match exactly 1:1, so sort and compare)
	if len(current.StandbyIds) != len(requested.StandbyIds) {
		return false
	}

	// Sort both lists by cell_name for comparison
	currentSorted := make([]string, len(current.StandbyIds))
	for i, id := range current.StandbyIds {
		currentSorted[i] = fmt.Sprintf("%s_%s", id.Cell, id.Name)
	}
	sort.Strings(currentSorted)

	requestedSorted := make([]string, len(requested.StandbyIds))
	for i, id := range requested.StandbyIds {
		requestedSorted[i] = fmt.Sprintf("%s_%s", id.Cell, id.Name)
	}
	sort.Strings(requestedSorted)

	// Compare sorted lists element by element
	for i := range currentSorted {
		if currentSorted[i] != requestedSorted[i] {
			return false
		}
	}

	return true
}

// ----------------------------------------------------------------------------
// Validation Helpers
// ----------------------------------------------------------------------------
// validateStandbyIDs validates a list of standby IDs
func validateStandbyIDs(standbyIDs []*clustermetadatapb.ID) error {
	if len(standbyIDs) == 0 {
		return mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "standby_ids cannot be empty")
	}

	for i, id := range standbyIDs {
		if id == nil {
			return mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT,
				fmt.Sprintf("standby_ids[%d] is nil", i))
		}
		if id.Cell == "" {
			return mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT,
				fmt.Sprintf("standby_ids[%d] has empty cell", i))
		}
		if id.Name == "" {
			return mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT,
				fmt.Sprintf("standby_ids[%d] has empty name", i))
		}
		// Underscores are not allowed in Cell or Name because they are used as delimiters
		// in the application_name format (cell_name). Allowing underscores would break parsing.
		if strings.Contains(id.Cell, "_") {
			return mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT,
				fmt.Sprintf("standby_ids[%d] cell contains underscore: %q (underscores not allowed)", i, id.Cell))
		}
		if strings.Contains(id.Name, "_") {
			return mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT,
				fmt.Sprintf("standby_ids[%d] name contains underscore: %q (underscores not allowed)", i, id.Name))
		}
	}

	return nil
}

// validateSyncReplicationParams validates the parameters for ConfigureSynchronousReplication
func validateSyncReplicationParams(numSync int32, standbyIDs []*clustermetadatapb.ID) error {
	// Validate numSync is non-negative
	if numSync < 0 {
		return mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT,
			fmt.Sprintf("num_sync must be non-negative, got: %d", numSync))
	}

	// If standbyIDs are provided, validate them
	if len(standbyIDs) > 0 {
		// Validate that numSync doesn't exceed the number of standbys (PostgreSQL requirement)
		// Note: numSync=0 is allowed and will be defaulted to 1 in setSynchronousStandbyNames
		if numSync > int32(len(standbyIDs)) {
			return mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT,
				fmt.Sprintf("num_sync (%d) cannot exceed number of standby_ids (%d)", numSync, len(standbyIDs)))
		}

		// Validate each standby ID
		if err := validateStandbyIDs(standbyIDs); err != nil {
			return err
		}
	}

	return nil
}

// ----------------------------------------------------------------------------
// Standby List Operations
// ----------------------------------------------------------------------------

// applyAddOperation adds new standbys to the standby list (idempotent)
func applyAddOperation(currentStandbys []*clustermetadatapb.ID, newStandbys []*clustermetadatapb.ID) []*clustermetadatapb.ID {
	updatedStandbys := append([]*clustermetadatapb.ID{}, currentStandbys...)
	existingMap := make(map[string]bool)
	for _, standby := range currentStandbys {
		existingMap[generateApplicationName(standby)] = true
	}
	for _, newStandby := range newStandbys {
		if !existingMap[generateApplicationName(newStandby)] {
			updatedStandbys = append(updatedStandbys, newStandby)
		}
	}
	return updatedStandbys
}

// applyRemoveOperation removes standbys from the standby list (idempotent)
func applyRemoveOperation(currentStandbys []*clustermetadatapb.ID, standbysToRemove []*clustermetadatapb.ID) []*clustermetadatapb.ID {
	removeMap := make(map[string]bool)
	for _, standby := range standbysToRemove {
		removeMap[generateApplicationName(standby)] = true
	}
	var updatedStandbys []*clustermetadatapb.ID
	for _, standby := range currentStandbys {
		if !removeMap[generateApplicationName(standby)] {
			updatedStandbys = append(updatedStandbys, standby)
		}
	}
	return updatedStandbys
}

// applyReplaceOperation replaces the entire standby list
func applyReplaceOperation(newStandbys []*clustermetadatapb.ID) []*clustermetadatapb.ID {
	return newStandbys
}
