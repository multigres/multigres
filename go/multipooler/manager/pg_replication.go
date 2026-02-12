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
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/multipooler/executor"

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
	inRecovery, err := pm.isInRecovery(ctx)
	return !inRecovery, err
}

// isInRecovery checks if the connected database is in recovery mode (standby).
// Returns true if the database is a standby, false if it's a primary.
func (pm *MultiPoolerManager) isInRecovery(ctx context.Context) (bool, error) {
	queryCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	result, err := pm.query(queryCtx, "SELECT pg_is_in_recovery()")
	if err != nil {
		return false, fmt.Errorf("failed to query pg_is_in_recovery: %w", err)
	}

	var inRecovery bool
	if err := executor.ScanSingleRow(result, &inRecovery); err != nil {
		return false, fmt.Errorf("failed to scan pg_is_in_recovery result: %w", err)
	}

	return inRecovery, nil
}

// getPrimaryLSN gets the current WAL write location (primary only)
func (pm *MultiPoolerManager) getPrimaryLSN(ctx context.Context) (string, error) {
	queryCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	result, err := pm.query(queryCtx, "SELECT pg_current_wal_lsn()::text")
	if err != nil {
		return "", mterrors.Wrap(err, "failed to get current WAL LSN")
	}
	var lsn string
	if err := executor.ScanSingleRow(result, &lsn); err != nil {
		return "", mterrors.Wrap(err, "failed to scan WAL LSN result")
	}
	return lsn, nil
}

// getStandbyReplayLSN gets the last replayed WAL location (standby only)
func (pm *MultiPoolerManager) getStandbyReplayLSN(ctx context.Context) (string, error) {
	queryCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	result, err := pm.query(queryCtx, "SELECT pg_last_wal_replay_lsn()::text")
	if err != nil {
		return "", mterrors.Wrap(err, "failed to get replay LSN")
	}
	var lsn string
	if err := executor.ScanSingleRow(result, &lsn); err != nil {
		return "", mterrors.Wrap(err, "failed to scan replay LSN result")
	}
	return lsn, nil
}

// getTimelineID gets the current timeline ID from pg_control_checkpoint()
func (pm *MultiPoolerManager) getTimelineID(ctx context.Context) (int64, error) {
	queryCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	result, err := pm.query(queryCtx, "SELECT timeline_id FROM pg_control_checkpoint()")
	if err != nil {
		return 0, mterrors.Wrap(err, "failed to get timeline ID")
	}
	var timelineID int64
	if err := executor.ScanSingleRow(result, &timelineID); err != nil {
		return 0, mterrors.Wrap(err, "failed to scan timeline ID result")
	}
	return timelineID, nil
}

// querySchemaExists checks if the multigres schema exists in the database
func (pm *MultiPoolerManager) querySchemaExists(ctx context.Context) (bool, error) {
	queryCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	sql := "SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = 'multigres')"
	result, err := pm.query(queryCtx, sql)
	if err != nil {
		return false, mterrors.Wrap(err, "failed to check schema exists")
	}
	var exists bool
	if err := executor.ScanSingleRow(result, &exists); err != nil {
		return false, mterrors.Wrap(err, "failed to scan schema exists result")
	}
	return exists, nil
}

// checkLSNReached checks if the standby has replayed up to or past the target LSN
func (pm *MultiPoolerManager) checkLSNReached(ctx context.Context, targetLsn string) (bool, error) {
	queryCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	result, err := pm.queryArgs(queryCtx, "SELECT pg_last_wal_replay_lsn() >= $1::pg_lsn", targetLsn)
	if err != nil {
		return false, mterrors.Wrap(err, "failed to check if replay LSN reached target")
	}
	var reachedTarget bool
	if err := executor.ScanSingleRow(result, &reachedTarget); err != nil {
		return false, mterrors.Wrap(err, "failed to scan LSN comparison result")
	}
	return reachedTarget, nil
}

// queryReplicationStatus queries PostgreSQL for all replication status fields.
// This method handles NULL values properly for LSN fields that may be NULL
// when not in recovery mode or when no WAL has been received/replayed.
func (pm *MultiPoolerManager) queryReplicationStatus(ctx context.Context) (*multipoolermanagerdatapb.StandbyReplicationStatus, error) {
	sql := `SELECT
		pg_last_wal_replay_lsn(),
		pg_last_wal_receive_lsn(),
		pg_is_wal_replay_paused(),
		pg_get_wal_replay_pause_state(),
		pg_last_xact_replay_timestamp(),
		current_setting('primary_conninfo'),
		(SELECT status FROM pg_stat_wal_receiver LIMIT 1)`

	queryCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	result, err := pm.query(queryCtx, sql)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to query replication status")
	}

	var replayLsn *string
	var receiveLsn *string
	var isPaused bool
	var pauseState string
	var lastXactTime *string
	var primaryConnInfo string
	var walReceiverStatus *string

	err = executor.ScanSingleRow(result, &replayLsn, &receiveLsn, &isPaused, &pauseState, &lastXactTime, &primaryConnInfo, &walReceiverStatus)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to query replication status")
	}
	status := &multipoolermanagerdatapb.StandbyReplicationStatus{
		IsWalReplayPaused:   isPaused,
		WalReplayPauseState: pauseState,
	}
	if replayLsn != nil {
		status.LastReplayLsn = *replayLsn
	}
	if receiveLsn != nil {
		status.LastReceiveLsn = *receiveLsn
	}
	if lastXactTime != nil {
		status.LastXactReplayTimestamp = *lastXactTime
	}
	if walReceiverStatus != nil {
		status.WalReceiverStatus = *walReceiverStatus
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
func (pm *MultiPoolerManager) waitForReplicationPause(ctx context.Context) (*multipoolermanagerdatapb.StandbyReplicationStatus, error) {
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

// setPrimaryConnInfo sets the primary_conninfo connection string
func (pm *MultiPoolerManager) setPrimaryConnInfo(ctx context.Context, connInfo string) error {
	pm.logger.InfoContext(ctx, "Setting primary_conninfo", "conninfo", connInfo)

	execCtx, execCancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer execCancel()
	sql := "ALTER SYSTEM SET primary_conninfo = " + ast.QuoteStringLiteral(connInfo)
	if err := pm.exec(execCtx, sql); err != nil {
		pm.logger.ErrorContext(ctx, "Failed to set primary_conninfo", "error", err)
		return mterrors.Wrap(err, "failed to set primary_conninfo")
	}

	return nil
}

// resetPrimaryConnInfo clears primary_conninfo and reloads PostgreSQL configuration.
// This effectively disconnects the replica from the primary.
func (pm *MultiPoolerManager) resetPrimaryConnInfo(ctx context.Context) error {
	// Clear primary_conninfo using ALTER SYSTEM (should be quick)
	pm.logger.InfoContext(ctx, "Clearing primary_conninfo")

	execCtx, execCancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer execCancel()
	if err := pm.exec(execCtx, "ALTER SYSTEM RESET primary_conninfo"); err != nil {
		pm.logger.ErrorContext(ctx, "Failed to clear primary_conninfo", "error", err)
		return mterrors.Wrap(err, "failed to clear primary_conninfo")
	}

	// Reload PostgreSQL configuration to apply changes (should be quick)
	pm.logger.InfoContext(ctx, "Reloading PostgreSQL configuration")

	reloadCtx, reloadCancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer reloadCancel()
	if err := pm.exec(reloadCtx, "SELECT pg_reload_conf()"); err != nil {
		pm.logger.ErrorContext(ctx, "Failed to reload configuration", "error", err)
		return mterrors.Wrap(err, "failed to reload PostgreSQL configuration")
	}

	return nil
}

// writePrimaryConnInfoToFile writes primary_conninfo directly to postgresql.auto.conf
// This is used during failover recovery when postgres is not yet running and we need
// to configure replication before starting the server.
//
// After pg_rewind, postgres needs primary_conninfo configured BEFORE it starts so it
// can immediately stream WAL from the primary to reach consistent recovery state.
func (pm *MultiPoolerManager) writePrimaryConnInfoToFile(ctx context.Context, poolerDir, connInfo string) error {
	pm.logger.InfoContext(ctx, "Writing primary_conninfo to postgresql.auto.conf", "conninfo", connInfo)

	autoConfPath := poolerDir + "/pg_data/postgresql.auto.conf"

	// Read existing postgresql.auto.conf content
	content, err := os.ReadFile(autoConfPath)
	if err != nil && !os.IsNotExist(err) {
		return mterrors.Wrap(err, "failed to read postgresql.auto.conf")
	}

	// Parse existing content and update primary_conninfo
	lines := strings.Split(string(content), "\n")
	var newLines []string
	foundPrimaryConnInfo := false

	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		// Skip existing primary_conninfo lines
		if strings.HasPrefix(trimmed, "primary_conninfo") || strings.HasPrefix(trimmed, "#primary_conninfo") {
			if !foundPrimaryConnInfo {
				// Replace with new value
				newLines = append(newLines, fmt.Sprintf("primary_conninfo = '%s'", connInfo))
				foundPrimaryConnInfo = true
			}
			// Skip the old line
			continue
		}
		newLines = append(newLines, line)
	}

	// If primary_conninfo wasn't found, add it
	if !foundPrimaryConnInfo {
		newLines = append(newLines, fmt.Sprintf("primary_conninfo = '%s'", connInfo))
	}

	// Write back to file
	newContent := strings.Join(newLines, "\n")
	if err := os.WriteFile(autoConfPath, []byte(newContent), 0o644); err != nil {
		return mterrors.Wrap(err, "failed to write postgresql.auto.conf")
	}

	pm.logger.InfoContext(ctx, "Successfully wrote primary_conninfo to postgresql.auto.conf")
	return nil
}

// waitForReceiverDisconnect waits for the WAL receiver to fully disconnect after clearing primary_conninfo.
// It polls pg_stat_wal_receiver to confirm the receiver has stopped.
func (pm *MultiPoolerManager) waitForReceiverDisconnect(ctx context.Context) (*multipoolermanagerdatapb.StandbyReplicationStatus, error) {
	// Create a context with timeout for the polling loop
	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-waitCtx.Done():
			if waitCtx.Err() == context.DeadlineExceeded {
				pm.logger.ErrorContext(ctx, "Timeout waiting for WAL receiver to disconnect")
				return nil, mterrors.New(mtrpcpb.Code_DEADLINE_EXCEEDED, "timeout waiting for WAL receiver to disconnect")
			}
			pm.logger.ErrorContext(ctx, "Context cancelled while waiting for WAL receiver to disconnect")
			return nil, mterrors.Wrap(waitCtx.Err(), "context cancelled while waiting for WAL receiver to disconnect")

		case <-ticker.C:
			// Check if WAL receiver has disconnected by counting rows in pg_stat_wal_receiver
			result, err := pm.query(waitCtx, "SELECT COUNT(*) FROM pg_stat_wal_receiver")
			if err != nil {
				pm.logger.ErrorContext(ctx, "Failed to query pg_stat_wal_receiver", "error", err)
				return nil, mterrors.Wrap(err, "failed to query pg_stat_wal_receiver")
			}

			var receiverCount int64
			if err := executor.ScanSingleRow(result, &receiverCount); err != nil {
				pm.logger.ErrorContext(ctx, "Failed to scan receiver count", "error", err)
				return nil, mterrors.Wrap(err, "failed to scan pg_stat_wal_receiver count")
			}

			// Once receiver is disconnected, query final replication status
			if receiverCount == 0 {
				pm.logger.InfoContext(ctx, "WAL receiver has disconnected")

				// Get the final replication status
				status, err := pm.queryReplicationStatus(waitCtx)
				if err != nil {
					pm.logger.ErrorContext(ctx, "Failed to get replication status", "error", err)
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
func (pm *MultiPoolerManager) pauseReplication(ctx context.Context, mode multipoolermanagerdatapb.ReplicationPauseMode, wait bool) (*multipoolermanagerdatapb.StandbyReplicationStatus, error) {
	switch mode {
	case multipoolermanagerdatapb.ReplicationPauseMode_REPLICATION_PAUSE_MODE_REPLAY_ONLY:
		// Pause WAL replay on the standby
		pm.logger.InfoContext(ctx, "Pausing WAL replay on standby")

		// Set tight timeout for the pause command itself (should be quick)
		execCtx, execCancel := context.WithTimeout(ctx, 500*time.Millisecond)
		defer execCancel()

		if err := pm.exec(execCtx, "SELECT pg_wal_replay_pause()"); err != nil {
			pm.logger.ErrorContext(ctx, "Failed to pause WAL replay", "error", err)
			return nil, mterrors.Wrap(err, "failed to pause WAL replay")
		}

		if wait {
			// Wait for WAL replay to actually be paused
			// pg_wal_replay_pause() is asynchronous, so we need to wait for it to complete
			pm.logger.InfoContext(ctx, "Waiting for WAL replay to complete pausing")
			status, err := pm.waitForReplicationPause(ctx)
			if err != nil {
				return nil, err
			}
			return status, nil
		}

		return nil, nil

	case multipoolermanagerdatapb.ReplicationPauseMode_REPLICATION_PAUSE_MODE_RECEIVER_ONLY:
		// Stop the WAL receiver by clearing primary_conninfo
		pm.logger.InfoContext(ctx, "Stopping WAL receiver")

		if err := pm.resetPrimaryConnInfo(ctx); err != nil {
			return nil, err
		}

		if wait {
			// Wait for receiver to fully disconnect
			pm.logger.InfoContext(ctx, "Waiting for WAL receiver to disconnect")
			status, err := pm.waitForReceiverDisconnect(ctx)
			if err != nil {
				return nil, err
			}
			return status, nil
		}

		return nil, nil

	case multipoolermanagerdatapb.ReplicationPauseMode_REPLICATION_PAUSE_MODE_REPLAY_AND_RECEIVER:
		// IMPORTANT: Must stop receiver BEFORE pausing replay
		// Reason: When replay is paused, the WAL receiver won't disconnect even if we clear primary_conninfo
		// So we must clear primary_conninfo while replay is still running
		pm.logger.InfoContext(ctx, "Pausing both WAL replay and receiver")

		// First stop receiver (while replay is still running)
		if err := pm.resetPrimaryConnInfo(ctx); err != nil {
			return nil, err
		}

		// Wait for receiver to disconnect before pausing replay
		_, err := pm.waitForReceiverDisconnect(ctx)
		if err != nil {
			return nil, err
		}

		// Now that receiver is disconnected, pause replay
		execCtx, execCancel := context.WithTimeout(ctx, 500*time.Millisecond)
		defer execCancel()
		if err := pm.exec(execCtx, "SELECT pg_wal_replay_pause()"); err != nil {
			pm.logger.ErrorContext(ctx, "Failed to pause WAL replay", "error", err)
			return nil, mterrors.Wrap(err, "failed to pause WAL replay")
		}

		if wait {
			// Wait for replay pause to complete
			pm.logger.InfoContext(ctx, "Waiting for WAL replay to complete pausing")
			status, err := pm.waitForReplicationPause(ctx)
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

// resumeWALReplay resumes WAL replay on a standby server
func (pm *MultiPoolerManager) resumeWALReplay(ctx context.Context) error {
	pm.logger.InfoContext(ctx, "Resuming WAL replay")

	execCtx, execCancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer execCancel()

	if err := pm.exec(execCtx, "SELECT pg_wal_replay_resume()"); err != nil {
		pm.logger.ErrorContext(ctx, "Failed to resume WAL replay", "error", err)
		return mterrors.Wrap(err, "failed to resume WAL replay")
	}

	return nil
}

// reloadPostgresConfig reloads PostgreSQL configuration to apply changes made via ALTER SYSTEM
func (pm *MultiPoolerManager) reloadPostgresConfig(ctx context.Context) error {
	pm.logger.InfoContext(ctx, "Reloading PostgreSQL configuration")

	reloadCtx, reloadCancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer reloadCancel()
	if err := pm.exec(reloadCtx, "SELECT pg_reload_conf()"); err != nil {
		pm.logger.ErrorContext(ctx, "Failed to reload configuration", "error", err)
		return mterrors.Wrap(err, "failed to reload PostgreSQL configuration")
	}

	return nil
}

// validateExpectedLSN validates that the current replay LSN matches the expected LSN
func (pm *MultiPoolerManager) validateExpectedLSN(ctx context.Context, expectedLSN string) error {
	if expectedLSN == "" {
		return nil // No validation requested
	}

	queryCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	sql := "SELECT pg_last_wal_replay_lsn()::text, pg_is_wal_replay_paused()"
	result, err := pm.query(queryCtx, sql)
	if err != nil {
		pm.logger.ErrorContext(ctx, "Failed to get current replay LSN and pause state", "error", err)
		return mterrors.Wrap(err, "failed to get current replay LSN and pause state")
	}

	var currentLSN string
	var isPaused bool
	err = executor.ScanSingleRow(result, &currentLSN, &isPaused)
	if err != nil {
		return mterrors.Wrap(err, "failed to get current replay LSN")
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
			"invalid synchronous_commit level: "+synchronousCommit.String())
	}

	execCtx, execCancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer execCancel()

	pm.logger.InfoContext(ctx, "Setting synchronous_commit", "value", syncCommitValue)
	sql := fmt.Sprintf("ALTER SYSTEM SET synchronous_commit = '%s'", syncCommitValue)
	if err := pm.exec(execCtx, sql); err != nil {
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
func (pm *MultiPoolerManager) applySynchronousStandbyNames(ctx context.Context, value string) error {
	pm.logger.InfoContext(ctx, "Setting synchronous_standby_names", "value", value)

	execCtx, execCancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer execCancel()

	// ALTER SYSTEM SET doesn't support parameterized queries, so we use string formatting
	sql := "ALTER SYSTEM SET synchronous_standby_names = " + ast.QuoteStringLiteral(value)
	if err := pm.exec(execCtx, sql); err != nil {
		pm.logger.ErrorContext(ctx, "Failed to set synchronous_standby_names", "error", err)
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
		execCtx, execCancel := context.WithTimeout(ctx, 500*time.Millisecond)
		defer execCancel()

		pm.logger.InfoContext(ctx, "Clearing synchronous_standby_names (empty standby list)")
		if err := pm.exec(execCtx, "ALTER SYSTEM SET synchronous_standby_names = ''"); err != nil {
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

	// Apply the setting
	return pm.applySynchronousStandbyNames(ctx, standbyNamesValue)
}

// getSynchronousReplicationConfig retrieves and parses the current synchronous replication configuration
func (pm *MultiPoolerManager) getSynchronousReplicationConfig(ctx context.Context) (*multipoolermanagerdatapb.SynchronousReplicationConfiguration, error) {
	config := &multipoolermanagerdatapb.SynchronousReplicationConfiguration{}

	queryCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	// Query synchronous_standby_names
	result, err := pm.query(queryCtx, "SHOW synchronous_standby_names")
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to query synchronous_standby_names")
	}

	var syncStandbyNamesStr string
	if err := executor.ScanSingleRow(result, &syncStandbyNamesStr); err != nil {
		return nil, mterrors.Wrap(err, "failed to scan synchronous_standby_names")
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
	result, err = pm.query(queryCtx, "SHOW synchronous_commit")
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to query synchronous_commit")
	}

	var syncCommitStr string
	if err := executor.ScanSingleRow(result, &syncCommitStr); err != nil {
		return nil, mterrors.Wrap(err, "failed to scan synchronous_commit")
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

// clearSyncReplicationForDemotion clears synchronous replication settings at the start of demotion.
//
// When a stale primary comes back online after failover:
// 1. It still has synchronous_standby_names configured
// 2. No standbys are connected (they're all connected to the new primary)
// 3. Any writes (like heartbeat) block indefinitely waiting for sync acknowledgment
// 4. This blocks the demote flow and causes timeout
//
// ALTER SYSTEM writes to postgresql.auto.conf, not to WAL, so it doesn't need sync
// replication acknowledgment and won't block even with no standbys connected.
func (pm *MultiPoolerManager) clearSyncReplicationForDemotion(ctx context.Context) error {
	pm.logger.InfoContext(ctx, "Clearing synchronous replication for demotion (early)")

	// Use a short timeout - if this hangs, the demote will fail anyway
	execCtx, execCancel := context.WithTimeout(ctx, 5*time.Second)
	defer execCancel()

	// ALTER SYSTEM writes to postgresql.auto.conf (not WAL), so it doesn't require
	// sync replication acknowledgment and won't block.
	if err := pm.exec(execCtx, "ALTER SYSTEM SET synchronous_standby_names = ''"); err != nil {
		pm.logger.WarnContext(ctx, "Failed to clear synchronous_standby_names for demotion", "error", err)
		return mterrors.Wrap(err, "failed to clear synchronous_standby_names for demotion")
	}

	// Reload configuration to apply changes immediately
	if err := pm.exec(execCtx, "SELECT pg_reload_conf()"); err != nil {
		pm.logger.WarnContext(ctx, "Failed to reload configuration for demotion", "error", err)
		return mterrors.Wrap(err, "failed to reload configuration for demotion")
	}

	pm.logger.InfoContext(ctx, "Successfully cleared synchronous replication for demotion")
	return nil
}

// resetSynchronousReplication clears the synchronous standby list
// This should be called after the server is read-only to safely clear settings
func (pm *MultiPoolerManager) resetSynchronousReplication(ctx context.Context) error {
	pm.logger.InfoContext(ctx, "Clearing synchronous standby list")

	execCtx, execCancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer execCancel()

	// Clear synchronous_standby_names to remove all standbys
	if err := pm.exec(execCtx, "ALTER SYSTEM SET synchronous_standby_names = ''"); err != nil {
		pm.logger.ErrorContext(ctx, "Failed to clear synchronous_standby_names", "error", err)
		return mterrors.Wrap(err, "failed to clear synchronous_standby_names")
	}

	// Reload configuration to apply changes
	if err := pm.exec(execCtx, "SELECT pg_reload_conf()"); err != nil {
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

// ----------------------------------------------------------------------------
// Primary-side Replication Queries
// ----------------------------------------------------------------------------

// getConnectedFollowerIDs queries pg_stat_replication for connected followers and returns their IDs
func (pm *MultiPoolerManager) getConnectedFollowerIDs(ctx context.Context) ([]*clustermetadatapb.ID, error) {
	queryCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	sql := "SELECT application_name FROM pg_stat_replication WHERE application_name IS NOT NULL AND application_name != ''"
	result, err := pm.query(queryCtx, sql)
	if err != nil {
		pm.logger.ErrorContext(ctx, "Failed to query pg_stat_replication", "error", err)
		return nil, mterrors.Wrap(err, "failed to query connected followers")
	}

	followers := []*clustermetadatapb.ID{}
	if result != nil {
		for _, row := range result.Rows {
			appName, err := executor.GetString(row, 0)
			if err != nil {
				pm.logger.ErrorContext(ctx, "Failed to scan application_name", "error", err)
				return nil, mterrors.Wrap(err, "failed to scan application_name from pg_stat_replication")
			}
			// Parse application_name back to cluster ID
			followerID, err := parseApplicationName(appName)
			if err != nil {
				pm.logger.ErrorContext(ctx, "Failed to parse application_name", "application_name", appName, "error", err)
				return nil, mterrors.Wrap(err, "failed to parse application_name: "+appName)
			}
			followers = append(followers, followerID)
		}
	}

	return followers, nil
}

// queryFollowerReplicationStats queries pg_stat_replication for detailed replication statistics
// Returns a map of application_name -> ReplicationStats
func (pm *MultiPoolerManager) queryFollowerReplicationStats(ctx context.Context) (map[string]*multipoolermanagerdatapb.ReplicationStats, error) {
	queryCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	sql := `SELECT
		pid,
		application_name,
		client_addr::text,
		state,
		sync_state,
		sent_lsn::text,
		write_lsn::text,
		flush_lsn::text,
		replay_lsn::text,
		EXTRACT(EPOCH FROM write_lag),
		EXTRACT(EPOCH FROM flush_lag),
		EXTRACT(EPOCH FROM replay_lag)
	FROM pg_stat_replication
	WHERE application_name IS NOT NULL AND application_name != ''`

	result, err := pm.query(queryCtx, sql)
	if err != nil {
		pm.logger.ErrorContext(ctx, "Failed to query pg_stat_replication", "error", err)
		return nil, mterrors.Wrap(err, "failed to query replication status")
	}

	// Build a map of connected followers by application_name
	connectedMap := make(map[string]*multipoolermanagerdatapb.ReplicationStats)
	if result != nil {
		for _, row := range result.Rows {
			var pid int32
			var appName string
			var clientAddr string
			var state string
			var syncState string
			var sentLsn string
			var writeLsn string
			var flushLsn string
			var replayLsn string
			var writeLagSecs *float64
			var flushLagSecs *float64
			var replayLagSecs *float64

			err := executor.ScanRow(
				row,
				&pid,
				&appName,
				&clientAddr,
				&state,
				&syncState,
				&sentLsn,
				&writeLsn,
				&flushLsn,
				&replayLsn,
				&writeLagSecs,
				&flushLagSecs,
				&replayLagSecs,
			)
			if err != nil {
				pm.logger.ErrorContext(ctx, "Failed to scan replication row", "error", err)
				return nil, mterrors.Wrap(err, "failed to scan replication statistics")
			}

			stats := &multipoolermanagerdatapb.ReplicationStats{
				Pid:        pid,
				ClientAddr: clientAddr,
				State:      state,
				SyncState:  syncState,
				SentLsn:    sentLsn,
				WriteLsn:   writeLsn,
				FlushLsn:   flushLsn,
				ReplayLsn:  replayLsn,
			}

			// Convert lag values from seconds to Duration (only if not null/empty)
			// Convert lag values from seconds to Duration (only if not null)
			if writeLagSecs != nil {
				stats.WriteLag = durationpb.New(time.Duration(*writeLagSecs * float64(time.Second)))
			}
			if flushLagSecs != nil {
				stats.FlushLag = durationpb.New(time.Duration(*flushLagSecs * float64(time.Second)))
			}
			if replayLagSecs != nil {
				stats.ReplayLag = durationpb.New(time.Duration(*replayLagSecs * float64(time.Second)))
			}

			connectedMap[appName] = stats
		}
	}

	return connectedMap, nil
}
