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
	"errors"
	"fmt"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor"
	backupengine "github.com/multigres/multigres/go/services/multipooler/internal/manager/backup"
	"github.com/multigres/multigres/go/services/multipooler/internal/manager/consensus"
	"github.com/multigres/multigres/go/tools/retry"

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

// archiverStats reads pg_stat_archiver for the backup-health poller. NULL
// timestamps map to zero time.Time values. It reuses the manager's query path
// (no new connection pool) and is injected into the backup engine via
// SetArchiverStatsProvider.
func (pm *MultiPoolerManager) archiverStats(ctx context.Context) (backupengine.ArchiverStats, error) {
	queryCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	sql := `SELECT
		COALESCE(EXTRACT(EPOCH FROM last_archived_time)::bigint, 0) AS last_archived,
		COALESCE(EXTRACT(EPOCH FROM last_failed_time)::bigint, 0)   AS last_failed,
		failed_count
	FROM pg_stat_archiver`
	result, err := pm.query(queryCtx, sql)
	if err != nil {
		return backupengine.ArchiverStats{}, mterrors.Wrap(err, "failed to query pg_stat_archiver")
	}

	var lastArchived, lastFailed, failedCount int64
	if err := executor.ScanSingleRow(result, &lastArchived, &lastFailed, &failedCount); err != nil {
		return backupengine.ArchiverStats{}, mterrors.Wrap(err, "failed to scan pg_stat_archiver result")
	}

	stats := backupengine.ArchiverStats{FailedCount: failedCount}
	if lastArchived > 0 {
		stats.LastArchived = time.Unix(lastArchived, 0)
	}
	if lastFailed > 0 {
		stats.LastFailed = time.Unix(lastFailed, 0)
	}
	return stats, nil
}

// backupSettings reads the backup-relevant PostgreSQL settings, used both by
// the backup-health poller and to capture server_version for the backup-time
// pg_version annotation. These are cheap pg_settings reads (no forced I/O). It
// reuses the manager's query path and is injected into the backup engine via
// SetPGSettingsProvider.
func (pm *MultiPoolerManager) backupSettings(ctx context.Context) (backupengine.PGSettings, error) {
	queryCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	sql := `SELECT
		COALESCE(current_setting('archive_command', true), '') AS archive_command,
		COALESCE(current_setting('archive_mode', true), '')    AS archive_mode,
		COALESCE(current_setting('restore_command', true), '') AS restore_command,
		COALESCE(split_part(current_setting('server_version', true), ' ', 1), '') AS server_version`
	result, err := pm.query(queryCtx, sql)
	if err != nil {
		return backupengine.PGSettings{}, mterrors.Wrap(err, "failed to query backup settings")
	}

	var archiveCommand, archiveMode, restoreCommand, serverVersion string
	if err := executor.ScanSingleRow(result, &archiveCommand, &archiveMode, &restoreCommand, &serverVersion); err != nil {
		return backupengine.PGSettings{}, mterrors.Wrap(err, "failed to scan backup settings result")
	}
	return backupengine.PGSettings{
		ArchiveCommand: archiveCommand,
		ArchiveMode:    archiveMode,
		RestoreCommand: restoreCommand,
		ServerVersion:  serverVersion,
	}, nil
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

// rewindSourceReady reports whether this pooler is safe to pg_rewind from: it is
// a primary (not in recovery) AND its last completed checkpoint is on its current
// running timeline. The check matters because pg_rewind copies the source's
// checkpoint-timeline into the target's minRecoveryPoint; a freshly promoted
// primary runs on a new timeline but its lazy post-promotion checkpoint may not
// have rewritten the control file yet, so it would hand a diverged follower a
// stale timeline that FATALs on startup. Compares pg_control_checkpoint().
// timeline_id against the running timeline parsed from the current WAL filename.
//
// Returns false (not an error) on a standby: pg_current_wal_lsn() errors during
// recovery, so the CASE guards it — a standby is never a rewind source.
func (pm *MultiPoolerManager) rewindSourceReady(ctx context.Context) (bool, error) {
	queryCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	const sql = `SELECT CASE WHEN pg_is_in_recovery() THEN false ELSE
		(SELECT timeline_id FROM pg_control_checkpoint())
		= ('x' || substring(pg_walfile_name(pg_current_wal_lsn()) from 1 for 8))::bit(32)::int
	END`
	result, err := pm.query(queryCtx, sql)
	if err != nil {
		return false, mterrors.Wrap(err, "failed to query rewind-source readiness")
	}
	var ready bool
	if err := executor.ScanSingleRow(result, &ready); err != nil {
		return false, mterrors.Wrap(err, "failed to scan rewind-source readiness result")
	}
	return ready, nil
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

// sqlGetReplicationStatus is the SQL query to retrieve all relevant replication
// status fields from pg_stat_wal_receiver. This is used by
// queryReplicationStatus to get a comprehensive view of the replication state
// in one query. Note that some fields may be NULL depending on the state of the
// standby (e.g., if not in recovery or if no WAL has been received).
//
// Scalar subqueries are used for pg_stat_wal_receiver fields so that NULL is
// returned when the view is empty (e.g., on the primary or when the WAL
// receiver is not running), rather than returning zero rows.
const sqlGetReplicationStatus = `
SELECT	pg_last_wal_replay_lsn(),
		pg_last_wal_receive_lsn(),
		pg_is_wal_replay_paused(),
		pg_get_wal_replay_pause_state(),
		pg_last_xact_replay_timestamp(),
		current_setting('primary_conninfo'),
		(SELECT status FROM pg_stat_wal_receiver LIMIT 1),
		(SELECT last_msg_receipt_time FROM pg_stat_wal_receiver LIMIT 1),
		current_setting('wal_receiver_status_interval'),
		current_setting('wal_receiver_timeout')
`

// queryReplicationStatus queries PostgreSQL for all replication status fields.
// This method handles NULL values properly for LSN fields that may be NULL
// when not in recovery mode or when no WAL has been received/replayed.
func (pm *MultiPoolerManager) queryReplicationStatus(ctx context.Context) (*multipoolermanagerdatapb.StandbyReplicationStatus, error) {
	queryCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	result, err := pm.query(queryCtx, sqlGetReplicationStatus)
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
	var lastMsgReceiveTime *time.Time
	var walReceiverStatusInterval *string
	var walReceiverTimeout *string

	err = executor.ScanSingleRow(result, &replayLsn, &receiveLsn, &isPaused, &pauseState, &lastXactTime, &primaryConnInfo, &walReceiverStatus, &lastMsgReceiveTime, &walReceiverStatusInterval, &walReceiverTimeout)
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

	if lastMsgReceiveTime != nil {
		status.LastMsgReceiveTime = timestamppb.New(*lastMsgReceiveTime)
	}

	if walReceiverStatusInterval != nil {
		// We can use ParseDuration here since PostgreSQL interval settings are
		// in a format compatible with Go durations (e.g., "10s", "500ms").
		if d, err := time.ParseDuration(*walReceiverStatusInterval); err == nil {
			status.WalReceiverStatusInterval = durationpb.New(d)
		}
	}

	if walReceiverTimeout != nil {
		// We can use ParseDuration here since PostgreSQL interval settings are
		// in a format compatible with Go durations (e.g., "10s", "500ms").
		if d, err := time.ParseDuration(*walReceiverTimeout); err == nil {
			status.WalReceiverTimeout = durationpb.New(d)
		}
	}

	// Parse primary_conninfo into structured format
	parsedConnInfo, err := parseAndRedactPrimaryConnInfo(primaryConnInfo)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to parse primary_conninfo")
	}
	status.PrimaryConnInfo = parsedConnInfo

	return status, nil
}

// pollForReplicationStatus polls poll on a 10s budget until it reports done,
// returning the accompanying status. The first attempt runs immediately and
// subsequent attempts back off exponentially from a small base: a state that
// settles within milliseconds (the common case for these replication waits)
// returns promptly instead of paying a fixed poll interval on the failover
// critical path, while the backoff avoids hammering postgres when it takes
// longer.
//
// poll returns (status, done, err): done==true stops the loop and returns
// status; a non-nil err aborts immediately, except that an err observed after
// the context expired mid-poll is reported as the canonical timeout. On
// budget/context exhaustion, onTimeout (if non-nil) is invoked for diagnostics
// and a canonical DEADLINE_EXCEEDED (timeoutMsg) or wrapped cancellation
// (cancelMsg) is returned.
func (pm *MultiPoolerManager) pollForReplicationStatus(
	ctx context.Context,
	timeoutMsg, cancelMsg string,
	onTimeout func(cause error),
	poll func(ctx context.Context) (status *multipoolermanagerdatapb.StandbyReplicationStatus, done bool, err error),
) (*multipoolermanagerdatapb.StandbyReplicationStatus, error) {
	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	fail := func(cause error) (*multipoolermanagerdatapb.StandbyReplicationStatus, error) {
		if onTimeout != nil {
			onTimeout(cause)
		}
		if errors.Is(cause, context.DeadlineExceeded) {
			return nil, mterrors.New(mtrpcpb.Code_DEADLINE_EXCEEDED, timeoutMsg)
		}
		return nil, mterrors.Wrap(cause, cancelMsg)
	}

	r := retry.New(5*time.Millisecond, 500*time.Millisecond)
	for _, err := range r.Attempts(waitCtx) {
		if err != nil {
			return fail(err)
		}
		status, done, pErr := poll(waitCtx)
		if pErr != nil {
			// Surface the cleaner timeout cause if waitCtx expired mid-poll
			// instead of an opaque "pool ctx expired" wrapper.
			if waitErr := waitCtx.Err(); waitErr != nil {
				return fail(waitErr)
			}
			return nil, pErr
		}
		if done {
			return status, nil
		}
	}

	// Attempts only stops yielding when waitCtx is done, which is handled by the
	// err branch above; this is unreachable but required for the compiler.
	return fail(waitCtx.Err())
}

// waitForReplicationPause polls until WAL replay is paused and returns the status at that moment.
// This ensures the LSN returned represents the exact point at which replication stopped.
func (pm *MultiPoolerManager) waitForReplicationPause(ctx context.Context) (*multipoolermanagerdatapb.StandbyReplicationStatus, error) {
	return pm.pollForReplicationStatus(ctx,
		"timeout waiting for WAL replay to pause",
		"context cancelled while waiting for WAL replay to pause",
		func(cause error) {
			if errors.Is(cause, context.DeadlineExceeded) {
				pm.logger.ErrorContext(ctx, "Timeout waiting for WAL replay to pause")
			} else {
				pm.logger.ErrorContext(ctx, "Context cancelled while waiting for WAL replay to pause")
			}
		},
		func(waitCtx context.Context) (*multipoolermanagerdatapb.StandbyReplicationStatus, bool, error) {
			// pg_wal_replay_pause() is asynchronous, so poll until it takes effect.
			status, err := pm.queryReplicationStatus(waitCtx)
			if err != nil {
				pm.logger.ErrorContext(ctx, "Failed to get replication status", "error", err)
				return nil, false, err
			}
			if !status.IsWalReplayPaused {
				return nil, false, nil
			}
			// Once paused, we have the exact state at the moment replication stopped.
			pm.logger.InfoContext(ctx, "WAL replay is now paused",
				"last_replay_lsn", status.LastReplayLsn,
				"last_receive_lsn", status.LastReceiveLsn,
				"pause_state", status.WalReplayPauseState)
			return status, true, nil
		})
}

// readPrimaryConnInfo returns the current primary_conninfo setting as a raw string.
// Returns an empty string if primary_conninfo is not set.
func (pm *MultiPoolerManager) readPrimaryConnInfo(ctx context.Context) (string, error) {
	queryCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	result, err := pm.query(queryCtx, "SELECT current_setting('primary_conninfo', true)")
	if err != nil {
		return "", mterrors.Wrap(err, "failed to read primary_conninfo")
	}
	var connInfo *string
	if err := executor.ScanSingleRow(result, &connInfo); err != nil {
		return "", mterrors.Wrap(err, "failed to scan primary_conninfo")
	}
	if connInfo == nil {
		return "", nil
	}
	return *connInfo, nil
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

	return pm.reloadPostgresConfig(ctx)
}

// waitForReplayStabilize waits, best effort, for WAL replay to stop making
// observable progress. The intent is to approximate replay is idle given the WAL
// that is currently available to this standby.
//
// WARNING: This function is not perfect and has some theoretical limitations.
// See decision: 2026-02-12-wait-for-replay-stabilize-during-revoke.md for more context.
func (pm *MultiPoolerManager) waitForReplayStabilize(ctx context.Context) (*multipoolermanagerdatapb.StandbyReplicationStatus, error) {
	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	// TODO: short-circuit when replay has provably caught up. On the recruit path
	// the receiver is stopped before this runs, so the received LSN is fixed; if
	// pg_last_wal_replay_lsn() == pg_last_wal_receive_lsn() we are maximally
	// applied and can return immediately instead of waiting out requiredStablePolls.
	// That needs queryReplayState to also return the receive LSN. Until then the
	// stability heuristic below is a safe (if slightly slower) fallback.
	//
	// requiredStablePolls: number of consecutive polls showing the same replay_lsn
	// before we declare stability. At 10ms per tick, 3 polls = 30ms of stability.
	const requiredStablePolls = 3
	var prevReplayLsn string
	consecutive := 0

	for {
		select {
		case <-waitCtx.Done():
			if waitCtx.Err() == context.DeadlineExceeded {
				return nil, mterrors.New(mtrpcpb.Code_DEADLINE_EXCEEDED, "timeout waiting for WAL replay to stabilize")
			}
			return nil, mterrors.Wrap(waitCtx.Err(), "context cancelled while waiting for replay to stabilize")

		case <-ticker.C:
			replayLsn, isPaused, err := pm.queryReplayState(waitCtx)
			if err != nil {
				return nil, err
			}

			if isPaused {
				return nil, mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION,
					"WAL replay is paused during revoke — unexpected state")
			}

			if replayLsn == prevReplayLsn {
				consecutive++
			} else {
				consecutive = 1
			}
			prevReplayLsn = replayLsn

			if consecutive >= requiredStablePolls {
				pm.logger.InfoContext(ctx, "WAL replay stabilized (maximally applied)",
					"replay_lsn", replayLsn)

				status, err := pm.queryReplicationStatus(waitCtx)
				if err != nil {
					return nil, err
				}
				return status, nil
			}
		}
	}
}

// queryReplayState returns the current replay LSN and pause state.
// Returns FAILED_PRECONDITION if the server is not in recovery (replay LSN is NULL).
func (pm *MultiPoolerManager) queryReplayState(ctx context.Context) (replayLsn string, isPaused bool, err error) {
	queryCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	result, err := pm.query(queryCtx, "SELECT pg_last_wal_replay_lsn(), pg_is_wal_replay_paused()")
	if err != nil {
		return "", false, mterrors.Wrap(err, "failed to query replay state")
	}

	var lsn *string
	if err := executor.ScanSingleRow(result, &lsn, &isPaused); err != nil {
		return "", false, mterrors.Wrap(err, "failed to scan replay state")
	}
	if lsn == nil {
		return "", false, mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION,
			"pg_last_wal_replay_lsn is NULL (not in recovery) — unexpected during revoke")
	}
	return *lsn, isPaused, nil
}

// waitForReceiverDisconnect waits for the WAL receiver to fully disconnect after clearing primary_conninfo.
// It polls pg_stat_wal_receiver to confirm the receiver has stopped.
func (pm *MultiPoolerManager) waitForReceiverDisconnect(ctx context.Context) (*multipoolermanagerdatapb.StandbyReplicationStatus, error) {
	// Track the latest poll snapshot so a timeout has diagnostic detail without
	// needing another query after the ctx has already expired.
	var (
		lastCount    int64 = -1 // -1 = not yet polled
		lastStatus   string
		lastConnInfo string
	)

	return pm.pollForReplicationStatus(ctx,
		"timeout waiting for WAL receiver to disconnect",
		"context cancelled while waiting for WAL receiver to disconnect",
		func(cause error) {
			pm.logger.ErrorContext(ctx, "WAL receiver did not disconnect",
				"cause", cause,
				"last_receiver_count", lastCount,
				"last_walreceiver_status", lastStatus,
				"last_primary_conninfo", lastConnInfo)
		},
		func(waitCtx context.Context) (*multipoolermanagerdatapb.StandbyReplicationStatus, bool, error) {
			// Pull the count, the walreceiver status, and the live primary_conninfo
			// in a single query so each poll is also a diagnostic snapshot.
			result, err := pm.query(waitCtx, `SELECT
				(SELECT COUNT(*) FROM pg_stat_wal_receiver),
				coalesce((SELECT status FROM pg_stat_wal_receiver), ''),
				current_setting('primary_conninfo')`)
			if err != nil {
				pm.logger.ErrorContext(ctx, "Failed to query pg_stat_wal_receiver", "error", err)
				return nil, false, mterrors.Wrap(err, "failed to query pg_stat_wal_receiver")
			}
			if err := executor.ScanSingleRow(result, &lastCount, &lastStatus, &lastConnInfo); err != nil {
				pm.logger.ErrorContext(ctx, "Failed to scan pg_stat_wal_receiver row", "error", err)
				return nil, false, mterrors.Wrap(err, "failed to scan pg_stat_wal_receiver row")
			}

			// Done when either the walreceiver slot is gone, OR it's sitting
			// in WALRCV_WAITING with primary_conninfo empty. The latter is
			// safe because:
			//
			//   - We hold the action lock, so no other in-process path can
			//     write primary_conninfo during this wait.
			//   - WAITING → STREAMING requires the startup process to call
			//     RequestXLogStreaming, which only fires when primary_conninfo
			//     is non-empty.
			//   - We can't actively terminate a walreceiver from SQL —
			//     pg_terminate_backend only works on regular backends, not
			//     auxiliary processes like the walreceiver. The walreceiver
			//     only exits when its in-flight libpq call returns, which is
			//     bounded by connect_timeout (potentially tens of seconds).
			//     Treating WAITING+empty as done lets us proceed without
			//     waiting out that timeout.
			done := lastCount == 0 || (lastStatus == "waiting" && lastConnInfo == "")
			if !done {
				return nil, false, nil
			}
			pm.logger.InfoContext(ctx, "WAL receiver has disconnected",
				"last_receiver_count", lastCount,
				"last_walreceiver_status", lastStatus)

			// Get the final replication status
			status, err := pm.queryReplicationStatus(waitCtx)
			if err != nil {
				pm.logger.ErrorContext(ctx, "Failed to get replication status", "error", err)
				return nil, false, err
			}
			return status, true, nil
		})
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

// reloadPostgresConfig is a convenience wrapper around the package-level
// reloadPostgresConfig helper bound to this manager's query service and logger.
func (pm *MultiPoolerManager) reloadPostgresConfig(ctx context.Context) error {
	return consensus.ReloadPostgresConfig(ctx, pm.logger, pm.internalQueryService())
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
// Application names are generated from multipooler IDs using the shared consensus.NewReplicaID helper
func (pm *MultiPoolerManager) setSynchronousStandbyNames(ctx context.Context, synchronousMethod multipoolermanagerdatapb.SynchronousMethod, numSync int32, names []consensus.ReplicaID) error {
	// If standby list is empty, clear synchronous_standby_names
	if len(names) == 0 {
		execCtx, execCancel := context.WithTimeout(ctx, 500*time.Millisecond)
		defer execCancel()

		pm.logger.InfoContext(ctx, "Clearing synchronous_standby_names (empty standby list)")
		if err := pm.exec(execCtx, "ALTER SYSTEM RESET synchronous_standby_names"); err != nil {
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
	standbyNamesValue, err := consensus.BuildSynchronousStandbyNamesValue(synchronousMethod, numSync, names)
	if err != nil {
		return err
	}

	// Apply the setting
	return pm.applySynchronousStandbyNames(ctx, standbyNamesValue)
}

// parseSyncReplicationConfig builds the synchronous replication configuration
// from the raw synchronous_standby_names and synchronous_commit GUC values. It
// is a pure function (no query) so the parsing rules are unit-testable directly;
// the caller (getPrimaryStatusInternal) reads the GUCs.
func parseSyncReplicationConfig(syncStandbyNames, syncCommit string) (*multipoolermanagerdatapb.SynchronousReplicationConfiguration, error) {
	config := &multipoolermanagerdatapb.SynchronousReplicationConfiguration{}

	// Only parse standby names if not empty
	syncStandbyNames = strings.TrimSpace(syncStandbyNames)
	if syncStandbyNames != "" {
		syncConfig, err := parseSynchronousStandbyNames(syncStandbyNames)
		if err != nil {
			return nil, err
		}
		config.SynchronousMethod = syncConfig.Method
		config.NumSync = syncConfig.NumSync
		config.StandbyIds = syncConfig.StandbyIDs
		appNames, err := consensus.ToReplicaIDs(syncConfig.StandbyIDs)
		if err != nil {
			return nil, mterrors.Wrap(err, "failed to convert standby IDs to application names")
		}
		config.StandbyApplicationNames = consensus.ReplicaIDsToAppNames(appNames)
	}

	// Map synchronous_commit string to enum
	var syncCommitLevel multipoolermanagerdatapb.SynchronousCommitLevel
	switch strings.ToLower(syncCommit) {
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
			fmt.Sprintf("unknown synchronous_commit value: %q", syncCommit))
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
	if err := pm.exec(execCtx, "ALTER SYSTEM RESET synchronous_standby_names"); err != nil {
		pm.logger.WarnContext(ctx, "Failed to clear synchronous_standby_names for demotion", "error", err)
		return mterrors.Wrap(err, "failed to clear synchronous_standby_names for demotion")
	}

	if err := pm.reloadPostgresConfig(ctx); err != nil {
		return mterrors.Wrap(err, "failed to reload configuration for demotion")
	}

	pm.logger.InfoContext(ctx, "Successfully cleared synchronous replication for demotion")
	return nil
}

// ----------------------------------------------------------------------------
// standbyUpdateOperationName maps a CohortUpdateOperation enum to a short string for logging/history.
func standbyUpdateOperationName(op multipoolermanagerdatapb.CohortUpdateOperation) string {
	switch op {
	case multipoolermanagerdatapb.CohortUpdateOperation_COHORT_UPDATE_OPERATION_ADD:
		return "add"
	case multipoolermanagerdatapb.CohortUpdateOperation_COHORT_UPDATE_OPERATION_REMOVE:
		return "remove"
	default:
		return "unknown"
	}
}

// Standby List Operations
// ----------------------------------------------------------------------------

// poolerIDSetEqual returns true if a and b contain the same set of pooler IDs
// (order-independent comparison using appName as the key).
func poolerIDSetEqual(a, b []consensus.ReplicaID) bool {
	if len(a) != len(b) {
		return false
	}
	m := make(map[string]struct{}, len(a))
	for _, p := range a {
		m[p.AppName()] = struct{}{}
	}
	for _, p := range b {
		if _, ok := m[p.AppName()]; !ok {
			return false
		}
	}
	return true
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
			followerID, err := consensus.ParseApplicationName(appName)
			if err != nil {
				pm.logger.ErrorContext(ctx, "Failed to parse application_name", "application_name", appName, "error", err)
				return nil, mterrors.Wrap(err, "failed to parse application_name: "+appName)
			}
			followers = append(followers, followerID)
		}
	}

	return followers, nil
}
