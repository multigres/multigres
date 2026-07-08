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
	"log/slog"
	"time"

	"github.com/multigres/multigres/go/common/backup"
	"github.com/multigres/multigres/go/common/eventlog"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolermanagerdata "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	pgctldpb "github.com/multigres/multigres/go/pb/pgctldservice"
	"github.com/multigres/multigres/go/services/multipooler/internal/manager/actionlock"
	backupengine "github.com/multigres/multigres/go/services/multipooler/internal/manager/backup"
	"github.com/multigres/multigres/go/tools/executil"
	"github.com/multigres/multigres/go/tools/telemetry"
)

// Backup performs a backup
func (pm *MultipoolerManager) Backup(ctx context.Context, forcePrimary bool, backupType string, jobID string, overrides map[string]string) (string, error) {
	// We can't proceed without the topo, which is loaded asynchronously at startup
	if err := pm.checkReady(); err != nil {
		return "", err
	}

	// Acquire the local action lock first to prevent deadlock with the distributed lease.
	var err error
	lockStart := time.Now()
	ctx, err = pm.actionLock.Acquire(ctx, "Backup")
	pm.backup.Metrics().RecordBackupLockWait(ctx, time.Since(lockStart).Seconds())
	if err != nil {
		return "", err
	}
	defer pm.actionLock.Release(ctx)

	// Acquire the distributed backup lease via steal protocol.
	// If another pooler holds the lease, revoke it and acquire a new one.
	// This ensures the most recent backup request always wins.
	var backupID string
	health := pm.backup.Health()
	err = pm.topoClient.WithStolenBackupLease(ctx, pm.shardKey(), pm.record.Id().Name, "backup", pm.logger, func(ctx context.Context) error {
		// The lease is held for the duration of this function.
		health.SetLeaseHeld(true)
		defer health.SetLeaseHeld(false)

		var backupErr error
		backupID, backupErr = pm.backupLocked(ctx, forcePrimary, backupType, jobID, overrides)
		// A lease-loss abort intentionally counts as BOTH a backup failure
		// (recorded inside backupLocked) and a lease loss recorded here.
		pm.recordLeaseLossIfApplicable(ctx, backupErr)
		return backupErr
	})
	if err != nil {
		return "", mterrors.Wrap(err, "failed to acquire backup lease")
	}
	return backupID, nil
}

// recordLeaseLossIfApplicable records a lost-lease event + counter when a backup
// failed because this pooler's lease was lost mid-operation (stolen by another
// pooler or expired) — detected via the ErrLeaseLost context cause set by the
// lease monitor. Returns true if it recorded a loss.
func (pm *MultipoolerManager) recordLeaseLossIfApplicable(ctx context.Context, backupErr error) bool {
	if backupErr == nil || !errors.Is(context.Cause(ctx), topoclient.ErrLeaseLost) {
		return false
	}
	pm.backup.Metrics().IncLeaseLost(ctx)
	eventlog.Emit(ctx, pm.logger, eventlog.Failed, eventlog.BackupLeaseLost{Holder: pm.record.Id().Name})
	return true
}

// backupLocked performs a backup. Caller must hold the action lock and backup lease.
func (pm *MultipoolerManager) backupLocked(ctx context.Context, forcePrimary bool, backupType string, jobID string, overrides map[string]string) (retBackupID string, retErr error) {
	// Record the attempt and its eventual outcome up front, so a failed
	// precondition below (pooler role, backup type, primary resolution) still
	// counts as an attempt and a failure — mirroring restoreFromBackupLocked.
	// The engine performs the pgBackRest work; the manager owns these
	// operation-level metrics.
	metrics := pm.backup.Metrics()
	health := pm.backup.Health()
	metrics.IncBackupAttempts(ctx)
	health.BackupStarted()
	defer func() {
		if retErr == nil {
			metrics.IncBackupSuccesses(ctx)
			health.BackupSucceeded()
			// Reflect the just-created backup in the age/count gauges and
			// status page immediately, rather than waiting for the next poll
			// tick. This covers both the regular backup RPC and the bootstrap
			// first-backup path, which both funnel through backupLocked.
			// Synchronous + repo-only (one `pgbackrest info`, no pg queries).
			pm.backup.RefreshRepoNow(ctx)
		} else {
			metrics.IncBackupFailures(ctx)
			health.BackupFailed(retErr)
		}
	}()

	// Policy and primary-source resolution are owned by the manager (it has the
	// role and the consensus-recorded primary); the engine is pure pgBackRest.
	if err := pm.allowBackupOnPrimary(ctx, forcePrimary); err != nil {
		return "", err
	}
	// Validate the requested backup type before resolving the backup source, so
	// an invalid type is rejected up front rather than masked by a pg2 error.
	pgBackRestType, err := backupengine.ValidateBackupType(backupType)
	if err != nil {
		return "", err
	}
	pg2Args, err := pm.GetPrimaryAsPg2Args(ctx, overrides, forcePrimary)
	if err != nil {
		return "", mterrors.Wrap(err, "failed to get primary as pg2 arguments")
	}

	retErr = telemetry.WithSpan(ctx, "backup", func(ctx context.Context) error {
		var err error
		retBackupID, err = pm.backup.Backup(ctx, pgBackRestType, jobID, pg2Args, pm.getPoolerType())
		return err
	})
	return retBackupID, retErr
}

// allowBackupOnPrimary checks if a backup operation is allowed on a primary pooler
func (pm *MultipoolerManager) allowBackupOnPrimary(ctx context.Context, forcePrimary bool) error {
	poolerType := pm.getPoolerType()
	isPrimary := (poolerType == clustermetadatapb.PoolerType_PRIMARY)

	if isPrimary && !forcePrimary {
		slog.WarnContext(ctx, "Backup requested on primary database without ForcePrimary flag")
		return mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION,
			"backups from primary databases are not allowed unless ForcePrimary is set")
	}
	return nil
}

// GetPrimaryAsPg2Args returns pgbackrest CLI arguments for pg2 (primary) configuration.
//
// When backing up from PRIMARY: Returns empty slice - pgBackRest does local backup from pg1.
// When backing up from REPLICA with TLS certs: Returns TLS connection parameters and pg2-path
// (resolved from topology or overrides) to the primary's pgBackRest server.
// When backing up from REPLICA without TLS certs: Returns direct postgres connection parameters (test mode).
//
// Returns error if this is a replica pooler without primary information.
func (pm *MultipoolerManager) GetPrimaryAsPg2Args(
	ctx context.Context,
	overrides map[string]string,
	forcePrimary bool,
) ([]string, error) {
	poolerType := pm.getPoolerType()

	// Primary poolers (or forced-primary nodes, e.g. during first-backup creation)
	// backup locally from pg1 — no pg2 needed.
	if poolerType == clustermetadatapb.PoolerType_PRIMARY || forcePrimary {
		return []string{}, nil
	}

	// Replica poolers MUST have primary info to backup from primary. The
	// canonical source is consensusPromises.ReplicationPrimary, populated by
	// every RPC that informs this pooler of a primary (SetPrimary and
	// Promote's leader path for the rare self-as-primary case).
	primary := pm.consensusMgr.GetReplicationPrimary().GetPrimary()
	primaryHost := primary.GetHost()
	primaryPort := primary.GetPostgresPort()
	primaryPoolerID := primary.GetId()

	if primaryHost == "" || primaryPort == 0 {
		return nil, mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION,
			"cannot backup from replica: primary host/port not configured")
	}

	// Check if TLS certs are configured to determine mode
	caFile := pm.config.PgBackRestCAFile
	certFile := pm.config.PgBackRestCertFile
	keyFile := pm.config.PgBackRestKeyFile
	isTLSMode := (caFile != "" && certFile != "" && keyFile != "")

	var args []string

	if isTLSMode {
		// TLS mode: need to get the PRIMARY's pgBackRest port and data dir from topology
		if primaryPoolerID == nil {
			return nil, mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION,
				"primary pooler ID not available")
		}

		primaryInfo, err := pm.topoClient.GetMultipooler(ctx, primaryPoolerID)
		if err != nil {
			return nil, mterrors.Wrap(err, "failed to get primary pooler info from topology")
		}

		primaryPgBackRestPort, ok := primaryInfo.Multipooler.PortMap["pgbackrest"]
		if !ok {
			return nil, mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION,
				"primary pooler does not have pgbackrest port configured")
		}

		// pg2-path is required by pgBackRest even in TLS mode.
		// Use the override if provided, otherwise read from topology.
		pg2Path := overrides["pg2_path"]
		if pg2Path == "" {
			pg2Path = primaryInfo.Multipooler.PgDataDir
		}
		if pg2Path == "" {
			return nil, mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION,
				"primary pooler does not have pg_data_dir set in topology (required for pg2-path)")
		}

		args = []string{
			"--pg2-host=" + primaryHost,
			"--pg2-path=" + pg2Path,
			"--pg2-host-type=tls",
			fmt.Sprintf("--pg2-host-port=%d", primaryPgBackRestPort),
			"--pg2-host-ca-file=" + caFile,
			"--pg2-host-cert-file=" + certFile,
			"--pg2-host-key-file=" + keyFile,
		}
	} else {
		// Local mode: pg2_path override is REQUIRED
		pg2Path := overrides["pg2_path"]
		if pg2Path == "" {
			return nil, mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION,
				"local mode backup requires pg2_path override (TLS certs not configured)")
		}

		args = []string{
			"--pg2-host=" + primaryHost,
			fmt.Sprintf("--pg2-port=%d", primaryPort),
			"--pg2-path=" + pg2Path,
		}
	}

	// Apply any additional overrides
	args = backup.ApplyPgBackRestOverrides(args, overrides)

	return args, nil
}

// restoreFromBackupLocked restores from a backup to a standby without a data
// directory. Caller must hold the action lock.
//
// Requirements:
// - The pooler must be a standby (not a primary)
// - PGDATA must not exist (caller's responsibility to stop PostgreSQL and remove it)
//
// This function will:
// 1. Execute pgbackrest restore to recreate PGDATA
// 2. Start PostgreSQL in standby mode using Restart (which handles the not-running case)
// 3. Reopen the pooler manager to establish fresh connections
//
// Only called from the postgres monitor's autonomous self-heal
// (restoreAndStartPostgres) when it observes no data directory and backups
// available — there is no externally-callable RPC for this; an explicit,
// caller-driven restore path was found to be unusable (it would race the
// monitor's own restore the instant PGDATA goes missing) and was removed.
func (pm *MultipoolerManager) restoreFromBackupLocked(ctx context.Context, backupID string) (retErr error) {
	if err := actionlock.AssertActionLockHeld(ctx); err != nil {
		return err
	}

	metrics := pm.backup.Metrics()
	metrics.IncRestoreAttempts(ctx)
	restoreStart := time.Now()
	defer func() {
		metrics.RecordRestoreDuration(ctx, time.Since(restoreStart).Seconds())
		if retErr == nil {
			metrics.IncRestoreSuccesses(ctx)
		} else {
			metrics.IncRestoreFailures(ctx)
		}
	}()

	pm.logger.InfoContext(ctx, "Starting restore operation", "backup_id", backupID)

	eventlog.Emit(ctx, pm.logger, eventlog.Started, eventlog.RestoreAttempt{BackupName: backupID})
	defer func() {
		if retErr == nil {
			eventlog.Emit(ctx, pm.logger, eventlog.Success, eventlog.RestoreAttempt{BackupName: backupID})
		} else {
			eventlog.Emit(ctx, pm.logger, eventlog.Failed, eventlog.RestoreAttempt{BackupName: backupID}, "error", retErr)
		}
	}()

	// Check that this is a standby, not a primary
	poolerType := pm.getPoolerType()
	if poolerType == clustermetadatapb.PoolerType_PRIMARY {
		return mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION,
			"cannot restore to a primary pooler; restore is only supported for standby poolers")
	}

	// Restore is archive-based catch-up, which only an observer may do — a
	// cohort member must only ever advance via streaming from the current
	// leader (see consensus.ConsensusManager.IsPotentialCohortMember and
	// resetRestoreCommand/setRestoreCommand). This is a distinct, stronger
	// check than the PoolerType one above: a pooler can be topology-typed
	// REPLICA while still being named in the shard's current cohort.
	if pm.consensusMgr.IsPotentialCohortMember(pm.serviceID) {
		return mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION,
			"cannot restore from backup: pooler is a cohort member of the highest known rule")
	}

	// Check that PGDATA doesn't exist (caller must remove it before restore)
	if pm.hasDataDirectory() {
		return mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION,
			"cannot restore: PGDATA already exists; caller must stop PostgreSQL and remove PGDATA first")
	}

	// Restore the backup
	if err := telemetry.WithSpan(ctx, "restore/pgbackrest", func(ctx context.Context) error {
		return pm.backup.Restore(ctx, backupID)
	}); err != nil {
		return err
	}

	// Reconfigure archive_command to use this pooler's local pgbackrest.conf
	// After restore, postgresql.auto.conf contains archive_command pointing to the
	// primary's pgbackrest.conf path. Each pooler needs its own config path.
	pm.logger.InfoContext(ctx, "Reconfiguring archive_command for local pgbackrest.conf")
	if err := telemetry.WithSpan(ctx, "restore/reconfigure-archive", func(ctx context.Context) error {
		if err := pm.backup.RemoveArchiveConfig(); err != nil {
			return mterrors.Wrap(err, "failed to remove old archive configuration")
		}
		return mterrors.Wrap(pm.backup.ConfigureArchiveMode(ctx), "failed to configure archive mode")
	}); err != nil {
		return err
	}

	// Wrap restore_command (pgbackrest's own, just written by Restore above)
	// with `pgctld restore-wrapper`, so a later Recruit/monitor stop request
	// can confirm whether it's still running and terminate it if needed (see
	// StopRestoreCommand). Direct file manipulation, not SQL: postgres hasn't
	// started yet at this point.
	if err := telemetry.WithSpan(ctx, "restore/wrap-restore-command", func(ctx context.Context) error {
		return mterrors.Wrap(
			pm.backup.WrapRestoreCommand(func(raw string) string {
				return wrapRestoreCommand(pm.record.PoolerDir(), raw)
			}),
			"failed to wrap restore_command")
	}); err != nil {
		return err
	}

	if err := telemetry.WithSpan(ctx, "restore/start-postgres", func(ctx context.Context) error {
		return pm.startPostgreSQLAfterRestore(ctx, backupID)
	}); err != nil {
		return err
	}

	if err := telemetry.WithSpan(ctx, "restore/reopen-pooler", func(ctx context.Context) error {
		return pm.reopenPoolerManager(ctx)
	}); err != nil {
		return err
	}

	// Best-effort refresh of the rule observation cache. The restore restarted
	// postgres with the backup's current_rule row and reopenPoolerManager just
	// re-established the query-service connection, so observe the position now
	// rather than leaving the cache stale until the next monitor tick. A failure
	// here is non-fatal — the next ObservePosition will refresh it.
	if _, err := pm.consensusMgr.Rules().ObservePosition(ctx); err != nil {
		pm.logger.WarnContext(ctx, "Could not refresh rule observation after restore", "error", err)
	}

	// Mark as initialized after successful restore
	return telemetry.WithSpan(ctx, "restore/mark-initialized", func(_ context.Context) error {
		if err := pm.setInitialized(); err != nil {
			return err
		}
		// Push an immediate health snapshot so the orchestrator learns about
		// IsInitialized=true without waiting for the next 30-second heartbeat.
		pm.broadcastHealth()
		return nil
	})
}

func (pm *MultipoolerManager) startPostgreSQLAfterRestore(ctx context.Context, backupID string) error {
	pgctldClient := pm.getPgCtldClient()
	if pgctldClient == nil {
		return mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "pgctld_client is required")
	}

	restartCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()

	slog.InfoContext(ctx, "Starting PostgreSQL after restore",
		"backup_id", backupID)

	_, err := pgctldClient.Restart(restartCtx, &pgctldpb.RestartRequest{
		AsStandby: true,
	})
	if err != nil {
		return mterrors.New(mtrpcpb.Code_INTERNAL,
			fmt.Sprintf("failed to start PostgreSQL after restore: %v", err))
	}

	slog.InfoContext(ctx, "PostgreSQL started successfully after restore")
	return nil
}

func (pm *MultipoolerManager) reopenPoolerManager(ctx context.Context) error {
	slog.InfoContext(ctx, "Reopening pooler manager after restore")
	// Use reopenConnections instead of Pause/Open to avoid canceling pm.ctx.
	// This is important during auto-restore at startup where the startup flow
	// is waiting on contexts derived from pm.ctx.
	pm.reopenConnections(ctx)
	slog.InfoContext(ctx, "Pooler manager reopened successfully after restore")
	return nil
}

// GetBackups retrieves backup information
func (pm *MultipoolerManager) GetBackups(ctx context.Context, limit uint32) ([]*multipoolermanagerdata.BackupMetadata, error) {
	// We can't proceed without the topo, which is loaded asynchronously at startup
	if err := pm.checkReady(); err != nil {
		return nil, err
	}

	// Acquire the action lock to ensure only one operation runs at a time
	var err error
	ctx, err = pm.actionLock.Acquire(ctx, "GetBackups")
	if err != nil {
		return nil, err
	}
	defer pm.actionLock.Release(ctx)

	if err := actionlock.AssertActionLockHeld(ctx); err != nil {
		return nil, err
	}
	return pm.backup.List(ctx, limit)
}

// GetBackupByJobId searches for a backup with the given job_id annotation.
// Returns nil Backup if not found.
func (pm *MultipoolerManager) GetBackupByJobId(ctx context.Context, jobID string) (*multipoolermanagerdata.BackupMetadata, error) {
	if jobID == "" {
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "job_id is required")
	}

	pm.logger.DebugContext(ctx, "Searching for backup by job_id", "job_id", jobID)

	// We can't proceed without the topo, which is loaded asynchronously at startup
	if err := pm.checkReady(); err != nil {
		return nil, err
	}

	// Acquire the action lock to ensure only one operation runs at a time
	var err error
	ctx, err = pm.actionLock.Acquire(ctx, "GetBackupByJobId")
	if err != nil {
		return nil, err
	}
	defer pm.actionLock.Release(ctx)

	// Get all backups and search for matching job_id
	backups, err := pm.backup.List(ctx, 0) // 0 = no limit
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to get backups")
	}

	for _, backup := range backups {
		if backup.JobId == jobID {
			pm.logger.DebugContext(ctx, "Found backup by job_id",
				"job_id", jobID,
				"backup_id", backup.BackupId,
				"status", backup.Status)
			return backup, nil
		}
	}

	pm.logger.DebugContext(ctx, "Backup not found by job_id", "job_id", jobID)
	return nil, nil
}

// runLongCommand executes a long-running command with periodic progress logging.
// Logs progress every 10 seconds. The cmd should be created with exec.CommandContext(ctx, ...)
// to ensure proper cleanup on context cancellation.
func (pm *MultipoolerManager) runLongCommand(ctx context.Context, cmd *executil.Cmd, operationName string) ([]byte, error) {
	pm.logger.InfoContext(ctx, "Starting command", "operation", operationName)

	startTime := time.Now()

	// Create a context for the logging goroutine
	logCtx, cancelLog := context.WithCancel(ctx)

	// Log progress periodically in background
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-logCtx.Done():
				return
			case <-ticker.C:
				elapsed := time.Since(startTime)
				pm.logger.InfoContext(ctx, "Command still in progress",
					"operation", operationName,
					"elapsed_seconds", int(elapsed.Seconds()))
			}
		}
	}()

	output, err := cmd.CombinedOutput()

	cancelLog()

	// Log completion
	elapsed := time.Since(startTime)
	if err != nil {
		pm.logger.ErrorContext(ctx, "Command failed",
			"operation", operationName,
			"elapsed_seconds", int(elapsed.Seconds()),
			"error", err)
	} else {
		pm.logger.InfoContext(ctx, "Command completed",
			"operation", operationName,
			"elapsed_seconds", int(elapsed.Seconds()))
	}

	return output, err
}

// ExpireBackups runs pgbackrest expire to remove backups that exceed the
// configured retention policy. This is safe to call at any time.
// Returns the IDs of backups that were removed.
func (pm *MultipoolerManager) ExpireBackups(ctx context.Context, overrides map[string]string) ([]string, error) {
	if err := pm.checkReady(); err != nil {
		return nil, err
	}

	// Acquire the action lock to ensure only one mutation runs at a time
	var err error
	ctx, err = pm.actionLock.Acquire(ctx, "ExpireBackups")
	if err != nil {
		return nil, err
	}
	defer pm.actionLock.Release(ctx)

	// Acquire the distributed backup lease (non-stealing).
	// Expire must not run concurrently with backup/stanza-create on any node.
	// Uses WithBackupLease (not WithStolenBackupLease) because expire should
	// not preempt an in-progress backup — it can wait or fail fast.
	var expiredIDs []string
	err = pm.topoClient.WithBackupLease(ctx, pm.shardKey(), pm.record.Id().Name, "expire", pm.logger, func(ctx context.Context) error {
		var expireErr error
		expiredIDs, expireErr = pm.backup.Expire(ctx, overrides)
		return expireErr
	})
	return expiredIDs, err
}

// VerifyBackups runs a full-stanza pgbackrest verify, validating every backup
// file and WAL segment in the repository. It delegates to the backup engine;
// see backup.Engine.Verify for the concurrency and error semantics.
func (pm *MultipoolerManager) VerifyBackups(ctx context.Context) (*backupengine.VerifyResult, error) {
	if err := pm.checkReady(); err != nil {
		return nil, err
	}
	return pm.backup.Verify(ctx)
}
