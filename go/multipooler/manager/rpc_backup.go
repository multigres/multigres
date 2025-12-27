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
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os/exec"
	"strings"
	"time"

	"github.com/multigres/multigres/go/common/backup"
	"github.com/multigres/multigres/go/common/mterrors"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolermanagerdata "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	pgctldpb "github.com/multigres/multigres/go/pb/pgctldservice"
	"github.com/multigres/multigres/go/tools/retry"
)

// Backup performs a backup
func (pm *MultiPoolerManager) Backup(ctx context.Context, forcePrimary bool, backupType string, jobID string) (string, error) {
	// We can't proceed without the topo, which is loaded asynchronously at startup
	if err := pm.checkReady(); err != nil {
		return "", err
	}

	// Acquire the action lock to ensure only one mutation runs at a time
	var err error
	ctx, err = pm.actionLock.Acquire(ctx, "Backup")
	if err != nil {
		return "", err
	}
	defer pm.actionLock.Release(ctx)

	return pm.backupLocked(ctx, forcePrimary, backupType, jobID)
}

// backupLocked performs a backup. Caller must hold the action lock.
func (pm *MultiPoolerManager) backupLocked(ctx context.Context, forcePrimary bool, backupType string, jobID string) (string, error) {
	if err := AssertActionLockHeld(ctx); err != nil {
		return "", err
	}

	// Check if backup is allowed on primary
	if err := pm.allowBackupOnPrimary(ctx, forcePrimary); err != nil {
		return "", err
	}

	configPath := pm.getBackupConfigPath()
	stanzaName := pm.getBackupStanza()
	tableGroup := pm.getTableGroup()
	shard := pm.getShard()
	multipoolerID, err := pm.getMultipoolerIDString()
	if err != nil {
		return "", err
	}
	multipoolerName, err := pm.getMultipoolerName()
	if err != nil {
		return "", err
	}

	// Use provided job_id or generate one (same format as multiadmin)
	effectiveJobID := jobID
	if effectiveJobID == "" {
		effectiveJobID = backup.GenerateJobID(multipoolerID)
	}

	// Validate parameters and get pgbackrest type
	pgBackRestType, err := pm.validateBackupParams(backupType, configPath, stanzaName)
	if err != nil {
		return "", err
	}

	// Execute pgbackrest backup command
	ctx, cancel := context.WithTimeout(ctx, 30*time.Minute) // Backups can take a long time
	defer cancel()

	args := []string{
		"--stanza=" + stanzaName,
		"--config=" + configPath,
		"--repo1-path=" + pm.backupLocation,
		"--type=" + pgBackRestType,
		"--log-level-console=info",
	}

	// Add annotations if table_group and shard are provided
	if tableGroup != "" {
		args = append(args, "--annotation=table_group="+tableGroup)
	}
	if shard != "" {
		args = append(args, "--annotation=shard="+shard)
	}

	// Add multipooler_id, pooler_type, and job_id annotations for unique identification
	args = append(args, "--annotation=multipooler_id="+multipoolerName)
	poolerType := pm.getPoolerType()
	args = append(args, "--annotation=pooler_type="+poolerType.String())
	args = append(args, "--annotation=job_id="+effectiveJobID)

	args = append(args, "backup")

	cmd := exec.CommandContext(ctx, "pgbackrest", args...)

	// Capture output for logging
	output, err := cmd.CombinedOutput()
	if err != nil {
		return "", mterrors.New(mtrpcpb.Code_INTERNAL,
			fmt.Sprintf("pgbackrest backup failed: %v\nOutput: %s", err, string(output)))
	}

	// Find the backup ID by querying pgbackrest info with our unique annotations
	foundBackupID, err := pm.findBackupByJobID(ctx, effectiveJobID)
	if err != nil {
		return "", mterrors.New(mtrpcpb.Code_INTERNAL,
			fmt.Sprintf("failed to find backup by annotations: %v\nBackup output: %s", err, string(output)))
	}

	// Verify the backup to ensure it's valid
	verifyCtx, verifyCancel := context.WithTimeout(ctx, 10*time.Minute)
	defer verifyCancel()

	verifyCmd := exec.CommandContext(verifyCtx, "pgbackrest",
		"--stanza="+stanzaName,
		"--config="+configPath,
		"--repo1-path="+pm.backupLocation,
		"--set="+foundBackupID,
		"--log-level-console=info",
		"verify")

	verifyOutput, verifyErr := verifyCmd.CombinedOutput()
	if verifyErr != nil {
		return "", mterrors.New(mtrpcpb.Code_INTERNAL,
			fmt.Sprintf("pgbackrest verify failed for backup %s: %v\nOutput: %s", foundBackupID, verifyErr, string(verifyOutput)))
	}
	// TODO: use `pgbackrest info` to verify that the database pages from the backed up
	// Postgres cluster pass checksum validation.

	return foundBackupID, nil
}

// RestoreFromBackup restores from a backup to a standby without a data directory.
//
// Requirements:
// - The pooler must be a standby (not a primary)
// - PGDATA must not exist (caller's responsibility to stop PostgreSQL and remove it)
//
// This function will:
// 1. Execute pgbackrest restore to recreate PGDATA
// 2. Start PostgreSQL in standby mode using Restart (which handles the not-running case)
// 3. Reopen the pooler manager to establish fresh connections
func (pm *MultiPoolerManager) RestoreFromBackup(ctx context.Context, backupID string) error {
	slog.InfoContext(ctx, "RestoreFromBackup called", "backup_id", backupID)

	// We can't proceed without the topo, which is loaded asynchronously at startup
	if err := pm.checkReady(); err != nil {
		return err
	}

	// Acquire the action lock to ensure only one mutation runs at a time
	var err error
	ctx, err = pm.actionLock.Acquire(ctx, "RestoreFromBackup")
	if err != nil {
		return err
	}
	defer pm.actionLock.Release(ctx)

	return pm.restoreFromBackupLocked(ctx, backupID)
}

// restoreFromBackupLocked performs the restore. Caller must hold the action lock.
func (pm *MultiPoolerManager) restoreFromBackupLocked(ctx context.Context, backupID string) error {
	if err := AssertActionLockHeld(ctx); err != nil {
		return err
	}

	// Check that this is a standby, not a primary
	poolerType := pm.getPoolerType()
	if poolerType == clustermetadatapb.PoolerType_PRIMARY {
		return mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION,
			"cannot restore to a primary pooler; restore is only supported for standby poolers")
	}

	// Check that PGDATA doesn't exist (caller must remove it before restore)
	if pm.hasDataDirectory() {
		return mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION,
			"cannot restore: PGDATA already exists; caller must stop PostgreSQL and remove PGDATA first")
	}

	// Restore the backup
	if err := pm.executePgBackrestRestore(ctx, backupID); err != nil {
		return err
	}
	if err := pm.startPostgreSQLAfterRestore(ctx, backupID); err != nil {
		return err
	}
	if err := pm.reopenPoolerManager(ctx); err != nil {
		return err
	}

	// Mark as initialized after successful restore
	return pm.setInitialized()
}

func (pm *MultiPoolerManager) executePgBackrestRestore(ctx context.Context, backupID string) error {
	stanzaName := pm.getBackupStanza()
	configPath := pm.getBackupConfigPath()

	if configPath == "" {
		return mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "config_path is required")
	}
	if stanzaName == "" {
		return mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "stanza_name is required")
	}

	restoreCtx, cancel := context.WithTimeout(ctx, 30*time.Minute)
	defer cancel()

	args := []string{
		"--stanza=" + stanzaName,
		"--config=" + configPath,
		"--repo1-path=" + pm.backupLocation,
		"--log-level-console=info",
	}

	if backupID != "" {
		args = append(args, "--set="+backupID)
	}

	args = append(args, "restore")

	cmd := exec.CommandContext(restoreCtx, "pgbackrest", args...)
	output, err := safeCombinedOutput(cmd)
	if err != nil {
		return mterrors.New(mtrpcpb.Code_INTERNAL,
			fmt.Sprintf("pgbackrest restore failed: %v\nOutput: %s", err, output))
	}

	return nil
}

func (pm *MultiPoolerManager) startPostgreSQLAfterRestore(ctx context.Context, backupID string) error {
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

func (pm *MultiPoolerManager) reopenPoolerManager(ctx context.Context) error {
	slog.InfoContext(ctx, "Reopening pooler manager after restore")
	// Use reopenConnections instead of Close/Open to avoid canceling pm.ctx.
	// This is important during auto-restore at startup where the startup flow
	// is waiting on contexts derived from pm.ctx.
	if err := pm.reopenConnections(ctx); err != nil {
		slog.ErrorContext(ctx, "Failed to reopen pooler manager after restore", "error", err)
		return mterrors.Wrap(err, "failed to reopen pooler manager after restore")
	}
	slog.InfoContext(ctx, "Pooler manager reopened successfully after restore")
	return nil
}

// GetBackups retrieves backup information
func (pm *MultiPoolerManager) GetBackups(ctx context.Context, limit uint32) ([]*multipoolermanagerdata.BackupMetadata, error) {
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

	return pm.getBackupsLocked(ctx, limit)
}

// getBackupsLocked retrieves backup information. Caller must hold the action lock.
func (pm *MultiPoolerManager) getBackupsLocked(ctx context.Context, limit uint32) ([]*multipoolermanagerdata.BackupMetadata, error) {
	if err := AssertActionLockHeld(ctx); err != nil {
		return nil, err
	}

	backups, err := pm.listBackups(ctx)
	if err != nil {
		return nil, err
	}

	// Apply limit if specified
	if limit > 0 && uint32(len(backups)) > limit {
		backups = backups[:limit]
	}

	return backups, nil
}

// listBackups retrieves backup metadata from pgbackrest.
// Caller must hold the action lock.
// pm.mu is NOT required because this function only reads config fields
// (which are immutable after construction) and backupLocation (which is
// set once during startup and never modified).
func (pm *MultiPoolerManager) listBackups(ctx context.Context) ([]*multipoolermanagerdata.BackupMetadata, error) {
	configPath := pm.getBackupConfigPath()
	stanzaName := pm.getBackupStanza()

	// Validate required configuration
	if configPath == "" {
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "config_path is required")
	}
	if stanzaName == "" {
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "stanza_name is required")
	}
	if pm.backupLocation == "" {
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "backup_location is required")
	}

	// Execute pgbackrest info command with JSON output
	queryCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	cmd := exec.CommandContext(queryCtx, "pgbackrest",
		"--stanza="+stanzaName,
		"--config="+configPath,
		"--repo1-path="+pm.backupLocation,
		"--output=json",
		"--log-level-console=off", // Override console logging to prevent contaminating JSON output
		"info")

	output, err := safeCombinedOutput(cmd)
	if err != nil {
		// Handle case where stanza doesn't exist yet or config file is missing - return empty list
		if output == "" || strings.Contains(output, "does not exist") || strings.Contains(output, "unable to open missing file") {
			return []*multipoolermanagerdata.BackupMetadata{}, nil
		}
		return nil, mterrors.New(mtrpcpb.Code_INTERNAL,
			fmt.Sprintf("pgbackrest info failed: %v\nOutput: %s", err, output))
	}

	// Parse JSON output
	var infoData []pgBackRestInfo
	if err := json.Unmarshal([]byte(output), &infoData); err != nil {
		return nil, mterrors.New(mtrpcpb.Code_INTERNAL,
			fmt.Sprintf("failed to parse pgbackrest info JSON: %v", err))
	}

	if len(infoData) == 0 || len(infoData[0].Backup) == 0 {
		return []*multipoolermanagerdata.BackupMetadata{}, nil
	}

	// Get current pooler's table_group and shard for filtering
	currentTableGroup := pm.config.TableGroup
	currentShard := pm.getShardID()

	// Extract backups from the first stanza (should be the only one)
	var backups []*multipoolermanagerdata.BackupMetadata
	for _, pgBackup := range infoData[0].Backup {
		status := multipoolermanagerdata.BackupMetadata_COMPLETE
		if pgBackup.Error {
			status = multipoolermanagerdata.BackupMetadata_INCOMPLETE
		}

		// Extract table_group, shard, job_id, multipooler_id, and pooler_type from annotations
		tableGroup := ""
		shard := ""
		jobID := ""
		multipoolerID := ""
		poolerType := clustermetadatapb.PoolerType_POOLER_TYPE_UNSPECIFIED
		if pgBackup.Annotation != nil {
			tableGroup = pgBackup.Annotation["table_group"]
			shard = pgBackup.Annotation["shard"]
			jobID = pgBackup.Annotation["job_id"]
			multipoolerID = pgBackup.Annotation["multipooler_id"]
			if pt, ok := clustermetadatapb.PoolerType_value[pgBackup.Annotation["pooler_type"]]; ok {
				poolerType = clustermetadatapb.PoolerType(pt)
			}
		}

		// Defense-in-depth: skip backups that don't match this pooler's shard.
		// This check is not strictly necessary since stanzas are shard-scoped,
		// but provides an extra layer of safety.
		if tableGroup != currentTableGroup {
			pm.logger.ErrorContext(ctx, "Skipping backup with mismatched table_group",
				"backup_id", pgBackup.Label,
				"backup_table_group", tableGroup,
				"current_table_group", currentTableGroup)
			continue
		}
		if shard != currentShard {
			pm.logger.ErrorContext(ctx, "Skipping backup with mismatched shard",
				"backup_id", pgBackup.Label,
				"backup_shard", shard,
				"current_shard", currentShard)
			continue
		}

		// Extract final LSN (stop LSN) from backup
		finalLSN := ""
		if pgBackup.LSN != nil && pgBackup.LSN.Stop != "" {
			finalLSN = pgBackup.LSN.Stop
		}

		// Extract backup size if available
		var backupSizeBytes uint64
		if pgBackup.Info != nil {
			backupSizeBytes = pgBackup.Info.Size
		}

		backups = append(backups, &multipoolermanagerdata.BackupMetadata{
			BackupId:        pgBackup.Label,
			Status:          status,
			TableGroup:      tableGroup,
			Shard:           shard,
			FinalLsn:        finalLSN,
			JobId:           jobID,
			BackupSizeBytes: backupSizeBytes,
			Type:            pgBackup.Type,
			MultipoolerId:   multipoolerID,
			PoolerType:      poolerType,
		})
	}

	return backups, nil
}

// pgBackRestInfo represents the structure of pgbackrest info JSON output
type pgBackRestInfo struct {
	Name   string             `json:"name"`
	Backup []pgBackRestBackup `json:"backup"`
}

// pgBackRestBackup represents a single backup in the info output
type pgBackRestBackup struct {
	Label      string              `json:"label"`
	Type       string              `json:"type"`
	Error      bool                `json:"error"`
	Timestamp  pgBackRestTimestamp `json:"timestamp"`
	Annotation map[string]string   `json:"annotation,omitempty"`
	LSN        *pgBackRestLSN      `json:"lsn,omitempty"` // LSN range for this backup
	Info       *pgBackRestSizeInfo `json:"info,omitempty"`
}

// pgBackRestSizeInfo represents size information for a backup
type pgBackRestSizeInfo struct {
	Size       uint64                    `json:"size"`       // Original database size in bytes
	Repository *pgBackRestRepositorySize `json:"repository"` // Compressed size in repository
}

// pgBackRestRepositorySize represents repository size info
type pgBackRestRepositorySize struct {
	Size uint64 `json:"size"` // Compressed size in repository in bytes
}

// pgBackRestTimestamp represents backup timestamps
type pgBackRestTimestamp struct {
	Start int64 `json:"start"`
	Stop  int64 `json:"stop"`
}

// pgBackRestLSN represents LSN start/stop positions for a backup
type pgBackRestLSN struct {
	Start string `json:"start"` // e.g., "0/21000028"
	Stop  string `json:"stop"`  // e.g., "0/21000100"
}

// allowBackupOnPrimary checks if a backup operation is allowed on a primary pooler
func (pm *MultiPoolerManager) allowBackupOnPrimary(ctx context.Context, forcePrimary bool) error {
	poolerType := pm.getPoolerType()
	isPrimary := (poolerType == clustermetadatapb.PoolerType_PRIMARY)

	if isPrimary && !forcePrimary {
		slog.WarnContext(ctx, "Backup requested on primary database without ForcePrimary flag")
		return mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION,
			"backups from primary databases are not allowed unless ForcePrimary is set")
	}
	return nil
}

// validateBackupParams validates the backup type and returns the pgbackrest type mapping
func (pm *MultiPoolerManager) validateBackupParams(backupType, configPath, stanzaName string) (pgBackRestType string, err error) {
	// Validate backup type is provided
	if backupType == "" {
		return "", mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "type is required")
	}

	// Map to pgbackrest types: full, diff, incr
	switch backupType {
	case "full":
		pgBackRestType = "full"
	case "differential":
		pgBackRestType = "diff"
	case "incremental":
		pgBackRestType = "incr"
	default:
		return "", mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT,
			fmt.Sprintf("invalid backup type '%s': must be one of: full, differential, incremental", backupType))
	}

	// Validate required backup configuration
	if configPath == "" {
		return "", mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "config_path is required")
	}
	if stanzaName == "" {
		return "", mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "stanza_name is required")
	}

	return pgBackRestType, nil
}

// safeCombinedOutput executes a command and streams its output to avoid blocking.
// This prevents deadlocks when commands produce large amounts of output that
// would fill the internal pipe buffers. Returns combined stdout and stderr.
func safeCombinedOutput(cmd *exec.Cmd) (string, error) {
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return "", fmt.Errorf("failed to create stdout pipe: %w", err)
	}

	stderr, err := cmd.StderrPipe()
	if err != nil {
		return "", fmt.Errorf("failed to create stderr pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		return "", fmt.Errorf("failed to start command: %w", err)
	}

	lines := make(chan string, 100)
	stdoutDone := make(chan struct{})
	stderrDone := make(chan struct{})

	go func() {
		defer close(stdoutDone)
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			lines <- scanner.Text() + "\n"
		}
	}()

	go func() {
		defer close(stderrDone)
		scanner := bufio.NewScanner(stderr)
		for scanner.Scan() {
			lines <- scanner.Text() + "\n"
		}
	}()

	go func() {
		<-stdoutDone
		<-stderrDone
		close(lines)
	}()

	var combinedBuf strings.Builder
	for line := range lines {
		combinedBuf.WriteString(line)
	}

	return combinedBuf.String(), cmd.Wait()
}

// findBackupByJobID finds a backup by matching the job_id annotation
//
// This function has to scan the entire backup history to find the backup, but the worst case
// should be manageable, because production deployments should have retention policies that
// trigger pgBackRest to delete older backups.
func (pm *MultiPoolerManager) findBackupByJobID(
	ctx context.Context,
	jobID string,
) (string, error) {
	stanzaName := pm.getBackupStanza()
	configPath := pm.getBackupConfigPath()

	// Execute pgbackrest info command with JSON output
	infoCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	cmd := exec.CommandContext(infoCtx, "pgbackrest",
		"--stanza="+stanzaName,
		"--config="+configPath,
		"--repo1-path="+pm.backupLocation,
		"--output=json",
		"--log-level-console=off",
		"info")

	output, err := safeCombinedOutput(cmd)
	if err != nil {
		return "", mterrors.New(mtrpcpb.Code_INTERNAL,
			fmt.Sprintf("pgbackrest info failed: %v\nOutput: %s", err, output))
	}

	// Parse JSON output
	var infoData []pgBackRestInfo
	if err := json.Unmarshal([]byte(output), &infoData); err != nil {
		return "", mterrors.New(mtrpcpb.Code_INTERNAL,
			fmt.Sprintf("failed to parse pgbackrest info JSON: %v", err))
	}

	// Search for backup with matching annotations
	if len(infoData) == 0 || len(infoData[0].Backup) == 0 {
		return "", mterrors.New(mtrpcpb.Code_NOT_FOUND,
			"no backups found in pgbackrest info output")
	}

	var matchedBackups []string
	for _, pgBackup := range infoData[0].Backup {
		if pgBackup.Annotation != nil {
			if pgBackup.Annotation["job_id"] == jobID {
				matchedBackups = append(matchedBackups, pgBackup.Label)
			}
		}
	}

	if len(matchedBackups) == 0 {
		return "", mterrors.New(mtrpcpb.Code_NOT_FOUND,
			fmt.Sprintf("no backup found with job_id=%s", jobID))
	}

	if len(matchedBackups) > 1 {
		return "", mterrors.New(mtrpcpb.Code_INTERNAL,
			fmt.Sprintf("found %d backups with job_id=%s, expected 1",
				len(matchedBackups), jobID))
	}

	return matchedBackups[0], nil
}

// tryAutoRestoreFromBackup attempts to restore from backup if the pooler is uninitialized.
// This runs as an independent goroutine started by Start().
// It waits for topo to be loaded, then retries until:
// - A backup is found and restore succeeds
// - The pooler becomes initialized (by an RPC)
// - The context is cancelled
//
// Auto-restore only applies to REPLICA poolers. PRIMARY poolers must be explicitly
// initialized via InitializeEmptyPrimary RPC.
func (pm *MultiPoolerManager) tryAutoRestoreFromBackup(ctx context.Context) {
	// Wait for manager to be ready (topo loaded) before checking initialization state
	pm.waitForReady(ctx)

	// Check context after waiting
	if ctx.Err() != nil {
		pm.logger.InfoContext(ctx, "Auto-restore: context cancelled while waiting for topo")
		return
	}

	// TODO: Handle partial initialization state (data dir exists but no marker).
	// Currently we proceed as if uninitialized, which may cause issues if
	// PostgreSQL is partially set up. Options for future:
	// - Detect and cleanup automatically (with explicit flag)
	// - Detect and block startup, require manual intervention

	// Check if already initialized - skip if so (no retry needed)
	if pm.isInitialized(ctx) {
		pm.logger.InfoContext(ctx, "Auto-restore skipped: pooler already initialized")
		return
	}

	// Only auto-restore REPLICA poolers - PRIMARY must be explicitly initialized.
	poolerType := pm.getPoolerType()
	if poolerType != clustermetadatapb.PoolerType_REPLICA {
		pm.logger.InfoContext(ctx, "Auto-restore skipped: only REPLICA poolers auto-restore", "pooler_type", poolerType.String())
		return
	}

	pm.logger.InfoContext(ctx, "Auto-restore: pooler is uninitialized, checking for backups")

	// Retry loop: keep trying until we succeed or context is cancelled
	r := retry.New(pm.autoRestoreRetryInterval, pm.autoRestoreRetryInterval)
	for attempt, err := range r.Attempts(ctx) {
		if err != nil {
			pm.logger.InfoContext(ctx, "Auto-restore: context cancelled", "attempts", attempt, "error", err)
			return
		}

		if attempt > 1 {
			pm.logger.InfoContext(ctx, "Auto-restore: retrying", "attempt", attempt)
		}

		_, done := pm.tryAutoRestoreOnce(ctx)
		if done {
			// Either succeeded or determined should not retry
			return
		}
		// continue to next iteration for retry
	}
}

// tryAutoRestoreOnce attempts a single auto-restore from backup.
// Returns (success, done) where:
//   - success=true, done=true: restore completed successfully
//   - success=false, done=true: should not retry (e.g., already initialized)
//   - success=false, done=false: should retry
func (pm *MultiPoolerManager) tryAutoRestoreOnce(ctx context.Context) (success bool, done bool) {
	// Acquire action lock to prevent races with initialization RPCs.
	// This ensures InitializeEmptyPrimary/InitializeAsStandby can't run concurrently.
	lockCtx, err := pm.actionLock.Acquire(ctx, "tryAutoRestoreFromBackup")
	if err != nil {
		pm.logger.ErrorContext(ctx, "Auto-restore: failed to acquire action lock, will retry", "error", err)
		return false, false
	}
	defer pm.actionLock.Release(lockCtx)

	// Check if we were initialized while waiting (by an RPC)
	if pm.isInitialized(lockCtx) {
		pm.logger.InfoContext(ctx, "Auto-restore skipped: pooler was initialized while waiting")
		return false, true
	}

	// Check for available backups
	backups, err := pm.listBackups(lockCtx)
	if err != nil {
		pm.logger.ErrorContext(ctx, "Auto-restore: failed to check for backups, will retry", "error", err)
		return false, false
	}

	// Filter to only complete backups for auto-restore
	var completeBackups []*multipoolermanagerdata.BackupMetadata
	for _, b := range backups {
		if b.Status == multipoolermanagerdata.BackupMetadata_COMPLETE {
			completeBackups = append(completeBackups, b)
		}
	}

	if len(completeBackups) == 0 {
		pm.logger.InfoContext(ctx, "Auto-restore: no complete backups available yet, will retry",
			"total_backups", len(backups))
		return false, false
	}

	// Use the latest complete backup (last in the list from pgbackrest, according to
	// pgbackrest docs)
	latestBackup := completeBackups[len(completeBackups)-1]
	pm.logger.InfoContext(ctx, "Auto-restore: found backup, attempting restore",
		"backup_id", latestBackup.BackupId,
		"total_backups", len(backups))

	// Perform the restore using restoreFromBackupLocked
	// This ensures consistent behavior between auto-restore and explicit RestoreFromBackup RPC
	pm.logger.InfoContext(ctx, "Executing auto-restore from backup", "backup_id", latestBackup.BackupId)
	if err := pm.restoreFromBackupLocked(lockCtx, latestBackup.BackupId); err != nil {
		pm.logger.ErrorContext(ctx, "Auto-restore: restore failed, will retry",
			"backup_id", latestBackup.BackupId,
			"error", err)
		return false, false
	}

	pm.logger.InfoContext(ctx, "Auto-restore: completed successfully", "backup_id", latestBackup.BackupId)
	return true, true
}

// GetBackupByJobId searches for a backup with the given job_id annotation.
// Returns nil Backup if not found.
func (pm *MultiPoolerManager) GetBackupByJobId(ctx context.Context, jobID string) (*multipoolermanagerdata.BackupMetadata, error) {
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
	backups, err := pm.getBackupsLocked(ctx, 0) // 0 = no limit
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
