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
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"text/template"
	"time"

	"github.com/multigres/multigres/config"
	"github.com/multigres/multigres/go/common/backup"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolermanagerdata "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	pgctldpb "github.com/multigres/multigres/go/pb/pgctldservice"
)

// PgBackRestConfigMode specifies how initPgBackRest should create the config file
type PgBackRestConfigMode bool

const (
	// ForBackup creates a temporary config file in /tmp with pg2 configuration for standby poolers
	ForBackup PgBackRestConfigMode = true
	// NotForBackup creates a persistent config file in the pooler directory without pg2 configuration
	NotForBackup PgBackRestConfigMode = false
)

// initPgBackRest creates the pgBackRest config file and sets up the necessary directories.
// Returns the path to the backup config file.
// If mode is NotForBackup and the config file already exists, it returns the path without recreating it.
// If mode is ForBackup, it creates a temporary config file and returns that path.
// The config file that is created NotForBackup will be used by postgres and other tooling. It should
// therefore not be overwritten once created. Otherwise, it can cause problems if postgres tries to
// archive a WAL just when we're overwriting it.
func (pm *MultiPoolerManager) initPgBackRest(ctx context.Context, mode PgBackRestConfigMode) (string, error) {
	pgbackrestPath := pm.pgbackrestPath()
	configPath := filepath.Join(pgbackrestPath, "pgbackrest.conf")

	// Initialize path names used in the config file
	logsPath := filepath.Join(pgbackrestPath, "logs")
	spoolPath := filepath.Join(pgbackrestPath, "spool")
	lockPath := filepath.Join(pgbackrestPath, "lock")

	// If this is for backup, we'll create a backup config file instead
	if mode == ForBackup {
		// Don't use the main config file for backups.
		// We need to regenerate this every time because the primary may change.
		configPath = filepath.Join(pgbackrestPath, "pgbackrest-backup.conf")
		// Create the pgbackrest directory to write the backup config file
		if err := os.MkdirAll(pgbackrestPath, 0o755); err != nil {
			return "", fmt.Errorf("failed to create config directory %s: %w", pgbackrestPath, err)
		}
	} else {
		// Do not regenerate the config file if it already exists.
		// We don't want to overwrite the file once it's created because postgres
		// will be using it to archive WALs.
		if _, err := os.Stat(configPath); err == nil {
			// Config file exists, nothing to do
			return configPath, nil
		}

		// Create directories along with the persistent config.
		if err := os.MkdirAll(pgbackrestPath, 0o755); err != nil {
			return "", fmt.Errorf("failed to create config directory %s: %w", pgbackrestPath, err)
		}
		if err := os.MkdirAll(logsPath, 0o755); err != nil {
			return "", fmt.Errorf("failed to create logs directory %s: %w", logsPath, err)
		}
		if err := os.MkdirAll(spoolPath, 0o755); err != nil {
			return "", fmt.Errorf("failed to create spool directory %s: %w", spoolPath, err)
		}
		if err := os.MkdirAll(lockPath, 0o755); err != nil {
			return "", fmt.Errorf("failed to create lock directory %s: %w", lockPath, err)
		}
	}

	// Validate backup config is loaded from topology
	if pm.backupConfig == nil {
		return "", errors.New("backup config not loaded from topology")
	}

	// Generate pgbackrest config file from template
	// TODO: Set process-max dynamically based on number of CPUs
	tmpl, err := template.New("pgbackrest").Parse(config.PgBackRestConfigTmpl)
	if err != nil {
		return "", fmt.Errorf("failed to parse pgbackrest config template: %w", err)
	}

	// Generate pgBackRest repo configuration
	repoConfig, err := pm.backupConfig.PgBackRestConfig(pm.stanzaName())
	if err != nil {
		return "", fmt.Errorf("failed to generate repo config: %w", err)
	}

	templateData := struct {
		LogPath   string
		SpoolPath string
		LockPath  string

		// Dynamic repo configuration
		RepoConfig map[string]string

		Pg1SocketPath string
		Pg1Port       int
		Pg1Path       string

		ServerCertFile string
		ServerKeyFile  string
		ServerCAFile   string
		ServerPort     int
		ServerAuths    []string

		RepoCredentials map[string]string

		Pg2Host string
		Pg2Port int
		Pg2Path string

		Pg2HostPort     int
		Pg2HostCAFile   string
		Pg2HostCertFile string
		Pg2HostKeyFile  string
	}{
		LogPath:   logsPath,
		SpoolPath: spoolPath,
		LockPath:  lockPath,

		RepoConfig: repoConfig,

		Pg1SocketPath: filepath.Join(pm.multipooler.PoolerDir, "pg_sockets"),
		Pg1Port:       func() int { return int(pm.multipooler.PortMap["postgres"]) }(),
		Pg1Path:       filepath.Join(pm.multipooler.PoolerDir, "pg_data"),

		// Use configured certificate paths for server
		ServerCertFile: pm.config.PgBackRestCertFile,
		ServerKeyFile:  pm.config.PgBackRestKeyFile,
		ServerCAFile:   pm.config.PgBackRestCAFile,
		ServerPort:     pm.config.PgBackRestPort,
		ServerAuths:    []string{"pgbackrest=*"},
	}

	// Add credentials to template data if using env credentials
	if pm.backupConfig.UsesEnvCredentials() {
		creds, err := pm.backupConfig.PgBackRestCredentials()
		if err != nil {
			return "", fmt.Errorf("failed to get credentials: %w", err)
		}
		templateData.RepoCredentials = creds
	}

	// We need a pg2 section only for backups. Other operations only need pg1.
	if mode == ForBackup {
		// It's possible that there's no primary (during initialization). If so, we skip pg2.
		if primaryPoolerID := pm.getPrimaryPoolerID(); primaryPoolerID != nil {
			primarymp, err := pm.topoClient.GetMultiPooler(ctx, primaryPoolerID)
			if err != nil {
				return "", fmt.Errorf("failed to get primary pooler info from topo: %w", err)
			}
			pm.mu.Lock()
			templateData.Pg2Host = pm.primaryHost
			templateData.Pg2Port = int(pm.primaryPort)
			pm.mu.Unlock()
			templateData.Pg2Path = filepath.Join(primarymp.PoolerDir, "pg_data")
			templateData.Pg2HostPort = int(primarymp.GetPortMap()["pgbackrest"])
			templateData.Pg2HostCAFile = pm.config.PgBackRestCAFile
			templateData.Pg2HostCertFile = pm.config.PgBackRestCertFile
			templateData.Pg2HostKeyFile = pm.config.PgBackRestKeyFile
		}
	}

	var configContent strings.Builder
	if err := tmpl.Execute(&configContent, templateData); err != nil {
		return "", fmt.Errorf("failed to execute pgbackrest config template: %w", err)
	}

	// Use restrictive permissions (0600) since config may contain credentials
	if err := os.WriteFile(configPath, []byte(configContent.String()), 0o600); err != nil {
		return "", fmt.Errorf("failed to write pgbackrest config file: %w", err)
	}

	return configPath, nil
}

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

	pm.logger.InfoContext(ctx, "Starting backup operation", "backup_type", pm.backupConfig.Type(), "job_type", backupType)

	// Check if backup is allowed on primary
	if err := pm.allowBackupOnPrimary(ctx, forcePrimary); err != nil {
		return "", err
	}

	configPath, err := pm.initPgBackRest(ctx, ForBackup)
	if err != nil {
		return "", err
	}

	tableGroup := pm.multipooler.TableGroup
	shard := pm.multipooler.Shard
	multipoolerID := topoclient.MultiPoolerIDString(pm.multipooler.Id)
	multipoolerName := pm.multipooler.Id.Name

	// Use provided job_id or generate one (same format as multiadmin)
	effectiveJobID := jobID
	if effectiveJobID == "" {
		effectiveJobID = backup.GenerateJobID(multipoolerID)
	}

	// Validate parameters and get pgbackrest type
	pgBackRestType, err := pm.validateBackupParams(backupType, configPath)
	if err != nil {
		return "", err
	}

	// Execute pgbackrest backup command
	ctx, cancel := context.WithTimeout(ctx, backup.BackupTimeout)
	defer cancel()

	args := []string{
		"--stanza=" + pm.stanzaName(),
		"--config=" + configPath,
		"--type=" + pgBackRestType,
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

	// Execute backup with progress logging
	output, err := pm.runLongCommand(ctx, cmd, "pgbackrest backup")
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
	verifyCtx, verifyCancel := context.WithTimeout(ctx, backup.VerifyTimeout)
	defer verifyCancel()

	verifyCmd := exec.CommandContext(verifyCtx, "pgbackrest",
		"--stanza="+pm.stanzaName(),
		"--config="+configPath,
		"--set="+foundBackupID,
		"verify")

	// Execute verify with progress logging
	verifyOutput, verifyErr := pm.runLongCommand(verifyCtx, verifyCmd, "pgbackrest verify backup_id="+foundBackupID)
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

	// Pause monitoring during restore to prevent interference
	resumeMonitor, err := pm.PausePostgresMonitor(ctx)
	if err != nil {
		return err
	}
	defer resumeMonitor(ctx)

	return pm.restoreFromBackupLocked(ctx, backupID)
}

// restoreFromBackupLocked performs the restore. Caller must hold the action lock
// and monitoring must be disabled to avoid interference.
func (pm *MultiPoolerManager) restoreFromBackupLocked(ctx context.Context, backupID string) error {
	if err := AssertActionLockHeld(ctx); err != nil {
		return err
	}

	pm.logger.InfoContext(ctx, "Starting restore operation", "backup_type", pm.backupConfig.Type(), "backup_id", backupID)

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

	// Reconfigure archive_command to use this pooler's local pgbackrest.conf
	// After restore, postgresql.auto.conf contains archive_command pointing to the
	// primary's pgbackrest.conf path. Each pooler needs its own config path.
	pm.logger.InfoContext(ctx, "Reconfiguring archive_command for local pgbackrest.conf")
	if err := pm.removeArchiveConfigFromAutoConf(); err != nil {
		return mterrors.Wrap(err, "failed to remove old archive configuration")
	}
	if err := pm.configureArchiveMode(ctx); err != nil {
		return mterrors.Wrap(err, "failed to configure archive mode")
	}

	if err := pm.startPostgreSQLAfterRestore(ctx, backupID); err != nil {
		return err
	}

	// Set consensus term after restore.
	// If the cluster is at a higher term,
	// validateAndUpdateTerm will automatically update our term when multiorch fixes replication.
	var term int64 = 0
	if pm.consensusState != nil {
		pm.logger.InfoContext(ctx, "Loading consensus term that was restored from backup")
		pm.loadConsensusTermFromDisk()
		term = pm.consensusState.term.TermNumber

		// Clear primary_term from backup since this is a standby restore
		// The backup may contain a non-zero primary_term if it was taken from a primary
		if err := pm.consensusState.SetPrimaryTerm(ctx, 0, false /* force */); err != nil {
			return mterrors.Wrap(err, "failed to clear primary_term after restore")
		}
	}

	if term == 0 {
		pm.logger.ErrorContext(ctx, "MonitorPostgres: term is uninitialized even after restore")
	}

	if err := pm.reopenPoolerManager(ctx); err != nil {
		return err
	}

	// Mark as initialized after successful restore
	return pm.setInitialized()
}

func (pm *MultiPoolerManager) executePgBackrestRestore(ctx context.Context, backupID string) error {
	configPath, err := pm.initPgBackRest(ctx, NotForBackup)
	if err != nil {
		return mterrors.Wrap(err, "failed to initialize pgbackrest")
	}

	restoreCtx, cancel := context.WithTimeout(ctx, backup.RestoreTimeout)
	defer cancel()

	args := []string{
		"--stanza=" + pm.stanzaName(),
		"--config=" + configPath,
		"--type=standby",
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
	configPath, err := pm.initPgBackRest(ctx, NotForBackup)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to initialize pgbackrest")
	}

	if pm.backupConfig == nil {
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "backup config not loaded from topology")
	}

	// Execute pgbackrest info command with JSON output
	queryCtx, cancel := context.WithTimeout(ctx, backup.InfoTimeout)
	defer cancel()

	cmd := exec.CommandContext(queryCtx, "pgbackrest",
		"--stanza="+pm.stanzaName(),
		"--config="+configPath,
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
	currentTableGroup := pm.multipooler.TableGroup
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
		poolerType := clustermetadatapb.PoolerType_UNKNOWN
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
func (pm *MultiPoolerManager) validateBackupParams(backupType, configPath string) (pgBackRestType string, err error) {
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
	configPath, err := pm.initPgBackRest(ctx, NotForBackup)
	if err != nil {
		return "", mterrors.Wrap(err, "failed to initialize pgbackrest")
	}

	// Execute pgbackrest info command with JSON output
	infoCtx, cancel := context.WithTimeout(ctx, backup.InfoTimeout)
	defer cancel()

	cmd := exec.CommandContext(infoCtx, "pgbackrest",
		"--stanza="+pm.stanzaName(),
		"--config="+configPath,
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
			"no backup found with job_id="+jobID)
	}

	if len(matchedBackups) > 1 {
		return "", mterrors.New(mtrpcpb.Code_INTERNAL,
			fmt.Sprintf("found %d backups with job_id=%s, expected 1",
				len(matchedBackups), jobID))
	}

	return matchedBackups[0], nil
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

// pgbackrestPath returns the path to the pgbackrest config and data directory
func (pm *MultiPoolerManager) pgbackrestPath() string {
	return filepath.Join(pm.multipooler.PoolerDir, "pgbackrest")
}

// runLongCommand executes a long-running command with periodic progress logging.
// Logs progress every 10 seconds. The cmd should be created with exec.CommandContext(ctx, ...)
// to ensure proper cleanup on context cancellation.
func (pm *MultiPoolerManager) runLongCommand(ctx context.Context, cmd *exec.Cmd, operationName string) ([]byte, error) {
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

	// Run command in main goroutine
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
