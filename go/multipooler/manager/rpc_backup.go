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
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/multigres/multigres/go/mterrors"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolermanagerdata "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	pgctldpb "github.com/multigres/multigres/go/pb/pgctldservice"
)

// Backup performs a backup
func (pm *MultiPoolerManager) Backup(ctx context.Context, forcePrimary bool, backupType string) (string, error) {
	// Check if this is a primary pooler based on topology
	poolerType := pm.GetPoolerType()
	isPrimary := (poolerType == clustermetadatapb.PoolerType_PRIMARY)

	// Prevent backups from primary databases unless ForcePrimary is set
	if isPrimary && !forcePrimary {
		slog.WarnContext(ctx, "Backup requested on primary database without ForcePrimary flag")
		return "", mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION,
			"backups from primary databases are not allowed unless ForcePrimary is set")
	}

	// Validation
	if backupType == "" {
		return "", mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "type is required")
	}

	// Type validation and mapping to pgbackrest types
	// pgbackrest uses abbreviated types: full, diff, incr
	pgBackRestType := ""
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

	configPath := pm.GetBackupConfigPath()
	stanzaName := pm.GetBackupStanza()
	tableGroup := pm.GetTableGroup()
	shard := pm.GetShard()

	// Validate required backup configuration
	if configPath == "" {
		return "", mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "config_path is required")
	}
	if stanzaName == "" {
		return "", mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "stanza_name is required")
	}

	// Execute pgbackrest backup command
	ctx, cancel := context.WithTimeout(ctx, 30*time.Minute) // Backups can take a long time
	defer cancel()

	args := []string{
		"--stanza=" + stanzaName,
		"--config=" + configPath,
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

	args = append(args, "backup")

	cmd := exec.CommandContext(ctx, "pgbackrest", args...)

	// Capture output for logging and to extract backup ID
	output, err := cmd.CombinedOutput()
	if err != nil {
		return "", mterrors.New(mtrpcpb.Code_INTERNAL,
			fmt.Sprintf("pgbackrest backup failed: %v\nOutput: %s", err, string(output)))
	}

	// Parse the backup ID from the output
	backupID, err := extractBackupID(string(output))
	if err != nil {
		return "", mterrors.New(mtrpcpb.Code_INTERNAL,
			fmt.Sprintf("failed to extract backup ID from output: %v\nOutput: %s", err, string(output)))
	}

	// Verify the backup to ensure it's valid
	verifyCtx, verifyCancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer verifyCancel()

	verifyCmd := exec.CommandContext(verifyCtx, "pgbackrest",
		"--stanza="+stanzaName,
		"--config="+configPath,
		"--set="+backupID,
		"--log-level-console=info",
		"verify")

	verifyOutput, verifyErr := verifyCmd.CombinedOutput()
	if verifyErr != nil {
		return "", mterrors.New(mtrpcpb.Code_INTERNAL,
			fmt.Sprintf("pgbackrest verify failed for backup %s: %v\nOutput: %s", backupID, verifyErr, string(verifyOutput)))
	}

	return backupID, nil
}

// extractBackupID extracts the backup label from pgbackrest output
//
// TODO: find a way of of doing this that does that does not rely on text matching
func extractBackupID(output string) (string, error) {
	// First, try to find "new backup label" in the output (most reliable)
	lines := strings.Split(output, "\n")
	for _, line := range lines {
		if strings.Contains(line, "new backup label") {
			// Extract the label after the "=" sign
			parts := strings.Split(line, "=")
			if len(parts) >= 2 {
				label := strings.TrimSpace(parts[len(parts)-1])
				if label != "" {
					return label, nil
				}
			}
		}
	}

	// Fallback: Look for backup label pattern like "20250104-100000F" or "20250104-100000F_20250104-120000I"
	// The pattern is: YYYYMMDD-HHMMSS followed by F (full), D (differential), or I (incremental)
	// Find all matches and take the last one (newest backup)
	re := regexp.MustCompile(`(\d{8}-\d{6}[FDI](?:_\d{8}-\d{6}[FDI])?)`)
	matches := re.FindAllStringSubmatch(output, -1)
	if len(matches) > 0 {
		// Return the last match (most recent backup ID)
		lastMatch := matches[len(matches)-1]
		if len(lastMatch) >= 2 {
			return lastMatch[1], nil
		}
	}

	return "", fmt.Errorf("backup ID not found in output")
}

// RestoreFromBackup restores from a backup
func (pm *MultiPoolerManager) RestoreFromBackup(ctx context.Context, backupID string) error {
	slog.InfoContext(ctx, "RestoreFromBackup called", "backup_id", backupID)

	pgctldClient := pm.GetPgCtldClient()
	configPath := pm.GetBackupConfigPath()
	stanzaName := pm.GetBackupStanza()

	// Get pg_data directory from the backup config path
	// configPath is like /path/to/pooler_dir/pgbackrest.conf, so we get the dir and append pg_data
	poolerDir := filepath.Dir(configPath)
	pgDataDir := filepath.Join(poolerDir, "pg_data")

	// Determine if we should maintain standby status after restore
	// We query PostgreSQL directly to get the current recovery status
	slog.InfoContext(ctx, "Checking recovery status before restore")
	isPrimary, err := pm.isPrimary(ctx)
	if err != nil {
		slog.ErrorContext(ctx, "Failed to check recovery status before restore", "error", err)
		return mterrors.Wrap(err, "failed to check recovery status")
	}

	asStandby := !isPrimary

	// If this is a standby, get the current primary connection info
	// so we can restore it after pgbackrest overwrites postgresql.auto.conf
	var primaryHost string
	var primaryPort int32
	if asStandby {
		replStatus, err := pm.ReplicationStatus(ctx)
		if err != nil {
			slog.ErrorContext(ctx, "Failed to get replication status", "error", err)
			return mterrors.Wrap(err, "failed to get replication status")
		}
		if replStatus == nil || replStatus.PrimaryConnInfo == nil || replStatus.PrimaryConnInfo.Host == "" {
			return mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION, "standby has no primary connection configured")
		}
		primaryHost = replStatus.PrimaryConnInfo.Host
		primaryPort = replStatus.PrimaryConnInfo.Port
	}

	slog.InfoContext(ctx, "Restore parameters determined",
		"is_primary", isPrimary,
		"as_standby", asStandby,
		"primary_host", primaryHost,
		"primary_port", primaryPort,
		"backup_id", backupID)

	// Validate required parameters
	if pgctldClient == nil {
		return mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "pgctld_client is required")
	}
	if configPath == "" {
		return mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "config_path is required")
	}
	if stanzaName == "" {
		return mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "stanza_name is required")
	}
	if pgDataDir == "" {
		return mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "pg_data_dir is required")
	}
	if asStandby && (primaryHost == "" || primaryPort == 0) {
		return mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "primary_host and primary_port required when restoring as standby")
	}

	// Step 1: Stop PostgreSQL server
	slog.InfoContext(ctx, "Stopping PostgreSQL before restore", "backup_id", backupID)
	stopCtx, stopCancel := context.WithTimeout(ctx, 2*time.Minute)
	defer stopCancel()

	_, err = pgctldClient.Stop(stopCtx, &pgctldpb.StopRequest{
		Mode: "fast", // Fast shutdown mode
	})
	if err != nil {
		return mterrors.New(mtrpcpb.Code_INTERNAL,
			fmt.Sprintf("failed to stop PostgreSQL: %v", err))
	}
	slog.InfoContext(ctx, "PostgreSQL stopped successfully")

	// Step 2: Execute pgBackRest restore command, which does most of the work
	// of writing necessary configuration to postgresql.auto.conf
	restoreCtx, restoreCancel := context.WithTimeout(ctx, 30*time.Minute) // Restores can take a long time
	defer restoreCancel()

	args := []string{
		"--stanza=" + stanzaName,
		"--config=" + configPath,
		"--log-level-console=info", // Verbose logging to see what pgBackRest is doing
		"--delta",                  // Preserve valid files and only restore changed/missing ones
	}

	// If a specific backup ID is specified, add the --set flag
	if backupID != "" {
		args = append(args, "--set="+backupID)
	}

	args = append(args, "restore")

	cmd := exec.CommandContext(restoreCtx, "pgbackrest", args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		// Attempt to restart PostgreSQL even if restore failed
		startCtx, startCancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer startCancel()
		_, _ = pgctldClient.Start(startCtx, &pgctldpb.StartRequest{})

		return mterrors.New(mtrpcpb.Code_INTERNAL,
			fmt.Sprintf("pgbackrest restore failed: %v\nOutput: %s", err, string(output)))
	}

	// Step 2.5: If restoring as standby, restore primary_conninfo to postgresql.auto.conf
	// (pgbackrest restore overwrites postgresql.auto.conf, removing replication settings)
	if asStandby {
		autoConfPath := filepath.Join(pgDataDir, "postgresql.auto.conf")
		slog.InfoContext(ctx, "Restoring primary_conninfo to postgresql.auto.conf",
			"path", autoConfPath,
			"primary_host", primaryHost,
			"primary_port", primaryPort)

		// Read existing content to preserve other settings
		existingContent, err := os.ReadFile(autoConfPath)
		if err != nil && !os.IsNotExist(err) {
			slog.WarnContext(ctx, "Failed to read existing postgresql.auto.conf", "error", err)
			existingContent = []byte{}
		}

		// Append primary_conninfo setting
		primaryConnInfo := fmt.Sprintf("\n# Restored by multigres after backup restore\nprimary_conninfo = 'host=%s port=%d user=postgres'\n",
			primaryHost, primaryPort)

		newContent := append(existingContent, []byte(primaryConnInfo)...)

		err = os.WriteFile(autoConfPath, newContent, 0o600)
		if err != nil {
			return mterrors.New(mtrpcpb.Code_INTERNAL,
				fmt.Sprintf("failed to write primary_conninfo to postgresql.auto.conf: %v", err))
		}

		slog.InfoContext(ctx, "Successfully restored primary_conninfo to postgresql.auto.conf")
	}

	// Step 3: Restart PostgreSQL server after successful restore
	// Use Restart instead of Start to properly handle standby.signal creation
	restartCtx, restartCancel := context.WithTimeout(ctx, 2*time.Minute)
	defer restartCancel()

	slog.InfoContext(ctx, "Restarting PostgreSQL after restore",
		"as_standby", asStandby,
		"backup_id", backupID)

	_, err = pgctldClient.Restart(restartCtx, &pgctldpb.RestartRequest{
		AsStandby: asStandby, // Maintains standby status if restoring to a replica
	})
	if err != nil {
		return mterrors.New(mtrpcpb.Code_INTERNAL,
			fmt.Sprintf("failed to restart PostgreSQL after restore: %v", err))
	}

	slog.InfoContext(ctx, "PostgreSQL restarted successfully after restore")

	return nil
}

// GetBackups retrieves backup information
func (pm *MultiPoolerManager) GetBackups(ctx context.Context, limit uint32) ([]*multipoolermanagerdata.BackupMetadata, error) {
	configPath := pm.GetBackupConfigPath()
	stanzaName := pm.GetBackupStanza()

	// Validate required configuration
	if configPath == "" {
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "config_path is required")
	}
	if stanzaName == "" {
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "stanza_name is required")
	}

	// Execute pgbackrest info command with JSON output
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, "pgbackrest",
		"--stanza="+stanzaName,
		"--config="+configPath,
		"--output=json",
		"info")

	output, err := cmd.CombinedOutput()
	if err != nil {
		// Handle case where stanza doesn't exist yet or config file is missing - return empty list
		outputStr := string(output)
		if outputStr == "" || strings.Contains(outputStr, "does not exist") || strings.Contains(outputStr, "unable to open missing file") {
			return []*multipoolermanagerdata.BackupMetadata{}, nil
		}
		return nil, mterrors.New(mtrpcpb.Code_INTERNAL,
			fmt.Sprintf("pgbackrest info failed: %v\nOutput: %s", err, outputStr))
	}

	// Parse JSON output
	var infoData []pgBackRestInfo
	if err := json.Unmarshal(output, &infoData); err != nil {
		return nil, mterrors.New(mtrpcpb.Code_INTERNAL,
			fmt.Sprintf("failed to parse pgbackrest info JSON: %v", err))
	}

	// Extract backups from the first stanza (should be the only one)
	var backups []*multipoolermanagerdata.BackupMetadata
	if len(infoData) > 0 && len(infoData[0].Backup) > 0 {
		for _, pgBackup := range infoData[0].Backup {
			// Determine backup status
			status := multipoolermanagerdata.BackupMetadata_COMPLETE
			if pgBackup.Error {
				status = multipoolermanagerdata.BackupMetadata_INCOMPLETE
			}

			// Extract table_group and shard from annotations
			tableGroup := ""
			shard := ""
			if pgBackup.Annotation != nil {
				tableGroup = pgBackup.Annotation["table_group"]
				shard = pgBackup.Annotation["shard"]
			}

			backups = append(backups, &multipoolermanagerdata.BackupMetadata{
				BackupId:   pgBackup.Label,
				Status:     status,
				TableGroup: tableGroup,
				Shard:      shard,
			})
		}
	}

	// Apply limit if specified
	if limit > 0 && uint32(len(backups)) > limit {
		backups = backups[:limit]
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
}

// pgBackRestTimestamp represents backup timestamps
type pgBackRestTimestamp struct {
	Start int64 `json:"start"`
	Stop  int64 `json:"stop"`
}
