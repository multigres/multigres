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
	// Check if backup is allowed on primary
	if err := pm.allowBackupOnPrimary(ctx, forcePrimary); err != nil {
		return "", err
	}

	configPath := pm.getBackupConfigPath()
	stanzaName := pm.getBackupStanza()
	tableGroup := pm.getTableGroup()
	shard := pm.getShard()

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

// RestoreFromBackup restores from a backup. This can only be used to restore a
// backup to an uniinitialized standby. If standby is already initialized,
// the caller is responsible for removing PGDATA.
func (pm *MultiPoolerManager) RestoreFromBackup(ctx context.Context, backupID string, asStandby bool) error {
	slog.InfoContext(ctx, "RestoreFromBackup called", "backup_id", backupID)

	// Perform validation
	if !asStandby {
		return mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT,
			"only restoring as standby is currently supported (as_standby must be true)")
	}

	if pm.isInitialized() {
		return mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION,
			"cannot restore onto already-initialized database")
	}

	pgctldClient := pm.getPgCtldClient()
	configPath := pm.getBackupConfigPath()
	stanzaName := pm.getBackupStanza()

	if pgctldClient == nil {
		return mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "pgctld_client is required")
	}
	if configPath == "" {
		return mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "config_path is required")
	}
	if stanzaName == "" {
		return mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "stanza_name is required")
	}

	// Restore the backup
	if err := pm.executeBackrestRestore(ctx, stanzaName, configPath, backupID); err != nil {
		return err
	}
	if err := pm.startPostgreSQLAfterRestore(ctx, pgctldClient, asStandby, backupID); err != nil {
		return err
	}
	return pm.reopenPoolerManager(ctx)
}

func (pm *MultiPoolerManager) executeBackrestRestore(ctx context.Context, stanzaName, configPath, backupID string) error {
	restoreCtx, cancel := context.WithTimeout(ctx, 30*time.Minute)
	defer cancel()

	args := []string{
		"--stanza=" + stanzaName,
		"--config=" + configPath,
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
			fmt.Sprintf("pgbackrest restore failed: %v\nOutput: %s", err, string(output)))
	}

	return nil
}

func (pm *MultiPoolerManager) startPostgreSQLAfterRestore(ctx context.Context, pgctldClient pgctldpb.PgCtldClient, asStandby bool, backupID string) error {
	restartCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()

	slog.InfoContext(ctx, "Starting PostgreSQL after restore",
		"as_standby", asStandby,
		"backup_id", backupID)

	_, err := pgctldClient.Restart(restartCtx, &pgctldpb.RestartRequest{
		AsStandby: asStandby,
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
	if err := pm.Open(); err != nil {
		slog.ErrorContext(ctx, "Failed to reopen pooler manager after restore", "error", err)
		return mterrors.Wrap(err, "failed to reopen pooler manager after restore")
	}
	slog.InfoContext(ctx, "Pooler manager reopened successfully after restore")
	return nil
}

// GetBackups retrieves backup information
func (pm *MultiPoolerManager) GetBackups(ctx context.Context, limit uint32) ([]*multipoolermanagerdata.BackupMetadata, error) {
	configPath := pm.getBackupConfigPath()
	stanzaName := pm.getBackupStanza()

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
		"--log-level-console=off", // Override console logging to prevent contaminating JSON output
		"info")

	// Use streamOutput to avoid blocking on large output
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

			// Extract final LSN (stop LSN) from backup
			finalLSN := ""
			if pgBackup.LSN != nil && pgBackup.LSN.Stop != "" {
				finalLSN = pgBackup.LSN.Stop
			}

			backups = append(backups, &multipoolermanagerdata.BackupMetadata{
				BackupId:   pgBackup.Label,
				Status:     status,
				TableGroup: tableGroup,
				Shard:      shard,
				FinalLsn:   finalLSN,
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
	LSN        *pgBackRestLSN      `json:"lsn,omitempty"` // LSN range for this backup
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
