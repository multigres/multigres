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

package backup

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"
	"time"

	"github.com/multigres/multigres/go/mterrors"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	backupservicepb "github.com/multigres/multigres/go/pb/multipoolerbackupservice"
)

// GetBackupsOptions contains options for listing backups
type GetBackupsOptions struct {
	Limit uint32 // Maximum number of backups to return, 0 means no limit
}

// GetBackupsResult contains the result of a backup listing operation
type GetBackupsResult struct {
	Backups []*backupservicepb.BackupMetadata
}

// GetBackups retrieves backup information for a shard
func GetBackups(ctx context.Context, configPath, stanzaName string, opts GetBackupsOptions) (*GetBackupsResult, error) {
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
			return &GetBackupsResult{Backups: []*backupservicepb.BackupMetadata{}}, nil
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
	var backups []*backupservicepb.BackupMetadata
	if len(infoData) > 0 && len(infoData[0].Backup) > 0 {
		for _, pgBackup := range infoData[0].Backup {
			// Determine backup status
			status := backupservicepb.BackupMetadata_COMPLETE
			if pgBackup.Error {
				status = backupservicepb.BackupMetadata_INCOMPLETE
			}

			// Extract table_group and shard from annotations
			tableGroup := ""
			shard := ""
			if pgBackup.Annotation != nil {
				tableGroup = pgBackup.Annotation["table_group"]
				shard = pgBackup.Annotation["shard"]
			}

			backups = append(backups, &backupservicepb.BackupMetadata{
				BackupId:   pgBackup.Label,
				Status:     status,
				TableGroup: tableGroup,
				Shard:      shard,
			})
		}
	}

	// Apply limit if specified
	if opts.Limit > 0 && uint32(len(backups)) > opts.Limit {
		backups = backups[:opts.Limit]
	}

	return &GetBackupsResult{Backups: backups}, nil
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
