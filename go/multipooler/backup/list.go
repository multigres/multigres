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

// ListOptions contains options for listing backups
type ListOptions struct {
	Limit uint32 // Maximum number of backups to return, 0 means no limit
}

// ListResult contains the result of a backup listing operation
type ListResult struct {
	Backups []*backupservicepb.BackupMetadata
}

// GetShardBackups retrieves backup information for a shard
func GetShardBackups(ctx context.Context, configPath, stanzaName string, opts ListOptions) (*ListResult, error) {
	// TODO: Implement backup listing logic
	// This should:
	// 1. Query pgBackRest for available backups
	//    - Execute: pgbackrest info --output=json --stanza=<stanza_name>
	//    - This returns detailed information about all backups for the stanza
	//    - Parse the JSON output to extract backup information
	//
	// 2. Parse the backup information from pgBackRest
	//    - pgBackRest info JSON structure typically includes:
	//      {
	//        "name": "stanza-name",
	//        "backup": [
	//          {
	//            "label": "20250104-100000F",
	//            "type": "full",
	//            "timestamp": {...},
	//            "database": {...},
	//            "archive": {...}
	//          }
	//        ]
	//      }
	//    - Extract relevant fields: label (backup_id), type, timestamp, status
	//
	// 3. Convert to BackupMetadata format
	//    - Map pgBackRest backup info to protobuf BackupMetadata structure:
	//      - backup_id: backup label from pgBackRest (e.g., "20250104-100000F")
	//      - table_group: from the context/config
	//      - shard: from the context/config
	//      - status: determine based on backup state
	//        - COMPLETE: backup finished successfully
	//        - INCOMPLETE: backup is in progress or was interrupted
	//        - UNKNOWN: unable to determine status
	//
	// 4. Sort backups by timestamp (most recent first)
	//    - pgBackRest typically returns backups in order, but verify
	//    - Ensure newest backups appear first in the list
	//
	// 5. Apply the limit if specified
	//    - If opts.Limit > 0, truncate the result to at most Limit backups
	//    - This helps with pagination and performance
	//
	// 6. Return the list of backups
	//    - Return the BackupMetadata array wrapped in ListResult
	//    - If no backups exist, return empty array (not an error)
	//
	// 7. Error handling
	//    - If pgbackrest command fails, return appropriate error
	//    - If JSON parsing fails, log and return error
	//    - Handle "stanza not found" gracefully (return empty list)

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
			return &ListResult{Backups: []*backupservicepb.BackupMetadata{}}, nil
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

			backups = append(backups, &backupservicepb.BackupMetadata{
				BackupId: pgBackup.Label,
				Status:   status,
				// TODO: Add table_group and shard fields when we have access to them
			})
		}
	}

	// Apply limit if specified
	if opts.Limit > 0 && uint32(len(backups)) > opts.Limit {
		backups = backups[:opts.Limit]
	}

	return &ListResult{Backups: backups}, nil
}

// pgBackRestInfo represents the structure of pgbackrest info JSON output
type pgBackRestInfo struct {
	Name   string             `json:"name"`
	Backup []pgBackRestBackup `json:"backup"`
}

// pgBackRestBackup represents a single backup in the info output
type pgBackRestBackup struct {
	Label     string              `json:"label"`
	Type      string              `json:"type"`
	Error     bool                `json:"error"`
	Timestamp pgBackRestTimestamp `json:"timestamp"`
}

// pgBackRestTimestamp represents backup timestamps
type pgBackRestTimestamp struct {
	Start int64 `json:"start"`
	Stop  int64 `json:"stop"`
}
