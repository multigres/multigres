// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
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
	"strings"

	commonbackup "github.com/multigres/multigres/go/common/backup"
	"github.com/multigres/multigres/go/common/mterrors"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolermanagerdata "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// List retrieves backup metadata, applying the given limit when non-zero.
func (e *Engine) List(ctx context.Context, limit uint32) ([]*multipoolermanagerdata.BackupMetadata, error) {
	backups, err := e.ListBackups(ctx)
	if err != nil {
		return nil, err
	}

	// Apply limit if specified
	if limit > 0 && uint32(len(backups)) > limit {
		backups = backups[:limit]
	}

	return backups, nil
}

// ListBackups retrieves backup metadata from pgbackrest.
func (e *Engine) ListBackups(ctx context.Context) ([]*multipoolermanagerdata.BackupMetadata, error) {
	configPath, err := e.requireConfigPath()
	if err != nil {
		return nil, mterrors.Wrap(err, "pgbackrest config not found")
	}

	if _, err := e.requireBackupConfig(); err != nil {
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "backup config not loaded from topology")
	}

	// Execute pgbackrest info command with JSON output
	queryCtx, cancel := context.WithTimeout(ctx, commonbackup.InfoTimeout)
	defer cancel()

	cmd := e.pgbackrestCmd(queryCtx,
		"--stanza="+stanzaName,
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
	currentTableGroup := e.id.ShardKey().GetTableGroup()
	currentShard := e.shardID()

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
			e.logger.ErrorContext(ctx, "Skipping backup with mismatched table_group",
				"backup_id", pgBackup.Label,
				"backup_table_group", tableGroup,
				"current_table_group", currentTableGroup)
			continue
		}
		if shard != currentShard {
			e.logger.ErrorContext(ctx, "Skipping backup with mismatched shard",
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

// FindByJobID finds a backup by matching the job_id annotation
//
// This function has to scan the entire backup history to find the backup, but the worst case
// should be manageable, because production deployments should have retention policies that
// trigger pgBackRest to delete older backups.
func (e *Engine) FindByJobID(
	ctx context.Context,
	jobID string,
) (string, error) {
	configPath, err := e.requireConfigPath()
	if err != nil {
		return "", mterrors.Wrap(err, "pgbackrest config not found")
	}

	// Execute pgbackrest info command with JSON output
	infoCtx, cancel := context.WithTimeout(ctx, commonbackup.InfoTimeout)
	defer cancel()

	cmd := e.pgbackrestCmd(infoCtx,
		"--stanza="+stanzaName,
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
