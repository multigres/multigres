// Copyright 2025 Supabase, Inc.
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

package cluster

import (
	"context"
	"fmt"
	"time"

	"github.com/spf13/cobra"

	"github.com/multigres/multigres/go/cmd/multigres/command/admin"
	"github.com/multigres/multigres/go/common/constants"
	multiadminpb "github.com/multigres/multigres/go/pb/multiadmin"
	"github.com/multigres/multigres/go/tools/viperutil"
)

const (
	backupPollInterval = 2 * time.Second
	backupPollTimeout  = 30 * time.Minute
)

// backupCmd holds the backup command configuration
type backupCmd struct {
	database   viperutil.Value[string]
	backupType viperutil.Value[string]
	primary    viperutil.Value[bool]
	timeout    viperutil.Value[time.Duration]
}

// AddBackupCommand adds the backup subcommand to the cluster command
func AddBackupCommand(clusterCmd *cobra.Command) {
	// Create a viperutil registry for backup command flags
	reg := viperutil.NewRegistry()

	bcmd := &backupCmd{
		database: viperutil.Configure(reg, "database", viperutil.Options[string]{
			Default:  "postgres",
			FlagName: "database",
			Dynamic:  false,
		}),
		backupType: viperutil.Configure(reg, "type", viperutil.Options[string]{
			Default:  "full",
			FlagName: "type",
			Dynamic:  false,
		}),
		primary: viperutil.Configure(reg, "primary", viperutil.Options[bool]{
			Default:  false,
			FlagName: "primary",
			Dynamic:  false,
		}),
		timeout: viperutil.Configure(reg, "timeout", viperutil.Options[time.Duration]{
			Default:  30 * time.Second,
			FlagName: "timeout",
			Dynamic:  false,
		}),
	}

	cmd := &cobra.Command{
		Use:   "backup",
		Short: "Trigger a backup",
		Long:  "Trigger a backup via the multiadmin API and wait for completion.",
		RunE:  bcmd.runBackup,
	}

	cmd.Flags().String("database", bcmd.database.Default(), "Database name")
	cmd.Flags().String("type", bcmd.backupType.Default(), "Backup type: full, differential, or incremental")
	cmd.Flags().Bool("primary", bcmd.primary.Default(), "Force backup on primary instead of replica (use with caution)")
	cmd.Flags().Duration("timeout", bcmd.timeout.Default(), "Timeout for initial backup call")
	cmd.Flags().String("admin-server", "", "host:port of the multiadmin server (overrides config)")

	viperutil.BindFlags(cmd.Flags(), bcmd.database, bcmd.backupType, bcmd.primary, bcmd.timeout)

	clusterCmd.AddCommand(cmd)
}

func (bcmd *backupCmd) runBackup(cmd *cobra.Command, args []string) error {
	database := bcmd.database.Get()
	backupType := bcmd.backupType.Get()
	primary := bcmd.primary.Get()
	timeout := bcmd.timeout.Get()

	// Create admin client
	client, err := admin.NewClient(cmd)
	if err != nil {
		return err
	}
	defer client.Close()

	// Start backup
	if primary {
		cmd.Printf("Starting backup on PRIMARY for database=%s...\n", database)
	} else {
		cmd.Printf("Starting backup for database=%s...\n", database)
	}

	// Create context with timeout for the initial Backup call
	ctx, cancel := context.WithTimeout(cmd.Context(), timeout)
	defer cancel()

	backupResp, err := client.Backup(ctx, &multiadminpb.BackupRequest{
		Database:     database,
		TableGroup:   constants.DefaultTableGroup,
		Shard:        constants.DefaultShard,
		Type:         backupType,
		ForcePrimary: primary,
	})
	if err != nil {
		return fmt.Errorf("failed to start backup: %w", err)
	}

	jobID := backupResp.JobId
	cmd.Printf("Backup job started: %s\n", jobID)

	// Poll for completion
	return pollBackupJobStatus(cmd, client, jobID)
}

func pollBackupJobStatus(cmd *cobra.Command, client multiadminpb.MultiAdminServiceClient, jobID string) error {
	ticker := time.NewTicker(backupPollInterval)
	defer ticker.Stop()

	timeout := time.After(backupPollTimeout)

	for {
		select {
		case <-timeout:
			return fmt.Errorf("backup job %s timed out after %v", jobID, backupPollTimeout)
		case <-ticker.C:
			// Create a fresh timeout context for each GetBackupJobStatus call
			ctx, cancel := context.WithTimeout(cmd.Context(), 10*time.Second)
			resp, err := client.GetBackupJobStatus(ctx, &multiadminpb.GetBackupJobStatusRequest{
				JobId: jobID,
			})
			cancel()
			if err != nil {
				return fmt.Errorf("failed to get job status: %w", err)
			}

			switch resp.Status {
			case multiadminpb.JobStatus_JOB_STATUS_COMPLETED:
				cmd.Printf("Backup completed successfully. Backup ID: %s\n", resp.BackupId)
				return nil
			case multiadminpb.JobStatus_JOB_STATUS_FAILED:
				return fmt.Errorf("backup failed: %s", resp.ErrorMessage)
			case multiadminpb.JobStatus_JOB_STATUS_RUNNING:
				cmd.Printf("Backup in progress...\n")
			case multiadminpb.JobStatus_JOB_STATUS_PENDING:
				cmd.Printf("Backup pending...\n")
			default:
				cmd.Printf("Unexpected job status: %v, continuing to poll...\n", resp.Status)
			}
		}
	}
}
