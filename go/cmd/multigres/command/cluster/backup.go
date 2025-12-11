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
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/multigres/multigres/go/cmd/multigres/command/topo"
	"github.com/multigres/multigres/go/common/constants"
	multiadminpb "github.com/multigres/multigres/go/pb/multiadmin"
)

const (
	backupPollInterval = 2 * time.Second
	backupPollTimeout  = 30 * time.Minute
)

// AddBackupCommand adds the backup subcommand to the cluster command
func AddBackupCommand(clusterCmd *cobra.Command) {
	cmd := &cobra.Command{
		Use:   "backup",
		Short: "Trigger a backup",
		Long:  "Trigger a backup via the multiadmin API and wait for completion.",
		RunE:  runBackup,
	}

	cmd.Flags().String("database", "postgres", "Database name")
	cmd.Flags().String("type", "full", "Backup type: full, differential, or incremental")
	cmd.Flags().Bool("primary", false, "Force backup on primary instead of replica (use with caution)")

	clusterCmd.AddCommand(cmd)
}

func runBackup(cmd *cobra.Command, args []string) error {
	database, _ := cmd.Flags().GetString("database")
	backupType, _ := cmd.Flags().GetString("type")
	primary, _ := cmd.Flags().GetBool("primary")

	// Resolve admin server address from config
	adminServer, err := topo.GetAdminServerAddress(cmd)
	if err != nil {
		return err
	}

	// Create gRPC connection
	conn, err := grpc.NewClient(adminServer, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to connect to admin server at %s: %w", adminServer, err)
	}
	defer conn.Close()

	client := multiadminpb.NewMultiAdminServiceClient(conn)

	// Start backup
	if primary {
		cmd.Printf("Starting backup on PRIMARY for database=%s...\n", database)
	} else {
		cmd.Printf("Starting backup for database=%s...\n", database)
	}

	// Create context with timeout for the initial Backup call
	//nolint:gocritic // CLI entry point - no parent context available
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
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
			//nolint:gocritic // CLI entry point - no parent context available
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
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
