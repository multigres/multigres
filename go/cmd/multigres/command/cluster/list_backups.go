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
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/multigres/multigres/go/cmd/multigres/command/admin"
	"github.com/multigres/multigres/go/common/constants"
	multiadminpb "github.com/multigres/multigres/go/pb/multiadmin"
)

// AddListBackupsCommand adds the list-backups subcommand to the cluster command
func AddListBackupsCommand(clusterCmd *cobra.Command) {
	cmd := &cobra.Command{
		Use:   "list-backups",
		Short: "List backup artifacts",
		Long:  "List backup artifacts from the backup repository via the multiadmin API.",
		RunE:  runListBackups,
	}

	cmd.Flags().String("database", "postgres", "Database name to list backups for")
	cmd.Flags().Uint32("limit", 0, "Maximum number of backups to return (0 = no limit)")

	clusterCmd.AddCommand(cmd)
}

func runListBackups(cmd *cobra.Command, args []string) error {
	database, _ := cmd.Flags().GetString("database")
	limit, _ := cmd.Flags().GetUint32("limit")

	// Create admin client
	client, err := admin.NewClient(cmd)
	if err != nil {
		return err
	}
	defer client.Close()

	// Create context with timeout
	//nolint:gocritic // CLI entry point - no parent context available
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	resp, err := client.GetBackups(ctx, &multiadminpb.GetBackupsRequest{
		Database:   database,
		TableGroup: constants.DefaultTableGroup,
		Shard:      constants.DefaultShard,
		Limit:      limit,
	})
	if err != nil {
		return fmt.Errorf("failed to list backups: %w", err)
	}

	if len(resp.Backups) == 0 {
		cmd.Println("No backups found.")
		return nil
	}

	// Calculate column widths based on data (start with header widths)
	colWidths := struct {
		backupID   int
		database   int
		backupType int
		status     int
		size       int
	}{
		backupID:   len("BACKUP ID"),
		database:   len("DATABASE"),
		backupType: len("TYPE"),
		status:     len("STATUS"),
		size:       len("SIZE"),
	}

	// Scan data to find max widths
	for _, b := range resp.Backups {
		colWidths.backupID = max(colWidths.backupID, len(b.BackupId))
		colWidths.database = max(colWidths.database, len(b.Database))
		colWidths.backupType = max(colWidths.backupType, len(b.Type))
		colWidths.status = max(colWidths.status, len(backupStatusToString(b.Status)))
		colWidths.size = max(colWidths.size, len(formatBytes(b.BackupSizeBytes)))
	}

	// Build format string
	format := fmt.Sprintf("%%-%ds  %%-%ds  %%-%ds  %%-%ds  %%s\n",
		colWidths.backupID, colWidths.database, colWidths.backupType, colWidths.status)

	// Calculate total width for separator
	totalWidth := colWidths.backupID + colWidths.database +
		colWidths.backupType + colWidths.status + colWidths.size + 8 // 8 for spacing (4 gaps × 2 spaces)

	// Print header
	cmd.Printf(format, "BACKUP ID", "DATABASE", "TYPE", "STATUS", "SIZE")
	cmd.Println(strings.Repeat("-", totalWidth))

	// Print each backup
	for _, b := range resp.Backups {
		status := backupStatusToString(b.Status)
		size := formatBytes(b.BackupSizeBytes)
		cmd.Printf(format, b.BackupId, b.Database, b.Type, status, size)
	}

	return nil
}

func backupStatusToString(status multiadminpb.BackupStatus) string {
	switch status {
	case multiadminpb.BackupStatus_BACKUP_STATUS_COMPLETE:
		return "complete"
	case multiadminpb.BackupStatus_BACKUP_STATUS_INCOMPLETE:
		return "incomplete"
	case multiadminpb.BackupStatus_BACKUP_STATUS_FAILED:
		return "failed"
	default:
		return "unknown"
	}
}

func formatBytes(bytes uint64) string {
	const (
		KB = 1024
		MB = KB * 1024
		GB = MB * 1024
	)

	switch {
	case bytes >= GB:
		return fmt.Sprintf("%.1f GB", float64(bytes)/float64(GB))
	case bytes >= MB:
		return fmt.Sprintf("%.1f MB", float64(bytes)/float64(MB))
	case bytes >= KB:
		return fmt.Sprintf("%.1f KB", float64(bytes)/float64(KB))
	default:
		return fmt.Sprintf("%d B", bytes)
	}
}
