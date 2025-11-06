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
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/multigres/multigres/go/mterrors"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	pgctldpb "github.com/multigres/multigres/go/pb/pgctldservice"
)

// RestoreOptions contains options for performing a restore
type RestoreOptions struct {
	BackupID    string // If empty, restore from the latest backup
	AsStandby   bool   // If true, restart as standby after restore (maintains replication)
	PrimaryHost string // Primary host for replication (required if AsStandby is true)
	PrimaryPort int32  // Primary port for replication (required if AsStandby is true)
}

// RestoreResult contains the result of a restore operation
type RestoreResult struct {
	// Currently empty, but can be extended with metadata about the restore
}

// RestoreShardFromBackup restores a shard from a backup
func RestoreShardFromBackup(ctx context.Context, pgctldClient pgctldpb.PgCtldClient, configPath, stanzaName, pgDataDir string, opts RestoreOptions) (*RestoreResult, error) {
	// Validate required parameters
	if pgctldClient == nil {
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "pgctld_client is required")
	}
	if configPath == "" {
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "config_path is required")
	}
	if stanzaName == "" {
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "stanza_name is required")
	}
	if pgDataDir == "" {
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "pg_data_dir is required")
	}
	if opts.AsStandby && (opts.PrimaryHost == "" || opts.PrimaryPort == 0) {
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "primary_host and primary_port required when restoring as standby")
	}

	// Step 1: Stop PostgreSQL server
	slog.InfoContext(ctx, "Stopping PostgreSQL before restore", "backup_id", opts.BackupID)
	stopCtx, stopCancel := context.WithTimeout(ctx, 2*time.Minute)
	defer stopCancel()

	_, err := pgctldClient.Stop(stopCtx, &pgctldpb.StopRequest{
		Mode: "fast", // Fast shutdown mode
	})
	if err != nil {
		return nil, mterrors.New(mtrpcpb.Code_INTERNAL,
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
	if opts.BackupID != "" {
		args = append(args, "--set="+opts.BackupID)
	}

	args = append(args, "restore")

	cmd := exec.CommandContext(restoreCtx, "pgbackrest", args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		// Attempt to restart PostgreSQL even if restore failed
		startCtx, startCancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer startCancel()
		_, _ = pgctldClient.Start(startCtx, &pgctldpb.StartRequest{})

		return nil, mterrors.New(mtrpcpb.Code_INTERNAL,
			fmt.Sprintf("pgbackrest restore failed: %v\nOutput: %s", err, string(output)))
	}

	// Step 2.5: If restoring as standby, restore primary_conninfo to postgresql.auto.conf
	// (pgbackrest restore overwrites postgresql.auto.conf, removing replication settings)
	if opts.AsStandby {
		autoConfPath := filepath.Join(pgDataDir, "postgresql.auto.conf")
		slog.InfoContext(ctx, "Restoring primary_conninfo to postgresql.auto.conf",
			"path", autoConfPath,
			"primary_host", opts.PrimaryHost,
			"primary_port", opts.PrimaryPort)

		// Read existing content to preserve other settings
		existingContent, err := os.ReadFile(autoConfPath)
		if err != nil && !os.IsNotExist(err) {
			slog.WarnContext(ctx, "Failed to read existing postgresql.auto.conf", "error", err)
			existingContent = []byte{}
		}

		// Append primary_conninfo setting
		primaryConnInfo := fmt.Sprintf("\n# Restored by multigres after backup restore\nprimary_conninfo = 'host=%s port=%d user=postgres'\n",
			opts.PrimaryHost, opts.PrimaryPort)

		newContent := append(existingContent, []byte(primaryConnInfo)...)

		err = os.WriteFile(autoConfPath, newContent, 0o600)
		if err != nil {
			return nil, mterrors.New(mtrpcpb.Code_INTERNAL,
				fmt.Sprintf("failed to write primary_conninfo to postgresql.auto.conf: %v", err))
		}

		slog.InfoContext(ctx, "Successfully restored primary_conninfo to postgresql.auto.conf")
	}

	// Step 3: Restart PostgreSQL server after successful restore
	// Use Restart instead of Start to properly handle standby.signal creation
	restartCtx, restartCancel := context.WithTimeout(ctx, 2*time.Minute)
	defer restartCancel()

	slog.InfoContext(ctx, "Restarting PostgreSQL after restore",
		"as_standby", opts.AsStandby,
		"backup_id", opts.BackupID)

	_, err = pgctldClient.Restart(restartCtx, &pgctldpb.RestartRequest{
		AsStandby: opts.AsStandby, // Maintains standby status if restoring to a replica
	})
	if err != nil {
		return nil, mterrors.New(mtrpcpb.Code_INTERNAL,
			fmt.Sprintf("failed to restart PostgreSQL after restore: %v", err))
	}

	slog.InfoContext(ctx, "PostgreSQL restarted successfully after restore")

	return &RestoreResult{}, nil
}
