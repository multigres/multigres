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

package command

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os/exec"
	"strings"
	"time"

	pb "github.com/multigres/multigres/go/pb/pgctldservice"
)

// needsCrashRecovery checks if PostgreSQL requires crash recovery.
// Returns (needsRecovery bool, clusterState enum, error)
func needsCrashRecovery(ctx context.Context, logger *slog.Logger, poolerDir string) (bool, pb.DatabaseClusterState, error) {
	cmd := exec.CommandContext(ctx, "pg_controldata", poolerDir+"/pg_data")
	output, err := cmd.CombinedOutput()
	if err != nil {
		return false, pb.DatabaseClusterState_DATABASE_CLUSTER_STATE_UNKNOWN, fmt.Errorf("pg_controldata failed: %w (output: %s)", err, string(output))
	}

	outputStr := string(output)

	// Extract the database cluster state line
	clusterStateStr := extractClusterState(outputStr)
	clusterState := stringToClusterState(clusterStateStr)

	// Guard against unknown/unspecified states - fail safely rather than assuming clean
	if clusterState == pb.DatabaseClusterState_DATABASE_CLUSTER_STATE_UNKNOWN ||
		clusterState == pb.DatabaseClusterState_DATABASE_CLUSTER_STATE_UNSPECIFIED {
		return false, clusterState, fmt.Errorf("unable to determine database cluster state from pg_controldata output (got: %q)", clusterStateStr)
	}

	// For PostgreSQL 17+, check database cluster state
	// States that indicate need for crash recovery:
	// - "in production" - was running when killed
	// - "shutting down" - was shutting down when killed
	// - "in crash recovery" - already in crash recovery
	// Clean states: "shut down", "shut down in recovery"

	needsRecovery := clusterState == pb.DatabaseClusterState_DATABASE_CLUSTER_STATE_IN_PRODUCTION ||
		clusterState == pb.DatabaseClusterState_DATABASE_CLUSTER_STATE_SHUTTING_DOWN ||
		clusterState == pb.DatabaseClusterState_DATABASE_CLUSTER_STATE_IN_CRASH_RECOVERY

	if needsRecovery {
		logger.InfoContext(ctx, "Database cluster state indicates crash recovery needed",
			"cluster_state", clusterState.String())
	} else {
		logger.InfoContext(ctx, "Database cluster state is clean",
			"cluster_state", clusterState.String())
	}

	return needsRecovery, clusterState, nil
}

// extractClusterState extracts the cluster state from pg_controldata output
func extractClusterState(output string) string {
	for line := range strings.SplitSeq(output, "\n") {
		if strings.Contains(line, "Database cluster state:") {
			// Format: "Database cluster state:               in production"
			parts := strings.Split(line, ":")
			if len(parts) >= 2 {
				return strings.TrimSpace(parts[1])
			}
		}
	}
	return "unknown"
}

// stringToClusterState converts a pg_controldata state string to the enum
func stringToClusterState(state string) pb.DatabaseClusterState {
	switch state {
	case "shut down":
		return pb.DatabaseClusterState_DATABASE_CLUSTER_STATE_SHUT_DOWN
	case "shut down in recovery":
		return pb.DatabaseClusterState_DATABASE_CLUSTER_STATE_SHUT_DOWN_IN_RECOVERY
	case "in production":
		return pb.DatabaseClusterState_DATABASE_CLUSTER_STATE_IN_PRODUCTION
	case "shutting down":
		return pb.DatabaseClusterState_DATABASE_CLUSTER_STATE_SHUTTING_DOWN
	case "in crash recovery":
		return pb.DatabaseClusterState_DATABASE_CLUSTER_STATE_IN_CRASH_RECOVERY
	default:
		return pb.DatabaseClusterState_DATABASE_CLUSTER_STATE_UNKNOWN
	}
}

// runCrashRecovery performs crash recovery in single-user mode.
// This runs postgres --single to complete crash recovery, then exits cleanly.
func runCrashRecovery(ctx context.Context, logger *slog.Logger, poolerDir string) error {
	logger.InfoContext(ctx, "Starting single-user crash recovery")

	crashCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	// Run postgres in single-user mode to perform crash recovery
	// postgres --single starts in single-user mode, performs recovery, and exits on EOF
	cmd := exec.CommandContext(crashCtx, "postgres", "--single", "-D", poolerDir+"/pg_data", "postgres")

	// Create stdin pipe - postgres --single reads from stdin until EOF
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return fmt.Errorf("failed to create stdin pipe: %w", err)
	}

	// Start the command
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start postgres --single: %w", err)
	}

	// Write a newline to stdin. postgres --single reads commands line-by-line,
	// so this acts as a no-op command. Unlike time.Sleep, this naturally blocks
	// until postgres has attached to the pipe and consumed the input.
	if _, err := io.WriteString(stdin, "\n"); err != nil {
		return fmt.Errorf("failed to write to postgres stdin: %w", err)
	}

	// Close stdin to signal EOF (causes postgres to exit after recovery)
	stdin.Close()

	// Wait for the command to complete
	err = cmd.Wait()
	if err != nil {
		logger.ErrorContext(ctx, "Single-user crash recovery failed",
			"error", err)
		return fmt.Errorf("crash recovery failed: %w", err)
	}

	logger.InfoContext(ctx, "Single-user crash recovery completed successfully")
	return nil
}
