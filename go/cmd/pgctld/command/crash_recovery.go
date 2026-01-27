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
	"log/slog"
	"os/exec"
	"strings"
)

// needsCrashRecovery checks if PostgreSQL requires crash recovery.
// Returns (needsRecovery bool, clusterState string, error)
func needsCrashRecovery(ctx context.Context, logger *slog.Logger, poolerDir string) (bool, string, error) {
	cmd := exec.CommandContext(ctx, "pg_controldata", poolerDir+"/pg_data")
	output, err := cmd.CombinedOutput()
	if err != nil {
		return false, "", fmt.Errorf("pg_controldata failed: %w (output: %s)", err, string(output))
	}

	outputStr := string(output)

	// Extract the database cluster state line
	clusterState := extractClusterState(outputStr)

	// For PostgreSQL 17+, check database cluster state
	// States that indicate need for crash recovery:
	// - "in production" - was running when killed
	// - "shutting down" - was shutting down when killed
	// - "in crash recovery" - already in crash recovery
	// Clean states: "shut down", "shut down in recovery"

	needsRecovery := strings.Contains(clusterState, "in production") ||
		strings.Contains(clusterState, "shutting down") ||
		strings.Contains(clusterState, "in crash recovery")

	if needsRecovery {
		logger.InfoContext(ctx, "Database cluster state indicates crash recovery needed",
			"cluster_state", clusterState)
	} else {
		logger.InfoContext(ctx, "Database cluster state is clean",
			"cluster_state", clusterState)
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

// runCrashRecovery performs crash recovery in single-user mode.
// This runs postgres --single to complete crash recovery, then exits cleanly.
func runCrashRecovery(ctx context.Context, logger *slog.Logger, poolerDir string) error {
	logger.InfoContext(ctx, "Starting single-user crash recovery")

	// Run postgres in single-user mode to perform crash recovery
	// postgres --single starts in single-user mode, performs recovery, and exits on EOF
	cmd := exec.CommandContext(ctx, "postgres", "--single", "-D", poolerDir+"/pg_data", "postgres")

	// Create stdin pipe - postgres --single reads from stdin until EOF
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return fmt.Errorf("failed to create stdin pipe: %w", err)
	}

	// Start the command
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start postgres --single: %w", err)
	}

	// Close stdin immediately to signal EOF (causes postgres to exit after recovery)
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
