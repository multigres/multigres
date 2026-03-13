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
	"os"
	"os/exec"
	"regexp"
	"strings"
)

// postgresAlreadyRunningPattern matches postgres error when it's already running
// Pattern: FATAL: lock file "<filename>" already exists
var postgresAlreadyRunningPattern = regexp.MustCompile(`lock file ".*" already exists`)

// isPostgresCleanlyStopped checks if PostgreSQL is in a clean shutdown state.
// Returns true if state is "shut down" or "shut down in recovery", false otherwise.
func isPostgresCleanlyStopped(ctx context.Context, poolerDir string) (bool, error) {
	cmd := exec.CommandContext(ctx, "pg_controldata", poolerDir+"/pg_data")
	output, err := cmd.CombinedOutput()
	if err != nil {
		return false, fmt.Errorf("pg_controldata failed: %w (output: %s)", err, string(output))
	}

	outputStr := string(output)
	clusterStateStr := extractClusterState(outputStr)

	// Clean states: "shut down", "shut down in recovery"
	// Anything else means we should try crash recovery
	cleanlyStopped := clusterStateStr == "shut down" || clusterStateStr == "shut down in recovery"

	return cleanlyStopped, nil
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
	// Using /dev/null for stdin is simpler than pipe management
	cmd := exec.CommandContext(ctx, "postgres", "--single", "-D", poolerDir+"/pg_data", "template1")

	// Open /dev/null for stdin
	devNull, err := os.Open("/dev/null")
	if err != nil {
		return fmt.Errorf("failed to open /dev/null: %w", err)
	}
	defer devNull.Close()

	cmd.Stdin = devNull

	// Run the command and wait for completion
	output, err := cmd.CombinedOutput()
	if err != nil {
		outputStr := string(output)

		if postgresAlreadyRunningPattern.MatchString(outputStr) {
			logger.InfoContext(ctx, "Single-user crash recovery not needed, postgres is already running",
				"error", err,
				"output", outputStr)
			return nil
		}

		logger.WarnContext(ctx, "Single-user crash recovery failed",
			"error", err,
			"output", outputStr)
		return fmt.Errorf("crash recovery failed: %w", err)
	}

	return nil
}
