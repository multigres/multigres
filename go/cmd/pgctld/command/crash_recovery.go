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

package command

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/multigres/multigres/go/services/pgctld"
	"github.com/multigres/multigres/go/tools/executil"
)

// postgresAlreadyRunningPattern matches postgres error when it's already running
// Pattern: FATAL: lock file "<filename>" already exists
var postgresAlreadyRunningPattern = regexp.MustCompile(`lock file ".*" already exists`)

// ExtractClusterState extracts the cluster state from pg_controldata output.
// The returned string is the trimmed value after "Database cluster state:".
func ExtractClusterState(output string) string {
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

// IsCleanClusterState reports whether the pg_controldata cluster state indicates
// a clean shutdown. The clean states are "shut down" and "shut down in recovery".
// Any other state (e.g. "in production", "in archive recovery") requires crash recovery
// before pg_rewind can run.
func IsCleanClusterState(state string) bool {
	return state == "shut down" || state == "shut down in recovery"
}

// isPostgresCleanlyStopped checks if PostgreSQL is in a clean shutdown state.
// Returns true if state is "shut down" or "shut down in recovery", false otherwise.
func isPostgresCleanlyStopped(ctx context.Context) (bool, error) {
	cmd := executil.Command(ctx, "pg_controldata", pgctld.PostgresDataDir())
	output, err := cmd.CombinedOutput()
	if err != nil {
		return false, fmt.Errorf("pg_controldata failed: %w (output: %s)", err, string(output))
	}

	return IsCleanClusterState(ExtractClusterState(string(output))), nil
}

// runCrashRecovery performs crash recovery in single-user mode.
// This runs postgres --single to complete crash recovery, then exits cleanly.
func runCrashRecovery(ctx context.Context, logger *slog.Logger) error {
	logger.InfoContext(ctx, "Starting single-user crash recovery")

	// Remove standby.signal if present. Single-user mode (postgres --single)
	// does not support standby mode — PostgreSQL exits with FATAL if
	// standby.signal exists when running as a standalone backend. Since
	// StartAsStandby always writes standby.signal before starting, any
	// previously started node will have this file. The caller re-creates it
	// via StartAsStandby after crash recovery completes.
	standbySignalPath := filepath.Join(pgctld.PostgresDataDir(), "standby.signal")
	if err := os.Remove(standbySignalPath); err == nil {
		logger.InfoContext(ctx, "Removed standby.signal before crash recovery", "path", standbySignalPath)
	} else if !os.IsNotExist(err) {
		logger.WarnContext(ctx, "Failed to remove standby.signal before crash recovery (continuing)", "error", err)
	}

	// Run postgres in single-user mode to perform crash recovery
	// postgres --single starts in single-user mode, performs recovery, and exits on EOF
	// Using /dev/null for stdin is simpler than pipe management
	cmd := executil.Command(ctx, "postgres", "--single", "-D", pgctld.PostgresDataDir(), "template1")

	// Open /dev/null for stdin
	devNull, err := os.Open("/dev/null")
	if err != nil {
		return fmt.Errorf("failed to open /dev/null: %w", err)
	}
	defer devNull.Close()

	cmd.SetStdin(devNull)

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
