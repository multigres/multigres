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
	"regexp"
	"strings"
	"time"

	"github.com/multigres/multigres/go/services/pgctld"
	"github.com/multigres/multigres/go/tools/executil"
)

// postgresAlreadyRunningPattern matches the postgres error reported when the
// postmaster.pid lock file is held. After a postmaster crash (kill -9 of a single
// PID, OOM, segfault), orphaned worker processes (writer, checkpointer,
// walreceiver, bgwriter) keep the SHM segment attached for ~1-5s while they
// detect parent death via PostmasterIsAlive() and exit. The same error is
// reported during that window even though postgres is not actually running.
var postgresAlreadyRunningPattern = regexp.MustCompile(`lock file ".*" already exists`)

// Bounds the retry window used to wait out the orphan-cleanup race after a
// postmaster crash. Suggested by MUL-394: ~5s covers the worst-case worker
// PostmasterIsAlive() detection latency observed in practice.
const (
	crashRecoveryMaxAttempts = 10
	crashRecoveryRetryDelay  = 500 * time.Millisecond
)

// isPostgresCleanlyStopped checks if PostgreSQL is in a clean shutdown state.
// Returns true if state is "shut down" or "shut down in recovery", false otherwise.
func isPostgresCleanlyStopped(ctx context.Context) (bool, error) {
	cmd := executil.Command(ctx, "pg_controldata", pgctld.PostgresDataDir())
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
func runCrashRecovery(ctx context.Context, logger *slog.Logger) error {
	return runCrashRecoveryAttempts(ctx, logger, runSingleUserPostgres, crashRecoveryRetryDelay)
}

// runCrashRecoveryAttempts retries `postgres --single` while the lock file is held.
// During the orphan-cleanup window after a postmaster crash, the lock will eventually
// release; if it does not within the retry window, postgres is genuinely running and
// we preserve the historical no-op behavior. Extracted for unit-test injection.
func runCrashRecoveryAttempts(
	ctx context.Context,
	logger *slog.Logger,
	run func(context.Context) ([]byte, error),
	retryDelay time.Duration,
) error {
	logger.InfoContext(ctx, "Starting single-user crash recovery")

	var lastOutput string
	for attempt := 1; attempt <= crashRecoveryMaxAttempts; attempt++ {
		output, err := run(ctx)
		if err == nil {
			return nil
		}

		outputStr := string(output)
		lastOutput = outputStr

		if !postgresAlreadyRunningPattern.MatchString(outputStr) {
			logger.WarnContext(ctx, "Single-user crash recovery failed",
				"error", err,
				"output", outputStr)
			return fmt.Errorf("crash recovery failed: %w", err)
		}

		if attempt < crashRecoveryMaxAttempts {
			logger.InfoContext(ctx, "Single-user crash recovery: lock file held, retrying",
				"attempt", attempt,
				"max_attempts", crashRecoveryMaxAttempts,
				"output", outputStr)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(retryDelay):
			}
		}
	}

	logger.InfoContext(ctx, "Single-user crash recovery not needed, postgres is already running",
		"attempts", crashRecoveryMaxAttempts,
		"output", lastOutput)
	return nil
}

// runSingleUserPostgres runs `postgres --single` once and returns its combined
// output and exit error. /dev/null on stdin causes single-user mode to perform
// recovery and exit on EOF.
func runSingleUserPostgres(ctx context.Context) ([]byte, error) {
	cmd := executil.Command(ctx, "postgres", "--single", "-D", pgctld.PostgresDataDir(), "template1")

	devNull, err := os.Open("/dev/null")
	if err != nil {
		return nil, fmt.Errorf("failed to open /dev/null: %w", err)
	}
	defer devNull.Close()

	cmd.SetStdin(devNull)
	return cmd.CombinedOutput()
}
