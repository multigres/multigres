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

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/services/pgctld"
	"github.com/multigres/multigres/go/tools/executil"
	"github.com/multigres/multigres/go/tools/retry"
)

// postgresAlreadyRunningPattern matches the postgres error reported when the
// postmaster.pid lock file is held. After a postmaster crash (kill -9 of a single
// PID, OOM, segfault), orphaned worker processes (writer, checkpointer,
// walreceiver, bgwriter) keep the SHM segment attached for ~1-5s while they
// detect parent death via PostmasterIsAlive() and exit. The same error is
// reported during that window even though postgres is not actually running.
var postgresAlreadyRunningPattern = regexp.MustCompile(`lock file ".*" already exists`)

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

// crashRecoveryNeeded reports whether PostgreSQL is not cleanly shut down and so
// must be crash recovered before it can start again.
func crashRecoveryNeeded(ctx context.Context) (bool, error) {
	cleanlyStopped, err := isPostgresCleanlyStopped(ctx)
	if err != nil {
		return false, err
	}
	return !cleanlyStopped, nil
}

// standbySignalPath returns the path to the standby.signal marker file inside dataDir.
func standbySignalPath(dataDir string) string {
	return filepath.Join(dataDir, constants.StandbySignalFile)
}

// hasStandbySignal reports whether a standby.signal marker file is present, i.e.
// PostgreSQL is configured to start in standby mode.
func hasStandbySignal() bool {
	_, err := os.Stat(standbySignalPath(pgctld.PostgresDataDir()))
	return err == nil
}

// createStandbySignal creates an empty standby.signal file in dataDir so that
// PostgreSQL comes up in recovery (standby) mode instead of as a writable
// primary. The write truncates any existing file, so it is idempotent.
func createStandbySignal(logger *slog.Logger, dataDir string) (string, error) {
	path := standbySignalPath(dataDir)
	if err := os.WriteFile(path, []byte(""), 0o644); err != nil {
		return path, fmt.Errorf("failed to create standby.signal: %w", err)
	}
	logger.Info("standby.signal created successfully", "path", path)
	return path, nil
}

// removeStandbySignal removes standby.signal from dataDir so that PostgreSQL
// starts as a writable primary instead of recovering as a standby. A no-op if
// the file does not exist.
func removeStandbySignal(logger *slog.Logger, dataDir string) (string, error) {
	path := standbySignalPath(dataDir)
	if err := os.Remove(path); err != nil {
		if os.IsNotExist(err) {
			return path, nil
		}
		return path, fmt.Errorf("failed to remove standby.signal: %w", err)
	}
	logger.Info("standby.signal removed successfully", "path", path)
	return path, nil
}

// runCrashRecovery performs crash recovery in single-user mode (postgres --single),
// which replays WAL to a clean shutdown and exits.
//
// A standby.signal blocks single-user mode ("standby mode is not supported by
// single-user servers"), so when one is present it is removed for the duration of
// recovery and recreated afterwards, preserving the node's standby identity. Both
// the start and rewind paths share this primitive, so a standby that can only be
// cleaned up via single-user recovery — e.g. one wedged by an early pg_rewind that
// stamped minRecoveryPoint onto the wrong timeline — is handled consistently
// (notably, this lets a re-issued pg_rewind clean-shut-down such a node).
func runCrashRecovery(ctx context.Context, logger *slog.Logger) error {
	r := retry.New(constants.CrashRecoveryRetryDelay, constants.CrashRecoveryRetryDelay)
	return runCrashRecoveryInDir(ctx, logger, pgctld.PostgresDataDir(), runSingleUserPostgres, r)
}

// runCrashRecoveryInDir is runCrashRecovery with the data directory and single-user
// runner injected, so the standby.signal save/restore can be unit-tested without a
// real postgres. Extracted for testing.
func runCrashRecoveryInDir(
	ctx context.Context,
	logger *slog.Logger,
	dataDir string,
	run func(context.Context) ([]byte, error),
	r *retry.Retry,
) error {
	signalPath := standbySignalPath(dataDir)
	if _, err := os.Stat(signalPath); err == nil {
		logger.InfoContext(ctx, "Temporarily removing standby.signal for single-user crash recovery",
			"path", signalPath)
		if _, rmErr := removeStandbySignal(logger, dataDir); rmErr != nil {
			return fmt.Errorf("failed to remove standby.signal before crash recovery: %w", rmErr)
		}
		// Recreate even if recovery fails, so the node is not silently converted
		// from a standby into a primary on the next start.
		defer func() {
			if _, wErr := createStandbySignal(logger, dataDir); wErr != nil {
				logger.ErrorContext(ctx, "failed to recreate standby.signal after crash recovery", "error", wErr, "path", signalPath)
			}
		}()
	}

	return runCrashRecoveryAttempts(ctx, logger, run, r)
}

// runCrashRecoveryAttempts retries `postgres --single` while the lock file is held.
// During the orphan-cleanup window after a postmaster crash, the lock will eventually
// release; if it does not within the retry window, postgres is genuinely running and
// we preserve the historical no-op behavior. Extracted for unit-test injection.
func runCrashRecoveryAttempts(
	ctx context.Context,
	logger *slog.Logger,
	run func(context.Context) ([]byte, error),
	r *retry.Retry,
) error {
	logger.InfoContext(ctx, "Starting single-user crash recovery")

	var lastOutput string
	for attempt, rerr := range r.Attempts(ctx) {
		if rerr != nil {
			return rerr
		}

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

		if attempt >= constants.CrashRecoveryMaxAttempts {
			break
		}

		logger.InfoContext(ctx, "Single-user crash recovery: lock file held, retrying",
			"attempt", attempt,
			"max_attempts", constants.CrashRecoveryMaxAttempts,
			"output", outputStr)
	}

	logger.InfoContext(ctx, "Single-user crash recovery not needed, postgres is already running",
		"attempts", constants.CrashRecoveryMaxAttempts,
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
