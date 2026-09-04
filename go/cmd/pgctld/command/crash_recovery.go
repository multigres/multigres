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
	"os"
	"regexp"
	"strings"

	"github.com/multigres/multigres/go/common/constants"
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
func (s *PgCtldService) isPostgresCleanlyStopped(ctx context.Context) (bool, error) {
	cmd := executil.Command(ctx, "pg_controldata", s.pgConfig.PostgresDataDir)
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
func (s *PgCtldService) crashRecoveryNeeded(ctx context.Context) (bool, error) {
	cleanlyStopped, err := s.isPostgresCleanlyStopped(ctx)
	if err != nil {
		return false, err
	}
	return !cleanlyStopped, nil
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
func (s *PgCtldService) runCrashRecovery(ctx context.Context) error {
	r := retry.New(constants.OrphanCleanupRetryDelay, constants.OrphanCleanupRetryDelay)
	return s.runCrashRecoveryInDir(ctx, s.runSingleUserPostgres, r)
}

// runCrashRecoveryInDir is runCrashRecovery with the single-user runner
// injected, so the standby.signal save/restore can be unit-tested against a
// PgCtldService pointed at a temp dir, without a real postgres. Extracted for
// testing.
func (s *PgCtldService) runCrashRecoveryInDir(
	ctx context.Context,
	run func(context.Context) ([]byte, error),
	r *retry.Retry,
) error {
	logger := s.logger
	dataDir := s.pgConfig.PostgresDataDir

	// Never disturb a live postmaster. The standby.signal removal below opens a
	// window in which the file is absent; a postmaster that is running — or still
	// mid-startup after a Start whose success was misreported — can observe that
	// absence and finish recovery as a primary on a timeline it must not claim.
	// Callers gate this path on a stopped node; this is the last-line guard for
	// the residual race where a postmaster appears between that check and here.
	// A running node needs no crash recovery, so skipping is also the correct no-op.
	if isPostgreSQLRunning(dataDir) {
		logger.InfoContext(ctx, "skipping crash recovery: postgres is running", "data_dir", dataDir)
		return nil
	}

	signalPath := s.standbySignalPath()
	if _, err := os.Stat(signalPath); err == nil {
		logger.InfoContext(ctx, "temporarily removing standby.signal for single-user crash recovery",
			"path", signalPath)
		if _, rmErr := s.removeStandbySignal(); rmErr != nil {
			return fmt.Errorf("failed to remove standby.signal before crash recovery: %w", rmErr)
		}
		// Recreate even if recovery fails, so the node is not silently converted
		// from a standby into a primary on the next start.
		defer func() {
			if _, wErr := s.createStandbySignal(); wErr != nil {
				logger.ErrorContext(ctx, "failed to recreate standby.signal after crash recovery", "error", wErr, "path", signalPath)
			}
		}()
	}

	return s.runCrashRecoveryAttempts(ctx, run, r)
}

// retryWhileLockHeld retries run while it fails with postgresAlreadyRunningPattern,
// the transient "lock file ... already exists" error left by orphaned children
// during the brief window after an unclean postgres kill, even though the
// postmaster itself is gone. Any other failure returns immediately. A context
// cancellation or deadline (checked via ctx.Err after the loop) also returns
// immediately, with whatever the retry iterator reports as rerr.
//
// Once maxAttempts is exhausted, the last attempt's output and error are
// returned exactly as run produced them: this primitive does not decide what
// a still-held lock means once the budget runs out, since that differs by
// caller (single-user crash recovery treats it as "must already be running";
// a plain start treats it as a genuine failure). Callers make that call
// themselves by inspecting the returned error.
func (s *PgCtldService) retryWhileLockHeld(
	ctx context.Context,
	run func(context.Context) ([]byte, error),
	maxAttempts int,
	r *retry.Retry,
) ([]byte, error) {
	logger := s.logger

	var output []byte
	var runErr error
	for attempt, rerr := range r.Attempts(ctx) {
		if rerr != nil {
			return output, rerr
		}

		output, runErr = run(ctx)
		if runErr == nil {
			return output, nil
		}

		if !postgresAlreadyRunningPattern.Match(output) {
			return output, runErr
		}

		if attempt >= maxAttempts {
			break
		}

		logger.InfoContext(ctx, "lock file still held by orphaned processes, retrying",
			"attempt", attempt,
			"max_attempts", maxAttempts,
			"output", string(output))
	}

	return output, runErr
}

// runCrashRecoveryAttempts retries `postgres --single` while the lock file is held.
// During the orphan-cleanup window after a postmaster crash, the lock will eventually
// release; if it does not within the retry window, postgres is genuinely running and
// we preserve the historical no-op behavior. Extracted for unit-test injection.
func (s *PgCtldService) runCrashRecoveryAttempts(
	ctx context.Context,
	run func(context.Context) ([]byte, error),
	r *retry.Retry,
) error {
	logger := s.logger
	logger.InfoContext(ctx, "starting single-user crash recovery")

	output, err := s.retryWhileLockHeld(ctx, run, constants.OrphanCleanupMaxAttempts, r)
	if err == nil {
		return nil
	}

	if ctx.Err() != nil {
		// The retry loop aborted via context cancellation/deadline, not a real
		// run() failure: propagate as-is, with no logging or wrapping.
		return err
	}

	if postgresAlreadyRunningPattern.Match(output) {
		logger.InfoContext(ctx, "single-user crash recovery not needed, postgres is already running",
			"attempts", constants.OrphanCleanupMaxAttempts,
			"output", string(output))
		return nil
	}

	logger.WarnContext(ctx, "single-user crash recovery failed",
		"error", err,
		"output", string(output))
	return fmt.Errorf("crash recovery failed: %w", err)
}

// runSingleUserPostgres runs `postgres --single` once and returns its combined
// output and exit error. /dev/null on stdin causes single-user mode to perform
// recovery and exit on EOF.
func (s *PgCtldService) runSingleUserPostgres(ctx context.Context) ([]byte, error) {
	cmd := executil.Command(ctx, "postgres", "--single", "-D", s.pgConfig.PostgresDataDir, "template1")

	devNull, err := os.Open("/dev/null")
	if err != nil {
		return nil, fmt.Errorf("failed to open /dev/null: %w", err)
	}
	defer devNull.Close()

	cmd.SetStdin(devNull)
	return cmd.CombinedOutput()
}
