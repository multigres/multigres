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
	r := retry.New(constants.CrashRecoveryRetryDelay, constants.CrashRecoveryRetryDelay)
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
			logger.WarnContext(ctx, "single-user crash recovery failed",
				"error", err,
				"output", outputStr)
			return fmt.Errorf("crash recovery failed: %w", err)
		}

		if attempt >= constants.CrashRecoveryMaxAttempts {
			break
		}

		logger.InfoContext(ctx, "single-user crash recovery: lock file held, retrying",
			"attempt", attempt,
			"max_attempts", constants.CrashRecoveryMaxAttempts,
			"output", outputStr)
	}

	logger.InfoContext(ctx, "single-user crash recovery not needed, postgres is already running",
		"attempts", constants.CrashRecoveryMaxAttempts,
		"output", lastOutput)
	return nil
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
