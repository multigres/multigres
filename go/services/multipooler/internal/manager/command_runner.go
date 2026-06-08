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

package manager

import (
	"bufio"
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/multigres/multigres/go/tools/executil"
)

// safeCombinedOutput executes a command and streams its output to avoid blocking.
// This prevents deadlocks when commands produce large amounts of output that
// would fill the internal pipe buffers. Returns combined stdout and stderr.
func safeCombinedOutput(cmd *executil.Cmd) (string, error) {
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return "", fmt.Errorf("failed to create stdout pipe: %w", err)
	}

	stderr, err := cmd.StderrPipe()
	if err != nil {
		return "", fmt.Errorf("failed to create stderr pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		return "", fmt.Errorf("failed to start command: %w", err)
	}

	lines := make(chan string, 100)
	stdoutDone := make(chan struct{})
	stderrDone := make(chan struct{})

	// scanErr captures a scanner error (e.g. bufio.ErrTooLong for lines
	// exceeding the default 64 KiB buffer) so silent truncation surfaces
	// to the caller instead of looking like clean EOF. Writes happen
	// before the corresponding Done channel closes; the close()-then-
	// channel-receive ordering through stdoutDone/stderrDone → lines →
	// the consumer loop establishes the happens-before edge that lets
	// the caller read these without a separate lock.
	var stdoutScanErr, stderrScanErr error

	go func() {
		defer close(stdoutDone)
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			lines <- scanner.Text() + "\n"
		}
		stdoutScanErr = scanner.Err()
	}()

	go func() {
		defer close(stderrDone)
		scanner := bufio.NewScanner(stderr)
		for scanner.Scan() {
			lines <- scanner.Text() + "\n"
		}
		stderrScanErr = scanner.Err()
	}()

	go func() {
		<-stdoutDone
		<-stderrDone
		close(lines)
	}()

	var combinedBuf strings.Builder
	for line := range lines {
		combinedBuf.WriteString(line)
	}

	// cmd.Wait's exit code is the operational signal, so it takes
	// precedence. If the process exited cleanly but a scanner failed
	// (e.g. ErrTooLong), surface that so a silently-truncated output
	// doesn't look successful.
	if waitErr := cmd.Wait(); waitErr != nil {
		return combinedBuf.String(), waitErr
	}
	if stdoutScanErr != nil {
		return combinedBuf.String(), fmt.Errorf("stdout scan: %w", stdoutScanErr)
	}
	if stderrScanErr != nil {
		return combinedBuf.String(), fmt.Errorf("stderr scan: %w", stderrScanErr)
	}
	return combinedBuf.String(), nil
}

// runLongCommand executes a long-running command with periodic progress logging.
// Logs progress every 10 seconds. The cmd should be created with exec.CommandContext(ctx, ...)
// to ensure proper cleanup on context cancellation.
func (pm *MultiPoolerManager) runLongCommand(ctx context.Context, cmd *executil.Cmd, operationName string) ([]byte, error) {
	pm.logger.InfoContext(ctx, "Starting command", "operation", operationName)

	startTime := time.Now()

	// Create a context for the logging goroutine
	logCtx, cancelLog := context.WithCancel(ctx)

	// Log progress periodically in background
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-logCtx.Done():
				return
			case <-ticker.C:
				elapsed := time.Since(startTime)
				pm.logger.InfoContext(ctx, "Command still in progress",
					"operation", operationName,
					"elapsed_seconds", int(elapsed.Seconds()))
			}
		}
	}()

	output, err := cmd.CombinedOutput()

	cancelLog()

	// Log completion
	elapsed := time.Since(startTime)
	if err != nil {
		pm.logger.ErrorContext(ctx, "Command failed",
			"operation", operationName,
			"elapsed_seconds", int(elapsed.Seconds()),
			"error", err)
	} else {
		pm.logger.InfoContext(ctx, "Command completed",
			"operation", operationName,
			"elapsed_seconds", int(elapsed.Seconds()))
	}

	return output, err
}
