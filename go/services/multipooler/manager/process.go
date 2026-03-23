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
	"context"
	"fmt"
	"io"
	"log/slog"
	"os/exec"
	"syscall"
	"time"

	"github.com/multigres/multigres/go/tools/ctxutil"
	"github.com/multigres/multigres/go/tools/telemetry"
)

// sigtermTimeout is how long to wait after sending SIGTERM before
// escalating to SIGKILL.
const sigtermTimeout = 5 * time.Second

// runBackupCommand runs a command while monitoring the context. If the context
// is cancelled (e.g., due to lease loss via topoclient.ErrLeaseLost, or parent
// cancellation), the process is terminated gracefully:
//  1. Send SIGTERM to give pgbackrest a chance to clean up partial writes
//  2. Wait up to sigtermTimeout (5s) for the process to exit
//  3. Send SIGKILL if the process is still running
//
// This replaces exec.CommandContext's default behavior of sending SIGKILL
// immediately on context cancellation.
//
// The cmd must NOT have been started yet — this function calls cmd.Start().
func runBackupCommand(
	ctx context.Context,
	cmd *exec.Cmd,
	logger *slog.Logger,
) ([]byte, error) {
	// Set up process group so we can signal the entire group
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	// Capture combined output
	outputPipe, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("failed to create stdout pipe: %w", err)
	}
	cmd.Stderr = cmd.Stdout // merge stderr into stdout pipe

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("failed to start command: %w", err)
	}

	// Read output in background
	type outputResult struct {
		data []byte
		err  error
	}
	outputCh := make(chan outputResult, 1)
	go func() {
		data, err := io.ReadAll(outputPipe)
		outputCh <- outputResult{data: data, err: err}
	}()

	// Wait for command completion or context cancellation
	doneCh := make(chan error, 1)
	go func() {
		doneCh <- cmd.Wait()
	}()

	select {
	case waitErr := <-doneCh:
		result := <-outputCh
		return result.data, waitErr
	case <-ctx.Done():
		reason := context.Cause(ctx)
		if reason == nil {
			reason = ctx.Err()
		}
		logger.WarnContext(ctx, "Terminating pgbackrest process",
			"pid", cmd.Process.Pid, "reason", reason)
		killErr := gracefulKill(ctx, cmd, logger)
		result := <-outputCh
		<-doneCh
		if killErr != nil {
			return result.data, fmt.Errorf("process killed (%w): %w", reason, killErr)
		}
		return result.data, reason
	}
}

// gracefulKill sends SIGTERM, waits up to sigtermTimeout, then sends SIGKILL.
func gracefulKill(ctx context.Context, cmd *exec.Cmd, logger *slog.Logger) error {
	// Detach from parent context so cleanup isn't cancelled mid-kill.
	return telemetry.WithSpan(ctxutil.Detach(ctx), "backup-lease/process-cleanup", func(_ context.Context) error {
		pid := cmd.Process.Pid

		// Step 1: Send SIGTERM to process group
		logger.Info("Sending SIGTERM to pgbackrest process", "pid", pid)
		if err := syscall.Kill(-pid, syscall.SIGTERM); err != nil {
			logger.Warn("Failed to send SIGTERM, sending SIGKILL",
				"pid", pid, "error", err)
			return cmd.Process.Kill()
		}

		// Step 2: Wait for process to exit with timeout
		done := make(chan struct{})
		defer close(done)

		exitCh := make(chan struct{})
		go func() {
			// cmd.Wait() is already being called by the caller, so we just
			// poll the process state here
			for {
				if err := syscall.Kill(-pid, 0); err != nil {
					close(exitCh)
					return
				}
				select {
				case <-done:
					return
				case <-time.After(100 * time.Millisecond):
				}
			}
		}()

		select {
		case <-exitCh:
			logger.Info("pgbackrest process exited after SIGTERM", "pid", pid)
			return nil
		case <-time.After(sigtermTimeout):
			// Step 3: Escalate to SIGKILL
			logger.Warn("pgbackrest process did not exit after SIGTERM, sending SIGKILL",
				"pid", pid, "timeout", sigtermTimeout)
			if err := syscall.Kill(-pid, syscall.SIGKILL); err != nil {
				logger.Warn("Failed to send SIGKILL", "pid", pid, "error", err)
			}
			return nil
		}
	})
}
