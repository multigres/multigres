// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package command

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/spf13/cobra"
)

// restoreWrapperGracePeriod is how long the wrapper waits for the wrapped
// command's process group to exit after forwarding a signal before escalating
// to SIGKILL.
const restoreWrapperGracePeriod = 500 * time.Millisecond

// AddRestoreWrapperCommand adds the internal restore-wrapper subcommand.
func AddRestoreWrapperCommand(root *cobra.Command) {
	root.AddCommand(&cobra.Command{
		Use:                "restore-wrapper <pidfile> -- <command> [args...]",
		Short:              "Internal: wraps a restore_command invocation for reliable stop detection",
		Hidden:             true,
		DisableFlagParsing: true,
		SilenceUsage:       true,
		RunE:               runRestoreWrapper,
		// Opts out of root's telemetry init/shutdown.
		// postgres invokes restore_command once per WAL segment during catch-up
		// and this wrapper isn't itself instrumented, so the OpenTelemetry lifecycle
		// overhead buys nothing here.
		Annotations: map[string]string{skipTelemetryAnnotation: "true"},
	})
}

// runRestoreWrapper is set as restore_command itself (see resetRestoreCommand
// / the observer catch-up path in the multipooler manager). Postgres invokes
// restore_command synchronously and has no way to cancel an in-flight
// invocation on its own — only an external actor with OS-level process access
// can. This wrapper makes that possible: it records its own PID (stable for
// its whole lifetime, since it stays alive as a real parent rather than
// exec-replacing itself) so StopRestoreCommand can check liveness or signal
// it, and it owns making sure the wrapped command's entire process group is
// actually dead before it exits — callers never need to know pgbackrest's own
// process structure.
func runRestoreWrapper(cmd *cobra.Command, args []string) error {
	var pidfile string
	if len(args) > 0 {
		pidfile = args[0]
		args = args[1:]
	}
	if len(args) > 0 && args[0] == "--" {
		args = args[1:]
	}
	if pidfile == "" || len(args) == 0 {
		return errors.New("usage: pgctld restore-wrapper <pidfile> -- <command> [args...]")
	}

	// Install the handler before publishing our PID so StopRestoreCommand cannot
	// terminate the wrapper in the gap between discovering it and signal.Notify.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	defer signal.Stop(sigCh)

	//nolint:gosec // The caller deliberately supplies the restore_command pidfile path.
	if err := os.WriteFile(pidfile, []byte(strconv.Itoa(os.Getpid())), 0o644); err != nil {
		return fmt.Errorf("restore-wrapper: failed to write pidfile %s: %w", pidfile, err)
	}
	// Best-effort: StopRestoreCommand's Signal(0) probe already handles a
	// stale pidfile correctly, but removing it on a normal exit keeps the
	// common case (no pidfile = nothing to check) the fast path.
	defer os.Remove(pidfile)

	return runWrappedCommand(args[0], args[1:], sigCh)
}

func runWrappedCommand(name string, args []string, sigCh <-chan os.Signal) error {
	//nolint:gosec // Executing the caller-supplied restore command is this wrapper's purpose.
	c := exec.Command(name, args...)
	c.Stdout = os.Stdout
	c.Stderr = os.Stderr
	// Its own process group so a signal can be forwarded to it and any
	// subprocesses it spawns (pgbackrest's archive-get can itself fork
	// workers) in one shot, via a negative PID.
	c.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	if err := c.Start(); err != nil {
		return fmt.Errorf("restore-wrapper: failed to start %s: %w", name, err)
	}

	done := make(chan error, 1)
	go func() { done <- c.Wait() }()

	select {
	case sig := <-sigCh:
		pgid := -c.Process.Pid
		_ = syscall.Kill(pgid, sig.(syscall.Signal))
		select {
		case <-done:
		case <-time.After(restoreWrapperGracePeriod):
			_ = syscall.Kill(pgid, syscall.SIGKILL)
			<-done
		}
		// PostgreSQL treats a signal-terminated restore_command as a shutdown
		// request. This signal came through StopRestoreCommand, so report a clean
		// wrapper exit after the child is gone; PostgreSQL will verify whether the
		// requested WAL file exists and continue normally when it does not.
		return nil
	case err := <-done:
		return err
	}
}
