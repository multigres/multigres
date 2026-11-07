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
// exec-replacing itself) in a pidfile it keeps flock'd until it exits, so
// StopRestoreCommand signals only a PID whose wrapper is still alive, and it
// owns making sure the wrapped command's entire process group is actually dead
// before it exits — callers never need to know pgbackrest's own process
// structure.
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
	// SIGQUIT is how postgres's immediate shutdown reaches this process group;
	// without handling it the Go runtime exits on the spot, skipping the pidfile
	// cleanup and leaving the wrapped command, which runs in its own process
	// group, orphaned.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT, syscall.SIGQUIT)
	defer signal.Stop(sigCh)

	lock, err := publishPidfile(pidfile, os.Getpid())
	if err != nil {
		return fmt.Errorf("restore-wrapper: failed to write pidfile %s: %w", pidfile, err)
	}
	defer releasePidfile(pidfile, lock)

	return runWrappedCommand(args[0], args[1:], sigCh)
}

// publishPidfile makes pid visible at path in one step and returns the open,
// exclusively flock'd pidfile, which the caller must hold until it exits.
//
// A plain write truncates the file before filling it, so StopRestoreCommand
// could read the empty file in between. Renaming a complete file from the same
// directory means a reader sees either no pidfile, the previous one, or the
// whole new one. The temp name carries the pid because only this live process
// can hold it, so concurrent wrappers never share a temp file.
//
// The lock is taken before the rename, so the pidfile is never visible
// unlocked while this wrapper lives. It belongs to the open file, which Go
// opens close-on-exec, so the wrapped command does not inherit it and the
// kernel drops it however this process dies. A pidfile whose lock is free
// therefore names no running wrapper, even if its PID has since been reused.
func publishPidfile(path string, pid int) (*os.File, error) {
	tmp := fmt.Sprintf("%s.tmp-%d", path, pid)
	//nolint:gosec // G304: tmp sits next to the caller-supplied restore_command pidfile.
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX)
	if err == nil {
		_, err = f.WriteString(strconv.Itoa(pid))
	}
	if err == nil {
		//nolint:gosec // G703: the caller deliberately supplies the restore_command pidfile path.
		err = os.Rename(tmp, path)
	}
	if err != nil {
		_ = f.Close()
		// Never leave debris in the pooler directory.
		//nolint:gosec // G703: tmp sits next to the caller-supplied restore_command pidfile.
		_ = os.Remove(tmp)
		return nil, err
	}
	return f, nil
}

// releasePidfile removes the pidfile only while path still names the file this
// wrapper published. A wrapper that outlived its postgres can overlap a newer
// one; removing by path alone would delete the newer wrapper's pidfile and hide
// it from StopRestoreCommand. Closing the file drops the lock.
func releasePidfile(path string, f *os.File) {
	defer f.Close()
	ours, err := f.Stat()
	if err != nil {
		return
	}
	//nolint:gosec // G703: the caller deliberately supplies the restore_command pidfile path.
	if current, err := os.Stat(path); err == nil && os.SameFile(ours, current) {
		//nolint:gosec // G703: the caller deliberately supplies the restore_command pidfile path.
		_ = os.Remove(path)
	}
}

func runWrappedCommand(name string, args []string, sigCh <-chan os.Signal) error {
	//nolint:gosec // G702: executing the caller-supplied restore command is this wrapper's purpose.
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
