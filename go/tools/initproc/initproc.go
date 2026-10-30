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

// Package initproc lets a binary that runs as a container's PID 1 act as a
// proper init, in the manner of tini: it re-executes itself as a child, forwards
// signals to that child, and reaps every process that is reparented to it.
//
// The real work runs in the child, never in PID 1. That split is what makes a
// blanket Wait4(-1) safe. wait4 only returns the caller's own children, so the
// child's os/exec subprocesses are invisible to PID 1 and only the child's own
// cmd.Wait collects them. PID 1 only ever sees its one direct child plus
// orphans whose parent has already died, which nothing else can wait on.
//
// The child must not set PR_SET_CHILD_SUBREAPER: orphans would then reparent
// to it instead of PID 1, and nothing there reaps them.
package initproc

import (
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
)

// forwardedSignals are relayed from PID 1 to the child. SIGCHLD is handled by
// PID 1 itself, and SIGURG is the Go runtime's preemption signal, not a request.
var forwardedSignals = []os.Signal{
	syscall.SIGTERM,
	syscall.SIGINT,
	syscall.SIGHUP,
	syscall.SIGQUIT,
	syscall.SIGUSR1,
	syscall.SIGUSR2,
}

// IsInit reports whether this process is PID 1 of its PID namespace.
func IsInit() bool {
	return os.Getpid() == 1
}

// Run re-executes the current binary with the same arguments and environment
// and supervises it until it exits, returning the exit code PID 1 should exit
// with. The child is never PID 1, so calling Run only when IsInit is true
// cannot recurse.
func Run(logger *slog.Logger) int {
	exe, err := os.Executable()
	if err != nil {
		logger.Error("initproc: cannot resolve own executable", "error", err)
		return 1
	}
	return Supervise(logger, exe, os.Args, os.Environ())
}

// Supervise starts path as a child, forwards signals to it, and reaps every
// child of this process until that one exits, returning its exit code (128+N
// if it was killed by signal N).
//
// The calling process must not have any other waiter on its children: no
// os/exec Wait, no os.Process.Wait. A second waiter races Wait4(-1) for the
// same exit status, which is the bug this package exists to avoid.
func Supervise(logger *slog.Logger, path string, argv, env []string) int {
	// Subscribe before forking so a child that exits immediately cannot deliver
	// SIGCHLD before anyone is listening.
	sigCh := make(chan os.Signal, 32)
	signal.Notify(sigCh, append([]os.Signal{syscall.SIGCHLD}, forwardedSignals...)...)
	defer signal.Stop(sigCh)

	// syscall.ForkExec rather than os/exec or os.StartProcess: those return a
	// handle whose Wait would be a second waiter, and on Linux hold a pidfd that
	// nothing here would ever release.
	//nolint:gosec // G702: re-executing the caller-supplied binary is this function's purpose.
	child, err := syscall.ForkExec(path, argv, &syscall.ProcAttr{
		Env:   env,
		Files: []uintptr{0, 1, 2},
	})
	if err != nil {
		logger.Error("initproc: failed to start child", "path", path, "error", err)
		return 1
	}

	for sig := range sigCh {
		if sig != syscall.SIGCHLD {
			if err := syscall.Kill(child, sig.(syscall.Signal)); err != nil && !errors.Is(err, syscall.ESRCH) {
				logger.Warn("initproc: failed to forward signal", "signal", sig, "pid", child, "error", err)
			}
			continue
		}
		if code, exited := reapAll(logger, child); exited {
			return code
		}
	}
	return 1
}

// reapAll collects every exited child. SIGCHLD is not queued, so one signal can
// stand for many exits, and the loop runs until nothing more is waitable.
func reapAll(logger *slog.Logger, child int) (code int, childExited bool) {
	for {
		var ws syscall.WaitStatus
		pid, err := syscall.Wait4(-1, &ws, syscall.WNOHANG, nil)
		switch {
		case errors.Is(err, syscall.EINTR):
			continue
		case errors.Is(err, syscall.ECHILD):
			return 0, false
		case err != nil:
			logger.Warn("initproc: wait4 failed", "error", err)
			return 0, false
		case pid <= 0:
			return 0, false
		case pid == child:
			return exitCode(ws), true
		}
	}
}

func exitCode(ws syscall.WaitStatus) int {
	if ws.Signaled() {
		return 128 + int(ws.Signal())
	}
	return ws.ExitStatus()
}
