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
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

// childReaper reaps PostgreSQL postmaster processes that pgctld starts.
//
// When pgctld runs as PID 1 (container init), `pg_ctl start -W` forks the
// postmaster and exits, so the postmaster is reparented to pgctld and must be
// wait()ed on or it lingers as a zombie.
//
// The reaper waits ONLY on the specific PIDs it has been told to track via
// TrackPID — it never calls Wait4(-1). A blanket Wait4(-1) reaper races every
// other subprocess pgctld runs through os/exec (pg_ctl, initdb, pg_rewind,
// pg_isready, psql, postgres --single, ...): both the reaper and os/exec's own
// Wait call wait4 on the same child, and whichever wins consumes the exit
// status. When the reaper wins, os/exec's Wait sees ECHILD ("waitid: no child
// processes") and reports a failure for a command that actually succeeded.
//
// That false failure is exactly what wedged a production cluster: a
// `pg_ctl start` that had in fact succeeded was reported as failed, a redundant
// Start RPC then treated the node as needing crash recovery, deleted
// standby.signal, and single-user crash-recovered a still-live postmaster into a
// second read-write server on the primary's timeline — an unrecoverable split
// (pg_rewind reports "no rewind required" for same-timeline peers).
//
// Tracking exact PIDs keeps the reaper from ever touching an os/exec-owned
// child, so os/exec always observes the true exit status.
type childReaper struct {
	logger *slog.Logger

	mu      sync.Mutex
	tracked map[int]struct{}

	sigCh chan os.Signal
}

// newChildReaper creates a childReaper. Call Run in a goroutine to start reaping.
func newChildReaper(logger *slog.Logger) *childReaper {
	return &childReaper{
		logger:  logger,
		tracked: make(map[int]struct{}),
		sigCh:   make(chan os.Signal, 1),
	}
}

// TrackPID registers a postmaster PID for reaping and attempts an immediate reap so
// a postmaster that has already exited does not linger until the next SIGCHLD.
//
// Safe to call on a nil receiver (the reaper is only created under PID 1), with a
// non-positive PID, or repeatedly with the same PID — all are no-ops.
func (r *childReaper) TrackPID(pid int) {
	if r == nil || pid <= 0 {
		return
	}
	r.mu.Lock()
	r.tracked[pid] = struct{}{}
	r.mu.Unlock()

	// Reap immediately in case the postmaster already exited between the caller
	// obtaining its PID and this call. SIGCHLD is not queued, so its death signal
	// may have already fired (and found nothing tracked) and will not fire again;
	// without this the zombie would linger until an unrelated child's SIGCHLD
	// happens to wake the run loop. In the common case (still running) this is a
	// cheap WNOHANG no-op.
	r.Reap()
}

// Run installs the SIGCHLD handler and reaps tracked children on each signal. It
// runs until sigCh is closed, i.e. for the process lifetime in production.
func (r *childReaper) Run() {
	signal.Notify(r.sigCh, syscall.SIGCHLD)
	for range r.sigCh {
		r.Reap()
	}
}

// Reap does a non-blocking wait on every tracked PID, dropping those that have
// exited or are no longer our children.
func (r *childReaper) Reap() {
	r.mu.Lock()
	pids := make([]int, 0, len(r.tracked))
	for pid := range r.tracked {
		pids = append(pids, pid)
	}
	r.mu.Unlock()

	for _, pid := range pids {
		var status syscall.WaitStatus
		wpid, err := syscall.Wait4(pid, &status, syscall.WNOHANG, nil)
		if wpid == pid || err == syscall.ECHILD {
			r.UntrackPID(pid)
		} else if err != nil {
			r.logger.Warn("failed to reap tracked postmaster process", "pid", pid, "error", err)
		}
	}
}

func (r *childReaper) UntrackPID(pid int) {
	r.mu.Lock()
	delete(r.tracked, pid)
	r.mu.Unlock()
}

// Stop detaches the SIGCHLD handler and ends the run loop. signal.Stop is called
// before close so the signal package cannot send on the closed channel.
func (r *childReaper) Stop() {
	signal.Stop(r.sigCh)
	close(r.sigCh)
}
