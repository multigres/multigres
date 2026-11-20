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

package pgbackrestd

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"connectrpc.com/connect"

	"github.com/multigres/multigres/go/common/backup"
	"github.com/multigres/multigres/go/common/safefs"
	pb "github.com/multigres/multigres/go/pb/pgbackrestdservice"
)

// Timing a test shortens.
var (
	// resetDrainWait is how long a stanza delete or spool reset waits for
	// in-flight commands and WAL requests before giving up with everything
	// still running.
	resetDrainWait = backup.ResetDrainWait
	// resetStepTimeout bounds each child of those sequences, so a wedged one
	// cannot hold the gate, the paused 8432 server and the stop file forever.
	resetStepTimeout = backup.ResetStepTimeout
	// preflightInfoTimeout bounds the info child a stanza delete reads.
	preflightInfoTimeout = backup.InfoTimeout
	// maxPreflightInfoBytes bounds the info output a stanza delete reads. A
	// bigger answer is refused rather than truncated and parsed.
	maxPreflightInfoBytes = 8 << 20
)

// errDeleteUnproven is the refusal every path that could lift the stop file, or
// run another destructive sequence, returns while a stanza delete is unproven.
// The sidecar restarts itself, so the window is short.
const errDeleteUnproven = "an earlier stanza delete is not proven settled: archiving stays stopped and no start, restore, stanza delete or spool reset runs until the sidecar has restarted"

func deleteUnprovenError() error {
	return connect.NewError(connect.CodeFailedPrecondition, errors.New(errDeleteUnproven))
}

// errDeleteUnprovenExit is what Run returns after a latch has shut the sidecar
// down, so the process exits non-zero and its container restarts.
var errDeleteUnprovenExit = errors.New("exiting after a stanza delete that could not be proven settled")

// latchDeleteUnproven records that the delete of run runID could not be proven
// settled, the first reason standing. It does not shut the sidecar down: the
// handler must still answer its caller, and every exclusive handler calls
// exitAfterLatch as it returns.
func (s *Server) latchDeleteUnproven(ctx context.Context, runID string, cause error) {
	reason := fmt.Sprintf("stanza delete run %q not proven settled: %v", runID, cause)
	s.deleteUnproven.CompareAndSwap(nil, &reason)
	s.logger.ErrorContext(ctx, "stanza delete not proven settled; WAL archiving stays stopped and the sidecar is exiting so its container restarts",
		"run_id", runID, "error", cause)
}

func (s *Server) deleteLatched() bool { return s.deleteUnproven.Load() != nil }

func (s *Server) unprovenReason() string {
	if r := s.deleteUnproven.Load(); r != nil {
		return *r
	}
	return ""
}

// exitAfterLatch starts the sidecar's normal shutdown, once, when the sidecar is
// latched; it does nothing otherwise. Every exclusive handler calls it as the
// handler method returns, so the run that latched triggers it, and so can a
// refused run that arrives while that run is still unwinding. The shutdown does
// not lift the stop file: the serve loop ends, the servers' graceful shutdown
// waits for handlers that are still answering (so the latching run's response
// and trailer go out, within shutdownGrace), the supervisors see their lifetime
// pipes close, and Run returns errDeleteUnprovenExit, so the process exits
// non-zero.
//
// The recovery that follows is the container's PID-1 contract: the restarted
// container retires the old container's worker markers, the orphaned delete dies
// with the old container, and the new instance lifts the stop file at startup.
// Outside that contract the sidecar stays down: a supervisor that died without
// retiring its marker keeps every later instance from starting until the marker
// is removed by hand, and the local provisioner does not restart pgbackrestd.
func (s *Server) exitAfterLatch() {
	if !s.deleteLatched() {
		return
	}
	s.exitOnce.Do(func() {
		if s.latchExit != nil {
			s.latchExit()
			return
		}
		s.serveMu.Lock()
		cancel := s.serveCancel
		s.serveMu.Unlock()
		s.stopAdmission()
		if cancel != nil {
			cancel()
		}
	})
}

// errStartFailed is what a stanza delete or spool reset fails with when it
// could not lift the stop file it put in place. The sequence's own outcome
// does not matter then: archiving is stopped until the background retry works.
const errStartFailed = "pgbackrest start failed; WAL archiving is stopped"

// runExclusive runs COMMAND_STANZA_DELETE or COMMAND_ARCHIVE_SPOOL_RESET. Both
// quiesce the repository the way a restore does: the gate is closed, the 8432
// server paused, and "pgbackrest stop --force" plus the worker barrier keep
// every archive-push worker out. They do not touch restore state.
//
// Two parts are detached from the request. After a delete that started a child,
// the worker barrier runs under its own bound (workerStopWait) so a cancelled
// caller still gets a proof that the delete's supervisor settled. Then the
// unwind starts archiving again (retrying in the background if that keeps
// failing), resumes the server and reopens the gate on every path, in that
// order, each step bounded; the one exception is a delete whose worker could not
// be proven settled, which latches the sidecar: the stop file stays, so no start
// runs, and the Run fails saying so. Everything else that reads or changes the
// repository or the spool runs under the request context: a caller that
// disconnects, times out or loses its backup lease must not leave a delete
// running that a later lease holder could meet.
//
// spawned reports whether a stanza-delete child was ever started for the run:
// false means the delete never ran, so a caller has nothing to wait for. Each
// return reads it from the delete's worker through didSpawn, not as a literal.
func (s *Server) runExclusive(ctx context.Context, req *pb.RunRequest, out *streamSender) (exit int, spawned bool, err error) {
	var deleteWorker atomic.Pointer[ownedWorker]
	didSpawn := func() bool { return deleteWorker.Load() != nil }
	if err := checkRequest(req); err != nil {
		return 0, didSpawn(), err
	}
	cmd := req.GetCommand()
	deleting := cmd == pb.Command_COMMAND_STANZA_DELETE
	if deleting {
		// Deleting needs a repository, and pgBackRest refuses stanza-delete when
		// several are configured (error 037), so refuse before quiescing anything.
		if !s.configured() {
			return 0, didSpawn(), connect.NewError(connect.CodeFailedPrecondition, errors.New("not configured: no Configure has been accepted"))
		}
		if _, repos := s.held(); len(repos) != 1 {
			return 0, didSpawn(), connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf("stanza delete needs exactly one repository, %d are configured", len(repos)))
		}
		// The caller decided to delete after reading this instance's state. A
		// replacement sidecar must not run that delete: the one the caller saw is
		// the one whose settlement it would wait for.
		if want := req.GetExpectedInstanceId(); want == "" || want != s.incarnation {
			return 0, didSpawn(), connect.NewError(connect.CodeFailedPrecondition, errors.New("expected_instance_id is empty or names a different sidecar instance"))
		}
	}
	if s.deleteLatched() {
		return 0, didSpawn(), deleteUnprovenError()
	}
	if err := ctx.Err(); err != nil {
		return -1, didSpawn(), err
	}

	// Visible to Status from acceptance on. A delete stays listed until its
	// owned worker is positively settled (see settleDeleteEntry), so a caller
	// that waits for the run to leave Status knows the delete is gone when it
	// does. It is not a fence: a caller that loses its backup lease while the
	// worker is unsettled is not protected from the next holder by this.
	lc := s.trackRun(ctx, cmd, req.GetRunId())
	var untrack sync.Once
	dropActive := func() { untrack.Do(func() { s.untrackChild(lc) }) }

	reopen, gerr := s.gate.Close(ctx, resetDrainWait)
	if gerr != nil {
		dropActive()
		return 0, didSpawn(), connect.NewError(connect.CodeUnavailable, fmt.Errorf("could not quiesce for %v: %w", cmd, gerr))
	}
	defer reopen()
	resume := s.pause()
	defer resume()

	deleteCtx := withWorkerObserver(ctx, func(w *ownedWorker) { deleteWorker.Store(w) })

	rctx := context.WithoutCancel(ctx)
	defer func() {
		// No delete child was ever spawned: nothing is left for a caller to wait for.
		if !didSpawn() {
			dropActive()
		}
		if s.deleteLatched() {
			// The delete may still be running. Lifting the stop file would let
			// archiving race it, so it stays and the Run says so.
			msg := "stanza delete is not proven settled; WAL archiving stays stopped until the sidecar has restarted"
			if err != nil {
				msg += fmt.Sprintf(" (after: %v)", err)
			}
			exit, err = -1, connect.NewError(connect.CodeFailedPrecondition, errors.New(msg))
			return
		}
		serr := s.restartRepository(rctx, out, resetStepTimeout)
		if serr == nil {
			return
		}
		msg := errStartFailed
		switch {
		case err != nil:
			msg += fmt.Sprintf(" (after: %v)", err)
		case exit != 0:
			msg += fmt.Sprintf(" (after: exit status %d)", exit)
		}
		exit, err = -1, connect.NewError(connect.CodeFailedPrecondition, errors.New(msg))
	}()

	// A stop file left by an earlier failure makes "stop --force" a no-op that
	// signals nobody, so a live async worker would survive the barrier. The gate
	// is closed, so clearing it first cannot let a push in.
	if code, serr := s.runStep(ctx, out, resetStepTimeout, cmdStart, nil); serr != nil || code != 0 {
		return code, didSpawn(), serr
	}
	if code, serr := s.runStep(ctx, out, resetStepTimeout, cmdStop, []string{"--force"}); serr != nil || code != 0 {
		return code, didSpawn(), serr
	}
	if err := s.quiesceStep(ctx); err != nil {
		return -1, didSpawn(), err
	}

	if deleting {
		code, derr := s.deleteStanza(ctx, deleteCtx, out)
		if didSpawn() {
			// Whatever the delete's outcome (success, non-zero exit, truncated
			// output, a cancelled caller), a result that is not an error must mean
			// the delete's worker is positively settled, so the barrier runs on
			// every path that spawned one, and past a cancelled request.
			if berr := s.quiesceStep(rctx); berr != nil {
				s.latchDeleteUnproven(rctx, req.GetRunId(), berr)
				s.settleDeleteEntry(deleteWorker.Load(), dropActive)
				return -1, didSpawn(), connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf("stanza delete worker not settled: %w", berr))
			}
		}
		s.settleDeleteEntry(deleteWorker.Load(), dropActive)
		if derr != nil || code != 0 {
			return code, didSpawn(), derr
		}
	}
	if err := s.clearArchiveSpool(); err != nil {
		return 0, didSpawn(), connect.NewError(connect.CodeInternal, fmt.Errorf("clear archive-push spool: %w", err))
	}
	return 0, didSpawn(), nil
}

// settleDeleteEntry drops a delete's active_commands entry once the delete's
// owned worker is positively settled: its supervisor acknowledged quiescence and
// exited cleanly. A worker that fails to settle (a stalled supervisor, an
// unproven acknowledgement) latches and its entry stays listed for the life of
// the latch, i.e. until the sidecar restarts; a worker still settling drops its
// entry when it does. The sidecar has no kill escalation of its own, so a caller
// that waits on this entry is waiting on the only proof there is that the delete
// is dead.
func (s *Server) settleDeleteEntry(w *ownedWorker, drop func()) {
	if w == nil {
		drop()
		return
	}
	select {
	case <-w.done:
		if w.err == nil {
			drop()
		}
	default:
		go func() {
			<-w.done
			if w.err == nil {
				drop()
			}
		}()
	}
}

// quiesceStep proves no archive worker is alive, within workerStopWait.
func (s *Server) quiesceStep(ctx context.Context) error {
	qctx, cancel := context.WithTimeout(ctx, workerStopWait)
	defer cancel()
	return s.quiesceWorkers(qctx)
}

// deleteStanza refuses unless the repository positively holds no backup, then
// removes the stanza. The caller has quiesced the repository.
func (s *Server) deleteStanza(ctx, deleteCtx context.Context, out *streamSender) (int, error) {
	if err := s.requireNoBackups(ctx); err != nil {
		return 0, err
	}
	// A caller that is gone (disconnected, timed out, lease lost) must not get a
	// delete started on its behalf.
	if err := ctx.Err(); err != nil {
		return -1, runError(ctx, err)
	}
	return s.runStep(deleteCtx, out, resetStepTimeout, cmdStanzaDelete, []string{"--force"})
}

// requireNoBackups reads the repository once through an internal info child
// (never the public INFO path, which the closed gate refuses, and never the
// response stream) and refuses unless it positively reports no backup of any
// shard, or no stanza at all. The interpretation is the one multipooler decides
// with, so the two cannot disagree on what an unreadable repository means:
// info exits 0 with a repository status of 99 when the repository cannot be
// reached, and that must not read as empty. This bounds a caller's mistake to
// WAL-only stanzas; the backup lease and the bootstrap marker are the guards
// against deleting a stanza another bootstrap just created.
func (s *Server) requireNoBackups(ctx context.Context) error {
	infoCtx, cancel := context.WithTimeout(ctx, preflightInfoTimeout)
	defer cancel()
	stdout := &cappedBuffer{limit: maxPreflightInfoBytes}
	var stderr cappedBuffer
	stderr.limit = 64 << 10
	code, err := s.runChild(infoCtx, KindS3Command, cmdInfo, []string{"--output=json", "--log-level-console=off"}, stdout, &stderr)
	switch {
	case err != nil:
		if infoCtx.Err() != nil && ctx.Err() == nil {
			return connect.NewError(connect.CodeDeadlineExceeded, fmt.Errorf("pgbackrest info exceeded %s: %w", preflightInfoTimeout, err))
		}
		return runError(ctx, err)
	case code != 0:
		return connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf("refusing to delete the stanza: pgbackrest info exited %d: %s", code, truncate(stderr.String(), maxStartLogBytes)))
	case stdout.overflowed():
		return connect.NewError(connect.CodeFailedPrecondition, errors.New("refusing to delete the stanza: pgbackrest info output is too large to read"))
	}
	info, err := backup.DecodeInfo(stdout.String())
	if err != nil {
		return connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf("refusing to delete the stanza: %w", err))
	}
	if err := backup.RepoStatusError(info); err != nil {
		return connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf("refusing to delete the stanza: %w", err))
	}
	if len(info) == 0 {
		return connect.NewError(connect.CodeFailedPrecondition, errors.New("refusing to delete the stanza: pgbackrest info reported no stanza entry"))
	}
	if !backup.StanzaExists(info) {
		return nil
	}
	backups := 0
	for _, st := range info {
		backups += len(st.Backup)
	}
	if backups > 0 {
		return connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf("refusing to delete the stanza: the repository holds %d backups", backups))
	}
	return nil
}

// clearArchiveSpool removes the asynchronous archive-push state. A new initdb
// restarts WAL numbering at the first segment, and a stale acknowledgement from
// an earlier cluster makes archive-push skip the new cluster's first segment,
// so the archive check never sees it. Only the sidecar's own state directory,
// through safefs because children can plant links in spool/, and only after the
// worker barrier proved no worker is alive. An absent directory is fine.
func (s *Server) clearArchiveSpool() error {
	root, err := safefs.OpenRoot(s.cfg.stateSub(dirSpool))
	if err != nil {
		return err
	}
	defer root.Close()
	return root.RemoveAll("archive")
}

// cappedBuffer keeps at most limit bytes and records that it dropped any.
type cappedBuffer struct {
	mu       sync.Mutex
	buf      bytes.Buffer
	limit    int
	overflow bool
}

func (b *cappedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	n := len(p)
	if room := b.limit - b.buf.Len(); n > room {
		b.overflow = true
		p = p[:max(room, 0)]
	}
	b.buf.Write(p)
	return n, nil
}

func (b *cappedBuffer) overflowed() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.overflow
}

func (b *cappedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}
