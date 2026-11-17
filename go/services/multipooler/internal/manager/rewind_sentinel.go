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
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/mterrors"
	pgctldpb "github.com/multigres/multigres/go/pb/pgctldservice"
)

// rewind_sentinel.go implements a durable, on-disk marker for an in-progress
// rewind recovery, mirroring the bootstrap sentinel (see rpc_first_backup.go).
// The recovery temporarily enables archive restore and pg_rewind mutates the
// target data directory in place, so an interruption can leave either unsafe
// archive recovery enabled or a partially rewound directory.
//
// runPgRewind writes the sentinel before enabling archive restore and
// restartAsStandbyLocked removes it only after postgres is verified back up as a
// standby. Its presence on a later monitor tick is therefore the authoritative signal that a
// prior rewind did not complete — the one durable signal that survives a process
// restart (the in-memory suspectedDivergence flag does not). The monitor uses it
// to re-arm the rewind-repair path instead of starting postgres on the
// half-rewound directory, and to let the unrecoverable-recovery classifier
// quarantine the node if repair keeps failing.

// rewindSentinel returns the rewind sentinel. Like every sentinel it lives in
// pooler_dir; see sentinel.go.
func (pm *MultipoolerManager) rewindSentinel() sentinel {
	return pm.newSentinel(constants.RewindSentinelFile, "pg_rewind in progress\n")
}

// A failed pg_rewind is not always a ruined data directory. pg_rewind only starts
// modifying the target after it has connected to the source, read both file
// lists and read the target's WAL since the last common checkpoint; a failure
// before that (source unreachable or demoted, WAL unreadable) leaves the
// directory exactly as it was, and re-running the rewind is the right repair.
// After that point it truncates, removes and overwrites files in place and
// writes the control file last, so a failure leaves a mix of old and new files
// that PostgreSQL documents as generally unrecoverable and that must not be
// rewound again (a retry against a different leader can mix timelines) or
// started. pg_rewind records no marker when it crosses that point, so the
// pooler derives the evidence itself:
//
//   - rewindAttempt is written just before the mutating pg_rewind call, after the
//     dry-run. Its modification time is the reference, and it records the run id
//     that the request carries.
//   - Before judging a run the pooler fences that id through pgctld
//     (PgRewindAbort): afterwards the run is not running and cannot start, so a
//     request that pgctld had not handled yet when the call ended cannot start
//     after the directory was judged untouched. If pgctld cannot say, nothing is
//     judged and the marker stays for the next settle. A marker without an id is
//     judged on the running record below. A pgctld without the RPC cannot fence
//     and does not write the running record either, so nothing says the run has
//     stopped and the directory is treated as modified.
//   - pgctld records that a mutating pg_rewind is running (RewindRunningFile) and
//     removes the record when the process has exited. A call that failed on a
//     timeout can return while pg_rewind is still being killed, so the pooler
//     waits for the record to clear before it assesses the directory, and treats
//     the directory as modified if the exit cannot be confirmed.
//   - After a failure, or on restart with the marker still present, any entry
//     in the data directory modified at or after the reference means the run
//     started modifying it. The pooler then writes rewindTainted into the data
//     directory, a durable verdict that blocks further rewinds and quarantines
//     the node so it is replaced from backup.
//   - The marker is removed once a run succeeds or a failure is verified to have
//     left the directory untouched, so a later postgres start (which legitimately
//     writes to the directory) is never mistaken for a rewind.
//
// The dry-run writes nothing, except that pgctld first crash-recovers a target
// that was not cleanly stopped. That happens before the marker is written and
// leaves the target cleanly stopped, so it is never mistaken for the rewind and a
// failed dry-run is always retryable.
//
// Limitations. Every one of them errs on the safe side: the node is replaced
// from backup when it might have been repaired, never the other way round.
//
//   - If the target is still not cleanly stopped when the mutating call starts,
//     the real pg_rewind crash-recovers it before it plans. That recovery writes
//     to the directory, so a failure after it is reported as modified even if
//     pg_rewind never got further.
//   - Timestamps are compared at the resolution the filesystem keeps, which can be
//     coarse. A write that lands in the same tick as the marker is read as made
//     after it (hence the >= comparison), so a directory can be reported as
//     modified when only the marker's own tick overlapped.
//   - The marker is in pooler_dir and the scanned entries are in the data
//     directory. Both are on the same host and normally on the same clock, so the
//     comparison is meaningful. If they were on volumes whose timestamps come from
//     different clocks, skew larger than the time pg_rewind takes to start writing
//     could hide a modification. This is not expected for the supported
//     deployments, and is noted here so that a change in volume layout revisits it.
//   - If pgctld cannot be confirmed to have reaped pg_rewind within
//     rewindRunningWait (for example pgctld died while it was running and left its
//     running record behind), the directory is treated as modified. The attempt
//     marker is kept then, because a pg_rewind that is still running can delete the
//     verdict (a file only the target has), and the next settle derives it again.
//   - If the verdict cannot be written into the data directory (for example the
//     disk is full, which is also a likely reason pg_rewind failed), it is logged
//     rather than returned as an error: pg_rewind cannot run on a directory that
//     cannot be written anyway. The pooler is quarantined directly, which is
//     durable on its own, and the attempt marker is kept so the verdict is derived
//     again on the next settle.
//   - The rewind sentinel itself is only removed by a successful rewind, not by a
//     restore of the data directory. That predates the taint verdict and is not
//     changed here.

// rewindAttempt returns the marker for a launched, not yet assessed pg_rewind
// run. Like every sentinel it lives in pooler_dir; see sentinel.go.
func (pm *MultipoolerManager) rewindAttempt() sentinel {
	return pm.rewindAttemptFor("")
}

// rewindAttemptFor is rewindAttempt carrying the id of the run it describes, which
// is what the pooler fences through pgctld before it judges the outcome. A marker
// without an id (an older pooler wrote it) is judged without the fence.
func (pm *MultipoolerManager) rewindAttemptFor(runID string) sentinel {
	contents := "pg_rewind run launched\n"
	if runID != "" {
		contents += rewindRunIDKey + runID + "\n"
	}
	return pm.newSentinel(constants.RewindAttemptFile, contents)
}

const rewindRunIDKey = "run_id="

// rewindAttemptRunID returns the run id recorded in the attempt marker. It is ""
// with no error when the marker is missing or has no id, which is the marker an
// older pooler wrote. A marker that cannot be read is an error and not "no id":
// the run is then unknown, and skipping the fence would judge it on a guess.
func (pm *MultipoolerManager) rewindAttemptRunID() (string, error) {
	data, err := os.ReadFile(pm.rewindAttempt().path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return "", nil
		}
		return "", err
	}
	for line := range strings.SplitSeq(string(data), "\n") {
		if id, ok := strings.CutPrefix(line, rewindRunIDKey); ok {
			return strings.TrimSpace(id), nil
		}
	}
	return "", nil
}

// rewindTainted returns the verdict marker for a data directory modified by a
// pg_rewind run that did not complete. It lives in the data directory it
// describes, unlike the other sentinels, so that replacing the directory removes
// it; see constants.RewindTaintedFile.
func (pm *MultipoolerManager) rewindTainted() sentinel {
	return sentinel{
		path:     filepath.Join(postgresDataDir(), constants.RewindTaintedFile),
		contents: "pg_rewind modified the data directory and did not complete\n",
	}
}

// rewindTaintedReason is the quarantine reason for a tainted data directory.
const rewindTaintedReason = "pg_rewind modified the data directory and did not complete; the data directory is unusable and the node must be replaced"

// autoConfFileName is excluded from the modification scan: the pooler edits it
// itself while a rewind is in progress (temporary restore_command).
const autoConfFileName = "postgresql.auto.conf"

// firstModifiedSince walks the tree under root, following a symlink at any level
// (pg_wal is commonly a symlink to a separate volume), and returns the path of
// the first entry whose modification time is not before ref, or "" if there is
// none. A root that cannot be read, missing included, is an error and not proof
// that nothing changed: a volume that disappeared during the assessment must not
// read as an untouched directory. The same holds for a symlink whose target
// cannot be inspected, such as a pg_wal or tablespace volume that dropped out.
// An entry below the root that disappears while walking is tolerated, because
// removing it changed its parent's modification time, which is compared. The
// root directory and subdirectories are included:
// removing or creating an entry changes the parent's modification time. excluded
// is skipped.
func firstModifiedSince(root string, ref time.Time, excluded string) (string, error) {
	var found string
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			if path != root && errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return err
		}
		if path == excluded {
			return nil
		}
		// A symlink's own modification time is when it was created; creating or
		// removing one changes its parent directory, which is compared. What matters
		// is the directory it points to.
		if d.Type()&fs.ModeSymlink != 0 {
			target, statErr := os.Stat(path)
			if statErr != nil {
				// A target that cannot be inspected, a vanished volume included, is
				// evidence that cannot be read and must not read as untouched.
				return statErr
			}
			if target.IsDir() {
				// The trailing separator makes the walk resolve the symlink.
				sub, err := firstModifiedSince(path+string(filepath.Separator), ref, excluded)
				if err != nil {
					return err
				}
				if sub != "" {
					found = sub
					return filepath.SkipAll
				}
			}
			return nil
		}
		info, err := d.Info()
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return err
		}
		if !info.ModTime().Before(ref) {
			found = path
			return filepath.SkipAll
		}
		return nil
	})
	return found, err
}

// rewindRunningWait bounds how long settleRewindAttempt waits for pgctld to report
// that the pg_rewind it launched has exited, and rewindRunningPoll is how often it
// looks. Variables so that tests need not wait in real time.
var (
	rewindRunningWait = 30 * time.Second
	rewindRunningPoll = 200 * time.Millisecond
)

// awaitRewindExit waits until pgctld has removed its running marker, meaning the
// pg_rewind it launched has exited. A call that failed on a timeout or
// cancellation can return before the process is gone, and a process that is still
// being killed can still write, so the data directory must not be assessed before
// then. It returns false when the marker is still there after rewindRunningWait,
// which also happens if pgctld died with a pg_rewind running.
func (pm *MultipoolerManager) awaitRewindExit(ctx context.Context) bool {
	running := pm.newSentinel(constants.RewindRunningFile, "")
	// The caller's context may already be cancelled, which is why we are here.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), rewindRunningWait)
	defer cancel()
	for {
		if present, err := running.present(); err == nil && !present {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(rewindRunningPoll):
		}
	}
}

// fenceRewindRun asks pgctld to abort the run id: when it reports true the run is
// not running and cannot start, so the data directory can be judged. The running
// record alone cannot say that, because it does not exist yet for a request pgctld
// has not handled. A marker without a run id (an older pooler wrote it) is judged
// on the running record, which the pgctld it ran against writes. A pgctld without
// the RPC reports false: it cannot fence, and it does not write the running record
// either, so nothing says the run has stopped and the caller must treat the
// directory as modified. Any other failure means the run's state is unknown; the
// caller must not judge the directory then.
func (pm *MultipoolerManager) fenceRewindRun(ctx context.Context, runID string) (bool, error) {
	if runID == "" || pm.pgctldClient == nil {
		return true, nil
	}
	// The caller's context may already be cancelled, which is why we are here.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), rewindRunningWait)
	defer cancel()
	resp, err := pm.pgctldClient.PgRewindAbort(ctx, &pgctldpb.PgRewindAbortRequest{RunId: runID})
	if err != nil {
		if status.Code(err) == codes.Unimplemented {
			return false, nil
		}
		return false, err
	}
	if resp.GetTerminated() {
		pm.logger.WarnContext(ctx, "terminated a pg_rewind that was still running", "run_id", runID)
	}
	return true, nil
}

// settleRewindAttempt resolves a rewind attempt marker whose outcome has not been
// assessed: after a failed run, and on restart when a run was interrupted. It
// returns true when the run modified the data directory, after durably writing
// the tainted verdict. Otherwise the directory is untouched and the marker is
// removed so that later writes by postgres are not attributed to the rewind. No
// marker means there is nothing to settle. If the evidence cannot be read, the
// directory is treated as modified.
//
// Before assessing, it waits for pgctld to report that the pg_rewind has exited;
// if that cannot be confirmed the directory is treated as modified.
//
// Postgres must not have been started since the run was launched. Callers hold
// the action lock.
func (pm *MultipoolerManager) settleRewindAttempt(ctx context.Context) (tainted bool, err error) {
	attempt := pm.rewindAttempt()
	present, err := attempt.present()
	if err != nil {
		return false, err
	}
	if !present {
		return false, nil
	}

	// Fence the run first, so a request pgctld has not handled yet cannot start
	// after the directory is judged. If pgctld cannot say, keep the marker and try
	// again later rather than judge on a guess.
	runID, err := pm.rewindAttemptRunID()
	if err != nil {
		return false, mterrors.Wrap(err, "cannot read the run id from the rewind attempt marker")
	}
	fenced, err := pm.fenceRewindRun(ctx, runID)
	if err != nil {
		return false, mterrors.Wrap(err, "cannot confirm what happened to the pg_rewind run")
	}

	modified, evidence := true, "unknown"
	// A pg_rewind that may still be running can remove the verdict: the marker is
	// a file only the target has, and pg_rewind deletes those. Then the attempt
	// marker is the only evidence left, so it is kept and the verdict is derived
	// again on the next settle.
	keepAttempt := false
	if !fenced {
		pm.logger.ErrorContext(ctx, "pgctld cannot fence pg_rewind runs, so nothing says the run has stopped; treating the data directory as modified", "run_id", runID)
		evidence = "pgctld cannot fence pg_rewind runs"
		keepAttempt = true
	} else if !pm.awaitRewindExit(ctx) {
		pm.logger.ErrorContext(ctx, "pgctld still reports a pg_rewind running; treating the data directory as modified")
		evidence = "pg_rewind not confirmed to have exited"
		keepAttempt = true
	} else if info, statErr := os.Stat(attempt.path); statErr != nil {
		pm.logger.ErrorContext(ctx, "cannot read the rewind attempt marker; treating the data directory as modified", "error", statErr)
	} else if path, scanErr := firstModifiedSince(postgresDataDir(), info.ModTime(), filepath.Join(postgresDataDir(), autoConfFileName)); scanErr != nil {
		pm.logger.ErrorContext(ctx, "cannot scan the data directory after pg_rewind; treating it as modified", "error", scanErr)
	} else {
		modified, evidence = path != "", path
	}

	if modified {
		if err := pm.rewindTainted().write(); err != nil {
			// Not an error for the caller: the directory cannot be rewound while it
			// cannot be written, and the quarantine below is durable without the
			// marker. Keep the attempt marker so the next settle derives the verdict
			// again and retries recording it.
			pm.logger.ErrorContext(ctx, "pg_rewind modified the data directory but the verdict could not be recorded in it; quarantining the pooler instead", "error", err, "first_modified", evidence)
			pm.markPoolerQuarantinedLocked(ctx, rewindTaintedReason)
			return true, nil
		}
		pm.logger.ErrorContext(ctx, "pg_rewind modified the data directory and did not complete; it will not be rewound again", "first_modified", evidence)
		if keepAttempt {
			return true, nil
		}
		if err := attempt.remove(); err != nil {
			pm.logger.WarnContext(ctx, "failed to remove rewind attempt marker after recording taint", "error", err)
		}
		return true, nil
	}
	return false, attempt.remove()
}

// failRewindRun settles the attempt marker after a pg_rewind run failed with
// runErr and returns the error the caller should return: runErr itself when the
// data directory is untouched (the rewind may be retried), or runErr annotated as
// tainted once the verdict is recorded. The monitor quarantines a tainted
// node; see determineRemedialAction.
func (pm *MultipoolerManager) failRewindRun(ctx context.Context, runErr error) error {
	tainted, err := pm.settleRewindAttempt(ctx)
	if err != nil {
		return errors.Join(runErr, mterrors.Wrap(err, "failed to settle rewind attempt marker"))
	}
	if tainted {
		return mterrors.Wrap(runErr, "data directory modified by a pg_rewind that did not complete")
	}
	return runErr
}

// passRewindRun removes the attempt marker after a pg_rewind run succeeded or
// found nothing to rewind. A failure to remove it is not fatal for the rewind
// but leaves a marker that a later restart would read as evidence, so it is
// surfaced.
func (pm *MultipoolerManager) passRewindRun(ctx context.Context) {
	if err := pm.rewindAttempt().remove(); err != nil {
		pm.logger.WarnContext(ctx, "failed to remove rewind attempt marker after pg_rewind", "error", err)
	}
}
