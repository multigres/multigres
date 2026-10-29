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
	"os"
	"path/filepath"
	"time"

	"github.com/multigres/multigres/go/common/mterrors"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	backupengine "github.com/multigres/multigres/go/services/multipooler/internal/manager/backup"
)

var errRestoreRunning = mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION, "a restore is still running in the sidecar; waiting for it to end")

// restoreWaitError is the outcome of a tick that can do nothing yet because a
// sidecar restore may still be writing PGDATA, or the sidecar cannot be asked
// while a marker says one may have been. Waiting is not a failed recovery: the
// monitor must not count it toward quarantine.
type restoreWaitError struct {
	reason error
}

func (e *restoreWaitError) Error() string { return e.reason.Error() }
func (e *restoreWaitError) Unwrap() error { return e.reason }

// isRestoreWaiting reports whether err only means "wait for the restore".
func isRestoreWaiting(err error) bool {
	if _, ok := errors.AsType[*restoreWaitError](err); ok {
		return true
	}
	return errors.Is(err, backupengine.ErrRestoreUnsettled)
}

// errRestoreMarkerUnreadable marks a wait whose cause is a restore marker that
// cannot be read, which is neither a down sidecar nor a running restore.
var errRestoreMarkerUnreadable = errors.New("cannot read the restore marker")

// restoreWaitLogEvery is how often a waiting monitor repeats its log line.
const restoreWaitLogEvery = 5 * time.Minute

// logRestoreWaiting logs the first wait loudly and then once per
// restoreWaitLogEvery, so a long restore does not fill the log every tick.
func (pm *MultipoolerManager) logRestoreWaiting(ctx context.Context, err error) {
	pm.behindMu.Lock()
	defer pm.behindMu.Unlock()
	now := pm.now()
	if !pm.lastWaitLog.IsZero() && now.Sub(pm.lastWaitLog) < restoreWaitLogEvery {
		return
	}
	pm.lastWaitLog = now
	pm.logger.InfoContext(ctx, "MonitorPostgres: waiting for a restore to finish before acting", "reason", err, "marker_age_seconds", pm.markerAgeSec.Load()) //nolint:sloglint // message intentionally starts with an operation name or proper noun
}

// sidecarStatusTimeout bounds each Status call made on a decision path.
var sidecarStatusTimeout = 5 * time.Second

// sidecarRestoreEnded returns nil only when the sidecar answers and reports no
// restore running. Anything else, including a failed call, means the data
// directory must not be touched yet.
func (pm *MultipoolerManager) sidecarRestoreEnded(ctx context.Context) error {
	callCtx, cancel := context.WithTimeout(ctx, sidecarStatusTimeout)
	defer cancel()
	st, err := pm.sidecar.Status(callCtx)
	if err != nil {
		return &restoreWaitError{reason: mterrors.Wrap(err, "cannot confirm that no sidecar restore is running")}
	}
	if st.GetRestoreInProgress() {
		return &restoreWaitError{reason: errRestoreRunning}
	}
	return nil
}

// restoreSettled is the one question every decision to start postgres, to treat
// PGDATA as initialized, or to restore must ask. The marker is upstream's restore
// sentinel (restore_sentinel.go): multipooler writes it before it sends
// Run(restore) and removes it only after the sidecar confirmed the restore ended
// and the steps after it ran. So no marker means this multipooler started no
// restore that is unaccounted for, and nothing is asked of the sidecar. A marker
// means not settled, and an unreadable marker counts as present; neither ever
// causes a delete.
func (pm *MultipoolerManager) restoreSettled(ctx context.Context) bool {
	present, mtime, err := pm.restoreSentinel().info()
	if err != nil {
		pm.markerAgeSec.Store(int64(pm.unreadableMarkerAge().Seconds()))
		pm.logEveryRestoreWait(&pm.lastSettledWarn, func() {
			pm.logger.WarnContext(ctx, "cannot read the restore marker; treating the restore as unsettled", "error", err)
		})
		return false
	}
	pm.clearUnreadableMarker()
	if !present {
		pm.markerAgeSec.Store(0)
		return true
	}
	age := pm.restoreMarkerAge(mtime)
	pm.markerAgeSec.Store(int64(age.Seconds()))
	pm.logEveryRestoreWait(&pm.lastSettledWarn, func() {
		pm.logger.WarnContext(ctx, "an incomplete restore marker exists; not trusting or starting the data directory",
			"marker", pm.restoreSentinel().path, "marker_age", age.Round(time.Second))
	})
	return false
}

// restoreMarkerAge is how long the marker has existed; a clock that reads
// before the marker's time counts as zero.
func (pm *MultipoolerManager) restoreMarkerAge(mtime time.Time) time.Duration {
	return max(0, pm.now().Sub(mtime))
}

// unreadableMarkerAge ages a marker whose stat failed: by its mtime when the
// pooler directory listing still yields one, else from the first time this
// process found it unreadable, so a persistent failure still ages toward
// quarantine instead of waiting forever. The listing is a read-only lstat of
// one name that only supplies an age; nothing is decided on its content.
func (pm *MultipoolerManager) unreadableMarkerAge() time.Duration {
	s := pm.restoreSentinel()
	entries, err := os.ReadDir(filepath.Dir(s.path))
	if err == nil {
		for _, e := range entries {
			if e.Name() != s.name {
				continue
			}
			if fi, ierr := e.Info(); ierr == nil {
				return pm.restoreMarkerAge(fi.ModTime())
			}
		}
	}
	pm.behindMu.Lock()
	defer pm.behindMu.Unlock()
	if pm.unreadableMarkerSince.IsZero() {
		pm.unreadableMarkerSince = pm.now()
	}
	return pm.restoreMarkerAge(pm.unreadableMarkerSince)
}

// clearUnreadableMarker forgets the first-seen time once the marker reads again.
func (pm *MultipoolerManager) clearUnreadableMarker() {
	pm.behindMu.Lock()
	pm.unreadableMarkerSince = time.Time{}
	pm.behindMu.Unlock()
}

// sidecarRestoreStepsBudget is the sidecar's bound on the stop and start
// children around a restore: the start before stop, the stop and up to three
// starts after, each capped at 60s by the sidecar's restoreStepTimeout.
const sidecarRestoreStepsBudget = 5 * time.Minute

// restoreMarkerStaleAfter is how long a marker may wait before the wait counts
// as a failed recovery: past the longest a restore can run in the sidecar (its
// own deadline, drain included), its bounded stop and start steps, and the
// steps after it on this side, no restore can still be writing. A sidecar that
// is down, or one that keeps reporting a restore that never ends, is a node to
// replace rather than wait on forever.
func restoreMarkerStaleAfter() time.Duration {
	return backupengine.SidecarRestoreDeadline() + sidecarRestoreStepsBudget + sidecarPostRestoreBudget
}

// restoreWaitCounts reports whether a wait that was classified as waiting has
// gone on so long that it must count as a failed recovery. Every child of the
// sidecar's restore sequence is bounded, so a restore it reports running for
// longer than restoreMarkerStaleAfter is stuck, exactly as an unreachable
// sidecar is. It never causes a delete or a start.
func (pm *MultipoolerManager) restoreWaitCounts(err error) bool {
	if _, ok := errors.AsType[*restoreWaitError](err); !ok {
		return false
	}
	present, mtime, merr := pm.restoreSentinel().info()
	if merr != nil {
		return pm.unreadableMarkerAge() > restoreMarkerStaleAfter()
	}
	pm.clearUnreadableMarker()
	if !present {
		return false
	}
	return pm.restoreMarkerAge(mtime) > restoreMarkerStaleAfter()
}

// restoreWaitCountsReason says why a wait that restoreWaitCounts counted is
// stuck, because the three causes call for different action.
func restoreWaitCountsReason(err error) string {
	switch {
	case errors.Is(err, errRestoreMarkerUnreadable):
		return "the restore marker has been unreadable longer than a restore can take"
	case errors.Is(err, errRestoreRunning):
		return "the sidecar reports a restore running longer than a restore can take"
	default:
		return "the sidecar has been unreachable longer than a restore can take"
	}
}

// logEveryRestoreWait runs log at most once per restoreWaitLogEvery for last.
func (pm *MultipoolerManager) logEveryRestoreWait(last *time.Time, log func()) {
	pm.behindMu.Lock()
	defer pm.behindMu.Unlock()
	now := pm.now()
	if !last.IsZero() && now.Sub(*last) < restoreWaitLogEvery {
		return
	}
	*last = now
	log()
}

// restoreDataDirUntrusted reports that PG_VERSION, if present, may belong to a
// restore that is running or never finished: the sidecar writes PG_VERSION
// early, so its presence proves nothing while the marker exists. It is local
// state only, because hasDataDirectory runs inside RPCs that must not wait on
// the sidecar. An unreadable marker is untrusted.
func (pm *MultipoolerManager) restoreDataDirUntrusted() bool {
	present, mtime, err := pm.restoreSentinel().info()
	if present {
		// This runs inside RPCs that are polled, so the ERROR is once per marker
		// (a marker is identified by when it was written) and debug after.
		pm.behindMu.Lock()
		first := !pm.untrustedLoggedFor.Equal(mtime)
		pm.untrustedLoggedFor = mtime
		pm.behindMu.Unlock()
		if first {
			pm.logger.Error("an incomplete restore marker exists; the data directory is not treated as initialized", "marker", pm.restoreSentinel().path)
		} else {
			pm.logger.Debug("data directory untrusted: restore marker present", "marker", pm.restoreSentinel().path)
		}
		return true
	}
	if err == nil {
		pm.behindMu.Lock()
		pm.untrustedLoggedFor = time.Time{}
		pm.behindMu.Unlock()
	}
	return err != nil
}

// dataDirPresentOrUntrusted is the guard for callers that create data: a
// directory being restored is present, not absent.
func (pm *MultipoolerManager) dataDirPresentOrUntrusted() bool {
	_, err := os.Stat(filepath.Join(postgresDataDir(), "PG_VERSION"))
	return err == nil || pm.restoreDataDirUntrusted()
}

// prepareRestore runs before every sidecar restore. It refuses while a restore
// runs or cannot be ruled out, so the data directory is never removed or
// restored over under a restore that is still writing it. Removing the
// unfinished directory is restoreAndStartPostgres's job, as upstream's restore
// sentinel defines it, and it asks the same question first.
func (pm *MultipoolerManager) prepareRestore(ctx context.Context) error {
	present, merr := pm.restoreSentinel().present()
	if err := pm.sidecarRestoreEnded(ctx); err != nil {
		if present || merr != nil || errors.Is(err, errRestoreRunning) {
			return err
		}
		// No marker: no restore of ours is unaccounted for, so a sidecar that
		// cannot be reached merely makes the restore itself fail below.
		return nil
	}
	if merr != nil {
		return mterrors.Wrap(merr, "cannot read the restore marker")
	}
	return nil
}
