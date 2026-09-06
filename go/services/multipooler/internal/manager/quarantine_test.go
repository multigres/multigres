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

package manager

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// fakeClock is a manually-advanced clock so the timeout gate is deterministic.
type fakeClock struct{ t time.Time }

func newFakeClock() *fakeClock {
	return &fakeClock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}
func (c *fakeClock) now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

// newQuarantineTestManager builds a manager wired with the given timeout and a
// controllable clock for the unrecoverable classifier.
func newQuarantineTestManager(t *testing.T, timeout time.Duration) (*MultipoolerManager, *fakeClock) {
	t.Helper()
	pm := newTestManager(t)
	pm.unrecoverableTimeout = timeout
	clock := newFakeClock()
	pm.nowFn = clock.now
	return pm, clock
}

// withLock runs fn with the action lock held, matching how the monitor calls
// trackRecoveryOutcome (which writes the quarantine record via record.Mutate).
func withLock(t *testing.T, pm *MultipoolerManager, fn func(ctx context.Context)) {
	t.Helper()
	lockCtx, err := pm.actionLock.Acquire(t.Context(), "test")
	require.NoError(t, err)
	defer pm.actionLock.Release(lockCtx)
	fn(lockCtx)
}

// failStart drives one failed start attempt through the classifier.
func failStart(ctx context.Context, pm *MultipoolerManager) {
	pm.trackRecoveryOutcome(ctx, remedialActionStartPostgres, postgresState{}, assert.AnError)
}

// quarantineState reads the quarantine verdict straight off the pooler record
// (the source of truth): whether it is quarantined, plus the reason and the
// time the verdict latched.
func quarantineState(pm *MultipoolerManager) (quarantined bool, reason string, since time.Time) {
	lc := pm.record.Snapshot().GetLifecycleStatus()
	if lc.GetStatus() != clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_QUARANTINED {
		return false, "", time.Time{}
	}
	return true, lc.GetReason(), lc.GetUpdated().AsTime()
}

func TestTrackRecoveryOutcome_QuarantinesAfterTimeoutAndAttempts(t *testing.T) {
	pm, clock := newQuarantineTestManager(t, 30*time.Second)

	withLock(t, pm, func(ctx context.Context) {
		// Attempts accrue but neither gate is satisfied yet.
		failStart(ctx, pm) // attempt 1, elapsed 0s
		clock.advance(10 * time.Second)
		failStart(ctx, pm) // attempt 2, elapsed 10s
		clock.advance(10 * time.Second)
		failStart(ctx, pm) // attempt 3 (floor met), elapsed 20s (< 30s timeout)
		quarantined, _, _ := quarantineState(pm)
		assert.False(t, quarantined, "must not quarantine before the timeout elapses")

		// Cross the timeout.
		clock.advance(15 * time.Second)
		failStart(ctx, pm) // attempt 4, elapsed 35s (>= 30s)
	})

	quarantined, reason, since := quarantineState(pm)
	assert.True(t, quarantined, "should quarantine once both timeout and attempt floor are met")
	assert.NotEmpty(t, reason)
	assert.False(t, since.IsZero(), "quarantine timestamp should be set")

	// Side effects: cohort ineligible + restarts disabled so the node stops looping.
	assert.Equal(t, clustermetadatapb.CohortEligibilitySignal_COHORT_ELIGIBILITY_SIGNAL_INELIGIBLE,
		pm.consensusMgr.CohortEligibility())
	assert.True(t, pm.postgresRestartsDisabled.Load(), "restarts should be disabled after quarantine")
}

// TestTrackRecoveryOutcome_RetriesCohortIneligibilityAfterPartialApply covers a
// node that latched the QUARANTINED lifecycle but is not yet cohort-INELIGIBLE —
// the state left behind if SetCohortEligibility failed on the tick the node
// first quarantined. A later monitor tick must re-apply the missing side effect;
// otherwise the coordinator keeps trying to recruit the node and the
// orchestrator never elects a new primary. (The lifecycle latch alone would make
// trackRecoveryOutcome short-circuit, so the retry has to be driven explicitly.)
func TestTrackRecoveryOutcome_RetriesCohortIneligibilityAfterPartialApply(t *testing.T) {
	pm, clock := newQuarantineTestManager(t, 30*time.Second)

	// Quarantine the node the normal way (both side effects applied).
	withLock(t, pm, func(ctx context.Context) {
		failStart(ctx, pm)
		clock.advance(10 * time.Second)
		failStart(ctx, pm)
		clock.advance(10 * time.Second)
		failStart(ctx, pm)
		clock.advance(15 * time.Second)
		failStart(ctx, pm) // crosses both gates -> quarantined
	})
	quarantined, _, _ := quarantineState(pm)
	require.True(t, quarantined, "precondition: node should be quarantined")
	require.Equal(t, clustermetadatapb.CohortEligibilitySignal_COHORT_ELIGIBILITY_SIGNAL_INELIGIBLE,
		pm.consensusMgr.CohortEligibility())

	// Simulate a first-quarantine tick where the lifecycle latched but the cohort
	// ineligibility write did not stick: force eligibility back to ELIGIBLE.
	withLock(t, pm, func(ctx context.Context) {
		require.NoError(t, pm.consensusMgr.SetCohortEligibility(ctx,
			clustermetadatapb.CohortEligibilitySignal_COHORT_ELIGIBILITY_SIGNAL_ELIGIBLE))
	})
	require.Equal(t, clustermetadatapb.CohortEligibilitySignal_COHORT_ELIGIBILITY_SIGNAL_ELIGIBLE,
		pm.consensusMgr.CohortEligibility(), "precondition: quarantined but still recruitable")

	// A later monitor tick must re-apply the missing side effect.
	withLock(t, pm, func(ctx context.Context) {
		failStart(ctx, pm)
	})

	stillQuarantined, _, _ := quarantineState(pm)
	assert.True(t, stillQuarantined, "lifecycle verdict must remain latched")
	assert.Equal(t, clustermetadatapb.CohortEligibilitySignal_COHORT_ELIGIBILITY_SIGNAL_INELIGIBLE,
		pm.consensusMgr.CohortEligibility(),
		"cohort ineligibility should be retried and applied on a later tick")
}

func TestTrackRecoveryOutcome_TimeoutGateHolds(t *testing.T) {
	// Many attempts but the timeout never elapses: must not quarantine.
	pm, clock := newQuarantineTestManager(t, time.Hour)

	withLock(t, pm, func(ctx context.Context) {
		for range 20 {
			failStart(ctx, pm)
			clock.advance(5 * time.Second) // 20 * 5s = 100s << 1h
		}
	})

	quarantined, _, _ := quarantineState(pm)
	assert.False(t, quarantined, "should not quarantine before the timeout regardless of attempt count")
}

func TestTrackRecoveryOutcome_MinAttemptsFloorHolds(t *testing.T) {
	// Timeout is tiny, so only the attempts floor should keep it from quarantining
	// on too few real attempts.
	pm, clock := newQuarantineTestManager(t, time.Second)

	withLock(t, pm, func(ctx context.Context) {
		failStart(ctx, pm) // attempt 1, elapsed 0
		clock.advance(10 * time.Second)
		failStart(ctx, pm) // attempt 2, elapsed 10s (>= timeout) but attempts < floor(3)
		quarantined, _, _ := quarantineState(pm)
		assert.False(t, quarantined, "must not quarantine before the min-attempts floor")

		failStart(ctx, pm) // attempt 3, elapsed 10s -> both gates satisfied
	})

	quarantined, _, _ := quarantineState(pm)
	assert.True(t, quarantined, "should quarantine once the attempt floor is also met")
}

func TestTrackRecoveryOutcome_ConfigurableMinAttemptsFloor(t *testing.T) {
	// A custom floor of 2 (below the default 3) must be honored: quarantine on
	// the second failed attempt once the timeout has elapsed, not the third.
	pm, clock := newQuarantineTestManager(t, time.Second)
	pm.unrecoverableMinAttempts = 2

	withLock(t, pm, func(ctx context.Context) {
		failStart(ctx, pm) // attempt 1, elapsed 0 (< timeout, and < floor)
		quarantined, _, _ := quarantineState(pm)
		assert.False(t, quarantined, "one attempt must not quarantine")

		clock.advance(2 * time.Second)
		failStart(ctx, pm) // attempt 2, elapsed 2s -> floor(2) met and timeout met
	})

	quarantined, _, _ := quarantineState(pm)
	assert.True(t, quarantined, "custom floor of 2 should quarantine on the second attempt")
}

func TestTrackRecoveryOutcome_ResetOnPostgresRunning(t *testing.T) {
	pm, clock := newQuarantineTestManager(t, 30*time.Second)

	withLock(t, pm, func(ctx context.Context) {
		failStart(ctx, pm)
		clock.advance(25 * time.Second)
		failStart(ctx, pm)

		// Postgres comes up: the streak breaks (attempts and anchor reset).
		pm.trackRecoveryOutcome(ctx, remedialActionNone, postgresState{postgresRunning: true}, nil)
		assert.Equal(t, 0, pm.unrecoverableFailedAttempts, "streak should reset when postgres runs")

		// A fresh streak: the timeout is now measured from here, so a short burst
		// must not quarantine even though a lot of wall-clock has passed overall.
		clock.advance(time.Hour)
		failStart(ctx, pm)
		failStart(ctx, pm)
		failStart(ctx, pm)
	})

	quarantined, _, _ := quarantineState(pm)
	assert.False(t, quarantined, "reset should re-anchor the timeout window")
}

func TestTrackRecoveryOutcome_DisabledWhenTimeoutZero(t *testing.T) {
	pm, clock := newQuarantineTestManager(t, 0) // classifier disabled

	withLock(t, pm, func(ctx context.Context) {
		for range 100 {
			failStart(ctx, pm)
			clock.advance(time.Minute)
		}
	})

	quarantined, _, _ := quarantineState(pm)
	assert.False(t, quarantined, "timeout 0 must never quarantine")
}

func TestTrackRecoveryOutcome_OnlyFailedRecoveryActionsCount(t *testing.T) {
	pm, clock := newQuarantineTestManager(t, 10*time.Second)

	withLock(t, pm, func(ctx context.Context) {
		// A successful start (nil error) does not count.
		pm.trackRecoveryOutcome(ctx, remedialActionStartPostgres, postgresState{}, nil)
		// A non-recovery action that errored does not count.
		pm.trackRecoveryOutcome(ctx, remedialActionReconcileState, postgresState{}, assert.AnError)
		assert.Equal(t, 0, pm.unrecoverableFailedAttempts)

		// Genuine failed recovery attempts across the timeout window quarantine.
		failStart(ctx, pm)
		clock.advance(6 * time.Second)
		pm.trackRecoveryOutcome(ctx, remedialActionRewindToLeader, postgresState{}, assert.AnError)
		clock.advance(6 * time.Second)
		pm.trackRecoveryOutcome(ctx, remedialActionRestoreFromBackup, postgresState{}, assert.AnError)
	})

	quarantined, _, _ := quarantineState(pm)
	assert.True(t, quarantined, "mixed failed recovery actions should count toward quarantine")
}

func TestTrackRecoveryOutcome_IdempotentAfterQuarantine(t *testing.T) {
	pm, clock := newQuarantineTestManager(t, 5*time.Second)

	withLock(t, pm, func(ctx context.Context) {
		failStart(ctx, pm)
		clock.advance(3 * time.Second)
		failStart(ctx, pm)
		clock.advance(3 * time.Second)
		failStart(ctx, pm) // attempt 3, elapsed 6s -> quarantine
		quarantined, _, firstSince := quarantineState(pm)
		require.True(t, quarantined)

		// Further failing ticks must not re-publish or move the latch time.
		clock.advance(time.Minute)
		failStart(ctx, pm)
		_, _, laterSince := quarantineState(pm)
		assert.Equal(t, firstSince, laterSince, "quarantine latch time should be stable")
	})
}

func TestMarkPoolerQuarantinedLocked_Idempotent(t *testing.T) {
	pm := newTestManager(t)

	withLock(t, pm, func(ctx context.Context) {
		pm.markPoolerQuarantinedLocked(ctx, "first")
		quarantined, reason, firstSince := quarantineState(pm)
		require.True(t, quarantined)
		require.Equal(t, "first", reason)

		// A second call is a no-op: the idempotency guard short-circuits before
		// re-publishing, so neither the reason nor the latch time changes.
		pm.markPoolerQuarantinedLocked(ctx, "second")
		quarantined, reason, laterSince := quarantineState(pm)
		assert.True(t, quarantined)
		assert.Equal(t, "first", reason, "reason must not change on a repeat call")
		assert.Equal(t, firstSince, laterSince, "latch time must not change on a repeat call")
	})
}

func TestManagerNow_FallsBackToWallClock(t *testing.T) {
	pm := newTestManager(t)
	pm.nowFn = nil // exercise the time.Now() fallback

	before := time.Now()
	got := pm.now()
	after := time.Now()

	assert.False(t, got.Before(before), "now() should not predate the call")
	assert.False(t, got.After(after), "now() should not postdate the call")
}
