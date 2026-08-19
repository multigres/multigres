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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	pgctldpb "github.com/multigres/multigres/go/pb/pgctldservice"
)

// newSentinelTestManager builds a manager whose pooler directory is a writable
// temp dir, so the on-disk rewind-sentinel helpers can be exercised.
func newSentinelTestManager(t *testing.T) *MultipoolerManager {
	t.Helper()
	return newTestManager(t, withRecord(newRecordFromProto(&clustermetadatapb.Multipooler{
		Type:          clustermetadatapb.PoolerType_REPLICA,
		ServingStatus: clustermetadatapb.PoolerServingStatus_DISABLED,
		PoolerDir:     t.TempDir(),
	})))
}

func TestRewindSentinel_RoundTrip(t *testing.T) {
	pm := newSentinelTestManager(t)

	present, err := pm.hasRewindSentinel()
	require.NoError(t, err)
	assert.False(t, present, "no sentinel initially")

	require.NoError(t, pm.writeRewindSentinel())
	present, err = pm.hasRewindSentinel()
	require.NoError(t, err)
	assert.True(t, present, "sentinel present after write")

	// Writing again is idempotent.
	require.NoError(t, pm.writeRewindSentinel())

	require.NoError(t, pm.removeRewindSentinel())
	present, err = pm.hasRewindSentinel()
	require.NoError(t, err)
	assert.False(t, present, "sentinel gone after remove")

	// Removing a missing sentinel is not an error.
	require.NoError(t, pm.removeRewindSentinel())
}

// TestDiscoverPostgresState_RewindSentinel verifies discoverPostgresState surfaces
// the on-disk rewind sentinel, the durable signal the monitor gates on.
func TestDiscoverPostgresState_RewindSentinel(t *testing.T) {
	ctx := t.Context()

	pm := NewTestMultipoolerManager(t)
	pm.pgctldClient = &mockPgctldClient{
		statusResponse: &pgctldpb.StatusResponse{Status: pgctldpb.ServerStatus_STOPPED},
	}

	state, err := pm.discoverPostgresState(ctx)
	require.NoError(t, err)
	assert.False(t, state.rewindSentinelPresent, "no rewind sentinel initially")

	require.NoError(t, pm.writeRewindSentinel())

	state, err = pm.discoverPostgresState(ctx)
	require.NoError(t, err)
	assert.True(t, state.rewindSentinelPresent, "rewind sentinel surfaced once written")
}

// TestDetermineRemedialAction_RewindSentinelRearmsDivergence verifies that a
// rewind sentinel re-arms the (in-memory, restart-lost) suspectedDivergence flag,
// routing the node through the rewind-repair path rather than a blind start.
func TestDetermineRemedialAction_RewindSentinelRearmsDivergence(t *testing.T) {
	ctx := t.Context()
	pm := newTestManager(t)

	// Sentinel present, divergence not yet suspected: re-arm.
	state := postgresState{pgctldAvailable: true, rewindSentinelPresent: true}
	assert.Equal(t, remedialActionMarkRewindInterrupted, pm.determineRemedialAction(ctx, state),
		"a rewind sentinel with divergence unset must re-arm suspected divergence")

	// Once divergence is already suspected, the re-arm does not fire again (it is a
	// one-shot per incident; the rewind path takes over).
	withLock(t, pm, func(lockCtx context.Context) {
		_, err := pm.consensusMgr.SetSuspectedDivergence(lockCtx, true)
		require.NoError(t, err)
	})
	assert.NotEqual(t, remedialActionMarkRewindInterrupted, pm.determineRemedialAction(ctx, state),
		"re-arm must not fire once suspected divergence is already set")
}

// TestTakeRemedialAction_MarkRewindInterruptedSetsDivergence verifies the action
// actually sets the flag the rewind path gates on.
func TestTakeRemedialAction_MarkRewindInterruptedSetsDivergence(t *testing.T) {
	pm := newTestManager(t)
	require.False(t, pm.consensusMgr.SuspectedDivergence())

	withLock(t, pm, func(ctx context.Context) {
		require.NoError(t, pm.takeRemedialAction(ctx, remedialActionMarkRewindInterrupted, postgresState{}))
	})

	assert.True(t, pm.consensusMgr.SuspectedDivergence(),
		"marking an interrupted rewind must set suspected divergence")
}

// TestTrackRecoveryOutcome_RewindSentinelCountsWhileRunning is the core of the
// fix: a half-rewound node that starts into recovery and then waits forever for
// unreachable WAL reports postgresRunning=true. Without the sentinel that would
// reset the unrecoverable streak every tick and the node would spin forever. With
// the sentinel present, failed rewind-repair attempts must keep counting so the
// node is eventually quarantined for replacement.
func TestTrackRecoveryOutcome_RewindSentinelCountsWhileRunning(t *testing.T) {
	pm, clock := newQuarantineTestManager(t, 30*time.Second)

	// "Running but mid-rewind": postgresRunning=true AND the sentinel is present.
	running := postgresState{postgresRunning: true, rewindSentinelPresent: true}
	failRewind := func(ctx context.Context) {
		pm.trackRecoveryOutcome(ctx, remedialActionRewindToLeader, running, assert.AnError)
	}

	withLock(t, pm, func(ctx context.Context) {
		failRewind(ctx) // attempt 1, elapsed 0
		assert.Equal(t, 1, pm.unrecoverableFailedAttempts,
			"a running-but-mid-rewind node must not reset the streak")
		clock.advance(15 * time.Second)
		failRewind(ctx) // attempt 2, elapsed 15s
		clock.advance(20 * time.Second)
		failRewind(ctx) // attempt 3 (floor met), elapsed 35s (>= 30s)
	})

	quarantined, reason, _ := quarantineState(pm)
	assert.True(t, quarantined, "an unrecoverable interrupted rewind must quarantine despite postgres appearing to run")
	assert.NotEmpty(t, reason)
}

// TestTrackRecoveryOutcome_ResetsWhenRunningWithoutSentinel guards the boundary:
// a genuinely healthy running node (no sentinel) still breaks the streak.
func TestTrackRecoveryOutcome_ResetsWhenRunningWithoutSentinel(t *testing.T) {
	pm, clock := newQuarantineTestManager(t, 30*time.Second)

	withLock(t, pm, func(ctx context.Context) {
		pm.trackRecoveryOutcome(ctx, remedialActionRewindToLeader,
			postgresState{postgresRunning: true, rewindSentinelPresent: true}, assert.AnError)
		require.Equal(t, 1, pm.unrecoverableFailedAttempts)

		clock.advance(time.Second)
		// A clean, sentinel-free running node: the incident is over.
		pm.trackRecoveryOutcome(ctx, remedialActionNone,
			postgresState{postgresRunning: true}, nil)
		assert.Equal(t, 0, pm.unrecoverableFailedAttempts,
			"a healthy running node without a sentinel resets the streak")
	})
}
