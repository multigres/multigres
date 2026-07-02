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
	"errors"
	"log/slog"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multipooler/internal/servingstate"
)

// testComponent records state transitions for testing.
type testComponent struct {
	lastType   clustermetadatapb.PoolerType
	lastRole   servingstate.RoutingRole
	lastStatus clustermetadatapb.PoolerServingStatus
	callCount  int
	err        error // if set, OnStateChange returns this error
}

func (c *testComponent) OnStateChange(_ context.Context, state servingstate.State) error {
	c.callCount++
	if c.err != nil {
		return c.err
	}
	// Record the derived PoolerType so existing assertions keep reading lastType.
	c.lastType = poolerTypeForLeader(state.RoutingRole.Writable())
	c.lastRole = state.RoutingRole
	c.lastStatus = state.ServingStatus
	return nil
}

// slowComponent tracks concurrent execution via an atomic counter.
type slowComponent struct {
	called atomic.Bool
}

func (c *slowComponent) OnStateChange(_ context.Context, _ servingstate.State) error {
	c.called.Store(true)
	return nil
}

func newTestLogger() *slog.Logger {
	return slog.New(slog.DiscardHandler)
}

// testPoolerID identifies the synthetic pooler used by StateManager tests. A
// PRIMARY record's SelfLeadership must name this id (the record invariant).
var testPoolerID = &clustermetadatapb.ID{
	Component: clustermetadatapb.ID_MULTIPOOLER,
	Cell:      "zone1",
	Name:      "test-pooler",
}

func newTestMultiPooler(poolerType clustermetadatapb.PoolerType, status clustermetadatapb.PoolerServingStatus) *clustermetadatapb.MultiPooler {
	mp := &clustermetadatapb.MultiPooler{
		Id:            testPoolerID,
		Type:          poolerType,
		ServingStatus: status,
	}
	// Keep the Type ⇔ SelfLeadership invariant so the record validates: a
	// PRIMARY names itself; any other type carries no self-leadership.
	if poolerType == clustermetadatapb.PoolerType_PRIMARY {
		mp.SelfLeadership = &clustermetadatapb.LeaderObservation{LeaderId: testPoolerID}
	}
	return mp
}

// primaryObs is the leadership observation a PRIMARY test record must carry to
// satisfy the Type ⇔ SelfLeadership invariant.
func primaryObs() *clustermetadatapb.LeaderObservation {
	return &clustermetadatapb.LeaderObservation{LeaderId: testPoolerID}
}

// newTestRecord returns a poolerRecord seeded with a proto carrying the given
// state and a no-op topo store. Suitable for StateManager tests that only
// exercise component fan-out, not publishing.
func newTestRecord(poolerType clustermetadatapb.PoolerType, status clustermetadatapb.PoolerServingStatus) *poolerRecord {
	r, err := newPoolerRecord(newTestLogger(), &fakeTopoStore{}, newTestMultiPooler(poolerType, status))
	if err != nil {
		panic(err)
	}
	return r
}

func TestStateManager_SetState_PrimaryServing(t *testing.T) {
	comp := &testComponent{}
	r := newTestRecord(clustermetadatapb.PoolerType_REPLICA, clustermetadatapb.PoolerServingStatus_DISABLED)

	ssm := NewStateManager(newTestLogger(), r, selfLeaderConsensusStatus, comp)

	err := ssm.Mutate(newActionLockedCtx(t), func(s *servingStateMutation) {
		s.PostgresPrimary = true
		s.ServingStatus = clustermetadatapb.PoolerServingStatus_SERVING
	})
	require.NoError(t, err)

	// Component should receive the target state.
	assert.Equal(t, clustermetadatapb.PoolerType_PRIMARY, comp.lastType)
	assert.Equal(t, clustermetadatapb.PoolerServingStatus_SERVING, comp.lastStatus)
	assert.Equal(t, 1, comp.callCount)

	// Record should be updated.
	assert.Equal(t, clustermetadatapb.PoolerType_PRIMARY, r.Type())
	assert.Equal(t, clustermetadatapb.PoolerServingStatus_SERVING, r.ServingStatus())
}

func TestStateManager_SetState_NotServing(t *testing.T) {
	comp := &testComponent{}
	r := newTestRecord(clustermetadatapb.PoolerType_PRIMARY, clustermetadatapb.PoolerServingStatus_SERVING)

	ssm := NewStateManager(newTestLogger(), r, selfLeaderConsensusStatus, comp)

	err := ssm.Mutate(newActionLockedCtx(t), func(s *servingStateMutation) {
		s.PostgresPrimary = true
		s.ServingStatus = clustermetadatapb.PoolerServingStatus_DISABLED
	})
	require.NoError(t, err)

	assert.Equal(t, clustermetadatapb.PoolerType_PRIMARY, comp.lastType)
	assert.Equal(t, clustermetadatapb.PoolerServingStatus_DISABLED, comp.lastStatus)

	assert.Equal(t, clustermetadatapb.PoolerType_PRIMARY, r.Type())
	assert.Equal(t, clustermetadatapb.PoolerServingStatus_DISABLED, r.ServingStatus())
}

func TestStateManager_SetState_ComponentError(t *testing.T) {
	comp := &testComponent{err: errors.New("transition failed")}
	r := newTestRecord(clustermetadatapb.PoolerType_PRIMARY, clustermetadatapb.PoolerServingStatus_SERVING)

	ssm := NewStateManager(newTestLogger(), r, nilConsensusStatus, comp)

	err := ssm.Mutate(newActionLockedCtx(t), func(s *servingStateMutation) {
		s.PostgresPrimary = false
		s.ServingStatus = clustermetadatapb.PoolerServingStatus_SERVING
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "transition failed")

	// Record should NOT be updated on error.
	assert.Equal(t, clustermetadatapb.PoolerType_PRIMARY, r.Type())
	assert.Equal(t, clustermetadatapb.PoolerServingStatus_SERVING, r.ServingStatus())
}

func TestStateManager_DemotionFlow(t *testing.T) {
	// Simulate the demotion flow:
	// Step 1: (PRIMARY, SERVING) -> (PRIMARY, DISABLED)
	// Step 2: (PRIMARY, DISABLED) -> (REPLICA, SERVING)
	comp := &testComponent{}
	r := newTestRecord(clustermetadatapb.PoolerType_PRIMARY, clustermetadatapb.PoolerServingStatus_SERVING)

	ssm := NewStateManager(newTestLogger(), r, selfLeaderConsensusStatus, comp)

	// Step 1: Stop serving (still the writable leader, so routing stays PRIMARY)
	err := ssm.Mutate(newActionLockedCtx(t), func(s *servingStateMutation) {
		s.PostgresPrimary = true
		s.ServingStatus = clustermetadatapb.PoolerServingStatus_DISABLED
	})
	require.NoError(t, err)
	assert.Equal(t, clustermetadatapb.PoolerType_PRIMARY, r.Type())
	assert.Equal(t, clustermetadatapb.PoolerServingStatus_DISABLED, r.ServingStatus())
	assert.Equal(t, 1, comp.callCount)

	// Step 1b: Retry DISABLED (idempotent — should be a no-op)
	err = ssm.Mutate(newActionLockedCtx(t), func(s *servingStateMutation) {
		s.PostgresPrimary = true
		s.ServingStatus = clustermetadatapb.PoolerServingStatus_DISABLED
	})
	require.NoError(t, err)
	assert.Equal(t, clustermetadatapb.PoolerType_PRIMARY, r.Type())
	assert.Equal(t, clustermetadatapb.PoolerServingStatus_DISABLED, r.ServingStatus())
	assert.Equal(t, 1, comp.callCount) // no additional call

	// Step 2: Transition to replica serving (recovery -> not the writable leader)
	err = ssm.Mutate(newActionLockedCtx(t), func(s *servingStateMutation) {
		s.PostgresPrimary = false
		s.ServingStatus = clustermetadatapb.PoolerServingStatus_SERVING
	})
	require.NoError(t, err)
	assert.Equal(t, clustermetadatapb.PoolerType_REPLICA, r.Type())
	assert.Equal(t, clustermetadatapb.PoolerServingStatus_SERVING, r.ServingStatus())
	assert.Equal(t, 2, comp.callCount)
}

func TestStateManager_MultipleComponents(t *testing.T) {
	comp1 := &testComponent{}
	comp2 := &testComponent{}
	r := newTestRecord(clustermetadatapb.PoolerType_REPLICA, clustermetadatapb.PoolerServingStatus_DISABLED)

	ssm := NewStateManager(newTestLogger(), r, selfLeaderConsensusStatus, comp1, comp2)

	err := ssm.Mutate(newActionLockedCtx(t), func(s *servingStateMutation) {
		s.PostgresPrimary = true
		s.ServingStatus = clustermetadatapb.PoolerServingStatus_SERVING
	})
	require.NoError(t, err)

	// Both components should have been called.
	assert.Equal(t, 1, comp1.callCount)
	assert.Equal(t, 1, comp2.callCount)
	assert.Equal(t, clustermetadatapb.PoolerType_PRIMARY, comp1.lastType)
	assert.Equal(t, clustermetadatapb.PoolerType_PRIMARY, comp2.lastType)
}

func TestStateManager_MultipleComponents_OneError(t *testing.T) {
	comp1 := &testComponent{}
	comp2 := &testComponent{err: errors.New("comp2 failed")}
	r := newTestRecord(clustermetadatapb.PoolerType_PRIMARY, clustermetadatapb.PoolerServingStatus_SERVING)

	ssm := NewStateManager(newTestLogger(), r, nilConsensusStatus, comp1, comp2)

	err := ssm.Mutate(newActionLockedCtx(t), func(s *servingStateMutation) {
		s.PostgresPrimary = false
		s.ServingStatus = clustermetadatapb.PoolerServingStatus_SERVING
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "comp2 failed")

	// Record should NOT be updated when any component fails.
	assert.Equal(t, clustermetadatapb.PoolerType_PRIMARY, r.Type())
	assert.Equal(t, clustermetadatapb.PoolerServingStatus_SERVING, r.ServingStatus())
}

func TestStateManager_Register(t *testing.T) {
	comp1 := &testComponent{}
	comp2 := &testComponent{}
	r := newTestRecord(clustermetadatapb.PoolerType_REPLICA, clustermetadatapb.PoolerServingStatus_DISABLED)

	ssm := NewStateManager(newTestLogger(), r, selfLeaderConsensusStatus, comp1)

	// Register a second component after creation.
	ssm.Register(comp2)

	err := ssm.Mutate(newActionLockedCtx(t), func(s *servingStateMutation) {
		s.PostgresPrimary = true
		s.ServingStatus = clustermetadatapb.PoolerServingStatus_SERVING
	})
	require.NoError(t, err)

	assert.Equal(t, 1, comp1.callCount)
	assert.Equal(t, 1, comp2.callCount)
}

func TestStateManager_RegisterAndSync(t *testing.T) {
	comp1 := &testComponent{}
	r := newTestRecord(clustermetadatapb.PoolerType_PRIMARY, clustermetadatapb.PoolerServingStatus_SERVING)

	// Create manager with no components, then transition state.
	ssm := NewStateManager(newTestLogger(), r, selfLeaderConsensusStatus)
	err := ssm.Mutate(newActionLockedCtx(t), func(s *servingStateMutation) {
		s.PostgresPrimary = true
		s.ServingStatus = clustermetadatapb.PoolerServingStatus_SERVING
	})
	require.NoError(t, err)

	// Register a late component — it should immediately receive the current state.
	comp2 := &testComponent{}
	err = ssm.RegisterAndSync(context.Background(), comp2)
	require.NoError(t, err)

	assert.Equal(t, 1, comp2.callCount)
	assert.Equal(t, clustermetadatapb.PoolerType_PRIMARY, comp2.lastType)
	assert.Equal(t, clustermetadatapb.PoolerServingStatus_SERVING, comp2.lastStatus)

	// comp1 was never registered, so it shouldn't have been called.
	assert.Equal(t, 0, comp1.callCount)

	// A subsequent SetState should notify both the constructor components and the late one.
	ssm2 := NewStateManager(newTestLogger(),
		newTestRecord(clustermetadatapb.PoolerType_REPLICA, clustermetadatapb.PoolerServingStatus_DISABLED),
		nilConsensusStatus, comp1)
	comp3 := &testComponent{}
	err = ssm2.RegisterAndSync(context.Background(), comp3)
	require.NoError(t, err)
	assert.Equal(t, clustermetadatapb.PoolerType_REPLICA, comp3.lastType)
	assert.Equal(t, clustermetadatapb.PoolerServingStatus_DISABLED, comp3.lastStatus)
}

func TestStateManager_RegisterAndSync_Error(t *testing.T) {
	r := newTestRecord(clustermetadatapb.PoolerType_PRIMARY, clustermetadatapb.PoolerServingStatus_SERVING)
	ssm := NewStateManager(newTestLogger(), r, nilConsensusStatus)

	comp := &testComponent{err: errors.New("sync failed")}
	err := ssm.RegisterAndSync(context.Background(), comp)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "sync failed")
}

func TestStateManager_NoComponents(t *testing.T) {
	r := newTestRecord(clustermetadatapb.PoolerType_REPLICA, clustermetadatapb.PoolerServingStatus_DISABLED)

	ssm := NewStateManager(newTestLogger(), r, selfLeaderConsensusStatus)

	err := ssm.Mutate(newActionLockedCtx(t), func(s *servingStateMutation) {
		s.PostgresPrimary = true
		s.ServingStatus = clustermetadatapb.PoolerServingStatus_SERVING
	})
	require.NoError(t, err)

	// Record should still be updated.
	assert.Equal(t, clustermetadatapb.PoolerType_PRIMARY, r.Type())
	assert.Equal(t, clustermetadatapb.PoolerServingStatus_SERVING, r.ServingStatus())
}

func TestStateManager_HealthStreamerIntegration(t *testing.T) {
	// Verify that when healthStreamer is registered as a component,
	// SetState triggers a health stream broadcast with the correct state.
	logger := newTestLogger()
	serviceID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "test-pooler",
	}
	hs := newHealthStreamer(logger, serviceID, "tg1", "0")
	comp := &testComponent{}
	r := newTestRecord(clustermetadatapb.PoolerType_REPLICA, clustermetadatapb.PoolerServingStatus_DISABLED)

	ssm := NewStateManager(logger, r, selfLeaderConsensusStatus, comp, hs)

	// Subscribe to health stream before state change
	_, ch := hs.subscribe()

	err := ssm.Mutate(newActionLockedCtx(t), func(s *servingStateMutation) {
		s.PostgresPrimary = true
		s.ServingStatus = clustermetadatapb.PoolerServingStatus_SERVING
	})
	require.NoError(t, err)

	// testComponent should be notified
	assert.Equal(t, 1, comp.callCount)
	assert.Equal(t, clustermetadatapb.PoolerType_PRIMARY, comp.lastType)

	// healthStreamer should have broadcast to subscriber
	select {
	case received := <-ch:
		require.NotNil(t, received)
		assert.Equal(t, clustermetadatapb.PoolerServingStatus_SERVING, received.ServingStatus)
		assert.Equal(t, serviceID, received.PoolerID)
	default:
		t.Fatal("healthStreamer did not broadcast on SetState")
	}

	// Record should be updated
	assert.Equal(t, clustermetadatapb.PoolerType_PRIMARY, r.Type())
	assert.Equal(t, clustermetadatapb.PoolerServingStatus_SERVING, r.ServingStatus())
}

func TestStateManager_ParallelExecution(t *testing.T) {
	// Verify both components are invoked (they run in parallel via errgroup).
	comp1 := &slowComponent{}
	comp2 := &slowComponent{}
	r := newTestRecord(clustermetadatapb.PoolerType_REPLICA, clustermetadatapb.PoolerServingStatus_DISABLED)

	ssm := NewStateManager(newTestLogger(), r, selfLeaderConsensusStatus, comp1, comp2)

	err := ssm.Mutate(newActionLockedCtx(t), func(s *servingStateMutation) {
		s.PostgresPrimary = true
		s.ServingStatus = clustermetadatapb.PoolerServingStatus_SERVING
	})
	require.NoError(t, err)

	assert.True(t, comp1.called.Load())
	assert.True(t, comp2.called.Load())
}

func TestStateManager_SetState_RequiresActionLock(t *testing.T) {
	comp := &testComponent{}
	r := newTestRecord(clustermetadatapb.PoolerType_REPLICA, clustermetadatapb.PoolerServingStatus_DISABLED)
	ssm := NewStateManager(newTestLogger(), r, nilConsensusStatus, comp)

	// No action lock on the context: SetState must reject before doing anything.
	err := ssm.Mutate(t.Context(), func(s *servingStateMutation) {
		s.PostgresPrimary = true
		s.ServingStatus = clustermetadatapb.PoolerServingStatus_SERVING
	})
	require.Error(t, err)

	// Fail fast: no component was transitioned and the record is unchanged.
	assert.Equal(t, 0, comp.callCount, "components must not transition without the action lock")
	assert.Equal(t, clustermetadatapb.PoolerType_REPLICA, r.Type())
	assert.Equal(t, clustermetadatapb.PoolerServingStatus_DISABLED, r.ServingStatus())
}

// TestStateManager_HasDrift covers the physical-primary / serving drift detection
// that the postgres monitor uses to decide whether to reconcile (fixDrift). The
// role facts are held fixed as the active writable leader (self-leader consensus +
// PostgresPrimary=true) so this isolates the recovery-flag / serving-status inputs,
// matching the old determineRemedialAction primary-drift coverage that moved here.
func TestStateManager_HasDrift(t *testing.T) {
	// newFannedOut builds a manager whose last-fanned-out effective state is
	// (PRIMARY, serving): the active writable leader currently in `serving`.
	newFannedOut := func(t *testing.T, serving clustermetadatapb.PoolerServingStatus) *StateManager {
		r := newTestRecord(clustermetadatapb.PoolerType_PRIMARY, serving)
		ssm := NewStateManager(newTestLogger(), r, selfLeaderConsensusStatus)
		// Establish lastFannedOut = (PRIMARY, serving) as the active writable leader.
		require.NoError(t, ssm.Mutate(newActionLockedCtx(t), func(s *servingStateMutation) {
			s.PostgresPrimary = true
			s.ServingStatus = serving
		}))
		return ssm
	}

	t.Run("primary drift reconciles", func(t *testing.T) {
		// Last fanned out as the writable leader (PostgresPrimary true), but postgres
		// now reports recovery (postgresPrimary=false): the routing role would flip,
		// so this is drift.
		ssm := newFannedOut(t, clustermetadatapb.PoolerServingStatus_SERVING)
		assert.True(t, ssm.hasDrift(false /* postgresPrimary */))
	})

	t.Run("draining always reconciles", func(t *testing.T) {
		// DRAINING is a transient drain the monitor owns; hasDrift always reports it
		// so fixDrift completes DRAINING->SERVING once healthy and role-aligned.
		ssm := newFannedOut(t, clustermetadatapb.PoolerServingStatus_DRAINING)
		assert.True(t, ssm.hasDrift(true /* postgresPrimary */))
	})

	t.Run("disabled is left alone", func(t *testing.T) {
		// DISABLED is a deliberate non-serving state; with the recovery flag and role
		// unchanged there is no drift, so the monitor must not auto-reconcile it.
		ssm := newFannedOut(t, clustermetadatapb.PoolerServingStatus_DISABLED)
		assert.False(t, ssm.hasDrift(true /* postgresPrimary */))
	})

	t.Run("no drift is a no-op", func(t *testing.T) {
		// Recovery flag and serving status match what was last fanned out: no drift.
		ssm := newFannedOut(t, clustermetadatapb.PoolerServingStatus_SERVING)
		assert.False(t, ssm.hasDrift(true /* postgresPrimary */))
	})
}

// TestStateManager_FixDrift verifies fixDrift re-derives and re-applies the
// effective state: a recovery flip re-projects the record to REPLICA, and a
// DRAINING status is completed to SERVING.
func TestStateManager_FixDrift(t *testing.T) {
	t.Run("recovery flip demotes to replica", func(t *testing.T) {
		r := newTestRecord(clustermetadatapb.PoolerType_PRIMARY, clustermetadatapb.PoolerServingStatus_SERVING)
		ssm := NewStateManager(newTestLogger(), r, selfLeaderConsensusStatus)
		require.NoError(t, ssm.Mutate(newActionLockedCtx(t), func(s *servingStateMutation) {
			s.PostgresPrimary = true
			s.ServingStatus = clustermetadatapb.PoolerServingStatus_SERVING
		}))
		require.Equal(t, clustermetadatapb.PoolerType_PRIMARY, r.Type())

		require.NoError(t, ssm.fixDrift(newActionLockedCtx(t), false /* postgresPrimary */))
		assert.Equal(t, clustermetadatapb.PoolerType_REPLICA, r.Type())
		assert.Equal(t, clustermetadatapb.PoolerServingStatus_SERVING, r.ServingStatus())
	})

	t.Run("draining completes to serving", func(t *testing.T) {
		r := newTestRecord(clustermetadatapb.PoolerType_PRIMARY, clustermetadatapb.PoolerServingStatus_DRAINING)
		ssm := NewStateManager(newTestLogger(), r, selfLeaderConsensusStatus)

		require.NoError(t, ssm.fixDrift(newActionLockedCtx(t), true /* postgresPrimary */))
		assert.Equal(t, clustermetadatapb.PoolerServingStatus_SERVING, r.ServingStatus())
		assert.Equal(t, clustermetadatapb.PoolerType_PRIMARY, r.Type())
	})
}

// TestStateManager_RegisterAndSyncAfterDedupedRecoveryChange is a regression test
// for the cached recovery fact going stale across a deduped (early-return) Mutate.
// A postgresPrimary change that does not flip the routing role — because this
// pooler is not yet the active consensus leader, so it stays REPLICA either way —
// must still refresh ssm.postgresPrimary. Otherwise a component registered later,
// once consensus does name this pooler the active leader, would derive REPLICA
// from the stale recovery flag instead of the correct PRIMARY.
func TestStateManager_RegisterAndSyncAfterDedupedRecoveryChange(t *testing.T) {
	r := newTestRecord(clustermetadatapb.PoolerType_REPLICA, clustermetadatapb.PoolerServingStatus_SERVING)

	// A consensus snapshot the test flips from "not the active leader" to "active
	// leader". While not the leader the routing role is REPLICA regardless of
	// recovery mode; once the leader, recovery mode decides PRIMARY vs REPLICA.
	var activeLeader atomic.Bool
	consensusStatus := func() *clustermetadatapb.ConsensusStatus {
		if activeLeader.Load() {
			return selfLeaderConsensusStatus()
		}
		return nilConsensusStatus()
	}
	ssm := NewStateManager(newTestLogger(), r, consensusStatus)

	// Baseline fan-out: REPLICA/SERVING with postgres in recovery.
	require.NoError(t, ssm.Mutate(newActionLockedCtx(t), func(s *servingStateMutation) {
		s.PostgresPrimary = false
		s.ServingStatus = clustermetadatapb.PoolerServingStatus_SERVING
	}))

	// Postgres leaves recovery, but this pooler is not yet the active leader, so
	// the derived routing role stays REPLICA and Mutate early-returns without a
	// fan-out. The cached recovery fact must still advance to true.
	require.NoError(t, ssm.Mutate(newActionLockedCtx(t), func(s *servingStateMutation) {
		s.PostgresPrimary = true
		s.ServingStatus = clustermetadatapb.PoolerServingStatus_SERVING
	}))

	// Consensus now names this pooler the active leader.
	activeLeader.Store(true)

	// A component registered now must see PRIMARY: postgres is out of recovery
	// (cached true) AND this pooler is the active leader. A stale cached false
	// would derive REPLICA.
	comp := &testComponent{}
	require.NoError(t, ssm.RegisterAndSync(t.Context(), comp))
	assert.Equal(t, servingstate.RoutingRolePrimary, comp.lastRole,
		"late-registered component must derive PRIMARY from the refreshed recovery cache")
}

// TestStateManager_Recalc verifies Recalc re-derives the effective state from
// the live consensus snapshot with no explicit recovery/serving change: when a
// revocation makes this pooler no longer the active leader, Recalc flips the
// fanned routing role PRIMARY->REPLICA and clears the self-leadership
// observation immediately, rather than leaving it stale until the next monitor
// drift tick. This is the behavior the Recruit path relies on after
// AcceptRevocation persists.
func TestStateManager_Recalc(t *testing.T) {
	comp := &testComponent{}
	r := newTestRecord(clustermetadatapb.PoolerType_PRIMARY, clustermetadatapb.PoolerServingStatus_SERVING)

	// A consensus snapshot the test flips from "self is the active leader" to
	// "self's term is revoked", simulating a revocation arriving between the
	// initial promotion and the Recalc. selfLeaderConsensusStatus names self at
	// term 1; revoking below term 2 makes IsActiveLeader false.
	var revoked atomic.Bool
	consensusStatus := func() *clustermetadatapb.ConsensusStatus {
		cs := selfLeaderConsensusStatus()
		if revoked.Load() {
			cs.TermRevocation = &clustermetadatapb.TermRevocation{RevokedBelowTerm: 2}
		}
		return cs
	}

	ssm := NewStateManager(newTestLogger(), r, consensusStatus, comp)

	// Baseline: the writable leader — PRIMARY + self-naming observation.
	require.NoError(t, ssm.Mutate(newActionLockedCtx(t), func(s *servingStateMutation) {
		s.PostgresPrimary = true
		s.ServingStatus = clustermetadatapb.PoolerServingStatus_SERVING
	}))
	require.Equal(t, servingstate.RoutingRolePrimary, comp.lastRole)
	require.Equal(t, clustermetadatapb.PoolerType_PRIMARY, r.Type())
	require.NotNil(t, r.SelfLeadership())
	callsAfterMutate := comp.callCount

	// A revocation arrives: this pooler's term is revoked, so it is no longer the
	// active leader even though postgres is still out of recovery. Recalc, with no
	// recovery/serving mutation, must re-fan the flipped routing role.
	revoked.Store(true)
	require.NoError(t, ssm.Recalc(newActionLockedCtx(t)))

	assert.Greater(t, comp.callCount, callsAfterMutate,
		"Recalc should re-fan on the consensus-only change")
	assert.Equal(t, servingstate.RoutingRoleReplica, comp.lastRole,
		"revoked leader routes as REPLICA")
	assert.Equal(t, clustermetadatapb.PoolerType_REPLICA, r.Type())
	assert.Nil(t, r.SelfLeadership(),
		"revoked leader must clear its self-leadership observation immediately")
}

// TestStateManager_Recalc_NoChangeIsNoOp verifies Recalc does not re-fan when the
// derived state is unchanged (it dedups like any other Mutate).
func TestStateManager_Recalc_NoChangeIsNoOp(t *testing.T) {
	comp := &testComponent{}
	r := newTestRecord(clustermetadatapb.PoolerType_PRIMARY, clustermetadatapb.PoolerServingStatus_SERVING)
	ssm := NewStateManager(newTestLogger(), r, selfLeaderConsensusStatus, comp)

	require.NoError(t, ssm.Mutate(newActionLockedCtx(t), func(s *servingStateMutation) {
		s.PostgresPrimary = true
		s.ServingStatus = clustermetadatapb.PoolerServingStatus_SERVING
	}))
	calls := comp.callCount

	require.NoError(t, ssm.Recalc(newActionLockedCtx(t)))
	assert.Equal(t, calls, comp.callCount, "Recalc with no derived change must not re-fan")
}

// nilConsensusStatus is a StateManager consensus-snapshot injector for tests that
// don't exercise the derived routing role (nil -> not the active leader -> REPLICA).
func nilConsensusStatus() *clustermetadatapb.ConsensusStatus { return nil }

// selfLeaderConsensusStatus is a StateManager consensus-snapshot injector in
// which testPoolerID is the active committed leader (commonconsensus.IsActiveLeader
// is true): the committed rule names self and is not revoked or superseded.
// Combined with s.PostgresPrimary = true it yields routing role PRIMARY.
func selfLeaderConsensusStatus() *clustermetadatapb.ConsensusStatus {
	return &clustermetadatapb.ConsensusStatus{
		Id: testPoolerID,
		CurrentPosition: &clustermetadatapb.PoolerPosition{
			Rule: &clustermetadatapb.ShardRule{
				RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 1},
				LeaderId:   testPoolerID,
			},
		},
	}
}

// TestDeriveRoutingRole pins the write-safety boundary: routing role is PRIMARY
// only when postgres is out of recovery AND the pooler is the active committed
// consensus leader (commonconsensus.IsActiveLeader). Every other combination is
// REPLICA — most importantly the pg_promote()->WAL-commit window, where postgres
// is already out of recovery but the pooler's committed rule does not yet name it,
// which must stay REPLICA so no write is admitted on a not-yet-committed term.
func TestDeriveRoutingRole(t *testing.T) {
	otherPoolerID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      testPoolerID.GetCell(),
		Name:      "other-pooler",
	}

	tests := []struct {
		name            string
		postgresPrimary bool
		cs              *clustermetadatapb.ConsensusStatus
		want            servingstate.RoutingRole
	}{
		{
			name:            "out of recovery and active leader is primary",
			postgresPrimary: true,
			cs:              selfLeaderConsensusStatus(),
			want:            servingstate.RoutingRolePrimary,
		},
		{
			name:            "in recovery is replica even as active leader",
			postgresPrimary: false,
			cs:              selfLeaderConsensusStatus(),
			want:            servingstate.RoutingRoleReplica,
		},
		{
			// pg_promote() has flipped postgres out of recovery, but the new rule has
			// not committed to this pooler's WAL yet, so its committed rule still names
			// the previous leader. Admitting writes here would land them on a
			// not-yet-committed term — this is the window the routing role closes.
			name:            "primary but committed rule names another is replica",
			postgresPrimary: true,
			cs: &clustermetadatapb.ConsensusStatus{
				Id: testPoolerID,
				CurrentPosition: &clustermetadatapb.PoolerPosition{
					Rule: &clustermetadatapb.ShardRule{
						RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 1},
						LeaderId:   otherPoolerID,
					},
				},
			},
			want: servingstate.RoutingRoleReplica,
		},
		{
			name:            "primary with no consensus snapshot is replica",
			postgresPrimary: true,
			cs:              nil,
			want:            servingstate.RoutingRoleReplica,
		},
		{
			// Self's committed rule (term 1) is revoked below term 2: the pooler was
			// recruited into a higher term's reconfiguration, so the rule no longer
			// carries write authority.
			name:            "primary but committed rule revoked is replica",
			postgresPrimary: true,
			cs: func() *clustermetadatapb.ConsensusStatus {
				cs := selfLeaderConsensusStatus()
				cs.TermRevocation = &clustermetadatapb.TermRevocation{RevokedBelowTerm: 2}
				return cs
			}(),
			want: servingstate.RoutingRoleReplica,
		},
		{
			// Self's committed rule (term 1) names self, but a higher rule (term 2)
			// naming another pooler is known via ReplicationPrimary — self has been
			// superseded, so it is no longer the active leader.
			name:            "primary but superseded by higher known rule is replica",
			postgresPrimary: true,
			cs: func() *clustermetadatapb.ConsensusStatus {
				cs := selfLeaderConsensusStatus()
				cs.ReplicationPrimary = &clustermetadatapb.ReplicationPrimary{
					Rule: &clustermetadatapb.ShardRule{
						RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 2},
						LeaderId:   otherPoolerID,
					},
				}
				return cs
			}(),
			want: servingstate.RoutingRoleReplica,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, deriveRoutingRole(tt.postgresPrimary, tt.cs))
		})
	}
}
