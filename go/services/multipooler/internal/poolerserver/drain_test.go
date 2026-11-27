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

package poolerserver

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/mterrors"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	"github.com/multigres/multigres/go/services/multipooler/internal/servingstate"
)

// The bounds of a timed-out drain's end, shortened for the tests.
func init() {
	drainKillTimeout = 50 * time.Millisecond
	drainSettleTimeout = 200 * time.Millisecond
}

func disabled(role servingstate.RoutingRole) servingstate.State {
	return servingstate.State{Routing: servingstate.RoutingState{Role: role}, ServingStatus: clustermetadatapb.PoolerServingStatus_DISABLED}
}

func serving(role servingstate.RoutingRole) servingstate.State {
	return servingstate.State{Routing: servingstate.RoutingState{Role: role}, ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING}
}

func requireDrainIncomplete(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrDrainIncomplete)
	assert.Equal(t, mtrpcpb.Code_UNAVAILABLE, mterrors.Code(err), "%v", err)
}

// A drain that runs out of time with a transaction and a single query still
// lent seals the pools, kills them, and once nothing is left unseals the
// pools: the transition reports drained.
func TestDrain_TimeoutEndsTheWorkLeft(t *testing.T) {
	mock := newDrainMockPoolManager()
	mock.onKill = endLent
	s := newTestPoolerWithDrain(mock)
	s.gracePeriod = 50 * time.Millisecond
	require.NoError(t, s.OnStateChange(t.Context(), serving(servingstate.RoutingRolePrimary)))
	mock.reservedAdd(1)
	mock.regularAdd(1)

	require.NoError(t, s.OnStateChange(t.Context(), disabled(servingstate.RoutingRolePrimary)))
	assert.Equal(t, 1, mock.killCallCount())
	assert.False(t, mock.isSealed())
	assert.Equal(t, 1, mock.sealCalls)
	assert.Equal(t, 1, mock.unsealCalls)
	assert.False(t, s.IsServing())
}

// A kill that is not confirmed fails the drain: the transition still
// completes (the pooler is not serving) and reports no error, and the pools
// stay sealed until the pooler serves again.
func TestDrain_UnconfirmedKillFails(t *testing.T) {
	reader := setupPoolerTelemetry(t)
	mock := newDrainMockPoolManager()
	mock.killErr = errors.New("backend 42 did not exit within 50ms")
	s := newTestPoolerWithDrain(mock)
	s.gracePeriod = 50 * time.Millisecond
	require.NoError(t, s.OnStateChange(t.Context(), serving(servingstate.RoutingRolePrimary)))
	mock.regularAdd(1)

	require.NoError(t, s.OnStateChange(t.Context(), disabled(servingstate.RoutingRolePrimary)))
	assert.False(t, s.IsServing(), "the transition completes")
	assert.True(t, mock.isSealed(), "a failed drain keeps the pools sealed")
	assert.Equal(t, int64(1), readCounterByAttrs(t, reader, "mg.pooler.drain.outcome", primaryDrainAttrs(drainOutcomeFailed)),
		"the failed drain is counted")

	mock.regularAdd(-1)
	require.NoError(t, s.OnStateChange(t.Context(), serving(servingstate.RoutingRolePrimary)))
	assert.False(t, mock.isSealed(), "serving again unseals the pools")
}

// With kills off (an unmanaged pooler), a drain that runs out of time seals
// the pools and waits; it fails if work is still lent at its bound, and
// succeeds if the work ends within it.
func TestDrain_NoKill(t *testing.T) {
	t.Run("work left at the bound fails", func(t *testing.T) {
		mock := newDrainMockPoolManager()
		s := newTestPoolerWithDrain(mock)
		s.killOnDrainTimeout = false
		s.gracePeriod = 50 * time.Millisecond
		require.NoError(t, s.OnStateChange(t.Context(), serving(servingstate.RoutingRolePrimary)))
		mock.regularAdd(1)

		require.NoError(t, s.OnStateChange(t.Context(), disabled(servingstate.RoutingRolePrimary)))
		assert.Zero(t, mock.killCallCount(), "an unmanaged pooler kills nothing")
		assert.True(t, mock.isSealed(), "work left at the bound fails the drain, which keeps the pools sealed")
		requireDrainIncomplete(t, s.EnsureDrained(t.Context()))
	})
	t.Run("work that ends within the bound succeeds", func(t *testing.T) {
		mock := newDrainMockPoolManager()
		s := newTestPoolerWithDrain(mock)
		s.killOnDrainTimeout = false
		s.gracePeriod = 50 * time.Millisecond
		require.NoError(t, s.OnStateChange(t.Context(), serving(servingstate.RoutingRolePrimary)))
		mock.regularAdd(1)
		go func() {
			time.Sleep(100 * time.Millisecond) // past the timeout, within the bound
			mock.regularAdd(-1)
		}()

		require.NoError(t, s.OnStateChange(t.Context(), disabled(servingstate.RoutingRolePrimary)))
		assert.Zero(t, mock.killCallCount())
		assert.False(t, mock.isSealed())
	})
}

// A fence drain that fails leaves the gate draining with no drain running:
// it refuses all new work, the pools stay sealed, and the fence reader keeps
// re-reading. The next read that finds the fence drains again and takes the
// failed drain's seal hold over, releasing it once it succeeds.
func TestFenceGate_FailedDrainStaysDrainingUntilADrainSucceeds(t *testing.T) {
	mock := newDrainMockPoolManager()
	mock.killErr = errors.New("backend 42 did not exit within 50ms")
	s := newFencedTestPooler(t, mock, servingstate.RoutingRolePrimary)
	applyFence(t, s, false)
	mock.reservedAdd(1)

	requireDrainIncomplete(t, s.ApplyFence(t.Context(), FenceRead{Epoch: s.FenceEpoch(), Fenced: true}, 50*time.Millisecond))
	s.mu.Lock()
	assert.Equal(t, fenceGateDraining, s.fence.state)
	assert.Nil(t, s.fence.drain)
	s.mu.Unlock()
	requireMTF02(t, admit(s.StartRequest(nil, RequestSingleQuery)), "a failed drain refuses everything")
	_, closed, _ := s.FenceReadState()
	assert.True(t, closed, "the reader keeps re-reading")
	assert.True(t, mock.isSealed())

	mock.mu.Lock()
	mock.killErr = nil
	mock.onKill = endLent
	mock.mu.Unlock()
	require.NoError(t, s.ApplyFence(t.Context(), FenceRead{Epoch: s.FenceEpoch(), Fenced: true}, 50*time.Millisecond))
	s.mu.Lock()
	assert.Equal(t, fenceGateClosed, s.fence.state)
	assert.Nil(t, s.fence.failedSeal)
	assert.Zero(t, s.sealHolds)
	s.mu.Unlock()
	assert.False(t, mock.isSealed(), "the drain that succeeded released the hold it took over")
}

// Lifting the fence on a gate whose drain failed releases the failed drain's
// seal hold.
func TestFenceGate_UnfenceAfterAFailedDrainUnseals(t *testing.T) {
	mock := newDrainMockPoolManager()
	mock.killErr = errors.New("backend 42 did not exit within 50ms")
	s := newFencedTestPooler(t, mock, servingstate.RoutingRolePrimary)
	applyFence(t, s, false)
	mock.reservedAdd(1)
	defer mock.reservedAdd(-1)

	requireDrainIncomplete(t, s.ApplyFence(t.Context(), FenceRead{Epoch: s.FenceEpoch(), Fenced: true}, 50*time.Millisecond))
	require.True(t, mock.isSealed())
	applyFence(t, s, false)
	assert.False(t, mock.isSealed())
	require.NoError(t, admit(s.StartRequest(nil, RequestNewReservation)))
}

// A read that joins a drain which then fails gets the drain's error.
func TestFenceGate_JoinerGetsTheFailedDrainsError(t *testing.T) {
	mock := newDrainMockPoolManager()
	mock.killErr = errors.New("backend 42 did not exit within 50ms")
	s := newFencedTestPooler(t, mock, servingstate.RoutingRolePrimary)
	applyFence(t, s, false)
	mock.reservedAdd(1)
	defer mock.reservedAdd(-1)

	running := make(chan error, 1)
	go func() {
		running <- s.ApplyFence(context.Background(), FenceRead{Epoch: s.FenceEpoch(), Fenced: true}, 100*time.Millisecond)
	}()
	waitForFenceDrainPhase(t, s, drainReserved)
	joined := s.ApplyFence(t.Context(), FenceRead{Epoch: s.FenceEpoch(), Fenced: true}, time.Second)
	requireDrainIncomplete(t, joined)
	requireDrainIncomplete(t, requireReturns(t, running, "the failing drain"))
}

// Lifting the fence while the drain is ending the work left cancels it: it
// stops, releases its seal hold, and kills nothing more.
func TestFenceGate_UnfenceWhileEndingTheDrainCancelsIt(t *testing.T) {
	mock := newDrainMockPoolManager() // kills succeed, but the work never ends
	s := newFencedTestPooler(t, mock, servingstate.RoutingRolePrimary)
	applyFence(t, s, false)
	mock.reservedAdd(1)
	defer mock.reservedAdd(-1)

	running := make(chan error, 1)
	go func() {
		running <- s.ApplyFence(context.Background(), FenceRead{Epoch: s.FenceEpoch(), Fenced: true}, 20*time.Millisecond)
	}()
	require.Eventually(t, func() bool { return mock.killCallCount() == 1 }, time.Second, 5*time.Millisecond,
		"the drain is ending the work left")
	applyFence(t, s, false)

	err := requireReturns(t, running, "the cancelled drain")
	assert.Equal(t, mtrpcpb.Code_ABORTED, mterrors.Code(err), "%v", err)
	assert.False(t, mock.isSealed())
	s.mu.Lock()
	assert.Zero(t, s.sealHolds)
	s.mu.Unlock()
	assert.Equal(t, 1, mock.killCallCount())
}

// The pools stay sealed while any drain holds the seal.
func TestSealHolds(t *testing.T) {
	mock := newDrainMockPoolManager()
	s := newTestPoolerWithDrain(mock)
	var fence, notServing bool
	s.holdSeal(&fence, errors.New("fenced"))
	s.holdSeal(&notServing, errors.New("not serving"))
	s.holdSeal(&fence, errors.New("fenced"))
	assert.Equal(t, 1, mock.sealCalls, "the pools are sealed once")
	s.releaseSeal(&fence)
	assert.True(t, mock.isSealed(), "another drain still holds the seal")
	s.releaseSeal(&fence)
	s.releaseSeal(&notServing)
	assert.False(t, mock.isSealed())
	assert.Equal(t, 1, mock.unsealCalls)
}

// A sealed pool refuses requests with the error of the drain that sealed it:
// MTF02 for a fence, which the gateway retries once the fence lifts, and
// MTF01 for a not-serving drain, which it buffers for the new primary. Both
// are retried only because the request ran nothing.
func TestDrain_SealsWithTheDrainsError(t *testing.T) {
	t.Run("fence", func(t *testing.T) {
		mock := newDrainMockPoolManager()
		mock.onKill = endLent
		s := newFencedTestPooler(t, mock, servingstate.RoutingRolePrimary)
		applyFence(t, s, false)
		mock.reservedAdd(1)
		require.NoError(t, s.ApplyFence(t.Context(), FenceRead{Epoch: s.FenceEpoch(), Fenced: true}, 20*time.Millisecond))
		assert.True(t, mterrors.IsErrorCode(mock.sealErr, mterrors.MTF02.ID), "got %v", mock.sealErr)
	})
	t.Run("not serving", func(t *testing.T) {
		mock := newDrainMockPoolManager()
		mock.onKill = endLent
		s := newTestPoolerWithDrain(mock)
		s.gracePeriod = 20 * time.Millisecond
		require.NoError(t, s.OnStateChange(t.Context(), serving(servingstate.RoutingRolePrimary)))
		mock.regularAdd(1)
		require.NoError(t, s.OnStateChange(t.Context(), disabled(servingstate.RoutingRolePrimary)))
		assert.True(t, mterrors.IsErrorCode(mock.sealErr, mterrors.MTF01.ID), "got %v", mock.sealErr)
	})
}

// EnsureDrained returns at once for a pooler whose drain succeeded, or that
// serves. After a drain that could not end its work, it runs the drain again:
// it fails while the work is still running, and succeeds, unsealing the
// pools, once it has ended.
func TestEnsureDrained(t *testing.T) {
	mock := newDrainMockPoolManager()
	s := newTestPoolerWithDrain(mock)
	s.gracePeriod = 20 * time.Millisecond
	require.NoError(t, s.OnStateChange(t.Context(), serving(servingstate.RoutingRolePrimary)))
	require.NoError(t, s.EnsureDrained(t.Context()), "a serving pooler")

	require.NoError(t, s.OnStateChange(t.Context(), disabled(servingstate.RoutingRolePrimary)))
	require.NoError(t, s.EnsureDrained(t.Context()), "a drain that succeeded")

	require.NoError(t, s.OnStateChange(t.Context(), serving(servingstate.RoutingRolePrimary)))
	mock.killErr = errors.New("backend 42 did not exit within 50ms")
	mock.regularAdd(1)
	require.NoError(t, s.OnStateChange(t.Context(), disabled(servingstate.RoutingRolePrimary)))
	require.True(t, mock.isSealed(), "precondition: the drain failed")

	requireDrainIncomplete(t, s.EnsureDrained(t.Context()))
	assert.True(t, mock.isSealed(), "still running: the pools stay sealed")
	requireMTF01(t, admit(s.StartRequest(nil, RequestSingleQuery)))

	mock.mu.Lock()
	mock.killErr = nil
	mock.mu.Unlock()
	mock.regularAdd(-1)
	require.NoError(t, s.EnsureDrained(t.Context()))
	assert.False(t, mock.isSealed(), "the drain ended the work: the pools unseal")
	assert.False(t, s.IsServing())
}

// A not-serving drain whose context ends first did not drain the pooler,
// though the transition completes and it holds no seal: EnsureDrained drains
// again rather than report the pooler drained.
func TestEnsureDrained_AfterACancelledDrain(t *testing.T) {
	mock := newDrainMockPoolManager()
	s := newTestPoolerWithDrain(mock)
	s.gracePeriod = 5 * time.Second
	require.NoError(t, s.OnStateChange(t.Context(), serving(servingstate.RoutingRolePrimary)))
	mock.reservedAdd(1) // a transaction still open

	ctx, cancel := context.WithCancel(t.Context())
	time.AfterFunc(50*time.Millisecond, cancel)
	require.NoError(t, s.OnStateChange(ctx, disabled(servingstate.RoutingRolePrimary)))
	require.False(t, s.IsServing(), "the transition completes")
	require.False(t, mock.isSealed(), "a cancelled drain holds no seal")

	s.gracePeriod = 20 * time.Millisecond
	mock.mu.Lock()
	mock.killErr = errors.New("backend 42 did not exit within 50ms")
	mock.mu.Unlock()
	requireDrainIncomplete(t, s.EnsureDrained(t.Context()))

	mock.mu.Lock()
	mock.killErr = nil
	mock.onKill = endLent
	mock.mu.Unlock()
	require.NoError(t, s.EnsureDrained(t.Context()))
	require.NoError(t, s.EnsureDrained(t.Context()), "drained: nothing to do")
	assert.Equal(t, 2, mock.killCallCount(), "the cancelled drain killed nothing; each EnsureDrained drain did")
}
