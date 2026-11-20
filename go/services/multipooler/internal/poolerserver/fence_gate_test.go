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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/shardingcatalog"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	"github.com/multigres/multigres/go/services/multipooler/internal/servingstate"
)

// requireMTF02 asserts that err is the MTF02 (fenced) signal.
func requireMTF02(t *testing.T, err error, msgAndArgs ...any) {
	t.Helper()
	require.Error(t, err, msgAndArgs...)
	assert.True(t, mterrors.IsErrorCode(err, mterrors.MTF02.ID), "expected MTF02 error, got: %v", err)
}

func servingState(role servingstate.RoutingRole) servingstate.State {
	return servingstate.State{Routing: servingstate.RoutingState{Role: role}, ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING}
}

func disabledState(role servingstate.RoutingRole) servingstate.State {
	return servingstate.State{Routing: servingstate.RoutingState{Role: role}, ServingStatus: clustermetadatapb.PoolerServingStatus_DISABLED}
}

// newFencedTestPooler returns a serving pooler with role and an armed fence
// gate, unread.
func newFencedTestPooler(t *testing.T, mock *drainMockPoolManager, role servingstate.RoutingRole) *QueryPoolerServer {
	t.Helper()
	s := newTestPoolerWithDrain(mock)
	s.fence.changed = make(chan struct{}, 1)
	s.EnableFenceGate()
	require.NoError(t, s.OnStateChange(t.Context(), servingState(role)))
	return s
}

// applyFence applies a fence read taken at the current epoch.
func applyFence(t *testing.T, s *QueryPoolerServer, fenced bool) {
	t.Helper()
	require.NoError(t, s.ApplyFence(t.Context(), FenceRead{Epoch: s.FenceEpoch(), Fenced: fenced}, time.Second))
}

// TestStartRequest_FenceMatrix checks every request kind against every fence
// gate state, on a primary and on a replica, with and without a not-serving
// drain under way.
func TestStartRequest_FenceMatrix(t *testing.T) {
	const (
		allow = "allow"
		mtf01 = "MTF01"
		mtf02 = "MTF02"
	)
	type gateState struct {
		gate  fenceGateState
		phase drainPhase
	}
	gates := map[string]gateState{
		"open":                 {fenceGateOpen, drainNone},
		"unread":               {fenceGateUnread, drainNone},
		"fencing, stage 1":     {fenceGateClosed, drainReserved},
		"fencing, stage 2":     {fenceGateClosed, drainRegular},
		"fenced":               {fenceGateClosed, drainNone},
		"off (nothing fenced)": {fenceGateOff, drainNone},
	}
	// want[gate][kind] on a primary that is serving and not draining.
	primary := map[string]map[RequestKind]string{
		"open":                 {RequestExistingReserved: allow, RequestSingleQuery: allow, RequestNewReservation: allow},
		"unread":               {RequestExistingReserved: allow, RequestSingleQuery: mtf02, RequestNewReservation: mtf02},
		"fencing, stage 1":     {RequestExistingReserved: allow, RequestSingleQuery: allow, RequestNewReservation: mtf02},
		"fencing, stage 2":     {RequestExistingReserved: allow, RequestSingleQuery: mtf02, RequestNewReservation: mtf02},
		"fenced":               {RequestExistingReserved: allow, RequestSingleQuery: mtf02, RequestNewReservation: mtf02},
		"off (nothing fenced)": {RequestExistingReserved: allow, RequestSingleQuery: allow, RequestNewReservation: allow},
	}
	kinds := map[RequestKind]string{RequestExistingReserved: "existing reserved", RequestSingleQuery: "single query", RequestNewReservation: "new reservation"}
	// The not-serving drain decides first: what it refuses is MTF01 whatever
	// the gate, and what it admits goes on to the gate.
	failoverPhases := map[string]struct {
		phase   drainPhase
		refuses map[RequestKind]bool
	}{
		"not draining":               {drainNone, nil},
		"not-serving drain, stage 1": {drainReserved, map[RequestKind]bool{RequestNewReservation: true}},
		"not-serving drain, stage 2": {drainRegular, map[RequestKind]bool{RequestSingleQuery: true, RequestNewReservation: true}},
	}

	for gateName, g := range gates {
		for fpName, fp := range failoverPhases {
			for _, role := range []servingstate.RoutingRole{servingstate.RoutingRolePrimary, servingstate.RoutingRoleReplica} {
				for kind, kindName := range kinds {
					t.Run(fmt.Sprintf("%s/%s/%s/%s", role, gateName, fpName, kindName), func(t *testing.T) {
						s := newStartRequestTestServer()
						s.servingStatus = clustermetadatapb.PoolerServingStatus_SERVING
						s.routingRole = role
						s.fence.state, s.fence.drainPhase = g.gate, g.phase
						s.drainPhase = fp.phase

						want := allow
						switch {
						case kind == RequestExistingReserved:
						case fp.refuses[kind]:
							want = mtf01
						case role == servingstate.RoutingRolePrimary:
							want = primary[gateName][kind]
						}
						err := admit(s.StartRequest(nil, kind))
						switch want {
						case allow:
							require.NoError(t, err)
						case mtf01:
							requireMTF01(t, err)
						case mtf02:
							requireMTF02(t, err)
						}
					})
				}
			}
		}
	}
}

// A fresh primary refuses new work until its fences are read, and admits it
// once a read finds none; a read that finds a fence closes it.
func TestFenceGate_FailsClosedUntilRead(t *testing.T) {
	s := newFencedTestPooler(t, newDrainMockPoolManager(), servingstate.RoutingRolePrimary)
	requireMTF02(t, admit(s.StartRequest(nil, RequestSingleQuery)))
	requireMTF02(t, admit(s.StartRequest(nil, RequestNewReservation)))
	unread, closed := s.FenceReadState()
	assert.True(t, unread)
	assert.False(t, closed)

	applyFence(t, s, false)
	require.NoError(t, admit(s.StartRequest(nil, RequestSingleQuery)))
	require.NoError(t, admit(s.StartRequest(nil, RequestNewReservation)))
	unread, closed = s.FenceReadState()
	assert.False(t, unread)
	assert.False(t, closed)

	applyFence(t, s, true)
	requireMTF02(t, admit(s.StartRequest(nil, RequestSingleQuery)))
	require.NoError(t, admit(s.StartRequest(nil, RequestExistingReserved)), "an existing reserved connection is always admitted")
	_, closed = s.FenceReadState()
	assert.True(t, closed)
	assert.True(t, s.IsServing(), "a fenced pooler stays serving")

	// Applying the same answer again is a no-op; an unfence reopens it.
	applyFence(t, s, true)
	requireMTF02(t, admit(s.StartRequest(nil, RequestSingleQuery)))
	applyFence(t, s, false)
	require.NoError(t, admit(s.StartRequest(nil, RequestNewReservation)))
}

// Becoming primary resets the gate to unread, and refuses a fence read taken
// before the promotion. A replica is never gated.
func TestFenceGate_PromotionResetsTheGate(t *testing.T) {
	s := newFencedTestPooler(t, newDrainMockPoolManager(), servingstate.RoutingRoleReplica)
	require.NoError(t, admit(s.StartRequest(nil, RequestSingleQuery)), "a replica is not fenced, read or not")
	unread, closed := s.FenceReadState()
	assert.False(t, unread, "a replica needs no read")
	assert.False(t, closed)
	applyFence(t, s, true)
	require.NoError(t, admit(s.StartRequest(nil, RequestNewReservation)), "a replica the read finds fenced is not gated")

	stale := s.FenceEpoch()
	drainSignal(s)
	require.NoError(t, s.OnStateChange(t.Context(), servingState(servingstate.RoutingRolePrimary)))
	select {
	case <-s.FenceGateChanged():
	default:
		t.Fatal("becoming primary did not ask for a fence read")
	}
	requireMTF02(t, admit(s.StartRequest(nil, RequestSingleQuery)), "a new primary is unread")
	err := s.ApplyFence(t.Context(), FenceRead{Epoch: stale, Fenced: false}, time.Second)
	assert.Equal(t, mtrpcpb.Code_ABORTED, mterrors.Code(err), "a read from before the promotion is refused")
	requireMTF02(t, admit(s.StartRequest(nil, RequestSingleQuery)))

	applyFence(t, s, false)
	require.NoError(t, admit(s.StartRequest(nil, RequestSingleQuery)))

	// Staying primary (a not-serving round trip) keeps the gate as it is.
	require.NoError(t, s.OnStateChange(t.Context(), disabledState(servingstate.RoutingRolePrimary)))
	require.NoError(t, s.OnStateChange(t.Context(), servingState(servingstate.RoutingRolePrimary)))
	require.NoError(t, admit(s.StartRequest(nil, RequestSingleQuery)))
}

// drainSignal empties the gate-changed channel.
func drainSignal(s *QueryPoolerServer) {
	select {
	case <-s.fence.changed:
	default:
	}
}

// A gate that is off (minigres) never refuses and never asks for reads.
func TestFenceGate_Off(t *testing.T) {
	s := newTestPoolerWithDrain(newDrainMockPoolManager())
	require.NoError(t, s.OnStateChange(t.Context(), servingState(servingstate.RoutingRolePrimary)))
	require.NoError(t, admit(s.StartRequest(nil, RequestNewReservation)))
	require.NoError(t, s.ApplyFence(t.Context(), FenceRead{Epoch: 0, Fenced: true}, time.Second))
	require.NoError(t, admit(s.StartRequest(nil, RequestNewReservation)))
	unread, closed := s.FenceReadState()
	assert.False(t, unread)
	assert.False(t, closed)
}

// waitForFenceDrainPhase blocks until the fence drain reaches phase.
func waitForFenceDrainPhase(t *testing.T, s *QueryPoolerServer, phase drainPhase) {
	t.Helper()
	require.Eventually(t, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.fence.drainPhase == phase
	}, time.Second, 5*time.Millisecond, "fence drain did not reach phase %d", phase)
}

// The fence drain lets an open transaction finish while single queries flow,
// then refuses single queries and waits out the in-flight ones.
func TestFenceGate_TwoStageDrain(t *testing.T) {
	mock := newDrainMockPoolManager()
	s := newFencedTestPooler(t, mock, servingstate.RoutingRolePrimary)
	applyFence(t, s, false)
	mock.reservedAdd(1)
	mock.regularAdd(1)

	done := make(chan error, 1)
	go func() {
		done <- s.ApplyFence(context.Background(), FenceRead{Epoch: s.FenceEpoch(), Fenced: true}, 5*time.Second)
	}()

	waitForFenceDrainPhase(t, s, drainReserved)
	require.NoError(t, admit(s.StartRequest(nil, RequestSingleQuery)), "stage 1 serves single queries")
	require.NoError(t, admit(s.StartRequest(nil, RequestExistingReserved)), "the open transaction continues")
	requireMTF02(t, admit(s.StartRequest(nil, RequestNewReservation)))

	mock.reservedAdd(-1)
	waitForFenceDrainPhase(t, s, drainRegular)
	requireMTF02(t, admit(s.StartRequest(nil, RequestSingleQuery)), "stage 2 refuses single queries")
	select {
	case <-done:
		t.Fatal("the fence drain returned with a single query in flight")
	case <-time.After(50 * time.Millisecond):
	}

	mock.regularAdd(-1)
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("the fence drain did not complete")
	}
	assert.Zero(t, mock.closeReservedCallCount(), "a drain that completes closes nothing")
	requireMTF02(t, admit(s.StartRequest(nil, RequestSingleQuery)))
	assert.True(t, s.IsServing())
}

// At the drain timeout the remaining reserved connections are force-closed
// and the fence completes.
func TestFenceGate_DrainTimeoutForceCloses(t *testing.T) {
	mock := newDrainMockPoolManager()
	s := newFencedTestPooler(t, mock, servingstate.RoutingRolePrimary)
	applyFence(t, s, false)
	mock.reservedAdd(1) // a transaction that never ends
	defer mock.reservedAdd(-1)

	start := time.Now()
	require.NoError(t, s.ApplyFence(t.Context(), FenceRead{Epoch: s.FenceEpoch(), Fenced: true}, 100*time.Millisecond))
	assert.GreaterOrEqual(t, time.Since(start), 80*time.Millisecond)
	assert.Equal(t, 1, mock.closeReservedCallCount())
	requireMTF02(t, admit(s.StartRequest(nil, RequestSingleQuery)))
}

// A cancelled caller does not cut the drain short: the drain ends at its
// timeout with a force-close, never leaving the gate half drained.
func TestFenceGate_DrainOutlivesTheCaller(t *testing.T) {
	mock := newDrainMockPoolManager()
	s := newFencedTestPooler(t, mock, servingstate.RoutingRolePrimary)
	applyFence(t, s, false)
	mock.reservedAdd(1)
	defer mock.reservedAdd(-1)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	start := time.Now()
	require.NoError(t, s.ApplyFence(ctx, FenceRead{Epoch: s.FenceEpoch(), Fenced: true}, 100*time.Millisecond))
	assert.GreaterOrEqual(t, time.Since(start), 80*time.Millisecond, "the drain waited for its timeout")
	assert.Equal(t, 1, mock.closeReservedCallCount())
}

// The fence drain and the not-serving drain keep their own phases: neither
// moves the other's, and the stricter of the two decides admission.
func TestFenceGate_DrainsDoNotInterfere(t *testing.T) {
	mock := newDrainMockPoolManager()
	s := newFencedTestPooler(t, mock, servingstate.RoutingRolePrimary)
	applyFence(t, s, false)
	mock.reservedAdd(1)

	fenceDone := make(chan error, 1)
	go func() {
		fenceDone <- s.ApplyFence(context.Background(), FenceRead{Epoch: s.FenceEpoch(), Fenced: true}, 5*time.Second)
	}()
	waitForFenceDrainPhase(t, s, drainReserved)

	stateDone := make(chan error, 1)
	go func() {
		stateDone <- s.OnStateChange(context.Background(), disabledState(servingstate.RoutingRolePrimary))
	}()
	waitForDrainPhase(t, s, drainReserved)

	s.mu.Lock()
	assert.Equal(t, drainReserved, s.fence.drainPhase, "the not-serving drain leaves the fence drain alone")
	s.mu.Unlock()
	require.NoError(t, admit(s.StartRequest(nil, RequestSingleQuery)), "both drains are in stage 1")
	requireMTF01(t, admit(s.StartRequest(nil, RequestNewReservation))) // the not-serving drain refuses first

	mock.reservedAdd(-1)
	for name, done := range map[string]chan error{"fence": fenceDone, "not-serving": stateDone} {
		select {
		case err := <-done:
			require.NoError(t, err, name)
		case <-time.After(2 * time.Second):
			t.Fatalf("the %s drain did not complete", name)
		}
	}
	assert.False(t, s.IsServing())
	s.mu.Lock()
	assert.Equal(t, fenceGateClosed, s.fence.state, "the not-serving transition leaves the gate closed")
	assert.Equal(t, drainNone, s.drainPhase)
	assert.Equal(t, drainNone, s.fence.drainPhase)
	s.mu.Unlock()

	// Serving again, the pooler is still fenced.
	require.NoError(t, s.OnStateChange(t.Context(), servingState(servingstate.RoutingRolePrimary)))
	requireMTF02(t, admit(s.StartRequest(nil, RequestSingleQuery)))
	assert.Zero(t, mock.closeReservedCallCount())
}

// A replica the read finds fenced records it without draining.
func TestFenceGate_ReplicaDoesNotDrain(t *testing.T) {
	mock := newDrainMockPoolManager()
	s := newFencedTestPooler(t, mock, servingstate.RoutingRoleReplica)
	mock.reservedAdd(1)
	defer mock.reservedAdd(-1)
	start := time.Now()
	require.NoError(t, s.ApplyFence(t.Context(), FenceRead{Epoch: s.FenceEpoch(), Fenced: true}, 5*time.Second))
	assert.Less(t, time.Since(start), time.Second, "no drain on a replica")
	assert.Zero(t, mock.closeReservedCallCount())
}

// A fence drain records the drain metrics with reason fence, so dashboards
// tell it from a failover drain.
func TestFenceGate_DrainMetricsReason(t *testing.T) {
	reader := setupPoolerTelemetry(t)
	mock := newDrainMockPoolManager()
	s := newFencedTestPooler(t, mock, servingstate.RoutingRolePrimary)
	applyFence(t, s, false)
	mock.reservedAdd(1)
	mock.closeReservedCount = 1
	require.NoError(t, s.ApplyFence(t.Context(), FenceRead{Epoch: s.FenceEpoch(), Fenced: true}, 50*time.Millisecond))
	mock.reservedAdd(-1)

	fence := map[string]string{"reason": drainReasonFence, "routing_role": "primary"}
	outcome := map[string]string{"reason": drainReasonFence, "routing_role": "primary", "outcome": drainOutcomeForceClose}
	assert.Equal(t, int64(1), readCounterByAttrs(t, reader, "mg.pooler.drain.outcome", outcome))
	assert.Equal(t, int64(1), readCounterByAttrs(t, reader, "mg.pooler.drain.force_closed", fence))
	assert.Equal(t, int64(0), readCounterByAttrs(t, reader, "mg.pooler.drain.outcome", primaryDrainAttrs(drainOutcomeForceClose)), "no failover drain was recorded")
}

// A credential lookup or a Describe is not application work: a fenced or
// unread primary still serves it, so clients can log in, and describe the
// statements they then get MTF02 for, which the gateway retries. The
// not-serving drain still refuses it.
func TestFenceGate_UnfencedRequests(t *testing.T) {
	s := newFencedTestPooler(t, newDrainMockPoolManager(), servingstate.RoutingRolePrimary)
	require.NoError(t, s.startUnfencedRequest(nil, RequestNewReservation), "unread")
	applyFence(t, s, true)
	require.NoError(t, s.startUnfencedRequest(nil, RequestNewReservation), "fenced")
	requireMTF02(t, admit(s.StartRequest(nil, RequestNewReservation)))

	s.mu.Lock()
	s.drainPhase = drainReserved
	s.mu.Unlock()
	requireMTF01(t, s.startUnfencedRequest(nil, RequestNewReservation))
}

// Through the query service, a fenced primary refuses an Execute with MTF02
// but admits a Describe past the gate (it then fails here only because the
// test server has no executor).
func TestFenceGate_DescribeIsAdmittedOnAFencedPrimary(t *testing.T) {
	s := newFencedTestPooler(t, newDrainMockPoolManager(), servingstate.RoutingRolePrimary)
	applyFence(t, s, true)
	qs := &admittedQueryService{s: s}

	_, _, err := qs.ExecuteQuery(t.Context(), nil, "SELECT 1", nil)
	requireMTF02(t, err)

	_, err = qs.Describe(t.Context(), nil, nil, nil, nil)
	require.Error(t, err)
	assert.False(t, mterrors.IsErrorCode(err, mterrors.MTF02.ID), "a Describe is not fenced: %v", err)
	assert.Contains(t, err.Error(), "executor not initialized")
}

// at is a snapshot position under coordinator term term at routing version
// version.
func at(term, version int64) shardingcatalog.Position {
	return shardingcatalog.Position{Rule: &clustermetadatapb.RuleNumber{CoordinatorTerm: term}, RoutingVersion: version}
}

// A read of a snapshot older than one the gate applied is refused, by rule
// first and then routing version, also after a promotion reset the gate.
func TestFenceGate_RefusesAnOlderSnapshot(t *testing.T) {
	s := newFencedTestPooler(t, newDrainMockPoolManager(), servingstate.RoutingRolePrimary)
	require.NoError(t, s.ApplyFence(t.Context(), FenceRead{Epoch: s.FenceEpoch(), Position: at(2, 4), Fenced: true}, time.Second))

	for name, pos := range map[string]shardingcatalog.Position{
		"a deposed primary's, at a higher version": at(1, 9),
		"the same primary's, at a lower version":   at(2, 3),
		"no catalog":                               {},
	} {
		err := s.ApplyFence(t.Context(), FenceRead{Epoch: s.FenceEpoch(), Position: pos, Fenced: false}, time.Second)
		assert.Equal(t, mtrpcpb.Code_UNAVAILABLE, mterrors.Code(err), "%s: %v", name, err)
		requireMTF02(t, admit(s.StartRequest(nil, RequestSingleQuery)), name)
	}

	// The same snapshot again, and a newer primary's at a lower version, are
	// applied.
	require.NoError(t, s.ApplyFence(t.Context(), FenceRead{Epoch: s.FenceEpoch(), Position: at(2, 4), Fenced: true}, time.Second))
	require.NoError(t, s.ApplyFence(t.Context(), FenceRead{Epoch: s.FenceEpoch(), Position: at(3, 1), Fenced: true}, time.Second))

	require.NoError(t, s.OnStateChange(t.Context(), servingState(servingstate.RoutingRoleReplica)))
	require.NoError(t, s.OnStateChange(t.Context(), servingState(servingstate.RoutingRolePrimary)))
	err := s.ApplyFence(t.Context(), FenceRead{Epoch: s.FenceEpoch(), Position: at(2, 9), Fenced: false}, time.Second)
	assert.Equal(t, mtrpcpb.Code_UNAVAILABLE, mterrors.Code(err), "the floor survives a promotion: %v", err)
	require.NoError(t, s.ApplyFence(t.Context(), FenceRead{Epoch: s.FenceEpoch(), Position: at(3, 2), Fenced: false}, time.Second))
	require.NoError(t, admit(s.StartRequest(nil, RequestSingleQuery)))
}
