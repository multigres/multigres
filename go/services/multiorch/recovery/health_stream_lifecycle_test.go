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

package recovery

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
	"github.com/multigres/multigres/go/services/multiorch/store"
)

// makeLifecycleSnapshotForPrimaryTerm builds a snapshot whose
// AvailabilityStatus carries the given LifecycleStatus, with a
// ConsensusStatus whose Id and Rule.LeaderId both reference poolerID and
// whose CoordinatorTerm is the given primaryTerm. This is the shape that
// consensus.LeaderTerm reads as "this pooler is leader at term=primaryTerm."
func makeLifecycleSnapshotForPrimaryTerm(poolerID *clustermetadata.ID, signal clustermetadata.LifecycleSignal, primaryTerm int64) *multipoolermanagerdatapb.ManagerHealthStreamResponse {
	cs := &clustermetadata.ConsensusStatus{
		Id: poolerID,
		CurrentPosition: &clustermetadata.PoolerPosition{
			Rule: &clustermetadata.ShardRule{
				LeaderId: poolerID,
				RuleNumber: &clustermetadata.RuleNumber{
					CoordinatorTerm: primaryTerm,
				},
			},
		},
	}
	return &multipoolermanagerdatapb.ManagerHealthStreamResponse{
		Message: &multipoolermanagerdatapb.ManagerHealthStreamResponse_Snapshot{
			Snapshot: &multipoolermanagerdatapb.ManagerHealthSnapshot{
				Status: &multipoolermanagerdatapb.StatusResponse{
					Status: &multipoolermanagerdatapb.Status{
						PoolerType: clustermetadata.PoolerType_PRIMARY,
					},
					AvailabilityStatus: &clustermetadata.AvailabilityStatus{
						LifecycleStatus: &clustermetadata.LifecycleStatus{Signal: signal},
					},
					ConsensusStatus: cs,
				},
			},
		},
	}
}

// TestHealthStream_StreamEOFAfterSTOPPED_SkipsBackoff verifies that once a
// pooler has sent STOPPED, the subsequent stream EOF (produced by
// healthStreamer.Shutdown closing subscriber channels) bypasses the normal
// reconnect-with-backoff path. The failover-trigger synthesis is exercised
// via the STOPPED snapshot itself (handleLifecycleSnapshot); this test
// covers the orthogonal "don't waste cycles reconnecting" path.
func TestHealthStream_StreamEOFAfterSTOPPED_SkipsBackoff(t *testing.T) {
	ctx := t.Context()

	fakeClient := rpcclient.NewFakeClient()
	streamCh := make(chan *rpcclient.FakeManagerHealthStream, 4)
	fakeClient.OnManagerHealthStream = func(_ string, s *rpcclient.FakeManagerHealthStream) {
		streamCh <- s
	}

	poolerStore := store.NewPoolerStore(fakeClient, slog.Default())
	sm := newTestHealthStream(ctx, fakeClient, poolerStore)

	poolerID := &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "p1"}
	key := seedPrimaryWithTerm(poolerStore, poolerID, 5)

	sm.Start(poolerID)
	stream := <-streamCh
	completeHandshake(t, stream)

	stream.Ch <- makeLifecycleSnapshotForPrimaryTerm(poolerID, clustermetadata.LifecycleSignal_LIFECYCLE_SIGNAL_STOPPED, 5)

	// STOPPED snapshot alone is the failover trigger.
	require.Eventually(t, func() bool {
		s, ok := poolerStore.Get(key)
		return ok && s.GetAvailabilityStatus().GetLeadershipStatus().GetSignal() == clustermetadata.LeadershipSignal_LEADERSHIP_SIGNAL_REQUESTING_DEMOTION
	}, 2*time.Second, 10*time.Millisecond, "STOPPED snapshot must synthesize REQUESTING_DEMOTION")

	cached, _ := poolerStore.Get(key)
	assert.True(t, types.LeaderNeedsReplacement(cached),
		"LeaderNeedsReplacement must trigger failover after STOPPED")

	// Now close the stream. Orchestrator must NOT attempt to reconnect.
	close(stream.Ch)

	select {
	case <-streamCh:
		t.Fatal("orchestrator should not reconnect after EOF that follows STOPPED")
	case <-time.After(500 * time.Millisecond):
	}
}

// TestHealthStream_StreamEOFWithoutLifecycleSignal_KeepsBackoff verifies that
// an EOF on a stream that never announced a lifecycle signal still goes
// through the existing reconnect-with-backoff path.
func TestHealthStream_StreamEOFWithoutLifecycleSignal_KeepsBackoff(t *testing.T) {
	ctx := t.Context()

	fakeClient := rpcclient.NewFakeClient()
	streamCh := make(chan *rpcclient.FakeManagerHealthStream, 4)
	fakeClient.OnManagerHealthStream = func(_ string, s *rpcclient.FakeManagerHealthStream) {
		streamCh <- s
	}

	poolerStore := store.NewPoolerStore(fakeClient, slog.Default())
	sm := newTestHealthStream(ctx, fakeClient, poolerStore)

	poolerID := &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "p1"}
	seedPooler(poolerStore, poolerID, clustermetadata.PoolerType_PRIMARY)

	sm.Start(poolerID)
	stream := <-streamCh
	completeHandshake(t, stream)

	// Send a regular snapshot — no lifecycle signal.
	stream.Ch <- makeSnapshot(&multipoolermanagerdatapb.Status{
		PoolerType:    clustermetadata.PoolerType_PRIMARY,
		PostgresReady: true,
	})

	// Close the stream. Orchestrator must reconnect.
	close(stream.Ch)

	select {
	case <-streamCh:
		// Expected: a reconnect attempt arrived.
	case <-time.After(3 * time.Second):
		t.Fatal("expected reconnect attempt after spurious EOF without lifecycle signal")
	}
}

// TestSynthesizeRequestingDemotion_SetsSignalForPrimary verifies the
// happy-path: a PRIMARY pooler with a non-zero primary_term gets
// REQUESTING_DEMOTION written into its cached LeadershipStatus with the
// captured term.
func TestSynthesizeRequestingDemotion_SetsSignalForPrimary(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	fakeClient := rpcclient.NewFakeClient()
	poolerStore := store.NewPoolerStore(fakeClient, logger)

	poolerID := &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "p1"}
	key := seedPrimaryWithTerm(poolerStore, poolerID, 7)

	hs := NewHealthStream(context.Background(), fakeClient, poolerStore, logger)
	hs.synthesizeRequestingDemotion(key, "test")

	cached, ok := poolerStore.Get(key)
	require.True(t, ok)
	leadership := cached.GetAvailabilityStatus().GetLeadershipStatus()
	require.NotNil(t, leadership, "LeadershipStatus must be written")
	assert.Equal(t, clustermetadata.LeadershipSignal_LEADERSHIP_SIGNAL_REQUESTING_DEMOTION, leadership.Signal)
	assert.Equal(t, int64(7), leadership.LeaderTerm,
		"leader_term must equal the captured primary_term")
	assert.True(t, types.LeaderNeedsReplacement(cached),
		"LeaderNeedsReplacement must fire after REQUESTING_DEMOTION is synthesized")
}

// TestSynthesizeRequestingDemotion_NoOpForNonPrimary verifies that a
// REPLICA pooler does not get REQUESTING_DEMOTION written even when the
// observer's trigger fires. Only the leader's departure should request
// demotion.
func TestSynthesizeRequestingDemotion_NoOpForNonPrimary(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	fakeClient := rpcclient.NewFakeClient()
	poolerStore := store.NewPoolerStore(fakeClient, logger)

	poolerID := &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "p1"}
	key := seedPooler(poolerStore, poolerID, clustermetadata.PoolerType_REPLICA)

	hs := NewHealthStream(context.Background(), fakeClient, poolerStore, logger)
	hs.synthesizeRequestingDemotion(key, "test")

	cached, _ := poolerStore.Get(key)
	assert.Nil(t, cached.GetAvailabilityStatus().GetLeadershipStatus(),
		"non-primary must not get a LeadershipStatus written")
}

// TestSynthesizeRequestingDemotion_NoOpForZeroTerm verifies that a PRIMARY
// with no consensus primary_term (or term==0) is left alone. Writing
// REQUESTING_DEMOTION with leader_term=0 would never pass LeaderNeedsReplacement's
// staleness check, so the helper short-circuits.
func TestSynthesizeRequestingDemotion_NoOpForZeroTerm(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	fakeClient := rpcclient.NewFakeClient()
	poolerStore := store.NewPoolerStore(fakeClient, logger)

	poolerID := &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "p1"}
	// PRIMARY type but no ConsensusStatus → primary_term == 0.
	key := seedPooler(poolerStore, poolerID, clustermetadata.PoolerType_PRIMARY)

	hs := NewHealthStream(context.Background(), fakeClient, poolerStore, logger)
	hs.synthesizeRequestingDemotion(key, "test")

	cached, _ := poolerStore.Get(key)
	assert.Nil(t, cached.GetAvailabilityStatus().GetLeadershipStatus(),
		"primary with zero term must not get a LeadershipStatus written")
}

// TestSynthesizeRequestingDemotion_IdempotentSameTerm verifies that the
// signal stays at the same term across repeated calls. Convergent trigger
// paths (STOPPED snapshot + EOF + deadline expiry) can all fire for the same
// pooler; the helper must short-circuit so the cached signal does not get
// rewritten with a stale value.
func TestSynthesizeRequestingDemotion_IdempotentSameTerm(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	fakeClient := rpcclient.NewFakeClient()
	poolerStore := store.NewPoolerStore(fakeClient, logger)

	poolerID := &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "p1"}
	key := seedPrimaryWithTerm(poolerStore, poolerID, 7)

	hs := NewHealthStream(context.Background(), fakeClient, poolerStore, logger)
	hs.synthesizeRequestingDemotion(key, "first")
	hs.synthesizeRequestingDemotion(key, "second")
	hs.synthesizeRequestingDemotion(key, "third")

	cached, _ := poolerStore.Get(key)
	leadership := cached.GetAvailabilityStatus().GetLeadershipStatus()
	require.NotNil(t, leadership)
	assert.Equal(t, clustermetadata.LeadershipSignal_LEADERSHIP_SIGNAL_REQUESTING_DEMOTION, leadership.Signal)
	assert.Equal(t, int64(7), leadership.LeaderTerm)
}
