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

package recovery

import (
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/multigres/multigres/go/common/rpcclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	"github.com/multigres/multigres/go/services/multiorch/store"
)

func leaderInfoTestRule(term int64) *clustermetadatapb.ShardRule {
	return &clustermetadatapb.ShardRule{RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: term}}
}

func leaderInfoTestAddress(name string) *clustermetadatapb.PoolerAddress {
	return &clustermetadatapb.PoolerAddress{
		Id:           &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: name},
		Host:         "host-" + name,
		PostgresPort: 5432,
	}
}

func TestShouldPropagateLeaderInfo(t *testing.T) {
	now := time.Unix(1700000000, 0)
	fresh := timestamppb.New(now.Add(-10 * time.Second))
	stale := timestamppb.New(now.Add(-2 * time.Minute))

	leaderAddress := leaderInfoTestAddress("leader")
	currentPosition := &clustermetadatapb.RulePosition{Decision: leaderInfoTestRule(5)}
	olderPosition := &clustermetadatapb.RulePosition{Decision: leaderInfoTestRule(3)}

	tests := []struct {
		name              string
		rp                *clustermetadatapb.ReplicationPrimary
		lastSeen          *timestamppb.Timestamp
		leaderRewindReady bool
		want              bool
	}{
		{
			name:              "NeverToldAnything_Propagates",
			rp:                nil,
			lastSeen:          fresh,
			leaderRewindReady: false,
			want:              true,
		},
		{
			name:              "StaleHealth_Skipped_EvenIfBehind",
			rp:                nil,
			lastSeen:          stale,
			leaderRewindReady: true,
			want:              false,
		},
		{
			name: "PositionBehind_Propagates",
			rp: &clustermetadatapb.ReplicationPrimary{
				Position: olderPosition,
				Primary:  leaderAddress,
			},
			lastSeen:          fresh,
			leaderRewindReady: false,
			want:              true,
		},
		{
			name: "PositionMatches_BothRewindReadyFalse_NoOp",
			rp: &clustermetadatapb.ReplicationPrimary{
				Position:    currentPosition,
				Primary:     leaderAddress,
				RewindReady: false,
			},
			lastSeen:          fresh,
			leaderRewindReady: false,
			want:              false,
		},
		{
			name: "PositionMatches_BothRewindReadyTrue_NoOp",
			rp: &clustermetadatapb.ReplicationPrimary{
				Position:    currentPosition,
				Primary:     leaderAddress,
				RewindReady: true,
			},
			lastSeen:          fresh,
			leaderRewindReady: true,
			want:              false,
		},
		{
			name: "PositionMatches_FollowerAlreadyTrue_LeaderViewStaleFalse_NeverRegress",
			rp: &clustermetadatapb.ReplicationPrimary{
				Position:    currentPosition,
				Primary:     leaderAddress,
				RewindReady: true,
			},
			lastSeen:          fresh,
			leaderRewindReady: false,
			want:              false,
		},
		{
			name: "PositionMatches_FollowerFalse_LeaderTrue_PropagatesGoodNews",
			rp: &clustermetadatapb.ReplicationPrimary{
				Position:    currentPosition,
				Primary:     leaderAddress,
				RewindReady: false,
			},
			lastSeen:          fresh,
			leaderRewindReady: true,
			want:              true,
		},
		{
			name: "PositionMatches_DifferentPrimaryContact_Propagates",
			rp: &clustermetadatapb.ReplicationPrimary{
				Position:    currentPosition,
				Primary:     leaderInfoTestAddress("stale-leader"),
				RewindReady: true,
			},
			lastSeen:          fresh,
			leaderRewindReady: true,
			want:              true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := shouldPropagateLeaderInfo(tt.rp, tt.lastSeen, now, leaderAddress, tt.leaderRewindReady, currentPosition)
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestPropagateLeaderInfoToPooler_CachesSuccessfulSetPrimary verifies that a
// successful SetPrimary call optimistically updates the pooler's cached
// ReplicationPrimary, so a reconcile tick immediately after doesn't
// needlessly re-send the same SetPrimary before the pooler's own streamed
// health update reports it back (see shouldPropagateLeaderInfo).
func TestPropagateLeaderInfoToPooler_CachesSuccessfulSetPrimary(t *testing.T) {
	fake := rpcclient.NewFakeClient()
	re := &Engine{rpcClient: fake, logger: slog.Default()}

	poolerID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "p1"}
	pooler := store.NewPooler(&multiorchdatapb.PoolerHealthState{
		Multipooler: &clustermetadatapb.Multipooler{Id: poolerID},
		LastSeen:    timestamppb.Now(),
	}, nil)

	leaderAddress := leaderInfoTestAddress("leader")
	position := &clustermetadatapb.RulePosition{Decision: leaderInfoTestRule(5)}

	re.propagateLeaderInfoToPooler(t.Context(), pooler, leaderAddress, true, position)

	require.Equal(t, []string{"SetPrimary(multipooler-zone1-p1)"}, fake.GetCallLog(), "SetPrimary must actually have been called")
	got := pooler.Health().GetConsensusStatus().GetReplicationPrimary()
	require.NotNil(t, got)
	assert.True(t, proto.Equal(leaderAddress, got.GetPrimary()))
	assert.True(t, got.GetRewindReady())
	assert.True(t, proto.Equal(position, got.GetPosition()))
}

// TestPropagateLeaderInfoToPooler_NeverRegressesCachedPrimary verifies the
// optimistic cache update never overwrites a fresher view with a stale one —
// e.g. a real streamed health update landed while an older SetPrimary call
// (still in flight from an earlier tick) was completing.
func TestPropagateLeaderInfoToPooler_NeverRegressesCachedPrimary(t *testing.T) {
	fake := rpcclient.NewFakeClient()
	re := &Engine{rpcClient: fake, logger: slog.Default()}

	poolerID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "p1"}
	newerPrimary := &clustermetadatapb.ReplicationPrimary{
		Position: &clustermetadatapb.RulePosition{Decision: leaderInfoTestRule(10)},
		Primary:  leaderInfoTestAddress("newer-leader"),
	}
	pooler := store.NewPooler(&multiorchdatapb.PoolerHealthState{
		Multipooler:     &clustermetadatapb.Multipooler{Id: poolerID},
		ConsensusStatus: &clustermetadatapb.ConsensusStatus{ReplicationPrimary: newerPrimary},
		LastSeen:        timestamppb.Now(),
	}, nil)

	stalePosition := &clustermetadatapb.RulePosition{Decision: leaderInfoTestRule(3)}
	re.propagateLeaderInfoToPooler(t.Context(), pooler, leaderInfoTestAddress("stale-leader"), false, stalePosition)

	require.Equal(t, []string{"SetPrimary(multipooler-zone1-p1)"}, fake.GetCallLog(), "SetPrimary must actually have been called with the stale info")
	got := pooler.Health().GetConsensusStatus().GetReplicationPrimary()
	assert.True(t, proto.Equal(newerPrimary, got), "must not regress a fresher cached view with a stale optimistic update")
}
