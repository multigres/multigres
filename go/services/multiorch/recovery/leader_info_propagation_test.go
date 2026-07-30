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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/timestamppb"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
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
