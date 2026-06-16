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

package actions

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/services/multiorch/store"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

func TestPollLeaderHealth(t *testing.T) {
	ctx := context.Background()
	leaderID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "primary"}

	leaderState := &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{Id: leaderID, Type: clustermetadatapb.PoolerType_PRIMARY},
	}
	// A status that names leaderID as the leader (self-claim under a real rule).
	servingStatus := &clustermetadatapb.ConsensusStatus{
		Id: leaderID,
		CurrentPosition: &clustermetadatapb.PoolerPosition{
			Rule: &clustermetadatapb.ShardRule{
				RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 1},
				LeaderId:   leaderID,
			},
		},
	}

	t.Run("returns the leader when reachable and still reporting itself leader", func(t *testing.T) {
		fakeClient := rpcclient.NewFakeClient()
		fakeClient.SetStatusResponse("multipooler-cell1-primary", &multipoolermanagerdatapb.StatusResponse{
			ConsensusStatus: servingStatus,
		})

		got, err := pollLeaderHealth(ctx, fakeClient, store.ShardMembers{Leader: leaderState})

		require.NoError(t, err)
		assert.Equal(t, "primary", got.MultiPooler.Id.Name)
	})

	t.Run("errors when no leader is known", func(t *testing.T) {
		got, err := pollLeaderHealth(ctx, rpcclient.NewFakeClient(), store.ShardMembers{Leader: nil})

		require.Error(t, err)
		assert.Nil(t, got)
		assert.Contains(t, err.Error(), "no consensus leader known")
	})

	t.Run("errors when the leader is unreachable", func(t *testing.T) {
		fakeClient := rpcclient.NewFakeClient()
		fakeClient.Errors["multipooler-cell1-primary"] = errors.New("connection refused")

		got, err := pollLeaderHealth(ctx, fakeClient, store.ShardMembers{Leader: leaderState})

		require.Error(t, err)
		assert.Nil(t, got)
		assert.Contains(t, err.Error(), "unreachable during health check")
	})

	t.Run("errors when the leader no longer reports itself as leader", func(t *testing.T) {
		// The live status no longer names this node as leader — e.g. it resigned
		// or dropped into recovery since the cached snapshot was taken.
		fakeClient := rpcclient.NewFakeClient()
		fakeClient.SetStatusResponse("multipooler-cell1-primary", &multipoolermanagerdatapb.StatusResponse{
			ConsensusStatus: &clustermetadatapb.ConsensusStatus{Id: leaderID},
		})

		got, err := pollLeaderHealth(ctx, fakeClient, store.ShardMembers{Leader: leaderState})

		require.Error(t, err)
		assert.Nil(t, got)
		assert.Contains(t, err.Error(), "no longer reports itself as the leader")
	})
}
