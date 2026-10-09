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

package actions

import (
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/services/multiorch/consensus"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
	"github.com/multigres/multigres/go/services/multiorch/store"
)

// TestAppointLeaderAction_ExecuteDoesNotSecondGuessTheRecheck pins the fix for
// "primary already exists, skipping leader appointment": the engine has already
// refreshed health and re-run the analyzer, so Execute must go straight to the
// coordinator even when the leader answers a live Status as healthy. It must not
// poll the leader or return nil on its own judgment.
func TestAppointLeaderAction_ExecuteDoesNotSecondGuessTheRecheck(t *testing.T) {
	ctx := t.Context()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "cell1")
	logger := slog.Default()
	shardKey := &clustermetadatapb.ShardKey{Database: "db1", TableGroup: "tg1", Shard: "0"}
	leaderID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "leader"}

	fakeClient := rpcclient.NewFakeClient()
	fakeClient.SetStatusResponse("multipooler-cell1-leader", &multipoolermanagerdatapb.StatusResponse{
		Status:          &multipoolermanagerdatapb.Status{PoolerType: clustermetadatapb.PoolerType_PRIMARY, PostgresReady: true},
		ConsensusStatus: &clustermetadatapb.ConsensusStatus{Id: leaderID},
	})
	cache := store.NewTestCache(t)
	store.SeedCache(t, cache, store.NewPooler(&multiorchdatapb.PoolerHealthState{
		Multipooler: &clustermetadatapb.Multipooler{Id: leaderID, ShardKey: shardKey},
		LastSeen:    timestamppb.Now(),
		Status:      &multipoolermanagerdatapb.Status{PoolerType: clustermetadatapb.PoolerType_PRIMARY, PostgresReady: true},
		ConsensusStatus: &clustermetadatapb.ConsensusStatus{
			Id: leaderID,
			CurrentPosition: &clustermetadatapb.PoolerPosition{Position: &clustermetadatapb.RulePosition{
				Decision: &clustermetadatapb.ShardRule{LeaderId: leaderID, RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 1}},
			}},
		},
	}, nil))
	coordinator := consensus.NewCoordinator(nil, ts, fakeClient, logger)
	action := NewAppointLeaderAction(nil, coordinator, cache, logger, time.Second)

	err := action.Execute(ctx, types.RecheckedProblem{Problem: types.Problem{Code: types.ProblemLeaderUnhealthy, ShardKey: shardKey}})

	// It reached the coordinator, which cannot recruit anyone in this fake. The
	// old behavior returned nil here ("primary already exists, skipping").
	require.ErrorContains(t, err, "failed to appoint leader")
	for _, call := range fakeClient.GetCallLog() {
		assert.NotContains(t, call, "Status(", "Execute must not poll the leader itself")
	}
}
