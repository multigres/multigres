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
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	"github.com/multigres/multigres/go/services/multiorch/config"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
	"github.com/multigres/multigres/go/services/multiorch/store"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// makeDemoteScenarioPoolers returns a stale-leader ID and a populated
// PoolerHealthState pair that places the correct leader in the same shard.
// The "correct" leader has the higher term and is recognized by IsLeader
// (current_position.rule.leader_id == self.id).
func makeDemoteScenarioPoolers(t *testing.T, poolerStore *store.PoolerStore) (staleLeaderID *clustermetadatapb.ID) {
	t.Helper()
	shardKey := &clustermetadatapb.ShardKey{
		Database:   "testdb",
		TableGroup: "default",
		Shard:      "0",
	}

	staleLeaderID = &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "cell1",
		Name:      "stale-leader",
	}
	correctLeaderID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "cell1",
		Name:      "correct-leader",
	}

	poolerStore.Set("multipooler-cell1-stale-leader", &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id:       staleLeaderID,
			Hostname: "stale.example.com",
			PortMap:  map[string]int32{"postgres": 5432},
			ShardKey: shardKey,
			Type:     clustermetadatapb.PoolerType_PRIMARY,
		},
		Status: &multipoolermanagerdatapb.Status{
			PostgresReady: true,
		},
	})

	correctPosition := &clustermetadatapb.PoolerPosition{
		Rule: &clustermetadatapb.ShardRule{
			RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 5},
			LeaderId:   correctLeaderID,
		},
		Lsn: "0/1000",
	}
	poolerStore.Set("multipooler-cell1-correct-leader", &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id:       correctLeaderID,
			Hostname: "correct.example.com",
			PortMap:  map[string]int32{"postgres": 5433},
			ShardKey: shardKey,
			Type:     clustermetadatapb.PoolerType_PRIMARY,
		},
		ConsensusStatus: &clustermetadatapb.ConsensusStatus{
			Id:              correctLeaderID,
			TermRevocation:  &clustermetadatapb.TermRevocation{RevokedBelowTerm: 5},
			CurrentPosition: correctPosition,
		},
	})
	return staleLeaderID
}

func TestDemoteStaleLeaderAction_Metadata(t *testing.T) {
	action := NewDemoteStaleLeaderAction(config.NewTestConfig(), nil, nil, nil, slog.Default())
	md := action.Metadata()
	assert.Equal(t, "DemoteStaleLeader", md.Name)
	assert.True(t, md.Retryable)
	assert.Equal(t, 60*time.Second, md.Timeout)
}

func TestDemoteStaleLeaderAction_RequiresHealthyLeader(t *testing.T) {
	action := NewDemoteStaleLeaderAction(config.NewTestConfig(), nil, nil, nil, slog.Default())
	// We are repairing a leader, so requiring a healthy one is the wrong precondition.
	assert.False(t, action.RequiresHealthyLeader())
}

func TestDemoteStaleLeaderAction_Priority(t *testing.T) {
	action := NewDemoteStaleLeaderAction(config.NewTestConfig(), nil, nil, nil, slog.Default())
	assert.Equal(t, types.PriorityHigh, action.Priority())
}

// TestDemoteStaleLeaderAction_ExecuteLegacyFlow asserts that with
// use-new-consensus-flow disabled, Execute routes through the
// DemoteStalePrimary RPC and forwards the correct-leader contact info + term.
// TestDemoteStaleLeaderAction_Execute asserts that Execute routes through
// SetPrimary and forwards the correct-leader contact info + position.
func TestDemoteStaleLeaderAction_Execute(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "cell1")
	defer ts.Close()

	fakeClient := rpcclient.NewFakeClient()
	fakeClient.SetPrimaryResponses["multipooler-cell1-stale-leader"] = &consensusdatapb.SetPrimaryResponse{}

	poolerStore := store.NewPoolerStore(fakeClient, slog.Default())
	staleLeaderID := makeDemoteScenarioPoolers(t, poolerStore)

	cfg := config.NewTestConfig()
	action := NewDemoteStaleLeaderAction(cfg, fakeClient, poolerStore, ts, slog.Default())

	problem := types.Problem{
		Code: types.ProblemStaleLeader,
		ShardKey: &clustermetadatapb.ShardKey{
			Database:   "testdb",
			TableGroup: "default",
			Shard:      "0",
		},
		PoolerID: staleLeaderID,
	}
	require.NoError(t, action.Execute(ctx, problem))

	assert.Contains(t, fakeClient.CallLog, "SetPrimary(multipooler-cell1-stale-leader)")
	assert.NotContains(t, fakeClient.CallLog, "DemoteStalePrimary(multipooler-cell1-stale-leader)")

	req := fakeClient.SetPrimaryRequests["multipooler-cell1-stale-leader"]
	require.NotNil(t, req)
	require.NotNil(t, req.Leader)
	assert.Equal(t, "correct-leader", req.Leader.Id.Name)
	assert.Equal(t, "correct.example.com", req.Leader.GetHost())
	require.NotNil(t, req.Rule)
	assert.Equal(t, int64(5), req.Rule.GetRuleNumber().GetCoordinatorTerm())
}

// TestDemoteStaleLeaderAction_ExecuteNoCorrectLeader asserts the error path
// when no other pooler in the shard claims leadership — the action must
// surface a clear error rather than calling either RPC.
func TestDemoteStaleLeaderAction_ExecuteNoCorrectLeader(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "cell1")
	defer ts.Close()

	fakeClient := rpcclient.NewFakeClient()
	poolerStore := store.NewPoolerStore(fakeClient, slog.Default())

	staleLeaderID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "cell1",
		Name:      "stale-leader",
	}
	shardKey := &clustermetadatapb.ShardKey{
		Database:   "testdb",
		TableGroup: "default",
		Shard:      "0",
	}
	// Only the stale leader is in the store — no current leader exists.
	poolerStore.Set("multipooler-cell1-stale-leader", &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id:       staleLeaderID,
			ShardKey: shardKey,
			Type:     clustermetadatapb.PoolerType_PRIMARY,
		},
	})

	cfg := config.NewTestConfig()
	action := NewDemoteStaleLeaderAction(cfg, fakeClient, poolerStore, ts, slog.Default())

	err := action.Execute(ctx, types.Problem{
		Code:     types.ProblemStaleLeader,
		ShardKey: shardKey,
		PoolerID: staleLeaderID,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no current leader found")
	assert.Empty(t, fakeClient.CallLog, "no RPC should be issued when there is no correct leader to demote toward")
}
