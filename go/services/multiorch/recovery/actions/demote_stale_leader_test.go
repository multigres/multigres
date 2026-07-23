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
func makeDemoteScenarioPoolers(t *testing.T, poolerStore *store.PoolerCache) (staleLeaderID *clustermetadatapb.ID) {
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

	store.SeedCache(t, poolerStore, store.NewPooler(&multiorchdatapb.PoolerHealthState{
		Multipooler: &clustermetadatapb.Multipooler{
			Id:       staleLeaderID,
			Hostname: "stale.example.com",
			PortMap:  map[string]int32{"postgres": 5432},
			ShardKey: shardKey,
			Type:     clustermetadatapb.PoolerType_PRIMARY,
		},
		Status: &multipoolermanagerdatapb.Status{
			PostgresReady: true,
		},
	}, nil))

	correctPosition := &clustermetadatapb.PoolerPosition{
		Position: &clustermetadatapb.RulePosition{Decision: &clustermetadatapb.ShardRule{
			RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 5},
			LeaderId:   correctLeaderID,
		}},
		Lsn: "0/1000",
	}
	store.SeedCache(t, poolerStore, store.NewPooler(&multiorchdatapb.PoolerHealthState{
		Multipooler: &clustermetadatapb.Multipooler{
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
	}, nil))
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

	poolerStore := store.NewTestCache(t)
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
	require.NotNil(t, req.GetReplicationPrimary().GetPrimary())
	assert.Equal(t, "correct-leader", req.GetReplicationPrimary().GetPrimary().GetId().GetName())
	assert.Equal(t, "correct.example.com", req.GetReplicationPrimary().GetPrimary().GetHost())
	require.NotNil(t, req.GetReplicationPrimary().GetPosition().GetDecision())
	assert.Equal(t, int64(5), req.GetReplicationPrimary().GetPosition().GetDecision().GetRuleNumber().GetCoordinatorTerm())
}

// TestDemoteStaleLeaderAction_ExecuteNoCorrectLeader asserts the error path
// when no other pooler in the shard claims leadership — the action must
// surface a clear error rather than calling either RPC.
func TestDemoteStaleLeaderAction_ExecuteNoCorrectLeader(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "cell1")
	defer ts.Close()

	fakeClient := rpcclient.NewFakeClient()
	poolerStore := store.NewTestCache(t)

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
	store.SeedCache(t, poolerStore, store.NewPooler(&multiorchdatapb.PoolerHealthState{
		Multipooler: &clustermetadatapb.Multipooler{
			Id:       staleLeaderID,
			ShardKey: shardKey,
			Type:     clustermetadatapb.PoolerType_PRIMARY,
		},
	}, nil))

	cfg := config.NewTestConfig()
	action := NewDemoteStaleLeaderAction(cfg, fakeClient, poolerStore, ts, slog.Default())

	err := action.Execute(ctx, types.Problem{
		Code:     types.ProblemStaleLeader,
		ShardKey: shardKey,
		PoolerID: staleLeaderID,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no consensus leader known")
	assert.Empty(t, fakeClient.CallLog, "no RPC should be issued when there is no leader to demote toward")
}

// TestDemoteStaleLeaderAction_ExecuteRewindsTowardRuleNamedLeader covers the
// rewind-source selection: it must follow the highest known rule across the shard
// (the global consensus view), not a node's local self-claim. A freshly promoted
// leader known only via a replica's replication rule must be chosen over the
// stale node's own (lower-term) self-claim.
func TestDemoteStaleLeaderAction_ExecuteRewindsTowardRuleNamedLeader(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "cell1")
	defer ts.Close()

	shardKey := &clustermetadatapb.ShardKey{Database: "testdb", TableGroup: "default", Shard: "0"}
	staleID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "stale-leader"}
	newID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "new-leader"}
	replicaID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "replica"}

	fakeClient := rpcclient.NewFakeClient()
	fakeClient.SetPrimaryResponses["multipooler-cell1-stale-leader"] = &consensusdatapb.SetPrimaryResponse{}
	ps := store.NewTestCache(t)

	// Stale leader: still self-claims term 5 and is the demote target.
	store.SeedCache(t, ps, store.NewPooler(&multiorchdatapb.PoolerHealthState{
		Multipooler:     &clustermetadatapb.Multipooler{Id: staleID, ShardKey: shardKey, Type: clustermetadatapb.PoolerType_PRIMARY},
		ConsensusStatus: selfLeaderRule(staleID, 5),
	}, nil))
	// New leader: has not published its own snapshot yet; only its address is known.
	store.SeedCache(t, ps, store.NewPooler(&multiorchdatapb.PoolerHealthState{
		Multipooler: &clustermetadatapb.Multipooler{
			Id: newID, ShardKey: shardKey, Type: clustermetadatapb.PoolerType_REPLICA,
			Hostname: "new.example.com", PortMap: map[string]int32{"postgres": 5433},
		},
	}, nil))
	// Replica replicating from the new leader at the higher term 6.
	store.SeedCache(t, ps, store.NewPooler(&multiorchdatapb.PoolerHealthState{
		Multipooler:     &clustermetadatapb.Multipooler{Id: replicaID, ShardKey: shardKey, Type: clustermetadatapb.PoolerType_REPLICA},
		ConsensusStatus: replicaFollowingRule(replicaID, newID, 6),
	}, nil))

	action := NewDemoteStaleLeaderAction(config.NewTestConfig(), fakeClient, ps, ts, slog.Default())
	require.NoError(t, action.Execute(ctx, types.Problem{
		Code:     types.ProblemStaleLeader,
		ShardKey: shardKey,
		PoolerID: staleID,
	}))

	assert.Contains(t, fakeClient.CallLog, "SetPrimary(multipooler-cell1-stale-leader)")
	req := fakeClient.SetPrimaryRequests["multipooler-cell1-stale-leader"]
	require.NotNil(t, req)
	require.NotNil(t, req.GetReplicationPrimary().GetPrimary())
	assert.Equal(t, "new-leader", req.GetReplicationPrimary().GetPrimary().GetId().GetName(),
		"rewind target must be the leader named by the replica's higher-term rule, not the stale self-claim")
	assert.Equal(t, int64(6), req.GetReplicationPrimary().GetPosition().GetDecision().GetRuleNumber().GetCoordinatorTerm())
}

// TestDemoteStaleLeaderAction_ExecuteNoOpWhenNodeIsCurrentLeader verifies that a
// spurious/already-resolved detection is a no-op: when the node flagged as stale
// is itself the highest known consensus leader, no SetPrimary RPC is issued (it
// would otherwise be self-targeted, which the pooler rejects).
func TestDemoteStaleLeaderAction_ExecuteNoOpWhenNodeIsCurrentLeader(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "cell1")
	defer ts.Close()

	shardKey := &clustermetadatapb.ShardKey{Database: "testdb", TableGroup: "default", Shard: "0"}
	nodeID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "stale-leader"}

	fakeClient := rpcclient.NewFakeClient()
	ps := store.NewTestCache(t)
	// The node multiorch flagged as stale is actually the highest known leader.
	store.SeedCache(t, ps, store.NewPooler(&multiorchdatapb.PoolerHealthState{
		Multipooler:     &clustermetadatapb.Multipooler{Id: nodeID, ShardKey: shardKey, Type: clustermetadatapb.PoolerType_PRIMARY},
		ConsensusStatus: selfLeaderRule(nodeID, 7),
	}, nil))

	action := NewDemoteStaleLeaderAction(config.NewTestConfig(), fakeClient, ps, ts, slog.Default())
	require.NoError(t, action.Execute(ctx, types.Problem{
		Code:     types.ProblemStaleLeader,
		ShardKey: shardKey,
		PoolerID: nodeID,
	}))

	assert.Empty(t, fakeClient.CallLog,
		"no SetPrimary RPC when the flagged node is actually the current consensus leader")
}

// selfLeaderRule builds a consensus status whose current position names the
// pooler itself as leader at the given coordinator term.
func selfLeaderRule(id *clustermetadatapb.ID, term int64) *clustermetadatapb.ConsensusStatus {
	return &clustermetadatapb.ConsensusStatus{
		Id: id,
		CurrentPosition: &clustermetadatapb.PoolerPosition{
			Position: &clustermetadatapb.RulePosition{Decision: &clustermetadatapb.ShardRule{
				RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: term},
				LeaderId:   id,
			}},
		},
	}
}

// replicaFollowingRule builds a follower's consensus status whose replication
// primary names leader at the given coordinator term.
func replicaFollowingRule(self, leader *clustermetadatapb.ID, term int64) *clustermetadatapb.ConsensusStatus {
	return &clustermetadatapb.ConsensusStatus{
		Id: self,
		ReplicationPrimary: &clustermetadatapb.ReplicationPrimary{
			Position: &clustermetadatapb.RulePosition{Decision: &clustermetadatapb.ShardRule{
				RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: term},
				LeaderId:   leader,
			}},
		},
	}
}
