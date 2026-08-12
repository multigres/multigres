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
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/services/multiorch/config"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
	"github.com/multigres/multigres/go/services/multiorch/store"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

func TestReconnectRecruitAbandonedAction(t *testing.T) {
	ctx := context.Background()
	shardKey := &clustermetadatapb.ShardKey{Database: "testdb", TableGroup: "default", Shard: "0"}
	replicaID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "replica1"}

	leaderMP := &clustermetadatapb.Multipooler{
		Id: fixReplPrimaryID, ShardKey: shardKey, Type: clustermetadatapb.PoolerType_PRIMARY,
		Hostname: "primary.example.com", PortMap: map[string]int32{"postgres": 5432},
	}
	replicaMP := &clustermetadatapb.Multipooler{Id: replicaID, ShardKey: shardKey, Type: clustermetadatapb.PoolerType_REPLICA}

	// followerRevocation revokes the term-1 rule (recruited at term 2, transitioning
	// away from the term-1 rule the leader still holds).
	followerRevocation := &clustermetadatapb.TermRevocation{
		RevokedBelowTerm: 2,
		OutgoingRule:     &clustermetadatapb.RuleNumber{CoordinatorTerm: 1},
	}
	ruleAt := func(term, subterm int64) *clustermetadatapb.RulePosition {
		return &clustermetadatapb.RulePosition{Decision: &clustermetadatapb.ShardRule{
			RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: term, LeaderSubterm: subterm},
			LeaderId:   fixReplPrimaryID,
		}}
	}

	// seed builds a cache with the leader at leaderTerm and the stranded follower.
	seed := func(t *testing.T, leaderPos *clustermetadatapb.PoolerPosition) *store.PoolerCache {
		cache := store.NewTestCache(t)
		store.SeedCache(t, cache, store.NewPooler(&multiorchdatapb.PoolerHealthState{
			Multipooler:      leaderMP,
			IsLastCheckValid: true,
			Status:           &multipoolermanagerdatapb.Status{PostgresReady: true},
			ConsensusStatus:  &clustermetadatapb.ConsensusStatus{Id: fixReplPrimaryID, CurrentPosition: leaderPos},
		}, nil))
		store.SeedCache(t, cache, store.NewPooler(&multiorchdatapb.PoolerHealthState{
			Multipooler:      replicaMP,
			IsLastCheckValid: true,
			ConsensusStatus: &clustermetadatapb.ConsensusStatus{
				Id:              replicaID,
				CurrentPosition: &clustermetadatapb.PoolerPosition{Position: ruleAt(1, 0)},
				TermRevocation:  followerRevocation,
			},
		}, nil))
		return cache
	}

	problem := types.Problem{
		Code:     types.ProblemReplicaRecruitAbandoned,
		ShardKey: shardKey,
		PoolerID: replicaID,
	}

	t.Run("advances the rule then reconnects the follower", func(t *testing.T) {
		fake := rpcclient.NewFakeClient()
		// The advance returns the rule at a fresh subterm (1.1), which outranks the
		// follower's revocation outgoing_rule.
		fake.UpdateConsensusRuleResponses["multipooler-cell1-primary"] = &multipoolermanagerdatapb.UpdateConsensusRuleResponse{
			CurrentPosition: &clustermetadatapb.PoolerPosition{Position: ruleAt(1, 1)},
		}

		action := NewReconnectRecruitAbandonedAction(config.NewTestConfig(), fake, seed(t, leaderCurrentPosition(1)), slog.Default())

		require.NoError(t, action.Execute(ctx, problem))
		assert.Contains(t, fake.CallLog, "UpdateConsensusRule(multipooler-cell1-primary)",
			"must advance the rule on the leader")
		assert.Contains(t, fake.CallLog, "SetPrimary(multipooler-cell1-replica1)",
			"must reconnect the follower after advancing")
		// The position relayed to the follower is the one the advance returned.
		relayed := fake.SetPrimaryRequests["multipooler-cell1-replica1"].GetReplicationPrimary().GetPosition()
		assert.EqualValues(t, 1, relayed.GetDecision().GetRuleNumber().GetLeaderSubterm(),
			"must relay the advanced rule the RPC returned")
	})

	t.Run("skips the advance when the rule already outranks the revocation", func(t *testing.T) {
		fake := rpcclient.NewFakeClient()
		// Leader already at term 2: the highest known rule is not revoked by the
		// follower's revocation, so no advance is needed — just reconnect.
		action := NewReconnectRecruitAbandonedAction(config.NewTestConfig(), fake, seed(t, leaderCurrentPosition(2)), slog.Default())

		require.NoError(t, action.Execute(ctx, problem))
		assert.NotContains(t, fake.CallLog, "UpdateConsensusRule(multipooler-cell1-primary)",
			"must not advance when the rule already outranks the revocation")
		assert.Contains(t, fake.CallLog, "SetPrimary(multipooler-cell1-replica1)",
			"must still reconnect the follower")
	})

	t.Run("defers when the highest rule has an undecided proposal", func(t *testing.T) {
		fake := rpcclient.NewFakeClient()
		// The leader's highest known rule is revoked but mid-transition (an
		// outstanding proposal). The leader CAS-guards the advance on the decided
		// outgoing rule, so we must wait rather than advance.
		undecided := &clustermetadatapb.PoolerPosition{Position: &clustermetadatapb.RulePosition{
			Decision: &clustermetadatapb.ShardRule{
				RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 1},
				LeaderId:   fixReplPrimaryID,
			},
			Proposal: &clustermetadatapb.ShardRule{
				RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 1, LeaderSubterm: 1},
				LeaderId:   fixReplPrimaryID,
			},
		}}
		action := NewReconnectRecruitAbandonedAction(config.NewTestConfig(), fake, seed(t, undecided), slog.Default())

		err := action.Execute(ctx, problem)
		require.Error(t, err)
		assert.Equal(t, mtrpcpb.Code_FAILED_PRECONDITION, mterrors.Code(err))
		assert.Contains(t, err.Error(), "undecided proposal")
		assert.NotContains(t, fake.CallLog, "UpdateConsensusRule(multipooler-cell1-primary)",
			"must not advance while a proposal is undecided")
	})

	t.Run("rejects an advance that did not clear the revocation", func(t *testing.T) {
		fake := rpcclient.NewFakeClient()
		// The advance returns a position still at the revoked rule (1.0), which the
		// follower would still reject, so the action refuses to relay it.
		fake.UpdateConsensusRuleResponses["multipooler-cell1-primary"] = &multipoolermanagerdatapb.UpdateConsensusRuleResponse{
			CurrentPosition: &clustermetadatapb.PoolerPosition{Position: ruleAt(1, 0)},
		}
		action := NewReconnectRecruitAbandonedAction(config.NewTestConfig(), fake, seed(t, leaderCurrentPosition(1)), slog.Default())

		err := action.Execute(ctx, problem)
		require.Error(t, err)
		assert.Equal(t, mtrpcpb.Code_INTERNAL, mterrors.Code(err))
		assert.Contains(t, err.Error(), "still short of the follower's revocation")
		assert.Contains(t, fake.CallLog, "UpdateConsensusRule(multipooler-cell1-primary)")
		assert.NotContains(t, fake.CallLog, "SetPrimary(multipooler-cell1-replica1)",
			"must not reconnect when the advance did not clear the revocation")
	})

	t.Run("wraps a failed leader-led advance", func(t *testing.T) {
		fake := rpcclient.NewFakeClient()
		fake.Errors["multipooler-cell1-primary"] = errors.New("rpc boom")

		action := NewReconnectRecruitAbandonedAction(config.NewTestConfig(), fake, seed(t, leaderCurrentPosition(1)), slog.Default())

		err := action.Execute(ctx, problem)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "leader-led rule advance failed")
		assert.Contains(t, err.Error(), "rpc boom")
	})

	t.Run("wraps a failed reconnect SetPrimary", func(t *testing.T) {
		fake := rpcclient.NewFakeClient()
		fake.Errors["multipooler-cell1-replica1"] = errors.New("rpc boom")

		// Leader already at term 2: no advance needed, so Execute proceeds straight
		// to the failing SetPrimary.
		action := NewReconnectRecruitAbandonedAction(config.NewTestConfig(), fake, seed(t, leaderCurrentPosition(2)), slog.Default())

		err := action.Execute(ctx, problem)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "SetPrimary to reconnect stranded follower failed")
		assert.Contains(t, err.Error(), "rpc boom")
	})
}
