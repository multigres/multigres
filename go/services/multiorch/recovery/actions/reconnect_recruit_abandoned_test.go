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
	"github.com/multigres/multigres/go/services/multiorch/config"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
	"github.com/multigres/multigres/go/services/multiorch/store"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
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
		// After the advance, the leader reports its committed rule at a fresh
		// subterm (1.1), which outranks the follower's revocation outgoing_rule.
		fake.SetStatusResponse("multipooler-cell1-primary", &multipoolermanagerdatapb.StatusResponse{
			ConsensusStatus: &clustermetadatapb.ConsensusStatus{
				Id:              fixReplPrimaryID,
				CurrentPosition: &clustermetadatapb.PoolerPosition{Position: ruleAt(1, 1)},
			},
		})

		action := NewReconnectRecruitAbandonedAction(config.NewTestConfig(), fake, seed(t, leaderCurrentPosition(1)), slog.Default())
		action.verifyPollInterval = 10 * time.Millisecond

		require.NoError(t, action.Execute(ctx, problem))
		assert.Contains(t, fake.CallLog, "UpdateConsensusRule(multipooler-cell1-primary)",
			"must advance the rule on the leader")
		assert.Contains(t, fake.CallLog, "SetPrimary(multipooler-cell1-replica1)",
			"must reconnect the follower after advancing")
	})

	t.Run("skips the advance when the rule already outranks the revocation", func(t *testing.T) {
		fake := rpcclient.NewFakeClient()
		// Leader already at term 2: the highest known rule is not revoked by the
		// follower's revocation, so no advance is needed — just reconnect.
		action := NewReconnectRecruitAbandonedAction(config.NewTestConfig(), fake, seed(t, leaderCurrentPosition(2)), slog.Default())
		action.verifyPollInterval = 10 * time.Millisecond

		require.NoError(t, action.Execute(ctx, problem))
		assert.NotContains(t, fake.CallLog, "UpdateConsensusRule(multipooler-cell1-primary)",
			"must not advance when the rule already outranks the revocation")
		assert.Contains(t, fake.CallLog, "SetPrimary(multipooler-cell1-replica1)",
			"must still reconnect the follower")
	})
}
