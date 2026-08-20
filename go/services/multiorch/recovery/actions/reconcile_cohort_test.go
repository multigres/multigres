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
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
	"github.com/multigres/multigres/go/services/multiorch/store"
)

func TestReconcileCohortAction_Metadata(t *testing.T) {
	action := NewReconcileCohortAction(nil, nil, nil, nil, slog.Default())
	md := action.Metadata()
	assert.Equal(t, "ReconcileCohort", md.Name)
	assert.True(t, md.Retryable)
	assert.True(t, action.RequiresHealthyLeader())
	assert.Nil(t, action.GracePeriod())
}

func TestReconcileCohortAction_Execute(t *testing.T) {
	ctx := context.Background()
	primaryID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "primary"}
	replicaID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "replica1"}
	shardKey := &clustermetadatapb.ShardKey{Database: "testdb", TableGroup: "default", Shard: "0"}

	// rule is the highest known consensus position setupStore's primary
	// publishes. Tests pass it as RecheckedProblem.HighestKnownRule to stand
	// in for what the engine's recheck would have supplied — Execute no
	// longer derives it from the store itself.
	rule := &clustermetadatapb.RulePosition{Decision: &clustermetadatapb.ShardRule{
		RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 3, LeaderSubterm: 7},
		LeaderId:   primaryID,
	}}

	setupStore := func(t *testing.T, fakeClient *rpcclient.FakeClient) (*store.PoolerCache, func()) {
		t.Helper()
		ts, _ := memorytopo.NewServerAndFactory(ctx, "cell1")
		ps := store.NewTestCache(t)
		store.SeedCache(t, ps, store.NewPooler(&multiorchdatapb.PoolerHealthState{
			Multipooler: &clustermetadatapb.Multipooler{
				Id:       primaryID,
				ShardKey: shardKey,
				Type:     clustermetadatapb.PoolerType_PRIMARY,
				Hostname: "primary.example.com",
				PortMap:  map[string]int32{"postgres": 5432},
			},
			LastSeen: timestamppb.Now(),
			Status:   &multipoolermanagerdatapb.Status{PostgresStatus: multipoolermanagerdatapb.PostgresStatus_POSTGRES_STATUS_PRIMARY},
			ConsensusStatus: &clustermetadatapb.ConsensusStatus{
				Id:              primaryID,
				TermRevocation:  &clustermetadatapb.TermRevocation{RevokedBelowTerm: 3},
				CurrentPosition: &clustermetadatapb.PoolerPosition{Position: rule},
			},
		}, nil))
		store.SeedCache(t, ps, store.NewPooler(&multiorchdatapb.PoolerHealthState{
			Multipooler: &clustermetadatapb.Multipooler{
				Id:       replicaID,
				ShardKey: shardKey,
				Type:     clustermetadatapb.PoolerType_REPLICA,
			},
			ConsensusStatus: &clustermetadatapb.ConsensusStatus{
				TermRevocation: &clustermetadatapb.TermRevocation{RevokedBelowTerm: 3},
			},
		}, nil))
		return ps, func() { _ = ts.Close() }
	}

	t.Run("ProblemPoolerNotInCohort issues UpdateConsensusRule with ADD", func(t *testing.T) {
		fakeClient := &rpcclient.FakeClient{
			StatusResponses: map[topoclient.ComponentID]*rpcclient.ResponseWithDelay[*multipoolermanagerdatapb.StatusResponse]{
				"multipooler-cell1-primary": {Response: &multipoolermanagerdatapb.StatusResponse{
					Status:          &multipoolermanagerdatapb.Status{IsInitialized: true, PoolerType: clustermetadatapb.PoolerType_PRIMARY},
					ConsensusStatus: selfLeaderConsensus(primaryID),
				}},
			},
			UpdateConsensusRuleResponses: map[topoclient.ComponentID]*multipoolermanagerdatapb.UpdateConsensusRuleResponse{
				"multipooler-cell1-primary": {},
			},
		}
		ps, cleanup := setupStore(t, fakeClient)
		defer cleanup()

		action := NewReconcileCohortAction(nil, fakeClient, ps, nil, slog.Default())
		err := action.Execute(ctx, types.RecheckedProblem{Problem: types.Problem{
			Code:     types.ProblemPoolerNotInCohort,
			ShardKey: shardKey,
			PoolerID: replicaID,
		}, HighestKnownRule: rule})

		require.NoError(t, err)
		assert.Contains(t, fakeClient.CallLog, "UpdateConsensusRule(multipooler-cell1-primary)")
		req := fakeClient.LastUpdateConsensusRuleRequest
		require.NotNil(t, req)
		assert.Equal(t, multipoolermanagerdatapb.RuleOperation_RULE_OPERATION_COHORT_ADD, req.Operation)
		require.Len(t, req.StandbyIds, 1)
		assert.Equal(t, replicaID.Name, req.StandbyIds[0].Name)
		require.NotNil(t, req.ExpectedOutgoingRule, "CAS guard must be set")
		assert.Equal(t, int64(3), req.ExpectedOutgoingRule.CoordinatorTerm)
		assert.Equal(t, int64(7), req.ExpectedOutgoingRule.LeaderSubterm)

		// After recording membership on the leader, the action re-issues
		// SetPrimary to the joining member so it clears restore_command
		// synchronously — a member that joins an already-established cohort
		// out-of-band never went through Recruit's clear. The rule relayed is
		// the leader's post-ADD rule (re-read from the leader's status).
		assert.Contains(t, fakeClient.CallLog, "Status(multipooler-cell1-primary)")
		assert.Contains(t, fakeClient.CallLog, "SetPrimary(multipooler-cell1-replica1)")
		setReq := fakeClient.SetPrimaryRequests["multipooler-cell1-replica1"]
		require.NotNil(t, setReq)
		assert.Equal(t, primaryID.Name, setReq.GetReplicationPrimary().GetPrimary().GetId().GetName())
	})

	t.Run("ProblemCohortMemberIneligible issues UpdateConsensusRule with REMOVE", func(t *testing.T) {
		// Removal safety (durability-policy quorum survival) is enforced by
		// CohortMismatchAnalyzer before this problem is ever produced — see
		// cohort_mismatch_analyzer_test.go — and re-verified by construction
		// via the RecheckedProblem the engine passes in (Execute trusts it
		// rather than re-deriving safety here). This test only exercises the
		// REMOVE plumbing itself.
		fakeClient := &rpcclient.FakeClient{
			StatusResponses: map[topoclient.ComponentID]*rpcclient.ResponseWithDelay[*multipoolermanagerdatapb.StatusResponse]{
				"multipooler-cell1-primary": {Response: &multipoolermanagerdatapb.StatusResponse{
					Status:          &multipoolermanagerdatapb.Status{IsInitialized: true, PoolerType: clustermetadatapb.PoolerType_PRIMARY},
					ConsensusStatus: selfLeaderConsensus(primaryID),
				}},
			},
			UpdateConsensusRuleResponses: map[topoclient.ComponentID]*multipoolermanagerdatapb.UpdateConsensusRuleResponse{
				"multipooler-cell1-primary": {},
			},
		}
		ps, cleanup := setupStore(t, fakeClient)
		defer cleanup()

		action := NewReconcileCohortAction(nil, fakeClient, ps, nil, slog.Default())
		err := action.Execute(ctx, types.RecheckedProblem{Problem: types.Problem{
			Code:     types.ProblemCohortMemberIneligible,
			ShardKey: shardKey,
			PoolerID: replicaID,
		}, HighestKnownRule: rule})

		require.NoError(t, err)
		assert.Contains(t, fakeClient.CallLog, "UpdateConsensusRule(multipooler-cell1-primary)")
		req := fakeClient.LastUpdateConsensusRuleRequest
		require.NotNil(t, req)
		assert.Equal(t, multipoolermanagerdatapb.RuleOperation_RULE_OPERATION_COHORT_REMOVE, req.Operation)
	})

	t.Run("returns error when target pooler is not in store", func(t *testing.T) {
		fakeClient := &rpcclient.FakeClient{}
		ts, _ := memorytopo.NewServerAndFactory(ctx, "cell1")
		defer ts.Close()
		ps := store.NewTestCache(t)
		// No poolers added to the store — FindPoolerByID will fail.

		action := NewReconcileCohortAction(nil, fakeClient, ps, nil, slog.Default())
		err := action.Execute(ctx, types.RecheckedProblem{Problem: types.Problem{
			Code:     types.ProblemPoolerNotInCohort,
			ShardKey: shardKey,
			PoolerID: replicaID,
		}, HighestKnownRule: rule})

		require.Error(t, err)
		assert.Contains(t, err.Error(), "find target pooler")
		assert.NotContains(t, fakeClient.CallLog, "UpdateConsensusRule(multipooler-cell1-primary)")
	})

	t.Run("returns error when no consensus rule is known for the shard", func(t *testing.T) {
		// HighestKnownRule is nil — the engine's recheck found no rule for the
		// shard (e.g. no poolers ever reported one). Execute trusts that
		// verdict directly rather than re-deriving it from the store.
		fakeClient := &rpcclient.FakeClient{}
		ts, _ := memorytopo.NewServerAndFactory(ctx, "cell1")
		defer ts.Close()
		ps := store.NewTestCache(t)

		action := NewReconcileCohortAction(nil, fakeClient, ps, nil, slog.Default())
		err := action.Execute(ctx, types.RecheckedProblem{Problem: types.Problem{
			Code:     types.ProblemPoolerNotInCohort,
			ShardKey: shardKey,
			PoolerID: replicaID,
		}})

		require.Error(t, err)
		assert.Contains(t, err.Error(), "no consensus rule known")
	})

	t.Run("rejects when the leader does not look able to commit writes", func(t *testing.T) {
		// A stale health observation fails LeaderWritesProgressing before any
		// RPC is attempted — distinct from the CAS rejection case below, which
		// covers the RPC itself failing against an otherwise-healthy leader.
		fakeClient := &rpcclient.FakeClient{}
		ts, _ := memorytopo.NewServerAndFactory(ctx, "cell1")
		defer ts.Close()
		ps := store.NewTestCache(t)
		store.SeedCache(t, ps, store.NewPooler(&multiorchdatapb.PoolerHealthState{
			Multipooler: &clustermetadatapb.Multipooler{
				Id:       primaryID,
				ShardKey: shardKey,
				Type:     clustermetadatapb.PoolerType_PRIMARY,
				Hostname: "primary.example.com",
				PortMap:  map[string]int32{"postgres": 5432},
			},
			LastSeen:        timestamppb.New(time.Now().Add(-time.Hour)),
			Status:          &multipoolermanagerdatapb.Status{PostgresStatus: multipoolermanagerdatapb.PostgresStatus_POSTGRES_STATUS_PRIMARY},
			ConsensusStatus: &clustermetadatapb.ConsensusStatus{Id: primaryID, TermRevocation: &clustermetadatapb.TermRevocation{RevokedBelowTerm: 3}, CurrentPosition: &clustermetadatapb.PoolerPosition{Position: rule}},
		}, nil))
		store.SeedCache(t, ps, store.NewPooler(&multiorchdatapb.PoolerHealthState{
			Multipooler: &clustermetadatapb.Multipooler{
				Id:       replicaID,
				ShardKey: shardKey,
				Type:     clustermetadatapb.PoolerType_REPLICA,
			},
		}, nil))

		action := NewReconcileCohortAction(nil, fakeClient, ps, nil, slog.Default())
		err := action.Execute(ctx, types.RecheckedProblem{Problem: types.Problem{
			Code:     types.ProblemPoolerNotInCohort,
			ShardKey: shardKey,
			PoolerID: replicaID,
		}, HighestKnownRule: rule})

		require.Error(t, err)
		assert.Contains(t, err.Error(), "does not look able to commit writes right now")
		assert.Empty(t, fakeClient.CallLog, "no RPC should be dispatched")
	})

	t.Run("surfaces an UpdateConsensusRule failure (e.g. stale-leader CAS rejection)", func(t *testing.T) {
		// setupStore's leader passes LeaderWritesProgressing, so the action still
		// issues the CAS-fenced UpdateConsensusRule; this covers the RPC itself
		// rejecting a stale write. A failure is surfaced so the engine retries
		// next cycle with a fresh view.
		fakeClient := &rpcclient.FakeClient{
			Errors: map[topoclient.ComponentID]error{
				"multipooler-cell1-primary": errors.New("rule CAS rejected"),
			},
		}
		ps, cleanup := setupStore(t, fakeClient)
		defer cleanup()

		action := NewReconcileCohortAction(nil, fakeClient, ps, nil, slog.Default())
		err := action.Execute(ctx, types.RecheckedProblem{Problem: types.Problem{
			Code:     types.ProblemPoolerNotInCohort,
			ShardKey: shardKey,
			PoolerID: replicaID,
		}, HighestKnownRule: rule})

		require.Error(t, err)
		assert.Contains(t, err.Error(), "UpdateConsensusRule failed")
		assert.Contains(t, fakeClient.CallLog, "UpdateConsensusRule(multipooler-cell1-primary)")
	})

	t.Run("rejects when the highest known rule has an undecided proposal", func(t *testing.T) {
		// Propagation isn't supported yet: a cohort change is leader-led and
		// meaningless against an outstanding, not-yet-decided proposal.
		undecided := &clustermetadatapb.RulePosition{
			Decision: &clustermetadatapb.ShardRule{
				RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 3, LeaderSubterm: 7},
				LeaderId:   primaryID,
			},
			Proposal: &clustermetadatapb.ShardRule{
				RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 3, LeaderSubterm: 8},
				LeaderId:   primaryID,
			},
		}
		fakeClient := &rpcclient.FakeClient{}
		ps, cleanup := setupStore(t, fakeClient)
		defer cleanup()

		action := NewReconcileCohortAction(nil, fakeClient, ps, nil, slog.Default())
		err := action.Execute(ctx, types.RecheckedProblem{Problem: types.Problem{
			Code:     types.ProblemPoolerNotInCohort,
			ShardKey: shardKey,
			PoolerID: replicaID,
		}, HighestKnownRule: undecided})

		require.Error(t, err)
		assert.Contains(t, err.Error(), "undecided rule proposal")
		assert.Empty(t, fakeClient.CallLog, "no RPC should be dispatched")
	})

	t.Run("rejects unsupported problem code", func(t *testing.T) {
		fakeClient := &rpcclient.FakeClient{
			StatusResponses: map[topoclient.ComponentID]*rpcclient.ResponseWithDelay[*multipoolermanagerdatapb.StatusResponse]{
				"multipooler-cell1-primary": {Response: &multipoolermanagerdatapb.StatusResponse{
					Status:          &multipoolermanagerdatapb.Status{IsInitialized: true, PoolerType: clustermetadatapb.PoolerType_PRIMARY},
					ConsensusStatus: selfLeaderConsensus(primaryID),
				}},
			},
		}
		ps, cleanup := setupStore(t, fakeClient)
		defer cleanup()

		action := NewReconcileCohortAction(nil, fakeClient, ps, nil, slog.Default())
		err := action.Execute(ctx, types.RecheckedProblem{Problem: types.Problem{
			Code:     types.ProblemReplicaNotReplicating,
			ShardKey: shardKey,
			PoolerID: replicaID,
		}, HighestKnownRule: rule})

		require.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported problem code")
		assert.NotContains(t, fakeClient.CallLog, "UpdateConsensusRule(multipooler-cell1-primary)")
	})
}

// selfLeaderConsensus builds a consensus status in which the pooler names itself
// as the consensus leader (so commonconsensus.HighestKnownRule/IsLeader identify
// it) without a recorded rule number.
func selfLeaderConsensus(id *clustermetadatapb.ID) *clustermetadatapb.ConsensusStatus {
	return &clustermetadatapb.ConsensusStatus{
		Id: id,
		CurrentPosition: &clustermetadatapb.PoolerPosition{
			Position: &clustermetadatapb.RulePosition{Decision: &clustermetadatapb.ShardRule{LeaderId: id}},
		},
	}
}
