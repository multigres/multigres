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

package consensus

import (
	"context"
	"log/slog"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonconsensus "github.com/multigres/multigres/go/common/consensus"
	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// createMockNode creates a mock pooler for testing using FakeClient.
// rule is the current shard rule shared by all nodes in the cluster (nil if no leader exists).
func createMockNode(fakeClient *rpcclient.FakeClient, name string, term int64, walPosition string, healthy bool, rule *clustermetadatapb.ShardRule) *multiorchdatapb.PoolerHealthState {
	poolerID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      name,
	}

	pooler := &clustermetadatapb.Multipooler{
		Id:       poolerID,
		Hostname: "localhost",
		PortMap: map[string]int32{
			"grpc": 9000,
		},
	}

	poolerKey := topoclient.ComponentIDString(poolerID)

	fakeClient.SetStatusResponse(poolerKey, &multipoolermanagerdatapb.StatusResponse{
		ConsensusStatus: &clustermetadatapb.ConsensusStatus{
			Id: poolerID,
			TermRevocation: &clustermetadatapb.TermRevocation{
				RevokedBelowTerm: term,
			},
			CurrentPosition: &clustermetadatapb.PoolerPosition{
				Lsn:      walPosition,
				Position: &clustermetadatapb.RulePosition{Decision: rule},
			},
		},
	})

	var consensusTerm *clustermetadatapb.TermRevocation
	if term > 0 {
		consensusTerm = &clustermetadatapb.TermRevocation{
			RevokedBelowTerm: term,
		}
	}

	healthState := &multiorchdatapb.PoolerHealthState{
		Multipooler:      pooler,
		IsLastCheckValid: healthy,
		ConsensusStatus:  &clustermetadatapb.ConsensusStatus{TermRevocation: consensusTerm},
		Status: &multipoolermanagerdatapb.Status{
			IsInitialized:   term > 0,
			PostgresRunning: healthy,
		},
	}
	if healthy {
		healthState.LastSeen = timestamppb.Now()
	}
	return healthState
}

func TestAppointLeader(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn}))
	coordID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIORCH,
		Cell:      "test-cell",
		Name:      "test-coordinator",
	}

	fakeClient := rpcclient.NewFakeClient()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	c := NewCoordinator(coordID, ts, fakeClient, logger)

	// Build the committed rule shared by all nodes. Coordinator term 5 means
	// the new revocation will be at term 6.
	cohortIDs := []*clustermetadatapb.ID{
		{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "mp1"},
		{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "mp2"},
		{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "mp3"},
	}
	// AT_LEAST_3 forces tryBuildProposal to wait for all three recruits before
	// forming quorum, making leader selection deterministic regardless of
	// recruit-RPC completion order.
	outgoingRule := &clustermetadatapb.ShardRule{
		RuleNumber:       &clustermetadatapb.RuleNumber{CoordinatorTerm: 5},
		LeaderId:         cohortIDs[0],
		CohortMembers:    cohortIDs,
		DurabilityPolicy: topoclient.AtLeastN(3),
	}

	// mp1 has the highest LSN, so failover should pick it as leader.
	walPositions := []string{"0/3000000", "0/2000000", "0/1000000"}
	cohort := make([]*multiorchdatapb.PoolerHealthState, 0, len(cohortIDs))
	for i, id := range cohortIDs {
		mp := createMockNode(fakeClient, id.Name, 5, walPositions[i], true, outgoingRule)
		// Pre-vote runs over cached cohort statuses (not Recruit responses), so
		// we need an Id and a populated CurrentPosition with the committed rule
		// here too. createMockNode leaves these fields zero on the cached status.
		mp.ConsensusStatus.Id = id
		mp.ConsensusStatus.CurrentPosition = &clustermetadatapb.PoolerPosition{
			Lsn:      walPositions[i],
			Position: &clustermetadatapb.RulePosition{Decision: outgoingRule},
		}
		key := topoclient.ComponentIDString(id)
		fakeClient.RecruitResponses[key] = &consensusdatapb.RecruitResponse{
			ConsensusStatus: &clustermetadatapb.ConsensusStatus{
				Id: id,
				CurrentPosition: &clustermetadatapb.PoolerPosition{
					Lsn:      walPositions[i],
					Position: &clustermetadatapb.RulePosition{Decision: outgoingRule},
				},
			},
		}
		require.NoError(t, ts.CreateMultipooler(ctx, mp.Multipooler))
		cohort = append(cohort, mp)
	}

	shardKey := &clustermetadatapb.ShardKey{Database: "testdb", TableGroup: "default", Shard: "shard0"}
	require.NoError(t, c.AppointLeader(ctx, shardKey, cohort, "test_failover"))

	// The designated leader (mp1) should receive a Promote with the full
	// CoordinatorProposal.
	leaderKey := topoclient.ComponentIDString(cohortIDs[0])
	propReq, ok := fakeClient.PromoteRequests[leaderKey]
	require.True(t, ok, "Promote should be sent to designated leader %s", cohortIDs[0].Name)
	require.NotNil(t, propReq.GetProposal())
	require.Equal(t, "test_failover", propReq.GetReason())
	require.Equal(t, "mp1", propReq.GetProposal().GetProposalLeader().GetId().GetName(),
		"failover should pick mp1 (highest LSN) as leader")
	require.Equal(t, int64(6), propReq.GetProposal().GetTermRevocation().GetRevokedBelowTerm(),
		"revocation term should be max prior term (5) + 1")

	// Followers should receive SetPrimary carrying the same leader + rule
	// (no Promote).
	for _, id := range cohortIDs[1:] {
		key := topoclient.ComponentIDString(id)
		_, isPromote := fakeClient.PromoteRequests[key]
		require.False(t, isPromote, "Promote should NOT be sent to follower %s", id.Name)
		stp, ok := fakeClient.SetPrimaryRequests[key]
		require.True(t, ok, "SetPrimary should be sent to %s", id.Name)
		require.Equal(t, "mp1", stp.GetReplicationPrimary().GetPrimary().GetId().GetName(),
			"follower %s should be informed of mp1 as leader", id.Name)
		require.Equal(t, int64(6), commonconsensus.PossiblyUndecidedRule(stp.GetReplicationPrimary().GetPosition()).GetRuleNumber().GetCoordinatorTerm())
	}
}

// TestAppointLeader_PropagatesUndecidedMostAdvancedPosition confirms that
// normal failover (requireOutgoingQuorum) trusts the cohort's most-advanced
// position as the outgoing baseline even when it's an undecided proposal —
// no separate quorum-verification step is needed, since the fresh write
// this promotion performs can't get its own synchronous ack unless the
// position it supersedes was already durable. mp1 is both the only cohort
// member and the only node reporting the undecided proposal, so recruitment
// trivially succeeds; the resulting rule keeps mp1 as leader (propagation
// never changes cohort or durability policy — see validateProposal).
func TestAppointLeader_PropagatesUndecidedMostAdvancedPosition(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn}))
	coordID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIORCH,
		Cell:      "test-cell",
		Name:      "test-coordinator",
	}

	fakeClient := rpcclient.NewFakeClient()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	c := NewCoordinator(coordID, ts, fakeClient, logger)

	mpID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "mp1"}
	oldRule := &clustermetadatapb.ShardRule{
		RuleNumber:       &clustermetadatapb.RuleNumber{CoordinatorTerm: 3},
		LeaderId:         mpID,
		CohortMembers:    []*clustermetadatapb.ID{mpID},
		DurabilityPolicy: topoclient.AtLeastN(1),
	}
	undecidedProposal := &clustermetadatapb.ShardRule{
		RuleNumber:       &clustermetadatapb.RuleNumber{CoordinatorTerm: 4},
		LeaderId:         mpID,
		CohortMembers:    []*clustermetadatapb.ID{mpID},
		DurabilityPolicy: topoclient.AtLeastN(1),
	}

	mp := createMockNode(fakeClient, "mp1", 3, "0/1000000", true, oldRule)
	mp.ConsensusStatus.Id = mpID
	mp.ConsensusStatus.CurrentPosition = &clustermetadatapb.PoolerPosition{
		Lsn:      "0/1000000",
		Position: &clustermetadatapb.RulePosition{Decision: oldRule, Proposal: undecidedProposal},
	}
	require.NoError(t, ts.CreateMultipooler(ctx, mp.Multipooler))

	fakeClient.RecruitResponses[topoclient.ComponentIDString(mpID)] = &consensusdatapb.RecruitResponse{
		ConsensusStatus: &clustermetadatapb.ConsensusStatus{
			Id: mpID,
			CurrentPosition: &clustermetadatapb.PoolerPosition{
				Lsn:      "0/1000000",
				Position: &clustermetadatapb.RulePosition{Decision: oldRule, Proposal: undecidedProposal},
			},
		},
	}

	shardKey := &clustermetadatapb.ShardKey{Database: "testdb", TableGroup: "default", Shard: "shard0"}
	require.NoError(t, c.AppointLeader(ctx, shardKey, []*multiorchdatapb.PoolerHealthState{mp}, "test_failover"))

	leaderKey := topoclient.ComponentIDString(mpID)
	propReq, ok := fakeClient.PromoteRequests[leaderKey]
	require.True(t, ok, "Promote should be sent to mp1")
	require.NotNil(t, propReq.GetProposal())
	require.Equal(t, "mp1", propReq.GetProposal().GetProposalLeader().GetId().GetName())
	require.Equal(t, int64(5), propReq.GetProposal().GetTermRevocation().GetRevokedBelowTerm(),
		"revocation term should exceed the undecided proposal's term (4) by one")
	proposedTransition := propReq.GetProposal().GetProposedTransition()
	require.Equal(t, int64(4), proposedTransition.GetDecision().GetRuleNumber().GetCoordinatorTerm(),
		"the undecided proposal is trusted as the outgoing decision")
	require.ElementsMatch(t, []*clustermetadatapb.ID{mpID}, proposedTransition.GetProposal().GetCohortMembers(),
		"propagation preserves the outgoing cohort — only the leader is re-proposed")
}

func TestAppointInitialLeader(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn}))
	coordID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIORCH,
		Cell:      "test-cell",
		Name:      "test-coordinator",
	}

	fakeClient := rpcclient.NewFakeClient()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	require.NoError(t, ts.CreateDatabase(ctx, "testdb", &clustermetadatapb.Database{
		Name:                      "testdb",
		BootstrapDurabilityPolicy: topoclient.AtLeastN(3),
	}))

	c := NewCoordinator(coordID, ts, fakeClient, logger)

	// AT_LEAST_3 forces tryBuild to wait for all three recruits before
	// quorum forms, making leader selection deterministic regardless of
	// recruit-RPC completion order.
	cohortIDs := []*clustermetadatapb.ID{
		{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "mp1"},
		{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "mp2"},
		{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "mp3"},
	}
	// mp1 has the highest LSN; bootstrap should pick it as leader. Each node
	// carries the sentinel rule that createSidecarSchema writes during db
	// init: term 0 / subterm 1, empty cohort, but a real durability policy.
	// {0,0} is reserved codebase-wide as the "no rule recorded" sentinel, so
	// the initial row is never actually zero. AppointInitialLeader requires
	// a rule to derive the cert from.
	sentinelRule := &clustermetadatapb.ShardRule{
		RuleNumber:       &clustermetadatapb.RuleNumber{LeaderSubterm: 1},
		DurabilityPolicy: topoclient.AtLeastN(3),
	}
	walPositions := []string{"0/2000000", "0/1500000", "0/1000000"}
	cohort := make([]*multiorchdatapb.PoolerHealthState, 0, len(cohortIDs))
	for i, id := range cohortIDs {
		mp := createMockNode(fakeClient, id.Name, 0, walPositions[i], true, sentinelRule)
		mp.Status.IsInitialized = true
		mp.Status.PostgresReady = true
		mp.Status.PostgresRunning = true
		// Cached cohort statuses populate Id and CurrentPosition (with sentinel
		// rule) so MostAdvancedPosition can derive the cert and pre-vote can
		// run on real positions. Fresh nodes carry no prior term revocation.
		mp.ConsensusStatus = &clustermetadatapb.ConsensusStatus{
			Id: id,
			CurrentPosition: &clustermetadatapb.PoolerPosition{
				Lsn:      walPositions[i],
				Position: &clustermetadatapb.RulePosition{Decision: sentinelRule},
			},
		}
		key := topoclient.ComponentIDString(id)
		fakeClient.RecruitResponses[key] = &consensusdatapb.RecruitResponse{
			ConsensusStatus: &clustermetadatapb.ConsensusStatus{
				Id: id,
				CurrentPosition: &clustermetadatapb.PoolerPosition{
					Lsn:      walPositions[i],
					Position: &clustermetadatapb.RulePosition{Decision: sentinelRule},
				},
			},
		}
		require.NoError(t, ts.CreateMultipooler(ctx, mp.Multipooler))
		cohort = append(cohort, mp)
	}

	shardKey := &clustermetadatapb.ShardKey{Database: "testdb", TableGroup: "default", Shard: "shard0"}
	require.NoError(t, c.AppointInitialLeader(ctx, shardKey, cohort))

	// The designated leader (mp1) should receive a Promote carrying the
	// bootstrap proposal.
	leaderKey := topoclient.ComponentIDString(cohortIDs[0])
	propReq, ok := fakeClient.PromoteRequests[leaderKey]
	require.True(t, ok, "Promote should be sent to designated leader %s", cohortIDs[0].Name)
	require.NotNil(t, propReq.GetProposal())
	require.Equal(t, "ShardInit", propReq.GetReason())
	require.Equal(t, "mp1", propReq.GetProposal().GetProposalLeader().GetId().GetName(),
		"bootstrap should pick mp1 (highest LSN) as leader")
	// Fresh cluster: max prior term is 0, so the new revocation is term 1.
	require.Equal(t, int64(1), propReq.GetProposal().GetTermRevocation().GetRevokedBelowTerm(),
		"bootstrap revocation term should be 1")
	// Proposed rule carries the bootstrap policy and the full cohort.
	propRule := propReq.GetProposal().GetProposedTransition().GetProposal()
	require.Equal(t, int64(1), propRule.GetRuleNumber().GetCoordinatorTerm())
	require.Equal(t, int32(3), propRule.GetDurabilityPolicy().GetRequiredCount())
	require.Len(t, propRule.GetCohortMembers(), 3)

	// Followers should receive SetPrimary carrying the same leader + rule.
	for _, id := range cohortIDs[1:] {
		key := topoclient.ComponentIDString(id)
		_, isPromote := fakeClient.PromoteRequests[key]
		require.False(t, isPromote, "Promote should NOT be sent to follower %s", id.Name)
		stp, ok := fakeClient.SetPrimaryRequests[key]
		require.True(t, ok, "SetPrimary should be sent to %s", id.Name)
		require.Equal(t, "mp1", stp.GetReplicationPrimary().GetPrimary().GetId().GetName(),
			"follower %s should be informed of mp1 as leader", id.Name)
		require.Equal(t, int64(1), commonconsensus.PossiblyUndecidedRule(stp.GetReplicationPrimary().GetPosition()).GetRuleNumber().GetCoordinatorTerm())
	}
}
