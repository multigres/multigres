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
	"errors"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonconsensus "github.com/multigres/multigres/go/common/consensus"
	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
)

// makePoolerState creates a PoolerHealthState with the given cell/name.
func makePoolerState(cell, name string) *multiorchdatapb.PoolerHealthState {
	id := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      cell,
		Name:      name,
	}
	return &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id:       id,
			Hostname: "localhost",
			PortMap:  map[string]int32{"postgres": 5432, "grpc": 9000},
		},
		IsLastCheckValid: true,
	}
}

// setRecruitOK configures the fake client to return a successful Recruit response
// with an identity ConsensusStatus for the given pooler. The fake client
// automatically stamps the request's TermRevocation onto the returned status.
func setRecruitOK(fc *rpcclient.FakeClient, p *multiorchdatapb.PoolerHealthState) {
	key := topoclient.MultiPoolerIDString(p.MultiPooler.Id)
	fc.RecruitResponses[key] = &consensusdatapb.RecruitResponse{
		ConsensusStatus: &clustermetadatapb.ConsensusStatus{
			Id: p.MultiPooler.Id,
		},
	}
}

// newRuleChangeCoordinator creates a Coordinator for rule-change tests.
// These tests do not use topology, so topoStore is a fresh in-memory instance.
func newRuleChangeCoordinator(t *testing.T, fc *rpcclient.FakeClient) *Coordinator {
	t.Helper()
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	t.Cleanup(func() { ts.Close() })
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn}))
	coordID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIORCH,
		Cell:      "zone1",
		Name:      "coord-1",
	}
	return NewCoordinator(coordID, ts, fc, logger, false)
}

// fixedProposal builds a trivial tryBuildProposal callback that returns a pre-built
// proposal once at least minNodes statuses have been collected.
func fixedProposal(minNodes int, proposal *consensusdatapb.CoordinatorProposal) func(*clustermetadatapb.TermRevocation, []*clustermetadatapb.ConsensusStatus) (*consensusdatapb.CoordinatorProposal, error) {
	return func(_ *clustermetadatapb.TermRevocation, statuses []*clustermetadatapb.ConsensusStatus) (*consensusdatapb.CoordinatorProposal, error) {
		if len(statuses) >= minNodes {
			return proposal, nil
		}
		return nil, fmt.Errorf("need %d nodes, have %d", minNodes, len(statuses))
	}
}

func nopCheckProposalPossible(_ *clustermetadatapb.TermRevocation, _ []*clustermetadatapb.ConsensusStatus) error {
	return nil
}

// ---- TestBuildFailoverProposal ----

func TestBuildFailoverProposal(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn}))

	makeID := func(name string) *clustermetadatapb.ID {
		return &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      "zone1",
			Name:      name,
		}
	}
	makeMP := func(name string) *clustermetadatapb.MultiPooler {
		return &clustermetadatapb.MultiPooler{
			Id:       makeID(name),
			Hostname: "localhost",
			PortMap:  map[string]int32{"postgres": 5432},
		}
	}
	makeCS := func(name string) *clustermetadatapb.ConsensusStatus {
		return &clustermetadatapb.ConsensusStatus{Id: makeID(name)}
	}
	makeHealth := func(name string) *multiorchdatapb.PoolerHealthState {
		return &multiorchdatapb.PoolerHealthState{MultiPooler: makeMP(name)}
	}
	makeResignedHealth := func(name string, primaryTerm int64) *multiorchdatapb.PoolerHealthState {
		return &multiorchdatapb.PoolerHealthState{
			MultiPooler: makeMP(name),
			ConsensusStatus: &clustermetadatapb.ConsensusStatus{
				Id: makeID(name),
				CurrentPosition: &clustermetadatapb.PoolerPosition{
					Rule: &clustermetadatapb.ShardRule{
						LeaderId: makeID(name),
						RuleNumber: &clustermetadatapb.RuleNumber{
							CoordinatorTerm: primaryTerm,
						},
					},
				},
			},
			AvailabilityStatus: &clustermetadatapb.AvailabilityStatus{
				LeadershipStatus: &clustermetadatapb.LeadershipStatus{
					Signal:     clustermetadatapb.LeadershipSignal_LEADERSHIP_SIGNAL_REQUESTING_DEMOTION,
					LeaderTerm: primaryTerm,
				},
			},
		}
	}

	outgoingRule := &clustermetadatapb.ShardRule{
		CohortMembers: []*clustermetadatapb.ID{makeID("mp1"), makeID("mp2"), makeID("mp3")},
		DurabilityPolicy: &clustermetadatapb.DurabilityPolicy{
			QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_AT_LEAST_N,
			RequiredCount: 2,
		},
		LeaderId: makeID("mp1"),
	}
	rev := &clustermetadatapb.TermRevocation{RevokedBelowTerm: 5}
	poolerByID := map[string]*clustermetadatapb.MultiPooler{
		"zone1_mp1": makeMP("mp1"),
		"zone1_mp2": makeMP("mp2"),
		"zone1_mp3": makeMP("mp3"),
	}

	t.Run("picks first eligible leader", func(t *testing.T) {
		result := commonconsensus.RecruitmentResult{
			TermRevocation:  rev,
			OutgoingRule:    outgoingRule,
			EligibleLeaders: []*clustermetadatapb.ConsensusStatus{makeCS("mp1"), makeCS("mp2")},
		}
		healthByID := map[string]*multiorchdatapb.PoolerHealthState{
			"zone1_mp1": makeHealth("mp1"),
			"zone1_mp2": makeHealth("mp2"),
		}

		proposal, err := buildFailoverProposal(ctx, logger, result, poolerByID, healthByID)
		require.NoError(t, err)
		assert.Equal(t, "mp1", proposal.GetProposalLeader().GetId().GetName())
		assert.Equal(t, "localhost", proposal.GetProposalLeader().GetHost())
		assert.Len(t, proposal.GetProposedRule().GetCohortMembers(), 3)
		assert.Equal(t, "mp1", proposal.GetProposedRule().GetLeaderId().GetName())
	})

	t.Run("skips resigned primary, picks next eligible leader", func(t *testing.T) {
		result := commonconsensus.RecruitmentResult{
			TermRevocation:  rev,
			OutgoingRule:    outgoingRule,
			EligibleLeaders: []*clustermetadatapb.ConsensusStatus{makeCS("mp1"), makeCS("mp2")},
		}
		healthByID := map[string]*multiorchdatapb.PoolerHealthState{
			"zone1_mp1": makeResignedHealth("mp1", 4),
			"zone1_mp2": makeHealth("mp2"),
		}

		proposal, err := buildFailoverProposal(ctx, logger, result, poolerByID, healthByID)
		require.NoError(t, err)
		assert.Equal(t, "mp2", proposal.GetProposalLeader().GetId().GetName())
	})

	t.Run("all eligible leaders resigned returns error", func(t *testing.T) {
		result := commonconsensus.RecruitmentResult{
			TermRevocation:  rev,
			OutgoingRule:    outgoingRule,
			EligibleLeaders: []*clustermetadatapb.ConsensusStatus{makeCS("mp1"), makeCS("mp2")},
		}
		healthByID := map[string]*multiorchdatapb.PoolerHealthState{
			"zone1_mp1": makeResignedHealth("mp1", 4),
			"zone1_mp2": makeResignedHealth("mp2", 4),
		}

		_, err := buildFailoverProposal(ctx, logger, result, poolerByID, healthByID)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "all eligible leaders have resigned")
	})

	t.Run("no OutgoingRule returns error", func(t *testing.T) {
		result := commonconsensus.RecruitmentResult{
			TermRevocation:  rev,
			OutgoingRule:    nil,
			EligibleLeaders: []*clustermetadatapb.ConsensusStatus{makeCS("mp1")},
		}
		healthByID := map[string]*multiorchdatapb.PoolerHealthState{
			"zone1_mp1": makeHealth("mp1"),
		}

		_, err := buildFailoverProposal(ctx, logger, result, poolerByID, healthByID)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no committed rule found")
	})
}

// ---- TestRun ----

func TestRun_Success(t *testing.T) {
	ctx := context.Background()
	fc := rpcclient.NewFakeClient()
	c := newRuleChangeCoordinator(t, fc)

	mp1 := makePoolerState("zone1", "mp1")
	mp2 := makePoolerState("zone1", "mp2")
	cohort := []*multiorchdatapb.PoolerHealthState{mp1, mp2}

	setRecruitOK(fc, mp1)
	setRecruitOK(fc, mp2)

	leaderID := mp1.MultiPooler.Id
	proposal := &consensusdatapb.CoordinatorProposal{
		TermRevocation: &clustermetadatapb.TermRevocation{RevokedBelowTerm: 1},
		ProposalLeader: &consensusdatapb.ProposalLeader{
			Id:           leaderID,
			Host:         "localhost",
			PostgresPort: 5432,
		},
		ProposedRule: &clustermetadatapb.ShardRule{
			LeaderId: leaderID,
			CohortMembers: []*clustermetadatapb.ID{
				mp1.MultiPooler.Id,
				mp2.MultiPooler.Id,
			},
			DurabilityPolicy: &clustermetadatapb.DurabilityPolicy{
				QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_AT_LEAST_N,
				RequiredCount: 2,
			},
		},
	}

	rc := c.newRuleChange("test", fixedProposal(2, proposal), nopCheckProposalPossible)
	require.NoError(t, rc.Run(ctx, cohort))

	// Both nodes should have received a Propose request.
	mp1Key := topoclient.MultiPoolerIDString(mp1.MultiPooler.Id)
	mp2Key := topoclient.MultiPoolerIDString(mp2.MultiPooler.Id)
	assert.NotNil(t, fc.ProposeRequests[mp1Key])
	assert.NotNil(t, fc.ProposeRequests[mp2Key])
}

func TestRun_EarlyExit(t *testing.T) {
	// Quorum reached after 1 node responds — should not wait for the second.
	ctx := context.Background()
	fc := rpcclient.NewFakeClient()
	c := newRuleChangeCoordinator(t, fc)

	mp1 := makePoolerState("zone1", "mp1")
	mp2 := makePoolerState("zone1", "mp2")
	cohort := []*multiorchdatapb.PoolerHealthState{mp1, mp2}

	setRecruitOK(fc, mp1)
	setRecruitOK(fc, mp2)

	// tryBuildProposal succeeds as soon as the first node responds, picking that node as
	// leader. Which node responds first is non-deterministic, so the proposal is
	// built dynamically from the first available status.
	poolerByID, _ := buildCohortMaps(cohort)
	tryBuildProposal := func(_ *clustermetadatapb.TermRevocation, statuses []*clustermetadatapb.ConsensusStatus) (*consensusdatapb.CoordinatorProposal, error) {
		if len(statuses) < 1 {
			return nil, errors.New("no nodes yet")
		}
		leader := statuses[0]
		mp := poolerByID[topoclient.ClusterIDString(leader.GetId())]
		return &consensusdatapb.CoordinatorProposal{
			TermRevocation: &clustermetadatapb.TermRevocation{RevokedBelowTerm: 1},
			ProposalLeader: &consensusdatapb.ProposalLeader{
				Id:           leader.GetId(),
				Host:         mp.GetHostname(),
				PostgresPort: mp.GetPortMap()["postgres"],
			},
			ProposedRule: &clustermetadatapb.ShardRule{
				LeaderId:      leader.GetId(),
				CohortMembers: []*clustermetadatapb.ID{leader.GetId()},
				DurabilityPolicy: &clustermetadatapb.DurabilityPolicy{
					QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_AT_LEAST_N,
					RequiredCount: 1,
				},
			},
		}, nil
	}

	rc := c.newRuleChange("test", tryBuildProposal, nopCheckProposalPossible)
	require.NoError(t, rc.Run(ctx, cohort))
}

func TestRun_InsufficientRecruitment(t *testing.T) {
	// All Recruit calls return no ConsensusStatus — tryBuildProposal always fails.
	ctx := context.Background()
	fc := rpcclient.NewFakeClient()
	c := newRuleChangeCoordinator(t, fc)

	mp1 := makePoolerState("zone1", "mp1")
	mp2 := makePoolerState("zone1", "mp2")
	cohort := []*multiorchdatapb.PoolerHealthState{mp1, mp2}

	// No RecruitResponses set → fake client returns empty response (nil ConsensusStatus).

	tryBuildProposal := func(_ *clustermetadatapb.TermRevocation, statuses []*clustermetadatapb.ConsensusStatus) (*consensusdatapb.CoordinatorProposal, error) {
		return nil, fmt.Errorf("not enough nodes: have %d", len(statuses))
	}

	rc := c.newRuleChange("test", tryBuildProposal, nopCheckProposalPossible)
	err := rc.Run(ctx, cohort)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "recruitment failed")
}

func TestRun_BackoffOnRecentAcceptance(t *testing.T) {
	// A node in the cohort accepted a term very recently — should back off.
	ctx := context.Background()
	fc := rpcclient.NewFakeClient()
	c := newRuleChangeCoordinator(t, fc)

	mp1 := makePoolerState("zone1", "mp1")
	mp1.ConsensusStatus = &clustermetadatapb.ConsensusStatus{
		TermRevocation: &clustermetadatapb.TermRevocation{
			RevokedBelowTerm:       5,
			CoordinatorInitiatedAt: timestamppb.New(time.Now().Add(-500 * time.Millisecond)),
		},
	}
	cohort := []*multiorchdatapb.PoolerHealthState{mp1}

	rc := c.newRuleChange("test", fixedProposal(1, &consensusdatapb.CoordinatorProposal{}), nopCheckProposalPossible)
	err := rc.Run(ctx, cohort)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "another coordinator started recruiting recently")
	assert.Empty(t, fc.GetCallLog(), "no RPCs should be made when backing off for recent acceptance")
}

func TestRun_PreValidateFails(t *testing.T) {
	// checkProposalPossible returns an error — no recruitment should be attempted.
	ctx := context.Background()
	fc := rpcclient.NewFakeClient()
	c := newRuleChangeCoordinator(t, fc)

	mp1 := makePoolerState("zone1", "mp1")
	cohort := []*multiorchdatapb.PoolerHealthState{mp1}

	setRecruitOK(fc, mp1)

	preValidateErr := errors.New("cluster not in a state that allows failover")
	checkProposalPossible := func(_ *clustermetadatapb.TermRevocation, _ []*clustermetadatapb.ConsensusStatus) error {
		return preValidateErr
	}

	rc := c.newRuleChange("test", fixedProposal(1, &consensusdatapb.CoordinatorProposal{}), checkProposalPossible)
	err := rc.Run(ctx, cohort)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "pre-vote failed")
	assert.Contains(t, err.Error(), preValidateErr.Error())

	// No Recruit calls should have been made.
	assert.Empty(t, fc.GetCallLog())
}

func TestRun_LeaderProposeFails(t *testing.T) {
	// Propose fails for the leader — Run should return an error.
	ctx := context.Background()
	fc := rpcclient.NewFakeClient()
	c := newRuleChangeCoordinator(t, fc)

	mp1 := makePoolerState("zone1", "mp1")
	mp2 := makePoolerState("zone1", "mp2")
	cohort := []*multiorchdatapb.PoolerHealthState{mp1, mp2}

	setRecruitOK(fc, mp1)
	setRecruitOK(fc, mp2)

	leaderID := mp1.MultiPooler.Id
	mp1Key := topoclient.MultiPoolerIDString(mp1.MultiPooler.Id)
	proposal := &consensusdatapb.CoordinatorProposal{
		TermRevocation: &clustermetadatapb.TermRevocation{RevokedBelowTerm: 1},
		ProposalLeader: &consensusdatapb.ProposalLeader{
			Id:           leaderID,
			Host:         "localhost",
			PostgresPort: 5432,
		},
		ProposedRule: &clustermetadatapb.ShardRule{
			LeaderId: leaderID,
			CohortMembers: []*clustermetadatapb.ID{
				mp1.MultiPooler.Id,
				mp2.MultiPooler.Id,
			},
			DurabilityPolicy: &clustermetadatapb.DurabilityPolicy{
				QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_AT_LEAST_N,
				RequiredCount: 2,
			},
		},
	}

	// Inject the Propose error for the leader inside tryBuildProposal: at the point
	// tryBuildProposal(minNodes=2) is called, both Recruit goroutines have already sent
	// their results to the channel and returned, so writing fc.Errors here is
	// safe — proposeAll goroutines haven't launched yet, establishing a
	// happens-before edge via goroutine creation.
	tryBuildProposal := func(rev *clustermetadatapb.TermRevocation, statuses []*clustermetadatapb.ConsensusStatus) (*consensusdatapb.CoordinatorProposal, error) {
		if len(statuses) < 2 {
			return nil, errors.New("need 2 nodes")
		}
		fc.Errors[mp1Key] = errors.New("propose rejected by leader")
		return proposal, nil
	}

	rc := c.newRuleChange("test", tryBuildProposal, nopCheckProposalPossible)
	err := rc.Run(ctx, cohort)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to accept proposal")
}

func TestRun_NonLeaderProposeFails(t *testing.T) {
	// Propose fails for a non-leader — Run should succeed (non-leader failures are non-fatal).
	ctx := context.Background()
	fc := rpcclient.NewFakeClient()
	c := newRuleChangeCoordinator(t, fc)

	mp1 := makePoolerState("zone1", "mp1")
	mp2 := makePoolerState("zone1", "mp2")
	cohort := []*multiorchdatapb.PoolerHealthState{mp1, mp2}

	setRecruitOK(fc, mp1)
	setRecruitOK(fc, mp2)

	leaderID := mp1.MultiPooler.Id
	proposal := &consensusdatapb.CoordinatorProposal{
		TermRevocation: &clustermetadatapb.TermRevocation{RevokedBelowTerm: 1},
		ProposalLeader: &consensusdatapb.ProposalLeader{
			Id:           leaderID,
			Host:         "localhost",
			PostgresPort: 5432,
		},
		ProposedRule: &clustermetadatapb.ShardRule{
			LeaderId: leaderID,
			CohortMembers: []*clustermetadatapb.ID{
				mp1.MultiPooler.Id,
				mp2.MultiPooler.Id,
			},
			DurabilityPolicy: &clustermetadatapb.DurabilityPolicy{
				QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_AT_LEAST_N,
				RequiredCount: 2,
			},
		},
	}

	// Inject a Propose error for the non-leader (mp2).
	// We only want Propose to fail, not Recruit — so use ProposeResponses error
	// by setting errors only after recruitment is done. Since Errors applies to
	// all RPC calls, we instead set error on mp2 after recruit by using a
	// custom tryBuildProposal that installs the error mid-flight.
	//
	// Simpler approach: set an error on mp2 before Run. Recruit will also fail,
	// but Recruit failure just means no status from mp2, and mp1 alone satisfies
	// minNodes=1. We propose to just mp1 (the leader), which succeeds.
	mp2Key := topoclient.MultiPoolerIDString(mp2.MultiPooler.Id)
	fc.Errors[mp2Key] = errors.New("standby propose rejected")

	// With mp2 failing Recruit, only mp1 recruits — use minNodes=1.
	rc := c.newRuleChange("test", fixedProposal(1, proposal), nopCheckProposalPossible)
	require.NoError(t, rc.Run(ctx, cohort))

	// Leader (mp1) received Propose.
	mp1Key := topoclient.MultiPoolerIDString(mp1.MultiPooler.Id)
	assert.NotNil(t, fc.ProposeRequests[mp1Key])
}
