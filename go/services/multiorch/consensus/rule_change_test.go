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
	"slices"
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
// A minimal ConsensusStatus carrying a zero-valued recorded rule is attached
// so the state is suitable as a cohort member: NewTermRevocation requires
// at least one cohort member to report a recorded rule. Tests that need
// richer status state overwrite the field after construction.
func makePoolerState(cell, name string) *multiorchdatapb.PoolerHealthState {
	id := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      cell,
		Name:      name,
	}
	return &multiorchdatapb.PoolerHealthState{
		Multipooler: &clustermetadatapb.Multipooler{
			Id:       id,
			Hostname: "localhost",
			PortMap:  map[string]int32{"postgres": 5432, "grpc": 9000},
		},
		IsLastCheckValid: true,
		ConsensusStatus: &clustermetadatapb.ConsensusStatus{
			Id: id,
			CurrentPosition: &clustermetadatapb.PoolerPosition{
				Position: &clustermetadatapb.RulePosition{Decision: &clustermetadatapb.ShardRule{
					RuleNumber: &clustermetadatapb.RuleNumber{},
				}},
			},
		},
	}
}

// testInitiatedAt is a fixed timestamp used by test helpers that need to
// supply CoordinatorInitiatedAt without varying it per test run.
var testInitiatedAt = timestamppb.New(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))

// newTestRevocation builds a TermRevocation suitable for rule_change.Run from
// the given cohort. Tests that don't care about specific revocation contents
// (term, outgoing_rule, etc.) use this to satisfy Run's "revocation is required"
// contract; tests that need richer state construct their own.
func newTestRevocation(t *testing.T, coord *Coordinator, cohort []*multiorchdatapb.PoolerHealthState) *clustermetadatapb.TermRevocation {
	t.Helper()
	var statuses []*clustermetadatapb.ConsensusStatus
	for _, p := range cohort {
		if cs := p.GetConsensusStatus(); cs != nil {
			statuses = append(statuses, cs)
		}
	}
	rev, err := commonconsensus.NewTermRevocation(statuses, coord.coordinatorID, testInitiatedAt)
	require.NoError(t, err)
	return rev
}

// setRecruitOK configures the fake client to return a successful Recruit response
// with an identity ConsensusStatus for the given pooler. The fake client
// automatically stamps the request's TermRevocation onto the returned status.
func setRecruitOK(fc *rpcclient.FakeClient, p *multiorchdatapb.PoolerHealthState) {
	key := topoclient.ComponentIDString(p.Multipooler.Id)
	fc.RecruitResponses[key] = &consensusdatapb.RecruitResponse{
		ConsensusStatus: &clustermetadatapb.ConsensusStatus{
			Id: p.Multipooler.Id,
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
	return NewCoordinator(coordID, ts, fc, logger)
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
	makeID := func(name string) *clustermetadatapb.ID {
		return &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      "zone1",
			Name:      name,
		}
	}
	makeAddr := func(name string) *clustermetadatapb.PoolerAddress {
		return &clustermetadatapb.PoolerAddress{
			Id:           makeID(name),
			Host:         "localhost",
			PostgresPort: 5432,
		}
	}
	makeCS := func(name string) *clustermetadatapb.ConsensusStatus {
		return &clustermetadatapb.ConsensusStatus{Id: makeID(name)}
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
	addressByID := map[string]*clustermetadatapb.PoolerAddress{
		"zone1_mp1": makeAddr("mp1"),
		"zone1_mp2": makeAddr("mp2"),
		"zone1_mp3": makeAddr("mp3"),
	}

	t.Run("picks first eligible leader", func(t *testing.T) {
		result := commonconsensus.RecruitmentResult{
			TermRevocation:  rev,
			OutgoingRule:    outgoingRule,
			EligibleLeaders: []*clustermetadatapb.ConsensusStatus{makeCS("mp1"), makeCS("mp2")},
		}

		proposal, err := buildFailoverProposal(result, addressByID)
		require.NoError(t, err)
		assert.Equal(t, "mp1", proposal.GetProposalLeader().GetId().GetName())
		assert.Equal(t, "localhost", proposal.GetProposalLeader().GetHost())
		assert.Len(t, proposal.GetProposedTransition().GetProposal().GetCohortMembers(), 3)
		assert.Equal(t, "mp1", proposal.GetProposedTransition().GetProposal().GetLeaderId().GetName())
	})

	t.Run("no OutgoingRule returns error", func(t *testing.T) {
		result := commonconsensus.RecruitmentResult{
			TermRevocation:  rev,
			OutgoingRule:    nil,
			EligibleLeaders: []*clustermetadatapb.ConsensusStatus{makeCS("mp1")},
		}

		_, err := buildFailoverProposal(result, addressByID)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no committed rule found")
	})

	t.Run("no EligibleLeaders returns error", func(t *testing.T) {
		result := commonconsensus.RecruitmentResult{
			TermRevocation:  rev,
			OutgoingRule:    outgoingRule,
			EligibleLeaders: nil,
		}

		_, err := buildFailoverProposal(result, addressByID)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no eligible leaders")
	})
}

// ---- TestBuildBootstrapProposal ----

func TestBuildBootstrapProposal(t *testing.T) {
	makeID := func(name string) *clustermetadatapb.ID {
		return &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      "zone1",
			Name:      name,
		}
	}
	makeAddr := func(name string) *clustermetadatapb.PoolerAddress {
		return &clustermetadatapb.PoolerAddress{
			Id:           makeID(name),
			Host:         "localhost",
			PostgresPort: 5432,
		}
	}
	makeCS := func(name string) *clustermetadatapb.ConsensusStatus {
		return &clustermetadatapb.ConsensusStatus{Id: makeID(name)}
	}

	cohortIDs := []*clustermetadatapb.ID{makeID("mp1"), makeID("mp2"), makeID("mp3")}
	policy := &clustermetadatapb.DurabilityPolicy{
		QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_AT_LEAST_N,
		RequiredCount: 2,
	}
	rev := &clustermetadatapb.TermRevocation{RevokedBelowTerm: 5}
	addressByID := map[string]*clustermetadatapb.PoolerAddress{
		"zone1_mp1": makeAddr("mp1"),
		"zone1_mp2": makeAddr("mp2"),
		"zone1_mp3": makeAddr("mp3"),
	}

	t.Run("sets skip_outgoing_quorum and picks first eligible leader", func(t *testing.T) {
		result := commonconsensus.RecruitmentResult{
			TermRevocation:  rev,
			EligibleLeaders: []*clustermetadatapb.ConsensusStatus{makeCS("mp1"), makeCS("mp2")},
		}

		proposal, err := buildBootstrapProposal(result, cohortIDs, policy, addressByID)
		require.NoError(t, err)
		assert.Equal(t, "mp1", proposal.GetProposalLeader().GetId().GetName())
		assert.True(t, proposal.GetSkipOutgoingQuorum(),
			"bootstrap proposals must set skip_outgoing_quorum so multipoolers apply the GUC without an outgoing-cohort quorum")
		assert.Equal(t, int64(5), proposal.GetProposedTransition().GetProposal().GetRuleNumber().GetCoordinatorTerm())
	})

	t.Run("no EligibleLeaders returns error", func(t *testing.T) {
		result := commonconsensus.RecruitmentResult{
			TermRevocation:  rev,
			EligibleLeaders: nil,
		}

		_, err := buildBootstrapProposal(result, cohortIDs, policy, addressByID)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no eligible leaders")
	})

	t.Run("leader missing from addressByID returns error", func(t *testing.T) {
		result := commonconsensus.RecruitmentResult{
			TermRevocation:  rev,
			EligibleLeaders: []*clustermetadatapb.ConsensusStatus{makeCS("ghost")},
		}

		_, err := buildBootstrapProposal(result, cohortIDs, policy, addressByID)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not found in cohort")
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

	leaderID := mp1.Multipooler.Id
	proposal := &consensusdatapb.CoordinatorProposal{
		TermRevocation: &clustermetadatapb.TermRevocation{RevokedBelowTerm: 1},
		ProposalLeader: &clustermetadatapb.PoolerAddress{
			Id:           leaderID,
			Host:         "localhost",
			PostgresPort: 5432,
		},
		ProposedTransition: &clustermetadatapb.RulePosition{Proposal: &clustermetadatapb.ShardRule{
			LeaderId: leaderID,
			CohortMembers: []*clustermetadatapb.ID{
				mp1.Multipooler.Id,
				mp2.Multipooler.Id,
			},
			DurabilityPolicy: &clustermetadatapb.DurabilityPolicy{
				QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_AT_LEAST_N,
				RequiredCount: 2,
			},
		}},
	}

	rc := c.newRuleChange("test", fixedProposal(2, proposal), nopCheckProposalPossible)
	require.NoError(t, rc.Run(ctx, cohort, newTestRevocation(t, c, cohort)))

	// mp1 (leader) receives Promote; mp2 (follower) receives SetPrimary.
	mp1Key := topoclient.ComponentIDString(mp1.Multipooler.Id)
	mp2Key := topoclient.ComponentIDString(mp2.Multipooler.Id)
	assert.NotNil(t, fc.PromoteRequests[mp1Key], "leader should receive Promote")
	assert.Nil(t, fc.PromoteRequests[mp2Key], "follower should not receive Promote")
	assert.NotNil(t, fc.SetPrimaryRequests[mp2Key], "follower should receive SetPrimary")
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
	addressByID, _ := buildCohortMaps(cohort)
	tryBuildProposal := func(_ *clustermetadatapb.TermRevocation, statuses []*clustermetadatapb.ConsensusStatus) (*consensusdatapb.CoordinatorProposal, error) {
		if len(statuses) < 1 {
			return nil, errors.New("no nodes yet")
		}
		leader := statuses[0]
		return &consensusdatapb.CoordinatorProposal{
			TermRevocation: &clustermetadatapb.TermRevocation{RevokedBelowTerm: 1},
			ProposalLeader: addressByID[topoclient.ClusterIDString(leader.GetId())],
			ProposedTransition: &clustermetadatapb.RulePosition{Proposal: &clustermetadatapb.ShardRule{
				LeaderId:      leader.GetId(),
				CohortMembers: []*clustermetadatapb.ID{leader.GetId()},
				DurabilityPolicy: &clustermetadatapb.DurabilityPolicy{
					QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_AT_LEAST_N,
					RequiredCount: 1,
				},
			}},
		}, nil
	}

	rc := c.newRuleChange("test", tryBuildProposal, nopCheckProposalPossible)
	require.NoError(t, rc.Run(ctx, cohort, newTestRevocation(t, c, cohort)))
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
	err := rc.Run(ctx, cohort, newTestRevocation(t, c, cohort))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "recruitment failed")
}

func TestRun_BackoffOnRecentAcceptance(t *testing.T) {
	// A node in the cohort accepted a term very recently — should back off.
	ctx := context.Background()
	fc := rpcclient.NewFakeClient()
	c := newRuleChangeCoordinator(t, fc)

	mp1 := makePoolerState("zone1", "mp1")
	mp1.ConsensusStatus.TermRevocation = &clustermetadatapb.TermRevocation{
		RevokedBelowTerm:       5,
		CoordinatorInitiatedAt: timestamppb.New(time.Now().Add(-500 * time.Millisecond)),
	}
	cohort := []*multiorchdatapb.PoolerHealthState{mp1}

	rc := c.newRuleChange("test", fixedProposal(1, &consensusdatapb.CoordinatorProposal{}), nopCheckProposalPossible)
	err := rc.Run(ctx, cohort, newTestRevocation(t, c, cohort))
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
	err := rc.Run(ctx, cohort, newTestRevocation(t, c, cohort))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "pre-vote failed")
	assert.Contains(t, err.Error(), preValidateErr.Error())

	// No Recruit calls should have been made.
	assert.Empty(t, fc.GetCallLog())
}

func TestRun_LeaderPromoteFails(t *testing.T) {
	// Promote fails for the leader — Run should return an error.
	ctx := context.Background()
	fc := rpcclient.NewFakeClient()
	c := newRuleChangeCoordinator(t, fc)

	mp1 := makePoolerState("zone1", "mp1")
	mp2 := makePoolerState("zone1", "mp2")
	cohort := []*multiorchdatapb.PoolerHealthState{mp1, mp2}

	setRecruitOK(fc, mp1)
	setRecruitOK(fc, mp2)

	leaderID := mp1.Multipooler.Id
	mp1Key := topoclient.ComponentIDString(mp1.Multipooler.Id)
	proposal := &consensusdatapb.CoordinatorProposal{
		TermRevocation: &clustermetadatapb.TermRevocation{RevokedBelowTerm: 1},
		ProposalLeader: &clustermetadatapb.PoolerAddress{
			Id:           leaderID,
			Host:         "localhost",
			PostgresPort: 5432,
		},
		ProposedTransition: &clustermetadatapb.RulePosition{Proposal: &clustermetadatapb.ShardRule{
			LeaderId: leaderID,
			CohortMembers: []*clustermetadatapb.ID{
				mp1.Multipooler.Id,
				mp2.Multipooler.Id,
			},
			DurabilityPolicy: &clustermetadatapb.DurabilityPolicy{
				QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_AT_LEAST_N,
				RequiredCount: 2,
			},
		}},
	}

	// Inject the Promote error for the leader inside tryBuildProposal: at the point
	// tryBuildProposal(minNodes=2) is called, both Recruit goroutines have already sent
	// their results to the channel and returned, so writing fc.Errors here is
	// safe — promoteAll goroutines haven't launched yet, establishing a
	// happens-before edge via goroutine creation.
	tryBuildProposal := func(rev *clustermetadatapb.TermRevocation, statuses []*clustermetadatapb.ConsensusStatus) (*consensusdatapb.CoordinatorProposal, error) {
		if len(statuses) < 2 {
			return nil, errors.New("need 2 nodes")
		}
		fc.Errors[mp1Key] = errors.New("promote rejected by leader")
		return proposal, nil
	}

	rc := c.newRuleChange("test", tryBuildProposal, nopCheckProposalPossible)
	err := rc.Run(ctx, cohort, newTestRevocation(t, c, cohort))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to accept proposal")
}

func TestRun_SlowRecruitDoesNotBlockAfterQuorum(t *testing.T) {
	// Phase 2 must not block on recruit goroutines still in flight after quorum
	// is reached. Recruit is sent to every cohort member regardless of health
	// status — node health can change during the recruit phase — but once Phase 1
	// has a viable proposal, Run should proceed without waiting for stragglers.
	//
	// Setup: mp1 responds immediately and satisfies quorum on its own. mp2's
	// Recruit is configured to block for 20 s. Without the non-blocking Phase 2,
	// Run would stall for 20 s waiting for mp2; with it, Run completes quickly
	// and mp2's goroutine finishes in the background.
	ctx := context.Background()
	fc := rpcclient.NewFakeClient()
	c := newRuleChangeCoordinator(t, fc)

	mp1 := makePoolerState("zone1", "mp1")
	mp2 := makePoolerState("zone1", "mp2")

	cohort := []*multiorchdatapb.PoolerHealthState{mp1, mp2}
	setRecruitOK(fc, mp1)

	mp2Key := topoclient.ComponentIDString(mp2.Multipooler.Id)
	fc.RecruitDelays[mp2Key] = 20 * time.Second

	leaderID := mp1.Multipooler.Id
	proposal := &consensusdatapb.CoordinatorProposal{
		TermRevocation: &clustermetadatapb.TermRevocation{RevokedBelowTerm: 1},
		ProposalLeader: &clustermetadatapb.PoolerAddress{
			Id:           leaderID,
			Host:         "localhost",
			PostgresPort: 5432,
		},
		ProposedTransition: &clustermetadatapb.RulePosition{Proposal: &clustermetadatapb.ShardRule{
			LeaderId:      leaderID,
			CohortMembers: []*clustermetadatapb.ID{mp1.Multipooler.Id},
			DurabilityPolicy: &clustermetadatapb.DurabilityPolicy{
				QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_AT_LEAST_N,
				RequiredCount: 1,
			},
		}},
	}

	rc := c.newRuleChange("test", fixedProposal(1, proposal), nopCheckProposalPossible)

	start := time.Now()
	err := rc.Run(ctx, cohort, newTestRevocation(t, c, cohort))
	elapsed := time.Since(start)

	require.NoError(t, err)
	assert.Less(t, elapsed, 2*time.Second,
		"Run blocked waiting for slow recruit; Phase 2 should not wait for in-flight goroutines after quorum")
	// mp2's Recruit RPC was issued (no pre-filtering on health), but its slow
	// response did not delay Run. The recruit fan-out runs in a background
	// goroutine, so poll for the logged call rather than asserting immediately:
	// Run can return before mp2's goroutine has been scheduled far enough to
	// record the call, especially under slow (e.g. coverage-instrumented)
	// scheduling.
	assert.Eventually(t, func() bool {
		return slices.Contains(fc.GetCallLog(), fmt.Sprintf("Recruit(%s)", string(mp2Key)))
	}, 5*time.Second, 10*time.Millisecond,
		"Recruit should be issued to all cohort members regardless of health status")
}

func TestRun_StragglersGetSetTermPrimary(t *testing.T) {
	// When a node's Recruit response arrives after Phase 1 has committed to a
	// proposal, Phase 2's non-blocking select should catch it and dispatch
	// SetTermPrimary so the node learns the new leader immediately rather than
	// waiting for a follow-up re-wiring round.
	//
	// Sequencing: mp2's Recruit is gated so it cannot complete until
	// tryBuildProposal releases it. tryBuildProposal runs on the Run() goroutine
	// synchronously inside Phase 1, so releasing the gate there and sleeping
	// briefly guarantees mp2's goroutine writes to the buffered channel before
	// Phase 2's select runs.
	ctx := context.Background()
	fc := rpcclient.NewFakeClient()
	c := newRuleChangeCoordinator(t, fc)

	mp1 := makePoolerState("zone1", "mp1") // leader, responds during Phase 1
	mp2 := makePoolerState("zone1", "mp2") // straggler, responds during Phase 2
	cohort := []*multiorchdatapb.PoolerHealthState{mp1, mp2}
	setRecruitOK(fc, mp1)
	setRecruitOK(fc, mp2)

	mp1Key := topoclient.ComponentIDString(mp1.Multipooler.Id)
	mp2Key := topoclient.ComponentIDString(mp2.Multipooler.Id)

	// Gate mp2: its Recruit blocks until the gate is closed.
	mp2Gate := make(chan struct{})
	fc.RecruitGates[mp2Key] = mp2Gate

	leaderID := mp1.Multipooler.Id
	proposal := &consensusdatapb.CoordinatorProposal{
		TermRevocation: &clustermetadatapb.TermRevocation{RevokedBelowTerm: 1},
		ProposalLeader: &clustermetadatapb.PoolerAddress{
			Id:           leaderID,
			Host:         "localhost",
			PostgresPort: 5432,
		},
		ProposedTransition: &clustermetadatapb.RulePosition{Proposal: &clustermetadatapb.ShardRule{
			LeaderId:      leaderID,
			CohortMembers: []*clustermetadatapb.ID{mp1.Multipooler.Id, mp2.Multipooler.Id},
			DurabilityPolicy: &clustermetadatapb.DurabilityPolicy{
				QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_AT_LEAST_N,
				RequiredCount: 1,
			},
		}},
	}

	// Phase 1 succeeds as soon as mp1 responds (mp2 is still gated at this point).
	// Close the gate here — inside tryBuildProposal on the Run() goroutine — then
	// sleep briefly so mp2's goroutine has time to pass the gate, complete Recruit,
	// and write its result to the buffered channel before Phase 2's select runs.
	gateOpened := false
	tryBuildProposal := func(_ *clustermetadatapb.TermRevocation, statuses []*clustermetadatapb.ConsensusStatus) (*consensusdatapb.CoordinatorProposal, error) {
		if len(statuses) < 1 {
			return nil, errors.New("need at least 1 node")
		}
		if !gateOpened {
			gateOpened = true
			close(mp2Gate)
			time.Sleep(5 * time.Millisecond)
		}
		return proposal, nil
	}

	rc := c.newRuleChange("test", tryBuildProposal, nopCheckProposalPossible)
	require.NoError(t, rc.Run(ctx, cohort, newTestRevocation(t, c, cohort)))

	assert.NotNil(t, fc.PromoteRequests[mp1Key],
		"leader (mp1) should receive Promote")
	assert.NotNil(t, fc.SetPrimaryRequests[mp2Key],
		"straggler (mp2) should receive SetPrimary")
}

func TestRun_NonLeaderPromoteFails(t *testing.T) {
	// Promote fails for a non-leader — Run should succeed (non-leader failures are non-fatal).
	ctx := context.Background()
	fc := rpcclient.NewFakeClient()
	c := newRuleChangeCoordinator(t, fc)

	mp1 := makePoolerState("zone1", "mp1")
	mp2 := makePoolerState("zone1", "mp2")
	cohort := []*multiorchdatapb.PoolerHealthState{mp1, mp2}

	setRecruitOK(fc, mp1)
	setRecruitOK(fc, mp2)

	leaderID := mp1.Multipooler.Id
	proposal := &consensusdatapb.CoordinatorProposal{
		TermRevocation: &clustermetadatapb.TermRevocation{RevokedBelowTerm: 1},
		ProposalLeader: &clustermetadatapb.PoolerAddress{
			Id:           leaderID,
			Host:         "localhost",
			PostgresPort: 5432,
		},
		ProposedTransition: &clustermetadatapb.RulePosition{Proposal: &clustermetadatapb.ShardRule{
			LeaderId: leaderID,
			CohortMembers: []*clustermetadatapb.ID{
				mp1.Multipooler.Id,
				mp2.Multipooler.Id,
			},
			DurabilityPolicy: &clustermetadatapb.DurabilityPolicy{
				QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_AT_LEAST_N,
				RequiredCount: 2,
			},
		}},
	}

	// Inject a Promote error for the non-leader (mp2).
	// We only want Promote to fail, not Recruit — so use PromoteResponses error
	// by setting errors only after recruitment is done. Since Errors applies to
	// all RPC calls, we instead set error on mp2 after recruit by using a
	// custom tryBuildProposal that installs the error mid-flight.
	//
	// Simpler approach: set an error on mp2 before Run. Recruit will also fail,
	// but Recruit failure just means no status from mp2, and mp1 alone satisfies
	// minNodes=1. We promote to just mp1 (the leader), which succeeds.
	mp2Key := topoclient.ComponentIDString(mp2.Multipooler.Id)
	fc.Errors[mp2Key] = errors.New("standby promote rejected")

	// With mp2 failing Recruit, only mp1 recruits — use minNodes=1.
	rc := c.newRuleChange("test", fixedProposal(1, proposal), nopCheckProposalPossible)
	require.NoError(t, rc.Run(ctx, cohort, newTestRevocation(t, c, cohort)))

	// Leader (mp1) received Promote.
	mp1Key := topoclient.ComponentIDString(mp1.Multipooler.Id)
	assert.NotNil(t, fc.PromoteRequests[mp1Key])
}
