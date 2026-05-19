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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
)

// makeCertifiedRequest builds a fully-populated rule + cert suitable for
// passing to ApplyCertifiedRuleChange. Outgoing term defaults to 0 (initial
// appointment); new term defaults to outgoing + 1.
func makeCertifiedRequest(outgoingTerm int64, leaderID *clustermetadatapb.ID, cohort []*clustermetadatapb.ID, orchID *clustermetadatapb.ID) (*clustermetadatapb.ShardKey, *clustermetadatapb.ShardRule, *clustermetadatapb.ExternallyCertifiedRevocation) {
	newTerm := outgoingTerm + 1
	frozenLSN := "0/0"
	if outgoingTerm > 0 {
		frozenLSN = "0/1000"
	}
	shardKey := &clustermetadatapb.ShardKey{Database: "db1", TableGroup: "default", Shard: "0-inf"}
	rule := &clustermetadatapb.ShardRule{
		RuleNumber:    &clustermetadatapb.RuleNumber{CoordinatorTerm: newTerm},
		LeaderId:      leaderID,
		CohortMembers: cohort,
		DurabilityPolicy: &clustermetadatapb.DurabilityPolicy{
			PolicyName:    "AT_LEAST_1",
			PolicyVersion: 1,
			QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_AT_LEAST_N,
			RequiredCount: 1,
		},
		CoordinatorId: orchID,
		CreationTime:  timestamppb.Now(),
	}
	cert := &clustermetadatapb.ExternallyCertifiedRevocation{
		FrozenLsn: frozenLSN,
		TermRevocation: &clustermetadatapb.TermRevocation{
			RevokedBelowTerm:       newTerm,
			AcceptedCoordinatorId:  orchID,
			CoordinatorInitiatedAt: timestamppb.Now(),
			OutgoingRule:           &clustermetadatapb.RuleNumber{CoordinatorTerm: outgoingTerm},
		},
	}
	return shardKey, rule, cert
}

// newCertifiedTestCoordinator builds a coordinator backed by an in-memory
// topo store with the supplied poolers pre-registered.
func newCertifiedTestCoordinator(t *testing.T, fc *rpcclient.FakeClient, poolers []*clustermetadatapb.MultiPooler) (*Coordinator, *clustermetadatapb.ID) {
	t.Helper()
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	t.Cleanup(func() { ts.Close() })
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn}))
	orchID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIORCH,
		Cell:      "zone1",
		Name:      "coord-1",
	}
	for _, p := range poolers {
		require.NoError(t, ts.CreateMultiPooler(ctx, p))
	}
	return NewCoordinator(orchID, ts, fc, logger, false), orchID
}

func TestApplyCertifiedRuleChange_RequiresShardKey(t *testing.T) {
	c, orchID := newCertifiedTestCoordinator(t, rpcclient.NewFakeClient(), nil)
	leader := makePoolerState("zone1", "mp1").MultiPooler.Id
	_, rule, cert := makeCertifiedRequest(0, leader, []*clustermetadatapb.ID{leader}, orchID)

	err := c.ApplyCertifiedRuleChange(context.Background(), nil, rule, cert, "test")
	require.Error(t, err)
	assert.Equal(t, mtrpcpb.Code_INVALID_ARGUMENT, mterrors.Code(err))
}

func TestApplyCertifiedRuleChange_LeaderNotInCohort(t *testing.T) {
	mp1 := makePoolerState("zone1", "mp1")
	mp2 := makePoolerState("zone1", "mp2")
	c, orchID := newCertifiedTestCoordinator(t, rpcclient.NewFakeClient(), []*clustermetadatapb.MultiPooler{mp1.MultiPooler, mp2.MultiPooler})
	// Leader is mp1, but cohort only contains mp2.
	shardKey, rule, cert := makeCertifiedRequest(0, mp1.MultiPooler.Id, []*clustermetadatapb.ID{mp2.MultiPooler.Id}, orchID)

	err := c.ApplyCertifiedRuleChange(context.Background(), shardKey, rule, cert, "test")
	require.Error(t, err)
	assert.Equal(t, mtrpcpb.Code_INVALID_ARGUMENT, mterrors.Code(err))
	assert.Contains(t, err.Error(), "must be a member of proposed_rule.cohort_members")
}

func TestApplyCertifiedRuleChange_RejectsEmptyFrozenLSN(t *testing.T) {
	mp1 := makePoolerState("zone1", "mp1")
	c, orchID := newCertifiedTestCoordinator(t, rpcclient.NewFakeClient(), []*clustermetadatapb.MultiPooler{mp1.MultiPooler})
	shardKey, rule, cert := makeCertifiedRequest(0, mp1.MultiPooler.Id, []*clustermetadatapb.ID{mp1.MultiPooler.Id}, orchID)
	cert.FrozenLsn = ""

	err := c.ApplyCertifiedRuleChange(context.Background(), shardKey, rule, cert, "test")
	require.Error(t, err)
	assert.Equal(t, mtrpcpb.Code_INVALID_ARGUMENT, mterrors.Code(err))
	assert.Contains(t, err.Error(), "frozen_lsn")
}

func TestApplyCertifiedRuleChange_RejectsTermMismatch(t *testing.T) {
	mp1 := makePoolerState("zone1", "mp1")
	c, orchID := newCertifiedTestCoordinator(t, rpcclient.NewFakeClient(), []*clustermetadatapb.MultiPooler{mp1.MultiPooler})
	shardKey, rule, cert := makeCertifiedRequest(0, mp1.MultiPooler.Id, []*clustermetadatapb.ID{mp1.MultiPooler.Id}, orchID)
	// Set proposed rule term to a value that doesn't match the cert's revoked_below_term.
	rule.RuleNumber.CoordinatorTerm = 7

	err := c.ApplyCertifiedRuleChange(context.Background(), shardKey, rule, cert, "test")
	require.Error(t, err)
	assert.Equal(t, mtrpcpb.Code_INVALID_ARGUMENT, mterrors.Code(err))
	assert.Contains(t, err.Error(), "must equal cert.term_revocation.revoked_below_term")
}

func TestApplyCertifiedRuleChange_RejectsRevocationNotAboveOutgoing(t *testing.T) {
	mp1 := makePoolerState("zone1", "mp1")
	c, orchID := newCertifiedTestCoordinator(t, rpcclient.NewFakeClient(), []*clustermetadatapb.MultiPooler{mp1.MultiPooler})
	shardKey, rule, cert := makeCertifiedRequest(5, mp1.MultiPooler.Id, []*clustermetadatapb.ID{mp1.MultiPooler.Id}, orchID)
	// New term must be > outgoing term. Force them equal.
	cert.TermRevocation.OutgoingRule.CoordinatorTerm = cert.TermRevocation.RevokedBelowTerm

	err := c.ApplyCertifiedRuleChange(context.Background(), shardKey, rule, cert, "test")
	require.Error(t, err)
	assert.Equal(t, mtrpcpb.Code_INVALID_ARGUMENT, mterrors.Code(err))
	assert.Contains(t, err.Error(), "must be greater than")
}

func TestApplyCertifiedRuleChange_RejectsMissingTermRevocationFields(t *testing.T) {
	mp1 := makePoolerState("zone1", "mp1")
	c, orchID := newCertifiedTestCoordinator(t, rpcclient.NewFakeClient(), []*clustermetadatapb.MultiPooler{mp1.MultiPooler})
	shardKey, rule, cert := makeCertifiedRequest(0, mp1.MultiPooler.Id, []*clustermetadatapb.ID{mp1.MultiPooler.Id}, orchID)
	cert.TermRevocation.AcceptedCoordinatorId = nil

	err := c.ApplyCertifiedRuleChange(context.Background(), shardKey, rule, cert, "test")
	require.Error(t, err)
	assert.Equal(t, mtrpcpb.Code_INVALID_ARGUMENT, mterrors.Code(err))
	assert.Contains(t, err.Error(), "accepted_coordinator_id")
}

func TestApplyCertifiedRuleChange_PoolerAheadOfOutgoingRuleRejected(t *testing.T) {
	mp1 := makePoolerState("zone1", "mp1")
	// The pre-flight shard probe filters poolers by shard key, so the
	// MultiPooler record must carry the matching database/table_group/shard.
	mp1.MultiPooler.ShardKey = &clustermetadatapb.ShardKey{
		Database:   "db1",
		TableGroup: "default",
		Shard:      "0-inf",
	}

	fc := rpcclient.NewFakeClient()
	// mp1 reports rule term 5 (ahead of the cert's outgoing term 2).
	mp1Key := topoclient.MultiPoolerIDString(mp1.MultiPooler.Id)
	fc.ConsensusStatusResponses = map[string]*consensusdatapb.StatusResponse{
		mp1Key: {
			ConsensusStatus: &clustermetadatapb.ConsensusStatus{
				Id: mp1.MultiPooler.Id,
				CurrentPosition: &clustermetadatapb.PoolerPosition{
					Rule: &clustermetadatapb.ShardRule{
						RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 5},
					},
					Lsn: "0/100",
				},
			},
		},
	}

	c, orchID := newCertifiedTestCoordinator(t, fc, []*clustermetadatapb.MultiPooler{mp1.MultiPooler})
	// Outgoing term 2 < observed term 5, so the pre-flight check fires.
	shardKey, rule, cert := makeCertifiedRequest(2, mp1.MultiPooler.Id, []*clustermetadatapb.ID{mp1.MultiPooler.Id}, orchID)

	err := c.ApplyCertifiedRuleChange(context.Background(), shardKey, rule, cert, "test")
	require.Error(t, err)
	assert.Equal(t, mtrpcpb.Code_FAILED_PRECONDITION, mterrors.Code(err))
	assert.Contains(t, err.Error(), "cert is stale")
}
