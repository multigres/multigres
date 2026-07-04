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
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
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
	return NewCoordinator(orchID, ts, fc, logger), orchID
}

func TestApplyCertifiedRuleChange_RequiresShardKey(t *testing.T) {
	c, orchID := newCertifiedTestCoordinator(t, rpcclient.NewFakeClient(), nil)
	leader := makePoolerState("zone1", "mp1").MultiPooler.Id
	_, rule, cert := makeCertifiedRequest(0, leader, []*clustermetadatapb.ID{leader}, orchID)

	err := c.ApplyCertifiedRuleChange(context.Background(), nil, &clustermetadatapb.RulePosition{Proposal: rule}, cert, "test")
	require.Error(t, err)
	assert.Equal(t, mtrpcpb.Code_INVALID_ARGUMENT, mterrors.Code(err))
}

func TestApplyCertifiedRuleChange_LeaderNotInCohort(t *testing.T) {
	mp1 := makePoolerState("zone1", "mp1")
	mp2 := makePoolerState("zone1", "mp2")
	c, orchID := newCertifiedTestCoordinator(t, rpcclient.NewFakeClient(), []*clustermetadatapb.MultiPooler{mp1.MultiPooler, mp2.MultiPooler})
	// Leader is mp1, but cohort only contains mp2.
	shardKey, rule, cert := makeCertifiedRequest(0, mp1.MultiPooler.Id, []*clustermetadatapb.ID{mp2.MultiPooler.Id}, orchID)

	err := c.ApplyCertifiedRuleChange(context.Background(), shardKey, &clustermetadatapb.RulePosition{Proposal: rule}, cert, "test")
	require.Error(t, err)
	assert.Equal(t, mtrpcpb.Code_INVALID_ARGUMENT, mterrors.Code(err))
	assert.Contains(t, err.Error(), "must be a member of proposed_rule.cohort_members")
}

func TestApplyCertifiedRuleChange_RejectsEmptyFrozenLSN(t *testing.T) {
	mp1 := makePoolerState("zone1", "mp1")
	c, orchID := newCertifiedTestCoordinator(t, rpcclient.NewFakeClient(), []*clustermetadatapb.MultiPooler{mp1.MultiPooler})
	shardKey, rule, cert := makeCertifiedRequest(0, mp1.MultiPooler.Id, []*clustermetadatapb.ID{mp1.MultiPooler.Id}, orchID)
	cert.FrozenLsn = ""

	err := c.ApplyCertifiedRuleChange(context.Background(), shardKey, &clustermetadatapb.RulePosition{Proposal: rule}, cert, "test")
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

	err := c.ApplyCertifiedRuleChange(context.Background(), shardKey, &clustermetadatapb.RulePosition{Proposal: rule}, cert, "test")
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

	err := c.ApplyCertifiedRuleChange(context.Background(), shardKey, &clustermetadatapb.RulePosition{Proposal: rule}, cert, "test")
	require.Error(t, err)
	assert.Equal(t, mtrpcpb.Code_INVALID_ARGUMENT, mterrors.Code(err))
	assert.Contains(t, err.Error(), "must be greater than")
}

func TestApplyCertifiedRuleChange_RejectsMissingTermRevocationFields(t *testing.T) {
	mp1 := makePoolerState("zone1", "mp1")
	c, orchID := newCertifiedTestCoordinator(t, rpcclient.NewFakeClient(), []*clustermetadatapb.MultiPooler{mp1.MultiPooler})
	shardKey, rule, cert := makeCertifiedRequest(0, mp1.MultiPooler.Id, []*clustermetadatapb.ID{mp1.MultiPooler.Id}, orchID)
	cert.TermRevocation.AcceptedCoordinatorId = nil

	err := c.ApplyCertifiedRuleChange(context.Background(), shardKey, &clustermetadatapb.RulePosition{Proposal: rule}, cert, "test")
	require.Error(t, err)
	assert.Equal(t, mtrpcpb.Code_INVALID_ARGUMENT, mterrors.Code(err))
	assert.Contains(t, err.Error(), "accepted_coordinator_id")
}

// TestValidateCertifiedRuleChange exercises validateCertifiedRuleChange's
// per-field guards directly. The branches go through ApplyCertifiedRuleChange
// in other tests too, but this avoids the cohort-lookup setup for each case.
func TestValidateCertifiedRuleChange(t *testing.T) {
	orchID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIORCH, Cell: "zone1", Name: "coord-1"}
	leaderID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "mp1"}
	// Use factories so each subtest gets fresh, independent proto messages —
	// mutating a shared instance bleeds state across cases.
	makeShardKey := func() *clustermetadatapb.ShardKey {
		return &clustermetadatapb.ShardKey{Database: "db1", TableGroup: "default", Shard: "0-inf"}
	}
	makeRule := func() *clustermetadatapb.ShardRule {
		return &clustermetadatapb.ShardRule{
			RuleNumber:    &clustermetadatapb.RuleNumber{CoordinatorTerm: 1},
			LeaderId:      leaderID,
			CohortMembers: []*clustermetadatapb.ID{leaderID},
			DurabilityPolicy: &clustermetadatapb.DurabilityPolicy{
				PolicyName:    "AT_LEAST_1",
				PolicyVersion: 1,
				QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_AT_LEAST_N,
				RequiredCount: 1,
			},
			CoordinatorId: orchID,
			CreationTime:  timestamppb.Now(),
		}
	}
	makeCert := func() *clustermetadatapb.ExternallyCertifiedRevocation {
		return &clustermetadatapb.ExternallyCertifiedRevocation{
			FrozenLsn: "0/0",
			TermRevocation: &clustermetadatapb.TermRevocation{
				RevokedBelowTerm:       1,
				AcceptedCoordinatorId:  orchID,
				CoordinatorInitiatedAt: timestamppb.Now(),
				OutgoingRule:           &clustermetadatapb.RuleNumber{},
			},
		}
	}

	tests := []struct {
		name      string
		mutate    func(sk **clustermetadatapb.ShardKey, rule *clustermetadatapb.ShardRule, cert *clustermetadatapb.ExternallyCertifiedRevocation)
		wantMatch string
	}{
		{
			name: "shard_key empty database",
			mutate: func(sk **clustermetadatapb.ShardKey, _ *clustermetadatapb.ShardRule, _ *clustermetadatapb.ExternallyCertifiedRevocation) {
				(*sk).Database = ""
			},
			wantMatch: "shard_key is required",
		},
		{
			name: "shard_key empty table_group",
			mutate: func(sk **clustermetadatapb.ShardKey, _ *clustermetadatapb.ShardRule, _ *clustermetadatapb.ExternallyCertifiedRevocation) {
				(*sk).TableGroup = ""
			},
			wantMatch: "shard_key is required",
		},
		{
			name: "shard_key empty shard",
			mutate: func(sk **clustermetadatapb.ShardKey, _ *clustermetadatapb.ShardRule, _ *clustermetadatapb.ExternallyCertifiedRevocation) {
				(*sk).Shard = ""
			},
			wantMatch: "shard_key is required",
		},
		{
			name: "proposed_rule.leader_id missing",
			mutate: func(_ **clustermetadatapb.ShardKey, rule *clustermetadatapb.ShardRule, _ *clustermetadatapb.ExternallyCertifiedRevocation) {
				rule.LeaderId = nil
			},
			wantMatch: "proposed_rule.leader_id is required",
		},
		{
			name: "proposed_rule.cohort_members empty",
			mutate: func(_ **clustermetadatapb.ShardKey, rule *clustermetadatapb.ShardRule, _ *clustermetadatapb.ExternallyCertifiedRevocation) {
				rule.CohortMembers = nil
			},
			wantMatch: "cohort_members",
		},
		{
			name: "proposed_rule.durability_policy missing",
			mutate: func(_ **clustermetadatapb.ShardKey, rule *clustermetadatapb.ShardRule, _ *clustermetadatapb.ExternallyCertifiedRevocation) {
				rule.DurabilityPolicy = nil
			},
			wantMatch: "durability_policy",
		},
		{
			name: "proposed_rule.rule_number missing",
			mutate: func(_ **clustermetadatapb.ShardKey, rule *clustermetadatapb.ShardRule, _ *clustermetadatapb.ExternallyCertifiedRevocation) {
				rule.RuleNumber = nil
			},
			wantMatch: "rule_number is required",
		},
		{
			name: "proposed_rule.coordinator_id missing",
			mutate: func(_ **clustermetadatapb.ShardKey, rule *clustermetadatapb.ShardRule, _ *clustermetadatapb.ExternallyCertifiedRevocation) {
				rule.CoordinatorId = nil
			},
			wantMatch: "proposed_rule.coordinator_id",
		},
		{
			name: "proposed_rule.creation_time missing",
			mutate: func(_ **clustermetadatapb.ShardKey, rule *clustermetadatapb.ShardRule, _ *clustermetadatapb.ExternallyCertifiedRevocation) {
				rule.CreationTime = nil
			},
			wantMatch: "creation_time",
		},
		{
			name: "cert.term_revocation missing",
			mutate: func(_ **clustermetadatapb.ShardKey, _ *clustermetadatapb.ShardRule, cert *clustermetadatapb.ExternallyCertifiedRevocation) {
				cert.TermRevocation = nil
			},
			wantMatch: "term_revocation is required",
		},
		{
			name: "cert.term_revocation.outgoing_rule missing",
			mutate: func(_ **clustermetadatapb.ShardKey, _ *clustermetadatapb.ShardRule, cert *clustermetadatapb.ExternallyCertifiedRevocation) {
				cert.TermRevocation.OutgoingRule = nil
			},
			wantMatch: "outgoing_rule is required",
		},
		{
			name: "cert.term_revocation.revoked_below_term non-positive",
			mutate: func(_ **clustermetadatapb.ShardKey, _ *clustermetadatapb.ShardRule, cert *clustermetadatapb.ExternallyCertifiedRevocation) {
				cert.TermRevocation.RevokedBelowTerm = 0
			},
			wantMatch: "revoked_below_term must be positive",
		},
		{
			name: "cert.term_revocation.coordinator_initiated_at missing",
			mutate: func(_ **clustermetadatapb.ShardKey, _ *clustermetadatapb.ShardRule, cert *clustermetadatapb.ExternallyCertifiedRevocation) {
				cert.TermRevocation.CoordinatorInitiatedAt = nil
			},
			wantMatch: "coordinator_initiated_at is required",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sk := makeShardKey()
			rule := makeRule()
			cert := makeCert()
			tt.mutate(&sk, rule, cert)

			err := validateCertifiedRuleChange(sk, &clustermetadatapb.RulePosition{Proposal: rule}, cert)
			require.Error(t, err)
			assert.Equal(t, mtrpcpb.Code_INVALID_ARGUMENT, mterrors.Code(err))
			assert.Contains(t, err.Error(), tt.wantMatch)
		})
	}
}

// poolerWithShard is a makePoolerState variant that tags the underlying
// MultiPooler with the canonical test shard. Required for
// refreshShardConsensusStatuses to enumerate it.
func poolerWithShard(cell, name string) *clustermetadatapb.MultiPooler {
	mp := makePoolerState(cell, name).MultiPooler
	mp.ShardKey = &clustermetadatapb.ShardKey{
		Database:   "db1",
		TableGroup: "default",
		Shard:      "0-inf",
	}
	return mp
}

func TestResolveCohort(t *testing.T) {
	mp1 := makePoolerState("zone1", "mp1").MultiPooler
	mp2 := makePoolerState("zone1", "mp2").MultiPooler
	c, _ := newCertifiedTestCoordinator(t, rpcclient.NewFakeClient(),
		[]*clustermetadatapb.MultiPooler{mp1, mp2})

	t.Run("resolves all members", func(t *testing.T) {
		addressByID, cohort, err := c.resolveCohort(t.Context(),
			[]*clustermetadatapb.ID{mp1.Id, mp2.Id})
		require.NoError(t, err)

		// Address map keyed by ClusterIDString.
		require.Len(t, addressByID, 2)
		assert.Equal(t, mp1.Hostname, addressByID[topoclient.ClusterIDString(mp1.Id)].GetHost())
		assert.Equal(t, mp1.GetPortMap()["postgres"], addressByID[topoclient.ClusterIDString(mp1.Id)].GetPostgresPort())
		assert.Equal(t, mp2.Hostname, addressByID[topoclient.ClusterIDString(mp2.Id)].GetHost())

		// Cohort slice preserves order and carries each MultiPooler.
		require.Len(t, cohort, 2)
		assert.Equal(t, mp1.Id.Name, cohort[0].MultiPooler.Id.Name)
		assert.Equal(t, mp2.Id.Name, cohort[1].MultiPooler.Id.Name)
	})

	t.Run("unknown member returns error", func(t *testing.T) {
		unknown := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "ghost"}
		_, _, err := c.resolveCohort(t.Context(),
			[]*clustermetadatapb.ID{mp1.Id, unknown})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to look up cohort member")
		assert.Contains(t, err.Error(), "ghost")
	})

	t.Run("empty cohort returns empty result", func(t *testing.T) {
		addressByID, cohort, err := c.resolveCohort(t.Context(), nil)
		require.NoError(t, err)
		assert.Empty(t, addressByID)
		assert.Empty(t, cohort)
	})
}

func TestRefreshShardConsensusStatuses(t *testing.T) {
	mp1 := poolerWithShard("zone1", "mp1")
	mp2 := poolerWithShard("zone1", "mp2")
	// A pooler in a different shard that should NOT appear in the result.
	mp3OtherShard := makePoolerState("zone1", "mp3").MultiPooler
	mp3OtherShard.ShardKey = &clustermetadatapb.ShardKey{
		Database:   "db1",
		TableGroup: "default",
		Shard:      "other",
	}

	fc := rpcclient.NewFakeClient()
	fc.SetStatusResponse(topoclient.ComponentIDString(mp1.Id), &multipoolermanagerdatapb.StatusResponse{
		ConsensusStatus: &clustermetadatapb.ConsensusStatus{
			Id: mp1.Id,
			CurrentPosition: &clustermetadatapb.PoolerPosition{
				Position: &clustermetadatapb.RulePosition{Decision: &clustermetadatapb.ShardRule{RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 4}}},
				Lsn:      "0/100",
			},
		},
	})
	fc.SetStatusResponse(topoclient.ComponentIDString(mp2.Id), &multipoolermanagerdatapb.StatusResponse{
		ConsensusStatus: &clustermetadatapb.ConsensusStatus{
			Id: mp2.Id,
			CurrentPosition: &clustermetadatapb.PoolerPosition{
				Position: &clustermetadatapb.RulePosition{Decision: &clustermetadatapb.ShardRule{RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 4}}},
				Lsn:      "0/200",
			},
		},
	})

	shardKey := &clustermetadatapb.ShardKey{Database: "db1", TableGroup: "default", Shard: "0-inf"}

	t.Run("returns statuses for matching-shard poolers only", func(t *testing.T) {
		c, _ := newCertifiedTestCoordinator(t, fc, []*clustermetadatapb.MultiPooler{mp1, mp2, mp3OtherShard})
		statuses, err := c.refreshShardConsensusStatuses(t.Context(), shardKey)
		require.NoError(t, err)

		require.Len(t, statuses, 2, "non-matching shard pooler should be filtered out")
		assert.Equal(t, "0/100", statuses[topoclient.ClusterIDString(mp1.Id)].GetCurrentPosition().GetLsn())
		assert.Equal(t, "0/200", statuses[topoclient.ClusterIDString(mp2.Id)].GetCurrentPosition().GetLsn())
		assert.NotContains(t, statuses, topoclient.ClusterIDString(mp3OtherShard.Id))
	})

	t.Run("unreachable pooler is absent, not an error", func(t *testing.T) {
		fc := rpcclient.NewFakeClient()
		fc.SetStatusResponse(topoclient.ComponentIDString(mp1.Id), &multipoolermanagerdatapb.StatusResponse{
			ConsensusStatus: &clustermetadatapb.ConsensusStatus{Id: mp1.Id},
		})
		// mp2 errors — it should silently drop out.
		fc.Errors[topoclient.ComponentIDString(mp2.Id)] = errors.New("network down")

		c, _ := newCertifiedTestCoordinator(t, fc, []*clustermetadatapb.MultiPooler{mp1, mp2})
		statuses, err := c.refreshShardConsensusStatuses(t.Context(), shardKey)
		require.NoError(t, err, "unreachable poolers should be reported as absent, not failure")

		require.Len(t, statuses, 1)
		assert.Contains(t, statuses, topoclient.ClusterIDString(mp1.Id))
	})

	t.Run("no poolers in shard returns empty map", func(t *testing.T) {
		c, _ := newCertifiedTestCoordinator(t, rpcclient.NewFakeClient(), nil)
		statuses, err := c.refreshShardConsensusStatuses(t.Context(), shardKey)
		require.NoError(t, err)
		assert.Empty(t, statuses)
	})
}
