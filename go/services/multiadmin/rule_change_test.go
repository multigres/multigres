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

package multiadmin

import (
	"errors"
	"log/slog"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiadminpb "github.com/multigres/multigres/go/pb/multiadmin"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

func newTestServer(t *testing.T, cells ...string) *MultiAdminServer {
	t.Helper()
	if len(cells) == 0 {
		cells = []string{"cell1"}
	}
	ts := memorytopo.NewServer(t.Context(), cells...)
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	return NewMultiAdminServer(ts, logger, grpc.WithTransportCredentials(insecure.NewCredentials()))
}

func poolerID(cell, name string) *clustermetadatapb.ID {
	return &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: cell, Name: name}
}

func orchID(cell, name string) *clustermetadatapb.ID {
	return &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIORCH, Cell: cell, Name: name}
}

func makePooler(cell, name string) *clustermetadatapb.MultiPooler {
	return &clustermetadatapb.MultiPooler{
		Id: poolerID(cell, name),
		ShardKey: &clustermetadatapb.ShardKey{
			Database:   "db1",
			TableGroup: "default",
			Shard:      "0-inf",
		},
		Hostname: "localhost",
		PortMap:  map[string]int32{"postgres": 5432, "grpc": 9000},
	}
}

func makeOrch(cell, name string) *clustermetadatapb.MultiOrch {
	return &clustermetadatapb.MultiOrch{
		Id:       orchID(cell, name),
		Hostname: "localhost",
		PortMap:  map[string]int32{"grpc": 9100},
	}
}

func atLeastN(n int32) *clustermetadatapb.DurabilityPolicy {
	return &clustermetadatapb.DurabilityPolicy{
		PolicyName:    "AT_LEAST_N",
		PolicyVersion: 1,
		QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_AT_LEAST_N,
		RequiredCount: n,
	}
}

// ---- fillIdentityFields ----

func TestFillIdentityFields_PopulatesEmpty(t *testing.T) {
	rule := &clustermetadatapb.ShardRule{}
	cert := &clustermetadatapb.ExternallyCertifiedRevocation{
		TermRevocation: &clustermetadatapb.TermRevocation{
			OutgoingRule: &clustermetadatapb.RuleNumber{CoordinatorTerm: 5},
		},
		FrozenLsn: "0/100",
	}
	orch := orchID("cell1", "orch1")
	now := timestamppb.New(timestamppb.Now().AsTime())

	require.NoError(t, fillIdentityFields(rule, cert, orch, now))

	// Cert revocation auto-populated.
	require.NotNil(t, cert.TermRevocation)
	assert.Equal(t, int64(6), cert.TermRevocation.RevokedBelowTerm, "revoked_below_term should be outgoing + 1")
	assert.Equal(t, orch, cert.TermRevocation.AcceptedCoordinatorId)
	assert.Equal(t, now, cert.TermRevocation.CoordinatorInitiatedAt)

	// Rule identity fields auto-populated.
	require.NotNil(t, rule.RuleNumber)
	assert.Equal(t, int64(6), rule.RuleNumber.CoordinatorTerm)
	assert.Equal(t, orch, rule.CoordinatorId)
	assert.Equal(t, now, rule.CreationTime)
}

func TestFillIdentityFields_PreservesCallerValues(t *testing.T) {
	callerOrch := orchID("cell1", "caller-chose-this")
	callerTime := timestamppb.New(timestamppb.Now().AsTime())
	rule := &clustermetadatapb.ShardRule{
		RuleNumber:    &clustermetadatapb.RuleNumber{CoordinatorTerm: 42},
		CoordinatorId: callerOrch,
		CreationTime:  callerTime,
	}
	cert := &clustermetadatapb.ExternallyCertifiedRevocation{
		FrozenLsn: "0/100",
		TermRevocation: &clustermetadatapb.TermRevocation{
			RevokedBelowTerm:       42,
			AcceptedCoordinatorId:  callerOrch,
			CoordinatorInitiatedAt: callerTime,
			OutgoingRule:           &clustermetadatapb.RuleNumber{CoordinatorTerm: 5},
		},
	}
	// Multiadmin happens to pick the same orch the caller named.
	now := timestamppb.Now()
	require.NoError(t, fillIdentityFields(rule, cert, callerOrch, now))

	// Caller's values preserved.
	assert.Equal(t, int64(42), cert.TermRevocation.RevokedBelowTerm)
	assert.Equal(t, callerTime, cert.TermRevocation.CoordinatorInitiatedAt)
	assert.Equal(t, int64(42), rule.RuleNumber.CoordinatorTerm)
	assert.Equal(t, callerOrch, rule.CoordinatorId)
	assert.Equal(t, callerTime, rule.CreationTime)
}

func TestFillIdentityFields_RejectsCoordinatorMismatch(t *testing.T) {
	callerOrch := orchID("cell1", "caller-chose-this")
	rule := &clustermetadatapb.ShardRule{}
	cert := &clustermetadatapb.ExternallyCertifiedRevocation{
		FrozenLsn: "0/100",
		TermRevocation: &clustermetadatapb.TermRevocation{
			AcceptedCoordinatorId: callerOrch,
			OutgoingRule:          &clustermetadatapb.RuleNumber{CoordinatorTerm: 5},
		},
	}
	differentOrch := orchID("cell2", "multiadmin-chose-this")

	err := fillIdentityFields(rule, cert, differentOrch, timestamppb.Now())
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.InvalidArgument, st.Code())
	assert.Contains(t, st.Message(), "does not match chosen multiorch")
}

func TestFillIdentityFields_BootstrapZeroOutgoing(t *testing.T) {
	rule := &clustermetadatapb.ShardRule{}
	cert := &clustermetadatapb.ExternallyCertifiedRevocation{
		TermRevocation: &clustermetadatapb.TermRevocation{
			OutgoingRule: &clustermetadatapb.RuleNumber{},
		},
		FrozenLsn: "0/0",
	}
	orch := orchID("cell1", "orch1")
	require.NoError(t, fillIdentityFields(rule, cert, orch, timestamppb.Now()))
	assert.Equal(t, int64(1), cert.TermRevocation.RevokedBelowTerm)
	assert.Equal(t, int64(1), rule.RuleNumber.CoordinatorTerm)
}

// ---- pickOrch ----

func TestPickOrch_PrefersLeaderCell(t *testing.T) {
	ctx := t.Context()
	s := newTestServer(t, "cellA", "cellB")
	require.NoError(t, s.ts.RegisterMultiOrch(ctx, makeOrch("cellA", "orchA"), false))
	require.NoError(t, s.ts.RegisterMultiOrch(ctx, makeOrch("cellB", "orchB"), false))

	leader := poolerID("cellB", "leader")
	orch, err := s.pickOrch(ctx, leader)
	require.NoError(t, err)
	assert.Equal(t, "cellB", orch.Id.Cell, "should prefer orch in the leader's cell")
}

func TestPickOrch_FallsBackToAnyCell(t *testing.T) {
	ctx := t.Context()
	s := newTestServer(t, "cellA", "cellB")
	// Only cellA has an orch; leader is in cellB.
	require.NoError(t, s.ts.RegisterMultiOrch(ctx, makeOrch("cellA", "orchA"), false))

	leader := poolerID("cellB", "leader")
	orch, err := s.pickOrch(ctx, leader)
	require.NoError(t, err)
	assert.Equal(t, "cellA", orch.Id.Cell)
}

func TestPickOrch_NoneFound(t *testing.T) {
	ctx := t.Context()
	s := newTestServer(t, "cellA")
	_, err := s.pickOrch(ctx, poolerID("cellA", "leader"))
	require.Error(t, err)
	st, _ := status.FromError(err)
	assert.Equal(t, codes.FailedPrecondition, st.Code())
}

// ---- probeMostAdvanced ----

func TestProbeMostAdvanced_FailsOnMissingPooler(t *testing.T) {
	ctx := t.Context()
	s := newTestServer(t)
	s.SetRPCClient(rpcclient.NewFakeClient())

	cohort := []*clustermetadatapb.ID{poolerID("cell1", "missing")}
	_, _, err := s.probeMostAdvanced(ctx, cohort, atLeastN(1))
	require.Error(t, err)
	st, _ := status.FromError(err)
	assert.Equal(t, codes.NotFound, st.Code())
}

func TestProbeMostAdvanced_PicksMostAdvanced(t *testing.T) {
	ctx := t.Context()
	s := newTestServer(t)

	// Register three poolers.
	mp1 := makePooler("cell1", "mp1")
	mp2 := makePooler("cell1", "mp2")
	mp3 := makePooler("cell1", "mp3")
	require.NoError(t, s.ts.CreateMultiPooler(ctx, mp1))
	require.NoError(t, s.ts.CreateMultiPooler(ctx, mp2))
	require.NoError(t, s.ts.CreateMultiPooler(ctx, mp3))

	// Fake Status responses: mp2 has the highest (rule, lsn).
	fc := rpcclient.NewFakeClient()
	for _, p := range []struct {
		pooler *clustermetadatapb.MultiPooler
		term   int64
		lsn    string
	}{
		{mp1, 3, "0/100"},
		{mp2, 5, "0/200"},
		{mp3, 5, "0/150"},
	} {
		fc.SetStatusResponse(mpKey(p.pooler.Id), &multipoolermanagerdatapb.StatusResponse{
			ConsensusStatus: &clustermetadatapb.ConsensusStatus{
				Id: p.pooler.Id,
				CurrentPosition: &clustermetadatapb.PoolerPosition{
					Rule: &clustermetadatapb.ShardRule{RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: p.term}},
					Lsn:  p.lsn,
				},
			},
		})
	}
	s.SetRPCClient(fc)

	outgoing, lsn, err := s.probeMostAdvanced(ctx,
		[]*clustermetadatapb.ID{mp1.Id, mp2.Id, mp3.Id},
		atLeastN(2),
	)
	require.NoError(t, err)
	assert.Equal(t, int64(5), outgoing.CoordinatorTerm)
	assert.Equal(t, "0/200", lsn)
}

func TestProbeMostAdvanced_InsufficientCohortRecruitment(t *testing.T) {
	ctx := t.Context()
	s := newTestServer(t)

	mp1 := makePooler("cell1", "mp1")
	mp2 := makePooler("cell1", "mp2")
	mp3 := makePooler("cell1", "mp3")
	require.NoError(t, s.ts.CreateMultiPooler(ctx, mp1))
	require.NoError(t, s.ts.CreateMultiPooler(ctx, mp2))
	require.NoError(t, s.ts.CreateMultiPooler(ctx, mp3))

	// Only mp1 responds; with AT_LEAST_2 across 3-member cohort, that's
	// insufficient recruitment.
	fc := rpcclient.NewFakeClient()
	fc.SetStatusResponse(mpKey(mp1.Id), &multipoolermanagerdatapb.StatusResponse{
		ConsensusStatus: &clustermetadatapb.ConsensusStatus{
			Id: mp1.Id,
			CurrentPosition: &clustermetadatapb.PoolerPosition{
				Rule: &clustermetadatapb.ShardRule{RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 1}},
				Lsn:  "0/100",
			},
		},
	})
	fc.Errors[mpKey(mp2.Id)] = errors.New("mp2 unreachable")
	fc.Errors[mpKey(mp3.Id)] = errors.New("mp3 unreachable")
	s.SetRPCClient(fc)

	_, _, err := s.probeMostAdvanced(ctx,
		[]*clustermetadatapb.ID{mp1.Id, mp2.Id, mp3.Id},
		atLeastN(2),
	)
	require.Error(t, err)
	st, _ := status.FromError(err)
	assert.Equal(t, codes.Unavailable, st.Code())
	assert.Contains(t, st.Message(), "insufficient cohort responses")
}

// ---- ApplyCertifiedRuleChange validation ----

func TestApplyCertifiedRuleChange_RejectsMissingShardKey(t *testing.T) {
	s := newTestServer(t)
	_, err := s.ApplyCertifiedRuleChange(t.Context(), &multiadminpb.ApplyCertifiedRuleChangeRequest{})
	require.Error(t, err)
	st, _ := status.FromError(err)
	assert.Equal(t, codes.InvalidArgument, st.Code())
	assert.Contains(t, st.Message(), "shard_key")
}

func TestApplyCertifiedRuleChange_RejectsMissingProposedRule(t *testing.T) {
	s := newTestServer(t)
	_, err := s.ApplyCertifiedRuleChange(t.Context(), &multiadminpb.ApplyCertifiedRuleChangeRequest{
		ShardKey: &clustermetadatapb.ShardKey{Database: "db1", TableGroup: "default", Shard: "0-inf"},
	})
	require.Error(t, err)
	st, _ := status.FromError(err)
	assert.Equal(t, codes.InvalidArgument, st.Code())
	assert.Contains(t, st.Message(), "proposed_rule")
}

func TestApplyCertifiedRuleChange_RejectsMissingCertSource(t *testing.T) {
	s := newTestServer(t)
	_, err := s.ApplyCertifiedRuleChange(t.Context(), &multiadminpb.ApplyCertifiedRuleChangeRequest{
		ShardKey:     &clustermetadatapb.ShardKey{Database: "db1", TableGroup: "default", Shard: "0-inf"},
		ProposedRule: &clustermetadatapb.ShardRule{},
	})
	require.Error(t, err)
	st, _ := status.FromError(err)
	assert.Equal(t, codes.InvalidArgument, st.Code())
	assert.Contains(t, st.Message(), "cert")
}

// mpKey returns the key the fake client uses to look up per-pooler responses.
// Mirrors topoclient.MultiPoolerIDString (the function the FakeClient calls
// to key its response maps).
func mpKey(id *clustermetadatapb.ID) string {
	return topoclient.MultiPoolerIDString(id)
}
