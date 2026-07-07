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
	"context"
	"errors"
	"log/slog"
	"net"
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiadminpb "github.com/multigres/multigres/go/pb/multiadmin"
	multiorchpb "github.com/multigres/multigres/go/pb/multiorch"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/tools/prototest"
)

func newTestServer(t *testing.T, cells ...string) *MultiadminServer {
	t.Helper()
	if len(cells) == 0 {
		cells = []string{"cell1"}
	}
	ts := memorytopo.NewServer(t.Context(), cells...)
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	return NewMultiadminServer(ts, logger, grpc.WithTransportCredentials(insecure.NewCredentials()))
}

func poolerID(cell, name string) *clustermetadatapb.ID {
	return &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: cell, Name: name}
}

func orchID(cell, name string) *clustermetadatapb.ID {
	return &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIORCH, Cell: cell, Name: name}
}

func makePooler(cell, name string) *clustermetadatapb.Multipooler {
	return &clustermetadatapb.Multipooler{
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

func makeOrch(cell, name string) *clustermetadatapb.Multiorch {
	return &clustermetadatapb.Multiorch{
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
	require.NoError(t, s.ts.RegisterMultiorch(ctx, makeOrch("cellA", "orchA"), false))
	require.NoError(t, s.ts.RegisterMultiorch(ctx, makeOrch("cellB", "orchB"), false))

	leader := poolerID("cellB", "leader")
	orch, err := s.pickOrch(ctx, leader)
	require.NoError(t, err)
	assert.Equal(t, "cellB", orch.Id.Cell, "should prefer orch in the leader's cell")
}

func TestPickOrch_FallsBackToAnyCell(t *testing.T) {
	ctx := t.Context()
	s := newTestServer(t, "cellA", "cellB")
	// Only cellA has an orch; leader is in cellB.
	require.NoError(t, s.ts.RegisterMultiorch(ctx, makeOrch("cellA", "orchA"), false))

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
	_, err := s.probeMostAdvanced(ctx, cohort, atLeastN(1))
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
	require.NoError(t, s.ts.CreateMultipooler(ctx, mp1))
	require.NoError(t, s.ts.CreateMultipooler(ctx, mp2))
	require.NoError(t, s.ts.CreateMultipooler(ctx, mp3))

	// Fake Status responses: mp2 has the highest (rule, lsn).
	fc := rpcclient.NewFakeClient()
	for _, p := range []struct {
		pooler *clustermetadatapb.Multipooler
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
					Position: &clustermetadatapb.RulePosition{Decision: &clustermetadatapb.ShardRule{RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: p.term}}},
					Lsn:      p.lsn,
				},
			},
		})
	}
	s.SetRPCClient(fc)

	best, err := s.probeMostAdvanced(ctx,
		[]*clustermetadatapb.ID{mp1.Id, mp2.Id, mp3.Id},
		atLeastN(2),
	)
	require.NoError(t, err)
	assert.Equal(t, int64(5), best.GetPosition().GetDecision().GetRuleNumber().GetCoordinatorTerm())
	assert.Equal(t, "0/200", best.GetLsn())
}

func TestProbeMostAdvanced_InsufficientCohortRecruitment(t *testing.T) {
	ctx := t.Context()
	s := newTestServer(t)

	mp1 := makePooler("cell1", "mp1")
	mp2 := makePooler("cell1", "mp2")
	mp3 := makePooler("cell1", "mp3")
	require.NoError(t, s.ts.CreateMultipooler(ctx, mp1))
	require.NoError(t, s.ts.CreateMultipooler(ctx, mp2))
	require.NoError(t, s.ts.CreateMultipooler(ctx, mp3))

	// Only mp1 responds; with AT_LEAST_2 across 3-member cohort, that's
	// insufficient recruitment.
	fc := rpcclient.NewFakeClient()
	fc.SetStatusResponse(mpKey(mp1.Id), &multipoolermanagerdatapb.StatusResponse{
		ConsensusStatus: &clustermetadatapb.ConsensusStatus{
			Id: mp1.Id,
			CurrentPosition: &clustermetadatapb.PoolerPosition{
				Position: &clustermetadatapb.RulePosition{Decision: &clustermetadatapb.ShardRule{RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 1}}},
				Lsn:      "0/100",
			},
		},
	})
	fc.Errors[mpKey(mp2.Id)] = errors.New("mp2 unreachable")
	fc.Errors[mpKey(mp3.Id)] = errors.New("mp3 unreachable")
	s.SetRPCClient(fc)

	_, err := s.probeMostAdvanced(ctx,
		[]*clustermetadatapb.ID{mp1.Id, mp2.Id, mp3.Id},
		atLeastN(2),
	)
	require.Error(t, err)
	st, _ := status.FromError(err)
	assert.Equal(t, codes.Unavailable, st.Code())
	assert.Contains(t, st.Message(), "insufficient cohort responses")
}

func TestFindRuleByNumber_Found(t *testing.T) {
	ctx := t.Context()
	s := newTestServer(t)

	mp1 := makePooler("cell1", "mp1")
	mp2 := makePooler("cell1", "mp2")
	require.NoError(t, s.ts.CreateMultipooler(ctx, mp1))
	require.NoError(t, s.ts.CreateMultipooler(ctx, mp2))

	fc := rpcclient.NewFakeClient()
	// mp1 reports a decided rule at term 3; mp2's own proposal (undecided,
	// beyond its decision) is at term 3 too — findRuleByNumber should match
	// either via PossiblyUndecidedRule, and returns the full ShardRule
	// content (here, its cohort_members), not just the bare rule number.
	fc.SetStatusResponse(mpKey(mp1.Id), &multipoolermanagerdatapb.StatusResponse{
		ConsensusStatus: &clustermetadatapb.ConsensusStatus{
			Id: mp1.Id,
			CurrentPosition: &clustermetadatapb.PoolerPosition{
				Position: &clustermetadatapb.RulePosition{Decision: &clustermetadatapb.ShardRule{
					RuleNumber:    &clustermetadatapb.RuleNumber{CoordinatorTerm: 2},
					CohortMembers: []*clustermetadatapb.ID{mp1.Id},
				}},
				Lsn: "0/100",
			},
		},
	})
	fc.SetStatusResponse(mpKey(mp2.Id), &multipoolermanagerdatapb.StatusResponse{
		ConsensusStatus: &clustermetadatapb.ConsensusStatus{
			Id: mp2.Id,
			CurrentPosition: &clustermetadatapb.PoolerPosition{
				Position: &clustermetadatapb.RulePosition{
					Decision: &clustermetadatapb.ShardRule{RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 2}},
					Proposal: &clustermetadatapb.ShardRule{
						RuleNumber:    &clustermetadatapb.RuleNumber{CoordinatorTerm: 3},
						CohortMembers: []*clustermetadatapb.ID{mp1.Id, mp2.Id},
					},
				},
				Lsn: "0/200",
			},
		},
	})
	s.SetRPCClient(fc)

	rule, err := s.findRuleByNumber(ctx, []*clustermetadatapb.ID{mp1.Id, mp2.Id}, &clustermetadatapb.RuleNumber{CoordinatorTerm: 3})
	require.NoError(t, err)
	require.NotNil(t, rule)
	assert.Len(t, rule.GetCohortMembers(), 2, "should return mp2's full proposal content, not a bare rule number")
}

func TestFindRuleByNumber_NotFound(t *testing.T) {
	ctx := t.Context()
	s := newTestServer(t)

	mp1 := makePooler("cell1", "mp1")
	require.NoError(t, s.ts.CreateMultipooler(ctx, mp1))

	fc := rpcclient.NewFakeClient()
	fc.SetStatusResponse(mpKey(mp1.Id), &multipoolermanagerdatapb.StatusResponse{
		ConsensusStatus: &clustermetadatapb.ConsensusStatus{
			Id: mp1.Id,
			CurrentPosition: &clustermetadatapb.PoolerPosition{
				Position: &clustermetadatapb.RulePosition{Decision: &clustermetadatapb.ShardRule{RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 2}}},
				Lsn:      "0/100",
			},
		},
	})
	s.SetRPCClient(fc)

	_, err := s.findRuleByNumber(ctx, []*clustermetadatapb.ID{mp1.Id}, &clustermetadatapb.RuleNumber{CoordinatorTerm: 9})
	require.Error(t, err)
	st, _ := status.FromError(err)
	assert.Equal(t, codes.NotFound, st.Code())
	assert.Contains(t, st.Message(), "no reachable cohort member reports rule")
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
	assert.Contains(t, st.Message(), "proposed_transition")
}

func TestApplyCertifiedRuleChange_RejectsMissingCertSource(t *testing.T) {
	s := newTestServer(t)
	_, err := s.ApplyCertifiedRuleChange(t.Context(), &multiadminpb.ApplyCertifiedRuleChangeRequest{
		ShardKey:           &clustermetadatapb.ShardKey{Database: "db1", TableGroup: "default", Shard: "0-inf"},
		ProposedTransition: &clustermetadatapb.RulePosition{Proposal: &clustermetadatapb.ShardRule{}},
	})
	require.Error(t, err)
	st, _ := status.FromError(err)
	assert.Equal(t, codes.InvalidArgument, st.Code())
	assert.Contains(t, st.Message(), "cert")
}

// mpKey returns the key the fake client uses to look up per-pooler responses.
// Mirrors topoclient.ComponentIDString (the function the FakeClient calls
// to key its response maps).
func mpKey(id *clustermetadatapb.ID) topoclient.ComponentID {
	return topoclient.ComponentIDString(id)
}

// ---- buildCert ----

func TestBuildCert_ExplicitCert_Cloned(t *testing.T) {
	ctx := t.Context()
	s := newTestServer(t)
	mp1 := makePooler("cell1", "mp1")
	require.NoError(t, s.ts.CreateMultipooler(ctx, mp1))
	fc := rpcclient.NewFakeClient()
	fc.SetStatusResponse(mpKey(mp1.Id), &multipoolermanagerdatapb.StatusResponse{
		ConsensusStatus: &clustermetadatapb.ConsensusStatus{
			Id: mp1.Id,
			CurrentPosition: &clustermetadatapb.PoolerPosition{
				Position: &clustermetadatapb.RulePosition{Decision: &clustermetadatapb.ShardRule{RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 3}}},
				Lsn:      "0/100",
			},
		},
	})
	s.SetRPCClient(fc)

	original := &clustermetadatapb.ExternallyCertifiedRevocation{
		FrozenLsn: "0/100",
		TermRevocation: &clustermetadatapb.TermRevocation{
			OutgoingRule: &clustermetadatapb.RuleNumber{CoordinatorTerm: 3},
		},
	}
	req := &multiadminpb.ApplyCertifiedRuleChangeRequest{
		CertSource: &multiadminpb.ApplyCertifiedRuleChangeRequest_Cert{Cert: original},
	}

	_, got, err := s.buildCert(ctx, req, &clustermetadatapb.ShardRule{CohortMembers: []*clustermetadatapb.ID{mp1.Id}})
	require.NoError(t, err)
	prototest.AssertEqual(t, original, got)

	// Mutating the result must not affect the caller's input.
	got.FrozenLsn = "9/9999"
	assert.Equal(t, "0/100", original.GetFrozenLsn(), "buildCert should clone, not alias")
}

func TestBuildCert_ExplicitCert_Nil(t *testing.T) {
	s := newTestServer(t)
	req := &multiadminpb.ApplyCertifiedRuleChangeRequest{
		CertSource: &multiadminpb.ApplyCertifiedRuleChangeRequest_Cert{Cert: nil},
	}
	_, _, err := s.buildCert(t.Context(), req, &clustermetadatapb.ShardRule{})
	require.Error(t, err)
	st, _ := status.FromError(err)
	assert.Equal(t, codes.InvalidArgument, st.Code())
	assert.Contains(t, st.Message(), "cert source is empty")
}

func TestBuildCert_UnsafeDerive_UsesProbe(t *testing.T) {
	ctx := t.Context()
	s := newTestServer(t)

	mp1 := makePooler("cell1", "mp1")
	mp2 := makePooler("cell1", "mp2")
	require.NoError(t, s.ts.CreateMultipooler(ctx, mp1))
	require.NoError(t, s.ts.CreateMultipooler(ctx, mp2))

	fc := rpcclient.NewFakeClient()
	fc.SetStatusResponse(mpKey(mp1.Id), &multipoolermanagerdatapb.StatusResponse{
		ConsensusStatus: &clustermetadatapb.ConsensusStatus{
			Id: mp1.Id,
			CurrentPosition: &clustermetadatapb.PoolerPosition{
				Position: &clustermetadatapb.RulePosition{Decision: &clustermetadatapb.ShardRule{RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 4}}},
				Lsn:      "0/200",
			},
		},
	})
	fc.SetStatusResponse(mpKey(mp2.Id), &multipoolermanagerdatapb.StatusResponse{
		ConsensusStatus: &clustermetadatapb.ConsensusStatus{
			Id: mp2.Id,
			CurrentPosition: &clustermetadatapb.PoolerPosition{
				Position: &clustermetadatapb.RulePosition{Decision: &clustermetadatapb.ShardRule{RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 4}}},
				Lsn:      "0/300",
			},
		},
	})
	s.SetRPCClient(fc)

	req := &multiadminpb.ApplyCertifiedRuleChangeRequest{
		CertSource: &multiadminpb.ApplyCertifiedRuleChangeRequest_UnsafeDeriveCert{
			UnsafeDeriveCert: &multiadminpb.UnsafeDeriveCertOptions{},
		},
	}
	rule := &clustermetadatapb.ShardRule{
		CohortMembers:    []*clustermetadatapb.ID{mp1.Id, mp2.Id},
		DurabilityPolicy: atLeastN(2),
	}

	decision, got, err := s.buildCert(ctx, req, rule)
	require.NoError(t, err)
	assert.Equal(t, int64(4), decision.GetRuleNumber().GetCoordinatorTerm())
	prototest.AssertEqual(t, &clustermetadatapb.ExternallyCertifiedRevocation{
		FrozenLsn: "0/300",
		TermRevocation: &clustermetadatapb.TermRevocation{
			OutgoingRule: &clustermetadatapb.RuleNumber{CoordinatorTerm: 4},
		},
	}, got)
}

func TestBuildCert_UnsafeDerive_PropagatesProbeError(t *testing.T) {
	ctx := t.Context()
	s := newTestServer(t)
	s.SetRPCClient(rpcclient.NewFakeClient())

	req := &multiadminpb.ApplyCertifiedRuleChangeRequest{
		CertSource: &multiadminpb.ApplyCertifiedRuleChangeRequest_UnsafeDeriveCert{
			UnsafeDeriveCert: &multiadminpb.UnsafeDeriveCertOptions{},
		},
	}
	rule := &clustermetadatapb.ShardRule{
		CohortMembers:    []*clustermetadatapb.ID{poolerID("cell1", "missing")},
		DurabilityPolicy: atLeastN(1),
	}

	_, _, err := s.buildCert(ctx, req, rule)
	require.Error(t, err)
	st, _ := status.FromError(err)
	assert.Equal(t, codes.NotFound, st.Code())
}

func TestBuildCert_NilCertSource(t *testing.T) {
	s := newTestServer(t)
	_, _, err := s.buildCert(t.Context(), &multiadminpb.ApplyCertifiedRuleChangeRequest{}, &clustermetadatapb.ShardRule{})
	require.Error(t, err)
	st, _ := status.FromError(err)
	assert.Equal(t, codes.InvalidArgument, st.Code())
	assert.Contains(t, st.Message(), "unknown cert_source variant")
}

// ---- end-to-end ApplyCertifiedRuleChange forwarding ----

// fakeMultiorchServer captures the last ApplyCertifiedRuleChange request the
// multiadmin forwarded, and returns either success or a configured error.
type fakeMultiorchServer struct {
	multiorchpb.UnimplementedMultiorchServiceServer
	mu       sync.Mutex
	received *multiorchpb.ApplyCertifiedRuleChangeRequest
	err      error
}

func (f *fakeMultiorchServer) ApplyCertifiedRuleChange(_ context.Context, req *multiorchpb.ApplyCertifiedRuleChangeRequest) (*multiorchpb.ApplyCertifiedRuleChangeResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.received = req
	if f.err != nil {
		return nil, f.err
	}
	return &multiorchpb.ApplyCertifiedRuleChangeResponse{}, nil
}

// startFakeOrch spins up a bufconn-backed gRPC server hosting the fake orch
// and returns a gatewayDialer that routes through it. The server is stopped
// via t.Cleanup.
func startFakeOrch(t *testing.T, fake *fakeMultiorchServer) func(context.Context, string) (*grpc.ClientConn, error) {
	t.Helper()
	lis := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	multiorchpb.RegisterMultiorchServiceServer(grpcServer, fake)
	go func() { _ = grpcServer.Serve(lis) }()
	t.Cleanup(func() {
		grpcServer.Stop()
		_ = lis.Close()
	})
	return func(_ context.Context, _ string) (*grpc.ClientConn, error) {
		return grpc.NewClient(
			"passthrough:///bufnet",
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return lis.Dial()
			}),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
	}
}

// validApplyRequest returns a complete multiadmin request that should pass
// validation and reach the orch.
func validApplyRequest(leader *clustermetadatapb.ID, cohort []*clustermetadatapb.ID) *multiadminpb.ApplyCertifiedRuleChangeRequest {
	return &multiadminpb.ApplyCertifiedRuleChangeRequest{
		ShardKey: &clustermetadatapb.ShardKey{Database: "db1", TableGroup: "default", Shard: "0-inf"},
		ProposedTransition: &clustermetadatapb.RulePosition{
			Proposal: &clustermetadatapb.ShardRule{
				LeaderId:         leader,
				CohortMembers:    cohort,
				DurabilityPolicy: atLeastN(2),
			},
		},
		Reason: "test forward",
		CertSource: &multiadminpb.ApplyCertifiedRuleChangeRequest_Cert{
			Cert: &clustermetadatapb.ExternallyCertifiedRevocation{
				FrozenLsn: "0/100",
				TermRevocation: &clustermetadatapb.TermRevocation{
					OutgoingRule: &clustermetadatapb.RuleNumber{CoordinatorTerm: 2},
				},
			},
		},
	}
}

func TestApplyCertifiedRuleChange_ForwardsToOrch(t *testing.T) {
	ctx := t.Context()
	s := newTestServer(t, "cell1")

	mp1 := makePooler("cell1", "mp1")
	mp2 := makePooler("cell1", "mp2")
	require.NoError(t, s.ts.CreateMultipooler(ctx, mp1))
	require.NoError(t, s.ts.CreateMultipooler(ctx, mp2))
	require.NoError(t, s.ts.RegisterMultiorch(ctx, makeOrch("cell1", "orch1"), false))

	fc := rpcclient.NewFakeClient()
	for _, id := range []*clustermetadatapb.ID{mp1.Id, mp2.Id} {
		fc.SetStatusResponse(mpKey(id), &multipoolermanagerdatapb.StatusResponse{
			ConsensusStatus: &clustermetadatapb.ConsensusStatus{
				Id: id,
				CurrentPosition: &clustermetadatapb.PoolerPosition{
					Position: &clustermetadatapb.RulePosition{Decision: &clustermetadatapb.ShardRule{RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 2}}},
					Lsn:      "0/100",
				},
			},
		})
	}
	s.SetRPCClient(fc)

	fake := &fakeMultiorchServer{}
	s.gatewayDialer = startFakeOrch(t, fake)

	req := validApplyRequest(mp1.Id, []*clustermetadatapb.ID{mp1.Id, mp2.Id})
	resp, err := s.ApplyCertifiedRuleChange(ctx, req)
	require.NoError(t, err)

	// Response echoes back the multiorch-bound proposed rule + cert.
	require.NotNil(t, resp.GetInstalledRule())
	assert.Equal(t, "mp1", resp.GetInstalledRule().GetLeaderId().GetName())
	require.NotNil(t, resp.GetCertUsed())

	// The orch saw the forwarded request with identity/timing fields filled.
	fake.mu.Lock()
	defer fake.mu.Unlock()
	require.NotNil(t, fake.received, "orch should have been called")
	forwardedRule := fake.received.GetProposedTransition().GetProposal()
	require.NotNil(t, forwardedRule.GetCoordinatorId(), "fillIdentityFields should have populated coordinator_id")
	assert.Equal(t, "cell1", forwardedRule.GetCoordinatorId().GetCell())
	assert.Equal(t, "orch1", forwardedRule.GetCoordinatorId().GetName())
	require.NotNil(t, forwardedRule.GetCreationTime(), "fillIdentityFields should have populated creation_time")
	require.NotNil(t, forwardedRule.GetRuleNumber(), "fillIdentityFields should have populated rule_number")
	assert.Equal(t, int64(3), forwardedRule.GetRuleNumber().GetCoordinatorTerm(),
		"derived from cert outgoing_rule (term 2) + 1")

	rev := fake.received.GetCert().GetTermRevocation()
	require.NotNil(t, rev)
	assert.Equal(t, int64(3), rev.GetRevokedBelowTerm())
	require.NotNil(t, rev.GetAcceptedCoordinatorId())
	assert.Equal(t, "orch1", rev.GetAcceptedCoordinatorId().GetName())
}

func TestApplyCertifiedRuleChange_PropagatesOrchError(t *testing.T) {
	ctx := t.Context()
	s := newTestServer(t, "cell1")

	mp1 := makePooler("cell1", "mp1")
	require.NoError(t, s.ts.CreateMultipooler(ctx, mp1))
	require.NoError(t, s.ts.RegisterMultiorch(ctx, makeOrch("cell1", "orch1"), false))

	fc := rpcclient.NewFakeClient()
	fc.SetStatusResponse(mpKey(mp1.Id), &multipoolermanagerdatapb.StatusResponse{
		ConsensusStatus: &clustermetadatapb.ConsensusStatus{
			Id: mp1.Id,
			CurrentPosition: &clustermetadatapb.PoolerPosition{
				Position: &clustermetadatapb.RulePosition{Decision: &clustermetadatapb.ShardRule{RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 2}}},
				Lsn:      "0/100",
			},
		},
	})
	s.SetRPCClient(fc)

	fake := &fakeMultiorchServer{
		err: status.Error(codes.FailedPrecondition, "cohort not ready"),
	}
	s.gatewayDialer = startFakeOrch(t, fake)

	_, err := s.ApplyCertifiedRuleChange(ctx, validApplyRequest(mp1.Id, []*clustermetadatapb.ID{mp1.Id}))
	require.Error(t, err)
	st, _ := status.FromError(err)
	assert.Equal(t, codes.FailedPrecondition, st.Code())
	assert.Contains(t, st.Message(), "cohort not ready")
}

func TestApplyCertifiedRuleChange_NoOrchAvailable(t *testing.T) {
	ctx := t.Context()
	s := newTestServer(t, "cell1")

	mp1 := makePooler("cell1", "mp1")
	require.NoError(t, s.ts.CreateMultipooler(ctx, mp1))
	// No multiorch registered → pickOrch should fail before we attempt to dial.

	_, err := s.ApplyCertifiedRuleChange(ctx, validApplyRequest(mp1.Id, []*clustermetadatapb.ID{mp1.Id}))
	require.Error(t, err)
	st, _ := status.FromError(err)
	assert.Equal(t, codes.FailedPrecondition, st.Code())
}
