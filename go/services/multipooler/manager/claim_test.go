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

package manager

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/multigres/multigres/go/common/mterrors"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
)

// acquireActionLockCtx returns a context with the action lock held. Callers
// should defer releasing via the returned lock.
func acquireActionLockCtx(t *testing.T) (context.Context, *ActionLock) {
	t.Helper()
	lock := NewActionLock()
	ctx, err := lock.Acquire(context.Background(), "claim-test")
	require.NoError(t, err)
	t.Cleanup(func() {
		// Ignore panics if already released by test.
		defer func() { _ = recover() }()
		lock.Release(ctx)
	})
	return ctx, lock
}

func newValidClaim(t *testing.T, term int64, coord string, initiatedAt time.Time) *consensusdatapb.CoordinatorClaim {
	t.Helper()
	return &consensusdatapb.CoordinatorClaim{
		Term:                   term,
		CoordinatorId:          &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIORCH, Name: coord},
		CoordinatorInitiatedAt: timestamppb.New(initiatedAt),
	}
}

func TestCheckClaim_RejectsNilFields(t *testing.T) {
	cs, _ := newTestConsensusState(t)
	ctx, _ := acquireActionLockCtx(t)

	cases := []struct {
		name  string
		claim *consensusdatapb.CoordinatorClaim
	}{
		{"nil claim", nil},
		{"nil timestamp", &consensusdatapb.CoordinatorClaim{Term: 1, CoordinatorId: &clustermetadatapb.ID{Name: "mo1"}}},
		{"nil coord id", &consensusdatapb.CoordinatorClaim{Term: 1, CoordinatorInitiatedAt: timestamppb.Now()}},
		{"zero term", &consensusdatapb.CoordinatorClaim{CoordinatorId: &clustermetadatapb.ID{Name: "mo1"}, CoordinatorInitiatedAt: timestamppb.Now()}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := cs.CheckClaim(ctx, tc.claim)
			require.Error(t, err)
			require.Equal(t, mtrpcpb.Code_INVALID_ARGUMENT, mterrors.Code(err))
		})
	}
}

func TestApplyClaim_AcceptsNewHigherTerm(t *testing.T) {
	cs, _ := newTestConsensusState(t)
	ctx, _ := acquireActionLockCtx(t)
	claim := newValidClaim(t, 5, "mo1", time.Unix(1000, 0))
	require.NoError(t, cs.ApplyClaim(ctx, claim))

	term, err := cs.GetTerm(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(5), term.GetTermNumber())
	require.True(t, proto.Equal(claim.CoordinatorId, term.GetAcceptedTermFromCoordinatorId()))
	require.True(t, proto.Equal(claim.CoordinatorInitiatedAt, term.GetCoordinatorInitiatedAt()))
}

func TestApplyClaim_IdempotentOnExactMatch(t *testing.T) {
	cs, _ := newTestConsensusState(t)
	ctx, _ := acquireActionLockCtx(t)
	claim := newValidClaim(t, 7, "mo1", time.Unix(2000, 0))
	require.NoError(t, cs.ApplyClaim(ctx, claim))
	require.NoError(t, cs.ApplyClaim(ctx, claim))

	term, err := cs.GetTerm(ctx)
	require.NoError(t, err)
	require.True(t, proto.Equal(claim.CoordinatorInitiatedAt, term.GetCoordinatorInitiatedAt()))
}

func TestApplyClaim_RejectsSameTermDifferentInitiatedAt(t *testing.T) {
	cs, _ := newTestConsensusState(t)
	ctx, _ := acquireActionLockCtx(t)
	first := newValidClaim(t, 7, "mo1", time.Unix(2000, 0))
	require.NoError(t, cs.ApplyClaim(ctx, first))

	second := newValidClaim(t, 7, "mo1", time.Unix(2001, 0))
	err := cs.ApplyClaim(ctx, second)
	require.Error(t, err)
	require.Contains(t, err.Error(), "coordinator likely restarted")

	term, err := cs.GetTerm(ctx)
	require.NoError(t, err)
	require.True(t, proto.Equal(first.CoordinatorInitiatedAt, term.GetCoordinatorInitiatedAt()),
		"stored state must not change on rejection")
}

func TestApplyClaim_RejectsSameTermDifferentCoordinator(t *testing.T) {
	cs, _ := newTestConsensusState(t)
	ctx, _ := acquireActionLockCtx(t)
	require.NoError(t, cs.ApplyClaim(ctx, newValidClaim(t, 7, "mo1", time.Unix(2000, 0))))
	err := cs.ApplyClaim(ctx, newValidClaim(t, 7, "mo2", time.Unix(2000, 0)))
	require.Error(t, err)
	require.Contains(t, err.Error(), "already accepted term from mo1")
}

func TestApplyClaim_RejectsLowerTerm(t *testing.T) {
	cs, _ := newTestConsensusState(t)
	ctx, _ := acquireActionLockCtx(t)
	require.NoError(t, cs.ApplyClaim(ctx, newValidClaim(t, 10, "mo1", time.Unix(3000, 0))))
	err := cs.ApplyClaim(ctx, newValidClaim(t, 9, "mo1", time.Unix(3000, 0)))
	require.Error(t, err)
}

func TestApplyClaimExactTerm_AcceptsExactMatch(t *testing.T) {
	cs, _ := newTestConsensusState(t)
	ctx, _ := acquireActionLockCtx(t)
	claim := newValidClaim(t, 7, "mo1", time.Unix(2000, 0))
	require.NoError(t, cs.ApplyClaim(ctx, claim))
	require.NoError(t, cs.ApplyClaimExactTerm(ctx, claim))
}

func TestApplyClaimExactTerm_RejectsHigherClaim(t *testing.T) {
	cs, _ := newTestConsensusState(t)
	ctx, _ := acquireActionLockCtx(t)
	require.NoError(t, cs.ApplyClaim(ctx, newValidClaim(t, 5, "mo1", time.Unix(1000, 0))))
	err := cs.ApplyClaimExactTerm(ctx, newValidClaim(t, 6, "mo1", time.Unix(2000, 0)))
	require.Error(t, err)
	require.Contains(t, err.Error(), "exact term match required")
}

func TestApplyClaimExactTerm_RejectsLowerClaim(t *testing.T) {
	cs, _ := newTestConsensusState(t)
	ctx, _ := acquireActionLockCtx(t)
	require.NoError(t, cs.ApplyClaim(ctx, newValidClaim(t, 5, "mo1", time.Unix(1000, 0))))
	err := cs.ApplyClaimExactTerm(ctx, newValidClaim(t, 4, "mo1", time.Unix(1000, 0)))
	require.Error(t, err)
}
