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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/tools/prototest"
)

// coordA and coordB are distinct coordinator IDs used across revocation tests.
var (
	coordA = &clustermetadatapb.ID{Name: "coord-a"}
	coordB = &clustermetadatapb.ID{Name: "coord-b"}

	ts1 = timestamppb.New(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	ts2 = timestamppb.New(time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC))

	// outgoingRuleAt4 is the shared outgoing_rule for revocationAt5 below —
	// term 4, matching positionAtCoordTerm(4)'s decision exactly, so tests
	// using it exercise the revoked_below_term / stored-revocation checks
	// without themselves being rejected by the WAL-position (decision vs.
	// outgoing_rule) check.
	outgoingRuleAt4 = &clustermetadatapb.RuleNumber{CoordinatorTerm: 4}
)

func TestValidateRevocation(t *testing.T) {
	revocationAt5 := &clustermetadatapb.TermRevocation{
		RevokedBelowTerm:       5,
		AcceptedCoordinatorId:  coordA,
		CoordinatorInitiatedAt: ts1,
		OutgoingRule:           outgoingRuleAt4,
	}

	tests := []struct {
		name       string
		status     *clustermetadatapb.ConsensusStatus
		revocation *clustermetadatapb.TermRevocation
		wantErr    string
	}{
		{
			name:       "NilRevocation_Refused",
			status:     nil,
			revocation: nil,
			wantErr:    "revocation is nil",
		},
		{
			name:   "NilCoordinatorID_Refused",
			status: nil,
			revocation: &clustermetadatapb.TermRevocation{
				RevokedBelowTerm:       5,
				CoordinatorInitiatedAt: ts1,
				OutgoingRule:           outgoingRuleAt4,
			},
			wantErr: "accepted_coordinator_id is required",
		},
		{
			name:   "NilTimestamp_Refused",
			status: nil,
			revocation: &clustermetadatapb.TermRevocation{
				RevokedBelowTerm:      5,
				AcceptedCoordinatorId: coordA,
				OutgoingRule:          outgoingRuleAt4,
			},
			wantErr: "coordinator_initiated_at is required",
		},
		{
			name:   "NilOutgoingRule_Refused",
			status: nil,
			revocation: &clustermetadatapb.TermRevocation{
				RevokedBelowTerm:       5,
				AcceptedCoordinatorId:  coordA,
				CoordinatorInitiatedAt: ts1,
			},
			wantErr: "outgoing_rule is required",
		},
		{
			// outgoing_rule.coordinator_term must be strictly less than
			// revoked_below_term. NewTermRevocation always produces values that
			// satisfy this, but a hand-built revocation (e.g. from an external
			// agent constructing a cert) could violate it and ValidateRevocation
			// catches it on read.
			name:   "OutgoingRuleTermAtOrAboveRevokedBelow_Refused",
			status: nil,
			revocation: &clustermetadatapb.TermRevocation{
				RevokedBelowTerm:       5,
				AcceptedCoordinatorId:  coordA,
				CoordinatorInitiatedAt: ts1,
				OutgoingRule:           &clustermetadatapb.RuleNumber{CoordinatorTerm: 5},
			},
			wantErr: "outgoing_rule coordinator_term 5 >= revoked_below_term 5",
		},
		{
			name:       "NilStatus_Refused",
			status:     nil,
			revocation: revocationAt5,
			wantErr:    "cannot accept revocation: unknown WAL position",
		},
		{
			name:       "NilPosition_Refused",
			status:     &clustermetadatapb.ConsensusStatus{},
			revocation: revocationAt5,
			wantErr:    "cannot accept revocation: unknown WAL position",
		},
		{
			name: "BadLsn_Refused",
			status: &clustermetadatapb.ConsensusStatus{
				CurrentPosition: &clustermetadatapb.PoolerPosition{
					Position: &clustermetadatapb.RulePosition{Decision: &clustermetadatapb.ShardRule{
						RuleNumber: &clustermetadatapb.RuleNumber{
							CoordinatorTerm: 4,
						},
					}},
					Lsn: "abc",
				},
			},
			revocation: revocationAt5,
			wantErr:    "cannot accept revocation: failed to parse LSN: unexpected EOF",
		},
		{
			name: "WALSafety_RuleTermBelowRevocation_Accepted",
			status: &clustermetadatapb.ConsensusStatus{
				CurrentPosition: positionAtCoordTerm(4),
			},
			revocation: revocationAt5,
		},
		{
			name: "WALSafety_RuleTermEqualsRevocation_Refused",
			status: &clustermetadatapb.ConsensusStatus{
				CurrentPosition: positionAtCoordTerm(5),
			},
			revocation: revocationAt5,
			wantErr:    "cannot accept revocation: recorded position 5.0 is not revoked by outgoing_rule 4.0 / revoked_below_term 5",
		},
		{
			name: "WALSafety_RuleTermAboveRevocation_Refused",
			status: &clustermetadatapb.ConsensusStatus{
				CurrentPosition: positionAtCoordTerm(7),
			},
			revocation: revocationAt5,
			wantErr:    "cannot accept revocation: recorded position 7.0 is not revoked by outgoing_rule 4.0 / revoked_below_term 5",
		},
		{
			// WAL safety extends to the proposal, not just the decision: the
			// proposal's own rule is ahead of outgoing_rule, so this
			// revocation doesn't reach real WAL content the node already
			// holds. A node's own unconfirmed proposal is not, by itself, a
			// reason to refuse — see ProposalBelowRevocation_Accepted below.
			name: "ProposalAtOrAboveRevocation_Refused",
			status: &clustermetadatapb.ConsensusStatus{
				CurrentPosition: positionWithUndecidedProposal(4, 6),
			},
			revocation: revocationAt5,
			wantErr:    "cannot accept revocation: recorded position 4.0 proposal=6.0 is not revoked by outgoing_rule 4.0 / revoked_below_term 5",
		},
		{
			// The proposal's term is below revoked_below_term, so the
			// revocation is authoritative over both the decision and the
			// outstanding proposal — accepted despite the pending proposal.
			// Whether that proposal should anchor the new leadership round is
			// the coordinator's concern (NewTermRevocation), not this check.
			name: "ProposalBelowRevocation_Accepted",
			status: &clustermetadatapb.ConsensusStatus{
				CurrentPosition: positionWithUndecidedProposal(2, 4),
			},
			revocation: revocationAt5,
		},
		{
			name: "StoredTerm_HigherThanRequested_Refused",
			status: &clustermetadatapb.ConsensusStatus{
				TermRevocation: &clustermetadatapb.TermRevocation{
					RevokedBelowTerm:      10,
					AcceptedCoordinatorId: coordA,
				},
				CurrentPosition: positionAtCoordTerm(4),
			},
			revocation: revocationAt5,
			wantErr:    "already accepted term 10 > requested 5",
		},
		{
			name: "StoredTerm_LowerThanRequested_Accepted",
			status: &clustermetadatapb.ConsensusStatus{
				TermRevocation: &clustermetadatapb.TermRevocation{
					RevokedBelowTerm:      3,
					AcceptedCoordinatorId: coordA,
				},
				CurrentPosition: positionAtCoordTerm(2),
			},
			revocation: revocationAt5,
		},
		{
			name: "SameTerm_SameCoordinator_SameTimestamp_Idempotent",
			status: &clustermetadatapb.ConsensusStatus{
				TermRevocation:  revocationAt5,
				CurrentPosition: positionAtCoordTerm(4),
			},
			revocation: revocationAt5,
		},
		{
			name: "SameTerm_DifferentCoordinator_Refused",
			status: &clustermetadatapb.ConsensusStatus{
				TermRevocation: &clustermetadatapb.TermRevocation{
					RevokedBelowTerm:       5,
					AcceptedCoordinatorId:  coordA,
					CoordinatorInitiatedAt: ts1,
				},
				CurrentPosition: positionAtCoordTerm(4),
			},
			revocation: &clustermetadatapb.TermRevocation{
				RevokedBelowTerm:       5,
				AcceptedCoordinatorId:  coordB,
				CoordinatorInitiatedAt: ts1,
				OutgoingRule:           outgoingRuleAt4,
			},
			wantErr: "already accepted term 5 from coordinator",
		},
		{
			name: "SameTerm_SameCoordinator_DifferentTimestamp_Refused",
			status: &clustermetadatapb.ConsensusStatus{
				TermRevocation: &clustermetadatapb.TermRevocation{
					RevokedBelowTerm:       5,
					AcceptedCoordinatorId:  coordA,
					CoordinatorInitiatedAt: ts1,
				},
				CurrentPosition: positionAtCoordTerm(4),
			},
			revocation: &clustermetadatapb.TermRevocation{
				RevokedBelowTerm:       5,
				AcceptedCoordinatorId:  coordA,
				CoordinatorInitiatedAt: ts2,
				OutgoingRule:           outgoingRuleAt4,
			},
			wantErr: "different coordinator_initiated_at",
		},
		{
			name: "WALAndStoredTerm_BothChecked_WALFails",
			status: &clustermetadatapb.ConsensusStatus{
				CurrentPosition: positionAtCoordTerm(6),
				TermRevocation: &clustermetadatapb.TermRevocation{
					RevokedBelowTerm:      3,
					AcceptedCoordinatorId: coordA,
				},
			},
			revocation: revocationAt5,
			wantErr:    "cannot accept revocation: recorded position 6.0 is not revoked by outgoing_rule 4.0 / revoked_below_term 5",
		},
		{
			// recruit_blocked_until is only ever present in ConsensusStatus
			// while still outstanding (see recruitPositionFloorIfOutstanding),
			// so its mere presence here is enough to refuse — regardless of
			// how the WAL-position and stored-revocation checks would resolve.
			name: "RecruitPositionFloorOutstanding_Refused",
			status: &clustermetadatapb.ConsensusStatus{
				CurrentPosition:     positionAtCoordTerm(4),
				RecruitBlockedUntil: &clustermetadatapb.LsnPosition{Lsn: "0/2000"},
			},
			revocation: revocationAt5,
			wantErr:    "has not caught up to its recruit position floor (floor lsn=0/2000)",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateRevocation(tc.status, tc.revocation)
			if tc.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErr)
			}
		})
	}
}

// positionAtCoordTerm builds a PoolerPosition whose recorded rule is at the
// given coordinator term.
func positionAtCoordTerm(coordTerm int64) *clustermetadatapb.PoolerPosition {
	return &clustermetadatapb.PoolerPosition{
		Position: &clustermetadatapb.RulePosition{Decision: &clustermetadatapb.ShardRule{
			RuleNumber: &clustermetadatapb.RuleNumber{
				CoordinatorTerm: coordTerm,
			},
		}},
		Lsn: "16/B374D848",
	}
}

// positionWithUndecidedProposal builds a position whose decision is at
// decisionTerm but which also carries an outstanding (undecided) proposal
// beyond it — WAL content that reached this node but was never marked
// decided.
func positionWithUndecidedProposal(decisionTerm, proposalTerm int64) *clustermetadatapb.PoolerPosition {
	return &clustermetadatapb.PoolerPosition{
		Position: &clustermetadatapb.RulePosition{
			Decision: &clustermetadatapb.ShardRule{RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: decisionTerm}},
			Proposal: &clustermetadatapb.ShardRule{RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: proposalTerm}},
		},
		Lsn: "16/B374D848",
	}
}

func TestIsSelfRevoked(t *testing.T) {
	// Recruited at term 2, transitioning away from the term-1 rule.
	rev := &clustermetadatapb.TermRevocation{
		RevokedBelowTerm: 2,
		OutgoingRule:     &clustermetadatapb.RuleNumber{CoordinatorTerm: 1},
	}
	rule := func(term, subterm int64) *clustermetadatapb.RulePosition {
		return &clustermetadatapb.RulePosition{Decision: &clustermetadatapb.ShardRule{
			RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: term, LeaderSubterm: subterm},
		}}
	}

	tests := []struct {
		name   string
		status *clustermetadatapb.ConsensusStatus
		want   bool
	}{
		{
			name:   "no revocation is never self-revoked",
			status: &clustermetadatapb.ConsensusStatus{CurrentPosition: &clustermetadatapb.PoolerPosition{Position: rule(1, 0)}},
			want:   false,
		},
		{
			name: "recruited above own rule with no higher known rule is stranded",
			status: &clustermetadatapb.ConsensusStatus{
				CurrentPosition: &clustermetadatapb.PoolerPosition{Position: rule(1, 0)},
				TermRevocation:  rev,
			},
			want: true,
		},
		{
			name: "recruited but following a higher accepted rule while WAL lags is not stranded",
			status: &clustermetadatapb.ConsensusStatus{
				CurrentPosition:    &clustermetadatapb.PoolerPosition{Position: rule(1, 0)},
				ReplicationPrimary: &clustermetadatapb.ReplicationPrimary{Position: rule(1, 5)},
				TermRevocation:     rev,
			},
			want: false,
		},
		{
			name: "consumed recruit: own rule reached the revoked term",
			status: &clustermetadatapb.ConsensusStatus{
				CurrentPosition: &clustermetadatapb.PoolerPosition{Position: rule(2, 0)},
				TermRevocation:  rev,
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, IsSelfRevoked(tt.status))
		})
	}
}

func TestHighestRevokedBelowTerm(t *testing.T) {
	t.Run("zero when no status carries a revocation", func(t *testing.T) {
		assert.Zero(t, highestRevokedBelowTerm(nil))
		assert.Zero(t, highestRevokedBelowTerm([]*clustermetadatapb.ConsensusStatus{{}}))
	})

	t.Run("highest term across statuses, regardless of decision", func(t *testing.T) {
		// Deliberately not decision-scoped: this is the safety floor a new
		// revocation's own term must exceed, which applies no matter what
		// decision each observed revocation targeted.
		got := highestRevokedBelowTerm([]*clustermetadatapb.ConsensusStatus{
			{TermRevocation: &clustermetadatapb.TermRevocation{RevokedBelowTerm: 3, RecruitIntent: &clustermetadatapb.RecruitIntent{ReplaceDecision: rn(1, 0)}}},
			{TermRevocation: &clustermetadatapb.TermRevocation{RevokedBelowTerm: 7, RecruitIntent: &clustermetadatapb.RecruitIntent{ReplaceDecision: rn(4, 0)}}},
		})
		assert.Equal(t, int64(7), got)
	})

	t.Run("ignores statuses with no revocation or a zero-valued one", func(t *testing.T) {
		got := highestRevokedBelowTerm([]*clustermetadatapb.ConsensusStatus{
			{},
			{TermRevocation: &clustermetadatapb.TermRevocation{RevokedBelowTerm: 0}},
			{TermRevocation: &clustermetadatapb.TermRevocation{RevokedBelowTerm: 4}},
		})
		assert.Equal(t, int64(4), got)
	})
}

func TestRevocationsMatchingDecision(t *testing.T) {
	decision4 := rn(4, 0)
	decision6 := rn(6, 0)

	t.Run("nil when no status matches the decision", func(t *testing.T) {
		got := revocationsMatchingDecision([]*clustermetadatapb.ConsensusStatus{
			{TermRevocation: &clustermetadatapb.TermRevocation{RevokedBelowTerm: 7, RecruitIntent: &clustermetadatapb.RecruitIntent{ReplaceDecision: decision6}}},
		}, decision4)
		assert.Nil(t, got)
	})

	t.Run("excludes a revocation with no RecruitIntent at all", func(t *testing.T) {
		// An untargeted revocation (e.g. an externally-supplied cert) must not
		// be treated as targeting decision4 just because CompareRuleNumbers
		// treats a nil ReplaceDecision as the zero RuleNumber.
		untargeted := &clustermetadatapb.TermRevocation{RevokedBelowTerm: 5}
		got := revocationsMatchingDecision([]*clustermetadatapb.ConsensusStatus{
			{TermRevocation: untargeted},
		}, decision4)
		assert.Nil(t, got)
	})

	t.Run("excludes a revocation targeting a different decision even at a higher term", func(t *testing.T) {
		// This is the shape of the bug fixed here: a global (decision-agnostic)
		// max-term scan would have picked this term-9 entry over the real,
		// lower-term match at decision4.
		stale := &clustermetadatapb.TermRevocation{RevokedBelowTerm: 9, RecruitIntent: &clustermetadatapb.RecruitIntent{ReplaceDecision: decision6}}
		relevant := &clustermetadatapb.TermRevocation{RevokedBelowTerm: 5, RecruitIntent: &clustermetadatapb.RecruitIntent{ReplaceDecision: decision4, Attempt: 2}}
		got := revocationsMatchingDecision([]*clustermetadatapb.ConsensusStatus{
			{TermRevocation: stale},
			{TermRevocation: relevant},
		}, decision4)
		require.Len(t, got, 1)
		assert.Same(t, relevant, got[0])
	})

	t.Run("returns every matching revocation, at any term, for the caller to reduce", func(t *testing.T) {
		low := &clustermetadatapb.TermRevocation{RevokedBelowTerm: 3, RecruitIntent: &clustermetadatapb.RecruitIntent{ReplaceDecision: decision4}}
		high := &clustermetadatapb.TermRevocation{RevokedBelowTerm: 5, RecruitIntent: &clustermetadatapb.RecruitIntent{ReplaceDecision: decision4}}
		got := revocationsMatchingDecision([]*clustermetadatapb.ConsensusStatus{
			{TermRevocation: low},
			{TermRevocation: high},
		}, decision4)
		require.Len(t, got, 2)
		assert.Same(t, low, got[0])
		assert.Same(t, high, got[1])
	})
}

func TestHighestRevokedBelowTermRevocation(t *testing.T) {
	t.Run("nil for no candidates", func(t *testing.T) {
		assert.Nil(t, HighestRevokedBelowTermRevocation(nil))
	})

	t.Run("the only candidate wins", func(t *testing.T) {
		only := &clustermetadatapb.TermRevocation{RevokedBelowTerm: 5}
		assert.Same(t, only, HighestRevokedBelowTermRevocation([]*clustermetadatapb.TermRevocation{only}))
	})

	t.Run("a strictly higher term wins over a lower one", func(t *testing.T) {
		low := &clustermetadatapb.TermRevocation{RevokedBelowTerm: 3}
		high := &clustermetadatapb.TermRevocation{RevokedBelowTerm: 5}
		assert.Same(t, high, HighestRevokedBelowTermRevocation([]*clustermetadatapb.TermRevocation{low, high}))
		assert.Same(t, high, HighestRevokedBelowTermRevocation([]*clustermetadatapb.TermRevocation{high, low}))
	})

	t.Run("ties are broken deterministically by AcceptedCoordinatorId, not candidate order", func(t *testing.T) {
		// Two different coordinators (coordA, coordB) each accepted at the same
		// term — sharing a term number doesn't mean sharing an outcome; at most
		// one could have actually won it, possibly none did. The winner must not
		// depend on which order the candidates were assembled in.
		tiedA := &clustermetadatapb.TermRevocation{RevokedBelowTerm: 7, AcceptedCoordinatorId: coordA}
		tiedB := &clustermetadatapb.TermRevocation{RevokedBelowTerm: 7, AcceptedCoordinatorId: coordB}
		got1 := HighestRevokedBelowTermRevocation([]*clustermetadatapb.TermRevocation{tiedA, tiedB})
		got2 := HighestRevokedBelowTermRevocation([]*clustermetadatapb.TermRevocation{tiedB, tiedA})
		require.NotNil(t, got1)
		assert.Same(t, got1, got2)
	})

	t.Run("collapses the same revocation reported by multiple cohort members", func(t *testing.T) {
		// The routine case: one coordinator's revocation replicated to every
		// cohort member. Each status holds its own (distinct) pointer, but the
		// content is identical, so this must not look like a genuine tie.
		same := func() *clustermetadatapb.TermRevocation {
			return &clustermetadatapb.TermRevocation{RevokedBelowTerm: 7, AcceptedCoordinatorId: coordA}
		}
		got := HighestRevokedBelowTermRevocation([]*clustermetadatapb.TermRevocation{same(), same(), same()})
		require.NotNil(t, got)
		assert.Equal(t, int64(7), got.GetRevokedBelowTerm())
	})

	t.Run("same coordinator and term but unequal content: the more recent CoordinatorInitiatedAt wins, not candidate order", func(t *testing.T) {
		// A restarted coordinator can reuse a term with a fresh
		// CoordinatorInitiatedAt — the exact conflict ValidateRevocation
		// rejects once a pooler has already accepted one of them, so different
		// poolers can end up holding different ones. Same AcceptedCoordinatorId
		// and RevokedBelowTerm rules out the earlier tie-breaks entirely; the
		// winner must still not depend on iteration order.
		older := &clustermetadatapb.TermRevocation{
			RevokedBelowTerm:       7,
			AcceptedCoordinatorId:  coordA,
			CoordinatorInitiatedAt: ts1,
		}
		newer := &clustermetadatapb.TermRevocation{
			RevokedBelowTerm:       7,
			AcceptedCoordinatorId:  coordA,
			CoordinatorInitiatedAt: ts2,
		}
		assert.Same(t, newer, HighestRevokedBelowTermRevocation([]*clustermetadatapb.TermRevocation{older, newer}))
		assert.Same(t, newer, HighestRevokedBelowTermRevocation([]*clustermetadatapb.TermRevocation{newer, older}))
	})
}

func TestRecruitAttempt(t *testing.T) {
	t.Run("1 when there is no prior recruit for the decision", func(t *testing.T) {
		assert.Equal(t, int64(1), recruitAttempt(nil, ts1, 5*time.Minute))
	})

	t.Run("carries the prior count forward when fresh", func(t *testing.T) {
		prior := &clustermetadatapb.TermRevocation{
			CoordinatorInitiatedAt: ts1,
			RecruitIntent:          &clustermetadatapb.RecruitIntent{Attempt: 2},
		}
		got := recruitAttempt(prior, timestamppb.New(ts1.AsTime().Add(time.Minute)), 5*time.Minute)
		assert.Equal(t, int64(3), got)
	})

	t.Run("resets to 1 when the prior recruit is older than the reset window", func(t *testing.T) {
		prior := &clustermetadatapb.TermRevocation{
			CoordinatorInitiatedAt: ts1,
			RecruitIntent:          &clustermetadatapb.RecruitIntent{Attempt: 7},
		}
		got := recruitAttempt(prior, timestamppb.New(ts1.AsTime().Add(time.Hour)), 5*time.Minute)
		assert.Equal(t, int64(1), got)
	})

	t.Run("a zero reset window disables the staleness reset", func(t *testing.T) {
		prior := &clustermetadatapb.TermRevocation{
			CoordinatorInitiatedAt: ts1,
			RecruitIntent:          &clustermetadatapb.RecruitIntent{Attempt: 2},
		}
		got := recruitAttempt(prior, timestamppb.New(ts1.AsTime().Add(time.Hour)), 0)
		assert.Equal(t, int64(3), got)
	})
}

func TestNewTermRevocation(t *testing.T) {
	coord := &clustermetadatapb.ID{Name: "coord-1"}

	t.Run("empty statuses returns error", func(t *testing.T) {
		rev, err := NewTermRevocation(nil, coord, ts1, 0)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "statuses must be non-empty")
		require.Nil(t, rev)
	})

	t.Run("nil initiatedAt returns error", func(t *testing.T) {
		statuses := []*clustermetadatapb.ConsensusStatus{
			{CurrentPosition: positionAtCoordTerm(4)},
		}
		rev, err := NewTermRevocation(statuses, coord, nil, 0)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "initiatedAt must be non-nil")
		require.Nil(t, rev)
	})

	t.Run("no cohort member has a recorded rule returns error", func(t *testing.T) {
		// Bootstrap-shaped scenario: cohort visible but nobody reports a
		// rule. NewTermRevocation refuses; the agent should construct the
		// revocation directly with an explicit outgoing_rule.
		statuses := []*clustermetadatapb.ConsensusStatus{{}, {}}
		rev, err := NewTermRevocation(statuses, coord, ts1, 0)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no cohort member reports a recorded rule")
		require.Nil(t, rev)
	})

	t.Run("cohort's most advanced position is an undecided proposal: trusted as outgoing_rule", func(t *testing.T) {
		// An undecided proposal beyond the decision is trusted as the
		// outgoing rule without a separate verification step: whoever gets
		// promoted still has to write a fresh proposal under the same
		// durability policy and cohort, and that write can't get its
		// synchronous ack unless the position it's superseding was
		// actually durable — an unverified minority proposal just makes
		// the promotion attempt fail to reach quorum, not succeed
		// incorrectly.
		statuses := []*clustermetadatapb.ConsensusStatus{
			{CurrentPosition: positionWithUndecidedProposal(4, 6)},
		}
		rev, err := NewTermRevocation(statuses, coord, ts1, 0)
		require.NoError(t, err)
		require.NotNil(t, rev)
		assert.Equal(t, int64(6), rev.GetOutgoingRule().GetCoordinatorTerm())
		assert.Equal(t, int64(7), rev.GetRevokedBelowTerm())
		// outgoing_rule is the undecided proposal (6), but the backoff scope is
		// the marked decision (4): the two deliberately diverge here.
		assert.Equal(t, int64(4), rev.GetRecruitIntent().GetReplaceDecision().GetCoordinatorTerm())
		assert.Equal(t, int64(1), rev.GetRecruitIntent().GetAttempt())
	})

	t.Run("revocation-term-only statuses with no recorded rule return error", func(t *testing.T) {
		// Same shape: statuses carry a stored revocation but no recorded
		// rule. NewTermRevocation requires at least one rule to derive
		// outgoing_rule from.
		statuses := []*clustermetadatapb.ConsensusStatus{
			{TermRevocation: &clustermetadatapb.TermRevocation{RevokedBelowTerm: 3}},
			{TermRevocation: &clustermetadatapb.TermRevocation{RevokedBelowTerm: 7}},
		}
		rev, err := NewTermRevocation(statuses, coord, ts1, 0)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no cohort member reports a recorded rule")
		require.Nil(t, rev)
	})

	t.Run("revocation term + recorded rule: max across both", func(t *testing.T) {
		statuses := []*clustermetadatapb.ConsensusStatus{
			{
				TermRevocation:  &clustermetadatapb.TermRevocation{RevokedBelowTerm: 7},
				CurrentPosition: positionAtCoordTerm(4),
			},
			{
				TermRevocation:  &clustermetadatapb.TermRevocation{RevokedBelowTerm: 5},
				CurrentPosition: positionAtCoordTerm(4),
			},
			{
				TermRevocation:  &clustermetadatapb.TermRevocation{RevokedBelowTerm: 3},
				CurrentPosition: positionAtCoordTerm(4),
			},
		}
		rev, err := NewTermRevocation(statuses, coord, ts1, 0)
		require.NoError(t, err)
		prototest.RequireEqual(t, &clustermetadatapb.TermRevocation{
			RevokedBelowTerm:       8,
			AcceptedCoordinatorId:  coord,
			CoordinatorInitiatedAt: ts1,
			OutgoingRule:           &clustermetadatapb.RuleNumber{CoordinatorTerm: 4},
			RecruitIntent: &clustermetadatapb.RecruitIntent{
				ReplaceDecision: &clustermetadatapb.RuleNumber{CoordinatorTerm: 4},
				Attempt:         1,
			},
		}, rev)
	})

	t.Run("uses max of recorded rule terms", func(t *testing.T) {
		statuses := []*clustermetadatapb.ConsensusStatus{
			{CurrentPosition: positionAtCoordTerm(4)},
			{CurrentPosition: positionAtCoordTerm(9)},
		}
		rev, err := NewTermRevocation(statuses, coord, ts1, 0)
		require.NoError(t, err)
		prototest.RequireEqual(t, &clustermetadatapb.TermRevocation{
			RevokedBelowTerm:       10,
			AcceptedCoordinatorId:  coord,
			CoordinatorInitiatedAt: ts1,
			OutgoingRule:           &clustermetadatapb.RuleNumber{CoordinatorTerm: 9},
			RecruitIntent: &clustermetadatapb.RecruitIntent{
				ReplaceDecision: &clustermetadatapb.RuleNumber{CoordinatorTerm: 9},
				Attempt:         1,
			},
		}, rev)
	})

	t.Run("takes max across both revocation and rule terms", func(t *testing.T) {
		statuses := []*clustermetadatapb.ConsensusStatus{
			{TermRevocation: &clustermetadatapb.TermRevocation{RevokedBelowTerm: 6}},
			{CurrentPosition: positionAtCoordTerm(11)},
		}
		rev, err := NewTermRevocation(statuses, coord, ts1, 0)
		require.NoError(t, err)
		prototest.RequireEqual(t, &clustermetadatapb.TermRevocation{
			RevokedBelowTerm:       12,
			AcceptedCoordinatorId:  coord,
			CoordinatorInitiatedAt: ts1,
			OutgoingRule:           &clustermetadatapb.RuleNumber{CoordinatorTerm: 11},
			RecruitIntent: &clustermetadatapb.RecruitIntent{
				ReplaceDecision: &clustermetadatapb.RuleNumber{CoordinatorTerm: 11},
				Attempt:         1,
			},
		}, rev)
	})

	t.Run("outgoing_rule picks the highest RuleNumber across statuses", func(t *testing.T) {
		statuses := []*clustermetadatapb.ConsensusStatus{
			{CurrentPosition: &clustermetadatapb.PoolerPosition{
				Position: &clustermetadatapb.RulePosition{Decision: &clustermetadatapb.ShardRule{
					RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 4, LeaderSubterm: 2},
				}},
				Lsn: "16/B374D848",
			}},
			{CurrentPosition: &clustermetadatapb.PoolerPosition{
				Position: &clustermetadatapb.RulePosition{Decision: &clustermetadatapb.ShardRule{
					RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 4, LeaderSubterm: 5},
				}},
				Lsn: "16/B374D900",
			}},
			{CurrentPosition: &clustermetadatapb.PoolerPosition{
				Position: &clustermetadatapb.RulePosition{Decision: &clustermetadatapb.ShardRule{
					RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 3, LeaderSubterm: 9},
				}},
				Lsn: "16/B374D700",
			}},
		}
		rev, err := NewTermRevocation(statuses, coord, ts1, 0)
		require.NoError(t, err)
		prototest.RequireEqual(t, &clustermetadatapb.TermRevocation{
			RevokedBelowTerm:       5,
			AcceptedCoordinatorId:  coord,
			CoordinatorInitiatedAt: ts1,
			OutgoingRule:           &clustermetadatapb.RuleNumber{CoordinatorTerm: 4, LeaderSubterm: 5},
			RecruitIntent: &clustermetadatapb.RecruitIntent{
				ReplaceDecision: &clustermetadatapb.RuleNumber{CoordinatorTerm: 4, LeaderSubterm: 5},
				Attempt:         1,
			},
		}, rev)
	})

	t.Run("carries attempt forward when replace_decision is unchanged", func(t *testing.T) {
		// The most recent prior revocation targeted decision {term 4} at attempt 2,
		// and the cohort's decision is still term 4 (no newer decision committed),
		// so this recruit is another attempt against the same baseline: attempt 3.
		statuses := []*clustermetadatapb.ConsensusStatus{
			{
				TermRevocation: &clustermetadatapb.TermRevocation{
					RevokedBelowTerm: 5,
					// Recent prior recruit, so it is not treated as stale.
					CoordinatorInitiatedAt: ts1,
					RecruitIntent: &clustermetadatapb.RecruitIntent{
						ReplaceDecision: &clustermetadatapb.RuleNumber{CoordinatorTerm: 4},
						Attempt:         2,
					},
				},
				CurrentPosition: positionAtCoordTerm(4),
			},
		}
		rev, err := NewTermRevocation(statuses, coord, ts1, 5*time.Minute)
		require.NoError(t, err)
		prototest.RequireEqual(t, &clustermetadatapb.TermRevocation{
			RevokedBelowTerm:       6,
			AcceptedCoordinatorId:  coord,
			CoordinatorInitiatedAt: ts1,
			OutgoingRule:           &clustermetadatapb.RuleNumber{CoordinatorTerm: 4},
			RecruitIntent: &clustermetadatapb.RecruitIntent{
				ReplaceDecision: &clustermetadatapb.RuleNumber{CoordinatorTerm: 4},
				Attempt:         3,
			},
		}, rev)
	})

	t.Run("resets attempt to 1 when the prior recruit is stale", func(t *testing.T) {
		// Same decided baseline as the prior revocation, so the count would
		// normally carry forward — but that prior recruit is far older than the
		// backoff window (recruitment paused, e.g. the cluster was scaled to zero
		// and restarted), so the stale count resets to 1.
		staleInitiated := timestamppb.New(ts1.AsTime().Add(-time.Hour))
		statuses := []*clustermetadatapb.ConsensusStatus{
			{
				TermRevocation: &clustermetadatapb.TermRevocation{
					RevokedBelowTerm:       5,
					CoordinatorInitiatedAt: staleInitiated,
					RecruitIntent: &clustermetadatapb.RecruitIntent{
						ReplaceDecision: &clustermetadatapb.RuleNumber{CoordinatorTerm: 4},
						Attempt:         7,
					},
				},
				CurrentPosition: positionAtCoordTerm(4),
			},
		}
		rev, err := NewTermRevocation(statuses, coord, ts1, 5*time.Minute)
		require.NoError(t, err)
		prototest.RequireEqual(t, &clustermetadatapb.TermRevocation{
			RevokedBelowTerm:       6,
			AcceptedCoordinatorId:  coord,
			CoordinatorInitiatedAt: ts1,
			OutgoingRule:           &clustermetadatapb.RuleNumber{CoordinatorTerm: 4},
			RecruitIntent: &clustermetadatapb.RecruitIntent{
				ReplaceDecision: &clustermetadatapb.RuleNumber{CoordinatorTerm: 4},
				Attempt:         1,
			},
		}, rev)
	})

	t.Run("does not reset on staleness when the window is zero", func(t *testing.T) {
		// Same decided baseline and an old prior recruit, but a zero reset window
		// disables the staleness heuristic, so the count still carries forward.
		oldInitiated := timestamppb.New(ts1.AsTime().Add(-time.Hour))
		statuses := []*clustermetadatapb.ConsensusStatus{
			{
				TermRevocation: &clustermetadatapb.TermRevocation{
					RevokedBelowTerm:       5,
					CoordinatorInitiatedAt: oldInitiated,
					RecruitIntent: &clustermetadatapb.RecruitIntent{
						ReplaceDecision: &clustermetadatapb.RuleNumber{CoordinatorTerm: 4},
						Attempt:         2,
					},
				},
				CurrentPosition: positionAtCoordTerm(4),
			},
		}
		rev, err := NewTermRevocation(statuses, coord, ts1, 0)
		require.NoError(t, err)
		prototest.RequireEqual(t, &clustermetadatapb.TermRevocation{
			RevokedBelowTerm:       6,
			AcceptedCoordinatorId:  coord,
			CoordinatorInitiatedAt: ts1,
			OutgoingRule:           &clustermetadatapb.RuleNumber{CoordinatorTerm: 4},
			RecruitIntent: &clustermetadatapb.RecruitIntent{
				ReplaceDecision: &clustermetadatapb.RuleNumber{CoordinatorTerm: 4},
				Attempt:         3,
			},
		}, rev)
	})

	t.Run("resets attempt to 1 when replace_decision advances", func(t *testing.T) {
		// The prior revocation targeted decision {term 4} at attempt 3, but the
		// cohort has since committed a newer decision (term 6). Real progress, so
		// the count resets to 1 — a stuck proposal would NOT advance the decision
		// and so would not land here.
		statuses := []*clustermetadatapb.ConsensusStatus{
			{
				TermRevocation: &clustermetadatapb.TermRevocation{
					RevokedBelowTerm: 5,
					RecruitIntent: &clustermetadatapb.RecruitIntent{
						ReplaceDecision: &clustermetadatapb.RuleNumber{CoordinatorTerm: 4},
						Attempt:         3,
					},
				},
				CurrentPosition: positionAtCoordTerm(6),
			},
		}
		rev, err := NewTermRevocation(statuses, coord, ts1, 0)
		require.NoError(t, err)
		prototest.RequireEqual(t, &clustermetadatapb.TermRevocation{
			RevokedBelowTerm:       7,
			AcceptedCoordinatorId:  coord,
			CoordinatorInitiatedAt: ts1,
			OutgoingRule:           &clustermetadatapb.RuleNumber{CoordinatorTerm: 6},
			RecruitIntent: &clustermetadatapb.RecruitIntent{
				ReplaceDecision: &clustermetadatapb.RuleNumber{CoordinatorTerm: 6},
				Attempt:         1,
			},
		}, rev)
	})

	t.Run("a stale revocation targeting an unrelated decision does not shadow the real prior attempt", func(t *testing.T) {
		// pooler-A carries an old, abandoned attempt at a long-superseded
		// decision (term 2) that happens to have a numerically higher term
		// (9) than the real prior attempt. pooler-B is decided at the current
		// decision (term 6) and holds the actual prior attempt at it (term 5,
		// attempt 2). A decision-agnostic max-term scan would pick pooler-A's
		// stale revocation and wrongly reset the count to 1; this must
		// instead find pooler-B's and carry it forward to 3.
		statuses := []*clustermetadatapb.ConsensusStatus{
			{
				CurrentPosition: positionAtCoordTerm(2),
				TermRevocation: &clustermetadatapb.TermRevocation{
					RevokedBelowTerm: 9,
					RecruitIntent: &clustermetadatapb.RecruitIntent{
						ReplaceDecision: &clustermetadatapb.RuleNumber{CoordinatorTerm: 2},
						Attempt:         5,
					},
				},
			},
			{
				CurrentPosition: positionAtCoordTerm(6),
				TermRevocation: &clustermetadatapb.TermRevocation{
					RevokedBelowTerm: 5,
					RecruitIntent: &clustermetadatapb.RecruitIntent{
						ReplaceDecision: &clustermetadatapb.RuleNumber{CoordinatorTerm: 6},
						Attempt:         2,
					},
				},
			},
		}
		rev, err := NewTermRevocation(statuses, coord, ts1, 0)
		require.NoError(t, err)
		prototest.RequireEqual(t, &clustermetadatapb.TermRevocation{
			RevokedBelowTerm:       10,
			AcceptedCoordinatorId:  coord,
			CoordinatorInitiatedAt: ts1,
			OutgoingRule:           &clustermetadatapb.RuleNumber{CoordinatorTerm: 6},
			RecruitIntent: &clustermetadatapb.RecruitIntent{
				ReplaceDecision: &clustermetadatapb.RuleNumber{CoordinatorTerm: 6},
				Attempt:         3,
			},
		}, rev)
	})
}

func TestIsRuleRevoked(t *testing.T) {
	ruleAt := func(term, subterm int64) *clustermetadatapb.ShardRule {
		return &clustermetadatapb.ShardRule{
			RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: term, LeaderSubterm: subterm},
		}
	}
	revocation := func(revokedBelow int64, outgoing *clustermetadatapb.RuleNumber) *clustermetadatapb.TermRevocation {
		return &clustermetadatapb.TermRevocation{RevokedBelowTerm: revokedBelow, OutgoingRule: outgoing}
	}
	ruleNum := func(term, subterm int64) *clustermetadatapb.RuleNumber {
		return &clustermetadatapb.RuleNumber{CoordinatorTerm: term, LeaderSubterm: subterm}
	}

	tests := []struct {
		name       string
		rule       *clustermetadatapb.ShardRule
		proposal   *clustermetadatapb.ShardRule
		revocation *clustermetadatapb.TermRevocation
		want       bool
	}{
		{
			name:       "NilRevocation_NotRevoked",
			rule:       ruleAt(2, 0),
			revocation: nil,
			want:       false,
		},
		{
			name:       "ZeroRevokedBelow_NotRevoked",
			rule:       ruleAt(2, 0),
			revocation: revocation(0, ruleNum(1, 0)),
			want:       false,
		},
		{
			name:       "RuleTermAboveRevokedBelow_NotRevoked",
			rule:       ruleAt(5, 0),
			revocation: revocation(3, ruleNum(1, 0)),
			want:       false,
		},
		{
			name:       "RuleTermEqualsRevokedBelow_NotRevoked",
			rule:       ruleAt(3, 0),
			revocation: revocation(3, ruleNum(1, 0)),
			want:       false,
		},
		{
			name:       "RuleBelowRevokedAndOverridesOutgoing_NotRevoked",
			rule:       ruleAt(2, 0),
			revocation: revocation(3, ruleNum(1, 0)),
			want:       false,
		},
		{
			name:       "RuleBelowRevokedAndOverridesOutgoingBySubterm_NotRevoked",
			rule:       ruleAt(2, 5),
			revocation: revocation(3, ruleNum(2, 4)),
			want:       false,
		},
		{
			name:       "RuleBelowRevokedAndEqualsOutgoing_Revoked",
			rule:       ruleAt(2, 0),
			revocation: revocation(3, ruleNum(2, 0)),
			want:       true,
		},
		{
			name:       "RuleBelowRevokedAndBelowOutgoing_Revoked",
			rule:       ruleAt(2, 0),
			revocation: revocation(3, ruleNum(2, 5)),
			want:       true,
		},
		{
			// A revocation without a real outgoing_rule is invalid — it
			// isn't authoritative relative to any specific rule, so it
			// revokes nothing, regardless of revoked_below_term.
			name:       "RuleBelowRevokedAndOutgoingNil_NotRevoked",
			rule:       ruleAt(2, 0),
			revocation: revocation(3, nil),
			want:       false,
		},
		{
			name:       "RuleBelowRevokedAndOutgoingZero_NotRevoked",
			rule:       ruleAt(0, 0),
			revocation: revocation(3, &clustermetadatapb.RuleNumber{}),
			want:       false,
		},
		{
			// Decision ties outgoing_rule, so the tiebreak falls to the
			// proposal: it's below revoked_below_term too, so the whole
			// position — decision and its outstanding proposal alike — is
			// revoked.
			name:       "DecisionTiesOutgoing_ProposalBelowRevoked_Revoked",
			rule:       ruleAt(2, 0),
			proposal:   ruleAt(4, 0),
			revocation: revocation(5, ruleNum(2, 0)),
			want:       true,
		},
		{
			// Same tie on decision, but the proposal is already at or beyond
			// revoked_below_term — real WAL content this revocation doesn't
			// reach, so the position is not revoked.
			name:       "DecisionTiesOutgoing_ProposalAtOrAboveRevoked_NotRevoked",
			rule:       ruleAt(2, 0),
			proposal:   ruleAt(5, 0),
			revocation: revocation(5, ruleNum(2, 0)),
			want:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsRuleRevoked(&clustermetadatapb.RulePosition{Decision: tt.rule, Proposal: tt.proposal}, tt.revocation)
			assert.Equal(t, tt.want, got)
		})
	}
}
