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

	t.Run("cohort's most advanced position is an undecided proposal returns error", func(t *testing.T) {
		// Propagation isn't supported yet: the most-advanced position across
		// the cohort must already be decided. A node reporting only an
		// undecided proposal beyond its decision must not be silently
		// trusted as the outgoing rule.
		statuses := []*clustermetadatapb.ConsensusStatus{
			{CurrentPosition: positionWithUndecidedProposal(4, 6)},
		}
		rev, err := NewTermRevocation(statuses, coord, ts1, 0)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "cohort's most advanced position is an undecided proposal at rule 6.0; propagation is not yet supported")
		require.Nil(t, rev)
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
