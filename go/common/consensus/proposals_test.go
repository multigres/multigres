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
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
)

// makeID builds a pooler ID for tests.
func makeID(cell, name string) *clustermetadatapb.ID {
	return &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      cell,
		Name:      name,
	}
}

// makeRule builds a ShardRule at the given coordinator term with the given cohort.
func makeRule(coordTerm int64, cohort []*clustermetadatapb.ID) *clustermetadatapb.ShardRule {
	return &clustermetadatapb.ShardRule{
		RuleNumber: &clustermetadatapb.RuleNumber{
			CoordinatorTerm: coordTerm,
		},
		PrimaryId:        cohort[0],
		CohortMembers:    cohort,
		DurabilityPolicy: topoclient.AtLeastN(2),
	}
}

// makeStatus builds a ConsensusStatus for a recruited node.
func makeStatus(id *clustermetadatapb.ID, rule *clustermetadatapb.ShardRule, revocation *clustermetadatapb.TermRevocation) *clustermetadatapb.ConsensusStatus {
	return makeStatusWithLSN(id, rule, revocation, "0/1000000")
}

// makeStatusWithLSN builds a ConsensusStatus with an explicit LSN.
func makeStatusWithLSN(id *clustermetadatapb.ID, rule *clustermetadatapb.ShardRule, revocation *clustermetadatapb.TermRevocation, lsn string) *clustermetadatapb.ConsensusStatus {
	return &clustermetadatapb.ConsensusStatus{
		Id:             id,
		TermRevocation: revocation,
		CurrentPosition: &clustermetadatapb.PoolerPosition{
			Rule: rule,
			Lsn:  lsn,
		},
	}
}

// simpleProposal returns a buildProposal callback that proposes the first
// eligible leader with a rule built from the given coordinator term and cohort.
func simpleProposal(coordTerm int64, cohort []*clustermetadatapb.ID) func(RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error) {
	return func(r RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error) {
		leader := r.EligibleLeaders[0]
		return &consensusdatapb.CoordinatorProposal{
			TermRevocation: r.TermRevocation,
			ProposalLeader: &consensusdatapb.ProposalLeader{
				Id:   leader.GetId(),
				Host: "localhost",
			},
			ProposedRule: makeRule(coordTerm, cohort),
		}, nil
	}
}

var testRevocation = &clustermetadatapb.TermRevocation{RevokedBelowTerm: 5}

// testCoordRevocation has all required fields populated for use with
// filterByPotentialRevocation / CheckProposalPossible tests. testRevocation is
// missing AcceptedCoordinatorId and CoordinatorInitiatedAt, so ValidateRevocation
// would reject it in those contexts.
var testCoordRevocation = &clustermetadatapb.TermRevocation{
	RevokedBelowTerm: 5,
	AcceptedCoordinatorId: &clustermetadatapb.ID{
		Cell: "z1",
		Name: "coord-1",
	},
	CoordinatorInitiatedAt: timestamppb.Now(),
}

// makeUnrecruitedStatus returns a status for a node that has not yet accepted
// any revocation. Used for pre-vote / CheckProposalPossible tests.
func makeUnrecruitedStatus(id *clustermetadatapb.ID, rule *clustermetadatapb.ShardRule) *clustermetadatapb.ConsensusStatus {
	return &clustermetadatapb.ConsensusStatus{
		Id: id,
		CurrentPosition: &clustermetadatapb.PoolerPosition{
			Rule: rule,
			Lsn:  "0/1000000",
		},
	}
}

func TestBuildProposalCore(t *testing.T) {
	a := makeID("z1", "pooler-a")
	b := makeID("z1", "pooler-b")
	c := makeID("z1", "pooler-c")
	cohort := []*clustermetadatapb.ID{a, b, c}
	rule := makeRule(3, cohort)

	type tc struct {
		name              string
		mode              cohortQuorumMode
		revocation        *clustermetadatapb.TermRevocation // nil → testRevocation
		recruitedStatuses []*clustermetadatapb.ConsensusStatus
		buildProposal     func(RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error)
		wantLeader        string
		checkResult       func(*testing.T, RecruitmentResult)
		wantErr           string
	}

	tests := []tc{
		// ── outgoing cohort mode ──────────────────────────────────────────────────
		{
			name: "all 3 recruited: success, pooler-a is first eligible leader",
			mode: outgoingCohortMode,
			recruitedStatuses: []*clustermetadatapb.ConsensusStatus{
				makeStatus(a, rule, testRevocation),
				makeStatus(b, rule, testRevocation),
				makeStatus(c, rule, testRevocation),
			},
			buildProposal: simpleProposal(5, cohort),
			wantLeader:    "pooler-a",
		},
		{
			name: "no current_position: filtered by filterByValidPosition",
			mode: outgoingCohortMode,
			recruitedStatuses: []*clustermetadatapb.ConsensusStatus{
				{Id: a, TermRevocation: testRevocation}, // no current_position → no LSN
			},
			buildProposal: simpleProposal(5, cohort),
			wantErr:       "all recruited nodes reported an invalid or missing WAL position",
		},
		{
			name: "1 of 3 recruited: insufficient outgoing cohort",
			mode: outgoingCohortMode,
			recruitedStatuses: []*clustermetadatapb.ConsensusStatus{
				makeStatus(a, rule, testRevocation),
			},
			buildProposal: simpleProposal(5, cohort),
			wantErr:       "insufficient outgoing cohort recruitment: majority not satisfied: recruited 1 of 3 cohort poolers, need at least 2",
		},
		{
			name: "callback proposes outsider: not among eligible leaders",
			mode: outgoingCohortMode,
			recruitedStatuses: []*clustermetadatapb.ConsensusStatus{
				makeStatus(a, rule, testRevocation),
				makeStatus(b, rule, testRevocation),
				makeStatus(c, rule, testRevocation),
			},
			buildProposal: func(r RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error) {
				outsider := makeID("z1", "outsider")
				return &consensusdatapb.CoordinatorProposal{
					TermRevocation: r.TermRevocation,
					ProposalLeader: &consensusdatapb.ProposalLeader{Id: outsider},
					ProposedRule:   rule,
				}, nil
			},
			wantErr: "proposal validation: proposed leader z1_outsider is not among eligible leaders",
		},
		{
			name: "proposed cohort has unrecruited member but passes (outsider not recruited, a+b satisfy AT_LEAST_2)",
			mode: outgoingCohortMode,
			recruitedStatuses: []*clustermetadatapb.ConsensusStatus{
				makeStatus(a, rule, testRevocation),
				makeStatus(b, rule, testRevocation),
				makeStatus(c, rule, testRevocation),
			},
			buildProposal: func(r RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error) {
				outsider := makeID("z1", "outsider")
				return &consensusdatapb.CoordinatorProposal{
					TermRevocation: r.TermRevocation,
					ProposalLeader: &consensusdatapb.ProposalLeader{Id: a},
					ProposedRule:   makeRule(5, []*clustermetadatapb.ID{a, b, outsider}),
				}, nil
			},
		},
		{
			name: "2 of 3 recruited, dead primary stays in proposed cohort",
			mode: outgoingCohortMode,
			recruitedStatuses: []*clustermetadatapb.ConsensusStatus{
				makeStatus(b, rule, testRevocation),
				makeStatus(c, rule, testRevocation),
			},
			buildProposal: func(r RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error) {
				return &consensusdatapb.CoordinatorProposal{
					TermRevocation: r.TermRevocation,
					ProposalLeader: &consensusdatapb.ProposalLeader{Id: b},
					ProposedRule:   makeRule(5, cohort),
				}, nil
			},
			wantLeader: "pooler-b",
		},
		{
			name: "proposed cohort has new members not recruited",
			mode: outgoingCohortMode,
			recruitedStatuses: []*clustermetadatapb.ConsensusStatus{
				makeStatus(a, rule, testRevocation),
				makeStatus(b, rule, testRevocation),
				makeStatus(c, rule, testRevocation),
			},
			buildProposal: func(r RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error) {
				d := makeID("z1", "pooler-d")
				e := makeID("z1", "pooler-e")
				return &consensusdatapb.CoordinatorProposal{
					TermRevocation: r.TermRevocation,
					ProposalLeader: &consensusdatapb.ProposalLeader{Id: a},
					ProposedRule:   makeRule(5, []*clustermetadatapb.ID{a, d, e}),
				}, nil
			},
			wantErr: "proposal validation: recruited proposed cohort cannot achieve durability: durability not achievable: proposed cohort has 1 poolers, required 2",
		},
		{
			name: "callback returns error",
			mode: outgoingCohortMode,
			recruitedStatuses: []*clustermetadatapb.ConsensusStatus{
				makeStatus(a, rule, testRevocation),
				makeStatus(b, rule, testRevocation),
				makeStatus(c, rule, testRevocation),
			},
			buildProposal: func(r RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error) {
				return nil, errors.New("no suitable candidate")
			},
			wantErr: "buildProposal: no suitable candidate",
		},
		{
			name: "callback returns nil proposal",
			mode: outgoingCohortMode,
			recruitedStatuses: []*clustermetadatapb.ConsensusStatus{
				makeStatus(a, rule, testRevocation),
				makeStatus(b, rule, testRevocation),
				makeStatus(c, rule, testRevocation),
			},
			buildProposal: func(r RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error) {
				return nil, nil
			},
			wantErr: "buildProposal returned nil proposal",
		},
		{
			// One node behind: c is at oldRule, a+b at rule (term 3). Higher rule
			// governs quorum; only a and b are eligible.
			name: "mixed rule numbers: higher rule is outgoing, lagging node excluded from eligibles",
			mode: outgoingCohortMode,
			recruitedStatuses: []*clustermetadatapb.ConsensusStatus{
				makeStatus(a, rule, testRevocation),
				makeStatus(b, rule, testRevocation),
				makeStatus(c, makeRule(2, cohort), testRevocation), // behind
			},
			buildProposal: func(r RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error) {
				return &consensusdatapb.CoordinatorProposal{
					TermRevocation: r.TermRevocation,
					ProposalLeader: &consensusdatapb.ProposalLeader{Id: r.EligibleLeaders[0].GetId()},
					ProposedRule:   makeRule(5, cohort),
				}, nil
			},
			checkResult: func(t *testing.T, r RecruitmentResult) {
				assert.Equal(t, int64(3), r.OutgoingRule.GetRuleNumber().GetCoordinatorTerm())
				assert.Len(t, r.EligibleLeaders, 2, "only nodes at outgoingRule are eligible")
				assert.NotEqual(t, "pooler-c", r.EligibleLeaders[0].GetId().GetName())
			},
		},
		{
			name: "proposed cohort too small for AT_LEAST_2",
			mode: outgoingCohortMode,
			recruitedStatuses: []*clustermetadatapb.ConsensusStatus{
				makeStatus(a, rule, testRevocation),
				makeStatus(b, rule, testRevocation),
				makeStatus(c, rule, testRevocation),
			},
			buildProposal: func(r RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error) {
				return &consensusdatapb.CoordinatorProposal{
					TermRevocation: r.TermRevocation,
					ProposalLeader: &consensusdatapb.ProposalLeader{Id: a},
					ProposedRule:   makeRule(5, []*clustermetadatapb.ID{a}),
				}, nil
			},
			wantErr: "proposal validation: recruited proposed cohort cannot achieve durability: durability not achievable: proposed cohort has 1 poolers, required 2",
		},
		{
			name: "extra non-cohort node does not inflate outgoing quorum count",
			mode: outgoingCohortMode,
			recruitedStatuses: []*clustermetadatapb.ConsensusStatus{
				makeStatus(a, rule, testRevocation),
				makeStatus(makeID("z1", "extra-node"), rule, testRevocation),
			},
			buildProposal: simpleProposal(5, cohort),
			wantErr:       "insufficient outgoing cohort recruitment: majority not satisfied: recruited 1 of 3 cohort poolers, need at least 2",
		},
		{
			name: "LSN tiebreaker: highest LSN is sole eligible leader",
			mode: outgoingCohortMode,
			recruitedStatuses: []*clustermetadatapb.ConsensusStatus{
				makeStatusWithLSN(a, rule, testRevocation, "0/3000000"), // highest
				makeStatusWithLSN(b, rule, testRevocation, "0/2000000"),
				makeStatusWithLSN(c, rule, testRevocation, "0/1000000"),
			},
			buildProposal: func(r RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error) {
				return &consensusdatapb.CoordinatorProposal{
					TermRevocation: r.TermRevocation,
					ProposalLeader: &consensusdatapb.ProposalLeader{Id: r.EligibleLeaders[0].GetId()},
					ProposedRule:   makeRule(5, cohort),
				}, nil
			},
			checkResult: func(t *testing.T, r RecruitmentResult) {
				require.Len(t, r.EligibleLeaders, 1)
				assert.Equal(t, "pooler-a", r.EligibleLeaders[0].GetId().GetName())
			},
		},
		{
			name: "LSN tie at max: two nodes tied at highest LSN both eligible",
			mode: outgoingCohortMode,
			recruitedStatuses: []*clustermetadatapb.ConsensusStatus{
				makeStatusWithLSN(a, rule, testRevocation, "0/3000000"),
				makeStatusWithLSN(b, rule, testRevocation, "0/3000000"), // tied with a
				makeStatusWithLSN(c, rule, testRevocation, "0/1000000"),
			},
			buildProposal: func(r RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error) {
				return &consensusdatapb.CoordinatorProposal{
					TermRevocation: r.TermRevocation,
					ProposalLeader: &consensusdatapb.ProposalLeader{Id: r.EligibleLeaders[0].GetId()},
					ProposedRule:   makeRule(5, cohort),
				}, nil
			},
			checkResult: func(t *testing.T, r RecruitmentResult) {
				require.Len(t, r.EligibleLeaders, 2)
				names := []string{r.EligibleLeaders[0].GetId().GetName(), r.EligibleLeaders[1].GetId().GetName()}
				assert.ElementsMatch(t, []string{"pooler-a", "pooler-b"}, names)
			},
		},
		{
			// a at newRule/high LSN, b at newRule/low LSN, c at oldRule — only a eligible
			name: "rule takes priority: node at old rule excluded despite higher LSN",
			mode: outgoingCohortMode,
			recruitedStatuses: []*clustermetadatapb.ConsensusStatus{
				makeStatusWithLSN(a, rule, testRevocation, "0/5000000"),
				makeStatusWithLSN(b, rule, testRevocation, "0/3000000"),
				makeStatusWithLSN(c, makeRule(2, cohort), testRevocation, "0/4000000"),
			},
			buildProposal: func(r RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error) {
				return &consensusdatapb.CoordinatorProposal{
					TermRevocation: r.TermRevocation,
					ProposalLeader: &consensusdatapb.ProposalLeader{Id: r.EligibleLeaders[0].GetId()},
					ProposedRule:   makeRule(5, cohort),
				}, nil
			},
			checkResult: func(t *testing.T, r RecruitmentResult) {
				require.Len(t, r.EligibleLeaders, 1)
				assert.Equal(t, "pooler-a", r.EligibleLeaders[0].GetId().GetName())
			},
		},
		{
			name: "duplicate status: same pooler twice counts once toward quorum",
			mode: outgoingCohortMode,
			recruitedStatuses: []*clustermetadatapb.ConsensusStatus{
				makeStatus(a, rule, testRevocation),
				makeStatus(a, rule, testRevocation), // duplicate — must not inflate quorum
			},
			buildProposal: simpleProposal(5, cohort),
			wantErr:       "insufficient outgoing cohort recruitment: majority not satisfied: recruited 1 of 3 cohort poolers, need at least 2",
		},
		{
			// a appears twice: stale at oldRule/high LSN, fresh at rule/low LSN.
			// Rule number wins in deduplication; a ends up with the rule entry.
			name: "duplicate: rule number wins over LSN when deduplicating",
			mode: outgoingCohortMode,
			recruitedStatuses: []*clustermetadatapb.ConsensusStatus{
				makeStatusWithLSN(a, makeRule(2, cohort), testRevocation, "0/3000000"),
				makeStatusWithLSN(a, rule, testRevocation, "0/2000000"),
				makeStatusWithLSN(b, rule, testRevocation, "0/1000000"),
				makeStatusWithLSN(c, rule, testRevocation, "0/1000000"),
			},
			buildProposal: simpleProposal(5, cohort),
			checkResult: func(t *testing.T, r RecruitmentResult) {
				require.Len(t, r.EligibleLeaders, 1)
				assert.Equal(t, "pooler-a", r.EligibleLeaders[0].GetId().GetName())
			},
		},
		{
			name: "all accepted nodes have empty LSN: invalid WAL position",
			mode: outgoingCohortMode,
			recruitedStatuses: []*clustermetadatapb.ConsensusStatus{
				makeStatusWithLSN(a, rule, testRevocation, ""),
				makeStatusWithLSN(b, rule, testRevocation, ""),
				makeStatusWithLSN(c, rule, testRevocation, ""),
			},
			buildProposal: simpleProposal(5, cohort),
			wantErr:       "all recruited nodes reported an invalid or missing WAL position",
		},
		{
			name: "all accepted nodes have unparsable LSN: invalid WAL position",
			mode: outgoingCohortMode,
			recruitedStatuses: []*clustermetadatapb.ConsensusStatus{
				makeStatusWithLSN(a, rule, testRevocation, "not-an-lsn"),
				makeStatusWithLSN(b, rule, testRevocation, "not-an-lsn"),
				makeStatusWithLSN(c, rule, testRevocation, "not-an-lsn"),
			},
			buildProposal: simpleProposal(5, cohort),
			wantErr:       "all recruited nodes reported an invalid or missing WAL position",
		},
		{
			name: "node with invalid LSN excluded: remaining 2 satisfy AT_LEAST_2",
			mode: outgoingCohortMode,
			recruitedStatuses: []*clustermetadatapb.ConsensusStatus{
				makeStatusWithLSN(a, rule, testRevocation, "0/3000000"),
				makeStatusWithLSN(b, rule, testRevocation, "0/2000000"),
				makeStatusWithLSN(c, rule, testRevocation, ""),
			},
			buildProposal: simpleProposal(5, cohort),
		},
		{
			name: "invalid LSN causes quorum failure: 1 valid of 3",
			mode: outgoingCohortMode,
			recruitedStatuses: []*clustermetadatapb.ConsensusStatus{
				makeStatusWithLSN(a, rule, testRevocation, "0/3000000"),
				makeStatusWithLSN(b, rule, testRevocation, ""),
				makeStatusWithLSN(c, rule, testRevocation, "not-an-lsn"),
			},
			buildProposal: simpleProposal(5, cohort),
			wantErr:       "insufficient outgoing cohort recruitment: majority not satisfied: recruited 1 of 3 cohort poolers, need at least 2",
		},
		{
			name: "node with invalid LSN cannot be proposed as leader",
			mode: outgoingCohortMode,
			recruitedStatuses: []*clustermetadatapb.ConsensusStatus{
				makeStatusWithLSN(a, rule, testRevocation, "0/3000000"),
				makeStatusWithLSN(b, rule, testRevocation, "0/2000000"),
				makeStatusWithLSN(c, rule, testRevocation, ""),
			},
			buildProposal: func(r RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error) {
				return &consensusdatapb.CoordinatorProposal{
					TermRevocation: r.TermRevocation,
					ProposalLeader: &consensusdatapb.ProposalLeader{Id: c},
					ProposedRule:   makeRule(5, cohort),
				}, nil
			},
			wantErr: "proposal validation: proposed leader z1_pooler-c is not among eligible leaders",
		},
		func() tc {
			// 5-node cohort [a,b,c,d,e]; only d and e respond. AT_LEAST_2 on 5
			// nodes needs majority=3 (and revocation requires 4). 2 < 3 → fails.
			d := makeID("z1", "pooler-d")
			e := makeID("z1", "pooler-e")
			newCohort := []*clustermetadatapb.ID{a, b, c, d, e}
			newRule := makeRule(4, newCohort)
			return tc{
				name: "cohort expansion: only 2 of 5 new-cohort members respond",
				mode: outgoingCohortMode,
				recruitedStatuses: []*clustermetadatapb.ConsensusStatus{
					makeStatusWithLSN(d, newRule, testRevocation, "0/4000000"),
					makeStatusWithLSN(e, newRule, testRevocation, "0/4000000"),
				},
				buildProposal: func(r RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error) {
					return &consensusdatapb.CoordinatorProposal{
						TermRevocation: r.TermRevocation,
						ProposalLeader: &consensusdatapb.ProposalLeader{Id: d},
						ProposedRule:   makeRule(5, []*clustermetadatapb.ID{d, e}),
					}, nil
				},
				wantErr: "insufficient outgoing cohort recruitment: majority not satisfied: recruited 2 of 5 cohort poolers, need at least 3",
			}
		}(),
		{
			name: "outgoing rule has unknown quorum type: failed to parse",
			mode: outgoingCohortMode,
			recruitedStatuses: []*clustermetadatapb.ConsensusStatus{
				makeStatus(a, &clustermetadatapb.ShardRule{
					RuleNumber:       &clustermetadatapb.RuleNumber{CoordinatorTerm: 3},
					CohortMembers:    cohort,
					DurabilityPolicy: &clustermetadatapb.DurabilityPolicy{QuorumType: clustermetadatapb.QuorumType_QUORUM_TYPE_UNKNOWN},
				}, testRevocation),
				makeStatus(b, &clustermetadatapb.ShardRule{
					RuleNumber:       &clustermetadatapb.RuleNumber{CoordinatorTerm: 3},
					CohortMembers:    cohort,
					DurabilityPolicy: &clustermetadatapb.DurabilityPolicy{QuorumType: clustermetadatapb.QuorumType_QUORUM_TYPE_UNKNOWN},
				}, testRevocation),
				makeStatus(c, &clustermetadatapb.ShardRule{
					RuleNumber:       &clustermetadatapb.RuleNumber{CoordinatorTerm: 3},
					CohortMembers:    cohort,
					DurabilityPolicy: &clustermetadatapb.DurabilityPolicy{QuorumType: clustermetadatapb.QuorumType_QUORUM_TYPE_UNKNOWN},
				}, testRevocation),
			},
			buildProposal: simpleProposal(5, cohort),
			wantErr:       "failed to parse durability policy from rule: unsupported quorum type: QUORUM_TYPE_UNKNOWN",
		},
		{
			name: "outgoing mode, no committed rule among recruited nodes",
			mode: outgoingCohortMode,
			recruitedStatuses: []*clustermetadatapb.ConsensusStatus{
				makeStatusWithLSN(a, nil, testRevocation, "0/2000000"),
				makeStatusWithLSN(b, nil, testRevocation, "0/1000000"),
			},
			buildProposal: simpleProposal(5, cohort),
			wantErr:       "no committed rule found among recruited nodes; cannot determine cohort for quorum check",
		},
		{
			name: "validate proposal: mismatched term revocation",
			mode: outgoingCohortMode,
			recruitedStatuses: []*clustermetadatapb.ConsensusStatus{
				makeStatus(a, rule, testRevocation),
				makeStatus(b, rule, testRevocation),
				makeStatus(c, rule, testRevocation),
			},
			buildProposal: func(r RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error) {
				return &consensusdatapb.CoordinatorProposal{
					TermRevocation: &clustermetadatapb.TermRevocation{RevokedBelowTerm: 99},
					ProposalLeader: &consensusdatapb.ProposalLeader{Id: a},
					ProposedRule:   rule,
				}, nil
			},
			wantErr: "proposal validation: proposal term revocation does not match the recruitment revocation",
		},
		{
			name: "validate proposal: nil leader ID",
			mode: outgoingCohortMode,
			recruitedStatuses: []*clustermetadatapb.ConsensusStatus{
				makeStatus(a, rule, testRevocation),
				makeStatus(b, rule, testRevocation),
				makeStatus(c, rule, testRevocation),
			},
			buildProposal: func(r RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error) {
				return &consensusdatapb.CoordinatorProposal{
					TermRevocation: r.TermRevocation,
					ProposalLeader: &consensusdatapb.ProposalLeader{Id: nil},
					ProposedRule:   rule,
				}, nil
			},
			wantErr: "proposal validation: proposal has no leader ID",
		},
		{
			name: "validate proposal: nil proposed rule",
			mode: outgoingCohortMode,
			recruitedStatuses: []*clustermetadatapb.ConsensusStatus{
				makeStatus(a, rule, testRevocation),
				makeStatus(b, rule, testRevocation),
				makeStatus(c, rule, testRevocation),
			},
			buildProposal: func(r RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error) {
				return &consensusdatapb.CoordinatorProposal{
					TermRevocation: r.TermRevocation,
					ProposalLeader: &consensusdatapb.ProposalLeader{Id: a},
					ProposedRule:   nil,
				}, nil
			},
			wantErr: "proposal validation: no proposed rule",
		},
		{
			name: "validate proposal: nil durability policy",
			mode: outgoingCohortMode,
			recruitedStatuses: []*clustermetadatapb.ConsensusStatus{
				makeStatus(a, rule, testRevocation),
				makeStatus(b, rule, testRevocation),
				makeStatus(c, rule, testRevocation),
			},
			buildProposal: func(r RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error) {
				return &consensusdatapb.CoordinatorProposal{
					TermRevocation: r.TermRevocation,
					ProposalLeader: &consensusdatapb.ProposalLeader{Id: a},
					ProposedRule: &clustermetadatapb.ShardRule{
						RuleNumber:    &clustermetadatapb.RuleNumber{CoordinatorTerm: 5},
						PrimaryId:     a,
						CohortMembers: cohort,
					},
				}, nil
			},
			wantErr: "proposal validation: invalid durability policy in proposal: durability policy is nil",
		},
		{
			name: "validate proposal: proposed rule term above recruitment revocation term",
			mode: outgoingCohortMode,
			recruitedStatuses: []*clustermetadatapb.ConsensusStatus{
				makeStatus(a, rule, testRevocation),
				makeStatus(b, rule, testRevocation),
				makeStatus(c, rule, testRevocation),
			},
			buildProposal: func(r RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error) {
				return &consensusdatapb.CoordinatorProposal{
					TermRevocation: r.TermRevocation,
					ProposalLeader: &consensusdatapb.ProposalLeader{Id: a},
					ProposedRule:   makeRule(99, cohort),
				}, nil
			},
			wantErr: "proposal validation: proposed rule term 99 is above the recruitment revocation term 5",
		},
		// ── incoming cohort mode (bootstrap) ─────────────────────────────────────
		{
			name: "bootstrap: nil outgoing rule allowed, highest LSN leads",
			mode: incomingCohortMode,
			recruitedStatuses: []*clustermetadatapb.ConsensusStatus{
				makeStatusWithLSN(a, nil, testRevocation, "0/3000000"),
				makeStatusWithLSN(b, nil, testRevocation, "0/2000000"),
				makeStatusWithLSN(c, nil, testRevocation, "0/1000000"),
			},
			buildProposal: func(r RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error) {
				leader := r.EligibleLeaders[0]
				return &consensusdatapb.CoordinatorProposal{
					TermRevocation: r.TermRevocation,
					ProposalLeader: &consensusdatapb.ProposalLeader{Id: leader.GetId()},
					ProposedRule: &clustermetadatapb.ShardRule{
						RuleNumber:       &clustermetadatapb.RuleNumber{CoordinatorTerm: 5},
						CohortMembers:    cohort,
						DurabilityPolicy: topoclient.AtLeastN(2),
						PrimaryId:        leader.GetId(),
					},
				}, nil
			},
			wantLeader: "pooler-a",
		},
		{
			name: "bootstrap: 1 of 3 recruited — cannot achieve durability",
			mode: incomingCohortMode,
			recruitedStatuses: []*clustermetadatapb.ConsensusStatus{
				makeStatusWithLSN(a, nil, testRevocation, "0/1000000"),
			},
			buildProposal: func(r RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error) {
				leader := r.EligibleLeaders[0]
				return &consensusdatapb.CoordinatorProposal{
					TermRevocation: r.TermRevocation,
					ProposalLeader: &consensusdatapb.ProposalLeader{Id: leader.GetId()},
					ProposedRule: &clustermetadatapb.ShardRule{
						RuleNumber:       &clustermetadatapb.RuleNumber{CoordinatorTerm: 5},
						CohortMembers:    cohort,
						DurabilityPolicy: topoclient.AtLeastN(2),
						PrimaryId:        leader.GetId(),
					},
				}, nil
			},
			wantErr: "proposal validation: recruited proposed cohort cannot achieve durability: durability not achievable: proposed cohort has 1 poolers, required 2",
		},
		{
			name: "bootstrap: unknown quorum type in proposed rule",
			mode: incomingCohortMode,
			recruitedStatuses: []*clustermetadatapb.ConsensusStatus{
				makeStatusWithLSN(a, nil, testRevocation, "0/1000000"),
				makeStatusWithLSN(b, nil, testRevocation, "0/1000000"),
				makeStatusWithLSN(c, nil, testRevocation, "0/1000000"),
			},
			buildProposal: func(r RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error) {
				leader := r.EligibleLeaders[0]
				return &consensusdatapb.CoordinatorProposal{
					TermRevocation: r.TermRevocation,
					ProposalLeader: &consensusdatapb.ProposalLeader{Id: leader.GetId()},
					ProposedRule: &clustermetadatapb.ShardRule{
						RuleNumber:       &clustermetadatapb.RuleNumber{CoordinatorTerm: 5},
						CohortMembers:    cohort,
						DurabilityPolicy: &clustermetadatapb.DurabilityPolicy{QuorumType: clustermetadatapb.QuorumType_QUORUM_TYPE_UNKNOWN},
						PrimaryId:        leader.GetId(),
					},
				}, nil
			},
			wantErr: "proposal validation: invalid durability policy in proposal: unsupported quorum type: QUORUM_TYPE_UNKNOWN",
		},
		func() tc {
			// 5-node proposed cohort [a,b,d,e,f], only a+b recruited. AT_LEAST_2
			// achievability passes (2≥N=2) but majority of 5 requires 3 → fails.
			d := makeID("z1", "pooler-d")
			e := makeID("z1", "pooler-e")
			f := makeID("z1", "pooler-f")
			bigCohort := []*clustermetadatapb.ID{a, b, d, e, f}
			return tc{
				name: "bootstrap: proposed cohort achievable but insufficient majority",
				mode: incomingCohortMode,
				recruitedStatuses: []*clustermetadatapb.ConsensusStatus{
					makeStatusWithLSN(a, nil, testRevocation, "0/3000000"),
					makeStatusWithLSN(b, nil, testRevocation, "0/2000000"),
				},
				buildProposal: func(r RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error) {
					leader := r.EligibleLeaders[0]
					return &consensusdatapb.CoordinatorProposal{
						TermRevocation: r.TermRevocation,
						ProposalLeader: &consensusdatapb.ProposalLeader{Id: leader.GetId()},
						ProposedRule: &clustermetadatapb.ShardRule{
							RuleNumber:       &clustermetadatapb.RuleNumber{CoordinatorTerm: 5},
							CohortMembers:    bigCohort,
							DurabilityPolicy: topoclient.AtLeastN(2),
							PrimaryId:        leader.GetId(),
						},
					}, nil
				},
				wantErr: "proposal validation: insufficient proposed cohort recruitment: majority not satisfied: recruited 2 of 5 cohort poolers, need at least 3",
			}
		}(),
		func() tc {
			// A in zone1 applied MULTI_CELL_AT_LEAST_2 at term 6; B and C in zone2
			// never received it (still at AT_LEAST_2 term 5). All three recruited
			// at term 7. A coordinator that re-proposes the term-6 outgoing rule
			// to propagate it is rejected: proposed term (6) < revocation term (7).
			// See the TODO in validateProposal for the two-round recovery path.
			nodeA := makeID("zone1", "pooler-a")
			nodeB := makeID("zone2", "pooler-b")
			nodeC := makeID("zone2", "pooler-c")
			stuckCohort := []*clustermetadatapb.ID{nodeA, nodeB, nodeC}
			stuckRev := &clustermetadatapb.TermRevocation{RevokedBelowTerm: 7}
			multiCellRule := &clustermetadatapb.ShardRule{
				RuleNumber:       &clustermetadatapb.RuleNumber{CoordinatorTerm: 6},
				PrimaryId:        nodeA,
				CohortMembers:    stuckCohort,
				DurabilityPolicy: topoclient.MultiCellAtLeastN(2),
			}
			atLeastNRule := &clustermetadatapb.ShardRule{
				RuleNumber:       &clustermetadatapb.RuleNumber{CoordinatorTerm: 5},
				PrimaryId:        nodeA,
				CohortMembers:    stuckCohort,
				DurabilityPolicy: topoclient.AtLeastN(2),
			}
			return tc{
				name:       "stuck rule change: coordinator re-proposes outgoing rule below revocation term",
				mode:       outgoingCohortMode,
				revocation: stuckRev,
				recruitedStatuses: []*clustermetadatapb.ConsensusStatus{
					makeStatus(nodeA, multiCellRule, stuckRev),
					makeStatus(nodeB, atLeastNRule, stuckRev),
					makeStatus(nodeC, atLeastNRule, stuckRev),
				},
				buildProposal: func(r RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error) {
					return &consensusdatapb.CoordinatorProposal{
						TermRevocation: r.TermRevocation,
						ProposalLeader: &consensusdatapb.ProposalLeader{Id: r.EligibleLeaders[0].GetId()},
						ProposedRule:   r.OutgoingRule, // re-propose term-6 rule to propagate it
					}, nil
				},
				wantErr: "proposal validation: proposed rule term 6 is below the recruitment revocation term 7",
			}
		}(),
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bp := tt.buildProposal
			rev := testRevocation
			if tt.revocation != nil {
				rev = tt.revocation
			}
			var gotResult RecruitmentResult
			proposal, err := buildProposalCore(rev, tt.recruitedStatuses, tt.mode,
				func(r RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error) {
					gotResult = r
					return bp(r)
				})
			if tt.wantErr != "" {
				assert.EqualError(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
				if tt.wantLeader != "" {
					assert.Equal(t, tt.wantLeader, proposal.GetProposalLeader().GetId().GetName())
				}
				if tt.checkResult != nil {
					tt.checkResult(t, gotResult)
				}
			}
		})
	}
}

// TestBuildSafeProposal tests the filterByRevocation wrapper behavior — it only
// verifies that the exact-match revocation filter and the "no nodes accepted"
// early exit work correctly. All other behaviors are covered by TestBuildProposalCore.
func TestBuildSafeProposal(t *testing.T) {
	a := makeID("z1", "pooler-a")
	b := makeID("z1", "pooler-b")
	c := makeID("z1", "pooler-c")
	cohort := []*clustermetadatapb.ID{a, b, c}
	rule := makeRule(3, cohort)

	coordID := makeID("z1", "multiorch-1")
	rev := &clustermetadatapb.TermRevocation{
		RevokedBelowTerm:       5,
		AcceptedCoordinatorId:  coordID,
		CoordinatorInitiatedAt: &timestamppb.Timestamp{Seconds: 1000},
	}

	tests := []struct {
		name     string
		statuses []*clustermetadatapb.ConsensusStatus
		wantErr  string
	}{
		{
			name:    "nil statuses: no nodes accepted",
			wantErr: "no nodes accepted the requested term revocation",
		},
		{
			name: "all nodes have old revocation: no nodes accepted",
			statuses: []*clustermetadatapb.ConsensusStatus{
				makeStatus(a, rule, &clustermetadatapb.TermRevocation{RevokedBelowTerm: 3}),
				makeStatus(b, rule, &clustermetadatapb.TermRevocation{RevokedBelowTerm: 3}),
				makeStatus(c, rule, &clustermetadatapb.TermRevocation{RevokedBelowTerm: 3}),
			},
			wantErr: "no nodes accepted the requested term revocation",
		},
		{
			name: "partial acceptance, quorum met: 2 of 3 accept exact revocation",
			statuses: []*clustermetadatapb.ConsensusStatus{
				makeStatus(a, rule, rev),
				makeStatus(b, rule, rev),
				makeStatus(c, rule, &clustermetadatapb.TermRevocation{RevokedBelowTerm: 3}),
			},
		},
		{
			name: "partial acceptance, quorum not met: only 1 of 3 accepts",
			statuses: []*clustermetadatapb.ConsensusStatus{
				makeStatus(a, rule, rev),
				makeStatus(b, rule, &clustermetadatapb.TermRevocation{RevokedBelowTerm: 3}),
				makeStatus(c, rule, &clustermetadatapb.TermRevocation{RevokedBelowTerm: 3}),
			},
			wantErr: "insufficient outgoing cohort recruitment: majority not satisfied: recruited 1 of 3 cohort poolers, need at least 2",
		},
		{
			name: "lower-term revocation does not count",
			statuses: []*clustermetadatapb.ConsensusStatus{
				makeStatus(a, rule, &clustermetadatapb.TermRevocation{RevokedBelowTerm: 3, AcceptedCoordinatorId: coordID}),
				makeStatus(b, rule, &clustermetadatapb.TermRevocation{RevokedBelowTerm: 3, AcceptedCoordinatorId: coordID}),
				makeStatus(c, rule, &clustermetadatapb.TermRevocation{RevokedBelowTerm: 3, AcceptedCoordinatorId: coordID}),
			},
			wantErr: "no nodes accepted the requested term revocation",
		},
		{
			name: "higher-term revocation does not count",
			statuses: []*clustermetadatapb.ConsensusStatus{
				makeStatus(a, rule, &clustermetadatapb.TermRevocation{RevokedBelowTerm: 9, AcceptedCoordinatorId: coordID}),
				makeStatus(b, rule, &clustermetadatapb.TermRevocation{RevokedBelowTerm: 9, AcceptedCoordinatorId: coordID}),
				makeStatus(c, rule, &clustermetadatapb.TermRevocation{RevokedBelowTerm: 9, AcceptedCoordinatorId: coordID}),
			},
			wantErr: "no nodes accepted the requested term revocation",
		},
		{
			name: "rival coordinator ID does not count",
			statuses: []*clustermetadatapb.ConsensusStatus{
				makeStatus(a, rule, &clustermetadatapb.TermRevocation{RevokedBelowTerm: 5, AcceptedCoordinatorId: makeID("z1", "multiorch-2"), CoordinatorInitiatedAt: &timestamppb.Timestamp{Seconds: 1000}}),
				makeStatus(b, rule, &clustermetadatapb.TermRevocation{RevokedBelowTerm: 5, AcceptedCoordinatorId: makeID("z1", "multiorch-2"), CoordinatorInitiatedAt: &timestamppb.Timestamp{Seconds: 1000}}),
				makeStatus(c, rule, &clustermetadatapb.TermRevocation{RevokedBelowTerm: 5, AcceptedCoordinatorId: makeID("z1", "multiorch-2"), CoordinatorInitiatedAt: &timestamppb.Timestamp{Seconds: 1000}}),
			},
			wantErr: "no nodes accepted the requested term revocation",
		},
		{
			name: "stale initiated_at does not count",
			statuses: []*clustermetadatapb.ConsensusStatus{
				makeStatus(a, rule, &clustermetadatapb.TermRevocation{RevokedBelowTerm: 5, AcceptedCoordinatorId: coordID, CoordinatorInitiatedAt: &timestamppb.Timestamp{Seconds: 999}}),
				makeStatus(b, rule, &clustermetadatapb.TermRevocation{RevokedBelowTerm: 5, AcceptedCoordinatorId: coordID, CoordinatorInitiatedAt: &timestamppb.Timestamp{Seconds: 999}}),
				makeStatus(c, rule, &clustermetadatapb.TermRevocation{RevokedBelowTerm: 5, AcceptedCoordinatorId: coordID, CoordinatorInitiatedAt: &timestamppb.Timestamp{Seconds: 999}}),
			},
			wantErr: "no nodes accepted the requested term revocation",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := BuildSafeProposal(rev, tt.statuses, simpleProposal(5, cohort))
			if tt.wantErr != "" {
				assert.EqualError(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestBuildProposalCore_EligibleLeadersOrderDeterministic(t *testing.T) {
	// EligibleLeaders must be in the same order regardless of which order
	// statuses arrive. Coordinators that pick by index need a stable list.
	a := makeID("z1", "pooler-a")
	b := makeID("z1", "pooler-b")
	c := makeID("z1", "pooler-c")
	cohort := []*clustermetadatapb.ID{a, b, c}
	rule := makeRule(3, cohort)

	forward := []*clustermetadatapb.ConsensusStatus{
		makeStatus(a, rule, testRevocation),
		makeStatus(b, rule, testRevocation),
		makeStatus(c, rule, testRevocation),
	}
	reversed := []*clustermetadatapb.ConsensusStatus{
		makeStatus(c, rule, testRevocation),
		makeStatus(b, rule, testRevocation),
		makeStatus(a, rule, testRevocation),
	}

	collect := func(statuses []*clustermetadatapb.ConsensusStatus) []string {
		var names []string
		_, err := BuildSafeProposal(testRevocation, statuses, func(r RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error) {
			for _, cs := range r.EligibleLeaders {
				names = append(names, cs.GetId().GetName())
			}
			return &consensusdatapb.CoordinatorProposal{
				TermRevocation: r.TermRevocation,
				ProposalLeader: &consensusdatapb.ProposalLeader{Id: r.EligibleLeaders[0].GetId()},
				ProposedRule:   makeRule(5, cohort),
			}, nil
		})
		require.NoError(t, err)
		return names
	}

	assert.Equal(t, collect(forward), collect(reversed), "eligible leader order must not depend on input order")
}

func TestBuildSafeProposal_CohortReplacementSplitBrain(t *testing.T) {
	// KNOWN LIMITATION: documents a split-brain scenario that CheckSufficientRecruitment
	// cannot detect. See the TODO in CheckSufficientRecruitment.
	//
	// A is the primary for cohort [A, B, C] (AT_LEAST_2). A coordinator sends a
	// Propose to replace the cohort with [D, E, F]. A writes the new rule to its
	// rule_history; D and E stream that WAL from A and apply it. But B and C never
	// receive the new rule, and A crashes before the coordinator gets a Propose
	// response — so the new rule was never durably decided. Under the outgoing
	// cohort's policy (AT_LEAST_2 on [A, B, C]), only A applied the rule change;
	// it needed at least one of B or C to commit. The new rule is a phantom: it
	// exists in D and E's WAL but was never agreed to by the outgoing cohort.
	//
	// Two coordinators now independently recruit disjoint sets of nodes:
	//
	//   Coordinator 1 sees B and C (both at old rule, cohort [A,B,C]):
	//     - outgoingRule = old rule, cohort = [A,B,C], recruited 2 of 3 → AT_LEAST_2 ✓
	//     - promotes B with cohort [B,C]
	//
	//   Coordinator 2 sees D and E (both at new rule, cohort [D,E,F]):
	//     - outgoingRule = new rule (phantom), cohort = [D,E,F], recruited 2 of 3 → AT_LEAST_2 ✓
	//     - promotes D with cohort [D,E]
	//
	// The two recruited sets share no nodes. Both promotions succeed, yielding
	// two independent primaries — split brain.
	//
	// A correct implementation would reject the new rule as outgoingRule when it has
	// not achieved quorum under the outgoing cohort's policy. We don't yet have
	// enough information from Recruit responses to enforce this. When the TODO is
	// resolved, at least one of the two calls below should return an error.
	a := makeID("z1", "pooler-a") // old primary, crashed — not recruited
	b := makeID("z1", "pooler-b") // old cohort, responds to coord 1
	c := makeID("z1", "pooler-c") // old cohort, responds to coord 1
	d := makeID("z1", "pooler-d") // new cohort, responds to coord 2
	e := makeID("z1", "pooler-e") // new cohort, responds to coord 2
	f := makeID("z1", "pooler-f") // new cohort, unreachable

	_ = a
	_ = f

	oldRule := makeRule(3, []*clustermetadatapb.ID{a, b, c})
	newRule := makeRule(4, []*clustermetadatapb.ID{d, e, f})

	// Each coordinator has its own TermRevocation (different accepted_coordinator_id).
	// B and C accepted coordinator 1; D and E accepted coordinator 2.
	revocationCoord1 := &clustermetadatapb.TermRevocation{
		RevokedBelowTerm:      6,
		AcceptedCoordinatorId: makeID("z1", "multiorch-1"),
	}
	revocationCoord2 := &clustermetadatapb.TermRevocation{
		RevokedBelowTerm:      6,
		AcceptedCoordinatorId: makeID("z1", "multiorch-2"),
	}

	// All four responding nodes' statuses are in the same pool. The revocation
	// embedded in each status records which coordinator that node pledged to.
	allStatuses := []*clustermetadatapb.ConsensusStatus{
		makeStatusWithLSN(b, oldRule, revocationCoord1, "0/3000000"),
		makeStatusWithLSN(c, oldRule, revocationCoord1, "0/3000000"),
		makeStatusWithLSN(d, newRule, revocationCoord2, "0/4000000"),
		makeStatusWithLSN(e, newRule, revocationCoord2, "0/4000000"),
	}

	// Each coordinator passes the full pool but its own revocation. The filtering
	// step ensures each coordinator only counts nodes that pledged to it.
	proposal1, err1 := BuildSafeProposal(revocationCoord1, allStatuses, func(r RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error) {
		return &consensusdatapb.CoordinatorProposal{
			TermRevocation: r.TermRevocation,
			ProposalLeader: &consensusdatapb.ProposalLeader{Id: b},
			ProposedRule:   makeRule(6, []*clustermetadatapb.ID{b, c}),
		}, nil
	})
	proposal2, err2 := BuildSafeProposal(revocationCoord2, allStatuses, func(r RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error) {
		return &consensusdatapb.CoordinatorProposal{
			TermRevocation: r.TermRevocation,
			ProposalLeader: &consensusdatapb.ProposalLeader{Id: d},
			ProposedRule:   makeRule(6, []*clustermetadatapb.ID{d, e}),
		}, nil
	})

	require.NoError(t, err1)
	assert.NotNil(t, proposal1)
	require.NoError(t, err2)
	assert.NotNil(t, proposal2)
}

func TestSameCohort(t *testing.T) {
	a := makeID("z1", "a")
	b := makeID("z1", "b")
	c := makeID("z1", "c")

	assert.True(t, sameCohort([]*clustermetadatapb.ID{a, b}, []*clustermetadatapb.ID{b, a}), "order should not matter")
	assert.False(t, sameCohort([]*clustermetadatapb.ID{a, b}, []*clustermetadatapb.ID{a, c}))
	assert.False(t, sameCohort([]*clustermetadatapb.ID{a, b}, []*clustermetadatapb.ID{a, b, c}))
	assert.True(t, sameCohort(nil, nil))
}

func TestCheckProposalPossible(t *testing.T) {
	a, b, c := makeID("z1", "a"), makeID("z1", "b"), makeID("z1", "c")
	cohort := []*clustermetadatapb.ID{a, b, c}
	rule := makeRule(3, cohort)     // coord term 3 < revocation term 5: nodes can accept
	highRule := makeRule(5, cohort) // coord term 5 = revocation term: nodes cannot accept

	tests := []struct {
		name     string
		statuses []*clustermetadatapb.ConsensusStatus
		wantErr  string
	}{
		{
			name: "all nodes eligible",
			statuses: []*clustermetadatapb.ConsensusStatus{
				makeUnrecruitedStatus(a, rule),
				makeUnrecruitedStatus(b, rule),
				makeUnrecruitedStatus(c, rule),
			},
		},
		{
			name: "no nodes can accept: all at revocation term",
			statuses: []*clustermetadatapb.ConsensusStatus{
				makeUnrecruitedStatus(a, highRule),
				makeUnrecruitedStatus(b, highRule),
			},
			wantErr: "no nodes could accept the proposed revocation",
		},
		{
			name: "insufficient quorum: only one of three can accept",
			statuses: []*clustermetadatapb.ConsensusStatus{
				makeUnrecruitedStatus(a, rule),
				makeUnrecruitedStatus(b, highRule),
				makeUnrecruitedStatus(c, highRule),
			},
			wantErr: "insufficient outgoing cohort recruitment: majority not satisfied: recruited 1 of 3 cohort poolers, need at least 2",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := CheckProposalPossible(testCoordRevocation, tt.statuses, simpleProposal(5, cohort))
			if tt.wantErr != "" {
				assert.EqualError(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestCheckForcedProposalPossible(t *testing.T) {
	a, b, c := makeID("z1", "a"), makeID("z1", "b"), makeID("z1", "c")
	cohort := []*clustermetadatapb.ID{a, b, c}

	bootstrapProposal := func(r RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error) {
		leader := r.EligibleLeaders[0]
		return &consensusdatapb.CoordinatorProposal{
			TermRevocation: r.TermRevocation,
			ProposalLeader: &consensusdatapb.ProposalLeader{Id: leader.GetId()},
			ProposedRule: &clustermetadatapb.ShardRule{
				RuleNumber:       &clustermetadatapb.RuleNumber{CoordinatorTerm: r.TermRevocation.GetRevokedBelowTerm()},
				CohortMembers:    cohort,
				DurabilityPolicy: topoclient.AtLeastN(2),
				PrimaryId:        leader.GetId(),
			},
		}, nil
	}

	tests := []struct {
		name     string
		statuses []*clustermetadatapb.ConsensusStatus
		wantErr  string
	}{
		{
			name: "bootstrap: nodes at term 0 can accept",
			statuses: []*clustermetadatapb.ConsensusStatus{
				makeUnrecruitedStatus(a, nil),
				makeUnrecruitedStatus(b, nil),
				makeUnrecruitedStatus(c, nil),
			},
		},
		{
			name: "no nodes can accept: all at or above revocation term",
			statuses: []*clustermetadatapb.ConsensusStatus{
				makeUnrecruitedStatus(a, makeRule(5, cohort)),
			},
			wantErr: "no nodes could accept the proposed revocation",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := CheckForcedProposalPossible(testCoordRevocation, tt.statuses, bootstrapProposal)
			if tt.wantErr != "" {
				assert.EqualError(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestBuildForcedProposal_NoNodesAccepted(t *testing.T) {
	a := makeID("z1", "a")
	cohort := []*clustermetadatapb.ID{a}
	_, err := BuildForcedProposal(testRevocation, []*clustermetadatapb.ConsensusStatus{
		makeStatus(a, makeRule(3, cohort), &clustermetadatapb.TermRevocation{RevokedBelowTerm: 3}),
	}, simpleProposal(5, cohort))
	assert.EqualError(t, err, "no nodes accepted the requested term revocation")
}

func TestDeduplicateStatuses_NilIDSkipped(t *testing.T) {
	a := makeID("z1", "a")
	cohort := []*clustermetadatapb.ID{a}

	statuses := []*clustermetadatapb.ConsensusStatus{
		{Id: nil},
		makeStatus(a, makeRule(1, cohort), testRevocation),
	}

	result := deduplicateStatuses(statuses)
	require.Len(t, result, 1)
	assert.Equal(t, "a", result[0].GetId().GetName())
}

func TestCohortIntersect_NilIDSkipped(t *testing.T) {
	a := makeID("z1", "a")
	cohort := []*clustermetadatapb.ID{a}

	statuses := []*clustermetadatapb.ConsensusStatus{
		{Id: nil},
		makeStatus(a, makeRule(1, cohort), testRevocation),
	}

	result := cohortIntersect(cohort, statuses)
	require.Len(t, result, 1)
	assert.Equal(t, "a", result[0].GetName())
}
