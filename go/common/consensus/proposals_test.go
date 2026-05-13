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

// ruleNum builds a *RuleNumber
//
//	ruleNum(3, 5)     // term=3, subterm=5
func ruleNum(term int64, subterm int64) *clustermetadatapb.RuleNumber {
	return &clustermetadatapb.RuleNumber{CoordinatorTerm: term, LeaderSubterm: subterm}
}

// makeRule builds a ShardRule with rule number, durability policy, and cohort
// all explicit at the call site. The first cohort member is used as the leader.
//
//	rule := makeRule(ruleNum(3, 0), atLeast(2), a, b, c)
//	rule := makeRule(ruleNum(3, 0), atLeast(2), cohort...)         // pre-built slice
//	rule := makeRule(ruleNum(6, 0), multiCell(2), zone1.a, zone2.b, zone2.c)
//	rule := makeRule(ruleNum(3, 5), atLeast(2), a, b, c)           // term=3, subterm=5
func makeRule(num *clustermetadatapb.RuleNumber, policy *clustermetadatapb.DurabilityPolicy, cohort ...*clustermetadatapb.ID) *clustermetadatapb.ShardRule {
	var leader *clustermetadatapb.ID
	if len(cohort) > 0 {
		leader = cohort[0]
	}
	return &clustermetadatapb.ShardRule{
		RuleNumber:       num,
		LeaderId:         leader,
		CohortMembers:    cohort,
		DurabilityPolicy: policy,
	}
}

// atLeast wraps topoclient.AtLeastN for use in newRule calls.
func atLeast(n int32) *clustermetadatapb.DurabilityPolicy {
	return topoclient.AtLeastN(n)
}

// multiCell wraps topoclient.MultiCellAtLeastN for use in newRule calls.
func multiCell(n int32) *clustermetadatapb.DurabilityPolicy {
	return topoclient.MultiCellAtLeastN(n)
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

// proposeFirstEligible returns a buildProposal callback that picks
// r.EligibleLeaders[0] as the proposed leader and uses the given rule.
func proposeFirstEligible(rule *clustermetadatapb.ShardRule) func(RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error) {
	return func(r RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error) {
		leader := r.EligibleLeaders[0]
		return &consensusdatapb.CoordinatorProposal{
			TermRevocation: r.TermRevocation,
			ProposalLeader: &consensusdatapb.ProposalLeader{
				Id:   leader.GetId(),
				Host: "localhost",
			},
			ProposedRule: rule,
		}, nil
	}
}

// revocation builds a *TermRevocation at the given term. Use inline so each
// test makes its term explicit.
func revocation(term int64) *clustermetadatapb.TermRevocation {
	return &clustermetadatapb.TermRevocation{RevokedBelowTerm: term}
}

// coordRevocation builds a *TermRevocation with coordinator fields populated.
// Used by tests exercising ValidateRevocation / CheckProposalPossible.
func coordRevocation(term int64) *clustermetadatapb.TermRevocation {
	return &clustermetadatapb.TermRevocation{
		RevokedBelowTerm:       term,
		AcceptedCoordinatorId:  makeID("zone1", "coord-1"),
		CoordinatorInitiatedAt: timestamppb.Now(),
	}
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

// cellPoolers holds the standard test pooler IDs (a..f) for one cell. The
// `all` field is the same six IDs as a slice in order, useful for
// `cohort := poolerIDs.zone1.all[:3]` style cohort construction.
type cellPoolers struct {
	a, b, c, d, e, f *clustermetadatapb.ID
	all              []*clustermetadatapb.ID
}

func newCellPoolers(cell string) cellPoolers {
	a := makeID(cell, "pooler-a")
	b := makeID(cell, "pooler-b")
	c := makeID(cell, "pooler-c")
	d := makeID(cell, "pooler-d")
	e := makeID(cell, "pooler-e")
	f := makeID(cell, "pooler-f")
	return cellPoolers{
		a: a, b: b, c: c, d: d, e: e, f: f,
		all: []*clustermetadatapb.ID{a, b, c, d, e, f},
	}
}

// poolerIDs holds pre-declared pooler IDs for tests. Reference as
// poolerIDs.<cell>.<letter>, e.g. poolerIDs.zone1.a is the ID for "zone1/pooler-a".
// Tests build their own cohorts and rules inline from these IDs.
var poolerIDs = struct {
	zone1, zone2, zone3 cellPoolers
}{
	zone1: newCellPoolers("zone1"),
	zone2: newCellPoolers("zone2"),
	zone3: newCellPoolers("zone3"),
}

func TestBuildProposalCore(t *testing.T) {
	type tc struct {
		name              string
		mode              quorumMode
		revocation        *clustermetadatapb.TermRevocation
		recruitedStatuses []*clustermetadatapb.ConsensusStatus
		buildProposal     func(RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error)
		wantLeader        string
		checkResult       func(*testing.T, RecruitmentResult)
		wantErr           string
	}

	tests := []tc{
		// ── outgoing cohort mode ──────────────────────────────────────────────────
		func() tc {
			zone1 := poolerIDs.zone1
			cohort := zone1.all[:3]
			rule := makeRule(ruleNum(3, 0), atLeast(2), cohort...)
			return tc{
				name:       "all 3 recruited: success, pooler-a is first eligible leader",
				mode:       requireTransitionQuorum,
				revocation: revocation(5),
				recruitedStatuses: []*clustermetadatapb.ConsensusStatus{
					makeStatus(zone1.a, rule, revocation(5)),
					makeStatus(zone1.b, rule, revocation(5)),
					makeStatus(zone1.c, rule, revocation(5)),
				},
				buildProposal: proposeFirstEligible(makeRule(ruleNum(5, 0), atLeast(2), cohort...)),
				wantLeader:    "pooler-a",
			}
		}(),
		func() tc {
			zone1 := poolerIDs.zone1
			cohort := zone1.all[:3]
			return tc{
				name:       "no current_position: filtered by filterByValidPosition",
				mode:       requireTransitionQuorum,
				revocation: revocation(5),
				recruitedStatuses: []*clustermetadatapb.ConsensusStatus{
					{Id: zone1.a, TermRevocation: revocation(5)}, // no current_position → no LSN
				},
				buildProposal: proposeFirstEligible(makeRule(ruleNum(5, 0), atLeast(2), cohort...)),
				wantErr:       "all recruited nodes reported an invalid or missing WAL position",
			}
		}(),
		func() tc {
			zone1 := poolerIDs.zone1
			cohort := zone1.all[:3]
			rule := makeRule(ruleNum(3, 0), atLeast(2), cohort...)
			return tc{
				name:       "1 of 3 recruited: insufficient outgoing cohort",
				mode:       requireTransitionQuorum,
				revocation: revocation(5),
				recruitedStatuses: []*clustermetadatapb.ConsensusStatus{
					makeStatus(zone1.a, rule, revocation(5)),
				},
				buildProposal: proposeFirstEligible(makeRule(ruleNum(5, 0), atLeast(2), cohort...)),
				wantErr:       "insufficient outgoing cohort recruitment: majority not satisfied: recruited 1 of 3 cohort poolers, need at least 2",
			}
		}(),
		func() tc {
			zone1 := poolerIDs.zone1
			cohort := zone1.all[:3]
			rule := makeRule(ruleNum(3, 0), atLeast(2), cohort...)
			return tc{
				name:       "callback proposes outsider: not among eligible leaders",
				mode:       requireTransitionQuorum,
				revocation: revocation(5),
				recruitedStatuses: []*clustermetadatapb.ConsensusStatus{
					makeStatus(zone1.a, rule, revocation(5)),
					makeStatus(zone1.b, rule, revocation(5)),
					makeStatus(zone1.c, rule, revocation(5)),
				},
				buildProposal: func(r RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error) {
					outsider := makeID("zone1", "outsider")
					return &consensusdatapb.CoordinatorProposal{
						TermRevocation: r.TermRevocation,
						ProposalLeader: &consensusdatapb.ProposalLeader{Id: outsider},
						ProposedRule:   rule,
					}, nil
				},
				wantErr: "proposal validation: proposed leader zone1_outsider is not among eligible leaders",
			}
		}(),
		func() tc {
			zone1 := poolerIDs.zone1
			cohort := zone1.all[:3]
			rule := makeRule(ruleNum(3, 0), atLeast(2), cohort...)
			return tc{
				name:       "proposed cohort has unrecruited member but passes (outsider not recruited, a+b satisfy AT_LEAST_2)",
				mode:       requireTransitionQuorum,
				revocation: revocation(5),
				recruitedStatuses: []*clustermetadatapb.ConsensusStatus{
					makeStatus(zone1.a, rule, revocation(5)),
					makeStatus(zone1.b, rule, revocation(5)),
					makeStatus(zone1.c, rule, revocation(5)),
				},
				buildProposal: func(r RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error) {
					outsider := makeID("zone1", "outsider")
					return &consensusdatapb.CoordinatorProposal{
						TermRevocation: r.TermRevocation,
						ProposalLeader: &consensusdatapb.ProposalLeader{Id: zone1.a},
						ProposedRule:   makeRule(ruleNum(5, 0), atLeast(2), zone1.a, zone1.b, outsider),
					}, nil
				},
				wantLeader: "pooler-a",
			}
		}(),
		func() tc {
			zone1 := poolerIDs.zone1
			cohort := zone1.all[:3]
			rule := makeRule(ruleNum(3, 0), atLeast(2), cohort...)
			return tc{
				name:       "2 of 3 recruited, dead leader stays in proposed cohort",
				mode:       requireTransitionQuorum,
				revocation: revocation(5),
				recruitedStatuses: []*clustermetadatapb.ConsensusStatus{
					makeStatus(zone1.b, rule, revocation(5)),
					makeStatus(zone1.c, rule, revocation(5)),
				},
				buildProposal: func(r RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error) {
					return &consensusdatapb.CoordinatorProposal{
						TermRevocation: r.TermRevocation,
						ProposalLeader: &consensusdatapb.ProposalLeader{Id: zone1.b},
						ProposedRule:   makeRule(ruleNum(5, 0), atLeast(2), cohort...),
					}, nil
				},
				wantLeader: "pooler-b",
			}
		}(),
		func() tc {
			zone1 := poolerIDs.zone1
			cohort := zone1.all[:3]
			rule := makeRule(ruleNum(3, 0), atLeast(2), cohort...)
			return tc{
				name:       "proposed cohort has new members not recruited",
				mode:       requireTransitionQuorum,
				revocation: revocation(5),
				recruitedStatuses: []*clustermetadatapb.ConsensusStatus{
					makeStatus(zone1.a, rule, revocation(5)),
					makeStatus(zone1.b, rule, revocation(5)),
					makeStatus(zone1.c, rule, revocation(5)),
				},
				buildProposal: func(r RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error) {
					d := makeID("zone1", "pooler-d")
					e := makeID("zone1", "pooler-e")
					return &consensusdatapb.CoordinatorProposal{
						TermRevocation: r.TermRevocation,
						ProposalLeader: &consensusdatapb.ProposalLeader{Id: zone1.a},
						ProposedRule:   makeRule(ruleNum(5, 0), atLeast(2), zone1.a, d, e),
					}, nil
				},
				wantErr: "proposal validation: recruited proposed cohort cannot achieve durability: durability not achievable: proposed cohort has 1 poolers, required 2",
			}
		}(),
		func() tc {
			zone1 := poolerIDs.zone1
			cohort := zone1.all[:3]
			rule := makeRule(ruleNum(3, 0), atLeast(2), cohort...)
			return tc{
				name:       "callback returns error",
				mode:       requireTransitionQuorum,
				revocation: revocation(5),
				recruitedStatuses: []*clustermetadatapb.ConsensusStatus{
					makeStatus(zone1.a, rule, revocation(5)),
					makeStatus(zone1.b, rule, revocation(5)),
					makeStatus(zone1.c, rule, revocation(5)),
				},
				buildProposal: func(r RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error) {
					return nil, errors.New("no suitable candidate")
				},
				wantErr: "buildProposal: no suitable candidate",
			}
		}(),
		func() tc {
			zone1 := poolerIDs.zone1
			cohort := zone1.all[:3]
			rule := makeRule(ruleNum(3, 0), atLeast(2), cohort...)
			return tc{
				name:       "callback returns nil proposal",
				mode:       requireTransitionQuorum,
				revocation: revocation(5),
				recruitedStatuses: []*clustermetadatapb.ConsensusStatus{
					makeStatus(zone1.a, rule, revocation(5)),
					makeStatus(zone1.b, rule, revocation(5)),
					makeStatus(zone1.c, rule, revocation(5)),
				},
				buildProposal: func(r RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error) {
					return nil, nil
				},
				wantErr: "buildProposal returned nil proposal",
			}
		}(),
		func() tc {
			// One node behind: c is at oldRule, a+b at rule (term 3). Higher rule
			// governs quorum; only a and b are eligible.
			zone1 := poolerIDs.zone1
			cohort := zone1.all[:3]
			rule := makeRule(ruleNum(3, 0), atLeast(2), cohort...)
			return tc{
				name:       "mixed rule numbers: higher rule is outgoing, lagging node excluded from eligibles",
				mode:       requireTransitionQuorum,
				revocation: revocation(5),
				recruitedStatuses: []*clustermetadatapb.ConsensusStatus{
					makeStatus(zone1.a, rule, revocation(5)),
					makeStatus(zone1.b, rule, revocation(5)),
					makeStatus(zone1.c, makeRule(ruleNum(2, 0), atLeast(2), cohort...), revocation(5)), // behind
				},
				buildProposal: func(r RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error) {
					return &consensusdatapb.CoordinatorProposal{
						TermRevocation: r.TermRevocation,
						ProposalLeader: &consensusdatapb.ProposalLeader{Id: r.EligibleLeaders[0].GetId()},
						ProposedRule:   makeRule(ruleNum(5, 0), atLeast(2), cohort...),
					}, nil
				},
				wantLeader: "pooler-a",
				checkResult: func(t *testing.T, r RecruitmentResult) {
					assert.Equal(t, int64(3), r.OutgoingRule.GetRuleNumber().GetCoordinatorTerm())
					assert.Len(t, r.EligibleLeaders, 2, "only nodes at outgoingRule are eligible")
					assert.NotEqual(t, "pooler-c", r.EligibleLeaders[0].GetId().GetName())
				},
			}
		}(),
		func() tc {
			zone1 := poolerIDs.zone1
			cohort := zone1.all[:3]
			rule := makeRule(ruleNum(3, 0), atLeast(2), cohort...)
			return tc{
				name:       "proposed cohort too small for AT_LEAST_2",
				mode:       requireTransitionQuorum,
				revocation: revocation(5),
				recruitedStatuses: []*clustermetadatapb.ConsensusStatus{
					makeStatus(zone1.a, rule, revocation(5)),
					makeStatus(zone1.b, rule, revocation(5)),
					makeStatus(zone1.c, rule, revocation(5)),
				},
				buildProposal: func(r RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error) {
					return &consensusdatapb.CoordinatorProposal{
						TermRevocation: r.TermRevocation,
						ProposalLeader: &consensusdatapb.ProposalLeader{Id: zone1.a},
						ProposedRule:   makeRule(ruleNum(5, 0), atLeast(2), zone1.a),
					}, nil
				},
				wantErr: "proposal validation: recruited proposed cohort cannot achieve durability: durability not achievable: proposed cohort has 1 poolers, required 2",
			}
		}(),
		func() tc {
			zone1 := poolerIDs.zone1
			cohort := zone1.all[:3]
			rule := makeRule(ruleNum(3, 0), atLeast(2), cohort...)
			return tc{
				name:       "extra non-cohort node does not inflate outgoing quorum count",
				mode:       requireTransitionQuorum,
				revocation: revocation(5),
				recruitedStatuses: []*clustermetadatapb.ConsensusStatus{
					makeStatus(zone1.a, rule, revocation(5)),
					makeStatus(makeID("zone1", "extra-node"), rule, revocation(5)),
				},
				buildProposal: proposeFirstEligible(makeRule(ruleNum(5, 0), atLeast(2), cohort...)),
				wantErr:       "insufficient outgoing cohort recruitment: majority not satisfied: recruited 1 of 3 cohort poolers, need at least 2",
			}
		}(),
		func() tc {
			zone1 := poolerIDs.zone1
			cohort := zone1.all[:3]
			rule := makeRule(ruleNum(3, 0), atLeast(2), cohort...)
			return tc{
				name:       "LSN tiebreaker: highest LSN is sole eligible leader",
				mode:       requireTransitionQuorum,
				revocation: revocation(5),
				recruitedStatuses: []*clustermetadatapb.ConsensusStatus{
					makeStatusWithLSN(zone1.a, rule, revocation(5), "0/3000000"), // highest
					makeStatusWithLSN(zone1.b, rule, revocation(5), "0/2000000"),
					makeStatusWithLSN(zone1.c, rule, revocation(5), "0/1000000"),
				},
				buildProposal: func(r RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error) {
					return &consensusdatapb.CoordinatorProposal{
						TermRevocation: r.TermRevocation,
						ProposalLeader: &consensusdatapb.ProposalLeader{Id: r.EligibleLeaders[0].GetId()},
						ProposedRule:   makeRule(ruleNum(5, 0), atLeast(2), cohort...),
					}, nil
				},
				wantLeader: "pooler-a",
				checkResult: func(t *testing.T, r RecruitmentResult) {
					require.Len(t, r.EligibleLeaders, 1)
					assert.Equal(t, "pooler-a", r.EligibleLeaders[0].GetId().GetName())
				},
			}
		}(),
		func() tc {
			zone1 := poolerIDs.zone1
			cohort := zone1.all[:3]
			rule := makeRule(ruleNum(3, 0), atLeast(2), cohort...)
			return tc{
				name:       "LSN tie at max: two nodes tied at highest LSN both eligible",
				mode:       requireTransitionQuorum,
				revocation: revocation(5),
				recruitedStatuses: []*clustermetadatapb.ConsensusStatus{
					makeStatusWithLSN(zone1.a, rule, revocation(5), "0/3000000"),
					makeStatusWithLSN(zone1.b, rule, revocation(5), "0/3000000"), // tied with a
					makeStatusWithLSN(zone1.c, rule, revocation(5), "0/1000000"),
				},
				buildProposal: func(r RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error) {
					return &consensusdatapb.CoordinatorProposal{
						TermRevocation: r.TermRevocation,
						ProposalLeader: &consensusdatapb.ProposalLeader{Id: r.EligibleLeaders[0].GetId()},
						ProposedRule:   makeRule(ruleNum(5, 0), atLeast(2), cohort...),
					}, nil
				},
				wantLeader: "pooler-a",
				checkResult: func(t *testing.T, r RecruitmentResult) {
					require.Len(t, r.EligibleLeaders, 2)
					names := []string{r.EligibleLeaders[0].GetId().GetName(), r.EligibleLeaders[1].GetId().GetName()}
					assert.ElementsMatch(t, []string{"pooler-a", "pooler-b"}, names)
				},
			}
		}(),
		func() tc {
			// a at newRule/high LSN, b at newRule/low LSN, c at oldRule — only a eligible
			zone1 := poolerIDs.zone1
			cohort := zone1.all[:3]
			rule := makeRule(ruleNum(3, 0), atLeast(2), cohort...)
			return tc{
				name:       "rule takes priority: node at old rule excluded despite higher LSN",
				mode:       requireTransitionQuorum,
				revocation: revocation(5),
				recruitedStatuses: []*clustermetadatapb.ConsensusStatus{
					makeStatusWithLSN(zone1.a, rule, revocation(5), "0/5000000"),
					makeStatusWithLSN(zone1.b, rule, revocation(5), "0/3000000"),
					makeStatusWithLSN(zone1.c, makeRule(ruleNum(2, 0), atLeast(2), cohort...), revocation(5), "0/4000000"),
				},
				buildProposal: func(r RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error) {
					return &consensusdatapb.CoordinatorProposal{
						TermRevocation: r.TermRevocation,
						ProposalLeader: &consensusdatapb.ProposalLeader{Id: r.EligibleLeaders[0].GetId()},
						ProposedRule:   makeRule(ruleNum(5, 0), atLeast(2), cohort...),
					}, nil
				},
				wantLeader: "pooler-a",
				checkResult: func(t *testing.T, r RecruitmentResult) {
					require.Len(t, r.EligibleLeaders, 1)
					assert.Equal(t, "pooler-a", r.EligibleLeaders[0].GetId().GetName())
				},
			}
		}(),
		func() tc {
			zone1 := poolerIDs.zone1
			cohort := zone1.all[:3]
			rule := makeRule(ruleNum(3, 0), atLeast(2), cohort...)
			return tc{
				name:       "duplicate status: same pooler twice counts once toward quorum",
				mode:       requireTransitionQuorum,
				revocation: revocation(5),
				recruitedStatuses: []*clustermetadatapb.ConsensusStatus{
					makeStatus(zone1.a, rule, revocation(5)),
					makeStatus(zone1.a, rule, revocation(5)), // duplicate — must not inflate quorum
				},
				buildProposal: proposeFirstEligible(makeRule(ruleNum(5, 0), atLeast(2), cohort...)),
				wantErr:       "insufficient outgoing cohort recruitment: majority not satisfied: recruited 1 of 3 cohort poolers, need at least 2",
			}
		}(),
		func() tc {
			// a appears twice: stale at oldRule/high LSN, fresh at rule/low LSN.
			// Rule number wins in deduplication; a ends up with the rule entry.
			zone1 := poolerIDs.zone1
			cohort := zone1.all[:3]
			rule := makeRule(ruleNum(3, 0), atLeast(2), cohort...)
			return tc{
				name:       "duplicate: rule number wins over LSN when deduplicating",
				mode:       requireTransitionQuorum,
				revocation: revocation(5),
				recruitedStatuses: []*clustermetadatapb.ConsensusStatus{
					makeStatusWithLSN(zone1.a, makeRule(ruleNum(2, 0), atLeast(2), cohort...), revocation(5), "0/3000000"),
					makeStatusWithLSN(zone1.a, rule, revocation(5), "0/2000000"),
					makeStatusWithLSN(zone1.b, rule, revocation(5), "0/1000000"),
					makeStatusWithLSN(zone1.c, rule, revocation(5), "0/1000000"),
				},
				buildProposal: proposeFirstEligible(makeRule(ruleNum(5, 0), atLeast(2), cohort...)),
				wantLeader:    "pooler-a",
				checkResult: func(t *testing.T, r RecruitmentResult) {
					require.Len(t, r.EligibleLeaders, 1)
					assert.Equal(t, "pooler-a", r.EligibleLeaders[0].GetId().GetName())
				},
			}
		}(),
		func() tc {
			zone1 := poolerIDs.zone1
			cohort := zone1.all[:3]
			rule := makeRule(ruleNum(3, 0), atLeast(2), cohort...)
			return tc{
				name:       "all accepted nodes have empty LSN: invalid WAL position",
				mode:       requireTransitionQuorum,
				revocation: revocation(5),
				recruitedStatuses: []*clustermetadatapb.ConsensusStatus{
					makeStatusWithLSN(zone1.a, rule, revocation(5), ""),
					makeStatusWithLSN(zone1.b, rule, revocation(5), ""),
					makeStatusWithLSN(zone1.c, rule, revocation(5), ""),
				},
				buildProposal: proposeFirstEligible(makeRule(ruleNum(5, 0), atLeast(2), cohort...)),
				wantErr:       "all recruited nodes reported an invalid or missing WAL position",
			}
		}(),
		func() tc {
			zone1 := poolerIDs.zone1
			cohort := zone1.all[:3]
			rule := makeRule(ruleNum(3, 0), atLeast(2), cohort...)
			return tc{
				name:       "all accepted nodes have unparsable LSN: invalid WAL position",
				mode:       requireTransitionQuorum,
				revocation: revocation(5),
				recruitedStatuses: []*clustermetadatapb.ConsensusStatus{
					makeStatusWithLSN(zone1.a, rule, revocation(5), "not-an-lsn"),
					makeStatusWithLSN(zone1.b, rule, revocation(5), "not-an-lsn"),
					makeStatusWithLSN(zone1.c, rule, revocation(5), "not-an-lsn"),
				},
				buildProposal: proposeFirstEligible(makeRule(ruleNum(5, 0), atLeast(2), cohort...)),
				wantErr:       "all recruited nodes reported an invalid or missing WAL position",
			}
		}(),
		func() tc {
			zone1 := poolerIDs.zone1
			cohort := zone1.all[:3]
			rule := makeRule(ruleNum(3, 0), atLeast(2), cohort...)
			return tc{
				name:       "node with invalid LSN excluded: remaining 2 satisfy AT_LEAST_2",
				mode:       requireTransitionQuorum,
				revocation: revocation(5),
				recruitedStatuses: []*clustermetadatapb.ConsensusStatus{
					makeStatusWithLSN(zone1.a, rule, revocation(5), "0/3000000"),
					makeStatusWithLSN(zone1.b, rule, revocation(5), "0/2000000"),
					makeStatusWithLSN(zone1.c, rule, revocation(5), ""),
				},
				buildProposal: proposeFirstEligible(makeRule(ruleNum(5, 0), atLeast(2), cohort...)),
				wantLeader:    "pooler-a",
			}
		}(),
		func() tc {
			zone1 := poolerIDs.zone1
			cohort := zone1.all[:3]
			rule := makeRule(ruleNum(3, 0), atLeast(2), cohort...)
			return tc{
				name:       "invalid LSN causes quorum failure: 1 valid of 3",
				mode:       requireTransitionQuorum,
				revocation: revocation(5),
				recruitedStatuses: []*clustermetadatapb.ConsensusStatus{
					makeStatusWithLSN(zone1.a, rule, revocation(5), "0/3000000"),
					makeStatusWithLSN(zone1.b, rule, revocation(5), ""),
					makeStatusWithLSN(zone1.c, rule, revocation(5), "not-an-lsn"),
				},
				buildProposal: proposeFirstEligible(makeRule(ruleNum(5, 0), atLeast(2), cohort...)),
				wantErr:       "insufficient outgoing cohort recruitment: majority not satisfied: recruited 1 of 3 cohort poolers, need at least 2",
			}
		}(),
		func() tc {
			zone1 := poolerIDs.zone1
			cohort := zone1.all[:3]
			rule := makeRule(ruleNum(3, 0), atLeast(2), cohort...)
			return tc{
				name:       "node with invalid LSN cannot be proposed as leader",
				mode:       requireTransitionQuorum,
				revocation: revocation(5),
				recruitedStatuses: []*clustermetadatapb.ConsensusStatus{
					makeStatusWithLSN(zone1.a, rule, revocation(5), "0/3000000"),
					makeStatusWithLSN(zone1.b, rule, revocation(5), "0/2000000"),
					makeStatusWithLSN(zone1.c, rule, revocation(5), ""),
				},
				buildProposal: func(r RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error) {
					return &consensusdatapb.CoordinatorProposal{
						TermRevocation: r.TermRevocation,
						ProposalLeader: &consensusdatapb.ProposalLeader{Id: zone1.c},
						ProposedRule:   makeRule(ruleNum(5, 0), atLeast(2), cohort...),
					}, nil
				},
				wantErr: "proposal validation: proposed leader zone1_pooler-c is not among eligible leaders",
			}
		}(),
		func() tc {
			// 5-node cohort [a,b,c,d,e]; only d and e respond. AT_LEAST_2 on 5
			// nodes needs majority=3 (and revocation requires 4). 2 < 3 → fails.
			zone1 := poolerIDs.zone1
			newCohort := zone1.all[:5]
			rule := makeRule(ruleNum(4, 0), atLeast(2), newCohort...)
			return tc{
				name:       "cohort expansion: only 2 of 5 new-cohort members respond",
				mode:       requireTransitionQuorum,
				revocation: revocation(5),
				recruitedStatuses: []*clustermetadatapb.ConsensusStatus{
					makeStatusWithLSN(zone1.d, rule, revocation(5), "0/4000000"),
					makeStatusWithLSN(zone1.e, rule, revocation(5), "0/4000000"),
				},
				buildProposal: func(r RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error) {
					return &consensusdatapb.CoordinatorProposal{
						TermRevocation: r.TermRevocation,
						ProposalLeader: &consensusdatapb.ProposalLeader{Id: zone1.d},
						ProposedRule:   makeRule(ruleNum(5, 0), atLeast(2), zone1.d, zone1.e),
					}, nil
				},
				wantErr: "insufficient outgoing cohort recruitment: majority not satisfied: recruited 2 of 5 cohort poolers, need at least 3",
			}
		}(),
		func() tc {
			zone1 := poolerIDs.zone1
			cohort := zone1.all[:3]
			unknownRule := &clustermetadatapb.ShardRule{
				RuleNumber:       &clustermetadatapb.RuleNumber{CoordinatorTerm: 3},
				CohortMembers:    cohort,
				DurabilityPolicy: &clustermetadatapb.DurabilityPolicy{QuorumType: clustermetadatapb.QuorumType_QUORUM_TYPE_UNKNOWN},
			}
			return tc{
				name:       "outgoing rule has unknown quorum type: failed to parse",
				mode:       requireTransitionQuorum,
				revocation: revocation(5),
				recruitedStatuses: []*clustermetadatapb.ConsensusStatus{
					makeStatus(zone1.a, unknownRule, revocation(5)),
					makeStatus(zone1.b, unknownRule, revocation(5)),
					makeStatus(zone1.c, unknownRule, revocation(5)),
				},
				buildProposal: proposeFirstEligible(makeRule(ruleNum(5, 0), atLeast(2), cohort...)),
				wantErr:       "failed to parse durability policy from rule: unsupported quorum type: QUORUM_TYPE_UNKNOWN",
			}
		}(),
		func() tc {
			zone1 := poolerIDs.zone1
			cohort := zone1.all[:3]
			return tc{
				name:       "outgoing mode, no recorded rule among recruited nodes",
				mode:       requireTransitionQuorum,
				revocation: revocation(5),
				recruitedStatuses: []*clustermetadatapb.ConsensusStatus{
					makeStatusWithLSN(zone1.a, nil, revocation(5), "0/2000000"),
					makeStatusWithLSN(zone1.b, nil, revocation(5), "0/1000000"),
				},
				buildProposal: proposeFirstEligible(makeRule(ruleNum(5, 0), atLeast(2), cohort...)),
				wantErr:       "no recorded rule found among recruited nodes; cannot determine cohort for quorum check",
			}
		}(),
		func() tc {
			zone1 := poolerIDs.zone1
			cohort := zone1.all[:3]
			rule := makeRule(ruleNum(3, 0), atLeast(2), cohort...)
			return tc{
				name:       "validate proposal: mismatched term revocation",
				mode:       requireTransitionQuorum,
				revocation: revocation(5),
				recruitedStatuses: []*clustermetadatapb.ConsensusStatus{
					makeStatus(zone1.a, rule, revocation(5)),
					makeStatus(zone1.b, rule, revocation(5)),
					makeStatus(zone1.c, rule, revocation(5)),
				},
				buildProposal: func(r RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error) {
					return &consensusdatapb.CoordinatorProposal{
						TermRevocation: revocation(99),
						ProposalLeader: &consensusdatapb.ProposalLeader{Id: zone1.a},
						ProposedRule:   rule,
					}, nil
				},
				wantErr: "proposal validation: proposal term revocation does not match the recruitment revocation",
			}
		}(),
		func() tc {
			zone1 := poolerIDs.zone1
			cohort := zone1.all[:3]
			rule := makeRule(ruleNum(3, 0), atLeast(2), cohort...)
			return tc{
				name:       "validate proposal: nil leader ID",
				mode:       requireTransitionQuorum,
				revocation: revocation(5),
				recruitedStatuses: []*clustermetadatapb.ConsensusStatus{
					makeStatus(zone1.a, rule, revocation(5)),
					makeStatus(zone1.b, rule, revocation(5)),
					makeStatus(zone1.c, rule, revocation(5)),
				},
				buildProposal: func(r RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error) {
					return &consensusdatapb.CoordinatorProposal{
						TermRevocation: r.TermRevocation,
						ProposalLeader: &consensusdatapb.ProposalLeader{Id: nil},
						ProposedRule:   rule,
					}, nil
				},
				wantErr: "proposal validation: proposal has no leader ID",
			}
		}(),
		func() tc {
			zone1 := poolerIDs.zone1
			cohort := zone1.all[:3]
			rule := makeRule(ruleNum(3, 0), atLeast(2), cohort...)
			return tc{
				name:       "validate proposal: nil proposed rule",
				mode:       requireTransitionQuorum,
				revocation: revocation(5),
				recruitedStatuses: []*clustermetadatapb.ConsensusStatus{
					makeStatus(zone1.a, rule, revocation(5)),
					makeStatus(zone1.b, rule, revocation(5)),
					makeStatus(zone1.c, rule, revocation(5)),
				},
				buildProposal: func(r RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error) {
					return &consensusdatapb.CoordinatorProposal{
						TermRevocation: r.TermRevocation,
						ProposalLeader: &consensusdatapb.ProposalLeader{Id: zone1.a},
						ProposedRule:   nil,
					}, nil
				},
				wantErr: "proposal validation: no proposed rule",
			}
		}(),
		func() tc {
			zone1 := poolerIDs.zone1
			cohort := zone1.all[:3]
			rule := makeRule(ruleNum(3, 0), atLeast(2), cohort...)
			return tc{
				name:       "validate proposal: nil durability policy",
				mode:       requireTransitionQuorum,
				revocation: revocation(5),
				recruitedStatuses: []*clustermetadatapb.ConsensusStatus{
					makeStatus(zone1.a, rule, revocation(5)),
					makeStatus(zone1.b, rule, revocation(5)),
					makeStatus(zone1.c, rule, revocation(5)),
				},
				buildProposal: func(r RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error) {
					return &consensusdatapb.CoordinatorProposal{
						TermRevocation: r.TermRevocation,
						ProposalLeader: &consensusdatapb.ProposalLeader{Id: zone1.a},
						ProposedRule: &clustermetadatapb.ShardRule{
							RuleNumber:    &clustermetadatapb.RuleNumber{CoordinatorTerm: 5},
							LeaderId:      zone1.a,
							CohortMembers: cohort,
						},
					}, nil
				},
				wantErr: "proposal validation: invalid durability policy in proposal: durability policy is nil",
			}
		}(),
		func() tc {
			zone1 := poolerIDs.zone1
			cohort := zone1.all[:3]
			rule := makeRule(ruleNum(3, 0), atLeast(2), cohort...)
			return tc{
				name:       "validate proposal: proposed rule term above recruitment revocation term",
				mode:       requireTransitionQuorum,
				revocation: revocation(5),
				recruitedStatuses: []*clustermetadatapb.ConsensusStatus{
					makeStatus(zone1.a, rule, revocation(5)),
					makeStatus(zone1.b, rule, revocation(5)),
					makeStatus(zone1.c, rule, revocation(5)),
				},
				buildProposal: func(r RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error) {
					return &consensusdatapb.CoordinatorProposal{
						TermRevocation: r.TermRevocation,
						ProposalLeader: &consensusdatapb.ProposalLeader{Id: zone1.a},
						ProposedRule:   makeRule(ruleNum(99, 0), atLeast(2), cohort...),
					}, nil
				},
				wantErr: "proposal validation: proposed rule term 99 is above the recruitment revocation term 5",
			}
		}(),
		// ── incoming cohort mode (bootstrap) ─────────────────────────────────────
		func() tc {
			zone1 := poolerIDs.zone1
			cohort := zone1.all[:3]
			return tc{
				name:       "bootstrap: nil outgoing rule allowed, highest LSN leads",
				mode:       onlyRequireIncomingQuorum,
				revocation: revocation(5),
				recruitedStatuses: []*clustermetadatapb.ConsensusStatus{
					makeStatusWithLSN(zone1.a, nil, revocation(5), "0/3000000"),
					makeStatusWithLSN(zone1.b, nil, revocation(5), "0/2000000"),
					makeStatusWithLSN(zone1.c, nil, revocation(5), "0/1000000"),
				},
				buildProposal: func(r RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error) {
					leader := r.EligibleLeaders[0]
					return &consensusdatapb.CoordinatorProposal{
						TermRevocation: r.TermRevocation,
						ProposalLeader: &consensusdatapb.ProposalLeader{Id: leader.GetId()},
						ProposedRule:   makeRule(ruleNum(5, 0), atLeast(2), cohort...),
					}, nil
				},
				wantLeader: "pooler-a",
			}
		}(),
		func() tc {
			zone1 := poolerIDs.zone1
			cohort := zone1.all[:3]
			return tc{
				name:       "bootstrap: 1 of 3 recruited — cannot achieve durability",
				mode:       onlyRequireIncomingQuorum,
				revocation: revocation(5),
				recruitedStatuses: []*clustermetadatapb.ConsensusStatus{
					makeStatusWithLSN(zone1.a, nil, revocation(5), "0/1000000"),
				},
				buildProposal: func(r RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error) {
					leader := r.EligibleLeaders[0]
					return &consensusdatapb.CoordinatorProposal{
						TermRevocation: r.TermRevocation,
						ProposalLeader: &consensusdatapb.ProposalLeader{Id: leader.GetId()},
						ProposedRule:   makeRule(ruleNum(5, 0), atLeast(2), cohort...),
					}, nil
				},
				wantErr: "proposal validation: recruited proposed cohort cannot achieve durability: durability not achievable: proposed cohort has 1 poolers, required 2",
			}
		}(),
		func() tc {
			zone1 := poolerIDs.zone1
			cohort := zone1.all[:3]
			return tc{
				name:       "bootstrap: unknown quorum type in proposed rule",
				mode:       onlyRequireIncomingQuorum,
				revocation: revocation(5),
				recruitedStatuses: []*clustermetadatapb.ConsensusStatus{
					makeStatusWithLSN(zone1.a, nil, revocation(5), "0/1000000"),
					makeStatusWithLSN(zone1.b, nil, revocation(5), "0/1000000"),
					makeStatusWithLSN(zone1.c, nil, revocation(5), "0/1000000"),
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
							LeaderId:         leader.GetId(),
						},
					}, nil
				},
				wantErr: "proposal validation: invalid durability policy in proposal: unsupported quorum type: QUORUM_TYPE_UNKNOWN",
			}
		}(),
		func() tc {
			// 5-node proposed cohort [a,b,d,e,f], only a+b recruited. AT_LEAST_2
			// achievability passes (2≥N=2) but majority of 5 requires 3 → fails.
			zone1 := poolerIDs.zone1
			bigCohort := []*clustermetadatapb.ID{zone1.a, zone1.b, zone1.d, zone1.e, zone1.f}
			return tc{
				name:       "bootstrap: proposed cohort achievable but insufficient majority",
				mode:       onlyRequireIncomingQuorum,
				revocation: revocation(5),
				recruitedStatuses: []*clustermetadatapb.ConsensusStatus{
					makeStatusWithLSN(zone1.a, nil, revocation(5), "0/3000000"),
					makeStatusWithLSN(zone1.b, nil, revocation(5), "0/2000000"),
				},
				buildProposal: func(r RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error) {
					leader := r.EligibleLeaders[0]
					return &consensusdatapb.CoordinatorProposal{
						TermRevocation: r.TermRevocation,
						ProposalLeader: &consensusdatapb.ProposalLeader{Id: leader.GetId()},
						ProposedRule:   makeRule(ruleNum(5, 0), atLeast(2), bigCohort...),
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
			cohort := []*clustermetadatapb.ID{poolerIDs.zone1.a, poolerIDs.zone2.b, poolerIDs.zone2.c}
			rev := revocation(7)
			multiCellRule := makeRule(ruleNum(6, 0), multiCell(2), cohort...)
			atLeastNRule := makeRule(ruleNum(5, 0), atLeast(2), cohort...)
			return tc{
				name:       "stuck rule change: coordinator re-proposes outgoing rule below revocation term",
				mode:       requireTransitionQuorum,
				revocation: rev,
				recruitedStatuses: []*clustermetadatapb.ConsensusStatus{
					makeStatus(poolerIDs.zone1.a, multiCellRule, rev),
					makeStatus(poolerIDs.zone2.b, atLeastNRule, rev),
					makeStatus(poolerIDs.zone2.c, atLeastNRule, rev),
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
			require.NotNil(t, tt.revocation, "test case must set revocation explicitly")
			rev := tt.revocation
			var gotResult RecruitmentResult
			proposal, err := buildProposalCore(rev, tt.recruitedStatuses, tt.mode, nil,
				func(r RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error) {
					gotResult = r
					return bp(r)
				})
			if tt.wantErr != "" {
				assert.EqualError(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
				assert.NotEmpty(t, tt.wantLeader)
				assert.Equal(t, tt.wantLeader, proposal.GetProposalLeader().GetId().GetName())
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
	zone1 := poolerIDs.zone1
	cohort := []*clustermetadatapb.ID{zone1.a, zone1.b, zone1.c}
	rule := makeRule(ruleNum(3, 0), atLeast(2), cohort...)

	coordID := makeID("zone1", "multiorch-1")
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
				makeStatus(zone1.a, rule, revocation(3)),
				makeStatus(zone1.b, rule, revocation(3)),
				makeStatus(zone1.c, rule, revocation(3)),
			},
			wantErr: "no nodes accepted the requested term revocation",
		},
		{
			name: "partial acceptance, quorum met: 2 of 3 accept exact revocation",
			statuses: []*clustermetadatapb.ConsensusStatus{
				makeStatus(zone1.a, rule, rev),
				makeStatus(zone1.b, rule, rev),
				makeStatus(zone1.c, rule, revocation(3)),
			},
		},
		{
			name: "partial acceptance, quorum not met: only 1 of 3 accepts",
			statuses: []*clustermetadatapb.ConsensusStatus{
				makeStatus(zone1.a, rule, rev),
				makeStatus(zone1.b, rule, revocation(3)),
				makeStatus(zone1.c, rule, revocation(3)),
			},
			wantErr: "insufficient outgoing cohort recruitment: majority not satisfied: recruited 1 of 3 cohort poolers, need at least 2",
		},
		{
			name: "lower-term revocation does not count",
			statuses: []*clustermetadatapb.ConsensusStatus{
				makeStatus(zone1.a, rule, &clustermetadatapb.TermRevocation{RevokedBelowTerm: 3, AcceptedCoordinatorId: coordID}),
				makeStatus(zone1.b, rule, &clustermetadatapb.TermRevocation{RevokedBelowTerm: 3, AcceptedCoordinatorId: coordID}),
				makeStatus(zone1.c, rule, &clustermetadatapb.TermRevocation{RevokedBelowTerm: 3, AcceptedCoordinatorId: coordID}),
			},
			wantErr: "no nodes accepted the requested term revocation",
		},
		{
			name: "higher-term revocation does not count",
			statuses: []*clustermetadatapb.ConsensusStatus{
				makeStatus(zone1.a, rule, &clustermetadatapb.TermRevocation{RevokedBelowTerm: 9, AcceptedCoordinatorId: coordID}),
				makeStatus(zone1.b, rule, &clustermetadatapb.TermRevocation{RevokedBelowTerm: 9, AcceptedCoordinatorId: coordID}),
				makeStatus(zone1.c, rule, &clustermetadatapb.TermRevocation{RevokedBelowTerm: 9, AcceptedCoordinatorId: coordID}),
			},
			wantErr: "no nodes accepted the requested term revocation",
		},
		{
			name: "rival coordinator ID does not count",
			statuses: []*clustermetadatapb.ConsensusStatus{
				makeStatus(zone1.a, rule, &clustermetadatapb.TermRevocation{RevokedBelowTerm: 5, AcceptedCoordinatorId: makeID("zone1", "multiorch-2"), CoordinatorInitiatedAt: &timestamppb.Timestamp{Seconds: 1000}}),
				makeStatus(zone1.b, rule, &clustermetadatapb.TermRevocation{RevokedBelowTerm: 5, AcceptedCoordinatorId: makeID("zone1", "multiorch-2"), CoordinatorInitiatedAt: &timestamppb.Timestamp{Seconds: 1000}}),
				makeStatus(zone1.c, rule, &clustermetadatapb.TermRevocation{RevokedBelowTerm: 5, AcceptedCoordinatorId: makeID("zone1", "multiorch-2"), CoordinatorInitiatedAt: &timestamppb.Timestamp{Seconds: 1000}}),
			},
			wantErr: "no nodes accepted the requested term revocation",
		},
		{
			name: "stale initiated_at does not count",
			statuses: []*clustermetadatapb.ConsensusStatus{
				makeStatus(zone1.a, rule, &clustermetadatapb.TermRevocation{RevokedBelowTerm: 5, AcceptedCoordinatorId: coordID, CoordinatorInitiatedAt: &timestamppb.Timestamp{Seconds: 999}}),
				makeStatus(zone1.b, rule, &clustermetadatapb.TermRevocation{RevokedBelowTerm: 5, AcceptedCoordinatorId: coordID, CoordinatorInitiatedAt: &timestamppb.Timestamp{Seconds: 999}}),
				makeStatus(zone1.c, rule, &clustermetadatapb.TermRevocation{RevokedBelowTerm: 5, AcceptedCoordinatorId: coordID, CoordinatorInitiatedAt: &timestamppb.Timestamp{Seconds: 999}}),
			},
			wantErr: "no nodes accepted the requested term revocation",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := BuildSafeProposal(rev, tt.statuses, proposeFirstEligible(makeRule(ruleNum(5, 0), atLeast(2), cohort...)))
			if tt.wantErr != "" {
				assert.EqualError(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestBuildSafeProposal_NoCommittedRule(t *testing.T) {
	zone1 := poolerIDs.zone1
	statuses := []*clustermetadatapb.ConsensusStatus{
		{Id: zone1.a, TermRevocation: revocation(5)}, // no current_position
	}

	// The proposal callback is never reached (filterByValidPosition fails first),
	// so the rule passed here is irrelevant beyond satisfying makeRule's invariants.
	_, err := BuildSafeProposal(revocation(5), statuses, proposeFirstEligible(makeRule(ruleNum(0, 0), atLeast(2), zone1.a)))

	// A node with no current_position has no parseable LSN, so it is filtered
	// out by filterByValidPosition before the recorded-rule check fires.
	require.EqualError(t, err, "all recruited nodes reported an invalid or missing WAL position")
}

func TestBuildSafeProposal_InsufficientQuorum(t *testing.T) {
	zone1 := poolerIDs.zone1
	cohort := []*clustermetadatapb.ID{zone1.a, zone1.b, zone1.c}
	rule := makeRule(ruleNum(3, 0), atLeast(2), cohort...)

	// Only one of three nodes recruited — not enough for AT_LEAST_2 with majority.
	statuses := []*clustermetadatapb.ConsensusStatus{
		makeStatus(zone1.a, rule, revocation(5)),
	}

	_, err := BuildSafeProposal(revocation(5), statuses, proposeFirstEligible(makeRule(ruleNum(3, 0), atLeast(2), cohort...)))

	require.EqualError(t, err, "insufficient outgoing cohort recruitment: majority not satisfied: recruited 1 of 3 cohort poolers, need at least 2")
}

func TestBuildSafeProposal_InvalidLeader(t *testing.T) {
	zone1 := poolerIDs.zone1
	outsider := makeID("zone1", "outsider")
	cohort := []*clustermetadatapb.ID{zone1.a, zone1.b, zone1.c}
	rule := makeRule(ruleNum(3, 0), atLeast(2), cohort...)

	statuses := []*clustermetadatapb.ConsensusStatus{
		makeStatus(zone1.a, rule, revocation(5)),
		makeStatus(zone1.b, rule, revocation(5)),
		makeStatus(zone1.c, rule, revocation(5)),
	}

	// Callback proposes a node that was not recruited.
	buildProposal := func(r RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error) {
		return &consensusdatapb.CoordinatorProposal{
			TermRevocation: r.TermRevocation,
			ProposalLeader: &consensusdatapb.ProposalLeader{Id: outsider},
			ProposedRule:   rule,
		}, nil
	}

	_, err := BuildSafeProposal(revocation(5), statuses, buildProposal)

	require.EqualError(t, err, "proposal validation: proposed leader zone1_outsider is not among eligible leaders")
}

// TestCheckSufficientRecruitment_UnrecruitedCohortMemberOK verifies that not all
// proposed cohort members need to be recruited, as long as the recruited subset
// satisfies the durability policy. Here outsider was not recruited, but a and b
// (2 nodes) cover AT_LEAST_2, so the proposal is accepted.
func TestBuildSafeProposal_UnrecruitedCohortMemberOK(t *testing.T) {
	zone1 := poolerIDs.zone1
	outsider := makeID("zone1", "outsider")
	cohort := []*clustermetadatapb.ID{zone1.a, zone1.b, zone1.c}
	rule := makeRule(ruleNum(3, 0), atLeast(2), cohort...)

	statuses := []*clustermetadatapb.ConsensusStatus{
		makeStatus(zone1.a, rule, revocation(5)),
		makeStatus(zone1.b, rule, revocation(5)),
		makeStatus(zone1.c, rule, revocation(5)),
	}

	proposedRule := makeRule(ruleNum(5, 0), atLeast(2), zone1.a, zone1.b, outsider)
	buildProposal := func(r RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error) {
		return &consensusdatapb.CoordinatorProposal{
			TermRevocation: r.TermRevocation,
			ProposalLeader: &consensusdatapb.ProposalLeader{Id: zone1.a},
			ProposedRule:   proposedRule,
		}, nil
	}

	_, err := BuildSafeProposal(revocation(5), statuses, buildProposal)

	require.NoError(t, err)
}

// TestCheckSufficientRecruitment_DeadLeaderRemainsInCohort verifies that a failover
// can succeed when the dead leader is kept in the new cohort (so it can rejoin as a
// standby later) but cannot be recruited. B and C are live and cover AT_LEAST_2.
func TestBuildSafeProposal_DeadLeaderRemainsInCohort(t *testing.T) {
	zone1 := poolerIDs.zone1
	// zone1.a is the dead leader — not recruited.
	cohort := []*clustermetadatapb.ID{zone1.a, zone1.b, zone1.c}
	rule := makeRule(ruleNum(3, 0), atLeast(2), cohort...)

	// Only B and C are reachable; A is dead.
	statuses := []*clustermetadatapb.ConsensusStatus{
		makeStatus(zone1.b, rule, revocation(5)),
		makeStatus(zone1.c, rule, revocation(5)),
	}

	// Proposed rule keeps A in the cohort (it will rejoin as standby) but promotes B.
	proposedRule := makeRule(ruleNum(5, 0), atLeast(2), cohort...)
	buildProposal := func(r RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error) {
		return &consensusdatapb.CoordinatorProposal{
			TermRevocation: r.TermRevocation,
			ProposalLeader: &consensusdatapb.ProposalLeader{Id: zone1.b},
			ProposedRule:   proposedRule,
		}, nil
	}

	_, err := BuildSafeProposal(revocation(5), statuses, buildProposal)

	require.NoError(t, err)
}

// TestCheckSufficientRecruitment_InsufficientRecruitedFromProposedCohort verifies
// that the proposal fails when too few proposed-cohort members were recruited to
// satisfy the durability policy. Here the proposed cohort is [a, d, e] with
// AT_LEAST_2, but only a was recruited from it — the new leader could not achieve
// durable writes immediately after promotion.
func TestBuildSafeProposal_InsufficientRecruitedFromProposedCohort(t *testing.T) {
	zone1 := poolerIDs.zone1
	// zone1.d and zone1.e are proposed new members, not recruited.
	cohort := []*clustermetadatapb.ID{zone1.a, zone1.b, zone1.c}
	rule := makeRule(ruleNum(3, 0), atLeast(2), cohort...)

	statuses := []*clustermetadatapb.ConsensusStatus{
		makeStatus(zone1.a, rule, revocation(5)),
		makeStatus(zone1.b, rule, revocation(5)),
		makeStatus(zone1.c, rule, revocation(5)),
	}

	// Proposed rule replaces b and c with d and e, but d and e were not recruited.
	proposedRule := makeRule(ruleNum(5, 0), atLeast(2), zone1.a, zone1.d, zone1.e)
	buildProposal := func(r RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error) {
		return &consensusdatapb.CoordinatorProposal{
			TermRevocation: r.TermRevocation,
			ProposalLeader: &consensusdatapb.ProposalLeader{Id: zone1.a},
			ProposedRule:   proposedRule,
		}, nil
	}

	_, err := BuildSafeProposal(revocation(5), statuses, buildProposal)

	require.EqualError(t, err, "proposal validation: recruited proposed cohort cannot achieve durability: durability not achievable: proposed cohort has 1 poolers, required 2")
}

func TestBuildSafeProposal_BuildProposalError(t *testing.T) {
	zone1 := poolerIDs.zone1
	cohort := []*clustermetadatapb.ID{zone1.a, zone1.b, zone1.c}
	rule := makeRule(ruleNum(3, 0), atLeast(2), cohort...)

	statuses := []*clustermetadatapb.ConsensusStatus{
		makeStatus(zone1.a, rule, revocation(5)),
		makeStatus(zone1.b, rule, revocation(5)),
		makeStatus(zone1.c, rule, revocation(5)),
	}

	buildProposal := func(r RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error) {
		return nil, errors.New("no suitable candidate")
	}

	_, err := BuildSafeProposal(revocation(5), statuses, buildProposal)

	require.EqualError(t, err, "buildProposal: no suitable candidate")
}

func TestBuildSafeProposal_OutgoingRuleSelected(t *testing.T) {
	// One node is behind; the others are at the higher rule.
	// The higher rule's cohort and policy must govern quorum.
	zone1 := poolerIDs.zone1
	cohort := []*clustermetadatapb.ID{zone1.a, zone1.b, zone1.c}
	oldRule := makeRule(ruleNum(2, 0), atLeast(2), cohort...)
	newRule := makeRule(ruleNum(3, 0), atLeast(2), cohort...)

	statuses := []*clustermetadatapb.ConsensusStatus{
		makeStatus(zone1.a, newRule, revocation(5)),
		makeStatus(zone1.b, newRule, revocation(5)),
		makeStatus(zone1.c, oldRule, revocation(5)), // behind
	}

	// Only a and b are eligible (at outgoingRule); callback picks a.
	// proposedRule uses the revocation term (5) since validateProposal requires it to match.
	proposedRule := makeRule(ruleNum(5, 0), atLeast(2), cohort...)
	var gotResult RecruitmentResult
	buildProposal := func(r RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error) {
		gotResult = r
		return &consensusdatapb.CoordinatorProposal{
			TermRevocation: r.TermRevocation,
			ProposalLeader: &consensusdatapb.ProposalLeader{Id: r.EligibleLeaders[0].GetId()},
			ProposedRule:   proposedRule,
		}, nil
	}

	proposal, err := BuildSafeProposal(revocation(5), statuses, buildProposal)

	require.NoError(t, err)
	require.NotNil(t, proposal)
	assert.Equal(t, int64(3), gotResult.OutgoingRule.GetRuleNumber().GetCoordinatorTerm())
	assert.Len(t, gotResult.EligibleLeaders, 2, "only nodes at outgoingRule are eligible")
	// Leader must be a or b (both at newRule), not c.
	leaderName := proposal.GetProposalLeader().GetId().GetName()
	assert.NotEqual(t, "pooler-c", leaderName)
	assert.Equal(t, topoclient.ClusterIDString(gotResult.EligibleLeaders[0].GetId()), topoclient.ClusterIDString(proposal.GetProposalLeader().GetId()))
}

func TestBuildSafeProposal_BuildProposalNil(t *testing.T) {
	zone1 := poolerIDs.zone1
	cohort := []*clustermetadatapb.ID{zone1.a, zone1.b, zone1.c}
	rule := makeRule(ruleNum(3, 0), atLeast(2), cohort...)

	statuses := []*clustermetadatapb.ConsensusStatus{
		makeStatus(zone1.a, rule, revocation(5)),
		makeStatus(zone1.b, rule, revocation(5)),
		makeStatus(zone1.c, rule, revocation(5)),
	}

	_, err := BuildSafeProposal(revocation(5), statuses, func(r RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error) {
		return nil, nil
	})

	require.EqualError(t, err, "buildProposal returned nil proposal")
}

func TestBuildSafeProposal_ProposedPolicyNotAchievable(t *testing.T) {
	zone1 := poolerIDs.zone1
	cohort := []*clustermetadatapb.ID{zone1.a, zone1.b, zone1.c}
	rule := makeRule(ruleNum(3, 0), atLeast(2), cohort...)

	statuses := []*clustermetadatapb.ConsensusStatus{
		makeStatus(zone1.a, rule, revocation(5)),
		makeStatus(zone1.b, rule, revocation(5)),
		makeStatus(zone1.c, rule, revocation(5)),
	}

	// Proposed rule has AT_LEAST_2 but only one cohort member — not achievable.
	tinyRule := &clustermetadatapb.ShardRule{
		RuleNumber:       &clustermetadatapb.RuleNumber{CoordinatorTerm: 5},
		CohortMembers:    []*clustermetadatapb.ID{zone1.a},
		DurabilityPolicy: topoclient.AtLeastN(2),
	}
	buildProposal := func(r RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error) {
		return &consensusdatapb.CoordinatorProposal{
			TermRevocation: r.TermRevocation,
			ProposalLeader: &consensusdatapb.ProposalLeader{Id: zone1.a},
			ProposedRule:   tinyRule,
		}, nil
	}

	_, err := BuildSafeProposal(revocation(5), statuses, buildProposal)

	require.EqualError(t, err, "proposal validation: recruited proposed cohort cannot achieve durability: durability not achievable: proposed cohort has 1 poolers, required 2")
}

func TestBuildSafeProposal_DuplicateStatusIgnoredForQuorum(t *testing.T) {
	// The same pooler appears twice in the statuses (e.g. two RPC responses
	// for the same node). It must count only once toward quorum.
	zone1 := poolerIDs.zone1
	cohort := []*clustermetadatapb.ID{zone1.a, zone1.b, zone1.c}
	rule := makeRule(ruleNum(3, 0), atLeast(2), cohort...)

	// Only a responds, but we see its response twice — still only 1 recruited.
	statuses := []*clustermetadatapb.ConsensusStatus{
		makeStatus(zone1.a, rule, revocation(5)),
		makeStatus(zone1.a, rule, revocation(5)), // duplicate
	}

	_, err := BuildSafeProposal(revocation(5), statuses, proposeFirstEligible(makeRule(ruleNum(3, 0), atLeast(2), cohort...)))

	require.EqualError(t, err, "insufficient outgoing cohort recruitment: majority not satisfied: recruited 1 of 3 cohort poolers, need at least 2")
}

func TestBuildSafeProposal_DuplicateBestPositionKept(t *testing.T) {
	// The same pooler appears twice with different positions (e.g. a retry
	// returned a fresher snapshot). The entry with the higher position must win,
	// regardless of which appeared first in the input.
	//
	// Specifically this tests that rule number takes precedence: a's stale
	// response has a higher LSN but an older rule, so the fresh lower-LSN
	// response at the newer rule must win.
	zone1 := poolerIDs.zone1
	cohort := []*clustermetadatapb.ID{zone1.a, zone1.b, zone1.c}
	oldRule := makeRule(ruleNum(2, 0), atLeast(2), cohort...)
	newRule := makeRule(ruleNum(3, 0), atLeast(2), cohort...)

	// a appears twice: stale at oldRule with a high LSN, fresh at newRule with
	// a lower LSN. Rule number wins, so the newRule entry must be kept.
	// Result: a ends up as the sole eligible leader (highest LSN at newRule).
	statuses := []*clustermetadatapb.ConsensusStatus{
		makeStatusWithLSN(zone1.a, oldRule, revocation(5), "0/3000000"), // stale, high LSN
		makeStatusWithLSN(zone1.a, newRule, revocation(5), "0/2000000"), // fresh, lower LSN
		makeStatusWithLSN(zone1.b, newRule, revocation(5), "0/1000000"),
		makeStatusWithLSN(zone1.c, newRule, revocation(5), "0/1000000"),
	}

	// proposedRule uses the revocation term (5) since validateProposal requires it to match.
	proposedRule := makeRule(ruleNum(5, 0), atLeast(2), cohort...)
	var gotResult RecruitmentResult
	_, err := BuildSafeProposal(revocation(5), statuses, func(r RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error) {
		gotResult = r
		return &consensusdatapb.CoordinatorProposal{
			TermRevocation: r.TermRevocation,
			ProposalLeader: &consensusdatapb.ProposalLeader{Id: r.EligibleLeaders[0].GetId()},
			ProposedRule:   proposedRule,
		}, nil
	})

	require.NoError(t, err)
	// a is at newRule after deduplication and has the highest LSN among newRule
	// nodes, so it is the sole eligible leader.
	require.Len(t, gotResult.EligibleLeaders, 1)
	assert.Equal(t, "pooler-a", gotResult.EligibleLeaders[0].GetId().GetName())
}

func TestBuildProposalCore_EligibleLeadersOrderDeterministic(t *testing.T) {
	// EligibleLeaders must be in the same order regardless of which order
	// statuses arrive. Coordinators that pick by index need a stable list.
	zone1 := poolerIDs.zone1
	cohort := []*clustermetadatapb.ID{zone1.a, zone1.b, zone1.c}
	rule := makeRule(ruleNum(3, 0), atLeast(2), cohort...)

	forward := []*clustermetadatapb.ConsensusStatus{
		makeStatus(zone1.a, rule, revocation(5)),
		makeStatus(zone1.b, rule, revocation(5)),
		makeStatus(zone1.c, rule, revocation(5)),
	}
	reversed := []*clustermetadatapb.ConsensusStatus{
		makeStatus(zone1.c, rule, revocation(5)),
		makeStatus(zone1.b, rule, revocation(5)),
		makeStatus(zone1.a, rule, revocation(5)),
	}

	collect := func(statuses []*clustermetadatapb.ConsensusStatus) []string {
		var names []string
		_, err := BuildSafeProposal(revocation(5), statuses, func(r RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error) {
			for _, cs := range r.EligibleLeaders {
				names = append(names, cs.GetId().GetName())
			}
			return &consensusdatapb.CoordinatorProposal{
				TermRevocation: r.TermRevocation,
				ProposalLeader: &consensusdatapb.ProposalLeader{Id: r.EligibleLeaders[0].GetId()},
				ProposedRule:   makeRule(ruleNum(5, 0), atLeast(2), cohort...),
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
	// A is the leader for cohort [A, B, C] (AT_LEAST_2). A coordinator sends a
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
	// two independent leaders — split brain.
	//
	// A correct implementation would reject the new rule as outgoingRule when it has
	// not achieved quorum under the outgoing cohort's policy. We don't yet have
	// enough information from Recruit responses to enforce this. When the TODO is
	// resolved, at least one of the two calls below should return an error.
	zone1 := poolerIDs.zone1
	// zone1.a: old leader, crashed — not recruited.
	// zone1.b, zone1.c: old cohort, respond to coord 1.
	// zone1.d, zone1.e: new cohort, respond to coord 2.
	// zone1.f: new cohort, unreachable.

	oldRule := makeRule(ruleNum(3, 0), atLeast(2), zone1.a, zone1.b, zone1.c)
	newRule := makeRule(ruleNum(4, 0), atLeast(2), zone1.d, zone1.e, zone1.f)

	// Each coordinator has its own TermRevocation (different accepted_coordinator_id).
	// B and C accepted coordinator 1; D and E accepted coordinator 2.
	revocationCoord1 := &clustermetadatapb.TermRevocation{
		RevokedBelowTerm:      6,
		AcceptedCoordinatorId: makeID("zone1", "multiorch-1"),
	}
	revocationCoord2 := &clustermetadatapb.TermRevocation{
		RevokedBelowTerm:      6,
		AcceptedCoordinatorId: makeID("zone1", "multiorch-2"),
	}

	// All four responding nodes' statuses are in the same pool. The revocation
	// embedded in each status records which coordinator that node pledged to.
	allStatuses := []*clustermetadatapb.ConsensusStatus{
		makeStatusWithLSN(zone1.b, oldRule, revocationCoord1, "0/3000000"),
		makeStatusWithLSN(zone1.c, oldRule, revocationCoord1, "0/3000000"),
		makeStatusWithLSN(zone1.d, newRule, revocationCoord2, "0/4000000"),
		makeStatusWithLSN(zone1.e, newRule, revocationCoord2, "0/4000000"),
	}

	// Each coordinator passes the full pool but its own revocation. The filtering
	// step ensures each coordinator only counts nodes that pledged to it.
	proposal1, err1 := BuildSafeProposal(revocationCoord1, allStatuses, func(r RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error) {
		return &consensusdatapb.CoordinatorProposal{
			TermRevocation: r.TermRevocation,
			ProposalLeader: &consensusdatapb.ProposalLeader{Id: zone1.b},
			ProposedRule:   makeRule(ruleNum(6, 0), atLeast(2), zone1.b, zone1.c),
		}, nil
	})
	proposal2, err2 := BuildSafeProposal(revocationCoord2, allStatuses, func(r RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error) {
		return &consensusdatapb.CoordinatorProposal{
			TermRevocation: r.TermRevocation,
			ProposalLeader: &consensusdatapb.ProposalLeader{Id: zone1.d},
			ProposedRule:   makeRule(ruleNum(6, 0), atLeast(2), zone1.d, zone1.e),
		}, nil
	})

	require.NoError(t, err1)
	assert.NotNil(t, proposal1)
	require.NoError(t, err2)
	assert.NotNil(t, proposal2)
}

func TestSameCohort(t *testing.T) {
	a := makeID("zone1", "a")
	b := makeID("zone1", "b")
	c := makeID("zone1", "c")

	assert.True(t, sameCohort([]*clustermetadatapb.ID{a, b}, []*clustermetadatapb.ID{b, a}), "order should not matter")
	assert.False(t, sameCohort([]*clustermetadatapb.ID{a, b}, []*clustermetadatapb.ID{a, c}))
	assert.False(t, sameCohort([]*clustermetadatapb.ID{a, b}, []*clustermetadatapb.ID{a, b, c}))
	assert.True(t, sameCohort(nil, nil))
}

func TestCheckProposalPossible(t *testing.T) {
	a, b, c := makeID("zone1", "a"), makeID("zone1", "b"), makeID("zone1", "c")
	cohort := []*clustermetadatapb.ID{a, b, c}
	rule := makeRule(ruleNum(3, 0), atLeast(2), cohort...)     // coord term 3 < revocation term 5: nodes can accept
	highRule := makeRule(ruleNum(5, 0), atLeast(2), cohort...) // coord term 5 = revocation term: nodes cannot accept

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
			err := CheckProposalPossible(coordRevocation(5), tt.statuses, proposeFirstEligible(makeRule(ruleNum(5, 0), atLeast(2), cohort...)))
			if tt.wantErr != "" {
				assert.EqualError(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestCheckExternallyCertifiedProposalPossible(t *testing.T) {
	a, b, c := makeID("zone1", "a"), makeID("zone1", "b"), makeID("zone1", "c")
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
				LeaderId:         leader.GetId(),
			},
		}, nil
	}

	// neutralCert covers a fresh-bootstrap scenario: term 0 means "no recorded
	// rule has ever existed" and "0/0" means "any reported LSN is acceptable".
	// Both fields are required by certLeaderFilter even when the caller has no
	// real constraint to express.
	neutralCert := &clustermetadatapb.ExternallyCertifiedRevocation{
		TermRevocation:     coordRevocation(5),
		OutgoingRuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 0},
		FrozenLsn:          "0/0",
	}

	t.Run("bootstrap: nodes at term 0 can accept", func(t *testing.T) {
		err := CheckExternallyCertifiedProposalPossible(neutralCert, []*clustermetadatapb.ConsensusStatus{
			makeUnrecruitedStatus(a, nil),
			makeUnrecruitedStatus(b, nil),
			makeUnrecruitedStatus(c, nil),
		}, bootstrapProposal)
		require.NoError(t, err)
	})

	t.Run("no nodes can accept: all at or above revocation term", func(t *testing.T) {
		err := CheckExternallyCertifiedProposalPossible(neutralCert, []*clustermetadatapb.ConsensusStatus{
			makeUnrecruitedStatus(a, makeRule(ruleNum(5, 0), atLeast(2), cohort...)),
		}, bootstrapProposal)
		require.EqualError(t, err, "no nodes could accept the proposed revocation")
	})

	t.Run("cert missing outgoing_rule_number", func(t *testing.T) {
		cert := &clustermetadatapb.ExternallyCertifiedRevocation{
			TermRevocation: coordRevocation(5),
			FrozenLsn:      "0/0",
		}
		err := CheckExternallyCertifiedProposalPossible(cert, []*clustermetadatapb.ConsensusStatus{
			makeUnrecruitedStatus(a, nil),
		}, bootstrapProposal)
		require.EqualError(t, err, "cert is missing outgoing_rule_number")
	})

	t.Run("cert missing frozen_lsn", func(t *testing.T) {
		cert := &clustermetadatapb.ExternallyCertifiedRevocation{
			TermRevocation:     coordRevocation(5),
			OutgoingRuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 0},
		}
		err := CheckExternallyCertifiedProposalPossible(cert, []*clustermetadatapb.ConsensusStatus{
			makeUnrecruitedStatus(a, nil),
		}, bootstrapProposal)
		require.EqualError(t, err, "cert is missing frozen_lsn")
	})

	t.Run("outgoing_rule_number: candidate rule exceeds certified term", func(t *testing.T) {
		cert := &clustermetadatapb.ExternallyCertifiedRevocation{
			TermRevocation:     coordRevocation(5),
			OutgoingRuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 2},
			FrozenLsn:          "0/0",
		}
		err := CheckExternallyCertifiedProposalPossible(cert, []*clustermetadatapb.ConsensusStatus{
			makeUnrecruitedStatus(a, makeRule(ruleNum(3, 0), atLeast(2), cohort...)),
		}, bootstrapProposal)
		require.EqualError(t, err, "node zone1_a is at rule term 3 but certified outgoing rule is term 2")
	})

	t.Run("frozen_lsn: invalid LSN string", func(t *testing.T) {
		cert := &clustermetadatapb.ExternallyCertifiedRevocation{
			TermRevocation:     coordRevocation(5),
			OutgoingRuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 0},
			FrozenLsn:          "bad-lsn",
		}
		err := CheckExternallyCertifiedProposalPossible(cert, []*clustermetadatapb.ConsensusStatus{
			makeUnrecruitedStatus(a, nil),
		}, bootstrapProposal)
		require.ErrorContains(t, err, "invalid frozen_lsn in cert")
	})

	t.Run("frozen_lsn: no node at or above threshold", func(t *testing.T) {
		cert := &clustermetadatapb.ExternallyCertifiedRevocation{
			TermRevocation:     coordRevocation(5),
			OutgoingRuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 0},
			FrozenLsn:          "0/9000000",
		}
		err := CheckExternallyCertifiedProposalPossible(cert, []*clustermetadatapb.ConsensusStatus{
			makeUnrecruitedStatus(a, nil),
			makeUnrecruitedStatus(b, nil),
			makeUnrecruitedStatus(c, nil),
		}, bootstrapProposal)
		require.EqualError(t, err, "no eligible leaders found among recruited nodes")
	})
}

func TestBuildExternallyCertifiedProposal(t *testing.T) {
	zone1 := poolerIDs.zone1
	incomingCohort := []*clustermetadatapb.ID{zone1.a, zone1.b, zone1.c}

	// bootstrapProposal picks the first eligible leader and proposes the incoming cohort.
	bootstrapProposal := func(r RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error) {
		leader := r.EligibleLeaders[0]
		return &consensusdatapb.CoordinatorProposal{
			TermRevocation: r.TermRevocation,
			ProposalLeader: &consensusdatapb.ProposalLeader{Id: leader.GetId()},
			ProposedRule: &clustermetadatapb.ShardRule{
				RuleNumber:       &clustermetadatapb.RuleNumber{CoordinatorTerm: r.TermRevocation.GetRevokedBelowTerm()},
				CohortMembers:    incomingCohort,
				DurabilityPolicy: topoclient.AtLeastN(2),
				LeaderId:         leader.GetId(),
			},
		}, nil
	}

	// neutralCert covers a fresh-bootstrap scenario: term 0 means "no recorded
	// rule has ever existed" and "0/0" means "any reported LSN is acceptable".
	// Both fields are required by certLeaderFilter even when the caller has no
	// real constraint to express.
	neutralCert := &clustermetadatapb.ExternallyCertifiedRevocation{
		TermRevocation:     revocation(5),
		OutgoingRuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 0},
		FrozenLsn:          "0/0",
	}

	t.Run("no nodes accepted the revocation", func(t *testing.T) {
		// filterByRevocation runs before certLeaderFilter, so we early-return
		// before the cert is inspected. Use neutralCert anyway for consistency.
		singleCohort := []*clustermetadatapb.ID{zone1.a}
		_, err := BuildExternallyCertifiedProposal(neutralCert, []*clustermetadatapb.ConsensusStatus{
			makeStatus(zone1.a, makeRule(ruleNum(3, 0), atLeast(2), singleCohort...), revocation(3)),
		}, proposeFirstEligible(makeRule(ruleNum(5, 0), atLeast(2), singleCohort...)))
		require.EqualError(t, err, "no nodes accepted the requested term revocation")
	})

	t.Run("bootstrap: no cert constraints, all recruited nodes eligible", func(t *testing.T) {
		statuses := []*clustermetadatapb.ConsensusStatus{
			makeStatus(zone1.a, nil, revocation(5)),
			makeStatus(zone1.b, nil, revocation(5)),
			makeStatus(zone1.c, nil, revocation(5)),
		}
		_, err := BuildExternallyCertifiedProposal(neutralCert, statuses, bootstrapProposal)
		require.NoError(t, err)
	})

	t.Run("cert missing outgoing_rule_number", func(t *testing.T) {
		cert := &clustermetadatapb.ExternallyCertifiedRevocation{
			TermRevocation: revocation(5),
			FrozenLsn:      "0/0",
		}
		_, err := BuildExternallyCertifiedProposal(cert, []*clustermetadatapb.ConsensusStatus{
			makeStatus(zone1.a, nil, revocation(5)),
		}, bootstrapProposal)
		require.EqualError(t, err, "cert is missing outgoing_rule_number")
	})

	t.Run("cert missing frozen_lsn", func(t *testing.T) {
		cert := &clustermetadatapb.ExternallyCertifiedRevocation{
			TermRevocation:     revocation(5),
			OutgoingRuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 0},
		}
		_, err := BuildExternallyCertifiedProposal(cert, []*clustermetadatapb.ConsensusStatus{
			makeStatus(zone1.a, nil, revocation(5)),
		}, bootstrapProposal)
		require.EqualError(t, err, "cert is missing frozen_lsn")
	})

	t.Run("outgoing_rule_number: node at certified term is allowed", func(t *testing.T) {
		outgoingRule := makeRule(ruleNum(3, 0), atLeast(2), incomingCohort...)
		cert := &clustermetadatapb.ExternallyCertifiedRevocation{
			TermRevocation:     revocation(5),
			OutgoingRuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 3},
			FrozenLsn:          "0/0",
		}
		statuses := []*clustermetadatapb.ConsensusStatus{
			makeStatus(zone1.a, outgoingRule, revocation(5)),
			makeStatus(zone1.b, outgoingRule, revocation(5)),
			makeStatus(zone1.c, outgoingRule, revocation(5)),
		}
		_, err := BuildExternallyCertifiedProposal(cert, statuses, bootstrapProposal)
		require.NoError(t, err)
	})

	t.Run("outgoing_rule_number: node rule exceeds certified term → error", func(t *testing.T) {
		outgoingRule := makeRule(ruleNum(4, 0), atLeast(2), incomingCohort...) // node progressed past the certified point
		cert := &clustermetadatapb.ExternallyCertifiedRevocation{
			TermRevocation:     revocation(5),
			OutgoingRuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 3},
			FrozenLsn:          "0/0",
		}
		statuses := []*clustermetadatapb.ConsensusStatus{
			makeStatus(zone1.a, outgoingRule, revocation(5)),
		}
		_, err := BuildExternallyCertifiedProposal(cert, statuses, bootstrapProposal)
		require.EqualError(t, err, "node zone1_pooler-a is at rule term 4 but certified outgoing rule is term 3")
	})

	t.Run("frozen_lsn: invalid LSN string → error", func(t *testing.T) {
		cert := &clustermetadatapb.ExternallyCertifiedRevocation{
			TermRevocation:     revocation(5),
			OutgoingRuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 0},
			FrozenLsn:          "not-an-lsn",
		}
		statuses := []*clustermetadatapb.ConsensusStatus{
			makeStatus(zone1.a, nil, revocation(5)),
		}
		_, err := BuildExternallyCertifiedProposal(cert, statuses, bootstrapProposal)
		require.ErrorContains(t, err, "invalid frozen_lsn in cert")
	})

	t.Run("frozen_lsn: node below threshold excluded from leadership, still endorses quorum", func(t *testing.T) {
		// a has LSN below frozen_lsn — excluded from EligibleLeaders.
		// b and c are at or above — eligible leaders.
		// All three are recruited so quorum is satisfied for the incoming cohort.
		cert := &clustermetadatapb.ExternallyCertifiedRevocation{
			TermRevocation:     revocation(5),
			OutgoingRuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 0},
			FrozenLsn:          "0/2000000",
		}
		statuses := []*clustermetadatapb.ConsensusStatus{
			makeStatusWithLSN(zone1.a, nil, revocation(5), "0/1000000"), // below frozen_lsn
			makeStatusWithLSN(zone1.b, nil, revocation(5), "0/2000000"), // at frozen_lsn → eligible
			makeStatusWithLSN(zone1.c, nil, revocation(5), "0/3000000"), // above → eligible
		}
		var gotResult RecruitmentResult
		_, err := BuildExternallyCertifiedProposal(cert, statuses, func(r RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error) {
			gotResult = r
			return bootstrapProposal(r)
		})
		require.NoError(t, err)
		eligibleNames := make([]string, 0, len(gotResult.EligibleLeaders))
		for _, cs := range gotResult.EligibleLeaders {
			eligibleNames = append(eligibleNames, cs.GetId().GetName())
		}
		assert.NotContains(t, eligibleNames, "pooler-a", "node below frozen_lsn must not be eligible leader")
		assert.Contains(t, eligibleNames, "pooler-c", "node above frozen_lsn must be eligible leader")
	})

	t.Run("frozen_lsn: no node at or above threshold → no eligible leaders", func(t *testing.T) {
		cert := &clustermetadatapb.ExternallyCertifiedRevocation{
			TermRevocation:     revocation(5),
			OutgoingRuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 0},
			FrozenLsn:          "0/9000000", // higher than all nodes
		}
		statuses := []*clustermetadatapb.ConsensusStatus{
			makeStatusWithLSN(zone1.a, nil, revocation(5), "0/1000000"),
			makeStatusWithLSN(zone1.b, nil, revocation(5), "0/2000000"),
			makeStatusWithLSN(zone1.c, nil, revocation(5), "0/3000000"),
		}
		_, err := BuildExternallyCertifiedProposal(cert, statuses, bootstrapProposal)
		require.EqualError(t, err, "no eligible leaders found among recruited nodes")
	})
}

func TestDeduplicateStatuses_NilIDSkipped(t *testing.T) {
	a := makeID("zone1", "a")
	cohort := []*clustermetadatapb.ID{a}

	statuses := []*clustermetadatapb.ConsensusStatus{
		{Id: nil},
		makeStatus(a, makeRule(ruleNum(1, 0), atLeast(2), cohort...), revocation(5)),
	}

	result := deduplicateStatuses(statuses)
	require.Len(t, result, 1)
	assert.Equal(t, "a", result[0].GetId().GetName())
}

func TestCohortIntersect_NilIDSkipped(t *testing.T) {
	a := makeID("zone1", "a")
	cohort := []*clustermetadatapb.ID{a}

	statuses := []*clustermetadatapb.ConsensusStatus{
		{Id: nil},
		makeStatus(a, makeRule(ruleNum(1, 0), atLeast(2), cohort...), revocation(5)),
	}

	result := cohortIntersect(cohort, statuses)
	require.Len(t, result, 1)
	assert.Equal(t, "a", result[0].GetName())
}
