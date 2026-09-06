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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
)

// eligibilityNode builds a minimal PoolerHealthState carrying a name and cohort
// eligibility signal. A nil AvailabilityStatus is expressed by passing setStatus=false.
func eligibilityNode(name string, signal clustermetadatapb.CohortEligibilitySignal, setStatus bool) *multiorchdatapb.PoolerHealthState {
	p := &multiorchdatapb.PoolerHealthState{
		Multipooler: &clustermetadatapb.Multipooler{
			Id: &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: name},
		},
	}
	if setStatus {
		p.AvailabilityStatus = &clustermetadatapb.AvailabilityStatus{
			CohortEligibilityStatus: &clustermetadatapb.CohortEligibilityStatus{Signal: signal},
		}
	}
	return p
}

func TestFilterCohortIneligible(t *testing.T) {
	eligible := clustermetadatapb.CohortEligibilitySignal_COHORT_ELIGIBILITY_SIGNAL_ELIGIBLE
	ineligible := clustermetadatapb.CohortEligibilitySignal_COHORT_ELIGIBILITY_SIGNAL_INELIGIBLE

	tests := []struct {
		name           string
		cohort         []*multiorchdatapb.PoolerHealthState
		wantEligible   []string
		wantIneligible []string
	}{
		{
			name: "all eligible are kept, none reported ineligible",
			cohort: []*multiorchdatapb.PoolerHealthState{
				eligibilityNode("mp1", eligible, true),
				eligibilityNode("mp2", eligible, true),
			},
			wantEligible:   []string{"mp1", "mp2"},
			wantIneligible: nil,
		},
		{
			name: "an ineligible member is excluded and reported by name",
			cohort: []*multiorchdatapb.PoolerHealthState{
				eligibilityNode("mp1", eligible, true),
				eligibilityNode("mp2", ineligible, true),
				eligibilityNode("mp3", eligible, true),
			},
			wantEligible:   []string{"mp1", "mp3"},
			wantIneligible: []string{"mp2"},
		},
		{
			name: "all ineligible leaves no eligible members",
			cohort: []*multiorchdatapb.PoolerHealthState{
				eligibilityNode("mp1", ineligible, true),
				eligibilityNode("mp2", ineligible, true),
			},
			wantEligible:   nil,
			wantIneligible: []string{"mp1", "mp2"},
		},
		{
			name: "a nil availability status is treated as eligible",
			cohort: []*multiorchdatapb.PoolerHealthState{
				eligibilityNode("mp1", eligible, false),
			},
			wantEligible:   []string{"mp1"},
			wantIneligible: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotEligible, gotIneligible := filterCohortIneligible(tt.cohort)

			var gotEligibleNames []string
			for _, p := range gotEligible {
				gotEligibleNames = append(gotEligibleNames, p.GetMultipooler().GetId().GetName())
			}
			require.Equal(t, tt.wantEligible, gotEligibleNames)
			require.Equal(t, tt.wantIneligible, gotIneligible)
		})
	}
}

func TestBackoffRelevantRevocations(t *testing.T) {
	decision4 := &clustermetadatapb.RuleNumber{CoordinatorTerm: 4}
	decision6 := &clustermetadatapb.RuleNumber{CoordinatorTerm: 6}
	statusWith := func(rev *clustermetadatapb.TermRevocation) *clustermetadatapb.ConsensusStatus {
		return &clustermetadatapb.ConsensusStatus{TermRevocation: rev}
	}

	t.Run("excludes a revocation with no accepted term", func(t *testing.T) {
		got := backoffRelevantRevocations([]*clustermetadatapb.ConsensusStatus{
			statusWith(&clustermetadatapb.TermRevocation{}),
		}, decision4)
		assert.Empty(t, got)
	})

	t.Run("matches a revocation targeting the same decision", func(t *testing.T) {
		matching := &clustermetadatapb.TermRevocation{
			RevokedBelowTerm: 5,
			RecruitIntent:    &clustermetadatapb.RecruitIntent{ReplaceDecision: decision4},
		}
		got := backoffRelevantRevocations([]*clustermetadatapb.ConsensusStatus{statusWith(matching)}, decision4)
		require.Len(t, got, 1)
		assert.Same(t, matching, got[0])
	})

	t.Run("excludes a revocation demonstrably targeting a different decision", func(t *testing.T) {
		// e.g. a shard's original bootstrap revocation: resolved history once
		// the shard has moved on, so it must not gate a failure against the
		// current decision.
		different := &clustermetadatapb.TermRevocation{
			RevokedBelowTerm: 9,
			RecruitIntent:    &clustermetadatapb.RecruitIntent{ReplaceDecision: decision6},
		}
		got := backoffRelevantRevocations([]*clustermetadatapb.ConsensusStatus{statusWith(different)}, decision4)
		assert.Empty(t, got)
	})

	t.Run("includes a revocation with no RecruitIntent at all", func(t *testing.T) {
		// We cannot tell whether this revocation (e.g. an externally-supplied
		// cert, or an external actor forcing a resignation) is for our current
		// problem or an unrelated one, so it is not excluded as a free pass —
		// unlike commonconsensus.NewTermRevocation's stricter, exact-match
		// filtering, which only needs a same-decision match to be useful.
		untargeted := &clustermetadatapb.TermRevocation{RevokedBelowTerm: 5}
		got := backoffRelevantRevocations([]*clustermetadatapb.ConsensusStatus{statusWith(untargeted)}, decision4)
		require.Len(t, got, 1)
		assert.Same(t, untargeted, got[0])
	})
}

func TestEligibleConsensusStatuses(t *testing.T) {
	poolerHealth := func(name string, revokedBelow int64, ineligible bool) *multiorchdatapb.PoolerHealthState {
		h := &multiorchdatapb.PoolerHealthState{
			Multipooler: &clustermetadatapb.Multipooler{
				Id: &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: name},
			},
			ConsensusStatus: &clustermetadatapb.ConsensusStatus{
				TermRevocation: &clustermetadatapb.TermRevocation{RevokedBelowTerm: revokedBelow},
			},
		}
		if ineligible {
			h.AvailabilityStatus = &clustermetadatapb.AvailabilityStatus{
				CohortEligibilityStatus: &clustermetadatapb.CohortEligibilityStatus{
					Signal: clustermetadatapb.CohortEligibilitySignal_COHORT_ELIGIBILITY_SIGNAL_INELIGIBLE,
				},
			}
		}
		return h
	}

	t.Run("returns every eligible member's status", func(t *testing.T) {
		got := eligibleConsensusStatuses([]*multiorchdatapb.PoolerHealthState{
			poolerHealth("p1", 3, false),
			poolerHealth("p2", 7, false),
		})
		require.Len(t, got, 2)
	})

	t.Run("excludes an INELIGIBLE member, matching runFailover's own cohort filtering", func(t *testing.T) {
		// A departing member (e.g. graceful shutdown) must be excluded here the
		// same way runFailover excludes it from NewTermRevocation's cohort, or
		// NextFailoverAttempt and NewTermRevocation can disagree on ReplaceDecision.
		got := eligibleConsensusStatuses([]*multiorchdatapb.PoolerHealthState{
			poolerHealth("p1", 3, false),
			poolerHealth("p2", 7, true),
		})
		require.Len(t, got, 1)
		assert.Equal(t, int64(3), got[0].GetTermRevocation().GetRevokedBelowTerm())
	})
}
