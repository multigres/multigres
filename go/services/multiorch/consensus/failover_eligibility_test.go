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

func TestFilterFailoverEligible(t *testing.T) {
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
			gotEligible, gotIneligible := filterFailoverEligible(tt.cohort)

			var gotEligibleNames []string
			for _, p := range gotEligible {
				gotEligibleNames = append(gotEligibleNames, p.GetMultipooler().GetId().GetName())
			}
			require.Equal(t, tt.wantEligible, gotEligibleNames)
			require.Equal(t, tt.wantIneligible, gotIneligible)
		})
	}
}
