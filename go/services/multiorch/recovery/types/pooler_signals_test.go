// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package types

import (
	"testing"

	"github.com/stretchr/testify/assert"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
)

func TestPoolerIsCohortIneligible(t *testing.T) {
	tests := []struct {
		name string
		av   *clustermetadatapb.AvailabilityStatus
		want bool
	}{
		{
			name: "nil availability status (older pooler) treated as eligible",
			av:   nil,
			want: false,
		},
		{
			name: "availability status with no cohort eligibility field treated as eligible",
			av:   &clustermetadatapb.AvailabilityStatus{},
			want: false,
		},
		{
			name: "UNKNOWN signal (default) treated as eligible",
			av: &clustermetadatapb.AvailabilityStatus{
				CohortEligibilityStatus: &clustermetadatapb.CohortEligibilityStatus{
					Signal: clustermetadatapb.CohortEligibilitySignal_COHORT_ELIGIBILITY_SIGNAL_UNKNOWN,
				},
			},
			want: false,
		},
		{
			name: "ELIGIBLE returns false",
			av: &clustermetadatapb.AvailabilityStatus{
				CohortEligibilityStatus: &clustermetadatapb.CohortEligibilityStatus{
					Signal: clustermetadatapb.CohortEligibilitySignal_COHORT_ELIGIBILITY_SIGNAL_ELIGIBLE,
				},
			},
			want: false,
		},
		{
			name: "INELIGIBLE returns true",
			av: &clustermetadatapb.AvailabilityStatus{
				CohortEligibilityStatus: &clustermetadatapb.CohortEligibilityStatus{
					Signal: clustermetadatapb.CohortEligibilitySignal_COHORT_ELIGIBILITY_SIGNAL_INELIGIBLE,
				},
			},
			want: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, PoolerIsCohortIneligible(tc.av))
		})
	}
}

// poolerWithLeaderTerm builds a PoolerHealthState whose ConsensusStatus has
// the given primary term. Used to construct fixtures that exercise the
// staleness check on REQUESTING_DEMOTION.
func poolerWithLeaderTerm(t *testing.T, primaryTerm int64) *multiorchdatapb.PoolerHealthState {
	t.Helper()
	id := &clustermetadatapb.ID{Name: "mp1"}
	return &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{Id: id},
		ConsensusStatus: &clustermetadatapb.ConsensusStatus{
			Id: id,
			CurrentPosition: &clustermetadatapb.PoolerPosition{
				Rule: &clustermetadatapb.ShardRule{
					LeaderId: id,
					RuleNumber: &clustermetadatapb.RuleNumber{
						CoordinatorTerm: primaryTerm,
					},
				},
			},
		},
	}
}

func TestLeaderNeedsReplacement(t *testing.T) {
	t.Run("nil PoolerHealthState returns false", func(t *testing.T) {
		assert.False(t, LeaderNeedsReplacement(nil))
	})

	t.Run("no AvailabilityStatus returns false", func(t *testing.T) {
		p := poolerWithLeaderTerm(t, 5)
		assert.False(t, LeaderNeedsReplacement(p))
	})

	t.Run("AvailabilityStatus with no signals returns false", func(t *testing.T) {
		p := poolerWithLeaderTerm(t, 5)
		p.AvailabilityStatus = &clustermetadatapb.AvailabilityStatus{}
		assert.False(t, LeaderNeedsReplacement(p))
	})

	t.Run("REQUESTING_DEMOTION with matching term returns true", func(t *testing.T) {
		p := poolerWithLeaderTerm(t, 5)
		p.AvailabilityStatus = &clustermetadatapb.AvailabilityStatus{
			LeadershipStatus: &clustermetadatapb.LeadershipStatus{
				LeaderTerm: 5,
				Signal:     clustermetadatapb.LeadershipSignal_LEADERSHIP_SIGNAL_REQUESTING_DEMOTION,
			},
		}
		assert.True(t, LeaderNeedsReplacement(p))
	})

	t.Run("REQUESTING_DEMOTION with stale term returns false", func(t *testing.T) {
		// Signal carries term 3 but node's current primary term is 5 — stale.
		p := poolerWithLeaderTerm(t, 5)
		p.AvailabilityStatus = &clustermetadatapb.AvailabilityStatus{
			LeadershipStatus: &clustermetadatapb.LeadershipStatus{
				LeaderTerm: 3,
				Signal:     clustermetadatapb.LeadershipSignal_LEADERSHIP_SIGNAL_REQUESTING_DEMOTION,
			},
		}
		assert.False(t, LeaderNeedsReplacement(p))
	})

	t.Run("LeadershipSignal_ACTIVE returns false even at matching term", func(t *testing.T) {
		p := poolerWithLeaderTerm(t, 5)
		p.AvailabilityStatus = &clustermetadatapb.AvailabilityStatus{
			LeadershipStatus: &clustermetadatapb.LeadershipStatus{
				LeaderTerm: 5,
				Signal:     clustermetadatapb.LeadershipSignal_LEADERSHIP_SIGNAL_ACTIVE,
			},
		}
		assert.False(t, LeaderNeedsReplacement(p))
	})
}
