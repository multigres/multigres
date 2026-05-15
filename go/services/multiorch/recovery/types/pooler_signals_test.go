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
