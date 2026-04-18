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

package consensus

import (
	"testing"

	"github.com/stretchr/testify/require"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

func TestMultiCellPolicy_CheckAchievable(t *testing.T) {
	tests := []struct {
		name           string
		n              int
		proposedCohort []*clustermetadatapb.ID
		wantErrMsg     string
	}{
		{
			name: "MULTI_CELL_2 with poolers in 2 cells is achievable",
			n:    2,
			proposedCohort: []*clustermetadatapb.ID{
				id("pooler-1", "cell1"),
				id("pooler-2", "cell2"),
			},
		},
		{
			name: "MULTI_CELL_2 with poolers in 3 cells is achievable",
			n:    2,
			proposedCohort: []*clustermetadatapb.ID{
				id("pooler-1", "cell1"),
				id("pooler-2", "cell2"),
				id("pooler-3", "cell3"),
			},
		},
		{
			name: "MULTI_CELL_2 with 3 poolers all in cell1 is not achievable",
			n:    2,
			proposedCohort: []*clustermetadatapb.ID{
				id("pooler-1", "cell1"),
				id("pooler-2", "cell1"),
				id("pooler-3", "cell1"),
			},
			wantErrMsg: "proposed cohort spans 1 cells, required 2",
		},
		{
			name: "MULTI_CELL_3 with poolers in 2 cells is not achievable",
			n:    3,
			proposedCohort: []*clustermetadatapb.ID{
				id("pooler-1", "cell1"),
				id("pooler-2", "cell2"),
			},
			wantErrMsg: "proposed cohort spans 2 cells, required 3",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := MultiCellPolicy{N: tc.n, Desc: "test"}
			err := p.CheckAchievable(tc.proposedCohort)
			if tc.wantErrMsg == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.wantErrMsg)
			}
		})
	}
}

func TestMultiCellPolicy_CheckSufficientRecruitment(t *testing.T) {
	tests := []struct {
		name       string
		n          int
		cohort     []*clustermetadatapb.ID
		recruited  []*clustermetadatapb.ID
		wantErrMsg string
	}{
		{
			name: "MULTI_CELL_2 with all 3 cohort cells recruited is sufficient",
			n:    2,
			cohort: []*clustermetadatapb.ID{
				id("pooler-1", "cell1"),
				id("pooler-2", "cell2"),
				id("pooler-3", "cell3"),
			},
			recruited: []*clustermetadatapb.ID{
				id("pooler-1", "cell1"),
				id("pooler-2", "cell2"),
				id("pooler-3", "cell3"),
			},
		},
		{
			name: "MULTI_CELL_2 with 2 of 3 cohort cells covered is sufficient",
			n:    2,
			cohort: []*clustermetadatapb.ID{
				id("pooler-1", "cell1"),
				id("pooler-2", "cell2"),
				id("pooler-3", "cell3"),
			},
			recruited: []*clustermetadatapb.ID{
				id("pooler-1", "cell1"),
				id("pooler-2", "cell2"),
			},
		},
		{
			name: "MULTI_CELL_2 with 3 of 4 cohort poolers (both cell1 poolers plus one cell2) is sufficient",
			n:    2,
			cohort: []*clustermetadatapb.ID{
				id("pooler-1", "cell1"),
				id("pooler-4", "cell1"),
				id("pooler-2", "cell2"),
				id("pooler-3", "cell3"),
			},
			recruited: []*clustermetadatapb.ID{
				id("pooler-1", "cell1"),
				id("pooler-4", "cell1"),
				id("pooler-2", "cell2"),
			},
		},
		{
			name: "MULTI_CELL_2 with 2 of 4 cohort poolers all in cell1 fails majority",
			n:    2,
			cohort: []*clustermetadatapb.ID{
				id("pooler-1", "cell1"),
				id("pooler-4", "cell1"),
				id("pooler-2", "cell2"),
				id("pooler-3", "cell3"),
			},
			recruited: []*clustermetadatapb.ID{
				id("pooler-1", "cell1"),
				id("pooler-4", "cell1"),
			},
			wantErrMsg: "majority not satisfied: recruited 2 of 4 cohort poolers, need at least 3",
		},
		{
			// 6 poolers, 3 cells × 2 per cell. Recruiting one pooler per cell
			// covers every cohort cell but is a pooler-minority (3 of 6), so
			// two coordinators could recruit disjoint halves at the same term.
			// Majority catches this.
			name: "MULTI_CELL_2 with 3 of 6 cohort poolers (one per cell) fails majority",
			n:    2,
			cohort: []*clustermetadatapb.ID{
				id("pooler-1a", "cell1"),
				id("pooler-1b", "cell1"),
				id("pooler-2a", "cell2"),
				id("pooler-2b", "cell2"),
				id("pooler-3a", "cell3"),
				id("pooler-3b", "cell3"),
			},
			recruited: []*clustermetadatapb.ID{
				id("pooler-1a", "cell1"),
				id("pooler-2a", "cell2"),
				id("pooler-3a", "cell3"),
			},
			wantErrMsg: "majority not satisfied: recruited 3 of 6 cohort poolers, need at least 4",
		},
		{
			// Cohort: 3 poolers in cell1, 1 in cell2, 1 in cell3. N=2.
			// Recruiting all 3 cell1 poolers clears majority (3 of 5, need 3)
			// but leaves un-recruited cells {cell2, cell3} = 2 cells, enough
			// for a parallel 2-cell quorum. Revocation catches it.
			name: "MULTI_CELL_2 with 3 of 5 cohort poolers passes majority but fails revocation",
			n:    2,
			cohort: []*clustermetadatapb.ID{
				id("pooler-1a", "cell1"),
				id("pooler-1b", "cell1"),
				id("pooler-1c", "cell1"),
				id("pooler-2", "cell2"),
				id("pooler-3", "cell3"),
			},
			recruited: []*clustermetadatapb.ID{
				id("pooler-1a", "cell1"),
				id("pooler-1b", "cell1"),
				id("pooler-1c", "cell1"),
			},
			wantErrMsg: "revocation not satisfied: un-recruited cohort poolers span 2 cells",
		},
		{
			name: "MULTI_CELL_2 with a recruited pooler outside the cohort is rejected",
			n:    2,
			cohort: []*clustermetadatapb.ID{
				id("pooler-1", "cell1"),
				id("pooler-2", "cell2"),
				id("pooler-3", "cell3"),
			},
			recruited: []*clustermetadatapb.ID{
				id("pooler-1", "cell1"),
				id("stranger", "cell2"),
			},
			wantErrMsg: "recruited pooler cell2_stranger is not in cohort",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := MultiCellPolicy{N: tc.n, Desc: "test"}
			err := p.CheckSufficientRecruitment(tc.cohort, tc.recruited)
			if tc.wantErrMsg == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.wantErrMsg)
			}
		})
	}
}
