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
			name: "MULTI_CELL_2 with multiple poolers in cell1 plus one in cell2 counts as 2 distinct cells",
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
			name: "MULTI_CELL_2 with recruited poolers all in cell1 fails candidacy",
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
			wantErrMsg: "candidacy not satisfied: recruited poolers span 1 cells, required 2",
		},
		{
			name: "MULTI_CELL_2 with cohort of 2 poolers per cell across 3 cells, recruiting one per cell fails revocation",
			n:    2,
			// 3 cells × 2 poolers per cell = 6 poolers.
			cohort: []*clustermetadatapb.ID{
				id("pooler-1a", "cell1"),
				id("pooler-1b", "cell1"),
				id("pooler-2a", "cell2"),
				id("pooler-2b", "cell2"),
				id("pooler-3a", "cell3"),
				id("pooler-3b", "cell3"),
			},
			// Recruit one pooler from each cell — candidacy passes (3 cells
			// covered) and every cohort cell is represented, but the 3
			// un-recruited poolers also span all 3 cells and could form a
			// separate 2-cell quorum on their own.
			recruited: []*clustermetadatapb.ID{
				id("pooler-1a", "cell1"),
				id("pooler-2a", "cell2"),
				id("pooler-3a", "cell3"),
			},
			wantErrMsg: "revocation not satisfied",
		},
		{
			name: "MULTI_CELL_2 a cell with more than one pooler, requires recruiting all poolers from that cell",
			n:    2,
			cohort: []*clustermetadatapb.ID{
				id("pooler-1", "cell1"),
				id("pooler-4", "cell1"),
				id("pooler-2", "cell2"),
				id("pooler-3", "cell3"),
			},
			recruited: []*clustermetadatapb.ID{
				id("pooler-1", "cell1"),
				id("pooler-2", "cell2"),
			},
			wantErrMsg: "revocation not satisfied",
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
		{
			name: "MULTI_CELL_3 with only 2 of 3 cohort cells covered fails candidacy",
			n:    3,
			cohort: []*clustermetadatapb.ID{
				id("pooler-1", "cell1"),
				id("pooler-2", "cell2"),
				id("pooler-3", "cell3"),
			},
			recruited: []*clustermetadatapb.ID{
				id("pooler-1", "cell1"),
				id("pooler-2", "cell2"),
			},
			wantErrMsg: "candidacy not satisfied: recruited poolers span 2 cells, required 3",
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
