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

// id builds a minimal *clustermetadatapb.ID for test fixtures. Shared across
// policy test files in this package.
func id(name, cell string) *clustermetadatapb.ID {
	return &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      cell,
		Name:      name,
	}
}

func TestAtLeastNPolicy_CheckAchievable(t *testing.T) {
	tests := []struct {
		name           string
		n              int
		proposedCohort []*clustermetadatapb.ID
		wantErrMsg     string // empty means expect nil error
	}{
		{
			name: "AT_LEAST_2 with 2 poolers in proposed cohort is achievable",
			n:    2,
			proposedCohort: []*clustermetadatapb.ID{
				id("pooler-1", "cell1"),
				id("pooler-2", "cell1"),
			},
		},
		{
			name: "AT_LEAST_2 with 3 poolers in proposed cohort is achievable",
			n:    2,
			proposedCohort: []*clustermetadatapb.ID{
				id("pooler-1", "cell1"),
				id("pooler-2", "cell1"),
				id("pooler-3", "cell1"),
			},
		},
		{
			name:           "AT_LEAST_2 with 1 pooler in proposed cohort is not achievable",
			n:              2,
			proposedCohort: []*clustermetadatapb.ID{id("pooler-1", "cell1")},
			wantErrMsg:     "proposed cohort has 1 poolers, required 2",
		},
		{
			name:           "AT_LEAST_1 with 1 pooler in proposed cohort is achievable",
			n:              1,
			proposedCohort: []*clustermetadatapb.ID{id("pooler-1", "cell1")},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := AtLeastNPolicy{N: tc.n, Desc: "test"}
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

func TestAtLeastNPolicy_CheckSufficientRecruitment(t *testing.T) {
	tests := []struct {
		name       string
		n          int
		cohort     []*clustermetadatapb.ID
		recruited  []*clustermetadatapb.ID
		wantErrMsg string // empty means expect nil error
	}{
		{
			name: "AT_LEAST_2 with all 3 cohort poolers recruited is sufficient",
			n:    2,
			cohort: []*clustermetadatapb.ID{
				id("pooler-1", "cell1"),
				id("pooler-2", "cell1"),
				id("pooler-3", "cell1"),
			},
			recruited: []*clustermetadatapb.ID{
				id("pooler-1", "cell1"),
				id("pooler-2", "cell1"),
				id("pooler-3", "cell1"),
			},
		},
		{
			name: "AT_LEAST_2 with exactly 2 of 3 cohort poolers recruited is sufficient",
			n:    2,
			cohort: []*clustermetadatapb.ID{
				id("pooler-1", "cell1"),
				id("pooler-2", "cell1"),
				id("pooler-3", "cell1"),
			},
			recruited: []*clustermetadatapb.ID{
				id("pooler-1", "cell1"),
				id("pooler-2", "cell1"),
			},
		},
		{
			name: "AT_LEAST_2 with only 1 of 3 cohort poolers recruited fails candidacy",
			n:    2,
			cohort: []*clustermetadatapb.ID{
				id("pooler-1", "cell1"),
				id("pooler-2", "cell1"),
				id("pooler-3", "cell1"),
			},
			recruited: []*clustermetadatapb.ID{
				id("pooler-1", "cell1"),
			},
			wantErrMsg: "candidacy not satisfied: recruited 1 poolers, required 2",
		},
		{
			name: "AT_LEAST_3 with only 2 of 3 cohort poolers recruited fails candidacy",
			n:    3,
			cohort: []*clustermetadatapb.ID{
				id("pooler-1", "cell1"),
				id("pooler-2", "cell1"),
				id("pooler-3", "cell1"),
			},
			recruited: []*clustermetadatapb.ID{
				id("pooler-1", "cell1"),
				id("pooler-2", "cell1"),
			},
			wantErrMsg: "candidacy not satisfied: recruited 2 poolers, required 3",
		},
		{
			name: "AT_LEAST_2 with 2 of 5 cohort poolers recruited passes candidacy but fails revocation (3 missing >= 2)",
			n:    2,
			cohort: []*clustermetadatapb.ID{
				id("pooler-1", "cell1"),
				id("pooler-2", "cell1"),
				id("pooler-3", "cell1"),
				id("pooler-4", "cell1"),
				id("pooler-5", "cell1"),
			},
			recruited: []*clustermetadatapb.ID{
				id("pooler-1", "cell1"),
				id("pooler-2", "cell1"),
			},
			wantErrMsg: "revocation not satisfied: 3 cohort poolers not recruited",
		},
		{
			name: "AT_LEAST_2 with 4 of 5 cohort poolers recruited is sufficient (1 missing < 2)",
			n:    2,
			cohort: []*clustermetadatapb.ID{
				id("pooler-1", "cell1"),
				id("pooler-2", "cell1"),
				id("pooler-3", "cell1"),
				id("pooler-4", "cell1"),
				id("pooler-5", "cell1"),
			},
			recruited: []*clustermetadatapb.ID{
				id("pooler-1", "cell1"),
				id("pooler-2", "cell1"),
				id("pooler-3", "cell1"),
				id("pooler-4", "cell1"),
			},
		},
		{
			name: "AT_LEAST_2 with a recruited pooler outside the cohort is rejected",
			n:    2,
			cohort: []*clustermetadatapb.ID{
				id("pooler-1", "cell1"),
				id("pooler-2", "cell1"),
				id("pooler-3", "cell1"),
			},
			recruited: []*clustermetadatapb.ID{
				id("pooler-1", "cell1"),
				id("stranger", "cell1"),
			},
			wantErrMsg: "recruited pooler cell1_stranger is not in cohort",
		},
		{
			name: "AT_LEAST_1 needs the whole cohort recruited because any single pooler can be an old quorum",
			n:    1,
			cohort: []*clustermetadatapb.ID{
				id("pooler-1", "cell1"),
				id("pooler-2", "cell1"),
				id("pooler-3", "cell1"),
			},
			recruited: []*clustermetadatapb.ID{
				id("pooler-1", "cell1"),
				id("pooler-2", "cell1"),
			},
			wantErrMsg: "revocation not satisfied: 1 cohort poolers not recruited",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := AtLeastNPolicy{N: tc.n, Desc: "test"}
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
