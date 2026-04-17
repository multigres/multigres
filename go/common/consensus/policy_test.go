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

	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

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
			wantErrMsg: "recruited pooler cell1/stranger is not in cohort",
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
			wantErrMsg: "recruited pooler cell2/stranger is not in cohort",
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

func TestPolicyFromProto(t *testing.T) {
	tests := []struct {
		name    string
		policy  *clustermetadatapb.DurabilityPolicy
		wantErr string
		wantOK  func(DurabilityPolicy) bool
	}{
		{
			name:   "AT_LEAST_N returns AtLeastNPolicy",
			policy: topoclient.AtLeastN(2),
			wantOK: func(p DurabilityPolicy) bool { _, ok := p.(AtLeastNPolicy); return ok },
		},
		{
			name:   "MULTI_CELL_AT_LEAST_N returns MultiCellPolicy",
			policy: topoclient.MultiCellAtLeastN(2),
			wantOK: func(p DurabilityPolicy) bool { _, ok := p.(MultiCellPolicy); return ok },
		},
		{
			name:    "nil policy returns error",
			policy:  nil,
			wantErr: "nil",
		},
		{
			name: "unknown quorum type returns error",
			policy: &clustermetadatapb.DurabilityPolicy{
				QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_UNKNOWN,
				RequiredCount: 2,
			},
			wantErr: "unsupported quorum type",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p, err := PolicyFromProto(tc.policy)
			if tc.wantErr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.wantErr)
				return
			}
			require.NoError(t, err)
			require.True(t, tc.wantOK(p))
		})
	}
}
