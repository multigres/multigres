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
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
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
			p := MultiCellPolicy{N: tc.n}
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
			p := MultiCellPolicy{N: tc.n}
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

func TestMultiCellPolicy_BuildLeaderDurabilityPostgresConfig(t *testing.T) {
	logger := testLogger()

	t.Run("excludes same-cell standbys", func(t *testing.T) {
		candidate := id("primary", "us-west-1a")
		p := MultiCellPolicy{N: 2}
		cohort := []*clustermetadatapb.ID{
			candidate,
			id("mp1", "us-west-1a"), // same cell — excluded
			id("mp2", "us-west-1b"),
			id("mp3", "us-west-1c"),
		}
		cfg, err := p.BuildLeaderDurabilityPostgresConfig(logger, cohort, candidate)
		require.NoError(t, err)
		require.NotNil(t, cfg)
		require.Equal(t, 1, cfg.NumSync)
		require.ElementsMatch(t,
			[]string{"us-west-1b_mp2", "us-west-1c_mp3"},
			clusterIDStrings(cfg.SyncStandbyIDs),
		)
	})

	t.Run("only different-cell standbys with mixed cells", func(t *testing.T) {
		candidate := id("primary", "cell-a")
		p := MultiCellPolicy{N: 3}
		cohort := []*clustermetadatapb.ID{
			candidate,
			id("mp1", "cell-a"), // same cell — excluded
			id("mp2", "cell-a"), // same cell — excluded
			id("mp3", "cell-b"),
			id("mp4", "cell-c"),
			id("mp5", "cell-d"),
		}
		cfg, err := p.BuildLeaderDurabilityPostgresConfig(logger, cohort, candidate)
		require.NoError(t, err)
		require.NotNil(t, cfg)
		require.Equal(t, 2, cfg.NumSync)
		require.ElementsMatch(t,
			[]string{"cell-b_mp3", "cell-c_mp4", "cell-d_mp5"},
			clusterIDStrings(cfg.SyncStandbyIDs),
		)
	})

	t.Run("ALLOW with all standbys in candidate's cell returns nil", func(t *testing.T) {
		candidate := id("primary", "us-west-1a")
		p := MultiCellPolicy{
			N:             2,
			AsyncFallback: clustermetadatapb.AsyncReplicationFallbackMode_ASYNC_REPLICATION_FALLBACK_MODE_ALLOW,
		}
		cohort := []*clustermetadatapb.ID{
			candidate,
			id("mp1", "us-west-1a"),
			id("mp2", "us-west-1a"),
			id("mp3", "us-west-1a"),
		}
		cfg, err := p.BuildLeaderDurabilityPostgresConfig(logger, cohort, candidate)
		require.NoError(t, err)
		require.Nil(t, cfg)
	})

	t.Run("REJECT with all standbys in candidate's cell returns error", func(t *testing.T) {
		candidate := id("primary", "us-west-1a")
		p := MultiCellPolicy{
			N:             2,
			AsyncFallback: clustermetadatapb.AsyncReplicationFallbackMode_ASYNC_REPLICATION_FALLBACK_MODE_REJECT,
		}
		cohort := []*clustermetadatapb.ID{
			candidate,
			id("mp1", "us-west-1a"),
			id("mp2", "us-west-1a"),
			id("mp3", "us-west-1a"),
		}
		cfg, err := p.BuildLeaderDurabilityPostgresConfig(logger, cohort, candidate)
		require.Error(t, err)
		require.Nil(t, cfg)
		require.Contains(t, err.Error(), "no eligible standbys in different cells")
		require.Contains(t, err.Error(), "candidate_cell=us-west-1a")
		require.Contains(t, err.Error(), "async_fallback=REJECT")
	})

	t.Run("UNKNOWN defaults to REJECT", func(t *testing.T) {
		candidate := id("primary", "us-west-1a")
		p := MultiCellPolicy{
			N:             2,
			AsyncFallback: clustermetadatapb.AsyncReplicationFallbackMode_ASYNC_REPLICATION_FALLBACK_MODE_UNKNOWN,
		}
		cohort := []*clustermetadatapb.ID{
			candidate,
			id("mp1", "us-west-1a"),
			id("mp2", "us-west-1a"),
		}
		cfg, err := p.BuildLeaderDurabilityPostgresConfig(logger, cohort, candidate)
		require.Error(t, err)
		require.Nil(t, cfg)
		require.Contains(t, err.Error(), "async_fallback=REJECT")
	})

	t.Run("ALLOW with insufficient different-cell standbys caps num_sync", func(t *testing.T) {
		candidate := id("primary", "cell-a")
		p := MultiCellPolicy{
			N:             4, // wants 3 different-cell standbys
			AsyncFallback: clustermetadatapb.AsyncReplicationFallbackMode_ASYNC_REPLICATION_FALLBACK_MODE_ALLOW,
		}
		cohort := []*clustermetadatapb.ID{
			candidate,
			id("mp1", "cell-a"), // excluded
			id("mp2", "cell-b"),
			id("mp3", "cell-c"),
		}
		cfg, err := p.BuildLeaderDurabilityPostgresConfig(logger, cohort, candidate)
		require.NoError(t, err)
		require.NotNil(t, cfg)
		require.Equal(t, 2, cfg.NumSync, "num_sync should be capped at the eligible-standby count")
		require.Len(t, cfg.SyncStandbyIDs, 2)
	})

	t.Run("REJECT with insufficient different-cell standbys returns error", func(t *testing.T) {
		candidate := id("primary", "cell-a")
		p := MultiCellPolicy{
			N:             4, // wants 3 different-cell standbys
			AsyncFallback: clustermetadatapb.AsyncReplicationFallbackMode_ASYNC_REPLICATION_FALLBACK_MODE_REJECT,
		}
		cohort := []*clustermetadatapb.ID{
			candidate,
			id("mp1", "cell-a"), // excluded
			id("mp2", "cell-a"), // excluded
			id("mp3", "cell-b"),
		}
		cfg, err := p.BuildLeaderDurabilityPostgresConfig(logger, cohort, candidate)
		require.Error(t, err)
		require.Nil(t, cfg)
		require.Contains(t, err.Error(), "insufficient different-cell standbys")
		require.Contains(t, err.Error(), "required 3 standbys")
		require.Contains(t, err.Error(), "async_fallback=REJECT")
	})

	t.Run("N=2 with 3 different-cell standbys includes all", func(t *testing.T) {
		candidate := id("primary", "cell-primary")
		p := MultiCellPolicy{N: 2}
		cohort := []*clustermetadatapb.ID{
			candidate,
			id("mp1", "us-west-1a"),
			id("mp2", "us-west-1b"),
			id("mp3", "us-west-1c"),
		}
		cfg, err := p.BuildLeaderDurabilityPostgresConfig(logger, cohort, candidate)
		require.NoError(t, err)
		require.NotNil(t, cfg)
		require.Equal(t, 1, cfg.NumSync, "num_sync should be N-1")
		require.Equal(t, multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_ANY, cfg.SyncMethod)
		require.ElementsMatch(t,
			[]string{"us-west-1a_mp1", "us-west-1b_mp2", "us-west-1c_mp3"},
			clusterIDStrings(cfg.SyncStandbyIDs),
		)
	})

	t.Run("uses ON commit and ANY method", func(t *testing.T) {
		candidate := id("primary", "cell-a")
		p := MultiCellPolicy{N: 2}
		cohort := []*clustermetadatapb.ID{
			candidate,
			id("mp1", "cell-b"),
		}
		cfg, err := p.BuildLeaderDurabilityPostgresConfig(logger, cohort, candidate)
		require.NoError(t, err)
		require.NotNil(t, cfg)
		require.Equal(t, multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON, cfg.SyncCommit)
		require.Equal(t, multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_ANY, cfg.SyncMethod)
	})
}
