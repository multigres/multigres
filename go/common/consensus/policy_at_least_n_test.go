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
	"io"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// testLogger discards output to keep test runs quiet.
func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// clusterIDStrings maps poolers to their "cell_name" keys for unordered
// membership assertions via require.ElementsMatch.
func clusterIDStrings(ids []*clustermetadatapb.ID) []string {
	out := make([]string, len(ids))
	for i, id := range ids {
		out[i] = topoclient.ClusterIDString(id)
	}
	return out
}

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
			name: "AT_LEAST_3 with 2 poolers in proposed cohort is not achievable",
			n:    3,
			proposedCohort: []*clustermetadatapb.ID{
				id("pooler-1", "cell1"),
				id("pooler-2", "cell1"),
			},
			wantErrMsg: "proposed cohort has 2 poolers, required 3",
		},
		{
			name:           "AT_LEAST_1 with 1 pooler in proposed cohort is achievable",
			n:              1,
			proposedCohort: []*clustermetadatapb.ID{id("pooler-1", "cell1")},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := AtLeastNPolicy{N: tc.n}
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
			// Uses poolers spread across cells to demonstrate that AT_LEAST_N
			// ignores cell layout — only pooler count matters.
			name: "AT_LEAST_2 with all 3 cohort poolers recruited is sufficient (cells ignored)",
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
			name: "AT_LEAST_2 with 2 of 3 cohort poolers is sufficient (majority + revocation both hold)",
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
			name: "AT_LEAST_2 with 1 of 3 cohort poolers fails majority",
			n:    2,
			cohort: []*clustermetadatapb.ID{
				id("pooler-1", "cell1"),
				id("pooler-2", "cell1"),
				id("pooler-3", "cell1"),
			},
			recruited: []*clustermetadatapb.ID{
				id("pooler-1", "cell1"),
			},
			wantErrMsg: "majority not satisfied: recruited 1 of 3 cohort poolers, need at least 2",
		},
		{
			name: "AT_LEAST_2 with 2 of 5 cohort poolers fails majority",
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
			wantErrMsg: "majority not satisfied: recruited 2 of 5 cohort poolers, need at least 3",
		},
		{
			name: "AT_LEAST_2 with 3 of 5 cohort poolers passes majority but fails revocation (2 missing >= 2)",
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
			},
			wantErrMsg: "revocation not satisfied: 2 cohort poolers not recruited",
		},
		{
			name: "AT_LEAST_2 with 4 of 5 cohort poolers is sufficient (1 missing < 2)",
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
			// N=8 is > half the cohort. Revocation alone would accept 3
			// recruited (since 10-8=2 missing cap). Majority forces 6+,
			// preventing two coordinators from recruiting disjoint sets of 3
			// at the same term.
			name: "AT_LEAST_8 with 3 of 10 cohort poolers fails majority even though revocation would pass alone",
			n:    8,
			cohort: []*clustermetadatapb.ID{
				id("pooler-1", "cell1"), id("pooler-2", "cell1"),
				id("pooler-3", "cell1"), id("pooler-4", "cell1"),
				id("pooler-5", "cell1"), id("pooler-6", "cell1"),
				id("pooler-7", "cell1"), id("pooler-8", "cell1"),
				id("pooler-9", "cell1"), id("pooler-10", "cell1"),
			},
			recruited: []*clustermetadatapb.ID{
				id("pooler-1", "cell1"), id("pooler-2", "cell1"),
				id("pooler-3", "cell1"),
			},
			wantErrMsg: "majority not satisfied: recruited 3 of 10 cohort poolers, need at least 6",
		},
		{
			// Flip side: N=2 is small, so revocation binds above majority.
			// Majority alone accepts 6; revocation requires 9.
			name: "AT_LEAST_2 with 6 of 10 cohort poolers passes majority but fails revocation",
			n:    2,
			cohort: []*clustermetadatapb.ID{
				id("pooler-1", "cell1"), id("pooler-2", "cell1"),
				id("pooler-3", "cell1"), id("pooler-4", "cell1"),
				id("pooler-5", "cell1"), id("pooler-6", "cell1"),
				id("pooler-7", "cell1"), id("pooler-8", "cell1"),
				id("pooler-9", "cell1"), id("pooler-10", "cell1"),
			},
			recruited: []*clustermetadatapb.ID{
				id("pooler-1", "cell1"), id("pooler-2", "cell1"),
				id("pooler-3", "cell1"), id("pooler-4", "cell1"),
				id("pooler-5", "cell1"), id("pooler-6", "cell1"),
			},
			wantErrMsg: "revocation not satisfied: 4 cohort poolers not recruited",
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
			p := AtLeastNPolicy{N: tc.n}
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

func TestAtLeastNPolicy_BuildLeaderDurabilityPostgresConfig(t *testing.T) {
	logger := testLogger()
	leader := id("primary", "cell-primary")

	t.Run("N=1 returns local-only config (clears sync standbys)", func(t *testing.T) {
		p := AtLeastNPolicy{N: 1}
		cohort := []*clustermetadatapb.ID{
			leader,
			id("mp1", "cell1"),
			id("mp2", "cell1"),
		}
		cfg, err := p.BuildLeaderDurabilityPostgresConfig(logger, cohort, leader)
		require.NoError(t, err)
		require.NotNil(t, cfg)
		require.Equal(t, multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_LOCAL, cfg.SyncCommit)
		require.Empty(t, cfg.SyncStandbyIDs, "N=1 should produce an empty standby list so Postgres clears synchronous_standby_names")
	})

	t.Run("N=2 with cohort of 2 sets num_sync=1", func(t *testing.T) {
		p := AtLeastNPolicy{N: 2}
		cohort := []*clustermetadatapb.ID{leader, id("mp1", "cell1")}
		cfg, err := p.BuildLeaderDurabilityPostgresConfig(logger, cohort, leader)
		require.NoError(t, err)
		require.NotNil(t, cfg)
		require.Equal(t, multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON, cfg.SyncCommit)
		require.Equal(t, multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_ANY, cfg.SyncMethod)
		require.Equal(t, 1, cfg.NumSync)
		require.ElementsMatch(t,
			[]string{"cell-primary_primary", "cell1_mp1"},
			clusterIDStrings(cfg.SyncStandbyIDs),
		)
	})

	t.Run("N=3 with cohort of 3 sets num_sync=2", func(t *testing.T) {
		p := AtLeastNPolicy{N: 3}
		cohort := []*clustermetadatapb.ID{
			leader,
			id("mp1", "cell1"),
			id("mp2", "cell1"),
		}
		cfg, err := p.BuildLeaderDurabilityPostgresConfig(logger, cohort, leader)
		require.NoError(t, err)
		require.NotNil(t, cfg)
		require.Equal(t, 2, cfg.NumSync)
		require.Len(t, cfg.SyncStandbyIDs, 3)
	})

	t.Run("N=3 with cohort of 6 keeps num_sync=2 (not capped by cohort size)", func(t *testing.T) {
		p := AtLeastNPolicy{N: 3}
		cohort := []*clustermetadatapb.ID{
			leader,
			id("mp1", "cell1"), id("mp2", "cell1"),
			id("mp3", "cell1"), id("mp4", "cell1"),
			id("mp5", "cell1"),
		}
		cfg, err := p.BuildLeaderDurabilityPostgresConfig(logger, cohort, leader)
		require.NoError(t, err)
		require.NotNil(t, cfg)
		require.Equal(t, 2, cfg.NumSync, "num_sync should be N-1, not the cohort size")
		require.Len(t, cfg.SyncStandbyIDs, 6)
	})

	t.Run("AT_LEAST_N does not filter by cell", func(t *testing.T) {
		p := AtLeastNPolicy{N: 2}
		sameCellLeader := id("primary", "us-west-1a")
		cohort := []*clustermetadatapb.ID{
			sameCellLeader,
			id("mp1", "us-west-1a"), // same cell as leader; AT_LEAST_N keeps it
			id("mp2", "us-west-1b"),
		}
		cfg, err := p.BuildLeaderDurabilityPostgresConfig(logger, cohort, sameCellLeader)
		require.NoError(t, err)
		require.NotNil(t, cfg)
		require.ElementsMatch(t,
			[]string{"us-west-1a_primary", "us-west-1a_mp1", "us-west-1b_mp2"},
			clusterIDStrings(cfg.SyncStandbyIDs),
		)
	})

	t.Run("empty cohort returns error", func(t *testing.T) {
		p := AtLeastNPolicy{N: 2}
		cfg, err := p.BuildLeaderDurabilityPostgresConfig(logger, []*clustermetadatapb.ID{}, leader)
		require.Error(t, err)
		require.Nil(t, cfg)
		require.Contains(t, err.Error(), "cannot establish synchronous replication")
		require.Contains(t, err.Error(), "insufficient cohort members")
	})

	t.Run("insufficient cohort returns error", func(t *testing.T) {
		p := AtLeastNPolicy{N: 5} // requires 4 standbys
		cohort := []*clustermetadatapb.ID{
			leader,
			id("mp1", "us-west-1a"),
			id("mp2", "us-west-1b"),
		}
		cfg, err := p.BuildLeaderDurabilityPostgresConfig(logger, cohort, leader)
		require.Error(t, err)
		require.Nil(t, cfg)
		require.Contains(t, err.Error(), "required 4 standbys")
		require.Contains(t, err.Error(), "available 3")
	})

	t.Run("includes the full cohort (leader + standbys) in result", func(t *testing.T) {
		p := AtLeastNPolicy{N: 2}
		cohort := []*clustermetadatapb.ID{
			leader,
			id("mp-alpha", "cell-a"),
			id("mp-beta", "cell-b"),
			id("mp-gamma", "cell-c"),
		}
		cfg, err := p.BuildLeaderDurabilityPostgresConfig(logger, cohort, leader)
		require.NoError(t, err)
		require.NotNil(t, cfg)
		require.ElementsMatch(t,
			[]string{"cell-primary_primary", "cell-a_mp-alpha", "cell-b_mp-beta", "cell-c_mp-gamma"},
			clusterIDStrings(cfg.SyncStandbyIDs),
		)
	})
}
