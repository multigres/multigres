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
	"fmt"
	"math/bits"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

func TestParseUserSpecifiedDurabilityPolicy(t *testing.T) {
	t.Run("AT_LEAST_2", func(t *testing.T) {
		policy, err := ParseUserSpecifiedDurabilityPolicy("AT_LEAST_2")
		require.NoError(t, err)
		assert.Equal(t, clustermetadatapb.QuorumType_QUORUM_TYPE_AT_LEAST_N, policy.QuorumType)
		assert.Equal(t, int32(2), policy.RequiredCount)
		assert.Equal(t, "At least 2 nodes must acknowledge", policy.Description)
	})

	t.Run("MULTI_CELL_AT_LEAST_2", func(t *testing.T) {
		policy, err := ParseUserSpecifiedDurabilityPolicy("MULTI_CELL_AT_LEAST_2")
		require.NoError(t, err)
		assert.Equal(t, clustermetadatapb.QuorumType_QUORUM_TYPE_MULTI_CELL_AT_LEAST_N, policy.QuorumType)
		assert.Equal(t, int32(2), policy.RequiredCount)
		assert.Equal(t, "At least 2 nodes from different cells must acknowledge", policy.Description)
	})

	t.Run("ANY_2 unsupported", func(t *testing.T) {
		policy, err := ParseUserSpecifiedDurabilityPolicy("ANY_2")
		require.Error(t, err)
		assert.Nil(t, policy)
		assert.Contains(t, err.Error(), "unsupported durability policy")
	})

	t.Run("MULTI_CELL_ANY_2 unsupported", func(t *testing.T) {
		policy, err := ParseUserSpecifiedDurabilityPolicy("MULTI_CELL_ANY_2")
		require.Error(t, err)
		assert.Nil(t, policy)
		assert.Contains(t, err.Error(), "unsupported durability policy")
	})

	t.Run("invalid policy name", func(t *testing.T) {
		policy, err := ParseUserSpecifiedDurabilityPolicy("INVALID_POLICY")
		require.Error(t, err)
		assert.Nil(t, policy)
		assert.Contains(t, err.Error(), "unsupported durability policy")
	})
}

func TestNewPolicyWithCohort(t *testing.T) {
	a := id("a", "zone1")
	b := id("b", "zone1")

	t.Run("valid proto: AtLeastN", func(t *testing.T) {
		got, err := NewPolicyWithCohort(
			[]*clustermetadatapb.ID{a, b},
			topoclient.AtLeastN(2),
		)
		require.NoError(t, err)
		assert.Equal(t, AtLeastNPolicy{N: 2}, got.Policy)
		assert.ElementsMatch(t, clusterIDStrings([]*clustermetadatapb.ID{a, b}), clusterIDStrings(got.Cohort))
	})

	t.Run("invalid proto returns wrapped error", func(t *testing.T) {
		got, err := NewPolicyWithCohort(
			[]*clustermetadatapb.ID{a, b},
			&clustermetadatapb.DurabilityPolicy{QuorumType: clustermetadatapb.QuorumType_QUORUM_TYPE_UNKNOWN},
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid durability policy")
		assert.Zero(t, got.Policy)
		assert.Nil(t, got.Cohort)
	})
}

// stubPolicy is a DurabilityPolicy used only to exercise the "unsupported
// policy type" branch of BuildPolicyTransition. It is not a real policy
// family — its method bodies are deliberately minimal.
type stubPolicy struct{}

func (stubPolicy) SatisfiedBy([]*clustermetadatapb.ID) error { return nil }

func (stubPolicy) BuildSyncReplicationConfig([]*clustermetadatapb.ID, *clustermetadatapb.ID) (*SyncReplicationConfig, error) {
	return nil, nil
}

func (stubPolicy) ToProto() *clustermetadatapb.DurabilityPolicy { return nil }

func (stubPolicy) Description() string { return "stub policy (test only)" }

func TestBuildPolicyTransition(t *testing.T) {
	a := id("a", "zone1")
	b := id("b", "zone1")
	c := id("c", "zone2")
	d := id("d", "zone2")

	type tc struct {
		name         string
		outgoing     PolicyWithCohort
		incoming     PolicyWithCohort
		wantBoth     PolicyWithCohort
		wantIncoming PolicyWithCohort
		wantErrMsg   string
	}

	pwc := func(policy DurabilityPolicy, cohort ...*clustermetadatapb.ID) PolicyWithCohort {
		return PolicyWithCohort{Policy: policy, Cohort: cohort}
	}
	assertPWC := func(t *testing.T, label string, want, got PolicyWithCohort) {
		t.Helper()
		assert.Equal(t, want.Policy, got.Policy, "%s: policy mismatch", label)
		assert.ElementsMatch(t, clusterIDStrings(want.Cohort), clusterIDStrings(got.Cohort), "%s: cohort mismatch", label)
	}

	tests := []tc{
		{
			name:         "identical: Both and Incoming equal the incoming policy",
			outgoing:     pwc(AtLeastNPolicy{N: 2}, a, b, c),
			incoming:     pwc(AtLeastNPolicy{N: 2}, a, b, c),
			wantBoth:     pwc(AtLeastNPolicy{N: 2}, a, b, c),
			wantIncoming: pwc(AtLeastNPolicy{N: 2}, a, b, c),
		},
		{
			name:         "N increases, same cohort: Both uses the larger (incoming) N",
			outgoing:     pwc(AtLeastNPolicy{N: 2}, a, b, c),
			incoming:     pwc(AtLeastNPolicy{N: 3}, a, b, c),
			wantBoth:     pwc(AtLeastNPolicy{N: 3}, a, b, c),
			wantIncoming: pwc(AtLeastNPolicy{N: 3}, a, b, c),
		},
		{
			name:         "N decreases, same cohort: Both uses the larger (outgoing) N",
			outgoing:     pwc(AtLeastNPolicy{N: 3}, a, b, c),
			incoming:     pwc(AtLeastNPolicy{N: 2}, a, b, c),
			wantBoth:     pwc(AtLeastNPolicy{N: 3}, a, b, c),
			wantIncoming: pwc(AtLeastNPolicy{N: 2}, a, b, c),
		},
		{
			name:         "cohort shrinks (incoming ⊂ outgoing), same N: Both uses the smaller (incoming) cohort",
			outgoing:     pwc(AtLeastNPolicy{N: 2}, a, b, c),
			incoming:     pwc(AtLeastNPolicy{N: 2}, a, b),
			wantBoth:     pwc(AtLeastNPolicy{N: 2}, a, b),
			wantIncoming: pwc(AtLeastNPolicy{N: 2}, a, b),
		},
		{
			name:         "cohort grows (outgoing ⊂ incoming), same N: Both uses the smaller (outgoing) cohort",
			outgoing:     pwc(AtLeastNPolicy{N: 2}, a, b),
			incoming:     pwc(AtLeastNPolicy{N: 2}, a, b, c),
			wantBoth:     pwc(AtLeastNPolicy{N: 2}, a, b),
			wantIncoming: pwc(AtLeastNPolicy{N: 2}, a, b, c),
		},
		{
			name:       "disjoint cohorts, same N: error — neither is a subset",
			outgoing:   pwc(AtLeastNPolicy{N: 2}, a, b),
			incoming:   pwc(AtLeastNPolicy{N: 2}, c, d),
			wantErrMsg: "neither cohort is a subset of the other",
		},
		{
			name:       "N and cohort both change: error",
			outgoing:   pwc(AtLeastNPolicy{N: 2}, a, b, c),
			incoming:   pwc(AtLeastNPolicy{N: 3}, a, b, c, d),
			wantErrMsg: "both N and cohort changed simultaneously",
		},
		{
			name:       "mixed policy families: error",
			outgoing:   pwc(AtLeastNPolicy{N: 2}, a, b, c),
			incoming:   pwc(multiCellPolicy{}, a, b, c),
			wantErrMsg: "policy types must match",
		},
		{
			// Defensive: a future DurabilityPolicy implementation that isn't
			// AtLeastNPolicy or MultiCellPolicy should be explicitly rejected
			// rather than silently fall through. Tested with a stub since no
			// such third type exists in production today.
			name:       "unsupported policy type: error",
			outgoing:   pwc(stubPolicy{}, a, b, c),
			incoming:   pwc(stubPolicy{}, a, b, c),
			wantErrMsg: "policies must be AtLeastN or MultiCellAtLeastN",
		},
		{
			name:         "MultiCell: cohort shrinks, same N",
			outgoing:     pwc(multiCellPolicy{}, a, b, c),
			incoming:     pwc(multiCellPolicy{}, a, b),
			wantBoth:     pwc(multiCellPolicy{}, a, b),
			wantIncoming: pwc(multiCellPolicy{}, a, b),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := BuildPolicyTransition(tc.outgoing, tc.incoming)
			if tc.wantErrMsg != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErrMsg)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, got)
			assertPWC(t, "Both", tc.wantBoth, got.Both)
			assertPWC(t, "Incoming", tc.wantIncoming, got.Incoming)
		})
	}
}

func TestNewPolicyFromProto(t *testing.T) {
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
			wantOK: func(p DurabilityPolicy) bool { _, ok := p.(multiCellPolicy); return ok },
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
		{
			name: "AT_LEAST_N with RequiredCount=0 is rejected",
			policy: &clustermetadatapb.DurabilityPolicy{
				QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_AT_LEAST_N,
				RequiredCount: 0,
			},
			wantErr: "AT_LEAST_N requires RequiredCount >= 1",
		},
		{
			name: "AT_LEAST_N with RequiredCount=-1 is rejected",
			policy: &clustermetadatapb.DurabilityPolicy{
				QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_AT_LEAST_N,
				RequiredCount: -1,
			},
			wantErr: "AT_LEAST_N requires RequiredCount >= 1",
		},
		{
			name: "MULTI_CELL_AT_LEAST_N with RequiredCount=0 is rejected",
			policy: &clustermetadatapb.DurabilityPolicy{
				QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_MULTI_CELL_AT_LEAST_N,
				RequiredCount: 0,
			},
			wantErr: "MULTI_CELL_AT_LEAST_N requires RequiredCount == 2, got 0",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p, err := NewPolicyFromProto(tc.policy)
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

// Any write Postgres treats as committed must meet the durability policy. Postgres
// commits once num_sync of the listed standbys acknowledge, so for every such set the
// primary plus those standbys must satisfy the policy. For AT_LEAST_3 over {primary,
// a, b}, `ANY 2 (a, b)` holds; a too-weak `ANY 1 (a, b)` would commit on {primary, a},
// two copies where the policy needs three.
func TestBuildSyncReplicationConfig_CommittedWritesSatisfyPolicy(t *testing.T) {
	// The primary is in cell-a; each row lists the cells of its standbys.
	primary := id("primary", "cell-a")
	tests := []struct {
		policy       DurabilityPolicy
		standbyCells []string
	}{
		{AtLeastNPolicy{N: 1}, []string{"cell-a"}},
		{AtLeastNPolicy{N: 2}, []string{"cell-a"}},
		{AtLeastNPolicy{N: 2}, []string{"cell-a", "cell-b"}},
		{AtLeastNPolicy{N: 3}, []string{"cell-a", "cell-b"}},
		{AtLeastNPolicy{N: 3}, []string{"cell-a", "cell-b", "cell-c"}},
		{AtLeastNPolicy{N: 4}, []string{"cell-a", "cell-b", "cell-c"}},
		{multiCellPolicy{}, []string{"cell-b"}},
		{multiCellPolicy{}, []string{"cell-a", "cell-b", "cell-c"}},
		{multiCellPolicy{}, []string{"cell-a", "cell-b", "cell-b", "cell-c"}},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("%s standbys in %v", tt.policy.Description(), tt.standbyCells), func(t *testing.T) {
			cohort := []*clustermetadatapb.ID{primary}
			for i, cell := range tt.standbyCells {
				cohort = append(cohort, id(fmt.Sprintf("standby-%d", i), cell))
			}
			cfg, err := tt.policy.BuildSyncReplicationConfig(cohort, primary)
			require.NoError(t, err)

			// Postgres ignores the primary's own entry in the standby list.
			var standbys []*clustermetadatapb.ID
			for _, s := range cfg.SyncStandbyIDs {
				if topoclient.ClusterIDString(s) != topoclient.ClusterIDString(primary) {
					standbys = append(standbys, s)
				}
			}
			if len(standbys) == 0 {
				require.NoError(t, tt.policy.SatisfiedBy([]*clustermetadatapb.ID{primary}), "the primary alone must satisfy a config with no sync standbys")
				return
			}
			require.LessOrEqual(t, cfg.NumSync, len(standbys), "num_sync must be satisfiable by the listed standbys")

			for mask := 0; mask < 1<<len(standbys); mask++ {
				if bits.OnesCount(uint(mask)) != cfg.NumSync {
					continue
				}
				holders := []*clustermetadatapb.ID{primary}
				for i, s := range standbys {
					if mask&(1<<i) != 0 {
						holders = append(holders, s)
					}
				}
				require.NoError(t, tt.policy.SatisfiedBy(holders), "a write acknowledged by %v is held by too few poolers", clusterIDStrings(holders))
			}
		})
	}
}
