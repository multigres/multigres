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
