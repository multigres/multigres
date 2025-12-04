// Copyright 2025 Supabase, Inc.
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

package store

import (
	"testing"

	"github.com/stretchr/testify/require"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

func TestPoolerHealthState_IsInitialized(t *testing.T) {
	// IsInitialized() now uses the IsInitialized field from Status RPC directly,
	// based on data directory state, not LSN values.
	tests := []struct {
		name     string
		pooler   *multiorchdatapb.PoolerHealthState
		expected bool
	}{
		{
			name: "unreachable node is uninitialized even with IsInitialized=true",
			pooler: &multiorchdatapb.PoolerHealthState{
				IsLastCheckValid: false,
				MultiPooler:      &clustermetadatapb.MultiPooler{},
				IsInitialized:    true,
			},
			expected: false,
		},
		{
			name: "reachable node with IsInitialized=true is initialized",
			pooler: &multiorchdatapb.PoolerHealthState{
				IsLastCheckValid: true,
				MultiPooler:      &clustermetadatapb.MultiPooler{},
				IsInitialized:    true,
			},
			expected: true,
		},
		{
			name: "reachable node with IsInitialized=false is uninitialized",
			pooler: &multiorchdatapb.PoolerHealthState{
				IsLastCheckValid: true,
				MultiPooler:      &clustermetadatapb.MultiPooler{},
				IsInitialized:    false,
			},
			expected: false,
		},
		{
			name: "reachable primary with IsInitialized=true is initialized",
			pooler: &multiorchdatapb.PoolerHealthState{
				IsLastCheckValid: true,
				MultiPooler:      &clustermetadatapb.MultiPooler{},
				PoolerType:       clustermetadatapb.PoolerType_PRIMARY,
				IsInitialized:    true,
				PrimaryStatus: &multipoolermanagerdatapb.PrimaryStatus{
					Lsn: "0/123ABC",
				},
			},
			expected: true,
		},
		{
			name: "reachable replica with IsInitialized=true is initialized",
			pooler: &multiorchdatapb.PoolerHealthState{
				IsLastCheckValid: true,
				MultiPooler:      &clustermetadatapb.MultiPooler{},
				PoolerType:       clustermetadatapb.PoolerType_REPLICA,
				IsInitialized:    true,
				ReplicationStatus: &multipoolermanagerdatapb.StandbyReplicationStatus{
					LastReplayLsn: "0/123ABC",
				},
			},
			expected: true,
		},
		{
			name: "reachable replica with IsInitialized=false is uninitialized even with LSN",
			pooler: &multiorchdatapb.PoolerHealthState{
				IsLastCheckValid: true,
				MultiPooler:      &clustermetadatapb.MultiPooler{},
				PoolerType:       clustermetadatapb.PoolerType_REPLICA,
				IsInitialized:    false,
				ReplicationStatus: &multipoolermanagerdatapb.StandbyReplicationStatus{
					LastReplayLsn: "0/123ABC",
				},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsInitialized(tt.pooler)
			require.Equal(t, tt.expected, got)
		})
	}
}
