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
	tests := []struct {
		name     string
		pooler   *multiorchdatapb.PoolerHealthState
		expected bool
	}{
		{
			name: "unreachable primary is uninitialized",
			pooler: &multiorchdatapb.PoolerHealthState{
				IsLastCheckValid: false,
				MultiPooler: &clustermetadatapb.MultiPooler{
					Type: clustermetadatapb.PoolerType_PRIMARY,
				},
				PrimaryStatus: &multipoolermanagerdatapb.PrimaryStatus{
					Lsn: "0/123ABC",
				},
			},
			expected: false,
		},
		{
			name: "reachable primary with LSN is initialized",
			pooler: &multiorchdatapb.PoolerHealthState{
				IsLastCheckValid: true,
				MultiPooler: &clustermetadatapb.MultiPooler{
					Type: clustermetadatapb.PoolerType_PRIMARY,
				},
				PrimaryStatus: &multipoolermanagerdatapb.PrimaryStatus{
					Lsn: "0/123ABC",
				},
			},
			expected: true,
		},
		{
			name: "reachable primary without LSN is uninitialized",
			pooler: &multiorchdatapb.PoolerHealthState{
				IsLastCheckValid: true,
				MultiPooler: &clustermetadatapb.MultiPooler{
					Type: clustermetadatapb.PoolerType_PRIMARY,
				},
				PrimaryStatus: &multipoolermanagerdatapb.PrimaryStatus{
					Lsn: "",
				},
			},
			expected: false,
		},
		{
			name: "reachable replica with replay LSN is initialized",
			pooler: &multiorchdatapb.PoolerHealthState{
				IsLastCheckValid: true,
				MultiPooler: &clustermetadatapb.MultiPooler{
					Type: clustermetadatapb.PoolerType_REPLICA,
				},
				ReplicationStatus: &multipoolermanagerdatapb.StandbyReplicationStatus{
					LastReplayLsn: "0/123ABC",
				},
			},
			expected: true,
		},
		{
			name: "reachable replica with receive LSN is initialized",
			pooler: &multiorchdatapb.PoolerHealthState{
				IsLastCheckValid: true,
				MultiPooler: &clustermetadatapb.MultiPooler{
					Type: clustermetadatapb.PoolerType_REPLICA,
				},
				ReplicationStatus: &multipoolermanagerdatapb.StandbyReplicationStatus{
					LastReceiveLsn: "0/123ABC",
				},
			},
			expected: true,
		},
		{
			name: "reachable replica without LSNs is uninitialized",
			pooler: &multiorchdatapb.PoolerHealthState{
				IsLastCheckValid: true,
				MultiPooler: &clustermetadatapb.MultiPooler{
					Type: clustermetadatapb.PoolerType_REPLICA,
				},
				ReplicationStatus: &multipoolermanagerdatapb.StandbyReplicationStatus{},
			},
			expected: false,
		},
		{
			name: "unreachable replica is uninitialized",
			pooler: &multiorchdatapb.PoolerHealthState{
				IsLastCheckValid: false,
				MultiPooler: &clustermetadatapb.MultiPooler{
					Type: clustermetadatapb.PoolerType_REPLICA,
				},
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
