// Copyright 2025 Supabase, Inc.
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

package store

import (
	"testing"

	"github.com/stretchr/testify/assert"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdata "github.com/multigres/multigres/go/pb/multiorchdata"
	"github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

func TestIsInitialized(t *testing.T) {
	// IsInitialized() now uses the IsInitialized field from Status RPC directly,
	// based on data directory state, not LSN values.
	tests := []struct {
		name     string
		pooler   *multiorchdata.PoolerHealthState
		expected bool
	}{
		{
			name: "unreachable node is not initialized",
			pooler: &multiorchdata.PoolerHealthState{
				IsLastCheckValid: false,
				MultiPooler:      &clustermetadatapb.MultiPooler{},
				IsInitialized:    true, // even if field is true, unreachable means not initialized
			},
			expected: false,
		},
		{
			name: "nil MultiPooler is not initialized",
			pooler: &multiorchdata.PoolerHealthState{
				IsLastCheckValid: true,
				MultiPooler:      nil,
				IsInitialized:    true,
			},
			expected: false,
		},
		{
			name: "reachable node with IsInitialized=true is initialized",
			pooler: &multiorchdata.PoolerHealthState{
				IsLastCheckValid: true,
				MultiPooler:      &clustermetadatapb.MultiPooler{},
				IsInitialized:    true,
			},
			expected: true,
		},
		{
			name: "reachable node with IsInitialized=false is not initialized",
			pooler: &multiorchdata.PoolerHealthState{
				IsLastCheckValid: true,
				MultiPooler:      &clustermetadatapb.MultiPooler{},
				IsInitialized:    false,
			},
			expected: false,
		},
		{
			name: "reachable primary with IsInitialized=true is initialized",
			pooler: &multiorchdata.PoolerHealthState{
				IsLastCheckValid: true,
				MultiPooler:      &clustermetadatapb.MultiPooler{},
				PoolerType:       clustermetadatapb.PoolerType_PRIMARY,
				IsInitialized:    true,
				PrimaryStatus:    &multipoolermanagerdata.PrimaryStatus{Lsn: "0/1234"},
			},
			expected: true,
		},
		{
			name: "reachable replica with IsInitialized=true is initialized",
			pooler: &multiorchdata.PoolerHealthState{
				IsLastCheckValid: true,
				MultiPooler:      &clustermetadatapb.MultiPooler{},
				PoolerType:       clustermetadatapb.PoolerType_REPLICA,
				IsInitialized:    true,
				ReplicationStatus: &multipoolermanagerdata.StandbyReplicationStatus{
					LastReplayLsn: "0/1234",
				},
			},
			expected: true,
		},
		{
			name: "reachable replica with IsInitialized=false is not initialized even with LSN",
			pooler: &multiorchdata.PoolerHealthState{
				IsLastCheckValid: true,
				MultiPooler:      &clustermetadatapb.MultiPooler{},
				PoolerType:       clustermetadatapb.PoolerType_REPLICA,
				IsInitialized:    false,
				ReplicationStatus: &multipoolermanagerdata.StandbyReplicationStatus{
					LastReplayLsn: "0/1234",
				},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsInitialized(tt.pooler)
			assert.Equal(t, tt.expected, result)
		})
	}
}
