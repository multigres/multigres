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
			},
			expected: false,
		},
		{
			name: "nil MultiPooler is not initialized",
			pooler: &multiorchdata.PoolerHealthState{
				IsLastCheckValid: true,
				MultiPooler:      nil,
			},
			expected: false,
		},
		{
			name: "primary with LSN is initialized",
			pooler: &multiorchdata.PoolerHealthState{
				IsLastCheckValid: true,
				MultiPooler:      &clustermetadatapb.MultiPooler{},
				PoolerType:       clustermetadatapb.PoolerType_PRIMARY,
				PrimaryStatus:    &multipoolermanagerdata.PrimaryStatus{Lsn: "0/1234"},
			},
			expected: true,
		},
		{
			name: "primary without LSN is not initialized",
			pooler: &multiorchdata.PoolerHealthState{
				IsLastCheckValid: true,
				MultiPooler:      &clustermetadatapb.MultiPooler{},
				PoolerType:       clustermetadatapb.PoolerType_PRIMARY,
				PrimaryStatus:    &multipoolermanagerdata.PrimaryStatus{Lsn: ""},
			},
			expected: false,
		},
		{
			name: "primary with nil PrimaryStatus is not initialized",
			pooler: &multiorchdata.PoolerHealthState{
				IsLastCheckValid: true,
				MultiPooler:      &clustermetadatapb.MultiPooler{},
				PoolerType:       clustermetadatapb.PoolerType_PRIMARY,
				PrimaryStatus:    nil,
			},
			expected: false,
		},
		{
			name: "replica with LastReplayLsn is initialized",
			pooler: &multiorchdata.PoolerHealthState{
				IsLastCheckValid: true,
				MultiPooler:      &clustermetadatapb.MultiPooler{},
				PoolerType:       clustermetadatapb.PoolerType_REPLICA,
				ReplicationStatus: &multipoolermanagerdata.StandbyReplicationStatus{
					LastReplayLsn: "0/1234",
				},
			},
			expected: true,
		},
		{
			name: "replica with LastReceiveLsn is initialized",
			pooler: &multiorchdata.PoolerHealthState{
				IsLastCheckValid: true,
				MultiPooler:      &clustermetadatapb.MultiPooler{},
				PoolerType:       clustermetadatapb.PoolerType_REPLICA,
				ReplicationStatus: &multipoolermanagerdata.StandbyReplicationStatus{
					LastReceiveLsn: "0/5678",
				},
			},
			expected: true,
		},
		{
			name: "replica with empty LSNs is not initialized",
			pooler: &multiorchdata.PoolerHealthState{
				IsLastCheckValid: true,
				MultiPooler:      &clustermetadatapb.MultiPooler{},
				PoolerType:       clustermetadatapb.PoolerType_REPLICA,
				ReplicationStatus: &multipoolermanagerdata.StandbyReplicationStatus{
					LastReplayLsn:  "",
					LastReceiveLsn: "",
				},
			},
			expected: false,
		},
		{
			name: "replica with nil ReplicationStatus is not initialized",
			pooler: &multiorchdata.PoolerHealthState{
				IsLastCheckValid:  true,
				MultiPooler:       &clustermetadatapb.MultiPooler{},
				PoolerType:        clustermetadatapb.PoolerType_REPLICA,
				ReplicationStatus: nil,
			},
			expected: false,
		},
		{
			name: "UNKNOWN type with ReplicationStatus is initialized (fallback to replica check)",
			pooler: &multiorchdata.PoolerHealthState{
				IsLastCheckValid: true,
				MultiPooler:      &clustermetadatapb.MultiPooler{},
				PoolerType:       clustermetadatapb.PoolerType_UNKNOWN,
				ReplicationStatus: &multipoolermanagerdata.StandbyReplicationStatus{
					LastReplayLsn: "0/1234",
				},
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsInitialized(tt.pooler)
			assert.Equal(t, tt.expected, result)
		})
	}
}
