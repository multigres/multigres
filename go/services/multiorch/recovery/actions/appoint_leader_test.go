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

package actions

import (
	"testing"

	"github.com/stretchr/testify/assert"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// Note: AppointLeaderAction.Execute delegates the election itself to a concrete
// consensus.Coordinator, exercised by the consensus package and integration
// tests. The unit-testable logic here is the guard that decides whether the
// shard already has a healthy primary (and so appointment can be skipped):
// isReachableConsensusLeader.

func consensusLeaderStatus(id *clustermetadatapb.ID) *clustermetadatapb.ConsensusStatus {
	return &clustermetadatapb.ConsensusStatus{
		Id: id,
		CurrentPosition: &clustermetadatapb.PoolerPosition{
			Rule: &clustermetadatapb.ShardRule{
				RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 1},
				LeaderId:   id,
			},
		},
	}
}

func TestIsReachableConsensusLeader(t *testing.T) {
	selfID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "node"}
	otherID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "other"}

	t.Run("consensus leader, reachable, postgres ready", func(t *testing.T) {
		pooler := &multiorchdatapb.PoolerHealthState{
			MultiPooler:      &clustermetadatapb.MultiPooler{Id: selfID},
			IsLastCheckValid: true,
			ConsensusStatus:  consensusLeaderStatus(selfID),
			Status:           &multipoolermanagerdatapb.Status{PostgresReady: true},
		}
		assert.True(t, isReachableConsensusLeader(pooler))
	})

	t.Run("REGRESSION: postgres-primary but NOT consensus leader does not count", func(t *testing.T) {
		// A stale/superseded primary: postgres still reports PRIMARY (not in
		// recovery) and it is reachable and ready, but its consensus status names
		// a different leader. The old PoolerType-based guard treated this as the
		// existing healthy primary and suppressed failover; the consensus-based
		// guard must not.
		pooler := &multiorchdatapb.PoolerHealthState{
			MultiPooler:      &clustermetadatapb.MultiPooler{Id: selfID},
			IsLastCheckValid: true,
			// This node's own consensus view: it is selfID, but the current rule
			// names otherID as leader — so it is not the consensus leader.
			ConsensusStatus: &clustermetadatapb.ConsensusStatus{
				Id: selfID,
				CurrentPosition: &clustermetadatapb.PoolerPosition{
					Rule: &clustermetadatapb.ShardRule{
						RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 1},
						LeaderId:   otherID,
					},
				},
			},
			Status: &multipoolermanagerdatapb.Status{
				PoolerType:    clustermetadatapb.PoolerType_PRIMARY, // postgres primary mode
				PostgresReady: true,
			},
		}
		assert.False(t, isReachableConsensusLeader(pooler),
			"a postgres-primary that is not the consensus leader must not suppress appointment")
	})

	t.Run("consensus leader but not reachable", func(t *testing.T) {
		pooler := &multiorchdatapb.PoolerHealthState{
			MultiPooler:      &clustermetadatapb.MultiPooler{Id: selfID},
			IsLastCheckValid: false,
			ConsensusStatus:  consensusLeaderStatus(selfID),
			Status:           &multipoolermanagerdatapb.Status{PostgresReady: true},
		}
		assert.False(t, isReachableConsensusLeader(pooler))
	})

	t.Run("consensus leader but postgres not ready", func(t *testing.T) {
		pooler := &multiorchdatapb.PoolerHealthState{
			MultiPooler:      &clustermetadatapb.MultiPooler{Id: selfID},
			IsLastCheckValid: true,
			ConsensusStatus:  consensusLeaderStatus(selfID),
			Status:           &multipoolermanagerdatapb.Status{PostgresReady: false},
		}
		assert.False(t, isReachableConsensusLeader(pooler))
	})

	t.Run("no consensus status", func(t *testing.T) {
		pooler := &multiorchdatapb.PoolerHealthState{
			MultiPooler:      &clustermetadatapb.MultiPooler{Id: selfID},
			IsLastCheckValid: true,
			Status:           &multipoolermanagerdatapb.Status{PostgresReady: true},
		}
		assert.False(t, isReachableConsensusLeader(pooler))
	})

	t.Run("nil MultiPooler", func(t *testing.T) {
		assert.False(t, isReachableConsensusLeader(&multiorchdatapb.PoolerHealthState{}))
	})
}
